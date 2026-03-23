#!/usr/bin/env python3
"""
IT/Veneto -- Regional Legislation of Veneto Data Fetcher

Fetches regional legislation from the Bollettino Ufficiale della Regione del Veneto (BUR).

Strategy:
  - Search the BUR database by act type and date range
  - Extract document IDs from search result links
  - Fetch individual law detail pages
  - Parse full text from HTML content embedded in the page

Endpoints:
  - Search: http://bur.regione.veneto.it/BurvServices/Pubblica/SommarioRicerca.aspx
  - Detail: http://bur.regione.veneto.it/BurvServices/Pubblica/DettaglioLegge.aspx?id={id}

Data:
  - Coverage: 2004-present (BUR online archive)
  - Document types: Leggi Regionali (Regional Laws), Decreti, Deliberazioni
  - Volume: 4,400+ regional laws
  - License: CC0 1.0 Universal (Italian public sector information)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional, Set
from urllib.parse import urlencode

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError as e:
    print(f"ERROR: Required package missing. Install with: pip install requests beautifulsoup4")
    print(f"Missing: {e}")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.veneto")

# Base URLs
BASE_URL = "http://bur.regione.veneto.it/BurvServices"
SEARCH_URL = f"{BASE_URL}/Pubblica/SommarioRicerca.aspx"
DETAIL_URL = f"{BASE_URL}/Pubblica/DettaglioLegge.aspx"

# BUR online archive starts from September 24, 2004
START_DATE = "24/09/2004"
CURRENT_YEAR = datetime.now().year

# Document type codes
DOC_TYPE_LEGGE_REGIONALE = "11"


class VenetoScraper(BaseScraper):
    """
    Scraper for IT/Veneto -- Veneto Regional Legislation.
    Country: IT
    URL: http://bur.regione.veneto.it/BurvServices/

    Data types: legislation
    Auth: none (CC0 1.0 Universal)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        })

    def _get_laws_for_year(self, year: int) -> List[str]:
        """
        Get all law IDs for a given year using SommarioLeggi endpoint.

        This endpoint shows all laws for a year and is more reliable than search.
        Returns list of document IDs.
        """
        url = f"{BASE_URL}/Pubblica/SommarioLeggi.aspx?anno={year}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            # Extract all document IDs from the page
            # Pattern: DettaglioLegge.aspx?id=576774
            ids = re.findall(r'DettaglioLegge\.aspx\?id=(\d+)', resp.text)

            # Remove duplicates while preserving order
            seen = set()
            unique_ids = []
            for doc_id in ids:
                if doc_id not in seen:
                    seen.add(doc_id)
                    unique_ids.append(doc_id)

            return unique_ids

        except Exception as e:
            logger.error(f"Error getting laws for year {year}: {e}")
            return []

    def _search_laws(self, from_date: str, to_date: str, tipo_atto: str = DOC_TYPE_LEGGE_REGIONALE) -> List[str]:
        """
        Search for laws in the given date range.

        Returns list of document IDs.
        NOTE: This returns max ~20 results per page. Use _get_laws_for_year for full enumeration.
        """
        params = {
            "tipoRicerca": "base",
            "oggetto": "legge",  # Search term - required, "*" doesn't work
            "daDta": from_date,
            "aDta": to_date,
            "tipoAtto": tipo_atto,
        }

        url = f"{SEARCH_URL}?{urlencode(params)}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            # Extract all document IDs from the page
            # Pattern: DettaglioLegge.aspx?id=576774
            ids = re.findall(r'DettaglioLegge\.aspx\?id=(\d+)', resp.text)

            # Remove duplicates while preserving order
            seen = set()
            unique_ids = []
            for doc_id in ids:
                if doc_id not in seen:
                    seen.add(doc_id)
                    unique_ids.append(doc_id)

            return unique_ids

        except Exception as e:
            logger.error(f"Search error for {from_date} to {to_date}: {e}")
            return []

    def _fetch_law_detail(self, doc_id: str) -> Optional[dict]:
        """
        Fetch full details for a law by ID.

        Returns dict with metadata and full text, or None on failure.
        """
        url = f"{DETAIL_URL}?id={doc_id}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Extract metadata from page title pattern:
            # "LEGGE REGIONALE  n. 1 del 17 febbraio 2026"
            # "Istituzione del nuovo Comune denominato..."
            metadata = {
                'id': doc_id,
                'law_number': '',
                'date_str': '',
                'title': '',
                'document_type': 'Legge Regionale',
            }

            # Parse the main content
            # The ASP.NET page stores content in a span with viewstate
            # But we can extract from the rendered HTML

            # Look for pattern: LEGGE REGIONALE n. X del DD mese YYYY
            text_content = soup.get_text()

            # Extract law number and date
            match = re.search(
                r'LEGGE\s+REGIONALE\s+n\.\s*(\d+)\s+del\s+(\d+\s+\w+\s+\d{4})',
                text_content,
                re.IGNORECASE
            )
            if match:
                metadata['law_number'] = match.group(1)
                metadata['date_str'] = match.group(2)

            # Extract title - it's in an h3 element with class cust-dettaglio-titolo-atto
            title_elem = soup.find('h3', class_='cust-dettaglio-titolo-atto')
            if title_elem:
                metadata['title'] = title_elem.get_text(strip=True)
            else:
                # Fallback: find title after the header
                # Look for the title in the content
                title_match = re.search(
                    r'LEGGE\s+REGIONALE.*?del\s+\d+\s+\w+\s+\d{4}\s*</h2>\s*<h3[^>]*>([^<]+)',
                    resp.text,
                    re.IGNORECASE | re.DOTALL
                )
                if title_match:
                    metadata['title'] = html.unescape(title_match.group(1).strip())

            # Extract BUR number and date
            bur_match = re.search(r'Bur\s+n\.\s*(\d+)\s+del\s+(\d+\s+\w+\s+\d{4})', text_content, re.IGNORECASE)
            if bur_match:
                metadata['bur_number'] = bur_match.group(1)
                metadata['bur_date_str'] = bur_match.group(2)

            # Extract full text
            # The law content is in the viewstate-rendered HTML
            # Look for the preamble and articles sections
            full_text = self._extract_full_text(soup, resp.text)

            if not full_text or len(full_text) < 100:
                logger.warning(f"Insufficient text for doc {doc_id}: {len(full_text) if full_text else 0} chars")
                return None

            metadata['full_text'] = full_text

            return metadata

        except Exception as e:
            logger.error(f"Error fetching doc {doc_id}: {e}")
            return None

    def _extract_full_text(self, soup: BeautifulSoup, raw_html: str) -> str:
        """
        Extract full law text from the page.

        The content is embedded in the ASP.NET ViewState but also rendered.
        We parse the rendered HTML to get the text.
        """
        text_parts = []

        # The content is in spans with cust-dettaglio-* classes
        # Preamble: class='cust-dettaglio-preamble'
        # Articles: class='cust-dettaglio-articoli'

        preamble = soup.find('p', class_='cust-dettaglio-preamble')
        if preamble:
            text_parts.append(self._clean_html_text(preamble))

        articles = soup.find('p', class_='cust-dettaglio-articoli')
        if articles:
            text_parts.append(self._clean_html_text(articles))

        # If we found content in the structured elements
        if text_parts:
            full_text = '\n\n'.join(text_parts)
        else:
            # Fallback: extract from the ViewState-decoded content
            # Look for content between preamble and articoli patterns
            match = re.search(
                r'<p\s+class=[\'"]cust-dettaglio-preamble[\'"]>(.*?)</p>.*?'
                r'<p\s+class=[\'"]cust-dettaglio-articoli[\'"]>(.*?)</p>',
                raw_html,
                re.DOTALL | re.IGNORECASE
            )
            if match:
                preamble_html = match.group(1)
                articles_html = match.group(2)
                full_text = self._html_to_text(preamble_html + '\n\n' + articles_html)
            else:
                # Last resort: extract all text from the main content area
                # Look for content after the title h3
                content_match = re.search(
                    r'cust-dettaglio-titolo-atto[\'"]>.*?</h3>(.*?)<div\s+class=[\'"]clearfloat',
                    raw_html,
                    re.DOTALL | re.IGNORECASE
                )
                if content_match:
                    full_text = self._html_to_text(content_match.group(1))
                else:
                    full_text = ""

        return full_text.strip()

    def _clean_html_text(self, element) -> str:
        """Clean text from a BeautifulSoup element."""
        if element is None:
            return ""

        # Get text preserving some structure
        text = element.get_text(separator='\n', strip=True)
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)

        return text.strip()

    def _html_to_text(self, html_content: str) -> str:
        """Convert HTML string to plain text."""
        # Decode HTML entities
        text = html.unescape(html_content)

        # Replace <br> and <p> with newlines
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'<p[^>]*>', '', text)

        # Remove all other HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)

        return text.strip()

    def _parse_italian_date(self, date_str: str) -> Optional[str]:
        """
        Parse Italian date string to ISO format.

        Examples:
          "17 febbraio 2026" -> "2026-02-17"
          "5 maggio 2023" -> "2023-05-05"
        """
        months = {
            'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
            'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
            'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12',
        }

        if not date_str:
            return None

        try:
            match = re.match(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str.strip(), re.IGNORECASE)
            if match:
                day = int(match.group(1))
                month_name = match.group(2).lower()
                year = match.group(3)

                if month_name in months:
                    return f"{year}-{months[month_name]}-{day:02d}"
        except Exception:
            pass

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all regional legislation from Veneto.

        Uses year-based listing from 2004 to present.
        """
        for year in range(2004, CURRENT_YEAR + 1):
            logger.info(f"Fetching laws for year {year}...")
            doc_ids = self._get_laws_for_year(year)
            logger.info(f"Year {year}: found {len(doc_ids)} laws")

            for doc_id in doc_ids:
                detail = self._fetch_law_detail(doc_id)
                if detail:
                    detail['year'] = year
                    yield detail

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent period.

        Searches from 'since' date to present.
        """
        from_date = since.strftime("%d/%m/%Y")
        to_date = datetime.now().strftime("%d/%m/%Y")

        logger.info(f"Searching updates from {from_date} to {to_date}...")
        doc_ids = self._search_laws(from_date, to_date)
        logger.info(f"Found {len(doc_ids)} laws since {from_date}")

        for doc_id in doc_ids:
            detail = self._fetch_law_detail(doc_id)
            if detail:
                detail['year'] = datetime.now().year
                yield detail

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get('id', '')
        law_number = raw.get('law_number', '')
        year = raw.get('year', '')

        # Parse date
        date_str = raw.get('date_str', '')
        date_iso = self._parse_italian_date(date_str) if date_str else None

        # Generate document ID
        if year and law_number:
            normalized_id = f"veneto_lr_{year}_{law_number}"
        else:
            normalized_id = f"veneto_{doc_id}"

        # Parse title
        title = raw.get('title', '')
        if not title:
            title = f"Legge Regionale n. {law_number}/{year}" if law_number and year else f"Law {doc_id}"

        # Build URL
        url = f"{DETAIL_URL}?id={doc_id}"

        # Parse BUR date
        bur_date_str = raw.get('bur_date_str', '')
        bur_date_iso = self._parse_italian_date(bur_date_str) if bur_date_str else ''

        return {
            # Required base fields
            "_id": normalized_id,
            "_source": "IT/Veneto",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get('full_text', ''),  # MANDATORY FULL TEXT
            "date": date_iso or '',
            "url": url,
            # Additional metadata
            "law_number": law_number,
            "year": str(year),
            "bur_number": raw.get('bur_number', ''),
            "bur_date": bur_date_iso,
            "document_type": raw.get('document_type', 'Legge Regionale'),
            "language": "it",
            "region": "Veneto",
            "country": "IT",
            "internal_id": doc_id,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Veneto BUR endpoints...")

        # Test 1: Year-based listing (preferred approach)
        print("\n1. Testing year-based listing (2024)...")
        try:
            ids = self._get_laws_for_year(2024)
            print(f"   Found {len(ids)} law IDs for 2024")
            if ids:
                print(f"   Sample IDs: {ids[:5]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test 2: Detail page
        print("\n2. Testing detail page...")
        try:
            if ids:
                doc_id = ids[0]
                detail = self._fetch_law_detail(doc_id)
                if detail:
                    print(f"   Document ID: {doc_id}")
                    print(f"   Law number: {detail.get('law_number', 'N/A')}")
                    print(f"   Date: {detail.get('date_str', 'N/A')}")
                    print(f"   Title: {detail.get('title', 'N/A')[:60]}...")
                    text_len = len(detail.get('full_text', ''))
                    print(f"   Text length: {text_len:,} characters")
                    if text_len > 0:
                        print(f"   Text preview: {detail['full_text'][:200]}...")
                else:
                    print("   ERROR: Failed to fetch detail")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test 3: Year coverage using year-based listing
        print("\n3. Testing year coverage...")
        total_count = 0
        try:
            for year in [2004, 2010, 2015, 2020, 2024, 2025]:
                ids = self._get_laws_for_year(year)
                total_count += len(ids)
                print(f"   {year}: {len(ids)} laws")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test 4: Estimate total
        print("\n4. Total estimate from sampled years...")
        print(f"   Sampled years total: {total_count} laws")
        print(f"   Estimated total (21 years): ~{total_count * 21 // 6} laws")

        print("\nTest complete!")


def main():
    scraper = VenetoScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
