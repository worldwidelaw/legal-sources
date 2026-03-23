#!/usr/bin/env python3
"""
IT/Lazio -- Regional Legislation of Lazio Data Fetcher

Fetches regional legislation from Lazio via Consiglio Regionale del Lazio website.

Strategy:
  - Iterate through years (1971-present)
  - Search for laws in each year via the form-based search
  - Handle pagination (10 results per page)
  - Fetch each law detail page for full text
  - Parse HTML to extract content from #contenuto_legge div

Endpoints:
  - Search: https://www.consiglio.regione.lazio.it/?vw=leggiregionali&sv=vigente&annoLegge={year}&invia=+Cerca+&pg={page}
  - Detail: https://www.consiglio.regione.lazio.it/?vw=leggiregionalidettaglio&id={id}&sv=vigente

Data:
  - Coverage: 1971-present (Regional laws since Lazio's first legislature)
  - License: CC BY-NC-SA 2.0
  - Document types: Legge Regionale (regional laws)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent years)
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
logger = logging.getLogger("legal-data-hunter.IT.lazio")

# Base URL
BASE_URL = "https://www.consiglio.regione.lazio.it"

# Year range for regional laws (Lazio region established 1970, first laws 1971)
START_YEAR = 1971
CURRENT_YEAR = datetime.now().year


class LazioScraper(BaseScraper):
    """
    Scraper for IT/Lazio -- Lazio Regional Legislation.
    Country: IT
    URL: https://www.consiglio.regione.lazio.it/?vw=leggiregionali

    Data types: legislation
    Auth: none (CC BY-NC-SA 2.0)
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

    def _search_year(self, year: int, page: int = 1) -> List[dict]:
        """
        Search for laws in a given year and page.

        Returns list of dict with id, title, number, date from search results.
        """
        url = (
            f"{BASE_URL}/?vw=leggiregionali&sv=vigente&materia=&numeroLegge="
            f"&annoLegge={year}&testo=&invia=+Cerca+&pg={page}"
        )

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')
            results = []

            # Find law links in search results
            # Pattern: <li>Legge n° X del DATE<br><a href='?vw=leggiregionalidettaglio&id=NNN&sv=vigente'>TITLE</a></li>
            for li in soup.select('ul.arrow_list li'):
                text = li.get_text(separator=' ', strip=True)

                # Extract law number and date from text before <a>
                # "Legge n° 1 del 10 gennaio 2024"
                match = re.search(r'Legge\s+n°?\s*(\d+)\s+del\s+(\d+\s+\w+\s+\d{4})', text, re.IGNORECASE)
                if not match:
                    continue

                law_number = match.group(1)
                date_str = match.group(2)

                # Find the detail link
                link = li.find('a', href=re.compile(r'leggiregionalidettaglio'))
                if not link:
                    continue

                href = link.get('href', '')
                id_match = re.search(r'id=(\d+)', href)
                if not id_match:
                    continue

                law_id = id_match.group(1)
                title = link.get_text(strip=True)

                results.append({
                    'id': law_id,
                    'law_number': law_number,
                    'date_str': date_str,
                    'title': title,
                    'year': year,
                })

            return results

        except Exception as e:
            logger.error(f"Error searching year {year} page {page}: {e}")
            return []

    def _get_total_pages(self, year: int) -> int:
        """Get total number of pages for a year's search results."""
        url = (
            f"{BASE_URL}/?vw=leggiregionali&sv=vigente&materia=&numeroLegge="
            f"&annoLegge={year}&testo=&invia=+Cerca+"
        )

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Find pagination buttons
            # <a href='?vw=leggiregionali&sv=vigente&annoLegge=2020&pg=3' class='button color' title='vai alla pagina'>3</a>
            page_links = soup.find_all('a', class_='button', href=re.compile(r'pg=\d+'))

            max_page = 1
            for link in page_links:
                pg_match = re.search(r'pg=(\d+)', link.get('href', ''))
                if pg_match:
                    page_num = int(pg_match.group(1))
                    max_page = max(max_page, page_num)

            return max_page

        except Exception as e:
            logger.warning(f"Error getting page count for year {year}: {e}")
            return 1

    def _fetch_law_detail(self, law_id: str) -> Optional[dict]:
        """
        Fetch full details for a law by ID.

        Returns dict with metadata and full text, or None on failure.
        """
        url = f"{BASE_URL}/?vw=leggiregionalidettaglio&id={law_id}&sv=vigente"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Extract metadata from #dati_legge
            dati_legge = soup.find('div', id='dati_legge')
            metadata = {}

            if dati_legge:
                text = dati_legge.get_text(separator='\n', strip=True)

                # Parse: "Numero della legge: 1\nData: 10 gennaio 2024\nNumero BUR: 4\nData BUR: 11/01/2024"
                for line in text.split('\n'):
                    if ':' in line:
                        key, _, value = line.partition(':')
                        key = key.strip().lower()
                        value = value.strip()

                        if 'numero della legge' in key:
                            metadata['law_number'] = value
                        elif key == 'data':
                            metadata['date_str'] = value
                        elif 'numero bur' in key:
                            metadata['bur_number'] = value
                        elif 'data bur' in key:
                            metadata['bur_date'] = value

            # Extract full text from #contenuto_legge
            contenuto = soup.find('div', id='contenuto_legge')
            full_text = ""

            if contenuto:
                # Remove the disclaimer paragraph
                disclaimer = contenuto.find('p', id='firma_legge')
                if disclaimer:
                    disclaimer.decompose()

                # Get text content, preserving some structure
                full_text = self._extract_text(contenuto)

            if not full_text or len(full_text) < 100:
                logger.warning(f"Insufficient text for law {law_id}: {len(full_text)} chars")
                return None

            return {
                'id': law_id,
                'full_text': full_text,
                **metadata,
            }

        except Exception as e:
            logger.error(f"Error fetching law {law_id}: {e}")
            return None

    def _extract_text(self, element) -> str:
        """
        Extract clean text from HTML element.

        Preserves article/paragraph structure while removing HTML.
        """
        # Get text with some structure
        lines = []

        for p in element.find_all(['p', 'div'], recursive=True):
            text = p.get_text(separator=' ', strip=True)
            if text:
                lines.append(text)

        # If no paragraphs found, get all text
        if not lines:
            lines = [element.get_text(separator=' ', strip=True)]

        # Join and clean
        full_text = '\n\n'.join(lines)

        # Clean up whitespace
        full_text = re.sub(r'[ \t]+', ' ', full_text)
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)

        # Decode HTML entities
        full_text = html.unescape(full_text)

        return full_text.strip()

    def _parse_italian_date(self, date_str: str) -> Optional[str]:
        """
        Parse Italian date string to ISO format.

        Examples:
          "10 gennaio 2024" -> "2024-01-10"
          "5 maggio 2023" -> "2023-05-05"
        """
        months = {
            'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
            'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
            'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12',
        }

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
        Yield all regional legislation from Lazio.

        Iterates through years from 1971 to present.
        """
        for year in range(START_YEAR, CURRENT_YEAR + 1):
            logger.info(f"Fetching laws for year {year}...")

            total_pages = self._get_total_pages(year)
            logger.info(f"Year {year} has {total_pages} page(s)")

            seen_ids: Set[str] = set()

            for page in range(1, total_pages + 1):
                results = self._search_year(year, page)
                logger.info(f"Year {year} page {page}: {len(results)} laws found")

                for result in results:
                    law_id = result['id']

                    # Skip duplicates
                    if law_id in seen_ids:
                        continue
                    seen_ids.add(law_id)

                    # Fetch full details
                    detail = self._fetch_law_detail(law_id)
                    if not detail:
                        continue

                    # Merge search result info with detail
                    yield {
                        **result,
                        **detail,
                    }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents from recent years.

        Checks years from 'since' date to present.
        """
        since_year = since.year

        for year in range(since_year, CURRENT_YEAR + 1):
            logger.info(f"Checking updates for year {year}...")

            total_pages = self._get_total_pages(year)
            seen_ids: Set[str] = set()

            for page in range(1, total_pages + 1):
                results = self._search_year(year, page)

                for result in results:
                    law_id = result['id']

                    if law_id in seen_ids:
                        continue
                    seen_ids.add(law_id)

                    detail = self._fetch_law_detail(law_id)
                    if not detail:
                        continue

                    yield {
                        **result,
                        **detail,
                    }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        law_id = raw.get('id', '')
        law_number = raw.get('law_number', '')
        year = raw.get('year', '')

        # Parse date
        date_str = raw.get('date_str', '')
        date_iso = self._parse_italian_date(date_str) if date_str else None

        # Generate document ID
        doc_id = f"lazio_lr_{year}_{law_number}" if year and law_number else f"lazio_{law_id}"

        # Parse title
        title = raw.get('title', '')
        if not title:
            title = f"Legge Regionale n. {law_number}/{year}"
        else:
            # Clean up title
            title = html.unescape(title)

        # Build URL
        url = f"{BASE_URL}/?vw=leggiregionalidettaglio&id={law_id}&sv=vigente"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IT/Lazio",
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
            "bur_date": raw.get('bur_date', ''),
            "document_type": "Legge Regionale",
            "language": "it",
            "region": "Lazio",
            "country": "IT",
            "internal_id": law_id,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Lazio Regional Council endpoints...")

        # Test 1: Search endpoint
        print("\n1. Testing search (year 2024)...")
        try:
            results = self._search_year(2024, 1)
            print(f"   Found {len(results)} laws on page 1")
            if results:
                print(f"   Sample: Law {results[0]['law_number']}: {results[0]['title'][:60]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test 2: Pagination
        print("\n2. Testing pagination (year 2020)...")
        try:
            pages = self._get_total_pages(2020)
            print(f"   Year 2020 has {pages} pages")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test 3: Detail page
        print("\n3. Testing detail page (first 2024 law)...")
        try:
            results = self._search_year(2024, 1)
            if results:
                law_id = results[0]['id']
                detail = self._fetch_law_detail(law_id)
                if detail:
                    text_len = len(detail.get('full_text', ''))
                    print(f"   Law ID {law_id}: {text_len:,} characters of text")
                    if text_len > 0:
                        print(f"   Text preview: {detail['full_text'][:200]}...")
                else:
                    print("   ERROR: Failed to fetch detail")
            else:
                print("   ERROR: No laws found to test")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test 4: Year range coverage
        print("\n4. Testing year coverage...")
        try:
            for year in [1980, 1990, 2000, 2010, 2020]:
                results = self._search_year(year, 1)
                pages = self._get_total_pages(year)
                print(f"   {year}: {len(results)} laws on page 1, {pages} total pages")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = LazioScraper()

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
