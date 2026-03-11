#!/usr/bin/env python3
"""
IT/Normattiva -- Italian National Legislation Database Data Fetcher

Fetches Italian legislation from Normattiva using HTML parsing of ELI pages.

Strategy:
  - Browse by year using /ricerca/elencoPerData/anno/{year} to get document references.
  - Extract codiceRedazionale (editorial code) and dataPubblicazioneGazzetta from listings.
  - Fetch full HTML via ELI endpoints: /eli/id/{YYYY}/{MM}/{DD}//{codiceRedaz}/CONSOLIDATED
  - Parse HTML to extract full text from the 'testoNormalizzato' element.

Endpoints:
  - Year listing: https://www.normattiva.it/ricerca/elencoPerData/anno/2024
  - ELI HTML: https://www.normattiva.it/eli/id/2024/12/30//24G00226/CONSOLIDATED

Data:
  - Legislation types: legge, decreto.legislativo, decreto.legge, etc.
  - Full text extracted from HTML pages
  - License: Open Government Data (Italian IODL 2.0)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent years only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import requests
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.normattiva")

# Base URL for Normattiva
BASE_URL = "https://www.normattiva.it"

# Years to scrape (most recent first for sample mode)
YEARS_TO_SCRAPE = list(range(2025, 2000, -1))  # 2025 down to 2001


class NormattivaScraper(BaseScraper):
    """
    Scraper for IT/Normattiva -- Italian National Legislation Database.
    Country: IT
    URL: https://www.normattiva.it

    Data types: legislation
    Auth: none (Open Government Data - IODL 2.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Use a persistent requests.Session for cookie handling
        self._http_session = requests.Session()
        self._http_session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
        })

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
            },
            timeout=60,
        )

    def _parse_year_listing(self, html_content: str, year: int, page: int = 0) -> List[Dict[str, Any]]:
        """
        Parse the year listing page to extract document references.

        The listing has entries with href like:
        /atto/caricaDettaglioAtto?atto.dataPubblicazioneGazzetta=2024-12-30&atto.codiceRedazionale=24G00226&...

        Returns a list of dicts with: codice_redazionale, title, date_publication
        """
        documents = []

        # Pattern to extract document references from href
        href_pattern = re.compile(
            r'href="[^"]*caricaDettaglioAtto\?'
            r'atto\.dataPubblicazioneGazzetta=(\d{4}-\d{2}-\d{2})'
            r'&atto\.codiceRedazionale=([^&"]+)',
            re.IGNORECASE
        )

        # Find all document entries
        seen_codes = set()
        for match in href_pattern.finditer(html_content):
            try:
                date_pub = match.group(1)  # e.g., 2024-12-30
                codice = match.group(2)    # e.g., 24G00226

                if codice in seen_codes:
                    continue
                seen_codes.add(codice)

                # Try to extract title from surrounding context
                context_start = max(0, match.start() - 200)
                context_end = min(len(html_content), match.end() + 1000)
                context = html_content[context_start:context_end]

                # Extract document type (e.g., "LEGGE", "DECRETO LEGISLATIVO")
                doc_type = "LEGGE"  # Default
                type_match = re.search(r'>([A-Z][A-Z\s-]+(?:LEGGE|DECRETO|COSTITUZIONE)[^<]*)<', context)
                if type_match:
                    doc_type = type_match.group(1).strip()

                # Extract title/description
                title = ""
                title_match = re.search(r'<p[^>]*>([^<]{20,})</p>', context, re.IGNORECASE)
                if title_match:
                    title = html.unescape(title_match.group(1).strip())
                    title = re.sub(r'\s+', ' ', title)

                documents.append({
                    "codice_redazionale": codice,
                    "date_publication": date_pub,
                    "document_type": doc_type,
                    "title_preview": title[:200] if title else "",
                    "year": year,
                })

            except Exception as e:
                logger.warning(f"Failed to parse document reference: {e}")
                continue

        return documents

    def _extract_text_from_html(self, html_content: str) -> tuple:
        """
        Parse the ELI HTML page and extract text content from testoNormalizzato.

        Returns (full_text, metadata_dict)
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            metadata = {}

            # Extract title from meta tag or page title
            title_meta = soup.find('meta', property='eli:title')
            if title_meta and title_meta.get('content'):
                metadata['title'] = html.unescape(title_meta['content'].strip())
            else:
                title_tag = soup.find('title')
                if title_tag:
                    title_text = title_tag.get_text()
                    # Remove " - Normattiva" suffix
                    title_text = re.sub(r'\s*-\s*Normattiva\s*$', '', title_text)
                    metadata['title'] = html.unescape(title_text.strip())

            # Extract document type from meta
            type_meta = soup.find('meta', property='eli:type_document')
            if type_meta and type_meta.get('content'):
                metadata['document_type'] = type_meta['content'].strip()

            # Extract date from meta
            date_meta = soup.find('meta', property='eli:date_document')
            if date_meta and date_meta.get('content'):
                metadata['date_document'] = date_meta['content'].strip()

            # Extract full text from testoNormalizzato div
            testo_div = soup.find(id='testoNormalizzato')
            if testo_div:
                # Remove script and style elements
                for elem in testo_div.find_all(['script', 'style', 'nav', 'button']):
                    elem.decompose()

                # Get text content
                full_text = testo_div.get_text(separator='\n', strip=True)

                # Clean up the text
                # Remove navigation elements like "articolo successivo"
                full_text = re.sub(r'articolo (?:successivo|precedente)\s*', '', full_text, flags=re.IGNORECASE)
                # Remove "Testo in vigore dal:" prefix line
                full_text = re.sub(r'^Testo in vigore dal:[^\n]+\n*', '', full_text.strip())
                # Normalize whitespace
                full_text = re.sub(r'\n{3,}', '\n\n', full_text)
                full_text = full_text.strip()

                return full_text, metadata

            # Fallback: try to find the main content area
            main_content = soup.find('div', class_='container-corpo') or soup.find('main')
            if main_content:
                for elem in main_content.find_all(['script', 'style', 'nav', 'button', 'header', 'footer']):
                    elem.decompose()
                full_text = main_content.get_text(separator='\n', strip=True)
                full_text = re.sub(r'\n{3,}', '\n\n', full_text)
                return full_text.strip(), metadata

            return "", metadata

        except Exception as e:
            logger.warning(f"HTML parsing error: {e}")
            return "", {}

    def _fetch_document_html(self, codice: str, date_pub: str) -> tuple:
        """
        Fetch the ELI HTML page for a document and extract full text.

        Returns (full_text, metadata_dict) or ("", {}) on failure.
        """
        try:
            # Convert date to ELI format: /eli/id/YYYY/MM/DD//codice/CONSOLIDATED
            parts = date_pub.split('-')
            eli_url = f"{BASE_URL}/eli/id/{parts[0]}/{parts[1]}/{parts[2]}//{codice}/CONSOLIDATED"

            self.rate_limiter.wait()

            resp = self._http_session.get(eli_url, timeout=60)

            if resp.status_code != 200:
                logger.warning(f"ELI request failed for {codice}: HTTP {resp.status_code}")
                return "", {}

            content_type = resp.headers.get('Content-Type', '')
            if 'html' not in content_type:
                logger.warning(f"Unexpected content type for {codice}: {content_type}")
                return "", {}

            return self._extract_text_from_html(resp.text)

        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching {codice}")
            return "", {}
        except Exception as e:
            logger.error(f"Failed to fetch HTML for {codice}: {e}")
            return "", {}

    def _fetch_year_listing(self, year: int, page: int = 0) -> List[Dict[str, Any]]:
        """Fetch and parse the listing of documents for a given year and page."""
        url = f"{BASE_URL}/ricerca/elencoPerData/anno/{year}" if page == 0 else f"{BASE_URL}/ricerca/elencoPerData/{page}"

        try:
            self.rate_limiter.wait()
            resp = self._http_session.get(url, timeout=60)
            resp.raise_for_status()

            content = resp.text
            documents = self._parse_year_listing(content, year, page)

            logger.info(f"Found {len(documents)} documents on page {page} for {year}")
            return documents

        except Exception as e:
            logger.error(f"Failed to fetch year listing for {year} page {page}: {e}")
            return []

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from Normattiva.

        Iterates through years, fetching document listings and then
        full HTML for each document.
        """
        for year in YEARS_TO_SCRAPE:
            logger.info(f"Fetching documents from {year}...")

            # Fetch first page
            page = 0
            documents = self._fetch_year_listing(year, page)

            while documents:
                for doc in documents:
                    codice = doc["codice_redazionale"]
                    date_pub = doc["date_publication"]

                    # Fetch full HTML and extract text
                    full_text, metadata = self._fetch_document_html(codice, date_pub)

                    if not full_text:
                        logger.warning(f"No full text for {codice}, skipping")
                        continue

                    # Skip documents with very short text (likely errors)
                    if len(full_text) < 100:
                        logger.warning(f"Text too short for {codice} ({len(full_text)} chars), skipping")
                        continue

                    # Merge metadata with doc info
                    doc["full_text"] = full_text
                    doc["title"] = metadata.get("title", doc.get("title_preview", ""))
                    doc["document_type"] = metadata.get("document_type", doc.get("document_type", ""))
                    doc["date_document"] = metadata.get("date_document", "")

                    yield doc

                # Try next page
                page += 1
                if page > 100:  # Safety limit
                    break
                documents = self._fetch_year_listing(year, page)
                if not documents:
                    break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Fetches recent years and filters by publication date.
        """
        since_year = since.year
        current_year = datetime.now().year

        years_to_check = list(range(current_year, since_year - 1, -1))

        for year in years_to_check:
            logger.info(f"Checking {year} for updates...")

            page = 0
            documents = self._fetch_year_listing(year, page)

            while documents:
                for doc in documents:
                    codice = doc["codice_redazionale"]
                    date_pub = doc["date_publication"]

                    # Filter by date
                    try:
                        doc_date = datetime.strptime(date_pub, '%Y-%m-%d').replace(tzinfo=timezone.utc)
                        if doc_date < since:
                            continue
                    except:
                        pass

                    full_text, metadata = self._fetch_document_html(codice, date_pub)
                    if not full_text or len(full_text) < 100:
                        continue

                    doc["full_text"] = full_text
                    doc["title"] = metadata.get("title", doc.get("title_preview", ""))
                    doc["document_type"] = metadata.get("document_type", "")
                    doc["date_document"] = metadata.get("date_document", "")

                    yield doc

                page += 1
                if page > 50:
                    break
                documents = self._fetch_year_listing(year, page)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        codice = raw.get("codice_redazionale", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        doc_type = raw.get("document_type", "LEGGE")
        date_pub = raw.get("date_publication", "")
        date_doc = raw.get("date_document", date_pub)
        year = raw.get("year", "")

        # Build ELI URL
        if date_pub:
            parts = date_pub.split('-')
            eli_url = f"{BASE_URL}/eli/id/{parts[0]}/{parts[1]}/{parts[2]}//{codice}/CONSOLIDATED"
        else:
            eli_url = f"{BASE_URL}/uri-res/N2Ls?urn:nir:stato:legge:{codice}"

        return {
            # Required base fields
            "_id": codice,
            "_source": "IT/Normattiva",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_doc if date_doc else date_pub,
            "url": eli_url,
            # Additional metadata
            "codice_redazionale": codice,
            "document_type": doc_type,
            "year": year,
            "date_publication": date_pub,
            "language": "it",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Normattiva endpoints...")

        # Test year listing
        print("\n1. Testing year listing (2024)...")
        try:
            docs = self._fetch_year_listing(2024, 0)
            print(f"   Found {len(docs)} documents")
            if docs:
                print(f"   Sample: {docs[0].get('codice_redazionale')} - {docs[0].get('date_publication')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test ELI HTML endpoint
        print("\n2. Testing ELI HTML endpoint...")
        try:
            # Use a known good codice
            test_codice = "24G00226"
            test_date = "2024-12-30"

            text, metadata = self._fetch_document_html(test_codice, test_date)
            print(f"   Text length: {len(text)} characters")
            print(f"   Title: {metadata.get('title', 'N/A')[:80]}...")
            print(f"   Type: {metadata.get('document_type', 'N/A')}")
            if text:
                print(f"   Text sample: {text[:300]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = NormattivaScraper()

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
