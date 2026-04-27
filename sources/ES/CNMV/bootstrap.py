#!/usr/bin/env python3
"""
ES/CNMV -- Spanish Securities Commission Sanctions Fetcher

Fetches sanctions resolutions from the CNMV public sanctions registry.
Full text extracted from PDF documents published in the Official State Gazette (BOE).

Strategy:
  - Scrapes the paginated sanctions registry HTML table
  - Extracts PDF links for each sanction resolution
  - Downloads PDFs and extracts full text using pdfminer

Data source: https://www.cnmv.es/Portal/Consultas/RegistroSanciones/verRegSanciones
License: Open Government License

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update
"""

import sys
import json
import logging
import re
import time
from io import BytesIO
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, parse_qs, urlparse

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.CNMV")

# Constants
BASE_URL = "https://www.cnmv.es"
SANCTIONS_URL = "https://www.cnmv.es/Portal/Consultas/RegistroSanciones/verRegSanciones"
REQUEST_TIMEOUT = 120

# User agent
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
}


class CNMVScraper(BaseScraper):
    """
    Scraper for ES/CNMV -- Spanish Securities Commission sanctions.
    Country: ES
    URL: https://www.cnmv.es

    Data types: regulatory_decisions
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="ES/CNMV",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _clean_text(self, text: str) -> str:
        """Clean extracted PDF text."""
        if not text:
            return ""

        # Normalize whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)

        # Remove page numbers
        text = re.sub(r'\n\s*\d+\s*\n', '\n', text)

        # Remove common header/footer patterns
        text = re.sub(r'BOLETÍN OFICIAL DEL ESTADO', '', text)
        text = re.sub(r'Núm\.\s*\d+\s*[A-Za-z]+,\s*\d+\s*de\s*[a-z]+\s*de\s*\d{4}', '', text)
        text = re.sub(r'Sec\.\s*[IVX]+\.\s*Pág\.\s*\d+', '', text)
        text = re.sub(r'cve:\s*BOE-[A-Z]-\d+-\d+', '', text, flags=re.IGNORECASE)
        text = re.sub(r'Verificable en https?://[^\s]+', '', text)

        return text.strip()

    def _get_total_pages(self) -> int:
        """Get the total number of pages in the sanctions registry."""
        try:
            response = self.session.get(SANCTIONS_URL, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Find pagination links
            pagination = soup.find_all('a', class_='submit')
            max_page = 0
            for link in pagination:
                href = link.get('href', '')
                if 'page=' in href:
                    match = re.search(r'page=(\d+)', href)
                    if match:
                        page_num = int(match.group(1))
                        max_page = max(max_page, page_num)

            return max_page + 1  # Pages are 0-indexed

        except Exception as e:
            logger.error(f"Error getting total pages: {e}")
            return 1

    def _parse_sanctions_page(self, page: int) -> Generator[dict, None, None]:
        """Parse a single page of the sanctions registry."""
        try:
            url = f"{SANCTIONS_URL}?page={page}"
            response = self.session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')

            # Find the sanctions table
            table = soup.find('table', id='ctl00_ContentPrincipal_grdRegSanciones')
            if not table:
                logger.warning(f"No sanctions table found on page {page}")
                return

            # Find all rows (skip header)
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) < 2:
                    continue

                # First cell contains date and link to PDF
                date_cell = cells[0]
                link = date_cell.find('a')
                if not link:
                    continue

                pdf_url = link.get('href', '')
                if not pdf_url:
                    continue

                # Make absolute URL if needed
                if not pdf_url.startswith('http'):
                    pdf_url = urljoin(BASE_URL, pdf_url)

                # Extract date from link text
                date_text = link.get_text(strip=True)

                # Second cell contains resolution description
                description_cell = cells[1]
                description = description_cell.get_text(strip=True)

                # Third cell may contain additional info (appeals, etc.)
                appeals_info = ""
                if len(cells) > 2:
                    appeals_info = cells[2].get_text(strip=True)

                yield {
                    'pdf_url': pdf_url,
                    'date_text': date_text,
                    'description': description,
                    'appeals_info': appeals_info,
                    'page': page
                }

                # Small delay between requests
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"Error parsing page {page}: {e}")

    def _parse_date(self, date_text: str) -> Optional[str]:
        """Parse date from text like '12/02/2026' to ISO format."""
        if not date_text:
            return None

        # Try DD/MM/YYYY format
        match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_text)
        if match:
            day, month, year = match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        return None

    def _extract_sanction_id(self, pdf_url: str, description: str) -> str:
        """Extract a unique ID for the sanction."""
        # Try to extract from URL parameter
        parsed = urlparse(pdf_url)
        params = parse_qs(parsed.query)
        if 'e' in params:
            # Use hash of the e parameter as part of ID
            e_param = params['e'][0]
            # Take first 20 chars of the encoded parameter
            url_id = re.sub(r'[^a-zA-Z0-9]', '', e_param)[:20]
            return f"cnmv-sancion-{url_id}"

        # Fallback: extract from description
        # Look for BOE reference
        boe_match = re.search(r'BOE[^\)]*(\d+\s*de\s*[a-z]+\s*de\s*\d{4})', description, re.IGNORECASE)
        if boe_match:
            boe_date = boe_match.group(1).lower().replace(' ', '-')
            return f"cnmv-sancion-boe-{boe_date}"

        # Hash the description
        import hashlib
        desc_hash = hashlib.sha256(description.encode()).hexdigest()[:16]
        return f"cnmv-sancion-{desc_hash}"

    def _extract_sanctioned_entity(self, description: str) -> Optional[str]:
        """Extract the name of the sanctioned entity from the description."""
        # Common patterns:
        # "... sanción ... a ENTITY_NAME (BOE...)"
        # "... sanciones ... a don/doña NAME"
        match = re.search(r'sancion(?:es)?\s+(?:por\s+)?[^a]+a\s+(?:don\s+|doña\s+)?([^(]+)', description, re.IGNORECASE)
        if match:
            entity = match.group(1).strip()
            # Clean up
            entity = re.sub(r'\s*\(BOE.*', '', entity, flags=re.IGNORECASE)
            entity = re.sub(r'\s+', ' ', entity).strip()
            if entity:
                return entity
        return None

    def _extract_infraction_type(self, description: str) -> Optional[str]:
        """Extract the type of infraction from the description."""
        if 'muy grave' in description.lower():
            return 'muy_grave'
        elif 'grave' in description.lower():
            return 'grave'
        elif 'leve' in description.lower():
            return 'leve'
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all CNMV sanctions by scraping the paginated registry.
        """
        total_pages = self._get_total_pages()
        logger.info(f"Found {total_pages} pages in sanctions registry")

        seen_urls = set()

        for page in range(total_pages):
            logger.info(f"Processing page {page + 1}/{total_pages}...")

            for item in self._parse_sanctions_page(page):
                pdf_url = item['pdf_url']

                # Skip duplicates
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                yield item

            # Delay between pages
            time.sleep(1)

        logger.info(f"Enumeration complete. Found {len(seen_urls)} unique sanctions.")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Fetch recent sanctions.
        Since the registry doesn't have a date filter API, we fetch first few pages
        and filter by date.
        """
        seen_urls = set()
        pages_to_check = 3  # Recent sanctions should be on first few pages

        for page in range(pages_to_check):
            for item in self._parse_sanctions_page(page):
                pdf_url = item['pdf_url']

                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                # Parse date and filter
                date_str = self._parse_date(item['date_text'])
                if date_str:
                    item_date = datetime.fromisoformat(date_str)
                    if item_date.replace(tzinfo=timezone.utc) < since:
                        continue

                yield item

            time.sleep(1)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw sanction data into normalized schema."""
        pdf_url = raw['pdf_url']
        description = raw['description']
        date_text = raw['date_text']

        # Extract full text from PDF
        text = self._extract_pdf_text(pdf_url)

        if not text:
            logger.debug(f"No text extracted from {pdf_url}")
            return None

        now = datetime.now(timezone.utc).isoformat()

        # Parse date
        date_str = self._parse_date(date_text)

        # Generate unique ID
        doc_id = self._extract_sanction_id(pdf_url, description)

        # Extract metadata
        sanctioned_entity = self._extract_sanctioned_entity(description)
        infraction_type = self._extract_infraction_type(description)

        # Build title
        title = f"CNMV Sanción: {description[:100]}"
        if len(description) > 100:
            title += "..."

        return {
            '_id': doc_id,
            '_source': 'ES/CNMV',
            '_type': 'case_law',  # Administrative decisions are classified as case_law
            '_fetched_at': now,
            'title': title,
            'text': text,
            'date': date_str,
            'url': pdf_url,
            'description': description,
            'sanctioned_entity': sanctioned_entity,
            'infraction_type': infraction_type,
            'appeals_info': raw.get('appeals_info', ''),
            'language': 'es'
        }


# ── CLI Interface ─────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description='CNMV sanctions fetcher')
    parser.add_argument('command', choices=['bootstrap', 'update', 'test-api'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Generate sample data only (10 records)')
    parser.add_argument('--since', type=str,
                       help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    scraper = CNMVScraper()

    if args.command == 'test-api':
        print("Testing CNMV sanctions registry...")
        try:
            response = requests.get(SANCTIONS_URL, headers=HEADERS, timeout=30)
            response.raise_for_status()
            print(f"Registry page OK: {response.status_code}")

            # Try to get total pages
            total = scraper._get_total_pages()
            print(f"Total pages: {total}")

            # Try one PDF
            for item in scraper._parse_sanctions_page(0):
                print(f"Sample item: {item['date_text']} - {item['description'][:80]}...")
                break

        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    elif args.command == 'bootstrap':
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=10)
        else:
            stats = scraper.bootstrap()
        print(json.dumps(stats, indent=2, default=str))

    elif args.command == 'update':
        if args.since:
            since = datetime.fromisoformat(args.since)
        else:
            since = datetime.now(timezone.utc).replace(day=1)
        stats = scraper.update()
        print(json.dumps(stats, indent=2, default=str))


if __name__ == '__main__':
    main()
