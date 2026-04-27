#!/usr/bin/env python3
"""
ES/AEPD -- Agencia Española de Protección de Datos Data Fetcher

Fetches resolutions from the Spanish Data Protection Authority.
Uses PDF enumeration to discover all documents across resolution types and years.

Strategy:
  - Bootstrap: Enumerates all resolution types (PS, AI, PD, PA, TD) across years
    by constructing PDF URLs directly. Falls back to RSS feed for recent additions.
  - Update: Fetches from RSS feed + enumerates current year.
  - Full text extracted from PDFs using pdfminer.

Data source: https://www.aepd.es/informes-y-resoluciones/resoluciones
License: Open Government License

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap-fast       # Concurrent download (recommended)
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

import requests
import xml.etree.ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.AEPD")

# Constants
RSS_URL = "https://www.aepd.es/informes-y-resoluciones/resoluciones/feed.xml"
BASE_DOC_URL = "https://www.aepd.es/documento/"
REQUEST_TIMEOUT = 60

# Resolution types to enumerate
# Based on actual AEPD resolution patterns
RESOLUTION_TYPES = ['PS', 'AI', 'PD', 'PA', 'TD']

# Resolution type names for metadata
RESOLUTION_TYPE_NAMES = {
    'PS': 'Procedimiento Sancionador',
    'AI': 'Archivo de Actuaciones',
    'PD': 'Procedimiento de Derechos',
    'PA': 'Procedimiento de Apercibimiento',
    'TD': 'Tutela de Derechos',
    'REPOSICION': 'Recurso de Reposición',
    'IT': 'Informe Técnico',
    'AT': 'Autorización de Transferencia',
    'IN': 'Informe',
    'EI': 'Expediente de Inspección',
    'AAPP': 'Administraciones Públicas',
    'AP': 'Archivo de Procedimiento'
}

# User agent to avoid being blocked
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'application/pdf,*/*',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
}

# Year range for enumeration (AEPD digital archive starts reliably around 2018)
START_YEAR = 2018
END_YEAR = datetime.now().year


class AEPDScraper(BaseScraper):
    """
    Scraper for ES/AEPD -- Spanish Data Protection Authority.
    Country: ES
    URL: https://www.aepd.es

    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="ES/AEPD",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _clean_text(self, text: str) -> str:
        """Clean extracted PDF text."""
        if not text:
            return ""

        # Remove page numbers like "1 / 7" or "1/7" at the start of pages
        text = re.sub(r'^\s*\d+\s*/\s*\d+\s*$', '', text, flags=re.MULTILINE)

        # Normalize whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)

        # Remove common header/footer patterns
        text = re.sub(r'c/\s*Jorge\s*Juan.*?Madrid', '', text, flags=re.IGNORECASE)
        text = re.sub(r'www\.aepd\.es', '', text, flags=re.IGNORECASE)

        return text.strip()

    def _parse_resolution_id(self, title: str) -> tuple:
        """
        Parse resolution ID to extract type, number, and year.
        E.g., "PS-00116-2025" -> ("PS", "00116", "2025")
        """
        # Handle REPOSICION first (compound prefix)
        reposicion_match = re.match(r'REPOSICION[-_]([A-Z]+)[-_](\d+)[-_](\d{4})', title, re.IGNORECASE)
        if reposicion_match:
            return ('REPOSICION-' + reposicion_match.group(1), reposicion_match.group(2), reposicion_match.group(3))

        # Standard pattern: TYPE-NUMBER-YEAR
        match = re.match(r'([A-Z]+)[-_](\d+)[-_](\d{4})', title, re.IGNORECASE)
        if match:
            return (match.group(1).upper(), match.group(2), match.group(3))

        return (None, None, None)

    def _extract_expediente(self, text: str) -> Optional[str]:
        """Extract expediente number from resolution text."""
        match = re.search(r'Expediente\s+N[\.°º]*\s*:?\s*(EXP\d+)', text, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _check_pdf_exists(self, pdf_url: str) -> bool:
        """
        Check if a PDF exists at the given URL.

        Uses GET with stream=True and reads only the first bytes to verify,
        because AEPD server returns 500 for HEAD requests even on valid PDFs.
        """
        try:
            response = requests.get(
                pdf_url,
                headers=HEADERS,
                timeout=30,
                stream=True
            )
            if response.status_code != 200:
                return False

            # Read just the first few bytes to check PDF magic number
            first_bytes = response.raw.read(10)
            response.close()

            return first_bytes.startswith(b'%PDF')
        except Exception:
            return False

    def _enumerate_resolution_type(
        self,
        res_type: str,
        year: int,
        max_consecutive_failures: int = 50,
        max_number: int = 1000,
    ) -> Generator[dict, None, None]:
        """
        Enumerate resolutions for a specific type and year.
        Stops after max_consecutive_failures 404s to avoid wasting requests.
        """
        consecutive_failures = 0
        found_count = 0

        for num in range(1, max_number + 1):
            num_str = f"{num:05d}"
            doc_id = f"{res_type.lower()}-{num_str}-{year}"
            pdf_url = f"{BASE_DOC_URL}{doc_id}.pdf"

            if self._check_pdf_exists(pdf_url):
                consecutive_failures = 0
                found_count += 1

                yield {
                    'title': f"{res_type.upper()}-{num_str}-{year}",
                    'pdf_url': pdf_url,
                    'description': ''
                }

                # Small delay between HEAD requests
                time.sleep(0.3)
            else:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.debug(
                        f"Stopping {res_type}-{year} enumeration after "
                        f"{max_consecutive_failures} consecutive 404s at number {num}"
                    )
                    break
                time.sleep(0.2)

        if found_count > 0:
            logger.info(f"Found {found_count} documents for {res_type}-{year}")

    def _fetch_from_rss(self) -> Generator[dict, None, None]:
        """Fetch recent resolutions from RSS feed."""
        try:
            response = requests.get(RSS_URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            root = ET.fromstring(response.content)

            for item in root.findall('.//item'):
                title_elem = item.find('title')
                link_elem = item.find('link')
                description_elem = item.find('description')

                if title_elem is None or link_elem is None:
                    continue

                title = title_elem.text.strip() if title_elem.text else ''
                pdf_url = link_elem.text.strip() if link_elem.text else ''
                description = description_elem.text.strip() if description_elem is not None and description_elem.text else ''

                if not title or not pdf_url:
                    continue

                yield {
                    'title': title,
                    'pdf_url': pdf_url,
                    'description': description
                }

        except Exception as e:
            logger.error(f"Error fetching RSS feed: {e}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all AEPD resolutions by enumerating across types and years.
        This is the main bootstrap method that discovers all documents.
        """
        seen_ids = set()
        current_year = datetime.now().year

        # First, enumerate all resolution types across years
        for year in range(current_year, START_YEAR - 1, -1):
            for res_type in RESOLUTION_TYPES:
                logger.info(f"Enumerating {res_type}-{year}...")

                for item in self._enumerate_resolution_type(res_type, year):
                    title = item['title']

                    if title in seen_ids:
                        continue
                    seen_ids.add(title)

                    # Yield the raw item - full text extraction happens in normalize()
                    yield item

        # Also check RSS feed for any recent documents not captured by enumeration
        logger.info("Checking RSS feed for recent additions...")
        for item in self._fetch_from_rss():
            title = item['title']

            if title in seen_ids:
                continue
            seen_ids.add(title)

            yield item

        logger.info(f"Enumeration complete. Discovered {len(seen_ids)} unique resolutions.")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Fetch resolutions updated since a given date.
        Uses RSS feed + enumeration of current year.
        """
        seen_ids = set()
        current_year = datetime.now().year

        # Enumerate current year for all types
        for res_type in RESOLUTION_TYPES:
            for item in self._enumerate_resolution_type(res_type, current_year):
                title = item['title']

                if title in seen_ids:
                    continue
                seen_ids.add(title)

                yield item

        # Also check previous year if we're early in the year
        if since.year < current_year:
            for res_type in RESOLUTION_TYPES:
                for item in self._enumerate_resolution_type(res_type, current_year - 1):
                    title = item['title']

                    if title in seen_ids:
                        continue
                    seen_ids.add(title)

                    yield item

        # RSS feed for most recent
        for item in self._fetch_from_rss():
            title = item['title']

            if title in seen_ids:
                continue
            seen_ids.add(title)

            yield item

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw resolution data into normalized schema."""
        title = raw['title']
        pdf_url = raw['pdf_url']

        # Extract full text from PDF
        text = self._extract_pdf_text(pdf_url)

        if not text:
            # Skip documents without extractable text
            return None

        now = datetime.now(timezone.utc).isoformat()
        res_type, number, year = self._parse_resolution_id(title)

        # Build document ID
        doc_id = title.lower().replace(' ', '-')

        # Determine resolution type name
        type_name = RESOLUTION_TYPE_NAMES.get(
            res_type.split('-')[0] if res_type else '',
            'Resolución'
        )
        if res_type and res_type.startswith('REPOSICION'):
            type_name = 'Recurso de Reposición'

        # Build full title
        full_title = f"AEPD {type_name}: {title}"

        # Extract date from text if possible
        date_str = None
        if year:
            date_str = f"{year}-01-01"  # Default to start of year

            # Try to find actual date in text
            date_match = re.search(
                r'(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+de\s+(\d{4})',
                text,
                re.IGNORECASE
            )
            if date_match:
                months = {
                    'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
                    'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
                    'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
                }
                day = date_match.group(1).zfill(2)
                month = months.get(date_match.group(2).lower(), '01')
                found_year = date_match.group(3)
                date_str = f"{found_year}-{month}-{day}"

        # Extract expediente from text
        expediente = self._extract_expediente(text)

        return {
            '_id': doc_id,
            '_source': 'ES/AEPD',
            '_type': 'case_law',
            '_fetched_at': now,
            'title': full_title,
            'text': text,
            'date': date_str,
            'url': pdf_url,
            'expediente': expediente,
            'resolution_type': res_type,
            'resolution_number': number,
            'resolution_year': year,
            'language': 'es'
        }


# ── CLI Interface ─────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description='AEPD resolutions fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'update', 'test-api'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Generate sample data only (10 records)')
    parser.add_argument('--workers', type=int, default=5,
                       help='Number of concurrent workers for bootstrap-fast')
    parser.add_argument('--since', type=str,
                       help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    scraper = AEPDScraper()

    if args.command == 'test-api':
        # Quick connectivity test
        print("Testing RSS feed...")
        try:
            response = requests.get(RSS_URL, headers=HEADERS, timeout=30)
            response.raise_for_status()
            print(f"RSS feed OK: {response.status_code}")

            # Try one PDF
            test_url = f"{BASE_DOC_URL}ps-00001-2024.pdf"
            response = requests.head(test_url, headers=HEADERS, timeout=30)
            print(f"PDF test ({test_url}): {response.status_code}")
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)

    elif args.command == 'bootstrap':
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=20)
        else:
            stats = scraper.bootstrap()
        print(json.dumps(stats, indent=2, default=str))

    elif args.command == 'bootstrap-fast':
        stats = scraper.bootstrap_fast(max_workers=args.workers)
        print(json.dumps(stats, indent=2, default=str))

    elif args.command == 'update':
        if args.since:
            since = datetime.fromisoformat(args.since)
        else:
            # Default: last 30 days
            since = datetime.now(timezone.utc).replace(day=1)
        stats = scraper.update()
        print(json.dumps(stats, indent=2, default=str))


if __name__ == '__main__':
    main()
