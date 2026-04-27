#!/usr/bin/env python3
"""
MX/CDMX-Legislation -- Ciudad de México Legislation Fetcher

Fetches consolidated laws and codes from the Congreso CDMX Marco Legal page.
178 laws/codes as text-based PDFs with full text extraction via PyPDF2.

Strategy:
  - Scrape listing page for law titles and PDF URLs
  - Download each PDF and extract text with PyPDF2
  - Parse metadata from the listing page (title, dates, reform info)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Same as bootstrap (small dataset)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.CDMX-Legislation")

LISTING_URL = "https://www.congresocdmx.gob.mx/marco-legal-cdmx-107-2.html"
BASE_URL = "https://www.congresocdmx.gob.mx"
SOURCE_ID = "MX/CDMX-Legislation"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.5",
}

REQUEST_DELAY = 2  # seconds between PDF downloads


class CDMXLegislationScraper(BaseScraper):
    """Scraper for MX/CDMX-Legislation -- Ciudad de México laws and codes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _parse_date(self, date_str: str) -> str:
        """Parse DD-MM-YYYY to ISO 8601."""
        match = re.match(r'(\d{1,2})-(\d{1,2})-(\d{4})', date_str)
        if match:
            day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}"
        return ""

    def _get_law_list(self) -> List[Dict[str, str]]:
        """Scrape the Marco Legal listing page for law titles and PDF URLs."""
        logger.info(f"Fetching law listing from {LISTING_URL}")
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        resp.encoding = 'utf-8'
        soup = BeautifulSoup(resp.text, 'html.parser')

        laws = []
        for media in soup.find_all('div', class_='media'):
            text = media.get_text(strip=True)
            pdf_link = media.find('a', href=lambda h: h and 'archivo' in h)
            if not pdf_link:
                continue
            href = pdf_link['href']
            if not href.startswith('http'):
                href = f"{BASE_URL}/{href}"
            # Ensure .pdf extension
            if not href.endswith('.pdf'):
                href += '.pdf'

            # Title is before " - Última reforma"
            parts = text.split(' - Última reforma')
            title = parts[0].strip() if parts else text[:200]
            # Clean up title
            if not title:
                title = text[:200]

            # Extract publication date
            pub_match = re.search(r'Fecha de publicación:\s*(\d{1,2}-\d{1,2}-\d{4})', text)
            pub_date = pub_match.group(1) if pub_match else ''

            # Extract reform info
            reform_match = re.search(r'Última reforma publicada en la GOCDMX el (.+?)(?:Fecha|$)', text)
            reform_info = reform_match.group(1).strip() if reform_match else ''

            # Generate stable ID from PDF hash
            pdf_hash = href.split('archivo-')[-1].replace('.pdf', '')[:12]

            laws.append({
                'title': title,
                'pdf_url': href,
                'pdf_hash': pdf_hash,
                'pub_date': pub_date,
                'reform_info': reform_info,
            })

        logger.info(f"Found {len(laws)} laws")
        return laws

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="MX/CDMX-Legislation",
            source_id="",
            pdf_bytes=pdf_content,
            table="legislation",
        ) or ""

    def _fetch_law(self, law: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Download PDF and extract full text for a single law."""
        time.sleep(REQUEST_DELAY)
        try:
            resp = self.session.get(law['pdf_url'], timeout=60)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to download {law['title'][:50]}: {e}")
            return None

        text = self._extract_pdf_text(resp.content)
        if len(text) < 100:
            logger.warning(f"Very short text for {law['title'][:50]}: {len(text)} chars")
            return None

        return {
            'title': law['title'],
            'text': text,
            'pdf_url': law['pdf_url'],
            'pdf_hash': law['pdf_hash'],
            'pub_date': law['pub_date'],
            'reform_info': law['reform_info'],
            'pdf_size': len(resp.content),
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw law data into standard schema."""
        iso_date = self._parse_date(raw.get('pub_date', ''))
        slug = re.sub(r'[^a-z0-9]+', '-', raw['title'].lower())[:60].strip('-')

        return {
            '_id': f"MX-CDMX-{raw['pdf_hash']}",
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw['title'],
            'text': raw['text'],
            'date': iso_date,
            'pub_date_raw': raw.get('pub_date', ''),
            'reform_info': raw.get('reform_info', ''),
            'url': raw['pdf_url'],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all CDMX laws with full text."""
        laws = self._get_law_list()

        if sample:
            # Take evenly spaced sample
            step = max(1, len(laws) // 15)
            laws = laws[::step][:15]

        total = 0
        for i, law in enumerate(laws):
            logger.info(f"[{i+1}/{len(laws)}] Downloading: {law['title'][:60]}...")
            raw = self._fetch_law(law)
            if raw:
                record = self.normalize(raw)
                yield record
                total += 1
                logger.info(f"  OK: {len(raw['text'])} chars")
            else:
                logger.warning(f"  SKIP: {law['title'][:60]}")

        logger.info(f"\nTotal laws fetched: {total}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Fetch all laws (small dataset, always full refresh)."""
        yield from self.fetch_all(sample=False)


if __name__ == "__main__":
    scraper = CDMXLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to congresocdmx.gob.mx...")
        laws = scraper._get_law_list()
        if laws:
            logger.info(f"Found {len(laws)} laws. Testing first PDF...")
            raw = scraper._fetch_law(laws[0])
            if raw:
                logger.info(f"SUCCESS: {raw['title'][:60]} - {len(raw['text'])} chars, {raw['page_count']} pages")
            else:
                logger.error("FAILED: Could not extract text from first PDF")
                sys.exit(1)
        else:
            logger.error("FAILED: Could not fetch law listing")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample)

    elif command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
