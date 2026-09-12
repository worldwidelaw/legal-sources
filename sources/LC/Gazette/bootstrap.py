#!/usr/bin/env python3
"""
LC/Gazette -- Saint Lucia Government Gazette

Fetches weekly Government Gazette issues from the National Printing Corporation
website (npc.govt.lc). Full text extracted from PDF issues using PyMuPDF.

Strategy:
  1. Iterate years 2004-current, months 1-12
  2. Fetch each month page to discover gazette PDF links
  3. Download each PDF and extract text with PyMuPDF
  4. Each gazette issue = one record

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict
from urllib.parse import urljoin, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LC.Gazette")

BASE_URL = "https://npc.govt.lc"
START_YEAR = 2004
MONTHS = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def _extract_text_from_pdf(pdf_bytes: bytes) -> tuple:
    """Extract text from PDF bytes using PyMuPDF. Returns (text, page_count)."""
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page_count = len(doc)
        text_parts = []
        for page in doc:
            page_text = page.get_text()
            if page_text:
                text_parts.append(page_text.strip())
        doc.close()
        return "\n\n".join(text_parts), page_count
    except Exception as e:
        logger.warning(f"PyMuPDF extraction failed: {e}")
        return "", 0


def _parse_date_from_filename(filename: str) -> Optional[str]:
    """Parse date from gazette filename like 'Gazette January 26th, 2026.pdf'."""
    # Remove ordinal suffixes and extra spaces
    cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', filename)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()

    # Try: "Gazette Month Day, Year" or "Gazette Month Day Year"
    m = re.search(
        r'Gazette\s+(\w+)\s+(\d{1,2}),?\s*(\d{4})',
        cleaned, re.IGNORECASE,
    )
    if m:
        month_str, day_str, year_str = m.groups()
        try:
            month_num = MONTHS.index(month_str.capitalize()) + 1
            return f"{int(year_str):04d}-{month_num:02d}-{int(day_str):02d}"
        except (ValueError, IndexError):
            pass

    return None


def _make_gazette_id(year: int, month: int, filename: str) -> str:
    """Create a unique gazette ID from year, month, and filename."""
    # Extract day if possible
    date_str = _parse_date_from_filename(filename)
    if date_str:
        return f"LC-Gazette-{date_str}"
    # Fallback: use sanitized filename
    safe = re.sub(r'[^a-zA-Z0-9]', '-', filename.replace('.pdf', ''))
    safe = re.sub(r'-+', '-', safe).strip('-')
    return f"LC-Gazette-{year}-{month:02d}-{safe}"


class LCGazetteScraper(BaseScraper):
    """Scraper for LC/Gazette -- Saint Lucia Government Gazette."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_month_page(self, year: int, month: int) -> str:
        """Fetch a gazette month listing page."""
        url = f"{BASE_URL}/gazettes/{year}/{month}"
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return ""
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return ""

    def _extract_pdf_links(self, html: str, year: int, month: int) -> List[Dict]:
        """Extract gazette PDF links from a month page."""
        # Find all PDF links that are gazette files
        pattern = r'href=["\']([^"\']*\.pdf)["\']'
        all_links = re.findall(pattern, html, re.IGNORECASE)

        gazette_links = []
        for link in all_links:
            # Only include gazette files, not Constitution or Staff Orders
            if '/gazettes/' not in link.lower():
                continue
            # Make absolute URL
            if link.startswith('http'):
                full_url = link
            else:
                full_url = urljoin(BASE_URL, link)

            filename = full_url.rsplit('/', 1)[-1]
            # URL-decode for display but keep original for download
            gazette_links.append({
                "url": full_url,
                "filename": requests.utils.unquote(filename),
            })

        return gazette_links

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file."""
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=120)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {url}")
                return None
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"PDF download error: {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all gazette issues with PDF URLs."""
        current_year = datetime.now().year
        for year in range(START_YEAR, current_year + 1):
            for month in range(1, 13):
                # Skip future months
                now = datetime.now()
                if year == now.year and month > now.month:
                    break

                html = self._get_month_page(year, month)
                if not html:
                    continue

                pdf_links = self._extract_pdf_links(html, year, month)
                if pdf_links:
                    logger.info(f"  {year}/{month:02d}: {len(pdf_links)} gazette(s)")

                for item in pdf_links:
                    yield {
                        "url": item["url"],
                        "filename": item["filename"],
                        "year": year,
                        "month": month,
                    }

            logger.info(f"Year {year} complete")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield gazette issues published since the given date."""
        current_year = datetime.now().year
        since_year = since.year
        since_month = since.month

        for year in range(since_year, current_year + 1):
            start_month = since_month if year == since_year else 1
            for month in range(start_month, 13):
                now = datetime.now()
                if year == now.year and month > now.month:
                    break

                html = self._get_month_page(year, month)
                if not html:
                    continue

                pdf_links = self._extract_pdf_links(html, year, month)
                for item in pdf_links:
                    yield {
                        "url": item["url"],
                        "filename": item["filename"],
                        "year": year,
                        "month": month,
                    }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download PDF, extract text, and normalize into standard schema."""
        url = raw["url"]
        filename = raw["filename"]
        year = raw["year"]
        month = raw["month"]

        # Download PDF
        pdf_bytes = self._download_pdf(url)
        if not pdf_bytes:
            return None

        # Extract text
        text, page_count = _extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text from {filename}: {len(text)} chars")
            return None

        # Parse date
        date_str = _parse_date_from_filename(filename)
        if not date_str:
            date_str = f"{year:04d}-{month:02d}-01"

        gazette_id = _make_gazette_id(year, month, filename)

        # Build title from filename
        title = filename.replace('.pdf', '').strip()
        title = re.sub(r'\s+', ' ', title)

        return {
            "_id": gazette_id,
            "_source": "LC/Gazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": url,
            "gazette_id": gazette_id,
            "year": year,
            "month": month,
            "page_count": page_count,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = LCGazetteScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        html = scraper._get_month_page(2026, 1)
        links = scraper._extract_pdf_links(html, 2026, 1)
        if not links:
            print("Connection FAILED - no gazette links found")
            sys.exit(1)
        print(f"Connection OK. Gazette PDFs found for 2026/01: {len(links)}")
        for link in links:
            print(f"  {link['filename']}")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
