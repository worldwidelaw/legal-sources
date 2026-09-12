#!/usr/bin/env python3
"""
GY/MOLA -- Laws of Guyana (Ministry of Legal Affairs)

Fetches consolidated Laws of Guyana from mola.gov.gy.

Strategy:
  - AJAX endpoint /ajax-search-law returns paginated HTML with chapter listings
  - Each entry links to a PDF containing the full text of one act
  - PDFs are text-based, extracted with pdfplumber

Data:
  - ~460 acts organized by volume and chapter
  - Full text in English
  - License: Public Domain (Government Works)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import io
import json
import logging
import time
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GY.MOLA")

BASE_URL = "https://mola.gov.gy"
AJAX_ENDPOINT = "/ajax-search-law"
MAX_PAGES = 60  # Safety cap


class MOLAScraper(BaseScraper):
    """
    Scraper for GY/MOLA -- Laws of Guyana.
    Country: GY
    URL: https://mola.gov.gy/laws-of-guyana

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=120,
        )
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })

    def _fetch_page(self, page: int, search: str = "") -> str:
        """Fetch one page of law entries from the AJAX endpoint."""
        self.rate_limiter.wait()
        params = {"page": page, "search": search}
        resp = self.client.get(AJAX_ENDPOINT, params=params)
        resp.raise_for_status()
        return resp.text

    def _parse_entries(self, html: str) -> List[Dict[str, str]]:
        """Parse law entries from AJAX HTML response."""
        entries = []
        # Pattern: <h6 class="card-title" >Chapter XXX:XX - Title</h6>
        # followed by volume info and PDF link
        pattern = re.compile(
            r'<a href="([^"]+\.pdf)"[^>]*><h6 class="card-title"\s*>(.*?)</h6></a>\s*'
            r'<p>(.*?)<a href="[^"]*"',
            re.DOTALL
        )
        for match in pattern.finditer(html):
            pdf_url = match.group(1).strip()
            raw_title = match.group(2).strip()
            volume_info = match.group(3).strip()

            # Parse chapter number and title
            # Handles both "Chapter 001:01 - Title" and "Chapter 001:10 Title"
            chapter_match = re.match(r'Chapter\s+(\d+:\d+)\s*[-–—]?\s*(.*)', raw_title)
            if chapter_match:
                chapter = chapter_match.group(1)
                title = chapter_match.group(2).strip()
                if not title:
                    title = raw_title
            else:
                chapter = raw_title
                title = raw_title

            # Clean volume info
            volume = re.sub(r'<[^>]+>', '', volume_info).strip()

            entries.append({
                "chapter": chapter,
                "title": title,
                "volume": volume,
                "pdf_url": pdf_url,
            })

        return entries

    def _get_all_entries(self) -> Generator[Dict[str, str], None, None]:
        """Paginate through all AJAX pages and yield entries."""
        page = 1
        while page <= MAX_PAGES:
            logger.info(f"Fetching page {page}...")
            try:
                html = self._fetch_page(page)
            except Exception as e:
                logger.warning(f"Failed to fetch page {page}: {e}")
                break

            entries = self._parse_entries(html)
            if not entries:
                logger.info(f"No entries on page {page}, stopping pagination")
                break

            for entry in entries:
                yield entry

            page += 1

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text using pdfplumber."""
        try:
            self.rate_limiter.wait()
            logger.info(f"Downloading PDF: {pdf_url[:80]}...")

            resp = self.session.get(pdf_url, timeout=180, stream=True)
            resp.raise_for_status()

            # Read into memory (check size first)
            content = resp.content
            size_mb = len(content) / (1024 * 1024)
            if size_mb > 100:
                logger.warning(f"PDF too large ({size_mb:.1f} MB), skipping: {pdf_url}")
                return None

            logger.info(f"PDF size: {size_mb:.1f} MB, extracting text...")

            text_parts = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                total_pages = len(pdf.pages)
                for i, page in enumerate(pdf.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                    except Exception as e:
                        logger.debug(f"Failed to extract page {i+1}/{total_pages}: {e}")
                        continue

            full_text = "\n\n".join(text_parts)
            logger.info(f"Extracted {len(full_text)} chars from {total_pages} pages")
            return full_text if full_text.strip() else None

        except Exception as e:
            logger.error(f"Failed to extract PDF text: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all law documents with full text."""
        for entry in self._get_all_entries():
            text = self._extract_text_from_pdf(entry["pdf_url"])
            if text:
                entry["text"] = text
                yield entry
            else:
                logger.warning(f"No text extracted for {entry['chapter']} - {entry['title']}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental update — re-fetches all (consolidated laws rarely change)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw entry into standard schema."""
        chapter = raw.get("chapter", "unknown")
        title = raw.get("title", "")
        text = raw.get("text", "")

        if not text or len(text.strip()) < 50:
            return None

        return {
            "_id": f"GY/MOLA/{chapter}",
            "_source": "GY/MOLA",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": f"GY-MOLA-{chapter}",
            "chapter": chapter,
            "title": title,
            "text": text,
            "volume": raw.get("volume", ""),
            "url": raw.get("pdf_url", ""),
            "date": None,
            "country": "GY",
            "language": "en",
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="GY/MOLA scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    scraper = MOLAScraper()

    if args.command == "test":
        logger.info("Testing connectivity to mola.gov.gy...")
        try:
            html = scraper._fetch_page(1)
            entries = scraper._parse_entries(html)
            logger.info(f"Connection OK. Found {len(entries)} entries on page 1.")
            for e in entries[:3]:
                logger.info(f"  {e['chapter']}: {e['title']}")
            print("TEST PASSED")
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            print("TEST FAILED")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        sample_size = 15 if sample_mode else 99999
        logger.info(f"Starting bootstrap (sample={sample_mode}, size={sample_size})")
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
