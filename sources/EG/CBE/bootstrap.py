#!/usr/bin/env python3
"""
EG/CBE -- Central Bank of Egypt Circulars

Fetches regulatory circulars from the Central Bank of Egypt.

Strategy:
  - JSON API at /api/listing/circulars?pageNo=N (10 per page, ~388 total)
  - Each record has title, date, categories, and PDF URL
  - Download PDFs and extract text via pdfplumber
  - Content is primarily Arabic; English titles from API

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import json
import sys
import time
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EG.CBE")

BASE_URL = "https://www.cbe.org.eg"
API_ENDPOINT = f"{BASE_URL}/api/listing/circulars"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


class CBEScraper(BaseScraper):
    """
    Scraper for EG/CBE -- Central Bank of Egypt Circulars.
    Country: EG
    URL: https://www.cbe.org.eg/en/laws-regulations/regulations/circulars

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_page(self, page_no: int) -> dict:
        """Fetch one page from the circulars API."""
        url = f"{API_ENDPOINT}?pageNo={page_no}"
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        return r.json()

    def _download_pdf_text(self, pdf_path: str) -> Optional[str]:
        """Download a PDF and extract text via pdfplumber."""
        url = f"{BASE_URL}{pdf_path}"
        try:
            r = self.session.get(url, timeout=120, headers={
                **HEADERS,
                "Accept": "application/pdf,*/*",
            })
            r.raise_for_status()

            # Verify it's actually a PDF
            if not r.content[:5].startswith(b"%PDF"):
                logger.warning(f"Not a PDF: {pdf_path[:80]}")
                return None

            with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
                f.write(r.content)
                f.flush()
                pdf = pdfplumber.open(f.name)
                pages = []
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    if text.strip():
                        pages.append(text)
                pdf.close()
                return "\n\n".join(pages) if pages else None

        except requests.RequestException as e:
            logger.warning(f"Failed to download PDF {pdf_path[:60]}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Failed to extract PDF text {pdf_path[:60]}: {e}")
            return None

    def _parse_date(self, custom_date: str) -> Optional[str]:
        """Parse ISO date from customDate field."""
        if not custom_date:
            return None
        try:
            dt = datetime.fromisoformat(custom_date.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw circular record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        item_id = raw.get("itemId", "").strip("{}")
        title = raw.get("title", "").strip()
        date = self._parse_date(raw.get("customDate"))
        pdf_url = raw.get("url", "")
        categories = raw.get("categories", [])
        category = categories[0]["value"] if categories else ""

        return {
            "_id": item_id or title[:80],
            "_source": "EG/CBE",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}{pdf_url}" if pdf_url else BASE_URL,
            "category": category,
            "institution": "Central Bank of Egypt",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all circulars from the API."""
        # First call to get total count
        first_page = self._fetch_page(1)
        total = first_page.get("totalResultsCount", 0)
        logger.info(f"Total circulars: {total}")

        page_no = 1
        yielded = 0

        while True:
            data = first_page if page_no == 1 else self._fetch_page(page_no)
            results = data.get("results", [])
            if not results:
                break

            for item in results:
                pdf_path = item.get("url", "")
                if not pdf_path:
                    logger.warning(f"No PDF URL for: {item.get('title', 'unknown')[:60]}")
                    continue

                text = self._download_pdf_text(pdf_path)
                if not text:
                    logger.warning(f"No text extracted: {item.get('title', 'unknown')[:60]}")
                    continue

                item["text"] = text
                yield item
                yielded += 1
                time.sleep(1)

            page_no += 1
            time.sleep(1)

        logger.info(f"Finished: {yielded}/{total} circulars yielded with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent circulars (page 1 only for updates)."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        data = self._fetch_page(1)
        for item in data.get("results", []):
            date = self._parse_date(item.get("customDate"))
            if date and date >= since_str:
                pdf_path = item.get("url", "")
                if pdf_path:
                    text = self._download_pdf_text(pdf_path)
                    if text:
                        item["text"] = text
                        yield item
                        time.sleep(1)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="EG/CBE data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = CBEScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            data = scraper._fetch_page(1)
            total = data.get("totalResultsCount", 0)
            results = data.get("results", [])
            logger.info(f"OK: API returned {total} total circulars, {len(results)} on page 1")
            if results:
                r = results[0]
                logger.info(f"First: {r.get('title', 'unknown')[:80]}")
                logger.info(f"  Date: {r.get('date')}, PDF: {r.get('url', '')[:80]}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
