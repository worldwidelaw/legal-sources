#!/usr/bin/env python3
"""
INTL/CAS-Sports -- Court of Arbitration for Sport

Fetches arbitration awards from the CAS/TAS jurisprudence database.

Strategy:
  - Paginate REST API at jurisprudence.tas-cas.org
  - Download PDFs from /pdf/{fileName}
  - Extract full text using PyMuPDF
  - ~2,581 awards (1986-2024)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests
import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.CAS-Sports")

API_BASE = "https://jurisprudence.tas-cas.org"
SEARCH_URL = f"{API_BASE}/CaseLawDocument/SearchCaseLawDocument"
DETAIL_URL = f"{API_BASE}/CaseLawDocument"
PDF_URL = f"{API_BASE}/pdf"


class CASSportsScraper(BaseScraper):
    """
    Scraper for INTL/CAS-Sports -- Court of Arbitration for Sport.
    Country: INTL
    URL: https://jurisprudence.tas-cas.org

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _search_page(self, page: int = 1, page_size: int = 100) -> dict:
        """Fetch a page of search results from the CAS API."""
        params = {
            "pageNumber": page,
            "pageSize": page_size,
            "orderByColumn": "decisionDate",
            "orderByDirection": "desc",
        }
        logger.info(f"Fetching search page {page} (size={page_size})")
        r = self.session.get(SEARCH_URL, params=params, timeout=60)
        r.raise_for_status()
        return r.json()

    def _get_detail(self, guid: str) -> dict:
        """Fetch full detail for a single case."""
        url = f"{DETAIL_URL}/{guid}"
        r = self.session.get(url, timeout=60)
        r.raise_for_status()
        return r.json()

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/CAS-Sports",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _download_pdf_text(self, file_name: str) -> str:
        """Download PDF and extract text."""
        if not file_name:
            return ""
        url = f"{PDF_URL}/{file_name}"
        try:
            r = self.session.get(url, timeout=120)
            if r.status_code != 200:
                logger.warning(f"PDF download failed ({r.status_code}): {url}")
                return ""
            ct = r.headers.get("Content-Type", "")
            if "pdf" not in ct and "octet" not in ct:
                logger.warning(f"Not a PDF response ({ct}): {url}")
                return ""
            return self._extract_text_from_pdf(r.content)
        except Exception as e:
            logger.warning(f"PDF download error for {url}: {e}")
            return ""

    def _extract_file_name(self, item: dict) -> str:
        """Extract PDF filename from an API item."""
        # The API provides fileName directly
        fn = item.get("fileName", "")
        if fn:
            return fn
        # Fallback: try to derive from title (e.g., "CAS 2023/A/10168" -> "10168.pdf")
        title = item.get("title", "")
        m = re.search(r'/(\d+)(?:\s|$)', title)
        if m:
            return f"{m.group(1)}.pdf"
        return ""

    def _parse_date(self, date_str: str) -> str:
        """Parse API date string to ISO 8601."""
        if not date_str:
            return None
        try:
            # API returns dates like "2024-04-02T00:00:00"
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw API item into standard schema."""
        case_number = raw.get("title", "").strip()
        return {
            "_id": f"CAS/{raw.get('id', '')}",
            "_source": "INTL/CAS-Sports",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": case_number,
            "text": raw.get("text", ""),
            "date": raw.get("decision_date_parsed"),
            "url": f"{API_BASE}/CaseLawDocument/ViewCaseLawDocument/{raw.get('id', '')}",
            "case_number": case_number,
            "court": "Court of Arbitration for Sport",
            "sport": raw.get("sport", ""),
            "matter": raw.get("matter", ""),
            "procedure": raw.get("procedure", ""),
            "outcome": raw.get("outcome", ""),
            "language": raw.get("language", ""),
            "category": raw.get("category", ""),
        }

    def _process_item(self, item: dict) -> dict:
        """Enrich an API item with full text from PDF."""
        file_name = self._extract_file_name(item)
        text = self._download_pdf_text(file_name) if file_name else ""

        return {
            "id": item.get("guid", ""),
            "title": item.get("title", ""),
            "text": text,
            "decision_date_parsed": self._parse_date(item.get("decisionDate", "")),
            "sport": item.get("sportEn", ""),
            "matter": item.get("matterAbrv", ""),
            "procedure": item.get("procedure", ""),
            "outcome": item.get("outcome", ""),
            "language": item.get("lang", ""),
            "category": item.get("categoryAbrv", ""),
            "appellants": item.get("appellants", ""),
            "respondents": item.get("respondents", ""),
            "fileName": item.get("fileName", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all awards with full text."""
        page = 1
        total = None
        idx = 0
        while True:
            data = self._search_page(page)
            if total is None:
                total = data.get("totalCount", 0)
                logger.info(f"Total awards: {total}")

            items = data.get("items", [])
            if not items:
                break

            for item in items:
                idx += 1
                title = item.get("title", "?")[:60]
                logger.info(f"[{idx}/{total}] Processing: {title}")

                enriched = self._process_item(item)
                if not enriched["text"]:
                    logger.warning(f"No text for: {title}")
                    continue
                yield enriched
                time.sleep(1.5)

            if not data.get("hasNext", False):
                break
            page += 1
            time.sleep(1.0)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield awards newer than `since` (ISO date string)."""
        since_date = datetime.fromisoformat(since).date()
        page = 1
        while page <= 5:
            data = self._search_page(page)
            items = data.get("items", [])
            if not items:
                break

            found_old = False
            for item in items:
                date_str = self._parse_date(item.get("decisionDate", ""))
                if date_str:
                    item_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                    if item_date < since_date:
                        found_old = True
                        continue

                enriched = self._process_item(item)
                if enriched["text"]:
                    yield enriched
                time.sleep(1.5)

            if found_old or not data.get("hasNext", False):
                break
            page += 1
            time.sleep(1.0)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="INTL/CAS-Sports -- Court of Arbitration for Sport"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = CASSportsScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            data = scraper._search_page(1, page_size=5)
            total = data.get("totalCount", 0)
            items = data.get("items", [])
            logger.info(f"OK: API returned {total} total awards, {len(items)} on first page")
            if items:
                item = items[0]
                logger.info(f"First: {item.get('title', '?')}")
                fn = scraper._extract_file_name(item)
                if fn:
                    logger.info(f"Testing PDF download: {fn}")
                    text = scraper._download_pdf_text(fn)
                    if text:
                        logger.info(f"PDF text extracted: {len(text)} chars")
                        logger.info(f"Preview: {text[:200]}")
                        logger.info("Connectivity test passed!")
                    else:
                        logger.error("Failed to extract PDF text")
                        sys.exit(1)
                else:
                    logger.error("No fileName found for first item")
                    sys.exit(1)
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
