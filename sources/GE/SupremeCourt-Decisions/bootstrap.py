#!/usr/bin/env python3
"""
GE/SupremeCourt-Decisions -- Georgia Supreme Court Data Fetcher

Fetches case law from Georgia's Supreme Court via AJAX endpoints.

Strategy:
  - Bootstrap: Paginates through all cases from /ka/getCases, then fetches
    full text for each case from /fullcase/{id}/{palata}.
  - Update: Fetches recent pages and stops when reaching old cases.
  - Sample: Fetches cases from different pages and palatas.

API: https://www.supremecourt.ge/ka/getCases (HTML listing)
     https://www.supremecourt.ge/fullcase/{id}/{palata} (full text AJAX)
Website: https://www.supremecourt.ge

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py update               # Incremental update
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GE.supremecourt")

BASE_URL = "https://www.supremecourt.ge"

# Palata IDs: 0=administrative, 1=civil, 2=criminal
PALATAS = {
    0: "ადმინისტრაციული (Administrative)",
    1: "სამოქალაქო (Civil)",
    2: "სისხლის სამართლის (Criminal)",
}


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_text:
        return ""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html_text, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\xa0", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_case_listing(html: str) -> list:
    """Parse the HTML listing from getCases into case records."""
    cases = []
    # Each case is in a <div class="cases clearfix"> block
    blocks = re.split(r'<div class="cases clearfix">', html)

    for block in blocks[1:]:  # Skip first (before first case)
        case = {}

        # Case number
        m = re.search(r"საქმის ნომერი:</span>\s*(.+?)\s*</div>", block)
        if m:
            case["case_number"] = m.group(1).strip()

        # Date
        m = re.search(r"თარიღი:</span>\s*(\d{4}-\d{2}-\d{2})", block)
        if m:
            case["date"] = m.group(1)

        # Subject
        m = re.search(r"დავის საგანი:</span>\s*(.*?)</span>", block, re.DOTALL)
        if m:
            case["subject"] = strip_html(m.group(1)).strip()

        # Result
        m = re.search(r"შედეგი:</span>\s*(.*?)</div>", block)
        if m:
            case["result"] = strip_html(m.group(1)).strip()

        # Appeal type
        m = re.search(r"საჩივრის სახე:</span>\s*(.*?)</div>", block, re.DOTALL)
        if m:
            case["appeal_type"] = strip_html(m.group(1)).strip()

        # Extract ID and palata from fullcase link
        m = re.search(r'href="/ka/fullcase/(\d+)/(\d+)"', block)
        if m:
            case["id"] = int(m.group(1))
            case["palata"] = int(m.group(2))

        if case.get("id"):
            cases.append(case)

    return cases


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for GE/SupremeCourt-Decisions -- Georgia Supreme Court.
    Country: GE
    URL: https://www.supremecourt.ge

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _get_cases_page(self, palata: int = 1, page: int = 1) -> str:
        """Fetch a page of case listings."""
        import requests

        url = f"{BASE_URL}/ka/getCases"
        params = {"palata": palata, "page": page}
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        }

        for attempt in range(3):
            try:
                resp = requests.get(
                    url, params=params, headers=headers, timeout=30
                )
                if resp.status_code == 200:
                    return resp.text
                logger.warning(
                    f"getCases returned {resp.status_code} "
                    f"(palata={palata}, page={page}, attempt {attempt + 1})"
                )
            except Exception as e:
                logger.warning(f"getCases error (attempt {attempt + 1}): {e}")
            time.sleep(2 * (attempt + 1))

        return ""

    def _get_full_case(self, case_id: int, palata: int) -> Optional[str]:
        """Fetch full text of a case via AJAX endpoint."""
        import requests

        url = f"{BASE_URL}/fullcase/{case_id}/{palata}"
        params = {"action": "js", "id": case_id, "fulltext": ""}
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        }

        for attempt in range(3):
            try:
                resp = requests.get(
                    url, params=params, headers=headers, timeout=60
                )
                if resp.status_code == 200 and len(resp.text) > 100:
                    return strip_html(resp.text)
                logger.warning(
                    f"fullcase returned {resp.status_code} for id={case_id} "
                    f"(attempt {attempt + 1})"
                )
            except Exception as e:
                logger.warning(
                    f"fullcase error for id={case_id} (attempt {attempt + 1}): {e}"
                )
            time.sleep(2 * (attempt + 1))

        return None

    def normalize(self, raw: dict) -> dict:
        """Transform raw case record into standard schema."""
        case_id = raw.get("id", 0)
        palata = raw.get("palata", 1)
        case_number = raw.get("case_number", f"Case-{case_id}")
        date = raw.get("date")

        text = raw.get("full_text", "")
        if not text:
            text = raw.get("subject", "")

        return {
            "_id": f"GE-SC-{case_id}",
            "_source": "GE/SupremeCourt-Decisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"საქმე {case_number}" if case_number else f"Case GE-SC-{case_id}",
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/ka/fullcase/{case_id}/{palata}",
            "case_number": case_number,
            "palata": PALATAS.get(palata, str(palata)),
            "subject": raw.get("subject", ""),
            "result": raw.get("result", ""),
            "appeal_type": raw.get("appeal_type", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all cases with full text.

        Yields raw case dicts (not normalized). BaseScraper.bootstrap()
        calls self.normalize() on each yielded record.
        """
        total_fetched = 0

        for palata in [0, 1, 2]:
            logger.info(f"Fetching palata {palata}: {PALATAS[palata]}")
            page = 1

            while True:
                html = self._get_cases_page(palata=palata, page=page)
                if not html or "cases clearfix" not in html:
                    break

                cases = parse_case_listing(html)
                if not cases:
                    break

                for case in cases:
                    full_text = self._get_full_case(case["id"], case["palata"])
                    if full_text:
                        case["full_text"] = full_text

                    yield case
                    total_fetched += 1
                    time.sleep(1)

                logger.info(
                    f"Palata {palata}, page {page}: {len(cases)} cases "
                    f"(total: {total_fetched})"
                )
                page += 1
                time.sleep(1)

        logger.info(f"Total fetched: {total_fetched} decisions")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch recently added cases. Yields raw case dicts."""
        since_str = str(since)[:10] if since else "2026-01-01"
        for palata in [0, 1, 2]:
            page = 1
            max_pages = 10

            while page <= max_pages:
                html = self._get_cases_page(palata=palata, page=page)
                if not html:
                    break

                cases = parse_case_listing(html)
                if not cases:
                    break

                found_old = False
                for case in cases:
                    if case.get("date") and case["date"] < since_str:
                        found_old = True
                        continue

                    full_text = self._get_full_case(case["id"], case["palata"])
                    if full_text:
                        case["full_text"] = full_text

                    yield case
                    time.sleep(1)

                if found_old:
                    break
                page += 1
                time.sleep(1)

    def test_api(self):
        """Quick API connectivity test."""
        print("Testing GE/SupremeCourt-Decisions API...")

        for palata in [0, 1, 2]:
            html = self._get_cases_page(palata=palata, page=1)
            if html:
                # Extract total count
                m = re.search(r"სულ მოიძებნა (\d+)", html)
                total = m.group(1) if m else "?"
                cases = parse_case_listing(html)
                print(f"  Palata {palata} ({PALATAS[palata]}): {total} total, "
                      f"{len(cases)} on page 1")

                if cases:
                    case = cases[0]
                    print(f"    First: {case.get('case_number')} ({case.get('date')})")

                    # Test full text
                    full_text = self._get_full_case(case["id"], case["palata"])
                    if full_text:
                        print(f"    Full text: {len(full_text)} chars")
                        print(f"    Preview: {full_text[:150]}...")
                    else:
                        print("    Full text: FAILED")
            else:
                print(f"  Palata {palata}: FAILED to fetch")

            time.sleep(1)

        print("\nAPI test complete.")


def main():
    scraper = SupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
            count = stats.get("sample_records_saved", 0)
        else:
            stats = scraper.bootstrap()
            count = stats.get("records_new", 0)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")
        sys.exit(0 if count >= 10 else (1 if not sample else 0))

    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2, default=str)}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
