#!/usr/bin/env python3
"""
JP/eGovLawsAPI -- e-Gov Laws API (Version 2) Data Fetcher

Fetches all Japanese statutes from the official e-Gov Laws API v2.
Covers constitution, acts, cabinet orders, ministerial ordinances, etc.

API docs: https://laws.e-gov.go.jp/api/2/swagger-ui/
Endpoints:
  - GET /api/2/laws?limit=100&offset=N  (paginated law listing)
  - GET /api/2/law_data/{law_id}         (full law text as JSON)

Usage:
  python bootstrap.py bootstrap            # Full initial pull (~9,400 laws)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap (no date filter)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JP.eGovLawsAPI")

BASE_URL = "https://laws.e-gov.go.jp/api/2"
PAGE_SIZE = 100


class EGovLawsScraper(BaseScraper):
    """
    Scraper for JP/eGovLawsAPI -- e-Gov Laws API v2.
    Country: JP
    URL: https://laws.e-gov.go.jp/api/2/
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json",
            },
            timeout=120,
        )

    def _list_laws(self, offset: int = 0, limit: int = PAGE_SIZE) -> dict:
        """Fetch a page of law listings."""
        url = f"{BASE_URL}/laws?limit={limit}&offset={offset}&response_format=json"
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.json()

    def _fetch_law_text(self, law_id: str) -> Optional[dict]:
        """Fetch full law text for a given law_id."""
        url = f"{BASE_URL}/law_data/{law_id}?response_format=json&json_format=full"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"  Failed to fetch law text for {law_id}: {e}")
            return None

    def _extract_text(self, node) -> str:
        """Recursively extract text from the tag/attr/children JSON structure."""
        if isinstance(node, str):
            return node
        if isinstance(node, dict):
            children = node.get("children", [])
            parts = []
            tag = node.get("tag", "")
            for child in children:
                parts.append(self._extract_text(child))
            text = "".join(parts)
            # Add newlines after structural elements
            if tag in (
                "LawTitle", "ArticleTitle", "ChapterTitle", "SectionTitle",
                "SubsectionTitle", "DivisionTitle", "PartTitle", "ParagraphSentence",
                "Sentence", "Column", "TableRow", "Preamble", "EnactStatement",
                "LawNum", "ArticleCaption", "Remark",
            ):
                text = text + "\n"
            return text
        if isinstance(node, list):
            return "".join(self._extract_text(item) for item in node)
        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all laws with full text."""
        offset = 0
        total = None
        fetched_count = 0

        while True:
            page = self._list_laws(offset=offset)
            if total is None:
                total = page.get("total_count", 0)
                logger.info(f"Total laws in API: {total}")

            laws = page.get("laws", [])
            if not laws:
                break

            for law_entry in laws:
                law_info = law_entry.get("law_info", {})
                revision_info = law_entry.get("revision_info", {})
                law_id = law_info.get("law_id", "")

                if not law_id:
                    continue

                # Skip repealed laws
                repeal_status = revision_info.get("repeal_status", "None")
                if repeal_status in ("Repeal", "Expire"):
                    continue

                # Fetch full text
                law_data = self._fetch_law_text(law_id)
                full_text = ""
                if law_data and "law_full_text" in law_data:
                    full_text = self._extract_text(law_data["law_full_text"]).strip()
                    # Collapse multiple blank lines
                    full_text = re.sub(r'\n{3,}', '\n\n', full_text)

                if not full_text:
                    logger.warning(f"  {law_id}: no text extracted, skipping")
                    continue

                yield {
                    "law_id": law_id,
                    "law_info": law_info,
                    "revision_info": revision_info,
                    "full_text": full_text,
                }
                fetched_count += 1

            next_offset = page.get("next_offset")
            if next_offset is None:
                break
            offset = next_offset

            logger.info(f"  Processed offset {offset}/{total}, {fetched_count} laws fetched so far")

        logger.info(f"Done: {fetched_count} laws fetched")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental update available — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw e-Gov data into standard schema."""
        law_info = raw.get("law_info", {})
        revision_info = raw.get("revision_info", {})
        law_id = law_info.get("law_id", "")
        title = revision_info.get("law_title", "")
        law_num = law_info.get("law_num", "")
        law_type = law_info.get("law_type", "")
        promulgation_date = law_info.get("promulgation_date", "")

        return {
            "_id": f"JP-EGOV-{law_id}",
            "_source": "JP/eGovLawsAPI",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": promulgation_date if promulgation_date else None,
            "url": f"https://laws.e-gov.go.jp/law/{law_id}",
            "law_number": law_num,
            "law_type": law_type,
        }


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = EGovLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    elif command == "test-api":
        print("Testing e-Gov Laws API connectivity...")
        try:
            page = scraper._list_laws(offset=0, limit=2)
            total = page.get("total_count", 0)
            laws = page.get("laws", [])
            print(f"Total laws: {total}")
            if laws:
                law_id = laws[0]["law_info"]["law_id"]
                title = laws[0]["revision_info"]["law_title"]
                print(f"First law: {law_id} - {title}")
                data = scraper._fetch_law_text(law_id)
                if data and "law_full_text" in data:
                    text = scraper._extract_text(data["law_full_text"]).strip()
                    print(f"Text length: {len(text)} chars")
                    print(f"First 200 chars: {text[:200]}")
                print("Test passed!")
            else:
                print("Test failed: no laws returned")
        except Exception as e:
            print(f"Test failed: {e}")
            import traceback
            traceback.print_exc()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
