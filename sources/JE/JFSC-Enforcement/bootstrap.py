#!/usr/bin/env python3
"""
JE/JFSC-Enforcement -- Jersey Financial Services Commission Enforcement Actions

Fetches public statements from jerseyfsc.org:
  - Listing via JSON API: POST /umbraco/api/NewsSearch/GetResults
  - Individual pages for full-text HTML extraction
  - ~303 public statements covering enforcement, sanctions, scam warnings (1999-present)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JE.JFSC-Enforcement")

BASE_URL = "https://www.jerseyfsc.org"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Content-Type": "application/json",
}

PAGE_SIZE = 96


def clean_html(html: str) -> str:
    """Strip HTML tags and decode entities, preserving paragraph breaks."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    # Remove MS Office cruft
    text = re.sub(r'\s*class="Mso[^"]*"', "", text)
    text = re.sub(r'\s*style="[^"]*mso-[^"]*"', "", text)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|li|h[1-6]|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


class JFSCScraper(BaseScraper):
    """Scraper for JFSC enforcement actions / public statements."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _list_statements(self, max_pages: int = 10) -> list[dict]:
        """Fetch all public statement listings from the JSON API."""
        statements = []
        url = f"{BASE_URL}/umbraco/api/NewsSearch/GetResults"

        for page_num in range(1, max_pages + 1):
            payload = {
                "Keyword": "",
                "PageSize": PAGE_SIZE,
                "PageNumber": page_num,
                "Refiners": [
                    {"Name": "contenttype", "SelectedValue": "Public statements"}
                ],
            }
            try:
                resp = self.session.post(url, json=payload, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"Failed to fetch listing page {page_num}: {e}")
                break

            results = data.get("Results", [])
            if not results:
                break

            for item in results:
                item_url = item.get("Url", "")
                if not item_url:
                    continue
                # Extract slug from URL
                slug = item_url.rstrip("/").split("/")[-1]

                # Parse date from ISO DateTime field
                date_iso = None
                dt_str = item.get("DateTime", "")
                if dt_str:
                    try:
                        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
                        date_iso = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

                statements.append({
                    "slug": slug,
                    "title": item.get("Title", "").strip(),
                    "date": date_iso,
                    "summary": item.get("Summary", "").strip(),
                    "doc_type": item.get("Type", "").strip(),
                    "url": item_url if item_url.startswith("http") else BASE_URL + item_url,
                })

            total_pages = data.get("Pages", 1)
            logger.info(f"Listing page {page_num}/{total_pages}: {len(results)} items")
            time.sleep(0.5)

            if page_num >= total_pages:
                break

        return statements

    def _fetch_detail(self, statement: dict) -> Optional[dict]:
        """Fetch a public statement detail page and extract full text."""
        url = statement["url"]
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {statement['slug']}: {e}")
            return None

        html = resp.text

        # Extract full text from generic-content div
        content_match = re.search(
            r'<div class="generic-content">(.*?)</div>\s*</article>',
            html, re.DOTALL,
        )
        if not content_match:
            # Try broader match
            content_match = re.search(
                r'<div class="generic-content">(.*?)</div>\s*(?:</div>|<aside)',
                html, re.DOTALL,
            )

        text = ""
        if content_match:
            text = clean_html(content_match.group(1))

        if not text or len(text) < 30:
            logger.debug(f"No/short text for {statement['slug']}")
            return None

        return {
            **statement,
            "text": text,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all JFSC public statements with full text."""
        statements = self._list_statements()
        logger.info(f"Found {len(statements)} public statements")

        for i, stmt in enumerate(statements):
            time.sleep(0.5)
            detail = self._fetch_detail(stmt)
            if not detail:
                continue

            yield detail

            if (i + 1) % 50 == 0:
                logger.info(f"Processed {i + 1}/{len(statements)} statements")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent statements only."""
        statements = self._list_statements(max_pages=2)
        since_str = since.strftime("%Y-%m-%d")
        recent = [s for s in statements if s.get("date") and s["date"] >= since_str]
        logger.info(f"Found {len(recent)} statements since {since_str}")

        for stmt in recent:
            time.sleep(0.5)
            detail = self._fetch_detail(stmt)
            if detail:
                yield detail

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 30:
            return None

        return {
            "_id": raw["slug"],
            "_source": "JE/JFSC-Enforcement",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "title": raw.get("title", raw["slug"]),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "doc_type": raw.get("doc_type"),
            "summary": raw.get("summary"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to jerseyfsc.org API."""
        try:
            url = f"{BASE_URL}/umbraco/api/NewsSearch/GetResults"
            payload = {
                "Keyword": "",
                "PageSize": 1,
                "PageNumber": 1,
                "Refiners": [
                    {"Name": "contenttype", "SelectedValue": "Public statements"}
                ],
            }
            resp = self.session.post(url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("Results"):
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: no results")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = JFSCScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
