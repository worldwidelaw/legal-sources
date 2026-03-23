#!/usr/bin/env python3
"""
Legal Data Hunter - UK Food Standards Agency Scraper

Fetches FSA publications via the internal search API:
  - GET /search-api?keywords=&page=N (paginated, 10 items/page, full text in body.#markup)

Coverage: ~3,500 documents including guidance, research, consultations, and alerts.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/FSA")


def html_to_text(html: str) -> str:
    """Strip HTML tags and return clean text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class UKFSAScraper(BaseScraper):
    """
    Scraper for UK Food Standards Agency.

    Strategy:
    - Use /search-api to paginate through all documents
    - Full text is in body.#markup field (HTML)
    - 10 items per page, sorted by updated date
    """

    BASE_URL = "https://www.food.gov.uk"
    SEARCH_URL = "/search-api"
    PAGE_SIZE = 10  # Fixed by the API

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )

    def _search_page(self, page: int = 1, keywords: str = "") -> dict:
        """Fetch a page of search results. Returns the #data dict."""
        params = {
            "keywords": keywords,
            "page": page,
            "sort": "updated",
        }
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.SEARCH_URL, params=params)
            if resp.status_code == 200:
                data = resp.json()
                # Response structure: {#data: {items: [...], total: N, ...}}
                return data.get("#data", {})
            logger.warning(f"Search API returned {resp.status_code} for page={page}")
            return {}
        except Exception as e:
            logger.error(f"Search API failed for page={page}: {e}")
            return {}

    def _extract_item(self, item: dict) -> Optional[dict]:
        """Extract document data from a search API result item."""
        # Get text from body markup
        body_markup = ""
        body = item.get("body", {})
        if isinstance(body, dict):
            body_markup = body.get("#markup", "")
        elif isinstance(body, str):
            body_markup = body

        text = html_to_text(body_markup)

        # Also include intro if available
        intro_markup = ""
        intro = item.get("intro", {})
        if isinstance(intro, dict):
            intro_markup = intro.get("#markup", "")
        elif isinstance(intro, str):
            intro_markup = intro

        intro_text = html_to_text(intro_markup)
        if intro_text and text:
            text = intro_text + "\n\n" + text
        elif intro_text:
            text = intro_text

        if not text or len(text) < 50:
            return None

        # Get title
        title = ""
        name = item.get("name", {})
        if isinstance(name, dict):
            title = html_to_text(name.get("#markup", ""))
        elif isinstance(name, str):
            title = name

        doc_id = item.get("id", "")
        url = item.get("url", "")
        if url and not url.startswith("http"):
            url = self.BASE_URL + url

        return {
            "id": str(doc_id),
            "title": title,
            "text": text,
            "content_type": item.get("content_type", ""),
            "filter_type": item.get("filter_type", ""),
            "created": item.get("created", ""),
            "updated": item.get("updated", ""),
            "nation": item.get("nation", []),
            "language": item.get("language", ""),
            "url": url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all FSA documents with full text."""
        page = 1
        first_result = self._search_page(page)

        total_count = first_result.get("total", 0)
        if not total_count:
            total_count = 3500
        total_pages = (total_count + self.PAGE_SIZE - 1) // self.PAGE_SIZE
        logger.info(f"Total items: {total_count}, pages: {total_pages}")

        count = 0
        skipped = 0
        result = first_result

        while True:
            items = result.get("items", [])
            if not items:
                break

            for item in items:
                extracted = self._extract_item(item)
                if extracted:
                    count += 1
                    yield extracted
                else:
                    skipped += 1

            if count % 100 == 0 and count > 0:
                logger.info(f"  {count} documents fetched ({skipped} skipped)")

            page += 1
            if page > total_pages + 5:
                break

            result = self._search_page(page)

        logger.info(f"Total: {count} documents with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents updated since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        page = 1

        while True:
            result = self._search_page(page)
            items = result.get("items", [])
            if not items:
                break

            found_old = False
            for item in items:
                updated = item.get("updated", "")
                if updated and updated[:10] < since_str:
                    found_old = True
                    break

                extracted = self._extract_item(item)
                if extracted:
                    yield extracted

            if found_old:
                break

            page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        date_str = raw.get("created", "") or raw.get("updated", "")
        date_iso = date_str[:10] if date_str else None

        return {
            "_id": f"UK/FSA/{raw.get('id', '')}",
            "_source": "UK/FSA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": raw.get("id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "content_type": raw.get("content_type", ""),
            "filter_type": raw.get("filter_type", ""),
            "date": date_iso,
            "nation": raw.get("nation", []),
            "url": raw.get("url", ""),
            "updated_at": raw.get("updated", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKFSAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
