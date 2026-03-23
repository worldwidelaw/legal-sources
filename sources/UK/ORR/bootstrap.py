#!/usr/bin/env python3
"""
World Wide Law - UK Office of Rail and Road (ORR) Scraper

Fetches ORR regulatory documents via:
  - GOV.UK Search API (paginated discovery of all ORR publications)
  - GOV.UK Content API (full text for each document)

Coverage: ~736 documents (regulatory decisions, guidance, statistics, etc.)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import re
import sys
import time
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
logger = logging.getLogger("UK/ORR")

SEARCH_URL = "https://www.gov.uk/api/search.json"
CONTENT_URL = "https://www.gov.uk/api/content"
ORG_FILTER = "office-of-rail-and-road"
PAGE_SIZE = 50


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


class UKORRScraper(BaseScraper):
    """
    Scraper for UK Office of Rail and Road publications on GOV.UK.

    Strategy:
    - Use GOV.UK Search API to list all ORR documents (paginated)
    - For each document, fetch full text via GOV.UK Content API
    - Extract body text from details.body (HTML -> plain text)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            headers={"Accept": "application/json", "User-Agent": "WorldWideLaw/1.0"},
            timeout=30,
        )

    def _search_page(self, start: int = 0) -> dict:
        """Fetch one page of ORR documents from GOV.UK Search API."""
        params = {
            "filter_organisations": ORG_FILTER,
            "count": PAGE_SIZE,
            "start": start,
            "fields": "title,link,content_store_document_type,public_timestamp,description",
            "order": "-public_timestamp",
        }
        resp = self.http.get(SEARCH_URL, params=params)
        resp.raise_for_status()
        return resp.json()

    def _fetch_content(self, link: str) -> Optional[dict]:
        """Fetch full document content from GOV.UK Content API."""
        url = f"{CONTENT_URL}{link}"
        try:
            resp = self.http.get(url)
            if resp.status_code in (404, 410):
                logger.debug(f"Content API {resp.status_code} for {link}")
                return None
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch content for {link}: {e}")
            return None

    def _extract_body_text(self, content: dict) -> str:
        """Extract full text from GOV.UK Content API response."""
        details = content.get("details", {})
        parts = []

        body = details.get("body")
        if body:
            parts.append(strip_html(body))

        for part in details.get("parts", []):
            part_body = part.get("body", "")
            if part_body:
                title = part.get("title", "")
                if title:
                    parts.append(f"\n## {title}\n")
                parts.append(strip_html(part_body))

        for doc in details.get("documents", []):
            if isinstance(doc, str):
                parts.append(strip_html(doc))

        return "\n\n".join(parts).strip()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ORR documents from GOV.UK."""
        start = 0
        total = None
        fetched = 0

        while True:
            data = self._search_page(start)
            if total is None:
                total = data.get("total", 0)
                logger.info(f"Total ORR documents on GOV.UK: {total}")

            results = data.get("results", [])
            if not results:
                break

            for item in results:
                link = item.get("link", "")
                if not link or link.startswith("http"):
                    continue

                doc_type = item.get("content_store_document_type", "")
                if doc_type in ("finder", "finder_email_signup", "organisation"):
                    continue

                time.sleep(1)
                content = self._fetch_content(link)
                if content is None:
                    continue

                body_text = self._extract_body_text(content)
                if not body_text or len(body_text) < 50:
                    desc = item.get("description", "")
                    if desc and len(desc) > len(body_text or ""):
                        body_text = desc

                yield {
                    "link": link,
                    "title": content.get("title", item.get("title", "")),
                    "description": content.get("description", item.get("description", "")),
                    "body_text": body_text,
                    "document_type": content.get("document_type", doc_type),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "first_published_at": content.get("first_published_at", ""),
                    "content_id": content.get("content_id", ""),
                }
                fetched += 1
                if fetched % 100 == 0:
                    logger.info(f"Fetched {fetched}/{total} documents")

            start += PAGE_SIZE
            if start >= (total or 0):
                break
            time.sleep(1)

        logger.info(f"Finished: fetched {fetched} documents total")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents updated since the given date."""
        since_str = since.isoformat()
        start = 0

        while True:
            data = self._search_page(start)
            results = data.get("results", [])
            if not results:
                break

            found_old = False
            for item in results:
                pub_date = item.get("public_timestamp", "")
                if pub_date and pub_date < since_str:
                    found_old = True
                    continue

                link = item.get("link", "")
                if not link or link.startswith("http"):
                    continue

                doc_type = item.get("content_store_document_type", "")
                if doc_type in ("finder", "finder_email_signup", "organisation"):
                    continue

                time.sleep(1)
                content = self._fetch_content(link)
                if content is None:
                    continue

                body_text = self._extract_body_text(content)
                yield {
                    "link": link,
                    "title": content.get("title", item.get("title", "")),
                    "description": content.get("description", ""),
                    "body_text": body_text,
                    "document_type": content.get("document_type", doc_type),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "first_published_at": content.get("first_published_at", ""),
                    "content_id": content.get("content_id", ""),
                }

            if found_old:
                break
            start += PAGE_SIZE
            time.sleep(1)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw GOV.UK document into standard schema."""
        link = raw.get("link", "")
        body_text = raw.get("body_text", "")

        if not body_text or len(body_text) < 20:
            return None

        date_str = raw.get("first_published_at") or raw.get("public_updated_at") or ""
        date_val = None
        if date_str:
            try:
                date_val = datetime.fromisoformat(date_str.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_val = date_str[:10] if len(date_str) >= 10 else None

        return {
            "_id": raw.get("content_id") or link,
            "_source": "UK/ORR",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": body_text,
            "description": raw.get("description", ""),
            "date": date_val,
            "url": f"https://www.gov.uk{link}",
            "document_type": raw.get("document_type", ""),
        }


if __name__ == "__main__":
    scraper = UKORRScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(f"\nBootstrap complete: {stats}")
    elif cmd == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.update(since)
        print(f"\nUpdate complete: {stats}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
