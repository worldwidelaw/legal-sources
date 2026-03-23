#!/usr/bin/env python3
"""
Legal Data Hunter - UK Environment Agency Scraper

Fetches regulatory guidance and publications from GOV.UK using:
  - GET /api/search.json?filter_organisations=environment-agency (document discovery)
  - GET /api/content/{path} (full document content via Content Store API)

Coverage: ~11,000+ documents including detailed guides, guidance, statutory
guidance, and notices. Full text from GOV.UK Content Store JSON API.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

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
logger = logging.getLogger("UK/EA")


def html_to_text(html: str) -> str:
    """Strip HTML tags and return clean text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    # Remove script/style
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class UKEAScraper(BaseScraper):
    """
    Scraper for: UK Environment Agency
    Country: UK
    URL: https://www.gov.uk/government/organisations/environment-agency

    Data types: doctrine
    Auth: none

    Strategy:
    - Use GOV.UK Search API to list EA documents by type
    - Use GOV.UK Content Store API to fetch full text
    - For publications with children (html_publication), fetch child pages
    """

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"

    # Document types to fetch (in priority order)
    DOC_TYPES = [
        "detailed_guide",
        "guidance",
        "statutory_guidance",
        "notice",
    ]

    PAGE_SIZE = 100

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

    def _search_documents(self, doc_type: str, start: int = 0) -> dict:
        """Search for EA documents of a given type."""
        params = {
            "filter_organisations": "environment-agency",
            "filter_content_store_document_type": doc_type,
            "count": self.PAGE_SIZE,
            "start": start,
            "order": "-public_timestamp",
            "fields": "title,link,public_timestamp,description,content_id,content_store_document_type",
        }
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.SEARCH_URL, params=params)
            if resp.status_code == 200:
                return resp.json()
            else:
                logger.warning(f"Search API returned {resp.status_code} for {doc_type} start={start}")
                return {}
        except Exception as e:
            logger.error(f"Search API failed: {e}")
            return {}

    def _fetch_content(self, path: str) -> Optional[dict]:
        """Fetch full document content from Content Store API."""
        # Path should start with /
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.CONTENT_URL}{path}"

        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                logger.debug(f"Content not found: {path}")
                return None
            else:
                logger.warning(f"Content API returned {resp.status_code} for {path}")
                return None
        except Exception as e:
            logger.warning(f"Content API failed for {path}: {e}")
            return None

    def _extract_full_text(self, content: dict) -> str:
        """Extract full text from a Content Store document.

        For detailed_guides: text is in details.body
        For publications (guidance/statutory_guidance): text may be in
        details.body (summary) + children pages (full content)
        """
        parts = []

        # Main body
        body = content.get("details", {}).get("body", "")
        if body:
            parts.append(html_to_text(body))

        # Check for children (html_publication sub-pages)
        children = content.get("links", {}).get("children", [])
        if children:
            for child in children:
                child_path = child.get("base_path", "")
                if child_path:
                    child_content = self._fetch_content(child_path)
                    if child_content:
                        child_body = child_content.get("details", {}).get("body", "")
                        if child_body:
                            child_title = child_content.get("title", "")
                            if child_title:
                                parts.append(f"\n## {child_title}\n")
                            parts.append(html_to_text(child_body))

        return "\n\n".join(parts).strip()

    def _list_all_documents(self, doc_type: str) -> Generator[dict, None, None]:
        """Yield all search results for a given document type."""
        start = 0
        result = self._search_documents(doc_type, start)
        total = result.get("total", 0)
        logger.info(f"Document type '{doc_type}': {total} total results")

        while True:
            results = result.get("results", [])
            if not results:
                break

            for item in results:
                yield item

            start += len(results)
            if start >= total:
                break

            result = self._search_documents(doc_type, start)

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all EA documents with full text."""
        for doc_type in self.DOC_TYPES:
            count = 0
            for item in self._list_all_documents(doc_type):
                link = item.get("link", "")
                if not link:
                    continue

                # Fetch full content
                content = self._fetch_content(link)
                if not content:
                    continue

                # Extract full text
                text = self._extract_full_text(content)
                if not text or len(text) < 50:
                    continue

                count += 1
                yield {
                    "content_id": content.get("content_id", ""),
                    "title": content.get("title", ""),
                    "text": text,
                    "description": content.get("description", ""),
                    "document_type": content.get("document_type", doc_type),
                    "schema_name": content.get("schema_name", ""),
                    "first_published_at": content.get("first_published_at", ""),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "link": link,
                }

                if count % 50 == 0:
                    logger.info(f"  [{doc_type}] {count} documents fetched so far")

            logger.info(f"  [{doc_type}] Total: {count} documents with text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents updated since the given date."""
        since_str = since.strftime("%Y-%m-%d")
        for doc_type in self.DOC_TYPES:
            for item in self._list_all_documents(doc_type):
                pub_date = item.get("public_timestamp", "")
                if pub_date and pub_date[:10] < since_str:
                    # Results are sorted newest first, so we can stop
                    break

                link = item.get("link", "")
                if not link:
                    continue

                content = self._fetch_content(link)
                if not content:
                    continue

                text = self._extract_full_text(content)
                if not text or len(text) < 50:
                    continue

                yield {
                    "content_id": content.get("content_id", ""),
                    "title": content.get("title", ""),
                    "text": text,
                    "description": content.get("description", ""),
                    "document_type": content.get("document_type", doc_type),
                    "schema_name": content.get("schema_name", ""),
                    "first_published_at": content.get("first_published_at", ""),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "link": link,
                }

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        # Parse date
        date_str = raw.get("first_published_at", "") or raw.get("public_updated_at", "")
        date_iso = date_str[:10] if date_str else None

        return {
            "_id": f"UK/EA/{raw.get('content_id', '')}",
            "_source": "UK/EA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "document_type": raw.get("document_type", ""),
            "date": date_iso,
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKEAScraper()

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
