#!/usr/bin/env python3
"""
World Wide Law - UK Recognised Professional Bodies (Insolvency) Scraper

Fetches Insolvency Service guidance and regulatory documents via GOV.UK
Content API. Covers insolvency practitioner regulation, debt relief,
bankruptcy guidance, and RPB disciplinary matters.

No authentication required. ~260+ documents with full text.

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
logger = logging.getLogger("UK/RPB")

BASE_URL = "https://www.gov.uk"
ORG_SLUG = "insolvency-service"

# Document types with substantial full text content
DOC_TYPES = [
    "detailed_guide",
    "guidance",
    "manual_section",
]


def strip_html(html: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class UKRPBScraper(BaseScraper):
    """
    Scraper for UK Insolvency Service documents via GOV.UK Content API.

    Strategy:
    - Search API with organisation + document type filters
    - Content API fetches full metadata + body
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )

    def _search_documents(self, doc_type: str, start: int = 0, count: int = 50) -> dict:
        """Search GOV.UK for Insolvency Service documents of a given type."""
        params = (
            f"?filter_organisations={ORG_SLUG}"
            f"&filter_content_store_document_type={doc_type}"
            f"&count={count}&start={start}"
            f"&fields=title,link,public_timestamp,description,content_store_document_type"
        )
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/api/search.json{params}")
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Search returned {resp.status_code} for {doc_type} start={start}")
            return {}
        except Exception as e:
            logger.error(f"Search failed for {doc_type}: {e}")
            return {}

    def _fetch_content(self, path: str) -> Optional[dict]:
        """Fetch a document from the GOV.UK Content API."""
        if not path.startswith("/"):
            path = "/" + path
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"/api/content{path}")
            if resp.status_code == 200:
                return resp.json()
            logger.debug(f"Content API returned {resp.status_code} for {path}")
            return None
        except Exception as e:
            logger.debug(f"Content API failed for {path}: {e}")
            return None

    def _extract_full_text(self, content_data: dict) -> str:
        """Extract full text from a GOV.UK content response."""
        details = content_data.get("details", {})

        body_html = details.get("body", "")
        if body_html and len(body_html) > 100:
            return strip_html(body_html)

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Insolvency Service documents with full text."""
        for doc_type in DOC_TYPES:
            first_result = self._search_documents(doc_type, start=0)
            total = first_result.get("total", 0)
            logger.info(f"Document type '{doc_type}': {total} results")

            if total == 0:
                continue

            start = 0
            while start < total:
                if start == 0:
                    result = first_result
                else:
                    result = self._search_documents(doc_type, start=start)

                items = result.get("results", [])
                if not items:
                    break

                for item in items:
                    link = item.get("link", "")
                    if not link:
                        continue

                    content_data = self._fetch_content(link)
                    if not content_data:
                        continue

                    full_text = self._extract_full_text(content_data)

                    yield {
                        "link": link,
                        "title": content_data.get("title", item.get("title", "")),
                        "description": content_data.get("description", item.get("description", "")),
                        "public_timestamp": (
                            content_data.get("public_updated_at")
                            or item.get("public_timestamp", "")
                        ),
                        "doc_type": doc_type,
                        "schema_name": content_data.get("schema_name", ""),
                        "text": full_text,
                        "base_path": content_data.get("base_path", link),
                    }

                start += len(items)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents updated since the given datetime."""
        for raw in self.fetch_all():
            ts = raw.get("public_timestamp", "")
            if ts:
                try:
                    doc_dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if doc_dt >= since:
                        yield raw
                except (ValueError, TypeError):
                    yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw GOV.UK content into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            logger.debug(f"Skipping {raw.get('link', '?')}: no/short text ({len(text)} chars)")
            return None

        link = raw.get("link", "")
        doc_id = link.strip("/").replace("/", "_") if link else ""
        if not doc_id:
            return None

        date_str = raw.get("public_timestamp", "")
        date_val = None
        if date_str:
            try:
                date_val = datetime.fromisoformat(date_str.replace("Z", "+00:00")).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_val = None

        return {
            "_id": doc_id,
            "_source": "UK/RPB",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": date_val,
            "url": f"{BASE_URL}{link}" if link else "",
            "description": raw.get("description", ""),
            "doc_type": raw.get("doc_type", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKRPBScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
