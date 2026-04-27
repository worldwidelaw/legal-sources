#!/usr/bin/env python3
"""
NR/RONLAW -- Nauru's Online Legal Database (RONLAW)

Fetches legislation (Acts, subordinate laws) and court decisions from
ronlaw.gov.nr via its Elasticsearch-backed API.

Categories:
  - acts: Primary legislation (~425 docs)
  - laws: Subordinate legislation (~497 docs)
  - court: Court decisions (~885 docs)
  - bills: Legislative bills (~806 docs)
  - gazettes: Government gazettes (~7461 docs)

The API endpoint /api/pdf/search accepts Elasticsearch query DSL.
Full text is stored in the "pages" array field (page_number + page_content).

Note: ronlaw.gov.nr requires TLS 1.3 which Python 3.9/LibreSSL 2.8 cannot
negotiate. We use subprocess curl for HTTP requests as a workaround.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # 12 sample records
  python bootstrap.py test               # Connectivity test
"""

import sys
import json
import logging
import re
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NR.RONLAW")

API_URL = "https://ronlaw.gov.nr/api/pdf/search"
SITE_URL = "https://ronlaw.gov.nr"

# Categories to fetch and their document types
CATEGORIES = {
    "acts": "legislation",
    "laws": "legislation",
    "court": "case_law",
    "bills": "legislation",
    "gazettes": "legislation",
}

# For sample mode, limit to core categories
SAMPLE_CATEGORIES = ["acts", "court", "laws"]

PAGE_SIZE = 50  # Max results per API call


def curl_post_json(url: str, payload: dict, timeout: int = 60) -> dict:
    """POST JSON via curl subprocess (bypasses Python SSL/TLS limitations)."""
    body = json.dumps(payload)
    result = subprocess.run(
        [
            "curl", "-s", "-S", "--fail-with-body",
            "--connect-timeout", "15",
            "--max-time", str(timeout),
            "-X", "POST", url,
            "-H", "Content-Type: application/json",
            "-H", "Accept: application/json",
            "-H", "User-Agent: LegalDataHunter/1.0 (Open Data Research)",
            "-d", body,
        ],
        capture_output=True,
        text=True,
        timeout=timeout + 10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"curl failed (exit {result.returncode}): {result.stderr.strip()}"
        )
    return json.loads(result.stdout)


def clean_page_text(pages: List[Dict]) -> str:
    """Extract and join page text content."""
    if not pages:
        return ""
    sorted_pages = sorted(pages, key=lambda p: p.get("page_number", 0))
    texts = []
    for page in sorted_pages:
        content = page.get("page_content", "").strip()
        if content:
            texts.append(content)
    full_text = "\n\n".join(texts)
    # Clean up excessive whitespace
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
    full_text = re.sub(r"[ \t]+", " ", full_text)
    return full_text.strip()


class NRRONLAWScraper(BaseScraper):
    """Scraper for NR/RONLAW -- Nauru's Online Legal Database."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _search(
        self,
        category: str,
        from_offset: int = 0,
        size: int = PAGE_SIZE,
        include_pages: bool = True,
    ) -> Dict:
        """Execute an Elasticsearch search against the RONLAW API."""
        source_config = {}
        if not include_pages:
            source_config = {"excludes": ["pages"]}

        query = {
            "_source": source_config,
            "query": {
                "bool": {
                    "must": [{"term": {"category": category}}]
                }
            },
            "size": size,
            "from": from_offset,
            "sort": [
                {"year": {"order": "desc"}},
                {"month": {"order": "desc"}},
                {"title.keyword": {"order": "asc"}},
            ],
        }

        self.rate_limiter.wait()
        return curl_post_json(API_URL, query)

    def _get_total_count(self, category: str) -> int:
        """Get total document count for a category."""
        result = self._search(category, from_offset=0, size=0, include_pages=False)
        return result.get("hits", {}).get("total", {}).get("value", 0)

    def _fetch_category(
        self, category: str, limit: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents in a category with full text."""
        total = self._get_total_count(category)
        doc_type = CATEGORIES[category]
        logger.info(f"Category '{category}': {total} documents (type: {doc_type})")

        if limit:
            total = min(total, limit)

        fetched = 0
        offset = 0

        while offset < total:
            batch_size = min(PAGE_SIZE, total - offset)
            try:
                result = self._search(
                    category, from_offset=offset, size=batch_size, include_pages=True
                )
            except Exception as e:
                logger.error(f"Search failed at offset {offset}: {e}")
                break

            hits = result.get("hits", {}).get("hits", [])
            if not hits:
                break

            for hit in hits:
                if limit and fetched >= limit:
                    return

                doc_id = hit.get("_id", "")
                source = hit.get("_source", {})
                pages = source.get("pages", [])
                text = clean_page_text(pages)

                if not text or len(text) < 50:
                    title = source.get("title", "?")[:60]
                    logger.warning(
                        f"  Insufficient text for {doc_id} ({len(text)} chars): {title}"
                    )
                    continue

                raw = {
                    "doc_id": doc_id,
                    "title": source.get("title", "").replace(".pdf", ""),
                    "text": text,
                    "year": source.get("year"),
                    "month": source.get("month"),
                    "category": category,
                    "subcategory": source.get("subcategory", ""),
                    "file_path": source.get("file_path", ""),
                    "doc_type": doc_type,
                }

                yield raw
                fetched += 1

            offset += len(hits)
            logger.info(
                f"  {category}: fetched {fetched} docs (offset {offset}/{total})"
            )

        logger.info(f"Category '{category}': {fetched} documents with full text")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "unknown")
        category = raw.get("category", "")
        year = raw.get("year")
        month = raw.get("month")

        date = None
        if year and month:
            date = f"{year}-{month:02d}-01"
        elif year:
            date = f"{year}-01-01"

        url = f"{SITE_URL}/#/view/{doc_id}" if doc_id else SITE_URL

        return {
            "_id": f"NR/RONLAW/{doc_id}",
            "_source": "NR/RONLAW",
            "_type": raw.get("doc_type", "legislation"),
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": date,
            "url": url,
            "doc_id": doc_id,
            "category": category,
            "subcategory": raw.get("subcategory", ""),
            "year": year,
            "month": month,
        }

    def fetch_all(
        self, sample: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        categories = SAMPLE_CATEGORIES if sample else list(CATEGORIES.keys())
        per_cat_limit = 4 if sample else None
        total_limit = 12 if sample else None
        count = 0

        for category in categories:
            if total_limit and count >= total_limit:
                break

            remaining = (total_limit - count) if total_limit else per_cat_limit
            cat_limit = min(per_cat_limit, remaining) if per_cat_limit and remaining else remaining

            for doc in self._fetch_category(category, limit=cat_limit):
                yield doc
                count += 1
                if total_limit and count >= total_limit:
                    break

        logger.info(f"Total: {count} documents fetched")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (last 50 per category)."""
        for category in CATEGORIES:
            logger.info(f"Checking updates for {category}...")
            for doc in self._fetch_category(category, limit=50):
                date = None
                if doc.get("year") and doc.get("month"):
                    date = f"{doc['year']}-{doc['month']:02d}-01"
                if date and date < since:
                    continue
                yield doc


if __name__ == "__main__":
    scraper = NRRONLAWScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
