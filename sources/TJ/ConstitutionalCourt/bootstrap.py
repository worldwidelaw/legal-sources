#!/usr/bin/env python3
"""
TJ/ConstitutionalCourt -- Tajikistan Constitutional Court Decisions

Fetches decisions, determinations, and legal documents from the Constitutional
Court of the Republic of Tajikistan via its WordPress REST API.

Custom post types:
  - qarorho  (decisions/resolutions): ~29 posts
  - tainotho (determinations): ~63 posts
  - hujatho  (legal documents): ~15 posts

Endpoints:
  - GET /wp-json/wp/v2/{post_type}?per_page=100&page={N}
  - Full text in content.rendered (Tajik/Russian)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TJ.ConstitutionalCourt")

API_BASE = "https://constcourt.tj"
PER_PAGE = 100

POST_TYPES = [
    {"slug": "qarorho", "label": "Decisions", "type": "case_law"},
    {"slug": "tainotho", "label": "Determinations", "type": "case_law"},
    {"slug": "hujatho", "label": "Documents", "type": "legislation"},
]

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(html_str: str) -> str:
    """Remove HTML tags and clean up whitespace."""
    text = TAG_RE.sub(" ", html_str)
    text = html_mod.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


class TJConstitutionalCourtScraper(BaseScraper):
    """Scraper for TJ/ConstitutionalCourt via WordPress REST API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=120,
        )

    def _fetch_page(self, post_type: str, page: int) -> tuple:
        """Fetch a page of posts. Returns (posts, total_pages)."""
        params = {
            "per_page": PER_PAGE,
            "page": page,
            "_fields": "id,title,slug,date,modified,link,content",
        }
        self.rate_limiter.wait()
        resp = self.client.get(f"/wp-json/wp/v2/{post_type}", params=params)
        resp.raise_for_status()
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        return resp.json(), total_pages

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        wp_id = raw.get("id", "")
        post_type = raw.get("_post_type", "qarorho")
        doc_type = raw.get("_doc_type", "case_law")

        title = raw.get("title", {})
        if isinstance(title, dict):
            title = title.get("rendered", "")
        title = html_mod.unescape(str(title)).strip()

        wp_date = raw.get("date", "")
        date = wp_date[:10] if wp_date else None

        content = raw.get("content", {})
        if isinstance(content, dict):
            content = content.get("rendered", "")
        text = strip_html(str(content))

        url = raw.get("link", "")

        return {
            "_id": f"TJ/ConstitutionalCourt/{post_type}/{wp_id}",
            "_source": "TJ/ConstitutionalCourt",
            "_type": doc_type,
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_id": str(wp_id),
            "post_type": post_type,
            "court": "Constitutional Court of Tajikistan",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        for pt in POST_TYPES:
            if limit and count >= limit:
                break

            post_type = pt["slug"]
            doc_type = pt["type"]
            logger.info(f"Fetching {pt['label']} ({post_type})...")

            page = 1
            total_pages = 1

            while page <= total_pages:
                if limit and count >= limit:
                    break

                try:
                    posts, total_pages = self._fetch_page(post_type, page)
                except Exception as e:
                    logger.error(f"Failed to fetch {post_type} page {page}: {e}")
                    break

                logger.info(f"  Page {page}/{total_pages}: {len(posts)} posts")

                for post in posts:
                    if limit and count >= limit:
                        break

                    content = post.get("content", {})
                    if isinstance(content, dict):
                        content_html = content.get("rendered", "")
                    else:
                        content_html = str(content)

                    text = strip_html(content_html)
                    if len(text) < 50:
                        title = post.get("title", {})
                        if isinstance(title, dict):
                            title = title.get("rendered", "")
                        logger.warning(f"  Skipping '{title[:60]}' - text too short ({len(text)} chars)")
                        continue

                    post["_post_type"] = post_type
                    post["_doc_type"] = doc_type
                    yield post
                    count += 1

                    title = post.get("title", {})
                    if isinstance(title, dict):
                        title = title.get("rendered", "")
                    logger.info(f"  [{count}] {html_mod.unescape(str(title))[:70]}")

                page += 1

        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        for pt in POST_TYPES:
            post_type = pt["slug"]
            doc_type = pt["type"]
            page = 1
            total_pages = 1

            while page <= total_pages:
                try:
                    params = {
                        "per_page": PER_PAGE,
                        "page": page,
                        "after": f"{since}T00:00:00",
                        "orderby": "date",
                        "order": "asc",
                        "_fields": "id,title,slug,date,modified,link,content",
                    }
                    self.rate_limiter.wait()
                    resp = self.client.get(f"/wp-json/wp/v2/{post_type}", params=params)
                    resp.raise_for_status()
                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    posts = resp.json()
                except Exception as e:
                    logger.error(f"Failed to fetch updates for {post_type}: {e}")
                    break

                for post in posts:
                    post["_post_type"] = post_type
                    post["_doc_type"] = doc_type
                    yield post

                page += 1


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = TJConstitutionalCourtScraper()

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
