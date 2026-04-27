#!/usr/bin/env python3
"""
VG/CommercialCourt -- BVI Commercial Court Judgments

Fetches Commercial Court judgments from the Eastern Caribbean Supreme Court
WordPress site at judgments.eccourts.org. Uses the WP REST API with
category 40 (Territory of the Virgin Islands) and filters for cases with
"COM" in the case number.

Endpoints:
  - List: GET /wp-json/wp/v2/posts?categories=40&per_page=100&page={N}
  - Each post has full HTML text in content.rendered and ACF metadata

Data:
  - ~960+ Commercial Court judgments
  - Full text in HTML (content.rendered)
  - Structured metadata via ACF fields (case_number, date_new, doc_vew)
  - Language: English

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
from typing import Generator, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VG.CommercialCourt")

API_BASE = "https://judgments.eccourts.org"
WP_API = f"{API_BASE}/wp-json/wp/v2/posts"
BVI_CATEGORY = 40
PER_PAGE = 100

# Strip HTML tags
TAG_RE = re.compile(r"<[^>]+>")
# Collapse whitespace
WS_RE = re.compile(r"\s+")


def strip_html(html_str: str) -> str:
    """Remove HTML tags and clean up whitespace."""
    text = TAG_RE.sub(" ", html_str)
    text = html_mod.unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


def is_commercial_court(case_number: str) -> bool:
    """Check if case number indicates a Commercial Court case."""
    if not case_number:
        return False
    return "COM" in case_number.upper()


def parse_ecsc_date(date_str: str) -> Optional[str]:
    """Parse date from ACF field (DD/MM/YYYY) to ISO format."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


class VGCommercialCourtScraper(BaseScraper):
    """Scraper for VG/CommercialCourt -- BVI Commercial Court Judgments."""

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

    def _fetch_page(self, page: int) -> tuple[list, int]:
        """Fetch a page of BVI posts. Returns (posts, total_pages)."""
        params = {
            "categories": BVI_CATEGORY,
            "per_page": PER_PAGE,
            "page": page,
            "_fields": "id,title,slug,date,link,content,acf",
        }
        self.rate_limiter.wait()
        resp = self.client.get("/wp-json/wp/v2/posts", params=params)
        resp.raise_for_status()
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        return resp.json(), total_pages

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        wp_id = raw.get("id", "")
        acf = raw.get("acf", {}) or {}
        title = raw.get("title", {})
        if isinstance(title, dict):
            title = title.get("rendered", "")
        title = html_mod.unescape(str(title)).strip()

        case_number = acf.get("case_number", "")
        date_str = acf.get("date_new", "")
        decision_date = parse_ecsc_date(date_str)

        # Use WP post date as fallback
        wp_date = raw.get("date", "")
        if not decision_date and wp_date:
            decision_date = wp_date[:10]

        content = raw.get("content", {})
        if isinstance(content, dict):
            content = content.get("rendered", "")
        text = strip_html(str(content))

        url = raw.get("link", "")

        return {
            "_id": f"VG/CommercialCourt/{wp_id}",
            "_source": "VG/CommercialCourt",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": decision_date,
            "url": url,
            "doc_id": str(wp_id),
            "case_number": case_number,
            "court": "BVI Commercial Court",
            "decision_date": decision_date,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0
        page = 1
        total_pages = 1

        while page <= total_pages:
            if limit and count >= limit:
                break

            logger.info(f"Fetching page {page}...")
            try:
                posts, total_pages = self._fetch_page(page)
            except Exception as e:
                logger.error(f"Failed to fetch page {page}: {e}")
                break

            logger.info(f"  Got {len(posts)} posts (page {page}/{total_pages})")

            for post in posts:
                if limit and count >= limit:
                    break

                acf = post.get("acf", {}) or {}
                case_number = acf.get("case_number", "")

                if not is_commercial_court(case_number):
                    continue

                title = post.get("title", {})
                if isinstance(title, dict):
                    title = title.get("rendered", "")
                logger.info(f"  [{count + 1}] {html_mod.unescape(str(title))[:70]}")

                content = post.get("content", {})
                if isinstance(content, dict):
                    content_html = content.get("rendered", "")
                else:
                    content_html = str(content)

                text = strip_html(content_html)
                if len(text) < 50:
                    logger.warning(f"    Skipping - text too short ({len(text)} chars)")
                    continue

                yield post
                count += 1

            page += 1

        logger.info(f"Fetched {count} Commercial Court judgments total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        page = 1
        total_pages = 1

        while page <= total_pages:
            logger.info(f"Fetching updates page {page}...")
            try:
                params = {
                    "categories": BVI_CATEGORY,
                    "per_page": PER_PAGE,
                    "page": page,
                    "after": f"{since}T00:00:00",
                    "orderby": "date",
                    "order": "asc",
                    "_fields": "id,title,slug,date,link,content,acf",
                }
                self.rate_limiter.wait()
                resp = self.client.get("/wp-json/wp/v2/posts", params=params)
                resp.raise_for_status()
                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                posts = resp.json()
            except Exception as e:
                logger.error(f"Failed: {e}")
                break

            for post in posts:
                acf = post.get("acf", {}) or {}
                if not is_commercial_court(acf.get("case_number", "")):
                    continue
                yield post

            page += 1


if __name__ == "__main__":
    scraper = VGCommercialCourtScraper()

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
