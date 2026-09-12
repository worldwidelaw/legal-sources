#!/usr/bin/env python3
"""
BW/NBFIRA -- Non-Bank Financial Institutions Regulatory Authority

Fetches regulatory publications, public notices, enforcement actions,
and tribunal judgements from NBFIRA Botswana via WordPress REST API.

Strategy:
  - WordPress REST API provides full text in content.rendered
  - Posts endpoint: public notices, news, circulars (~600+ records)
  - Tribunal-judgements custom post type: case decisions (~6 records)
  - No PDF extraction needed — content is inline HTML

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import time
import logging
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BW.NBFIRA")

API_BASE = "https://www.nbfira.org.bw/wp-json/wp/v2"
USER_AGENT = "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)"

# Category IDs on the NBFIRA WordPress site
CAT_PUBLIC_NOTICES = 15
CAT_NEWS = 19
CAT_PRESS_RELEASES = 154
CAT_GUIDELINES = 119

# Minimum clean text length to keep a record (skip very short notices)
MIN_TEXT_LENGTH = 150


def strip_html(raw_html: str) -> str:
    """Remove HTML tags, decode entities, collapse whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", raw_html, flags=re.S | re.I)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.S | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?(p|div|h[1-6]|li|tr|td|th|blockquote)[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def wp_get(endpoint: str, params: dict = None, timeout: int = 30) -> requests.Response:
    """Make a GET request to the WordPress REST API."""
    url = f"{API_BASE}/{endpoint}" if not endpoint.startswith("http") else endpoint
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp


def paginate_wp(endpoint: str, params: dict = None, max_pages: int = 50) -> Generator[dict, None, None]:
    """Paginate through a WordPress REST API endpoint, yielding each record."""
    if params is None:
        params = {}
    params.setdefault("per_page", 100)
    page = 1

    while page <= max_pages:
        params["page"] = page
        try:
            resp = wp_get(endpoint, params=params)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                # WP returns 400 when page exceeds total pages
                break
            raise

        data = resp.json()
        if not data:
            break

        for item in data:
            yield item

        total_pages = int(resp.headers.get("X-WP-TotalPages", max_pages))
        if page >= total_pages:
            break

        page += 1
        time.sleep(1.0)


class NBFIRAScraper(BaseScraper):
    """
    Scraper for BW/NBFIRA — Non-Bank Financial Institutions Regulatory Authority.
    Country: BW
    URL: https://www.nbfira.org.bw/

    Data types: doctrine, case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _classify_type(self, post: dict, is_tribunal: bool = False) -> str:
        """Determine _type based on post origin."""
        if is_tribunal:
            return "case_law"
        return "doctrine"

    def _normalize_post(self, post: dict, is_tribunal: bool = False) -> Optional[dict]:
        """Normalize a WordPress post/tribunal-judgement into standard schema."""
        title = strip_html(post.get("title", {}).get("rendered", ""))
        content_html = post.get("content", {}).get("rendered", "")
        text = strip_html(content_html)

        if len(text) < MIN_TEXT_LENGTH:
            return None

        date_str = post.get("date", "")
        if date_str:
            date_str = date_str[:10]  # YYYY-MM-DD

        post_id = post.get("id", 0)
        link = post.get("link", f"https://www.nbfira.org.bw/?p={post_id}")

        return {
            "_id": f"nbfira-{post_id}",
            "_source": "BW/NBFIRA",
            "_type": self._classify_type(post, is_tribunal),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str or None,
            "url": link,
            "wp_id": post_id,
            "wp_modified": post.get("modified", "")[:10] if post.get("modified") else None,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema (called by BaseScraper)."""
        return raw  # Already normalized in _normalize_post

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulatory documents and tribunal judgements."""
        yielded = 0

        # 1. Tribunal judgements (case_law)
        logger.info("Fetching tribunal judgements...")
        for post in paginate_wp("tribunal-judgements"):
            record = self._normalize_post(post, is_tribunal=True)
            if record:
                yield record
                yielded += 1
        logger.info(f"Tribunal judgements: {yielded} records")

        # 2. All posts (doctrine — public notices, news, circulars, etc.)
        post_count = 0
        logger.info("Fetching all posts (public notices, news, circulars)...")
        for post in paginate_wp("posts"):
            record = self._normalize_post(post, is_tribunal=False)
            if record:
                yield record
                yielded += 1
                post_count += 1
                if post_count % 50 == 0:
                    logger.info(f"Posts processed: {post_count}")

        logger.info(f"fetch_all complete: {yielded} total records ({post_count} posts)")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield documents modified after 'since' date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        params = {"after": f"{since}T00:00:00", "orderby": "modified"}

        for post in paginate_wp("tribunal-judgements", params=dict(params)):
            record = self._normalize_post(post, is_tribunal=True)
            if record:
                yield record

        for post in paginate_wp("posts", params=dict(params)):
            record = self._normalize_post(post, is_tribunal=False)
            if record:
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="BW/NBFIRA — Non-Bank Financial Institutions Regulatory Authority"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = NBFIRAScraper()

    if args.command == "test":
        logger.info("Testing NBFIRA connectivity...")
        try:
            resp = wp_get("tribunal-judgements", {"per_page": 1})
            data = resp.json()
            logger.info(f"Tribunal endpoint: {len(data)} records returned")

            resp2 = wp_get("posts", {"per_page": 1})
            data2 = resp2.json()
            logger.info(f"Posts endpoint: {len(data2)} records returned")
            total_pages = resp2.headers.get("X-WP-TotalPages", "?")
            total_posts = resp2.headers.get("X-WP-Total", "?")
            logger.info(f"Total posts available: {total_posts} across {total_pages} pages")

            if data:
                record = scraper._normalize_post(data[0], is_tribunal=True)
                if record:
                    logger.info(f"Sample tribunal: {record['title'][:80]}")
                    logger.info(f"Text length: {len(record['text'])} chars")
                    logger.info(f"Preview: {record['text'][:200]}")

            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
