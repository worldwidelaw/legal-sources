#!/usr/bin/env python3
"""
CI/LoiDiCI — Côte d'Ivoire Legal Codes (loidici.biz)

Fetches Ivorian legal articles from the WordPress REST API.
~15,500 posts covering 30+ legal codes (Civil, Penal, Labor, etc.).

Strategy:
  - Paginate the WP REST API: /wp-json/wp/v2/posts?per_page=100&page=N
  - Extract full text from content.rendered (HTML → plain text)
  - Map category IDs to names for classification

Usage:
  python bootstrap.py bootstrap          # Fetch all legislation
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import re
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CI.LoiDiCI")

BASE_URL = "https://loidici.biz"
API_URL = f"{BASE_URL}/wp-json/wp/v2"


def _html_to_text(html_content: str) -> str:
    """Convert HTML content to clean plain text."""
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, "html.parser")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for p in soup.find_all("p"):
        p.insert_after("\n\n")
    text = soup.get_text()
    text = html.unescape(text)
    # Normalize whitespace
    lines = []
    for line in text.split("\n"):
        stripped = line.strip()
        if stripped:
            lines.append(stripped)
    return "\n".join(lines)


class LoiDiCIScraper(BaseScraper):
    """Scraper for CI/LoiDiCI — Côte d'Ivoire legal codes via WP REST API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })
        self._categories: Optional[Dict[int, str]] = None

    def _request_json(self, url: str, timeout: int = 60) -> Optional[Any]:
        """HTTP GET returning JSON with rate limiting and retry."""
        for attempt in range(3):
            try:
                time.sleep(1.0)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 400:
                    return None
                resp.raise_for_status()
                return resp.json(), resp.headers
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _get_categories(self) -> Dict[int, str]:
        """Fetch and cache all WP categories."""
        if self._categories is not None:
            return self._categories

        self._categories = {}
        page = 1
        while True:
            url = f"{API_URL}/categories?per_page=100&page={page}"
            result = self._request_json(url)
            if result is None:
                break
            data, headers = result
            if not data:
                break
            for cat in data:
                self._categories[cat["id"]] = cat["name"]
            total_pages = int(headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1

        logger.info(f"Loaded {len(self._categories)} categories")
        return self._categories

    def _post_to_raw(self, post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Convert a WP API post object to raw record dict."""
        content_html = post.get("content", {}).get("rendered", "")
        text = _html_to_text(content_html)

        if not text or len(text) < 20:
            return None

        title_html = post.get("title", {}).get("rendered", "")
        title = html.unescape(BeautifulSoup(title_html, "html.parser").get_text())

        # Map category IDs to names
        categories = self._get_categories()
        cat_ids = post.get("categories", [])
        cat_names = [categories.get(cid, f"cat-{cid}") for cid in cat_ids]

        date_str = post.get("date", "")
        if date_str:
            date_str = date_str[:10]  # Just YYYY-MM-DD

        return {
            "post_id": str(post["id"]),
            "title": title,
            "text": text,
            "date": date_str,
            "modified": post.get("modified", "")[:10],
            "url": post.get("link", ""),
            "categories": cat_names,
            "slug": post.get("slug", ""),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        post_id = raw.get("post_id", "")
        return {
            "_id": f"CI-LOIDICI-{post_id}",
            "_source": "CI/LoiDiCI",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "categories": raw.get("categories", []),
            "modified": raw.get("modified", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all posts via WP REST API pagination."""
        # Pre-load categories
        self._get_categories()

        count = 0
        page = 1
        total_pages = None
        # Track post IDs already seen. The loidici.com WP REST endpoint was
        # observed to silently ignore the `page` param and keep returning
        # page 1, so the loop re-fetched the same 100 posts until `page`
        # crossed X-WP-TotalPages — thousands of duplicates, only 100 new
        # (issue #970). Break as soon as a page contributes no new IDs.
        seen_ids = set()

        while True:
            url = f"{API_URL}/posts?per_page=100&page={page}&_fields=id,date,modified,slug,link,title,content,categories"
            result = self._request_json(url)
            if result is None:
                logger.warning(f"Failed to fetch page {page}")
                break

            data, headers = result
            if not data:
                break

            if total_pages is None:
                total_pages = int(headers.get("X-WP-TotalPages", 1))
                total_posts = int(headers.get("X-WP-Total", 0))
                logger.info(f"Total: {total_posts} posts across {total_pages} pages")

            page_ids = [p.get("id") for p in data if p.get("id") is not None]
            new_ids = [i for i in page_ids if i not in seen_ids]
            if not new_ids:
                logger.warning(
                    f"Page {page} returned 0 new post IDs (all {len(page_ids)} "
                    f"already seen) — server is repeating results, stopping "
                    f"pagination."
                )
                break
            seen_ids.update(new_ids)
            new_id_set = set(new_ids)

            for post in data:
                if post.get("id") not in new_id_set:
                    continue
                raw = self._post_to_raw(post)
                if raw:
                    count += 1
                    yield raw

            logger.info(f"Page {page}/{total_pages}: {len(data)} posts (total yielded: {count})")

            if page >= total_pages:
                break
            page += 1

        logger.info(f"Completed: {count} records fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recently modified posts."""
        self._get_categories()
        count = 0

        # Fetch first 3 pages ordered by modification date
        for page in range(1, 4):
            url = f"{API_URL}/posts?per_page=100&page={page}&orderby=modified&order=desc&_fields=id,date,modified,slug,link,title,content,categories"
            result = self._request_json(url)
            if result is None:
                break
            data, _ = result
            if not data:
                break
            for post in data:
                raw = self._post_to_raw(post)
                if raw:
                    count += 1
                    yield raw

        logger.info(f"Updates: {count} records fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        url = f"{API_URL}/posts?per_page=3&_fields=id,title,content,categories"
        result = self._request_json(url)
        if result is None:
            logger.error("Cannot reach WP REST API")
            return False

        data, headers = result
        if not data:
            logger.error("No posts returned from API")
            return False

        total = headers.get("X-WP-Total", "?")
        logger.info(f"API OK: {total} total posts")

        # Check one post has content
        post = data[0]
        text = _html_to_text(post.get("content", {}).get("rendered", ""))
        title = html.unescape(
            BeautifulSoup(post["title"]["rendered"], "html.parser").get_text()
        )
        logger.info(f"Sample: '{title}' ({len(text)} chars)")
        return len(text) > 20


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CI/LoiDiCI data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LoiDiCIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
