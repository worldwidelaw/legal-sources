#!/usr/bin/env python3
"""
NG/FCCPC -- Nigeria Federal Competition and Consumer Protection Commission

Fetches publications via WordPress REST API (wp-json/wp/v2/posts).

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import re
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.FCCPC")

API_BASE = "https://fccpc.gov.ng/wp-json/wp/v2"
PER_PAGE = 50
DELAY = 1.0
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Category mapping (from WP category IDs)
CATEGORY_MAP = {
    27: "alert_announcement",
    31: "anti_tobacco",
    1: "news_events",
    15: "releases",
    16: "speeches",
    17: "testimonials",
    18: "testimonials",
    19: "tips",
}


def _clean_html(html: str) -> str:
    """Strip HTML tags and clean whitespace from WP content."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?(p|div|li|h[1-6]|tr|td|th|ul|ol|table|blockquote)[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#039;", "'").replace("&nbsp;", " ")
    text = text.replace("\u00a0", " ").replace("&#8211;", "–").replace("&#8217;", "'")
    text = text.replace("&#8220;", "\u201c").replace("&#8221;", "\u201d")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class FCCPCScraper(BaseScraper):
    """Scraper for FCCPC publications via WordPress REST API."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(headers={"User-Agent": UA})

    def _fetch_posts_page(self, page: int = 1, per_page: int = PER_PAGE) -> list:
        """Fetch a page of posts from the WP REST API."""
        url = f"{API_BASE}/posts?per_page={per_page}&page={page}&_fields=id,title,date,slug,link,content,categories"
        try:
            resp = self.http.get(url, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 400:
                return []  # No more pages
            logger.warning("HTTP %d fetching page %d", resp.status_code, page)
            return []
        except Exception as e:
            logger.warning("Error fetching page %d: %s", page, e)
            return []

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all posts with full content."""
        page = 1
        total_yielded = 0

        while True:
            logger.info("Fetching posts page %d...", page)
            posts = self._fetch_posts_page(page)

            if not posts:
                logger.info("No more posts on page %d, done.", page)
                break

            for post in posts:
                content_html = post.get("content", {}).get("rendered", "")
                text = _clean_html(content_html)

                if len(text) < 50:
                    logger.warning("Post %d has too little text (%d chars), skipping",
                                   post["id"], len(text))
                    continue

                title_raw = post.get("title", {}).get("rendered", "")
                title = _clean_html(title_raw)

                categories = post.get("categories", [])
                cat_name = CATEGORY_MAP.get(categories[0], "other") if categories else "other"

                yield {
                    "post_id": str(post["id"]),
                    "title": title,
                    "text": text,
                    "date": post.get("date", ""),
                    "slug": post.get("slug", ""),
                    "url": post.get("link", ""),
                    "category": cat_name,
                }
                total_yielded += 1

            logger.info("Page %d: %d posts (total yielded: %d)", page, len(posts), total_yielded)
            page += 1
            time.sleep(DELAY)

        logger.info("Total posts yielded: %d", total_yielded)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch posts modified since a given date."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1
        while True:
            url = (f"{API_BASE}/posts?per_page={PER_PAGE}&page={page}"
                   f"&after={since_str}&_fields=id,title,date,slug,link,content,categories")
            try:
                resp = self.http.get(url, timeout=30)
                if resp.status_code != 200:
                    break
                posts = resp.json()
                if not posts:
                    break
                for post in posts:
                    content_html = post.get("content", {}).get("rendered", "")
                    text = _clean_html(content_html)
                    if len(text) < 50:
                        continue
                    title = _clean_html(post.get("title", {}).get("rendered", ""))
                    categories = post.get("categories", [])
                    cat_name = CATEGORY_MAP.get(categories[0], "other") if categories else "other"
                    yield {
                        "post_id": str(post["id"]),
                        "title": title,
                        "text": text,
                        "date": post.get("date", ""),
                        "slug": post.get("slug", ""),
                        "url": post.get("link", ""),
                        "category": cat_name,
                    }
                page += 1
                time.sleep(DELAY)
            except Exception:
                break

    def normalize(self, raw: dict) -> dict:
        """Normalize a post into the standard schema."""
        date_str = raw.get("date", "")
        if date_str:
            date_str = date_str[:10]  # Keep YYYY-MM-DD
        return {
            "_id": f"NG_FCCPC_{raw['post_id']}",
            "_source": "NG/FCCPC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": date_str,
            "url": raw["url"],
            "category": raw.get("category", ""),
            "post_id": raw["post_id"],
            "slug": raw.get("slug", ""),
        }


# ── CLI ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = FCCPCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test":
        logger.info("Testing FCCPC WordPress REST API...")
        posts = scraper._fetch_posts_page(1, 3)
        if posts:
            logger.info("OK — %d posts returned", len(posts))
            for p in posts:
                title = _clean_html(p["title"]["rendered"])
                content = _clean_html(p["content"]["rendered"])
                logger.info("  %s — %d chars", title[:60], len(content))
        else:
            logger.error("FAILED — no posts returned")
            sys.exit(1)

    elif cmd == "bootstrap":
        sample = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        logger.info("Bootstrap complete: %s", stats)

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
