#!/usr/bin/env python3
"""
OM/DecreeOm -- Oman Decrees Portal (decree.om)

Fetches English translations of Omani legislation from decree.om, which provides
the largest database of English-translated Omani laws.

Strategy:
  - Use WordPress REST API (wp-json/wp/v2/posts) to paginate through all posts
  - Fetch Royal Decrees (4,876), Ministerial Decisions (4,549), Consolidated Laws (95),
    Consolidated Regulations (55), and Treaties (679)
  - Content is returned as HTML in the API response — clean to plain text
  - ~10,000+ legal instruments total

Source: https://decree.om/ (Decree Tech LLC, English translations of Omani law)
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull (all decrees)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.OM.DecreeOm")

BASE_URL = "https://decree.om"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"

# Categories to fetch (id -> name)
CATEGORIES = {
    2: "Royal Decree",
    98: "Ministerial Decision",
    7: "Consolidated Law",
    419: "Consolidated Regulation",
    14: "Treaty",
}


class DecreeOmScraper(BaseScraper):
    """
    Scraper for OM/DecreeOm -- Oman Decrees Portal.
    Country: OM
    URL: https://decree.om/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    # -- HTML cleaning -------------------------------------------------------

    @staticmethod
    def _clean_html(html_str: str) -> str:
        """Convert HTML content to plain text."""
        if not html_str:
            return ""
        # Remove MemberPress unauthorized wrappers
        text = re.sub(r'<div class="mp_wrapper">.*?<div class="mepr-unauthorized-excerpt">', '', text if 'text' in dir() else html_str, flags=re.DOTALL)
        text = html_str
        # Remove script/style
        text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', text, flags=re.DOTALL | re.I)
        # Convert <br> and block elements to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.I)
        text = re.sub(r'</(p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.I)
        text = re.sub(r'<(p|div|h[1-6]|li|tr|blockquote)[^>]*>', '\n', text, flags=re.I)
        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode HTML entities
        text = html_module.unescape(text)
        # Clean whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'^\s+$', '', text, flags=re.MULTILINE)
        return text.strip()

    # -- API fetching --------------------------------------------------------

    def _fetch_page(self, category_id: int, page: int, per_page: int = 100) -> list[dict]:
        """Fetch one page of posts from the WP API."""
        self.rate_limiter.wait()
        params = {
            "categories": category_id,
            "per_page": per_page,
            "page": page,
            "_fields": "id,title,date,link,slug,content,categories",
        }
        url = f"{API_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"

        try:
            resp = self.client.get(url, timeout=60)
            if resp is None or resp.status_code == 400:
                return []  # Past last page
            if resp.status_code != 200:
                logger.warning(f"API returned {resp.status_code} for category {category_id} page {page}")
                return []
            return resp.json()
        except Exception as e:
            logger.warning(f"Error fetching page {page} of category {category_id}: {e}")
            return []

    def _fetch_category(self, category_id: int, category_name: str, limit: int = 0) -> Generator[dict, None, None]:
        """Yield all posts from a category, paginating through API."""
        page = 1
        total = 0

        while True:
            posts = self._fetch_page(category_id, page)
            if not posts:
                break

            for post in posts:
                content_html = post.get("content", {}).get("rendered", "")
                text = self._clean_html(content_html)

                if len(text) < 30:
                    continue

                title = post.get("title", {}).get("rendered", "")
                title = html_module.unescape(title)

                post["_clean_text"] = text
                post["_clean_title"] = title
                post["_category_name"] = category_name

                total += 1
                yield post

                if limit > 0 and total >= limit:
                    return

            page += 1
            if total % 200 == 0 and total > 0:
                logger.info(f"  {category_name}: {total} posts fetched")

        logger.info(f"  {category_name}: {total} total posts")

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation from all categories."""
        total = 0
        for cat_id, cat_name in CATEGORIES.items():
            logger.info(f"Fetching category: {cat_name} (id={cat_id})")
            for post in self._fetch_category(cat_id, cat_name):
                total += 1
                yield post

        logger.info(f"Fetch complete: {total} total records")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch posts modified since a given date."""
        since_str = since.strftime('%Y-%m-%dT%H:%M:%S')
        for cat_id, cat_name in CATEGORIES.items():
            page = 1
            while True:
                self.rate_limiter.wait()
                url = f"{API_URL}?categories={cat_id}&per_page=100&page={page}&after={since_str}&_fields=id,title,date,link,slug,content,categories"
                try:
                    resp = self.client.get(url, timeout=60)
                    if resp is None or resp.status_code == 400 or not resp.json():
                        break
                    for post in resp.json():
                        content_html = post.get("content", {}).get("rendered", "")
                        text = self._clean_html(content_html)
                        if len(text) < 30:
                            continue
                        title = html_module.unescape(post.get("title", {}).get("rendered", ""))
                        post["_clean_text"] = text
                        post["_clean_title"] = title
                        post["_category_name"] = cat_name
                        yield post
                    page += 1
                except Exception:
                    break

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample posts — 3 from each of 5 categories."""
        found = 0
        per_cat = max(count // len(CATEGORIES), 3)

        for cat_id, cat_name in CATEGORIES.items():
            if found >= count:
                break

            cat_found = 0
            for post in self._fetch_category(cat_id, cat_name, limit=per_cat):
                if found >= count:
                    break
                found += 1
                cat_found += 1
                title = post["_clean_title"][:60]
                text_len = len(post["_clean_text"])
                logger.info(f"Sample {found}/{count}: [{cat_name}] {title} ({text_len} chars)")
                yield post

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw WP post to standard schema."""
        wp_id = raw.get("id", 0)
        title = raw.get("_clean_title", "Unknown")
        text = raw.get("_clean_text", "")
        category = raw.get("_category_name", "")
        date_str = raw.get("date", "")
        link = raw.get("link", "")
        slug = raw.get("slug", "")

        # Parse date
        date_iso = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str)
                date_iso = dt.strftime('%Y-%m-%d')
            except Exception:
                pass

        return {
            "_id": f"OM-DecreeOm-{wp_id}",
            "_source": "OM/DecreeOm",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": link,
            "category": category,
            "wp_id": wp_id,
            "slug": slug,
            "language": "en",
        }

    def test_api(self) -> bool:
        """Test connectivity to decree.om WP API."""
        logger.info("Testing decree.om WordPress API...")

        # Test API endpoint
        resp = self.client.get(f"{API_URL}?per_page=1&categories=2&_fields=id,title", timeout=15)
        if not resp or resp.status_code != 200:
            logger.error(f"API failed: {resp.status_code if resp else 'None'}")
            return False
        data = resp.json()
        if not data:
            logger.error("API returned empty result")
            return False
        logger.info(f"API: OK (first post: {data[0].get('title', {}).get('rendered', '')[:60]})")

        # Test full content
        self.rate_limiter.wait()
        resp = self.client.get(f"{API_URL}?per_page=1&categories=7&_fields=id,title,content", timeout=15)
        if resp and resp.status_code == 200:
            post = resp.json()[0]
            text = self._clean_html(post.get("content", {}).get("rendered", ""))
            logger.info(f"Content extraction: {len(text)} chars")
            if len(text) < 50:
                logger.error("Content too short")
                return False

        logger.info("All tests passed!")
        return True


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = DecreeOmScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        logger.info("Running full fetch")
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_all():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
