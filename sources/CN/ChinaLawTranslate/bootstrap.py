#!/usr/bin/env python3
"""
CN/ChinaLawTranslate -- English Translations of Chinese Laws

Fetches English translations of Chinese laws and regulations from
chinalawtranslate.com via the WordPress REST API.

Strategy:
  - Paginate /wp-json/wp/v2/posts (100 per page)
  - Filter for law translation categories (exclude CLT Originals commentary)
  - Full text from content.rendered (HTML → clean text)

API:
  - Base: https://www.chinalawtranslate.com
  - Posts: /wp-json/wp/v2/posts?per_page=100&page={N}
  - Categories: /wp-json/wp/v2/categories
  - No auth required

Categories of interest (law translations):
  - 10: Administrative/Regulatory (148 posts)
  - 4: Civil (84 posts)
  - 3: Criminal Law (227 posts)
  - 822: Environment (23 posts)
  - 1357: Internet (180 posts)
  - 6: IP Law (17 posts)
  - 153: Labor and Employment (10 posts)
  - 607: Disability (7 posts)
  - 2339: Juvenile (18 posts)
  - 706: Guiding Cases (13 posts) → case_law

Commentary/analysis (included as doctrine):
  - 422: CLT Originals (223 posts) → doctrine

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmlmod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CN.ChinaLawTranslate")

BASE_URL = "https://www.chinalawtranslate.com"

# Categories that map to case_law (guiding cases)
CASE_LAW_CATEGORIES = {706}  # Guiding Cases

# Categories that are original commentary/analysis → doctrine
DOCTRINE_CATEGORIES = {422}  # CLT Originals

# All other categories are legislation translations
LEGISLATION_CATEGORIES = {10, 4, 3, 822, 1357, 6, 153, 607, 2339, 2436, 2435, 2347}


def clean_html_text(html_str: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_str:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr|blockquote)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = htmlmod.unescape(text)
    lines = [line.strip() for line in text.split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines).strip()


class ChinaLawTranslateScraper(BaseScraper):
    """Scraper for CN/ChinaLawTranslate via WordPress REST API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=60,
        )

    def _get_posts_page(self, page: int, per_page: int = 10) -> tuple:
        """Fetch a page of posts from WordPress API. Returns (posts, total_pages)."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(
                f"/wp-json/wp/v2/posts?per_page={per_page}&page={page}"
                f"&_fields=id,title,date,link,categories,content,excerpt"
            )
            if not resp or resp.status_code != 200:
                if resp and resp.status_code == 400:
                    return [], 0  # Past last page
                logger.warning(f"API error page {page}: {resp.status_code if resp else 'no response'}")
                return [], 0
            total_pages = int(resp.headers.get("X-WP-TotalPages", 0))
            return resp.json(), total_pages
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            return [], 0

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all posts from WordPress API with pagination."""
        page = 1
        total_pages = None

        while True:
            posts, tp = self._get_posts_page(page)
            if total_pages is None:
                total_pages = tp
                logger.info(f"Total pages: {total_pages}")

            if not posts:
                break

            for post in posts:
                yield post

            logger.info(f"Fetched page {page}/{total_pages} ({len(posts)} posts)")
            page += 1
            if total_pages and page > total_pages:
                break

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch posts modified since a given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1
        while True:
            self.rate_limiter.wait()
            try:
                resp = self.client.get(
                    f"/wp-json/wp/v2/posts?per_page=10&page={page}"
                    f"&after={since_iso}"
                    f"&_fields=id,title,date,link,categories,content,excerpt"
                )
                if not resp or resp.status_code != 200:
                    break
                posts = resp.json()
                if not posts:
                    break
                for post in posts:
                    yield post
                page += 1
            except Exception as e:
                logger.error(f"Error fetching updates page {page}: {e}")
                break

    def _determine_type(self, categories: list) -> str:
        """Determine record type based on WordPress categories."""
        cat_set = set(categories)
        if cat_set & CASE_LAW_CATEGORIES:
            return "case_law"
        if cat_set & DOCTRINE_CATEGORIES and not (cat_set & LEGISLATION_CATEGORIES):
            return "doctrine"
        return "legislation"

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform WordPress post into standard schema."""
        wp_id = raw.get("id")
        title_obj = raw.get("title", {})
        title = htmlmod.unescape((title_obj.get("rendered") or "").strip())
        content_obj = raw.get("content", {})
        content_html = content_obj.get("rendered", "")
        date_str = (raw.get("date") or "")[:10]
        link = raw.get("link", "")
        categories = raw.get("categories", [])

        if not title:
            return None

        text = clean_html_text(content_html)
        if not text or len(text) < 50:
            return None

        doc_type = self._determine_type(categories)
        doc_id = f"CN-CLT-{wp_id}"

        return {
            "_id": doc_id,
            "_source": "CN/ChinaLawTranslate",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str if date_str else None,
            "url": link,
            "jurisdiction": "CN",
            "language": "en",
            "wp_categories": categories,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing China Law Translate WordPress API...")

        posts, total_pages = self._get_posts_page(1, per_page=5)
        if not posts:
            print("FAILED: No posts returned from API")
            return

        total = None
        self.rate_limiter.wait()
        resp = self.client.get("/wp-json/wp/v2/posts?per_page=1")
        if resp and resp.status_code == 200:
            total = resp.headers.get("X-WP-Total", "?")
        print(f"Total posts: {total}, pages: {total_pages}")

        for post in posts[:3]:
            title = htmlmod.unescape(post.get("title", {}).get("rendered", ""))[:80]
            date = post.get("date", "")[:10]
            cats = post.get("categories", [])
            content = post.get("content", {}).get("rendered", "")
            text = clean_html_text(content)

            print(f"\n  Date: {date}")
            print(f"  Title: {title}")
            print(f"  Categories: {cats}")
            print(f"  Type: {self._determine_type(cats)}")
            print(f"  Text length: {len(text)} chars")
            print(f"  Preview: {text[:200]}...")

        print("\nTest complete!")


def main():
    scraper = ChinaLawTranslateScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
