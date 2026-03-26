#!/usr/bin/env python3
"""
LY/DCAF -- Libya DCAF Security Sector Legal Database

Fetches Libyan security sector legislation from DCAF Geneva Centre
via WordPress REST API.

Strategy:
  - Paginate through /wp-json/wp/v2/latest-laws (2155 laws, 100/page)
  - Filter for posts with actual content (skip Arabic-only placeholders)
  - Extract full text from HTML content field
  - Resolve taxonomy terms for metadata

API:
  - Base: https://security-legislation.ly
  - List: GET /wp-json/wp/v2/latest-laws?per_page=100&page=N
  - Detail: GET /wp-json/wp/v2/latest-laws/{id}
  - Taxonomies: database-index-categories, text-type-categories,
    status-categories, institution-categories
  - No auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LY.DCAF")

BASE_URL = "https://security-legislation.ly"
WP_API = "/wp-json/wp/v2"
LAWS_ENDPOINT = f"{WP_API}/latest-laws"
ARABIC_PLACEHOLDER = "ONLY AVAILABLE IN ARABIC"
PER_PAGE = 100


def clean_html(html_text: str) -> str:
    """Remove HTML tags and decode entities, preserving paragraph breaks."""
    if not html_text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', html_text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<li[^>]*>', '- ', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r' +', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class DCAFScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={"Accept": "application/json"},
        )
        self._taxonomy_cache: Dict[str, Dict[int, str]] = {}

    def _resolve_taxonomy(self, taxonomy: str, term_ids: List[int]) -> List[str]:
        """Resolve taxonomy term IDs to names."""
        if not term_ids:
            return []
        if taxonomy not in self._taxonomy_cache:
            self._taxonomy_cache[taxonomy] = {}
        cache = self._taxonomy_cache[taxonomy]
        missing = [tid for tid in term_ids if tid not in cache]
        if missing:
            try:
                params = {"include": ",".join(str(t) for t in missing), "per_page": 100}
                resp = self.http.get(f"{WP_API}/{taxonomy}", params=params)
                if resp.status_code == 200:
                    for term in resp.json():
                        cache[term["id"]] = unescape(term.get("name", ""))
            except Exception as e:
                logger.warning(f"Failed to resolve taxonomy {taxonomy}: {e}")
        return [cache[tid] for tid in term_ids if tid in cache]

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all laws with English content via WP REST API."""
        page = 1
        total_fetched = 0
        skipped_arabic = 0

        while True:
            logger.info(f"Fetching page {page} (fetched={total_fetched}, skipped={skipped_arabic})")
            try:
                resp = self.http.get(
                    LAWS_ENDPOINT,
                    params={"per_page": PER_PAGE, "page": page, "orderby": "date", "order": "asc"},
                )
                if resp.status_code == 400:
                    logger.info("Reached end of results (400 response)")
                    break
                resp.raise_for_status()
                posts = resp.json()
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break

            if not posts:
                break

            for post in posts:
                content_html = ""
                if isinstance(post.get("content"), dict):
                    content_html = post["content"].get("rendered", "")
                text = clean_html(content_html)

                if ARABIC_PLACEHOLDER in text or len(text) < 50:
                    skipped_arabic += 1
                    continue

                total_fetched += 1
                yield post

            total_pages = int(resp.headers.get("X-WP-TotalPages", 0))
            if page >= total_pages:
                break
            page += 1

        logger.info(f"Done: {total_fetched} laws with content, {skipped_arabic} Arabic-only skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch laws modified since a given date."""
        if not since:
            yield from self.fetch_all()
            return

        page = 1
        while True:
            try:
                resp = self.http.get(
                    LAWS_ENDPOINT,
                    params={
                        "per_page": PER_PAGE,
                        "page": page,
                        "modified_after": since,
                        "orderby": "modified",
                        "order": "asc",
                    },
                )
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
                posts = resp.json()
            except Exception:
                break

            if not posts:
                break

            for post in posts:
                content_html = ""
                if isinstance(post.get("content"), dict):
                    content_html = post["content"].get("rendered", "")
                text = clean_html(content_html)
                if ARABIC_PLACEHOLDER not in text and len(text) >= 50:
                    yield post

            total_pages = int(resp.headers.get("X-WP-TotalPages", 0))
            if page >= total_pages:
                break
            page += 1

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform WP post into standard schema."""
        title = ""
        if isinstance(raw.get("title"), dict):
            title = unescape(raw["title"].get("rendered", ""))
        elif isinstance(raw.get("title"), str):
            title = unescape(raw["title"])

        content_html = ""
        if isinstance(raw.get("content"), dict):
            content_html = raw["content"].get("rendered", "")
        text = clean_html(content_html)

        post_date = raw.get("date", "")
        if post_date:
            try:
                dt = datetime.fromisoformat(post_date.replace("Z", "+00:00"))
                post_date = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        text_types = self._resolve_taxonomy(
            "text-type-categories",
            raw.get("text-type-categories", [])
        )
        status_cats = self._resolve_taxonomy(
            "status-categories",
            raw.get("status-categories", [])
        )
        institution_cats = self._resolve_taxonomy(
            "institution-categories",
            raw.get("institution-categories", [])
        )
        db_index_cats = self._resolve_taxonomy(
            "database-index-categories",
            raw.get("database-index-categories", [])
        )

        url = raw.get("link", f"{BASE_URL}/latest-laws/{raw.get('slug', '')}/")

        return {
            "_id": f"LY-DCAF-{raw['id']}",
            "_source": "LY/DCAF",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": post_date,
            "url": url,
            "language": "en",
            "country": "LY",
            "text_type": text_types[0] if text_types else None,
            "status": status_cats[0] if status_cats else None,
            "institution": institution_cats[0] if institution_cats else None,
            "index_category": db_index_cats[0] if db_index_cats else None,
            "wp_id": raw["id"],
            "slug": raw.get("slug", ""),
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.http.get(LAWS_ENDPOINT, params={"per_page": 1})
            resp.raise_for_status()
            data = resp.json()
            total = resp.headers.get("X-WP-Total", "?")
            logger.info(f"API OK: {total} total laws, got {len(data)} in test")
            return True
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="LY/DCAF Legal Database Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test", "update"])
    parser.add_argument("--sample", action="store_true", help="Fetch only ~12 sample records")
    args = parser.parse_args()

    scraper = DCAFScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    if args.command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 12 if args.sample else 999999

        gen = scraper.fetch_all() if args.command == "bootstrap" else scraper.fetch_updates()

        for raw in gen:
            if count >= max_records:
                break
            record = scraper.normalize(raw)
            if not record.get("text"):
                continue

            fname = f"{record['_id']}.json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(
                f"[{count}] {record['title'][:60]}... "
                f"({len(record['text'])} chars)"
            )

        logger.info(f"Saved {count} records to {sample_dir}/")


if __name__ == "__main__":
    main()
