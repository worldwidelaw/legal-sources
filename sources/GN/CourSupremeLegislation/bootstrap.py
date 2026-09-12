#!/usr/bin/env python3
"""
GN/CourSupremeLegislation -- Guinea Supreme Court Legislative Texts

Fetches legislative PDFs from coursupreme.org.gn via the WordPress REST API.
Covers codes, laws, decrees, constitutions, and arrêtés.

Strategy:
  - Query WP REST API for posts in legislation-related categories
  - Extract PDF URLs from post content HTML
  - Deduplicate by WordPress post ID (posts may be in multiple categories)
  - Download and extract text via common/pdf_extract
  - Skip scanned-image PDFs with no extractable text

Usage:
  python bootstrap.py bootstrap          # Fetch all legislative texts
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import time
import re
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Set
from html import unescape

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GN.CourSupremeLegislation")

BASE_URL = "https://www.coursupreme.org.gn"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"

# Legislation-related category IDs from the WP REST API
# textes-legislatifs=18, codes-en-vigueur=48, codes-anciens=47,
# constitutions-guineennes=49, lois=46, decrets=44, arretes=45,
# codes=103, legislation=104
LEGISLATION_CATEGORIES = [18, 48, 47, 49, 46, 44, 45, 103, 104]

# Map category IDs to human-readable names
CATEGORY_NAMES = {
    18: "textes_legislatifs",
    48: "codes_en_vigueur",
    47: "codes_anciens",
    49: "constitutions",
    46: "lois",
    44: "decrets",
    45: "arretes",
    103: "codes",
    104: "legislation",
}


def _extract_pdf_urls(html_content: str) -> List[str]:
    """Extract PDF URLs from WordPress post content HTML."""
    urls = set()
    soup = BeautifulSoup(html_content, "html.parser")

    # Pattern 1: Direct <a> links to PDFs
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if ".pdf" in href.lower():
            urls.add(href)

    # Pattern 2: iframe src pointing to PDFs (Google Docs viewer or direct)
    for iframe in soup.find_all("iframe", src=True):
        src = iframe["src"]
        m = re.search(r"url=([^&]+\.pdf)", src)
        if m:
            from urllib.parse import unquote
            urls.add(unquote(m.group(1)))
        elif ".pdf" in src.lower():
            urls.add(src)

    # Pattern 3: data attributes with PDF URLs
    for tag in soup.find_all(attrs={"data-src": True}):
        src = tag["data-src"]
        if ".pdf" in src.lower():
            urls.add(src)

    return list(urls)


def _clean_title(raw_title: str) -> str:
    """Clean HTML entities from WP title."""
    return unescape(raw_title).strip()


def _categorize(categories: List[int]) -> str:
    """Return the most specific category name."""
    for cat_id in [49, 48, 47, 46, 44, 45, 103, 104, 18]:
        if cat_id in categories:
            return CATEGORY_NAMES.get(cat_id, "legislation")
    return "legislation"


class CourSupremeLegislationScraper(BaseScraper):
    """Scraper for GN/CourSupremeLegislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _fetch_posts_for_category(self, cat_id: int) -> List[Dict]:
        """Fetch all posts for a category via WP REST API with pagination."""
        posts = []
        page = 1
        while True:
            params = {
                "categories": cat_id,
                "per_page": 100,
                "page": page,
                "_fields": "id,title,content,date,link,categories",
            }
            try:
                time.sleep(2)
                resp = self.session.get(API_URL, params=params, timeout=30)
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.warning(f"API request failed for cat {cat_id} page {page}: {e}")
                break

            data = resp.json()
            if not data:
                break
            posts.extend(data)

            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1

        return posts

    def _fetch_all_legislation_posts(self) -> Dict[int, Dict]:
        """Fetch posts from all legislation categories, deduplicated by post ID."""
        all_posts = {}
        for cat_id in LEGISLATION_CATEGORIES:
            cat_name = CATEGORY_NAMES.get(cat_id, str(cat_id))
            posts = self._fetch_posts_for_category(cat_id)
            new_count = 0
            for post in posts:
                pid = post["id"]
                if pid not in all_posts:
                    all_posts[pid] = post
                    new_count += 1
                else:
                    # Merge category lists
                    existing_cats = set(all_posts[pid].get("categories", []))
                    existing_cats.update(post.get("categories", []))
                    all_posts[pid]["categories"] = list(existing_cats)
            logger.info(f"Category {cat_name}: {len(posts)} posts ({new_count} new)")

        logger.info(f"Total unique legislation posts: {len(all_posts)}")
        return all_posts

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": "GN/CourSupremeLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url", ""),
            "category": raw.get("category", ""),
            "wp_post_id": raw.get("wp_post_id"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislative PDFs from the Supreme Court website."""
        existing = preload_existing_ids("GN/CourSupremeLegislation", table="legislation")
        all_posts = self._fetch_all_legislation_posts()

        count = 0
        skipped_no_pdf = 0
        skipped_no_text = 0

        for pid, post in all_posts.items():
            doc_id = f"GN-CSL-{pid}"
            if doc_id in existing:
                logger.debug(f"Skipping {doc_id} — already in Neon")
                continue

            title = _clean_title(post["title"]["rendered"])
            content_html = post["content"]["rendered"]
            date = post.get("date", "")[:10]  # YYYY-MM-DD
            url = post.get("link", "")
            categories = post.get("categories", [])

            # Extract PDF URLs from content
            pdf_urls = _extract_pdf_urls(content_html)
            if not pdf_urls:
                skipped_no_pdf += 1
                logger.debug(f"No PDF in post {pid}: {title[:50]}")
                continue

            # Use the first PDF (usually the main document)
            pdf_url = pdf_urls[0]
            logger.info(f"Extracting: {title[:60]} (post {pid})")

            try:
                text = extract_pdf_markdown(
                    source="GN/CourSupremeLegislation",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )
            except Exception as e:
                logger.warning(f"PDF extraction failed for post {pid}: {e}")
                text = None

            if not text or len(text) < 50:
                skipped_no_text += 1
                logger.warning(f"Insufficient text for post {pid} ({title[:40]}): {len(text) if text else 0} chars")
                continue

            category = _categorize(categories)

            entry = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "pdf_url": pdf_url,
                "url": url,
                "category": category,
                "wp_post_id": pid,
            }
            count += 1
            yield entry

        logger.info(
            f"Completed: {count} texts fetched, "
            f"{skipped_no_pdf} skipped (no PDF), "
            f"{skipped_no_text} skipped (no text)"
        )

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates — same as fetch_all since the corpus is small."""
        yield from self.fetch_all()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GN/CourSupremeLegislation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CourSupremeLegislationScraper()

    if args.command == "test":
        logger.info("Testing connectivity to coursupreme.org.gn WP REST API...")
        try:
            resp = scraper.session.get(
                f"{BASE_URL}/wp-json/wp/v2/categories",
                params={"per_page": 1},
                timeout=30,
            )
            resp.raise_for_status()
            logger.info(f"Connection OK — status {resp.status_code}")
            sys.exit(0)
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            sys.exit(1)
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
