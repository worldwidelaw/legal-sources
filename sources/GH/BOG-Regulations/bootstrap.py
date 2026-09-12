#!/usr/bin/env python3
"""
GH/BOG-Regulations -- Bank of Ghana Regulations, Directives & Banking Acts

Fetches regulatory documents from the Bank of Ghana website via WordPress
REST API custom post types (reg_directives, banking_acts). Each post has
PDF attachments which are downloaded and text-extracted.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GH.BOG-Regulations")

BASE_URL = "https://www.bog.gov.gh"
WP_API = f"{BASE_URL}/wp-json/wp/v2"
DELAY = 2.0

# Custom post types that contain regulatory documents
POST_TYPES = ["reg_directives", "banking_acts"]


def _make_id(post_type: str, wp_id: int, title: str) -> str:
    """Generate a stable ID from post type, WP ID, and title."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", title).strip("_")
    if len(slug) > 80:
        slug = slug[:80]
    return f"BOG_{post_type}_{wp_id}_{slug}"


def _clean_title(wp_title: str) -> str:
    """Clean up WP title (HTML entities, etc.)."""
    title = html.unescape(wp_title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _extract_date(wp_date: str) -> Optional[str]:
    """Extract ISO date from WP date field."""
    if not wp_date:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", wp_date)
    return m.group(1) if m else None


class BOGRegulationsScraper(BaseScraper):
    """Scraper for Bank of Ghana regulations and directives."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
            },
            verify=False,
        )

    def _discover_posts(self) -> List[Dict[str, Any]]:
        """Discover all regulatory posts via WP REST API custom post types."""
        all_posts = []

        for post_type in POST_TYPES:
            page = 1
            while page <= 20:
                try:
                    url = f"{WP_API}/{post_type}"
                    resp = self.http.get(
                        url,
                        params={"per_page": 100, "page": page},
                        timeout=30,
                    )
                    if resp.status_code != 200:
                        break
                    items = resp.json()
                    if not items:
                        break

                    for item in items:
                        wp_id = item.get("id", 0)
                        title = _clean_title(item.get("title", {}).get("rendered", ""))
                        if not title:
                            continue

                        all_posts.append({
                            "wp_id": wp_id,
                            "post_type": post_type,
                            "title": title,
                            "date": _extract_date(item.get("date", "")),
                            "link": item.get("link", ""),
                            "content": item.get("content", {}).get("rendered", ""),
                        })

                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    if page >= total_pages:
                        break
                    page += 1
                    time.sleep(1.0)
                except Exception as e:
                    logger.warning("API error %s page %d: %s", post_type, page, e)
                    break

            logger.info("Discovered %d posts of type '%s'",
                        sum(1 for p in all_posts if p["post_type"] == post_type),
                        post_type)

        logger.info("Total posts discovered: %d", len(all_posts))
        return all_posts

    def _get_pdf_attachments(self, wp_id: int) -> List[Dict[str, str]]:
        """Fetch PDF attachments for a given post."""
        try:
            url = f"{WP_API}/media"
            resp = self.http.get(
                url,
                params={"parent": wp_id, "mime_type": "application/pdf", "per_page": 20},
                timeout=30,
            )
            if resp.status_code != 200:
                return []
            items = resp.json()
            results = []
            for item in items:
                source_url = item.get("source_url", "")
                if source_url:
                    results.append({
                        "media_id": item.get("id", 0),
                        "source_url": source_url,
                        "title": _clean_title(item.get("title", {}).get("rendered", "")),
                    })
            return results
        except Exception as e:
            logger.warning("Failed to fetch attachments for post %d: %s", wp_id, e)
            return []

    def _download_and_extract(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        if pdf_url.startswith("/"):
            pdf_url = f"{BASE_URL}{pdf_url}"
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("GH/BOG-Regulations", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def _strip_html(self, html_str: str) -> str:
        """Strip HTML tags from content."""
        text = re.sub(r"<[^>]+>", " ", html_str)
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        return text

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BOG regulatory documents with full text."""
        all_posts = self._discover_posts()
        logger.info("Total posts to process: %d", len(all_posts))

        for post in all_posts:
            wp_id = post["wp_id"]
            title = post["title"]
            logger.info("Processing: [%s] %s", post["post_type"], title[:80])

            # Try PDF attachments first
            attachments = self._get_pdf_attachments(wp_id)
            time.sleep(1.0)

            if attachments:
                for att in attachments:
                    doc_id = _make_id(post["post_type"], wp_id, att["title"] or title)
                    text = self._download_and_extract(att["source_url"], doc_id)
                    if not text or len(text.strip()) < 50:
                        logger.warning("Insufficient text for %s, skipping", doc_id)
                        continue

                    yield {
                        "_id": doc_id,
                        "wp_id": wp_id,
                        "post_type": post["post_type"],
                        "title": att["title"] or title,
                        "date": post["date"],
                        "link": post["link"],
                        "pdf_url": att["source_url"],
                        "text": text,
                    }
                    time.sleep(DELAY)
            else:
                # Fall back to post content if no PDF attachment
                content_text = self._strip_html(post.get("content", ""))
                if content_text and len(content_text) >= 50:
                    doc_id = _make_id(post["post_type"], wp_id, title)
                    yield {
                        "_id": doc_id,
                        "wp_id": wp_id,
                        "post_type": post["post_type"],
                        "title": title,
                        "date": post["date"],
                        "link": post["link"],
                        "pdf_url": "",
                        "text": content_text,
                    }
                else:
                    logger.warning("No PDF attachment or content for post %d: %s", wp_id, title)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        """Fetch updates — re-fetch all for this collection."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "GH/BOG-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "doc_type": raw.get("post_type", ""),
            "url": raw.get("pdf_url") or raw.get("link", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GH/BOG-Regulations bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = BOGRegulationsScraper()

    if args.command == "test":
        posts = scraper._discover_posts()
        print(f"OK — found {len(posts)} regulatory posts")
        types = {}
        for p in posts:
            t = p["post_type"]
            types[t] = types.get(t, 0) + 1
        for t, c in sorted(types.items()):
            print(f"  {t}: {c}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
