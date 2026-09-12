#!/usr/bin/env python3
"""
GY/GRA-TaxPolicy -- Guyana Revenue Authority Tax Policies

Fetches tax policy documents from gra.gov.gy via the WordPress REST API
(category 12 = "Tax Policies"). Posts contain full text HTML content.

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GY.GRA-TaxPolicy")

BASE_URL = "https://www.gra.gov.gy"
WP_API = f"{BASE_URL}/wp-json/wp/v2"
CATEGORY_ID = 12  # "Tax Policies"
DELAY = 2.0


def _make_id(wp_id: int, title: str) -> str:
    """Generate a stable ID from WP ID and title."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", title).strip("_")
    if len(slug) > 80:
        slug = slug[:80]
    return f"GRA_TaxPolicy_{wp_id}_{slug}"


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


def _strip_html(html_str: str) -> str:
    """Strip HTML tags, WP shortcodes, and clean whitespace."""
    text = re.sub(r"\[/?vc_[^\]]*\]", "", html_str)
    text = re.sub(r"\[/?et_[^\]]*\]", "", text)
    text = re.sub(r"\[/?fusion_[^\]]*\]", "", text)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"</?(p|div|li|tr|td|th|h[1-6])[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class GRATaxPolicyScraper(BaseScraper):
    """Scraper for Guyana Revenue Authority tax policies."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
            },
        )

    def _fetch_posts(self) -> List[Dict[str, Any]]:
        """Fetch all tax policy posts via WP REST API."""
        all_posts = []
        page = 1

        while page <= 20:
            try:
                resp = self.http.get(
                    f"{WP_API}/posts",
                    params={
                        "categories": CATEGORY_ID,
                        "per_page": 100,
                        "page": page,
                    },
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

                    content_html = item.get("content", {}).get("rendered", "")
                    content_text = _strip_html(content_html)

                    all_posts.append({
                        "wp_id": wp_id,
                        "title": title,
                        "date": _extract_date(item.get("date", "")),
                        "link": item.get("link", ""),
                        "text": content_text,
                    })

                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break
                page += 1
                time.sleep(1.0)
            except Exception as e:
                logger.warning("API error page %d: %s", page, e)
                break

        logger.info("Fetched %d tax policy posts", len(all_posts))
        return all_posts

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all tax policy documents with full text."""
        posts = self._fetch_posts()

        for post in posts:
            if not post["text"] or len(post["text"]) < 50:
                logger.warning("Insufficient text for post %d: %s",
                               post["wp_id"], post["title"][:60])
                continue

            post["_id"] = _make_id(post["wp_id"], post["title"])
            yield post
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        """Fetch updates — re-fetch all for this small collection."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "GY/GRA-TaxPolicy",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("link", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GY/GRA-TaxPolicy bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = GRATaxPolicyScraper()

    if args.command == "test":
        posts = scraper._fetch_posts()
        print(f"OK — found {len(posts)} tax policy posts")
        for p in posts[:5]:
            print(f"  [{p['date']}] {p['title'][:70]} ({len(p['text'])} chars)")
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
