#!/usr/bin/env python3
"""
TN/JORT-Legislation -- Tunisian Legislation Fetcher

Fetches Tunisian legislation from the DCAF Security Sector Governance database
at legislation-securite.tn via WordPress REST API.

Strategy:
  - WordPress REST API exposes custom post type "latest-laws"
  - Endpoint: /wp-json/wp/v2/latest-laws?per_page=100&page=N
  - ~3,954 legal texts, ~80% with French full text
  - Content in HTML rendered field, cleaned to plain text
  - Rich taxonomy: text-type, institution, status, database-index

Endpoints:
  - Base: https://legislation-securite.tn
  - API: /wp-json/wp/v2/latest-laws
  - Params: per_page (max 100), page, orderby, order, search

Data:
  - Tunisian laws, decrees, orders, decisions from 1956-present
  - French and Arabic, managed by DCAF (Geneva Centre for Security Sector Governance)
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TN.JORT-Legislation")

BASE_URL = "https://legislation-securite.tn"
API_ENDPOINT = "/wp-json/wp/v2/latest-laws"
PER_PAGE = 100


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, preserving meaningful whitespace."""
    if not text:
        return ""
    # Decode HTML entities
    text = html.unescape(text)
    # Replace block-level tags with newlines
    text = re.sub(r'<(?:br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


class JORTLegislationScraper(BaseScraper):
    """
    Scraper for TN/JORT-Legislation -- Tunisian Legislation via DCAF WP API.
    Country: TN
    URL: https://legislation-securite.tn
    Data types: legislation
    Auth: none (Open access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _fetch_page(self, page: int, per_page: int = PER_PAGE) -> list:
        """Fetch a single page of posts from the WP REST API."""
        params = {
            "per_page": per_page,
            "page": page,
            "orderby": "date",
            "order": "desc",
        }
        url = f"{API_ENDPOINT}?per_page={per_page}&page={page}&orderby=date&order=desc"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 400:
                # Past last page
                return []
            else:
                logger.warning(f"Page {page}: HTTP {resp.status_code}")
                return []
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            return []

    def _get_total_pages(self) -> int:
        """Get total number of pages from WP headers."""
        url = f"{API_ENDPOINT}?per_page={PER_PAGE}&page=1"
        try:
            resp = self.client.get(url)
            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            total_posts = int(resp.headers.get("X-WP-Total", 0))
            logger.info(f"Total posts: {total_posts}, pages: {total_pages}")
            return total_pages
        except Exception as e:
            logger.error(f"Failed to get total pages: {e}")
            return 1

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation posts from the WordPress API."""
        total_pages = self._get_total_pages()
        for page in range(1, total_pages + 1):
            logger.info(f"Fetching page {page}/{total_pages}...")
            posts = self._fetch_page(page)
            if not posts:
                break
            for post in posts:
                yield post

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield posts modified since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1
        while True:
            url = (
                f"{API_ENDPOINT}?per_page={PER_PAGE}&page={page}"
                f"&orderby=modified&order=desc&modified_after={since_iso}"
            )
            try:
                self.rate_limiter.wait()
                resp = self.client.get(url)
                if resp.status_code != 200:
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

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a WP post into the standard schema."""
        wp_id = raw.get("id")
        title_rendered = raw.get("title", {}).get("rendered", "")
        content_rendered = raw.get("content", {}).get("rendered", "")

        # Clean title
        title = html.unescape(title_rendered).strip()
        if not title:
            return None

        # Clean content to plain text
        text = strip_html(content_rendered)

        # Skip Arabic-only placeholders
        if not text or "uniquement en langue arabe" in text.lower():
            return None

        # Skip if text is too short (just a placeholder)
        if len(text) < 50:
            return None

        # Extract date
        date_str = raw.get("date", "")
        if date_str:
            date_str = date_str[:10]  # YYYY-MM-DD

        # Build URL
        link = raw.get("link", f"{BASE_URL}/?post_type=latest-laws&p={wp_id}")

        return {
            "_id": f"TN/JORT-Legislation/{wp_id}",
            "_source": "TN/JORT-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": link,
            "wp_id": wp_id,
            "slug": raw.get("slug", ""),
            "modified": raw.get("modified", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="TN/JORT-Legislation bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full bootstrap or sample")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--full", action="store_true", help="Full fetch (all pages)")

    upd = sub.add_parser("update", help="Incremental update")
    sub.add_parser("test", help="Connectivity test")

    args = parser.parse_args()

    scraper = JORTLegislationScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        posts = scraper._fetch_page(1, per_page=1)
        if posts:
            logger.info(f"OK: Got post ID {posts[0].get('id')}")
            title = posts[0].get("title", {}).get("rendered", "")
            logger.info(f"Title: {html.unescape(title)}")
        else:
            logger.error("FAILED: No data returned")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        logger.info(f"Bootstrap complete: {stats}")

    elif args.command == "update":
        last_run = scraper.status.get("last_run")
        if last_run:
            since = datetime.fromisoformat(last_run)
        else:
            since = datetime(2025, 1, 1, tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {stats}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
