#!/usr/bin/env python3
"""
SR/SupremeCourt -- Suriname Court Decisions Fetcher (rechtspraak.sr)

Fetches Suriname court decisions from the WordPress REST API at rechtspraak.sr.

Strategy:
  - Paginate through all posts via /wp-json/wp/v2/posts
  - Extract full text from content.rendered HTML
  - Parse court type from SRU prefix in title
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import html as htmlmod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SR.SupremeCourt")

API_BASE = "https://rechtspraak.sr/wp-json/wp/v2"

# Map SRU prefixes to court names
COURT_MAP = {
    "SRU-HvJ": "Hof van Justitie",
    "SRU-K1": "Kantongerecht Eerste Kanton",
    "SRU-K2": "Kantongerecht Tweede Kanton",
    "SRU-K3": "Kantongerecht Derde Kanton",
    "SRU-RC": "Rechter-Commissaris",
    "SRU-ATC": "Advocaten Tuchtcollege",
    "SRU-MTC": "Medisch Tuchtcollege",
    "SRU-AA": "Ambtenaren Appèl",
}


class SupremeCourtScraper(BaseScraper):
    """Scraper for SR/SupremeCourt -- Suriname court decisions via WordPress REST API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=60)
        except ImportError:
            self.client = None

    def _http_get(self, url: str) -> Optional[str]:
        """HTTP GET returning response text."""
        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.get(url)
                    if resp.status_code == 200:
                        return resp.text
                    if resp.status_code in (400, 404, 500):
                        return None
                    logger.warning(f"HTTP {resp.status_code} for {url[:100]}")
                else:
                    import urllib.request
                    req = urllib.request.Request(url, headers={
                        "User-Agent": "Mozilla/5.0 (LegalDataHunter)",
                        "Accept": "application/json",
                    })
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                if "404" in str(e) or "400" in str(e):
                    return None
                logger.warning(f"Attempt {attempt+1} GET failed for {url[:100]}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _get_json(self, url: str) -> Optional[Any]:
        """GET and parse JSON."""
        text = self._http_get(url)
        if not text:
            return None
        try:
            return json.loads(text, strict=False)
        except json.JSONDecodeError:
            return None

    def _clean_html(self, raw_html: str) -> str:
        """Strip HTML tags and clean text."""
        text = re.sub(r'<script[^>]*>.*?</script>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = htmlmod.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _parse_court(self, title: str) -> str:
        """Extract court name from SRU prefix."""
        for prefix, name in COURT_MAP.items():
            if title.startswith(prefix):
                return name
        return "Unknown"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title_html = raw.get("title", {}).get("rendered", "")
        title = htmlmod.unescape(re.sub(r'<[^>]+>', '', title_html)).strip()

        content_html = raw.get("content", {}).get("rendered", "")
        text = self._clean_html(content_html)

        date_str = raw.get("date", "")
        date = date_str[:10] if date_str else ""

        wp_id = raw.get("id", 0)
        slug = raw.get("slug", "")
        link = raw.get("link", f"https://rechtspraak.sr/?p={wp_id}")
        court = self._parse_court(title)

        return {
            "_id": f"SR-SupremeCourt-{wp_id}",
            "_source": "SR/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": link,
            "wp_id": wp_id,
            "court": court,
            "slug": slug,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions via paginated WP REST API."""
        page = 1
        count = 0
        while True:
            time.sleep(1)
            url = f"{API_BASE}/posts?per_page=100&page={page}&_fields=id,title,slug,date,content.rendered,link"
            data = self._get_json(url)
            if not data:
                break

            for post in data:
                record = self.normalize(post)
                if not record["text"]:
                    logger.warning(f"Empty text for WP ID {post.get('id')}")
                    continue
                count += 1
                yield record

            logger.info(f"Page {page}: {len(data)} decisions (total: {count})")
            if len(data) < 100:
                break
            page += 1

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions modified since a date."""
        if not since:
            yield from self.fetch_all()
            return

        page = 1
        count = 0
        while True:
            time.sleep(1)
            url = f"{API_BASE}/posts?per_page=100&page={page}&modified_after={since}T00:00:00&orderby=modified&_fields=id,title,slug,date,content.rendered,link"
            data = self._get_json(url)
            if not data:
                break
            for post in data:
                record = self.normalize(post)
                if record["text"]:
                    count += 1
                    yield record
            if len(data) < 100:
                break
            page += 1

        logger.info(f"Updates: {count} decisions modified since {since}")

    def test(self) -> bool:
        """Quick connectivity test."""
        data = self._get_json(f"{API_BASE}/posts?per_page=3&_fields=id,title,slug,date,content.rendered,link")
        if not data:
            logger.error("Could not fetch posts")
            return False
        logger.info(f"Posts OK: {len(data)} items")

        if data:
            content = data[0].get("content", {}).get("rendered", "")
            text = self._clean_html(content)
            logger.info(f"First decision text: {len(text)} chars")
            if len(text) < 10:
                logger.error("Decision has no meaningful text")
                return False

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SR/SupremeCourt data fetcher")
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

    scraper = SupremeCourtScraper()

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
    main()
