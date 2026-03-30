#!/usr/bin/env python3
"""
SR/WettenSR -- Suriname Legislation Fetcher (wetten.sr)

Fetches Suriname legislation from the WordPress REST API at wetten.sr.

Strategy:
  - Fetch law taxonomy (wet2) for law names
  - Paginate through all articles via /wp-json/wp/v2/artikel
  - Extract full text from content.rendered HTML
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
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SR.WettenSR")

API_BASE = "https://wetten.sr/wp-json/wp/v2"


class WettenSRScraper(BaseScraper):
    """Scraper for SR/WettenSR -- Suriname legislation via WordPress REST API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._law_names: Dict[int, str] = {}
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

    def _load_law_names(self):
        """Load wet2 taxonomy to map IDs to law names."""
        if self._law_names:
            return
        page = 1
        while True:
            data = self._get_json(f"{API_BASE}/wet2?per_page=100&page={page}")
            if not data:
                break
            for item in data:
                self._law_names[item["id"]] = htmlmod.unescape(item.get("name", ""))
            if len(data) < 100:
                break
            page += 1
            time.sleep(1)
        logger.info(f"Loaded {len(self._law_names)} law names")

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

    def _get_law_name(self, wet2_ids: List[int]) -> str:
        """Get law name from wet2 taxonomy IDs."""
        for wid in wet2_ids:
            name = self._law_names.get(wid)
            if name:
                return name
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title_html = raw.get("title", {}).get("rendered", "")
        title = htmlmod.unescape(re.sub(r'<[^>]+>', '', title_html)).strip()

        content_html = raw.get("content", {}).get("rendered", "")
        text = self._clean_html(content_html)

        wet2_ids = raw.get("wet2", [])
        law_name = self._get_law_name(wet2_ids)

        # Use modified date as primary date
        date_str = raw.get("modified", raw.get("date", ""))
        date = date_str[:10] if date_str else ""

        wp_id = raw.get("id", 0)
        slug = raw.get("slug", "")
        link = raw.get("link", f"https://wetten.sr/?p={wp_id}")

        # Build a more descriptive title
        full_title = f"{law_name} - {title}" if law_name and title else title or law_name

        return {
            "_id": f"SR-WettenSR-{wp_id}",
            "_source": "SR/WettenSR",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": text,
            "date": date,
            "url": link,
            "wp_id": wp_id,
            "law_name": law_name,
            "slug": slug,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all articles via paginated WP REST API."""
        self._load_law_names()

        page = 1
        count = 0
        while True:
            time.sleep(1)
            data = self._get_json(f"{API_BASE}/artikel?per_page=100&page={page}")
            if not data:
                break

            for article in data:
                record = self.normalize(article)
                if not record["text"]:
                    logger.warning(f"Empty text for WP ID {article.get('id')}")
                    continue
                count += 1
                yield record

            logger.info(f"Page {page}: {len(data)} articles (total: {count})")
            if len(data) < 100:
                break
            page += 1

        logger.info(f"Completed: {count} articles fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch articles modified since a date."""
        self._load_law_names()
        if not since:
            yield from self.fetch_all()
            return

        page = 1
        count = 0
        while True:
            time.sleep(1)
            url = f"{API_BASE}/artikel?per_page=100&page={page}&modified_after={since}T00:00:00&orderby=modified"
            data = self._get_json(url)
            if not data:
                break
            for article in data:
                record = self.normalize(article)
                if record["text"]:
                    count += 1
                    yield record
            if len(data) < 100:
                break
            page += 1

        logger.info(f"Updates: {count} articles modified since {since}")

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test law taxonomy
        data = self._get_json(f"{API_BASE}/wet2?per_page=5")
        if not data:
            logger.error("Could not fetch law taxonomy")
            return False
        logger.info(f"Law taxonomy OK: {len(data)} items")

        # Test articles endpoint
        data = self._get_json(f"{API_BASE}/artikel?per_page=3")
        if not data:
            logger.error("Could not fetch articles")
            return False
        logger.info(f"Articles OK: {len(data)} items")

        # Check first article has content
        if data:
            content = data[0].get("content", {}).get("rendered", "")
            text = self._clean_html(content)
            logger.info(f"First article text: {len(text)} chars")
            if len(text) < 10:
                logger.error("Article has no meaningful text")
                return False

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SR/WettenSR data fetcher")
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
    args = parser.parse_args()

    scraper = WettenSRScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
