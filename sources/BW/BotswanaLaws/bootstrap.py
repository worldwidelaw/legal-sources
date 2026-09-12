#!/usr/bin/env python3
"""
BW/BotswanaLaws -- Botswana Consolidated Legislation

Fetches consolidated principal acts from botswanalaws.com.

Strategy:
  - Paginate the principal legislation listing (?start=0, 50, 100, ...)
  - Extract individual act URLs from each listing page
  - Fetch each act page and extract full text from the item-page div
  - Clean HTML tags and normalize

Data Coverage:
  - ~354 principal acts (consolidated)
  - Full text in HTML format

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BW.BotswanaLaws")

BASE_URL = "https://botswanalaws.com"
LISTING_URL = f"{BASE_URL}/consolidated-statutes/principle-legislation"
PAGE_SIZE = 50


class BotswanaLawsScraper(BaseScraper):
    """Scraper for Botswana consolidated legislation."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html",
        })

    def _list_all_acts(self) -> list[str]:
        """Paginate the listing page to get all act URLs."""
        all_urls = []
        start = 0

        while True:
            url = LISTING_URL
            params = {"start": start} if start > 0 else {}
            time.sleep(2)

            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Error fetching listing page start={start}: {e}")
                break

            links = re.findall(
                r'href="(/consolidated-statutes/principle-legislation/[a-z][\w-]*)"',
                resp.text,
            )

            if not links:
                break

            new_count = 0
            for link in links:
                full_url = f"{BASE_URL}{link}"
                if full_url not in all_urls:
                    all_urls.append(full_url)
                    new_count += 1

            logger.info(f"Listing start={start}: {new_count} new acts (total {len(all_urls)})")

            if new_count == 0:
                break

            start += PAGE_SIZE

        return all_urls

    def _fetch_act(self, url: str) -> Optional[dict]:
        """Fetch and parse a single act page."""
        time.sleep(2)
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                return None
        except requests.RequestException as e:
            logger.error(f"Error fetching {url}: {e}")
            return None

        html = resp.text

        # Extract title from <title> tag (most reliable)
        title = ""
        title_tag = re.search(r'<title>(.*?)</title>', html)
        if title_tag:
            title = unescape(title_tag.group(1)).strip()

        # Extract main content from item-page div
        # Pattern: <div class="item-page" ...> ... </div>
        content = ""
        item_page = re.search(
            r'<div[^>]*class="[^"]*item-page[^"]*"[^>]*>(.*?)(?=<div[^>]*class="[^"]*afterDisplayContent|<footer|<aside)',
            html, re.DOTALL
        )
        if item_page:
            content = item_page.group(1)
        else:
            # Fallback: look for article tag
            article = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)
            if article:
                content = article.group(1)

        if not content:
            return None

        # Clean HTML to plain text
        text = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</p>', '\n\n', text)
        text = re.sub(r'</div>', '\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        # Extract chapter number from text
        chapter_match = re.search(r'CHAPTER\s+([\d:]+)', text)
        chapter = chapter_match.group(1) if chapter_match else ""

        # Extract slug from URL
        slug = url.rstrip("/").split("/")[-1]

        return {
            "slug": slug,
            "title": title or slug.replace("-", " ").title(),
            "text": text,
            "chapter": chapter,
            "url": url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all acts."""
        urls = self._list_all_acts()
        logger.info(f"Total acts to fetch: {len(urls)}")

        for i, url in enumerate(urls):
            try:
                logger.info(f"[{i+1}/{len(urls)}] Fetching {url.split('/')[-1]}")
                act = self._fetch_act(url)
                if act and act.get("text") and len(act["text"]) > 100:
                    yield act
                else:
                    logger.warning(f"  No/insufficient text for {url}")
            except Exception as e:
                logger.error(f"  Error: {e}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all acts (no date filtering available)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw act data into standard schema."""
        slug = raw.get("slug", "unknown")

        return {
            "_id": f"bw-law-{slug}",
            "_source": "BW/BotswanaLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("url", ""),
            "chapter": raw.get("chapter", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv
    scraper = BotswanaLawsScraper()

    if command == "test":
        urls = scraper._list_all_acts()
        print(f"Found {len(urls)} acts")
        if urls:
            act = scraper._fetch_act(urls[0])
            if act:
                print(f"Title: {act['title']}")
                print(f"Chapter: {act['chapter']}")
                print(f"Text length: {len(act.get('text', ''))}")
                print(f"Text preview: {act.get('text', '')[:300]}...")
        sys.exit(0)

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
