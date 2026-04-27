#!/usr/bin/env python3
"""
KE/KRA-TaxCirculars -- Kenya Revenue Authority Public Notices

Fetches official tax administration notices from kra.go.ke.

Strategy:
  - GET the public notices listing page (all notices on one page)
  - Extract notice links and dates
  - For each notice, GET the detail page and extract text from div.blog-content
  - Clean HTML, extract full text

Data:
  - ~280 notices spanning 2018-2026
  - Topics: VAT, income tax, customs, excise, PAYE, tax compliance,
    system updates, regulatory changes
  - English language

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KE.KRA-TaxCirculars")

BASE_URL = "https://www.kra.go.ke"
LISTING_URL = f"{BASE_URL}/news-center/public-notices"


class KRAScraper(BaseScraper):
    """Scraper for KE/KRA-TaxCirculars -- Kenya Revenue Authority notices."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _get_notice_links(self) -> List[Tuple[str, str, str]]:
        """Get all notice (url, title, date) tuples from the listing page."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        notices = []
        seen = set()
        # Find all media-block anchor tags
        for block in soup.find_all("a", class_="media-block"):
            href = block.get("href", "")
            if "/public-notices/" not in href:
                continue
            # Deduplicate
            if href in seen:
                continue
            seen.add(href)

            text = block.get_text(strip=True)
            # Extract date (DD/MM/YYYY) from the block text
            m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
            date_str = m.group(1) if m else ""
            # Title is everything after the date
            title = re.sub(r"^.*?\d{2}/\d{2}/\d{4}", "", text).strip()
            if not title:
                title = text

            url = href if href.startswith("http") else f"{BASE_URL}{href}"
            notices.append((url, title, date_str))

        logger.info(f"Found {len(notices)} notices on listing page")
        return notices

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string like '23/04/2026' to ISO format."""
        date_str = date_str.strip()
        for fmt in ("%d/%m/%Y", "%d %B %Y", "%B %d, %Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _fetch_notice_text(self, url: str) -> Tuple[str, str, str]:
        """Fetch a notice page and extract title, text, and date."""
        time.sleep(self.config.get("fetch", {}).get("rate_limit", 2.0))
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract title from page
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True).split("|")[0].strip()

        # Extract date from page
        date_str = ""
        date_elem = soup.find("time") or soup.find("span", class_="create")
        if date_elem:
            date_str = date_elem.get_text(strip=True)

        # Extract content from blog-content div
        content_div = soup.find("div", class_="blog-content")
        if not content_div:
            content_div = soup.find("div", class_="item-page")
        if not content_div:
            content_div = soup.find("article")

        text = ""
        if content_div:
            # Remove scripts and styles
            for tag in content_div.find_all(["script", "style"]):
                tag.decompose()
            text = content_div.get_text(separator="\n", strip=True)

        return title, text, date_str

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all public notices."""
        notices = self._get_notice_links()
        for url, list_title, list_date in notices:
            try:
                page_title, text, page_date = self._fetch_notice_text(url)
                title = page_title or list_title
                date_iso = self._parse_date(page_date) or self._parse_date(list_date)

                if not text or len(text) < 20:
                    logger.warning(f"Skipping {url}: insufficient text ({len(text)} chars)")
                    continue

                # Extract numeric ID from URL
                m = re.search(r'/(\d+)-', url)
                notice_id = m.group(1) if m else str(hash(url))

                yield {
                    "_id": notice_id,
                    "_url": url,
                    "_title": title,
                    "_text": text,
                    "_date": date_iso,
                }
            except requests.RequestException as e:
                logger.error(f"Error fetching {url}: {e}")
                continue

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent notices."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw notice data into standard schema."""
        return {
            "_id": f"KE-KRA-{raw['_id']}",
            "_source": "KE/KRA-TaxCirculars",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["_title"],
            "text": raw["_text"],
            "date": raw.get("_date"),
            "url": raw["_url"],
            "language": "en",
            "authority": "Kenya Revenue Authority (KRA)",
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="KE/KRA-TaxCirculars Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KRAScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
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
