#!/usr/bin/env python3
"""
SY/Legislation -- Syrian Presidential Decrees (SANA) Fetcher

Fetches presidential decrees from the Syrian Arab News Agency (sana.sy).
Decrees contain full legal text in Arabic with numbered articles.

Strategy:
  - Search SANA for "مرسوم" (decree) in the presidency category
  - Paginate through search results (12 per page)
  - Fetch each decree page and extract full text from entry-content div

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import hashlib
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SY.Legislation")

BASE_URL = "https://sana.sy"

# Search for مرسوم (decree) in presidency category
SEARCH_URL_TEMPLATE = BASE_URL + "/?s=%D9%85%D8%B1%D8%B3%D9%88%D9%85&category_name=presidency&paged={page}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ar,en;q=0.5",
}


class SyriaLegislationScraper(BaseScraper):
    """Scraper for SY/Legislation -- Syrian presidential decrees."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code in (404, 410):
                    logger.warning(f"Not found: {url[:80]}")
                    return None
                if resp.status_code == 503:
                    logger.warning(f"503 Service Unavailable: {url[:80]}")
                    if attempt < 2:
                        time.sleep(10)
                        continue
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _parse_search_results(self, page: int) -> list:
        """Parse a search results page and return article URLs and metadata."""
        url = SEARCH_URL_TEMPLATE.format(page=page)
        resp = self._request(url)
        if resp is None:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = []
        seen = set()

        # Find article links - they follow pattern /presidency/NNNNNN/ or /education/NNNNNN/ etc.
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href.startswith("https://sana.sy/"):
                continue

            # Match decree article URLs (post ID pattern)
            m = re.match(r'https://sana\.sy/\w+/(\d+)/?$', href)
            if not m:
                continue

            post_id = m.group(1)
            if post_id in seen:
                continue

            # Get title from link text or parent
            title = a.get_text(strip=True)
            if not title or len(title) < 10:
                continue

            # Must contain مرسوم (decree) in title
            if "مرسوم" not in title:
                continue

            seen.add(post_id)
            articles.append({
                "url": href.rstrip("/") + "/",
                "post_id": post_id,
                "title": title,
            })

        return articles

    def _extract_decree_text(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a decree page and extract full text."""
        resp = self._request(url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Get title
        title = ""
        title_el = soup.find("h1", class_="entry-title") or soup.find("h1")
        if title_el:
            title = title_el.get_text(strip=True)
        if not title:
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True)

        # Get content from entry-content div
        content_div = soup.find("div", class_="entry-content")
        if not content_div:
            # Fallback: try article tag
            content_div = soup.find("article")

        if not content_div:
            logger.warning(f"No content div found: {url[:80]}")
            return None

        # Remove scripts, styles, social sharing, etc.
        for tag in content_div.find_all(["script", "style", "iframe", "noscript"]):
            tag.decompose()

        text = content_div.get_text(separator="\n", strip=True)

        # Clean up
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        text = text.strip()

        if len(text) < 200:
            logger.warning(f"Insufficient text ({len(text)} chars): {title[:60]}")
            return None

        # Extract date from the page
        date = None
        time_el = soup.find("time", attrs={"datetime": True})
        if time_el:
            dt_str = time_el["datetime"]
            m = re.match(r"(\d{4}-\d{2}-\d{2})", dt_str)
            if m:
                date = m.group(1)

        # Extract post ID from URL
        m = re.search(r'/(\d+)/?$', url.rstrip("/"))
        post_id = m.group(1) if m else hashlib.md5(url.encode()).hexdigest()[:12]

        return {
            "document_id": f"SY-LEG-{post_id}",
            "title": title,
            "text": text,
            "url": url,
            "date": date,
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("document_id", ""),
            "_source": "SY/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decree documents by paginating through search results."""
        count = 0
        seen_ids = set()
        max_pages = 15  # Safety limit

        for page in range(1, max_pages + 1):
            logger.info(f"Fetching search page {page}")
            articles = self._parse_search_results(page)

            if not articles:
                logger.info(f"No more results at page {page}")
                break

            for article in articles:
                if article["post_id"] in seen_ids:
                    continue
                seen_ids.add(article["post_id"])

                doc = self._extract_decree_text(article["url"])
                if doc is None:
                    continue

                count += 1
                yield doc

        logger.info(f"Completed: {count} decrees fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decrees."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        articles = self._parse_search_results(1)
        if not articles:
            logger.error("No decree articles found in search")
            return False

        logger.info(f"Search page 1: {len(articles)} decree articles")

        doc = self._extract_decree_text(articles[0]["url"])
        if doc and len(doc.get("text", "")) > 200:
            logger.info(f"Decree OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
            return True
        else:
            logger.error("Failed to extract decree text")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SY/Legislation data fetcher")
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

    scraper = SyriaLegislationScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records -- {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
