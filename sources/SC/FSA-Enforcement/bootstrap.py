#!/usr/bin/env python3
"""
SC/FSA-Enforcement -- Seychelles FSA Regulatory Enforcements

Fetches regulatory enforcement actions from the Seychelles Financial Services
Authority (FSA). Content includes scam alerts, unauthorized activity warnings,
license revocations/terminations, circulars, and public notices.

Strategy:
  - Paginated HTML listing at /media-corner/regulatory-updates?start=N
  - 10 items per page, ~32 pages
  - Individual article pages contain full text inline as HTML
  - Some circulars have PDF attachments

Data Coverage:
  - ~316 regulatory updates from 2019 to present
  - Full text available inline for most items

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SC.FSA-Enforcement")

BASE_URL = "https://fsaseychelles.sc"
LIST_URL = f"{BASE_URL}/media-corner/regulatory-updates"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Extract article links from listing page
ARTICLE_LINK_RE = re.compile(
    r'<a\s+href="(/media-corner/regulatory-updates/[^"]+)"[^>]*>',
    re.IGNORECASE,
)

# Extract date from listing page items (e.g., "15 April, 2026" or "April 15, 2026")
DATE_FORMATS = [
    "%d %B, %Y",   # 15 April, 2026
    "%B %d, %Y",   # April 15, 2026
    "%d %B %Y",    # 15 April 2026
    "%d/%m/%Y",    # 15/04/2026
]


def _clean_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h[1-6]>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def _parse_date(date_str: str) -> str:
    """Parse various date formats to ISO 8601."""
    date_str = date_str.strip()
    for fmt in DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


class FSAEnforcementScraper(BaseScraper):
    """
    Scraper for SC/FSA-Enforcement -- Seychelles FSA regulatory updates.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.7",
            "Accept-Language": "en,fr;q=0.9",
        })

    def _fetch_page(self, url: str, timeout: int = 30) -> str:
        """Fetch an HTML page with rate limiting and error handling."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=timeout)
            if resp.status_code == 500:
                logger.warning(f"HTTP 500 for {url}, retrying once...")
                self.rate_limiter.wait()
                resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

    def _get_article_slugs(self, html_content: str) -> List[str]:
        """Extract article URL paths from a listing page."""
        matches = ARTICLE_LINK_RE.findall(html_content)
        # Deduplicate while preserving order
        seen = set()
        slugs = []
        for path in matches:
            if path not in seen:
                seen.add(path)
                slugs.append(path)
        return slugs

    def _parse_article(self, url: str, html_content: str) -> Optional[Dict[str, Any]]:
        """Parse an individual article page for metadata and full text."""
        # Extract title from h2[itemprop="headline"] (not h1 which is category name)
        title = ""
        title_m = re.search(
            r'<h2[^>]*itemprop="headline"[^>]*>(.*?)</h2>',
            html_content, re.DOTALL | re.IGNORECASE,
        )
        if not title_m:
            # Fallback to og:title meta tag
            title_m = re.search(
                r'<meta\s+property="og:title"\s+content="([^"]+)"',
                html_content, re.IGNORECASE,
            )
            if title_m:
                title = html_module.unescape(title_m.group(1)).strip()
        if title_m and not title:
            title = _clean_html(title_m.group(1)).strip()

        # Extract date - look for common patterns
        date_iso = ""
        # Try <time> tag first
        time_m = re.search(r'<time[^>]*datetime="([^"]+)"', html_content)
        if time_m:
            date_iso = time_m.group(1)[:10]
        else:
            # Look for date patterns in text near article header
            date_m = re.search(
                r'(?:Published|Posted|Date)[:\s]*(\d{1,2}\s+\w+[,\s]+\d{4})',
                html_content, re.IGNORECASE,
            )
            if date_m:
                date_iso = _parse_date(date_m.group(1))
            else:
                # Try finding dates in dd/mm/yyyy format
                date_m2 = re.search(r'(\d{1,2}/\d{1,2}/\d{4})', html_content)
                if date_m2:
                    date_iso = _parse_date(date_m2.group(1))

        # Extract article body from div[itemprop="articleBody"]
        # Content ends at the article-info div (date section)
        body = ""
        m = re.search(
            r'<div\s+itemprop="articleBody"[^>]*>(.*?)</div>\s*<div\s+class="article-info',
            html_content, re.DOTALL | re.IGNORECASE,
        )
        if not m:
            # Fallback: grab everything after articleBody opening tag
            m = re.search(
                r'<div\s+itemprop="articleBody"[^>]*>(.*)',
                html_content, re.DOTALL | re.IGNORECASE,
            )
        if not m:
            m = re.search(r'<article[^>]*>(.*?)</article>', html_content, re.DOTALL)
        if m:
            body = m.group(1)

        if not body:
            logger.warning(f"Could not extract body from {url}")
            return None

        text = _clean_html(body)

        if len(text) < 20:
            logger.warning(f"Insufficient text for {url}: {len(text)} chars")
            return None

        # Generate ID from slug
        slug = url.rstrip("/").split("/")[-1]
        doc_id = slug[:120]

        return {
            "doc_id": doc_id,
            "title": title or slug,
            "text": text,
            "date": date_iso,
            "url": urljoin(BASE_URL, url),
            "slug": slug,
        }

    def _crawl_listing(self) -> Generator[Dict[str, Any], None, None]:
        """Crawl all paginated listing pages and yield article data."""
        start = 0
        consecutive_empty = 0

        while True:
            url = f"{LIST_URL}?start={start}" if start > 0 else LIST_URL
            logger.info(f"Fetching listing page start={start}")

            html = self._fetch_page(url)
            if not html:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                start += 10
                continue

            slugs = self._get_article_slugs(html)
            if not slugs:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                start += 10
                continue

            consecutive_empty = 0
            logger.info(f"Found {len(slugs)} articles on page start={start}")

            for slug_path in slugs:
                article_url = urljoin(BASE_URL, slug_path)
                article_html = self._fetch_page(article_url)
                if not article_html:
                    continue

                article = self._parse_article(slug_path, article_html)
                if article:
                    yield article

            start += 10

            if start > 400:  # safety limit (~40 pages max)
                break

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all enforcement documents."""
        logger.info("Starting FSA Enforcement crawl...")
        yield from self._crawl_listing()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recent enforcement documents (first few pages only)."""
        logger.info(f"Fetching updates since {since.isoformat()}...")
        since_date = since.date()

        for start in range(0, 50, 10):  # Check first 5 pages
            url = f"{LIST_URL}?start={start}" if start > 0 else LIST_URL
            html = self._fetch_page(url)
            if not html:
                break

            slugs = self._get_article_slugs(html)
            if not slugs:
                break

            for slug_path in slugs:
                article_url = urljoin(BASE_URL, slug_path)
                article_html = self._fetch_page(article_url)
                if not article_html:
                    continue

                article = self._parse_article(slug_path, article_html)
                if article:
                    if article["date"]:
                        try:
                            art_date = datetime.strptime(article["date"], "%Y-%m-%d").date()
                            if art_date < since_date:
                                return
                        except ValueError:
                            pass
                    yield article

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "SC/FSA-Enforcement",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "language": "en",
        }

    def test_api(self):
        """Quick connectivity test."""
        print("Testing FSA Enforcement site...")

        html = self._fetch_page(LIST_URL)
        if not html:
            print("ERROR: Could not fetch listing page")
            return

        slugs = self._get_article_slugs(html)
        print(f"Articles on page 1: {len(slugs)}")

        if slugs:
            print(f"\nFirst article: {slugs[0]}")
            article_html = self._fetch_page(urljoin(BASE_URL, slugs[0]))
            if article_html:
                article = self._parse_article(slugs[0], article_html)
                if article:
                    print(f"  Title: {article['title'][:80]}")
                    print(f"  Date: {article['date']}")
                    print(f"  Text: {len(article['text'])} chars")
                    print(f"  Preview: {article['text'][:200]}...")
                else:
                    print("  ERROR: Could not parse article")

        # Check last page
        html_last = self._fetch_page(f"{LIST_URL}?start=310")
        if html_last:
            slugs_last = self._get_article_slugs(html_last)
            print(f"\nArticles on last page (start=310): {len(slugs_last)}")

        print("\nTest complete!")


def main():
    scraper = FSAEnforcementScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
