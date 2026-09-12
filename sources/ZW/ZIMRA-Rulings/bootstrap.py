#!/usr/bin/env python3
"""
ZW/ZIMRA-Rulings — Zimbabwe Revenue Authority Tax Guidance & Rulings

Fetches "Taxman's Corner" doctrine articles from the ZIMRA website.
These are official tax guidance publications covering topics like CGT,
VAT, partnerships, employer obligations, customs procedures, etc.

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Tuple, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZW.ZIMRA-Rulings")

SOURCE_ID = "ZW/ZIMRA-Rulings"
BASE_URL = "https://www.zimra.co.zw"
LIST_URL = BASE_URL + "/news/22-taxmans-corner"
PAGE_SIZE = 9  # Joomla default


def _extract_article_links(html: str) -> List[Tuple[str, str]]:
    """Extract article title and URL pairs from a listing page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    articles = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        text = a.get_text(strip=True)
        if "/news/" in href and ":" in href and text and len(text) > 5:
            if "taxmans-corner" in href.lower():
                continue
            if not href.startswith("http"):
                href = BASE_URL + href
            articles.append((text, href))
    return articles


def _extract_article_content(html: str) -> Tuple[str, Optional[str]]:
    """Extract article body text and publication date from an article page."""
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # Find article body
    body = (
        soup.find("div", {"itemprop": "articleBody"})
        or soup.find("div", class_="item-page")
        or soup.find("article")
    )
    text = ""
    if body:
        # Remove script/style tags
        for tag in body.find_all(["script", "style", "nav"]):
            tag.decompose()
        text = body.get_text(separator="\n", strip=True)

    # Try to extract date
    date_str = None
    # Look for itemprop datePublished
    date_el = soup.find(attrs={"itemprop": "datePublished"})
    if date_el:
        date_str = date_el.get("content") or date_el.get_text(strip=True)

    # Look for date in text like "published on DD/MM/YYYY" or "25/09/2025"
    if not date_str and text:
        m = re.search(r'published\s+(?:on\s+)?(\d{1,2})[/.](\d{1,2})[/.](\d{4})', text, re.IGNORECASE)
        if m:
            day, month, year = m.group(1), m.group(2), m.group(3)
            try:
                date_str = f"{year}-{int(month):02d}-{int(day):02d}"
            except ValueError:
                pass

    # Also try "Published: Month Day, Year" pattern
    if not date_str:
        time_el = soup.find("time")
        if time_el:
            date_str = time_el.get("datetime") or time_el.get_text(strip=True)

    # Normalize date to ISO format
    if date_str and not re.match(r'\d{4}-\d{2}-\d{2}', date_str):
        for fmt in ["%d %B %Y", "%d/%m/%Y", "%B %d, %Y", "%d %b %Y"]:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                date_str = dt.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        else:
            date_str = None

    return text, date_str


class ZIMRARulingsScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        super().__init__(source_dir=str(Path(__file__).parent))
        self.http = HttpClient(base_url=BASE_URL)

    def test_api(self):
        """Quick connectivity check."""
        resp = self.http.get(LIST_URL, timeout=30)
        logger.info(f"Taxman's Corner listing: {resp.status_code}")
        return resp.status_code == 200

    def _enumerate_articles(self) -> List[Tuple[str, str]]:
        """Enumerate all Taxman's Corner articles across pages."""
        all_articles = []
        seen_urls = set()

        for start in range(0, 500, PAGE_SIZE):
            url = f"{LIST_URL}?start={start}" if start > 0 else LIST_URL
            resp = self.http.get(url, timeout=30)
            if resp.status_code != 200:
                break

            articles = _extract_article_links(resp.text)
            new_articles = []
            for title, href in articles:
                if href not in seen_urls:
                    seen_urls.add(href)
                    new_articles.append((title, href))

            if not new_articles:
                break

            all_articles.extend(new_articles)
            logger.info(f"Page start={start}: {len(new_articles)} new articles (total: {len(all_articles)})")
            time.sleep(1)

        return all_articles

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all Taxman's Corner articles."""
        articles = self._enumerate_articles()
        logger.info(f"Found {len(articles)} articles to fetch")

        if sample:
            articles = articles[:15]

        for i, (title, url) in enumerate(articles):
            try:
                resp = self.http.get(url, timeout=30)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                    continue

                text, date = _extract_article_content(resp.text)
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text ({len(text)} chars) for: {title}")
                    continue

                # Generate stable ID from URL slug
                slug = url.rstrip("/").split("/")[-1]
                _id = f"zimra-{slug}"

                yield {
                    "_id": _id,
                    "_source": SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": title,
                    "text": text,
                    "date": date,
                    "url": url,
                    "category": "Taxman's Corner",
                    "jurisdiction": "ZW",
                    "authority": "Zimbabwe Revenue Authority",
                }

                if (i + 1) % 10 == 0:
                    logger.info(f"Fetched {i + 1}/{len(articles)} articles")
                time.sleep(1.5)

            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                continue

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Re-fetch all — no incremental endpoint available."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Records are already normalized during fetching."""
        return raw


# --------------- CLI ---------------
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ZIMRARulingsScraper()
    args = sys.argv[1:]

    if not args or args[0] == "test-api":
        ok = scraper.test_api()
        print("API OK" if ok else "API FAILED")
        sys.exit(0 if ok else 1)

    if args[0] == "bootstrap":
        sample = "--sample" in args
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        total_text_len = 0
        for record in scraper.fetch_all(sample=sample):
            normalized = scraper.normalize(record)
            text_len = len(normalized.get("text", ""))
            total_text_len += text_len

            if sample or count < 15:
                out_path = sample_dir / f"{normalized['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

            count += 1

        avg_text = total_text_len // max(count, 1)
        logger.info(f"Done: {count} records, avg text length {avg_text} chars")
        print(f"Records: {count}")
        print(f"Avg text: {avg_text} chars")
        if sample:
            print(f"Samples saved to: {sample_dir}")
