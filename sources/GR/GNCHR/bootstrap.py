#!/usr/bin/env python3
"""
GR/GNCHR -- Greek National Commission for Human Rights (EEDA)

Fetches GNCHR opinions, positions, and decisions from nchr.gr.

Strategy:
  - Joomla CMS: use ?limit=0 on category page to get all article links
  - Use ?format=raw on individual articles for clean HTML (no template chrome)
  - Extract full text from Schema.org Article markup
  - ~443 documents across 35 thematic subcategories

Endpoints:
  - Listing: https://www.nchr.gr/2020-02-26-05-51-20.html?limit=0
  - Article: https://www.nchr.gr/{path}?format=raw

License: Public

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.GNCHR")

BASE_URL = "https://www.nchr.gr"
LISTING_URL = f"{BASE_URL}/2020-02-26-05-51-20.html"
ARTICLE_PATH_RE = re.compile(r'/2020-02-26-05-51-20/(\d+-[^/]+)/(\d+)-([^"\.]+)\.html')


class GNCHRScraper(BaseScraper):
    """
    Scraper for GR/GNCHR -- Greek National Commission for Human Rights.
    Country: GR
    URL: https://www.nchr.gr

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=60,
        )

    def _get_article_links(self) -> List[Dict[str, str]]:
        """Fetch the full listing page and extract all article URLs."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(
                "/2020-02-26-05-51-20.html",
                params={"limit": 0},
            )
            resp.raise_for_status()
            page_html = resp.text
        except Exception as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return []

        articles = []
        seen_ids = set()

        for match in ARTICLE_PATH_RE.finditer(page_html):
            subcat = match.group(1)
            article_id = match.group(2)
            slug = match.group(3)

            if article_id in seen_ids:
                continue
            seen_ids.add(article_id)

            path = f"/2020-02-26-05-51-20/{subcat}/{article_id}-{slug}.html"
            articles.append({
                "article_id": article_id,
                "slug": slug,
                "subcategory": subcat,
                "path": path,
            })

        logger.info(f"Found {len(articles)} unique article links")
        return articles

    def _strip_html(self, text: str) -> str:
        """Strip HTML tags and decode entities."""
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = html.unescape(text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def _fetch_article(self, article_info: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single article using ?format=raw."""
        path = article_info["path"]

        try:
            self.rate_limiter.wait()
            resp = self.client.get(path, params={"format": "raw"})
            resp.raise_for_status()
            raw_html = resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch article {article_info['article_id']}: {e}")
            return None

        # Extract title
        title = ""
        title_match = re.search(r'property="name"\s+content="([^"]*)"', raw_html)
        if title_match:
            title = html.unescape(title_match.group(1))
        if not title:
            title_match = re.search(r'<h[12][^>]*>(.*?)</h[12]>', raw_html, re.DOTALL)
            if title_match:
                title = self._strip_html(title_match.group(1))

        # Extract date
        date_str = None
        date_match = re.search(r'property="datePublished"\s+content="([^"]*)"', raw_html)
        if date_match:
            date_str = date_match.group(1)
        if not date_str:
            date_match = re.search(r'property="dateModified"\s+content="([^"]*)"', raw_html)
            if date_match:
                date_str = date_match.group(1)

        # Extract category
        category = ""
        cat_match = re.search(r'property="articleSection"\s+content="([^"]*)"', raw_html)
        if cat_match:
            category = html.unescape(cat_match.group(1))

        # Extract full text from property="text" div
        full_text = ""
        text_match = re.search(
            r'<div[^>]*property="text"[^>]*>(.*?)</div>\s*(?:</div>|<div)',
            raw_html,
            re.DOTALL,
        )
        if text_match:
            full_text = self._strip_html(text_match.group(1))
        else:
            # Fallback: try to get the article body
            body_match = re.search(
                r'<article[^>]*>(.*?)</article>',
                raw_html,
                re.DOTALL,
            )
            if body_match:
                full_text = self._strip_html(body_match.group(1))
            else:
                # Last resort: strip the whole raw HTML
                full_text = self._strip_html(raw_html)

        if not full_text or len(full_text) < 50:
            logger.warning(f"Insufficient text for article {article_info['article_id']}: {len(full_text)} chars")
            return None

        return {
            "article_id": article_info["article_id"],
            "title": title,
            "full_text": full_text,
            "date": date_str,
            "category": category,
            "subcategory_slug": article_info["subcategory"],
            "url": f"{BASE_URL}{path}",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all GNCHR articles."""
        articles = self._get_article_links()
        logger.info(f"Fetching {len(articles)} articles...")

        for i, article_info in enumerate(articles):
            logger.info(f"  [{i+1}/{len(articles)}] Article {article_info['article_id']}...")
            result = self._fetch_article(article_info)
            if result:
                yield result

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield articles published since the given date."""
        for record in self.fetch_all():
            date_str = record.get("date")
            if date_str:
                try:
                    pub_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    if pub_date.tzinfo is None:
                        pub_date = pub_date.replace(tzinfo=timezone.utc)
                    if pub_date >= since:
                        yield record
                except (ValueError, TypeError):
                    yield record
            else:
                yield record

    def normalize(self, raw: dict) -> dict:
        """Transform raw article to standard schema."""
        title = raw.get("title", "") or f"GNCHR Article {raw.get('article_id')}"

        return {
            "_id": f"GNCHR-{raw.get('article_id')}",
            "_source": "GR/GNCHR",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": raw.get("date"),
            "url": raw.get("url"),
            "category": raw.get("category"),
            "subcategory": raw.get("subcategory_slug"),
        }

    def _fetch_sample(self, sample_size: int = 12) -> list:
        """Fetch sample records for validation."""
        articles = self._get_article_links()
        samples = []

        for article_info in articles[:sample_size * 2]:
            if len(samples) >= sample_size:
                break

            result = self._fetch_article(article_info)
            if result:
                normalized = self.normalize(result)
                samples.append(normalized)
                logger.info(
                    f"Sample {len(samples)}/{sample_size}: {normalized['_id']} "
                    f"({len(normalized.get('text', ''))} chars)"
                )

        return samples


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GR/GNCHR Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GNCHRScraper()

    if args.command == "test":
        print("Testing GNCHR connection...")
        articles = scraper._get_article_links()
        if articles:
            print(f"SUCCESS: Found {len(articles)} article links")
            print(f"Sample: {articles[0]}")
        else:
            print("FAILED: Could not retrieve article links")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            samples = scraper._fetch_sample(sample_size=12)

            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)

            for record in samples:
                safe_id = record['_id'].replace('/', '_')
                filepath = sample_dir / f"{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to {sample_dir}/")

            if samples:
                text_lengths = [len(s.get("text", "")) for s in samples]
                avg_len = sum(text_lengths) / len(text_lengths)
                print(f"Average text length: {avg_len:.0f} characters")
                print(f"Min text length: {min(text_lengths)} chars")
                print(f"Max text length: {max(text_lengths)} chars")
        else:
            print("Running full bootstrap...")
            count = 0
            for record in scraper.fetch_all():
                normalized = scraper.normalize(record)
                print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
                count += 1
            print(f"\nFetched {count} GNCHR articles")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        print(f"Fetching updates since {since.isoformat()}...")
        count = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
            count += 1
        print(f"\nFetched {count} new GNCHR articles")


if __name__ == "__main__":
    main()
