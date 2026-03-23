#!/usr/bin/env python3
"""
US/IRS_IRB -- Internal Revenue Bulletins Fetcher

Fetches IRS Internal Revenue Bulletins containing revenue rulings,
revenue procedures, Treasury decisions, notices, and announcements.
Full text HTML scraping. No auth required.

Strategy:
  - Scrape /irb listing page for bulletin URLs
  - Each bulletin is a single HTML page with full text of all items
  - Paginate through listing pages for historical bulletins

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IRS_IRB")

BASE_URL = "https://www.irs.gov"


class IRSIRBScraper(BaseScraper):
    """
    Scraper for US/IRS_IRB -- Internal Revenue Bulletins.
    Country: US
    URL: https://www.irs.gov/irb

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,  # Bulletins can be large
        )

    # -- Helpers ------------------------------------------------------------

    def _get_bulletin_urls(self, max_pages=None):
        """Scrape the IRB listing page for bulletin URLs."""
        bulletins = []
        page = 0

        while True:
            if max_pages and page >= max_pages:
                break

            self.rate_limiter.wait()
            try:
                params = {}
                if page > 0:
                    params["page"] = page
                resp = self.client.get("/irb", params=params)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to fetch listing page {page}: {e}")
                break

            # Find bulletin links
            links = re.findall(
                r'href="(https://www\.irs\.gov/irb/(\d{4}-\d+_IRB))"',
                resp.text
            )

            if not links:
                break

            seen = set()
            for url, bulletin_id in links:
                if bulletin_id not in seen:
                    seen.add(bulletin_id)
                    # Extract year from ID
                    year_match = re.match(r'(\d{4})', bulletin_id)
                    year = year_match.group(1) if year_match else ""
                    bulletins.append({
                        "url": url,
                        "bulletin_id": bulletin_id,
                        "year": year,
                    })

            # Check for next page
            if f'page={page + 1}' in resp.text or f'page%3D{page + 1}' in resp.text:
                page += 1
            else:
                break

        logger.info(f"Found {len(bulletins)} bulletins")
        return bulletins

    def _fetch_bulletin_text(self, url):
        """Fetch a bulletin HTML page and extract clean text."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

        html = resp.text

        # Extract main content
        main_match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL)
        content = main_match.group(1) if main_match else html

        # Clean HTML
        text = re.sub(r'<script[^>]*>.*?</script>', ' ', content, flags=re.DOTALL)
        text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<nav[^>]*>.*?</nav>', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<header[^>]*>.*?</header>', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<footer[^>]*>.*?</footer>', ' ', text, flags=re.DOTALL)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&#\d+;', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    # -- BaseScraper interface ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IRB bulletins with full text."""
        bulletins = self._get_bulletin_urls()

        for i, bulletin in enumerate(bulletins):
            text = self._fetch_bulletin_text(bulletin["url"])
            if text and len(text) >= 200:
                bulletin["text"] = text
                yield bulletin

                if (i + 1) % 10 == 0:
                    logger.info(f"Processed {i + 1}/{len(bulletins)} bulletins")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent bulletins."""
        since_year = since.year
        bulletins = self._get_bulletin_urls(max_pages=2)

        for bulletin in bulletins:
            year = int(bulletin.get("year", "0"))
            if year >= since_year:
                text = self._fetch_bulletin_text(bulletin["url"])
                if text and len(text) >= 200:
                    bulletin["text"] = text
                    yield bulletin

    def normalize(self, raw: dict) -> dict:
        """Transform raw bulletin data into standard schema."""
        bulletin_id = raw.get("bulletin_id", "")
        year = raw.get("year", "")

        return {
            "_id": bulletin_id,
            "_source": "US/IRS_IRB",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"Internal Revenue Bulletin: {bulletin_id.replace('_IRB', '')}",
            "text": raw.get("text", ""),
            "date": f"{year}-01-01" if year else None,
            "url": raw.get("url", ""),
            "bulletin_id": bulletin_id,
            "year": year,
        }

    # -- Sample mode --------------------------------------------------------

    def _fetch_sample(self) -> list:
        """Fetch sample bulletins for validation."""
        samples = []
        bulletins = self._get_bulletin_urls(max_pages=1)

        for bulletin in bulletins[:12]:
            text = self._fetch_bulletin_text(bulletin["url"])
            if not text or len(text) < 200:
                continue

            bulletin["text"] = text
            normalized = self.normalize(bulletin)
            samples.append(normalized)
            logger.info(
                f"  {bulletin['bulletin_id']}: {len(text)} chars"
            )

            if len(samples) >= 12:
                break

        return samples


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/IRS_IRB data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = IRSIRBScraper()

    if args.command == "test-api":
        print("Testing IRS IRB page...")
        bulletins = scraper._get_bulletin_urls(max_pages=1)
        if bulletins:
            print(f"OK: {len(bulletins)} bulletins found on first page")
            for b in bulletins[:3]:
                print(f"  {b['bulletin_id']}")
        else:
            print("FAIL: No bulletins found")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        if args.sample:
            print("Running sample mode...")
            samples = scraper._fetch_sample()
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                fname = sample_dir / f"sample_{i+1:03d}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to sample/")
            if samples:
                texts = [s["text"] for s in samples if s.get("text")]
                avg_len = sum(len(t) for t in texts) // max(len(texts), 1)
                print(f"Average text length: {avg_len} chars")
                for s in samples:
                    assert s.get("text"), f"Missing text: {s['_id']}"
                    assert s.get("title"), f"Missing title: {s['_id']}"
                print("All validation checks passed!")
            return

        result = scraper.bootstrap()
        print(f"Bootstrap complete: {result}")

    elif args.command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")


if __name__ == "__main__":
    main()
