#!/usr/bin/env python3
"""
MM/IRD-TaxGuidance -- Myanmar Internal Revenue Department Tax Guidance

Fetches tax doctrine from digitallibrary.ird.gov.mm:
  - Tax Laws (~33 docs) from policyandlaw.php
  - Interpretations & Rulings (~116 docs) from policy.php
  - Pamphlets (~54 docs) from pamphlet.php
  - PDF full text extracted via common/pdf_extract
  - ~200 documents total

Strategy:
  1. Scrape three HTML listing pages to collect document IDs & metadata
  2. Download PDFs via dedicated download endpoints
  3. Extract text with pdf_extract
  4. 1.5-second delay between requests

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MM.IRD-TaxGuidance")

BASE_URL = "https://digitallibrary.ird.gov.mm"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "MM/IRD-TaxGuidance"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

CRAWL_DELAY = 1.5

# Three collections with different download URL patterns
COLLECTIONS = [
    {
        "name": "Tax Laws",
        "list_page": "policyandlaw.php",
        "download_tpl": "download_file.php?id={id}",
        "link_pattern": "download_file",
        "category": "tax_law",
        "id_prefix": "law",
        "year_col": 2,  # 0-indexed: Title, Category, Year, Links
        "cols": 4,
    },
    {
        "name": "Interpretations & Rulings",
        "list_page": "policy.php",
        "download_tpl": "downloadp.php?id={id}",
        "link_pattern": "downloadp",
        "category": "interpretation",
        "id_prefix": "ruling",
        "year_col": None,  # Title, Category, Links (no year col)
        "cols": 3,
    },
    {
        "name": "Pamphlets",
        "list_page": "pamphlet.php",
        "download_tpl": "pamphletdownload.php?id={id}",
        "link_pattern": "pamphletdownload",
        "category": "pamphlet",
        "id_prefix": "pamphlet",
        "year_col": 1,  # Title, Year, Links
        "cols": 3,
    },
]


class IRDTaxGuidanceScraper(BaseScraper):
    """Scraper for MM/IRD-TaxGuidance -- Myanmar IRD Digital Library."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        return {"fetched_ids": []}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint, f, indent=2)

    def _get_page(self, url: str) -> Optional[str]:
        time.sleep(CRAWL_DELAY)
        try:
            resp = self.session.get(url, timeout=60)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _parse_listing(self, collection: dict) -> list:
        """Parse a listing page and return list of document dicts."""
        url = f"{BASE_URL}/{collection['list_page']}"
        logger.info(f"Fetching listing: {url}")
        html = self._get_page(url)
        if not html:
            logger.warning(f"Could not fetch listing page: {url}")
            return []

        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table")
        if not table:
            logger.warning(f"No table found on {url}")
            return []

        rows = table.select("tr")
        items = []

        for row in rows[1:]:  # skip header
            tds = row.select("td")
            if len(tds) < 2:
                continue

            title = tds[0].get_text(strip=True)
            if not title:
                continue

            # Extract year if available
            year = ""
            if collection["year_col"] is not None and collection["year_col"] < len(tds):
                year = tds[collection["year_col"]].get_text(strip=True)

            # Extract subcategory from column 1 (for rulings page)
            subcategory = ""
            if collection["category"] == "interpretation" and len(tds) > 1:
                subcategory = tds[1].get_text(strip=True)

            # Find download link to get the ID
            download_link = row.select_one(f'a[href*="{collection["link_pattern"]}"]')
            if not download_link:
                continue

            href = download_link.get("href", "")
            id_match = re.search(r"id=(\d+)", href)
            if not id_match:
                continue

            doc_id = id_match.group(1)
            download_url = f"{BASE_URL}/{collection['download_tpl'].format(id=doc_id)}"

            items.append({
                "doc_id": f"{collection['id_prefix']}-{doc_id}",
                "raw_id": doc_id,
                "title": title,
                "year": year,
                "subcategory": subcategory,
                "category": collection["category"],
                "download_url": download_url,
                "collection": collection["name"],
            })

        logger.info(f"  Found {len(items)} documents in {collection['name']}")
        return items

    def _fetch_document(self, item: dict) -> Optional[dict]:
        """Download PDF and extract text for a single document."""
        logger.info(f"  Fetching: {item['doc_id']} - {item['title'][:60]}")

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=item["doc_id"],
            pdf_url=item["download_url"],
            table="doctrine",
        )

        if not text:
            logger.warning(f"  No text extracted from: {item['download_url']}")
            return None

        logger.info(f"  Extracted {len(text)} chars")

        return {
            "doc_id": item["doc_id"],
            "title": item["title"],
            "text": text,
            "year": item.get("year", ""),
            "subcategory": item.get("subcategory", ""),
            "category": item["category"],
            "collection": item["collection"],
            "download_url": item["download_url"],
        }

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"MM-IRD-{raw['doc_id']}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("year", ""),
            "url": raw["download_url"],
            "category": raw.get("category", ""),
            "subcategory": raw.get("subcategory", ""),
            "collection": raw.get("collection", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        sample_limit = 15 if sample else None
        total_yielded = 0
        checkpoint = self._load_checkpoint()
        fetched_ids = set(checkpoint.get("fetched_ids", []))

        for collection in COLLECTIONS:
            if sample_limit and total_yielded >= sample_limit:
                break

            items = self._parse_listing(collection)

            for item in items:
                if sample_limit and total_yielded >= sample_limit:
                    break

                if item["doc_id"] in fetched_ids:
                    logger.info(f"  Skipping already-fetched: {item['doc_id']}")
                    continue

                raw = self._fetch_document(item)
                if raw:
                    yield raw
                    total_yielded += 1
                    fetched_ids.add(item["doc_id"])

                    if total_yielded % 5 == 0:
                        checkpoint["fetched_ids"] = list(fetched_ids)
                        self._save_checkpoint(checkpoint)

            checkpoint["fetched_ids"] = list(fetched_ids)
            self._save_checkpoint(checkpoint)

        logger.info(f"\nTotal documents fetched: {total_yielded}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch documents from the first collection (tax laws) for updates."""
        logger.info("Fetching updates (Tax Laws listing)")
        items = self._parse_listing(COLLECTIONS[0])
        checkpoint = self._load_checkpoint()
        fetched_ids = set(checkpoint.get("fetched_ids", []))

        for item in items:
            if item["doc_id"] in fetched_ids:
                continue
            raw = self._fetch_document(item)
            if raw:
                yield self.normalize(raw)

    def test(self):
        logger.info("Testing connectivity to digitallibrary.ird.gov.mm...")
        try:
            resp = self.session.get(f"{BASE_URL}/policyandlaw.php", timeout=60)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.select_one("table")
            if not table:
                logger.error("No table found")
                return False
            rows = table.select("tr")
            logger.info(f"Connection OK. Found {len(rows) - 1} tax law entries.")

            # Test PDF download
            download_link = table.select_one('a[href*="download_file"]')
            if download_link:
                href = download_link["href"]
                id_match = re.search(r"id=(\d+)", href)
                if id_match:
                    test_url = f"{BASE_URL}/download_file.php?id={id_match.group(1)}"
                    logger.info(f"Testing PDF download: {test_url}")
                    text = extract_pdf_markdown(
                        source=SOURCE_ID,
                        source_id="test",
                        pdf_url=test_url,
                        table="doctrine",
                    )
                    if text and len(text) > 100:
                        logger.info(f"PDF extraction OK: {len(text)} chars")
                        logger.info("Test PASSED")
                        return True
                    else:
                        logger.error("PDF extraction failed or too short")
                        return False
            logger.error("No download link found")
            return False
        except Exception as e:
            logger.error(f"Test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MM/IRD-TaxGuidance fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IRDTaxGuidanceScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample)
    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            out_file = SAMPLE_DIR / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} new records")


if __name__ == "__main__":
    main()
