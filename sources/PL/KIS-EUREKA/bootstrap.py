#!/usr/bin/env python3
"""
PL/KIS-EUREKA -- Polish Tax Interpretations (EUREKA System)

Fetches individual and general tax interpretations from the Polish Ministry
of Finance via the EUREKA REST API (Spring Boot + Elasticsearch backend).

Strategy:
  - POST search API with date range filters to stay under 10k ES limit
  - GET individual documents for full HTML text (TRESC_INTERESARIUSZ field)
  - Strip HTML tags from content

Source: https://eureka.mf.gov.pl/
Coverage: ~541,000 documents (508,999 individual interpretations + others)

Usage:
  python bootstrap.py bootstrap --sample     # Fetch sample records
  python bootstrap.py bootstrap              # Full fetch
  python bootstrap.py update                 # Recent interpretations
  python bootstrap.py test-api               # Connectivity test
"""

import sys
import json
import logging
import time
import re
import html as html_module
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.KIS-EUREKA")

API_BASE = "https://eureka.mf.gov.pl/api/public/v1"
SPA_BASE = "https://eureka.mf.gov.pl"

# Category IDs from the EUREKA system
CATEGORIES = {
    "1": "Interpretacja indywidualna",
    "2": "Interpretacja ogólna",
    "3": "Objaśnienia podatkowe",
    "4": "Wiążąca informacja stawkowa",
    "5": "Zmiana interpretacji indywidualnej",
    "6": "Wiążąca informacja akcyzowa",
    "7": "Orzeczenia sądów i trybunałów",
    "8": "Pisma urzędowe",
}

CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<p[^>]*>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def fields_to_dict(fields: list) -> dict:
    """Convert dokument.fields array [{key, value}, ...] to a flat dict."""
    result = {}
    for f in fields:
        key = f.get("key", "")
        value = f.get("value")
        if value is not None:
            result[key] = value
    return result


class KISEurekaScraper(BaseScraper):
    """
    Scraper for PL/KIS-EUREKA -- Polish tax interpretations.
    Country: PL
    URL: https://eureka.mf.gov.pl/

    Data types: doctrine
    Auth: none (public API)
    Coverage: ~541,000 tax interpretations and related documents
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=60,
        )

    def _search(self, page: int = 0, size: int = 100, date_start: str = None,
                date_end: str = None) -> dict:
        """Search EUREKA API. Returns {results: [...], totalHits: N}."""
        self.rate_limiter.wait()

        filters = {}
        if date_start:
            filters["DT_WYD_start"] = date_start
        if date_end:
            filters["DT_WYD_end"] = date_end

        body = {
            "filter": filters,
            "columns": ["DT_WYD", "SYG", "TEZA", "ID_INFORMACJI", "KATEGORIA_INFORMACJI"],
            "searchInFullPhrase": False,
            "searchInContent": False,
            "searchInSynonyms": False,
            "searchQuery": "",
            "warunkiDodatkowe": [],
        }

        url = f"/wyszukiwarka/informacje?size={size}&page={page}&sort=DT_WYD%2Cdesc"
        resp = self.client.post(url, json_data=body)
        resp.raise_for_status()
        return resp.json()

    def _get_document(self, doc_id) -> dict:
        """Fetch full document by ID. Returns raw API response."""
        self.rate_limiter.wait()
        resp = self.client.get(f"/informacje/{doc_id}")
        resp.raise_for_status()
        return resp.json()

    def _get_total_count(self, date_start: str = None, date_end: str = None) -> int:
        """Get total number of results for a query."""
        result = self._search(page=0, size=1, date_start=date_start, date_end=date_end)
        return result.get("totalHits", 0)

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from EUREKA.

        Uses date-range partitioning to stay under Elasticsearch's 10k limit.
        Iterates year by year, fetching document IDs from search, then
        fetching full documents individually.
        """
        checkpoint = self._load_checkpoint()
        start_year = checkpoint.get("last_year", 2004)
        skip_ids = set(checkpoint.get("completed_ids", []))

        current_year = datetime.now().year

        for year in range(start_year, current_year + 1):
            date_start = f"{year}-01-01"
            date_end = f"{year}-12-31"

            total = self._get_total_count(date_start=date_start, date_end=date_end)
            logger.info(f"Year {year}: {total} documents")

            if total == 0:
                continue

            if total > 9500:
                yield from self._fetch_by_month(year, skip_ids)
            else:
                yield from self._fetch_date_range(date_start, date_end, skip_ids)

            self._save_checkpoint({"last_year": year, "completed_ids": []})
            skip_ids.clear()

    def _fetch_by_month(self, year: int, skip_ids: set) -> Generator[dict, None, None]:
        """Fetch documents month by month when a year has >10k results."""
        for month in range(1, 13):
            date_start = f"{year}-{month:02d}-01"
            if month == 12:
                date_end = f"{year}-12-31"
            else:
                next_month = datetime(year, month + 1, 1) - timedelta(days=1)
                date_end = next_month.strftime("%Y-%m-%d")

            yield from self._fetch_date_range(date_start, date_end, skip_ids)

    def _fetch_date_range(self, date_start: str, date_end: str,
                          skip_ids: set) -> Generator[dict, None, None]:
        """Fetch all documents in a date range."""
        page = 0
        size = 200
        total_hits = None

        while True:
            try:
                result = self._search(page=page, size=size,
                                      date_start=date_start, date_end=date_end)
            except Exception as e:
                logger.error(f"Search failed page={page} [{date_start}..{date_end}]: {e}")
                break

            if total_hits is None:
                total_hits = result.get("totalHits", 0)

            items = result.get("results", [])
            if not items:
                break

            for item in items:
                doc_id = item.get("ID_INFORMACJI")
                if not doc_id or doc_id in skip_ids:
                    continue

                try:
                    full_doc = self._get_document(doc_id)
                    if full_doc:
                        yield full_doc
                except Exception as e:
                    logger.warning(f"Failed to fetch document {doc_id}: {e}")
                    continue

            page += 1
            # ES hard limit: page * size must be < 10000
            if (page + 1) * size > 10000:
                break
            if len(items) < size:
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents issued since the given date."""
        date_start = since.strftime("%Y-%m-%d")
        date_end = datetime.now().strftime("%Y-%m-%d")
        yield from self._fetch_date_range(date_start, date_end, set())

    def normalize(self, raw: dict) -> dict:
        """Transform raw EUREKA document into standard schema."""
        doc_id = str(raw.get("id", ""))
        doc_name = raw.get("nazwa", "")

        # Parse fields from dokument.fields array
        fields_list = raw.get("dokument", {}).get("fields", [])
        meta = fields_to_dict(fields_list)

        signature = meta.get("SYG", "")
        date_str = meta.get("DT_WYD", "")
        title = meta.get("TEZA", "") or doc_name or signature
        category_id = meta.get("KATEGORIA_INFORMACJI", "")
        category_name = CATEGORIES.get(str(category_id), str(category_id))

        # Full text from TRESC_INTERESARIUSZ
        full_text = meta.get("TRESC_INTERESARIUSZ", "")
        if not full_text:
            full_text = meta.get("TRESC", "")
        clean_text = strip_html(full_text)

        # Parse date
        date_iso = None
        if date_str:
            try:
                if "T" in str(date_str):
                    dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
                    date_iso = dt.strftime("%Y-%m-%d")
                else:
                    date_iso = str(date_str)[:10]
            except (ValueError, TypeError):
                date_iso = str(date_str)

        url = f"{SPA_BASE}/informacje/podglad/{doc_id}"

        return {
            "_id": f"PL-KIS-{doc_id}",
            "_source": "PL/KIS-EUREKA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": strip_html(title) if title else signature,
            "text": clean_text,
            "date": date_iso,
            "url": url,
            "signature": signature,
            "category": category_name,
        }

    def _load_checkpoint(self) -> dict:
        """Load checkpoint for resumable fetching."""
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE) as f:
                return json.load(f)
        return {}

    def _save_checkpoint(self, data: dict):
        """Save checkpoint."""
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(data, f)

    def test_api(self):
        """Test API connectivity and show sample data."""
        logger.info("Testing EUREKA API connectivity...")

        result = self._search(page=0, size=3)
        total = result.get("totalHits", 0)
        logger.info(f"Total documents in EUREKA: {total:,}")

        items = result.get("results", [])
        for item in items:
            doc_id = item.get("ID_INFORMACJI")
            sig = item.get("SYG", "")
            date = item.get("DT_WYD", "")
            logger.info(f"  [{doc_id}] {sig} ({date})")

        if items:
            test_id = items[0].get("ID_INFORMACJI")
            logger.info(f"\nFetching full document {test_id}...")
            doc = self._get_document(test_id)
            normalized = self.normalize(doc)
            text = normalized.get("text", "")
            logger.info(f"  Full text length: {len(text)} chars")
            logger.info(f"  Preview: {text[:200]}...")

        logger.info("\nAPI test passed!")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PL/KIS-EUREKA data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api",
                                            "bootstrap-fast"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only a small sample")
    parser.add_argument("--sample-size", type=int, default=15,
                        help="Number of sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KISEurekaScraper()

    if args.command == "test-api":
        scraper.test_api()
    elif args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample,
                                   sample_size=args.sample_size)
        logger.info(f"Bootstrap complete: {result}")
    elif args.command == "bootstrap-fast":
        result = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {result}")
    elif args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=30)
        result = scraper.update(since)
        logger.info(f"Update complete: {result}")


if __name__ == "__main__":
    main()
