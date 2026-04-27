#!/usr/bin/env python3
"""
AE/FSRA-Enforcement -- ADGM FSRA Regulatory Actions Fetcher

Fetches enforcement decisions (Final Notices, Penalty Notices, Enforceable
Undertakings) from the ADGM Financial Services Regulatory Authority via
their internal JSON API, then downloads full-text PDFs from assets.adgm.com.

Strategy:
  - Bootstrap: Paginate the JSON API (contentid=84284, 50 items/page).
  - Each item includes a PDF link on assets.adgm.com with the full decision.
  - Sample: Fetch 12 records with full text for validation.

API: POST /RegulatoryTable/GetRegulatoryItemsAfterFilterationPagination
PDFs: https://assets.adgm.com/download/assets/...

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AE.FSRA-Enforcement")

API_URL = "https://www.adgm.com/RegulatoryTable/GetRegulatoryItemsAfterFilterationPagination"
CONTENT_ID = "84284"

MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def parse_adgm_date(date_str: str) -> str:
    """Parse dates like '08 Jan 2026' to '2026-01-08'."""
    if not date_str:
        return ""
    date_str = date_str.strip()
    match = re.match(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", date_str)
    if match:
        day, mon, year = match.groups()
        mon_num = MONTHS.get(mon.lower(), "01")
        return f"{year}-{mon_num}-{day.zfill(2)}"
    return date_str


def make_doc_id(item: dict) -> str:
    """Create a stable document ID from the item."""
    date = parse_adgm_date(item.get("date", ""))
    person = item.get("person", "unknown").strip()
    slug = re.sub(r"[^a-z0-9]+", "-", person.lower()).strip("-")[:60]
    return f"fsra-{date}-{slug}"


class FSRAEnforcementScraper(BaseScraper):
    """
    Scraper for AE/FSRA-Enforcement -- ADGM FSRA Regulatory Actions.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Content-Type": "application/json; charset=utf-8",
            },
            timeout=60,
        )

    def _fetch_page(self, page: int = 1, page_size: int = 50) -> dict:
        """Fetch a page of regulatory actions from the JSON API."""
        payload = {
            "query": "",
            "category": "",
            "itemsperpage": str(page_size),
            "currentpage": str(page),
            "contentid": CONTENT_ID,
            "sortby": "date#desc",
            "externalpreview": False,
        }
        self.rate_limiter.wait()
        resp = self.client.post(API_URL, json_data=payload)
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, str):
            data = json.loads(data)
        return data

    def _fetch_all_items(self) -> list[dict]:
        """Fetch all regulatory action items from the API."""
        first_page = self._fetch_page(page=1, page_size=50)
        total = first_page.get("TotalItems", 0)
        items = first_page.get("RegulatoryTableItems", [])
        logger.info(f"Total items: {total}, first page: {len(items)}")

        if total > 50:
            page = 2
            while len(items) < total:
                data = self._fetch_page(page=page, page_size=50)
                batch = data.get("RegulatoryTableItems", [])
                if not batch:
                    break
                items.extend(batch)
                logger.info(f"Page {page}: {len(batch)} items (total: {len(items)})")
                page += 1

        return items

    def _extract_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download and extract text from an enforcement PDF."""
        if not pdf_url:
            return ""
        try:
            text = extract_pdf_markdown(
                "AE/FSRA-Enforcement", doc_id,
                pdf_url=pdf_url, table="case_law"
            )
            return text or ""
        except Exception as e:
            logger.warning(f"PDF extraction failed for {doc_id}: {e}")
            return ""

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw API item into standard schema."""
        doc_id = make_doc_id(raw)
        date = parse_adgm_date(raw.get("date", ""))
        title = raw.get("title", "").strip()
        person = raw.get("person", "").strip()
        category = raw.get("category", "").strip()
        pdf_url = raw.get("actionlink", "").strip()

        # Extract full text from PDF
        text = self._extract_pdf_text(pdf_url, doc_id)

        return {
            "_id": doc_id,
            "_source": "AE/FSRA-Enforcement",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url or "https://www.adgm.com/operating-in-adgm/additional-obligations-of-financial-services-entities/enforcement/regulatory-actions",
            "person": person,
            "category": category,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all FSRA enforcement decisions."""
        items = self._fetch_all_items()
        for i, item in enumerate(items):
            logger.info(f"[{i+1}/{len(items)}] {item.get('title', '')[:60]}...")
            yield item

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield enforcement decisions newer than `since`."""
        items = self._fetch_all_items()
        if isinstance(since, str):
            since = datetime.fromisoformat(since)
        since_date = since.date() if hasattr(since, 'date') else since

        for item in items:
            date_str = parse_adgm_date(item.get("date", ""))
            if not date_str:
                continue
            try:
                item_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if item_date < since_date:
                continue
            yield item

    def test_api(self) -> dict:
        """Quick connectivity test."""
        data = self._fetch_page(page=1, page_size=3)
        total = data.get("TotalItems", 0)
        items = data.get("RegulatoryTableItems", [])
        return {
            "status": "ok" if total > 0 else "error",
            "total_items": total,
            "sample_count": len(items),
            "first_item": items[0] if items else None,
        }


def main():
    scraper = FSRAEnforcementScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        result = scraper.test_api()
        print(json.dumps(result, indent=2, default=str))
    elif command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
