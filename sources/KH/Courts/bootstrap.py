#!/usr/bin/env python3
"""
KH/Courts -- Extraordinary Chambers in the Courts of Cambodia (ECCC)

Fetches decisions, orders, and judgments from the ECCC archive portal.

API: POST https://archive.eccc.gov.kh/api/search (JSON, no auth)
Documents: PDF download via /api/documents/{id}/download?matterId=49
Text: Extracted from PDFs using pdfminer.

Usage:
  python bootstrap.py bootstrap --sample   # Sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py update                # Incremental update
"""

import sys
import json
import logging
import time
import io
import re
from pathlib import Path
from datetime import datetime, timezone
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
logger = logging.getLogger("legal-data-hunter.KH.Courts")

API_BASE = "https://archive.eccc.gov.kh/api"
MATTER_ID = 49  # Judicial documents

# Record types we want (case law)
RECORD_TYPES = ["Decision", "Order", "Judgment"]

# Max results per page
PER_PAGE = 100


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="KH/Courts",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

class ECCCScraper(BaseScraper):
    """
    Scraper for KH/Courts -- ECCC Archive Portal.
    Country: KH
    URL: https://archive.eccc.gov.kh

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Content-Type": "application/json",
            },
            timeout=120,
        )

    # -- API helpers --------------------------------------------------------

    def _search(self, record_types: list, page: int = 1, per_page: int = PER_PAGE,
                filters: Optional[dict] = None) -> dict:
        """Search the ECCC archive API."""
        body = {
            "matterId": MATTER_ID,
            "sortOption": 1,
            "keyword": "",
            "page": page,
            "perPage": per_page,
            "filters": filters or {"record_type": record_types},
            "searchWithin": "",
            "sortOrderName": "Doc_Date",
        }
        if "record_type" not in (filters or {}):
            body["filters"]["record_type"] = record_types

        resp = self.client.post("/search", json_data=body)
        resp.raise_for_status()
        data = resp.json()
        if data and data.get("status") == "success":
            return data["data"]["documents"]
        return {"records": [], "totalCount": 0, "hasMore": False}

    def _download_pdf(self, doc_id: int) -> bytes:
        """Download a document PDF."""
        url = f"/documents/{doc_id}/download?matterId={MATTER_ID}"
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.content if resp.content else b""

    # -- Core methods -------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions, orders, and judgments."""
        for record_type in RECORD_TYPES:
            logger.info(f"Fetching {record_type} documents...")
            page = 1
            total = None

            while True:
                result = self._search([record_type], page=page)
                records = result.get("records", [])

                if total is None:
                    total = result.get("totalCount", 0)
                    logger.info(f"  Total {record_type}: {total}")

                if not records:
                    break

                for rec in records:
                    # Download PDF and extract text
                    doc_id = rec["id"]
                    pdf_bytes = self._download_pdf(doc_id)

                    if pdf_bytes:
                        text = extract_text_from_pdf(pdf_bytes)
                    else:
                        text = ""
                        logger.warning(f"  No PDF for doc {doc_id}")

                    rec["_extracted_text"] = text
                    yield rec

                    self.rate_limiter.wait()

                if not result.get("hasMore", False):
                    break

                page += 1
                self.rate_limiter.wait()

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recently updated documents."""
        # The API doesn't have a native since filter, so we paginate
        # through recent documents sorted by date
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize an ECCC document record."""
        doc_id = raw.get("id")
        doc_no = raw.get("doc_no", "")
        title_en = raw.get("title_en") or raw.get("title_kh") or doc_no
        title = title_en.strip() if title_en else doc_no

        # Parse dates
        doc_date = raw.get("doc_date") or raw.get("filing_date")
        date_str = None
        if doc_date:
            try:
                dt = datetime.fromisoformat(doc_date.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                date_str = None

        text = raw.get("_extracted_text", "")
        download_url = f"https://archive.eccc.gov.kh/api/documents/{doc_id}/download?matterId={MATTER_ID}"

        return {
            "_id": f"KH-ECCC-{doc_id}",
            "_source": "KH/Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": download_url,
            "doc_id": doc_id,
            "doc_no": doc_no,
            "record_type": raw.get("record_type", ""),
            "case_file": raw.get("last_updated_by") or raw.get("cf_no", ""),
            "language": raw.get("doc_language", ""),
            "filing_party": raw.get("filing_party", ""),
            "classification": raw.get("classification", ""),
            "page_count": raw.get("pagecount"),
            "summary_en": raw.get("doc_summary_en", ""),
            "ern": raw.get("ern", ""),
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="KH/Courts ECCC case law fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records")
    args = parser.parse_args()

    scraper = ECCCScraper()

    if args.command == "test-api":
        print("Testing ECCC Archive API...")
        result = scraper._search(["Decision"], page=1, per_page=2)
        total = result.get("totalCount", 0)
        records = result.get("records", [])
        print(f"  Decisions available: {total}")
        if records:
            rec = records[0]
            print(f"  First: {rec.get('title_en', 'N/A')} ({rec.get('doc_no')})")
        print("API test PASSED")

    elif args.command == "bootstrap":
        sample_mode = args.sample
        sample_size = args.count if sample_mode else None
        print(f"Starting bootstrap (sample={sample_mode})...")
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=sample_size or 10)
        print(f"\nBootstrap complete:")
        print(f"  Records fetched: {stats.get('records_fetched', 0)}")
        print(f"  Records new: {stats.get('records_new', 0)}")
        print(f"  Errors: {stats.get('errors', 0)}")
        if sample_mode:
            sample_dir = scraper.source_dir / "sample"
            print(f"  Sample records saved to: {sample_dir}")

    elif args.command == "update":
        print("Starting incremental update...")
        stats = scraper.bootstrap(sample_mode=False)
        print(f"\nUpdate complete:")
        print(f"  Records fetched: {stats.get('records_fetched', 0)}")
        print(f"  Records new: {stats.get('records_new', 0)}")
