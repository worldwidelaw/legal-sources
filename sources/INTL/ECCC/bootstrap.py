#!/usr/bin/env python3
"""
INTL/ECCC -- Extraordinary Chambers in the Courts of Cambodia

Fetches judgments, decisions, and orders from the ECCC (Khmer Rouge tribunal)
archive via its REST API.

Strategy:
  - Use POST archive.eccc.gov.kh/api/search to enumerate public documents
  - Filter by record_type (Judgment, Decision, Order) and language (EN)
  - Download PDFs via /api/documents/{id}/download
  - Extract text with pdfplumber (OCR text layer present on scanned docs)

Data Coverage:
  - Judicial matter (matterId=49): ~101K documents total
  - Focus on Judgments (~28), Decisions (~5K), Orders (~2.7K) in English
  - Cases: 001 (Duch), 002 (Nuon Chea/Khieu Samphan), 003, 004
  - Classification: Public only

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import io
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ECCC")

ARCHIVE_BASE = "https://archive.eccc.gov.kh"
SEARCH_URL = f"{ARCHIVE_BASE}/api/search"
DOWNLOAD_URL = f"{ARCHIVE_BASE}/api/documents/{{id}}/download?matterId={{matter_id}}"

MATTER_ID = 49  # Judicial
RECORD_TYPES = ["Decision", "Order", "Judgment"]
PER_PAGE = 25
MAX_PDF_BYTES = 50 * 1024 * 1024  # 50MB limit per PDF


class ECCCScraper(BaseScraper):
    """Scraper for ECCC archive documents."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Content-Type": "application/json",
        })

    def _search(self, record_types: list, page: int = 1, per_page: int = PER_PAGE) -> dict:
        """Search the ECCC archive API."""
        payload = {
            "matterId": MATTER_ID,
            "sortOption": 1,
            "keyword": "",
            "page": page,
            "perPage": per_page,
            "filters": {
                "record_type": record_types,
                "doc_language": ["EN"],
                "classification": ["Public"],
            },
            "searchWithin": "",
            "sortOrderName": "Doc_Date",
        }
        resp = self.session.post(SEARCH_URL, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _download_pdf_text(self, doc_id: int) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/ECCC",
            source_id="",
            pdf_bytes=doc_id,
            table="case_law",
        ) or ""

    def _iterate_documents(self, record_types: list = None) -> Generator[dict, None, None]:
        """Iterate through all matching documents via pagination."""
        if record_types is None:
            record_types = RECORD_TYPES

        for rtype in record_types:
            page = 1
            total_yielded = 0
            while True:
                logger.info(f"Fetching {rtype} page {page}...")
                try:
                    result = self._search([rtype], page=page)
                except Exception as e:
                    logger.error(f"Search failed for {rtype} page {page}: {e}")
                    break

                docs_data = result.get("data", {}).get("documents", {})
                records = docs_data.get("records", [])
                total_count = docs_data.get("totalCount", 0)

                if not records:
                    break

                for rec in records:
                    yield rec
                    total_yielded += 1

                has_more = docs_data.get("hasMore", False)
                if not has_more or total_yielded >= total_count:
                    break

                page += 1
                time.sleep(1)

            logger.info(f"Fetched {total_yielded} {rtype} records")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments, decisions, and orders with full text."""
        for rec in self._iterate_documents():
            doc_id = rec.get("id")
            title = rec.get("title_en", "") or rec.get("name", "")

            if not doc_id:
                continue

            # Skip confidential documents
            if rec.get("classification", "").lower() != "public":
                continue

            logger.info(f"  Downloading {rec.get('doc_no', '?')}: {title[:50]}...")
            time.sleep(1.5)

            text = self._download_pdf_text(doc_id)
            if not text:
                logger.warning(f"  No text for doc {doc_id}")
                continue

            rec["_extracted_text"] = text
            yield rec

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents filed since a given date."""
        # The API doesn't support date filtering directly in a useful way,
        # so we paginate through recent documents and stop when we hit older ones
        for rtype in RECORD_TYPES:
            page = 1
            while True:
                try:
                    result = self._search([rtype], page=page)
                except Exception as e:
                    logger.error(f"Search failed: {e}")
                    break

                records = result.get("data", {}).get("documents", {}).get("records", [])
                if not records:
                    break

                found_old = False
                for rec in records:
                    filing_date = rec.get("filing_date", "")
                    if filing_date:
                        try:
                            filed = datetime.fromisoformat(filing_date.replace("Z", "+00:00"))
                            if filed < since:
                                found_old = True
                                continue
                        except (ValueError, TypeError):
                            pass

                    doc_id = rec.get("id")
                    if not doc_id or rec.get("classification", "").lower() != "public":
                        continue

                    time.sleep(1.5)
                    text = self._download_pdf_text(doc_id)
                    if text:
                        rec["_extracted_text"] = text
                        yield rec

                if found_old:
                    break
                page += 1
                time.sleep(1)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw ECCC record into standard schema."""
        text = raw.get("_extracted_text", "").strip()
        if not text or len(text) < 50:
            return None

        doc_id = raw.get("id", "")
        doc_no = raw.get("doc_no", "") or ""
        title = raw.get("title_en", "") or raw.get("name", "") or doc_no
        record_type = raw.get("record_type", "")
        case_file = raw.get("last_updated_by", "")

        # Parse date
        doc_date = raw.get("doc_date", "")
        date_str = None
        if doc_date:
            try:
                dt = datetime.fromisoformat(doc_date.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        # Build download URL for reference
        download_url = DOWNLOAD_URL.format(id=doc_id, matter_id=MATTER_ID)

        return {
            "_id": f"INTL_ECCC_{doc_no or doc_id}",
            "_source": "INTL/ECCC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": date_str,
            "url": download_url,
            "doc_no": doc_no,
            "record_type": record_type,
            "case_file": case_file,
            "filing_party": raw.get("filing_party", ""),
            "classification": raw.get("classification", ""),
            "pagecount": raw.get("pagecount"),
            "ern": raw.get("ern", ""),
            "language": raw.get("doc_language", "EN"),
            "court": "Extraordinary Chambers in the Courts of Cambodia (ECCC)",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ECCC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = ECCCScraper()

    if args.command == "test":
        print("Testing ECCC archive API...")
        try:
            result = scraper._search(["Judgment"], page=1, per_page=5)
            docs = result.get("data", {}).get("documents", {})
            total = docs.get("totalCount", 0)
            records = docs.get("records", [])
            print(f"OK: {total} English Judgments found")
            if records:
                r = records[0]
                print(f"  First: {r.get('doc_no')} - {r.get('title_en', '')[:60]}")
            # Test download
            if records:
                text = scraper._download_pdf_text(records[0]["id"])
                if text:
                    print(f"  PDF text extraction: OK ({len(text)} chars)")
                else:
                    print("  PDF text extraction: FAILED")
        except Exception as e:
            print(f"FAIL: {e}")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
