#!/usr/bin/env python3
"""
US/TX-Comptroller -- Texas Comptroller STAR Tax Doctrine

Fetches tax doctrine from the Texas Comptroller of Public Accounts via
the STAR (State Tax Automated Research) REST API.

Strategy:
  1. Enumerate documents month-by-month via /search?doc_date_month_year=YYYY-MM
  2. Paginate within each month (API returns max 10 per page, use start= offset)
  3. For each document, fetch full text via /view/{acc_no}
  4. Strip HTML tags from content, normalize into standard schema

Data: Public domain (US government works). No auth required.
Rate limit: 1 req/sec. ~24,700+ documents spanning 1960-present.

Usage:
  python bootstrap.py bootstrap            # Full pull (all documents)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample documents
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-Comptroller")

API_BASE = "https://api.comptroller.texas.gov/star/v1"
PAGE_SIZE = 10  # API hard limit

# HTML tag stripping
TAG_RE = re.compile(r"<[^>]+>")
MULTI_NEWLINE_RE = re.compile(r"\n{3,}")
MULTI_SPACE_RE = re.compile(r"[ \t]{2,}")


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if not html_text:
        return ""
    text = TAG_RE.sub("", html_text)
    text = unescape(text)
    text = MULTI_SPACE_RE.sub(" ", text)
    text = MULTI_NEWLINE_RE.sub("\n\n", text)
    return text.strip()


class TXComptrollerScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "application/json",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get_json(self, path: str, params: dict = None) -> dict:
        time.sleep(self.delay)
        resp = self.http.get(path, params=params)
        return resp.json()

    def test_api(self):
        """Test connectivity to STAR API."""
        logger.info("Testing STAR API...")
        try:
            resp = self._get_json("/search")
            data = resp.get("data", {})
            docs = data.get("documents", [])
            found = data.get("found", 0)
            if docs:
                first = docs[0]
                logger.info(f"  Total records: {found}")
                logger.info(f"  First result: {first.get('acc_no')} - {first.get('doc_type')}")

                # Test full text retrieval
                acc_no = first.get("acc_no")
                if acc_no:
                    view_resp = self._get_json(f"/view/{acc_no}")
                    view_data = view_resp.get("data", {})
                    contents = view_data.get("contents", "")
                    text = strip_html(contents)
                    logger.info(f"  Full text: {len(text)} chars")
                    if len(text) > 50:
                        logger.info("API test PASSED")
                        return True
                    else:
                        logger.error("API test FAILED: text too short")
                        return False
            logger.error("API test FAILED: no results")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def search_by_month(self, year: int, month: int) -> list:
        """Fetch ALL documents for a given month, paginating as needed."""
        month_str = f"{year:04d}-{month:02d}"
        all_docs = []
        start = 0

        while True:
            resp = self._get_json("/search", params={
                "doc_date_month_year": month_str,
                "sort": "doc_date asc",
                "start": start,
            })
            data = resp.get("data", {})
            docs = data.get("documents", [])
            found = data.get("found", 0)

            if not docs:
                break

            all_docs.extend(docs)

            # If we've fetched all, stop
            if len(all_docs) >= found or len(docs) < PAGE_SIZE:
                break

            start += PAGE_SIZE

        return all_docs

    def fetch_document(self, acc_no: str) -> Optional[dict]:
        """Fetch full document by accession number."""
        try:
            resp = self._get_json(f"/view/{acc_no}")
            return resp.get("data", {})
        except Exception as e:
            logger.warning(f"Failed to fetch {acc_no}: {e}")
            return None

    def normalize(self, doc: dict) -> dict:
        """Normalize a STAR document into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        acc_no = doc.get("acc_no", "")
        contents = doc.get("contents", "")
        text = strip_html(contents)

        # Build a readable title
        doc_type = doc.get("doc_type", "")
        tax_type = doc.get("tax_type_long", "") or doc.get("tax_type_short", "")
        title_parts = [p for p in [doc_type, tax_type, acc_no] if p]
        title = " - ".join(title_parts) if title_parts else acc_no

        # Date is already YYYY-MM-DD from the API
        doc_date = doc.get("doc_date", "")

        # Collect subjects
        subjects = doc.get("subjects", {})
        subject_list = []
        if isinstance(subjects, dict):
            subject_list = list(subjects.values())
        elif isinstance(subjects, list):
            subject_list = subjects

        return {
            "_id": f"tx-comptroller-{acc_no}",
            "_source": "US/TX-Comptroller",
            "_type": "doctrine",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": doc_date,
            "url": f"https://star.comptroller.texas.gov/view/{acc_no}",
            "acc_no": acc_no,
            "doc_type": doc_type,
            "doc_type_code": doc.get("doc_type_code", ""),
            "tax_type": tax_type,
            "tax_type_code": doc.get("tax_type_code", ""),
            "status": doc.get("status", ""),
            "subjects": subject_list,
            "publish_date": doc.get("publish_date", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Enumerate all documents month-by-month from 1960 to present."""
        total = 0
        now = datetime.now()
        for year in range(1960, now.year + 1):
            end_month = now.month if year == now.year else 12
            for month in range(1, end_month + 1):
                try:
                    results = self.search_by_month(year, month)
                except Exception as e:
                    logger.warning(f"Failed to search {year}-{month:02d}: {e}")
                    continue

                if not results:
                    continue

                logger.info(f"  {year}-{month:02d}: {len(results)} documents")
                for item in results:
                    acc_no = item.get("acc_no")
                    if not acc_no:
                        continue
                    doc = self.fetch_document(acc_no)
                    if not doc:
                        continue
                    record = self.normalize(doc)
                    if record["text"] and len(record["text"]) > 50:
                        yield record
                        total += 1
                        if total % 100 == 0:
                            logger.info(f"  Progress: {total} documents fetched")
                    else:
                        logger.warning(f"Skipping {acc_no}: no/short text")

        logger.info(f"Total documents fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch documents from a given date to present."""
        total = 0
        try:
            since_dt = datetime.strptime(since, "%Y-%m-%d")
        except ValueError:
            since_dt = datetime.strptime(since, "%Y-%m-%dT%H:%M:%SZ")

        now = datetime.now()
        year = since_dt.year
        month = since_dt.month

        while (year < now.year) or (year == now.year and month <= now.month):
            try:
                results = self.search_by_month(year, month)
            except Exception as e:
                logger.warning(f"Failed to search {year}-{month:02d}: {e}")
                month += 1
                if month > 12:
                    month = 1
                    year += 1
                continue

            for item in results:
                acc_no = item.get("acc_no")
                if not acc_no:
                    continue
                doc = self.fetch_document(acc_no)
                if not doc:
                    continue
                record = self.normalize(doc)
                if record["text"] and len(record["text"]) > 50:
                    yield record
                    total += 1

            month += 1
            if month > 12:
                month = 1
                year += 1

        logger.info(f"Updates fetched: {total} documents since {since}")

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a sample of recent documents."""
        logger.info("Fetching sample TX Comptroller documents...")
        now = datetime.now()
        count = 0
        year = now.year
        month = now.month

        while count < 15 and year >= 2020:
            try:
                results = self.search_by_month(year, month)
            except Exception as e:
                logger.warning(f"Failed to search {year}-{month:02d}: {e}")
                month -= 1
                if month < 1:
                    month = 12
                    year -= 1
                continue

            for item in results:
                if count >= 15:
                    break
                acc_no = item.get("acc_no")
                if not acc_no:
                    continue
                doc = self.fetch_document(acc_no)
                if not doc:
                    continue
                record = self.normalize(doc)
                if record["text"] and len(record["text"]) > 50:
                    yield record
                    count += 1
                    logger.info(f"  Sample {count}: {acc_no} ({len(record['text'])} chars)")
                else:
                    logger.warning(f"Skipping {acc_no}: no/short text")

            month -= 1
            if month < 1:
                month = 12
                year -= 1

        logger.info(f"Sample complete: {count} documents fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TX-Comptroller bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = TXComptrollerScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            gen = scraper.fetch_sample()
        else:
            gen = scraper.fetch_all()

        count = 0
        for record in gen:
            safe_id = record["_id"].replace("/", "_")
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} - {record['title'][:60]} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
