#!/usr/bin/env python3
"""
UA/REYESTR-CourtDecisions -- Ukraine Unified State Register of Court Decisions

Fetches full-text court decisions from Ukraine's open data portal (data.gov.ua).
Annual ZIP archives contain a CSV index (documents.csv) with metadata and URLs
to RTF full-text documents hosted on od.reyestr.court.gov.ua.

Data access:
  - Annual ZIP from data.gov.ua contains documents.csv with doc_url field
  - Each doc_url points to an RTF file with full judgment text
  - RTF files are in cp1251 encoding (Ukrainian/Cyrillic)
  - Text extracted via striprtf library

Usage:
  python bootstrap.py bootstrap          # Full initial pull (WARNING: millions of records)
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Incremental (latest year)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import csv
import io
import json
import logging
import re
import time
import zipfile
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

from striprtf.striprtf import rtf_to_text

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UA.REYESTR-CourtDecisions")

DELAY = 2.0

# 2026 dataset URL
DATASET_URL_2026 = "https://data.gov.ua/dataset/16ab7f06-7414-405f-8354-0a492475272d/resource/b1a4ac1c-b17a-4988-8e6d-dedae8b2dd63/download/"


class UACourtDecisionsScraper(BaseScraper):
    """Scraper for Ukraine court decisions from open data portal."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
            }
        )
        self._courts = {}  # code -> name mapping

    def _load_courts_from_zip(self, zf: zipfile.ZipFile) -> Dict[str, str]:
        """Load court code to name mapping from courts.csv in the ZIP."""
        try:
            with zf.open("courts.csv") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8"), delimiter="\t"
                )
                courts = {}
                for row in reader:
                    code = row.get("court_code", "")
                    name = row.get("name", "").strip('"')
                    if code and name:
                        courts[code] = name
                return courts
        except Exception as e:
            logger.warning("Could not load courts.csv: %s", e)
            return {}

    def _fetch_rtf_text(self, doc_url: str) -> Optional[str]:
        """Fetch and extract text from an RTF document URL."""
        if not doc_url or not doc_url.strip():
            return None

        time.sleep(DELAY)
        try:
            resp = self.http.get(doc_url.strip(), timeout=30)
            if resp.status_code != 200:
                logger.warning("Failed to fetch RTF %s: %s", doc_url, resp.status_code)
                return None

            # Try cp1251 encoding first (standard for Ukrainian legal docs)
            try:
                rtf_content = resp.content.decode("cp1251")
            except UnicodeDecodeError:
                rtf_content = resp.content.decode("utf-8", errors="replace")

            text = rtf_to_text(rtf_content)
            return text.strip() if text else None

        except Exception as e:
            logger.warning("Error fetching RTF %s: %s", doc_url, e)
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw court decision record."""
        doc_id = raw.get("doc_id", "")
        court_code = raw.get("court_code", "")
        court_name = self._courts.get(court_code, f"Court {court_code}")
        cause_num = raw.get("cause_num", "")
        adj_date = raw.get("adjudication_date", "")
        judge = raw.get("judge", "").strip('"')
        doc_url = raw.get("doc_url", "").strip()
        text = raw.get("text", "")

        # Parse date
        date = None
        if adj_date:
            try:
                dt = datetime.strptime(adj_date.strip('"')[:10], "%Y-%m-%d")
                date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Build title from available info
        title_parts = []
        if court_name:
            title_parts.append(court_name)
        if cause_num:
            title_parts.append(f"справа {cause_num}")
        if date:
            title_parts.append(f"від {date}")
        title = " — ".join(title_parts) if title_parts else f"Decision {doc_id}"

        return {
            "_id": f"UA-REYESTR-{doc_id}",
            "_source": "UA/REYESTR-CourtDecisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": doc_url if doc_url else f"https://reyestr.court.gov.ua/Review/{doc_id}",
            "court_code": court_code,
            "court_name": court_name,
            "cause_num": cause_num,
            "judge": judge,
        }

    def _iter_documents_from_zip(self, zip_path: str, limit: Optional[int] = None):
        """Iterate document rows from a downloaded ZIP file."""
        with zipfile.ZipFile(zip_path) as zf:
            self._courts = self._load_courts_from_zip(zf)
            logger.info("Loaded %d court mappings", len(self._courts))

            with zf.open("documents.csv") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8"), delimiter="\t"
                )
                count = 0
                for row in reader:
                    doc_url = row.get("doc_url", "").strip()
                    if not doc_url:
                        continue  # Skip records without full text URL

                    text = self._fetch_rtf_text(doc_url)
                    if not text or len(text) < 100:
                        continue

                    row["text"] = text
                    yield row

                    count += 1
                    if limit and count >= limit:
                        return

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all court decisions from the 2026 dataset."""
        logger.info("Downloading 2026 court decisions ZIP (~92MB)...")
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            resp = self.http.get(DATASET_URL_2026, stream=True, timeout=600)
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp_path = tmp.name

        logger.info("Downloaded to %s", tmp_path)

        for row in self._iter_documents_from_zip(tmp_path):
            record = self.normalize(row)
            if record["text"] and len(record["text"]) > 100:
                yield record

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Same as fetch_all for now - uses latest year."""
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Test connectivity to data.gov.ua and RTF server."""
        # Test data.gov.ua
        resp = self.http.get(DATASET_URL_2026, stream=True, timeout=30)
        if resp.status_code != 200:
            logger.error("data.gov.ua test failed: %s", resp.status_code)
            return False
        resp.close()
        logger.info("data.gov.ua accessible")

        # Test RTF server
        test_url = "http://od.reyestr.court.gov.ua/files/66/2ae08362192bc6abb8fac6bda6e44d07.rtf"
        resp2 = self.http.get(test_url, timeout=30)
        if resp2.status_code != 200:
            logger.error("RTF server test failed: %s", resp2.status_code)
            return False
        logger.info("RTF server accessible")
        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UA/REYESTR-CourtDecisions bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch 12 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UACourtDecisionsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).resolve().parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap" and args.sample:
        logger.info("Downloading 2026 court decisions ZIP for sampling...")
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            resp = scraper.http.get(DATASET_URL_2026, stream=True, timeout=600)
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp_path = tmp.name

        logger.info("Downloaded ZIP, extracting samples...")
        count = 0
        target = 12
        for row in scraper._iter_documents_from_zip(tmp_path, limit=target):
            record = scraper.normalize(row)
            if record["text"] and len(record["text"]) > 100:
                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info(
                    "Sample %d/%d: %s (%d chars, court: %s)",
                    count, target, record["_id"], len(record["text"]),
                    record.get("court_name", "?")
                )
                if count >= target:
                    break

        logger.info("Sample complete: %d records saved to %s", count, sample_dir)

        # Clean up
        import os
        os.unlink(tmp_path)
    else:
        count = 0
        for record in scraper.fetch_all():
            scraper.storage.save(record)
            count += 1
            if count % 100 == 0:
                logger.info("Saved %d records", count)
        logger.info("Bootstrap complete: %d records", count)


if __name__ == "__main__":
    main()
