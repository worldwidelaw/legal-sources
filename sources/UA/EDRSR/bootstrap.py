#!/usr/bin/env python3
"""
UA/EDRSR -- Unified State Register of Court Decisions (ЄДРСР)

Fetches Ukrainian court decisions from data.gov.ua (metadata CSV) and
od.reyestr.court.gov.ua (full text HTML).

Strategy:
  - Download yearly CSV ZIP from data.gov.ua (tab-delimited, UTF-8)
  - Parse documents.csv to get doc_id, metadata, and doc_url
  - Fetch full text HTML from od.reyestr.court.gov.ua
  - Strip HTML tags, extract clean text

Data: 132M+ decisions (2006-present). Largest court register in Europe.
License: Creative Commons Attribution.

Usage:
  python bootstrap.py bootstrap            # Full pull (WARNING: 132M+ docs)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records (2006 data)
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import time
import io
import csv
import re
import html as html_module
import zipfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UA.EDRSR")

# data.gov.ua CKAN API
CKAN_BASE = "https://data.gov.ua/api/3/action"

# Full text open data subdomain (HTTP only, no CAPTCHA)
OD_BASE = "http://od.reyestr.court.gov.ua"

# Max concurrent full-text downloads
MAX_WORKERS = 5

# CSV columns in documents.csv
CSV_COLUMNS = [
    "doc_id", "court_code", "judgment_code", "justice_kind",
    "category_code", "cause_num", "adjudication_date", "receipt_date",
    "judge", "doc_url", "status", "date_publ",
]


class EDRSRScraper(BaseScraper):
    """
    Scraper for UA/EDRSR -- Ukrainian Unified State Register of Court Decisions.
    Country: UA
    URL: https://reyestr.court.gov.ua

    Data types: case_law
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=30,
        )
        # Reference tables loaded from ZIP
        self._courts = {}
        self._justice_kinds = {}
        self._judgment_forms = {}
        self._categories = {}

    # -- Data access methods ------------------------------------------------

    def _find_dataset_zips(self) -> list:
        """Find all EDRSR yearly ZIP URLs from data.gov.ua."""
        url = f"{CKAN_BASE}/package_search"
        params = {"q": "Єдиний державний реєстр судових рішень", "rows": 30}
        try:
            resp = self.client.get(url, params=params, timeout=30)
            if resp is None or resp.status_code != 200:
                return []
            data = resp.json()
            zips = []
            for pkg in data.get("result", {}).get("results", []):
                for res in pkg.get("resources", []):
                    if res.get("format") == "ZIP" and "edrsr_data" in res.get("url", ""):
                        # Extract year from URL
                        m = re.search(r"edrsr_data_(\d{4})", res["url"])
                        year = int(m.group(1)) if m else 0
                        size = int(res.get("size", 0))
                        zips.append({
                            "year": year,
                            "url": res["url"],
                            "size": size,
                            "size_mb": size / (1024 * 1024),
                        })
            # Deduplicate by year (keep first/smallest)
            seen = {}
            for z in sorted(zips, key=lambda x: x["size"]):
                if z["year"] not in seen:
                    seen[z["year"]] = z
            return sorted(seen.values(), key=lambda x: x["year"])
        except Exception as e:
            logger.warning(f"Failed to list datasets: {e}")
            return []

    def _download_zip(self, url: str) -> Optional[zipfile.ZipFile]:
        """Download and open a ZIP file."""
        logger.info(f"Downloading ZIP: {url}")
        try:
            resp = self.client.get(url, timeout=300)
            if resp is None or resp.status_code != 200:
                return None
            return zipfile.ZipFile(io.BytesIO(resp.content))
        except Exception as e:
            logger.warning(f"Failed to download ZIP: {e}")
            return None

    def _load_reference_tables(self, zf: zipfile.ZipFile):
        """Load reference CSV tables (courts, justice_kinds, etc.) from ZIP."""
        for name in zf.namelist():
            if name == "courts.csv":
                self._courts = self._parse_ref_csv(zf, name, "court_code", "court_name")
            elif name == "justice_kinds.csv":
                self._justice_kinds = self._parse_ref_csv(zf, name, "justice_kind", "justice_name")
            elif name == "judgment_forms.csv":
                self._judgment_forms = self._parse_ref_csv(zf, name, "judgment_code", "judgment_name")

    def _parse_ref_csv(self, zf: zipfile.ZipFile, name: str, key_col: str, val_col: str) -> dict:
        """Parse a reference CSV into a lookup dict."""
        result = {}
        try:
            with zf.open(name) as f:
                content = f.read().decode("utf-8")
                reader = csv.DictReader(io.StringIO(content), delimiter="\t")
                for row in reader:
                    k = row.get(key_col, "").strip()
                    v = row.get(val_col, "").strip()
                    if k:
                        result[k] = v
        except Exception as e:
            logger.debug(f"Failed to parse {name}: {e}")
        return result

    def _parse_documents_csv(self, zf: zipfile.ZipFile, limit: int = 0) -> list:
        """Parse documents.csv from ZIP, returning list of row dicts."""
        rows = []
        with zf.open("documents.csv") as f:
            content = f.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(content), delimiter="\t")
            for row in reader:
                # Only include published documents (status=1)
                if row.get("status", "").strip() == "1" and row.get("doc_url", "").strip():
                    rows.append(row)
                    if limit and len(rows) >= limit:
                        break
        return rows

    def _fetch_full_text(self, doc_url: str) -> Optional[str]:
        """Fetch and extract clean text from od.reyestr.court.gov.ua HTML."""
        try:
            resp = self.client.get(doc_url, timeout=20)
            if resp is None or resp.status_code != 200:
                return None

            # Try windows-1251 decoding (common for this endpoint)
            try:
                raw_html = resp.content.decode("windows-1251")
            except (UnicodeDecodeError, AttributeError):
                raw_html = resp.text

            # Extract body content
            body_match = re.search(r"<BODY[^>]*>(.*?)</BODY>", raw_html, re.DOTALL | re.IGNORECASE)
            if not body_match:
                return None

            body = body_match.group(1)
            # Strip HTML tags
            text = re.sub(r"<[^>]+>", " ", body)
            text = html_module.unescape(text)
            # Normalize whitespace but preserve paragraph breaks
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n\s*\n", "\n\n", text)
            text = text.strip()

            return text if len(text) > 20 else None
        except Exception as e:
            logger.debug(f"Failed to fetch full text from {doc_url}: {e}")
            return None

    # -- Normalize ---------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw CSV row + full text into standard schema."""
        doc_id = raw.get("doc_id", "").strip().strip('"')
        if not doc_id:
            return None

        cause_num = raw.get("cause_num", "").strip().strip('"')
        doc_url = raw.get("doc_url", "").strip().strip('"')
        court_code = raw.get("court_code", "").strip().strip('"')
        judge = raw.get("judge", "").strip().strip('"')
        justice_kind = raw.get("justice_kind", "").strip().strip('"')
        judgment_code = raw.get("judgment_code", "").strip().strip('"')

        # Parse dates
        adj_date = raw.get("adjudication_date", "").strip().strip('"')
        pub_date = raw.get("date_publ", "").strip().strip('"')

        date_iso = self._parse_date(adj_date)
        date_pub_iso = self._parse_date(pub_date)

        # Get full text (should already be fetched and attached)
        text = raw.get("_full_text", "")
        if not text:
            return None

        # Build title from case number + court
        court_name = self._courts.get(court_code, "")
        title = cause_num
        if court_name:
            title = f"{cause_num} — {court_name}"

        record = {
            "_id": f"UA-EDRSR-{doc_id}",
            "_source": "UA/EDRSR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": doc_id,
            "title": title or f"Decision {doc_id}",
            "text": text,
            "date": date_iso,
            "date_published": date_pub_iso,
            "url": doc_url,
            "court_code": court_code or None,
            "court_name": court_name or None,
            "judge": judge or None,
            "cause_num": cause_num or None,
            "justice_kind": self._justice_kinds.get(justice_kind, justice_kind) or None,
            "judgment_form": self._judgment_forms.get(judgment_code, judgment_code) or None,
        }
        return record

    @staticmethod
    def _parse_date(date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        # Format: "2006-06-01 00:00:00+03" or "2006-06-01"
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        return m.group(1) if m else None

    # -- Fetch methods -----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Ukrainian court decisions with full text.

        Downloads yearly CSV ZIPs from data.gov.ua, then fetches
        full text HTML for each document.
        """
        datasets = self._find_dataset_zips()
        if not datasets:
            logger.error("No EDRSR datasets found on data.gov.ua")
            return

        logger.info(f"Found {len(datasets)} yearly datasets")

        for ds in datasets:
            logger.info(f"Processing year {ds['year']} ({ds['size_mb']:.0f} MB)")
            zf = self._download_zip(ds["url"])
            if not zf:
                continue

            self._load_reference_tables(zf)
            rows = self._parse_documents_csv(zf)
            logger.info(f"Year {ds['year']}: {len(rows)} documents")

            # Fetch full text concurrently
            yield from self._fetch_batch_with_text(rows)

    def _fetch_batch_with_text(self, rows: list) -> Generator[dict, None, None]:
        """Fetch full text for a batch of rows concurrently."""
        fetched = 0

        def fetch_one(row):
            doc_url = row.get("doc_url", "").strip().strip('"')
            if doc_url:
                text = self._fetch_full_text(doc_url)
                if text:
                    row["_full_text"] = text
                    return row
            return None

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_one, row): row for row in rows}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        fetched += 1
                        if fetched % 1000 == 0:
                            logger.info(f"Fetched full text for {fetched} documents")
                        yield result
                except Exception as e:
                    logger.debug(f"Error: {e}")
                    continue

        logger.info(f"Batch complete: {fetched}/{len(rows)} with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions published since the given date."""
        since_str = since.strftime("%Y-%m-%d")
        # Only download the current year's dataset
        datasets = self._find_dataset_zips()
        current_year = datetime.now().year
        for ds in datasets:
            if ds["year"] >= current_year - 1:
                zf = self._download_zip(ds["url"])
                if not zf:
                    continue
                self._load_reference_tables(zf)
                rows = self._parse_documents_csv(zf)
                # Filter by publication date
                filtered = [
                    r for r in rows
                    if self._parse_date(r.get("date_publ", "")) and
                    self._parse_date(r.get("date_publ", "")) >= since_str
                ]
                logger.info(f"Year {ds['year']}: {len(filtered)} docs since {since_str}")
                yield from self._fetch_batch_with_text(filtered)

    # -- Sample mode override ----------------------------------------------

    def bootstrap(self, sample_mode: bool = False, sample_size: int = 15) -> dict:
        """Override bootstrap for sample mode to use 2006 (smallest dataset)."""
        if not sample_mode:
            return super().bootstrap(sample_mode=False, sample_size=sample_size)

        # For sample mode, download 2006 ZIP (11MB) and get 15 docs
        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "records_fetched": 0,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": 0,
        }

        sample_records = []
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        # Find the 2006 dataset (smallest)
        datasets = self._find_dataset_zips()
        ds_2006 = next((d for d in datasets if d["year"] == 2006), None)
        if not ds_2006:
            # Fallback: use smallest available
            ds_2006 = min(datasets, key=lambda x: x["size"]) if datasets else None

        if not ds_2006:
            stats["error_message"] = "No datasets found"
            stats["finished_at"] = datetime.now(timezone.utc).isoformat()
            return stats

        logger.info(f"Sample mode: using {ds_2006['year']} dataset ({ds_2006['size_mb']:.0f} MB)")
        zf = self._download_zip(ds_2006["url"])
        if not zf:
            stats["error_message"] = "Failed to download ZIP"
            stats["finished_at"] = datetime.now(timezone.utc).isoformat()
            return stats

        self._load_reference_tables(zf)

        # Get first 50 rows, try to get 15 with full text
        rows = self._parse_documents_csv(zf, limit=50)
        logger.info(f"Got {len(rows)} candidate rows, fetching full text...")

        for row in rows:
            if len(sample_records) >= sample_size:
                break

            doc_url = row.get("doc_url", "").strip().strip('"')
            if not doc_url:
                continue

            text = self._fetch_full_text(doc_url)
            if not text:
                stats["errors"] += 1
                continue

            row["_full_text"] = text
            record = self.normalize(row)
            if record:
                sample_records.append(record)
                stats["records_fetched"] += 1
                logger.info(f"Sample {len(sample_records)}/{sample_size}: {record['title'][:60]}")

            time.sleep(0.3)  # Gentle rate limiting

        # Save samples
        for i, rec in enumerate(sample_records):
            path = sample_dir / f"record_{i:04d}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)

        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(sample_records, f, ensure_ascii=False, indent=2)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        stats["sample_records_saved"] = len(sample_records)
        logger.info(f"Saved {len(sample_records)} sample records to {sample_dir}")

        self._save_status()
        return stats

    # -- CLI ---------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UA/EDRSR Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Concurrent workers")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = EDRSRScraper()

    if args.command == "test-api":
        logger.info("Testing data.gov.ua dataset discovery...")
        datasets = scraper._find_dataset_zips()
        logger.info(f"Found {len(datasets)} datasets:")
        for ds in datasets:
            logger.info(f"  {ds['year']}: {ds['size_mb']:.0f} MB")

        logger.info("Testing full-text endpoint...")
        test_url = "http://od.reyestr.court.gov.ua/files/479c08e113b776fbed961f1f39e4124e.html"
        text = scraper._fetch_full_text(test_url)
        if text:
            logger.info(f"Full text OK: {len(text)} chars")
        else:
            logger.error("Full text endpoint failed")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
