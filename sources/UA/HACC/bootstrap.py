#!/usr/bin/env python3
"""
UA/HACC -- High Anti-Corruption Court of Ukraine (ВАКС)

Fetches HACC court decisions from the EDRSR registry via data.gov.ua
(metadata CSV) and od.reyestr.court.gov.ua (full text RTF/HTML).

Subset of UA/EDRSR filtered to HACC court codes:
  - 4910: HACC first instance
  - 4911: HACC Appeals Chamber (Апеляційна палата)

HACC was established April 2019, so only ZIPs from 2019 onward are used.

Usage:
  python bootstrap.py bootstrap            # Full pull (all HACC decisions)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
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
logger = logging.getLogger("legal-data-hunter.UA.HACC")

# data.gov.ua CKAN API
CKAN_BASE = "https://data.gov.ua/api/3/action"

# Full text open data subdomain (HTTP only, no CAPTCHA)
OD_BASE = "http://od.reyestr.court.gov.ua"

# HACC court codes
HACC_COURT_CODES = {"4910", "4911"}

# HACC established in 2019
HACC_START_YEAR = 2019

# Max concurrent full-text downloads
MAX_WORKERS = 5


class HACCScraper(BaseScraper):
    """
    Scraper for UA/HACC -- High Anti-Corruption Court of Ukraine.
    Country: UA
    URL: https://hcac.court.gov.ua

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
        self._courts = {}
        self._justice_kinds = {}
        self._judgment_forms = {}

    # -- Data access methods ------------------------------------------------

    def _find_dataset_zips(self) -> list:
        """Find EDRSR yearly ZIP URLs from data.gov.ua (2019+ only)."""
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
                        m = re.search(r"edrsr_data_(\d{4})", res["url"])
                        year = int(m.group(1)) if m else 0
                        if year < HACC_START_YEAR:
                            continue
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
        """Load reference CSV tables from ZIP."""
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
        """Parse documents.csv, filtering to HACC court codes only."""
        rows = []
        with zf.open("documents.csv") as f:
            content = f.read().decode("utf-8")
            reader = csv.DictReader(io.StringIO(content), delimiter="\t")
            for row in reader:
                court_code = row.get("court_code", "").strip().strip('"')
                if court_code not in HACC_COURT_CODES:
                    continue
                if row.get("status", "").strip() == "1" and row.get("doc_url", "").strip():
                    rows.append(row)
                    if limit and len(rows) >= limit:
                        break
        return rows

    def _fetch_full_text(self, doc_url: str) -> Optional[str]:
        """Fetch and extract clean text from od.reyestr.court.gov.ua (RTF or HTML)."""
        try:
            resp = self.client.get(doc_url, timeout=20)
            if resp is None or resp.status_code != 200:
                return None

            raw = resp.content

            # Detect format by URL extension or content sniff
            if doc_url.endswith(".rtf") or raw[:5] == b"{\\rtf":
                return self._extract_rtf(raw)
            else:
                return self._extract_html(raw)
        except Exception as e:
            logger.debug(f"Failed to fetch full text from {doc_url}: {e}")
            return None

    @staticmethod
    def _extract_rtf(raw: bytes) -> Optional[str]:
        """Extract text from RTF content using striprtf."""
        from striprtf.striprtf import rtf_to_text
        try:
            rtf_str = raw.decode("cp1251", errors="replace")
            text = rtf_to_text(rtf_str)
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n\s*\n", "\n\n", text)
            text = text.strip()
            return text if len(text) > 20 else None
        except Exception as e:
            logger.debug(f"RTF extraction failed: {e}")
            return None

    @staticmethod
    def _extract_html(raw: bytes) -> Optional[str]:
        """Extract text from HTML content."""
        try:
            html_str = raw.decode("windows-1251", errors="replace")
        except (UnicodeDecodeError, AttributeError):
            html_str = raw.decode("utf-8", errors="replace")
        body_match = re.search(r"<BODY[^>]*>(.*?)</BODY>", html_str, re.DOTALL | re.IGNORECASE)
        if not body_match:
            return None
        body = body_match.group(1)
        text = re.sub(r"<[^>]+>", " ", body)
        text = html_module.unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n", "\n\n", text)
        text = text.strip()
        return text if len(text) > 20 else None

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

        adj_date = raw.get("adjudication_date", "").strip().strip('"')
        pub_date = raw.get("date_publ", "").strip().strip('"')

        date_iso = self._parse_date(adj_date)
        date_pub_iso = self._parse_date(pub_date)

        text = raw.get("_full_text", "")
        if not text:
            return None

        court_name = self._courts.get(court_code, "")
        if not court_name:
            court_name = "HACC" if court_code == "4910" else "HACC Appeals Chamber"
        title = cause_num
        if court_name:
            title = f"{cause_num} — {court_name}"

        return {
            "_id": f"UA-HACC-{doc_id}",
            "_source": "UA/HACC",
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

    @staticmethod
    def _parse_date(date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        return m.group(1) if m else None

    # -- Fetch methods -----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all HACC court decisions with full text."""
        datasets = self._find_dataset_zips()
        if not datasets:
            logger.error("No EDRSR datasets found on data.gov.ua")
            return

        logger.info(f"Found {len(datasets)} datasets (2019+)")

        for ds in datasets:
            logger.info(f"Processing year {ds['year']} ({ds['size_mb']:.0f} MB)")
            zf = self._download_zip(ds["url"])
            if not zf:
                continue

            self._load_reference_tables(zf)
            rows = self._parse_documents_csv(zf)
            logger.info(f"Year {ds['year']}: {len(rows)} HACC documents")

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
                        if fetched % 100 == 0:
                            logger.info(f"Fetched full text for {fetched} HACC documents")
                        yield result
                except Exception as e:
                    logger.debug(f"Error: {e}")
                    continue

        logger.info(f"Batch complete: {fetched}/{len(rows)} with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield HACC decisions published since the given date."""
        since_str = since.strftime("%Y-%m-%d")
        datasets = self._find_dataset_zips()
        current_year = datetime.now().year
        for ds in datasets:
            if ds["year"] >= current_year - 1:
                zf = self._download_zip(ds["url"])
                if not zf:
                    continue
                self._load_reference_tables(zf)
                rows = self._parse_documents_csv(zf)
                filtered = [
                    r for r in rows
                    if self._parse_date(r.get("date_publ", "")) and
                    self._parse_date(r.get("date_publ", "")) >= since_str
                ]
                logger.info(f"Year {ds['year']}: {len(filtered)} HACC docs since {since_str}")
                yield from self._fetch_batch_with_text(filtered)

    # -- Sample mode -------------------------------------------------------

    def bootstrap(self, sample_mode: bool = False, sample_size: int = 15) -> dict:
        """Override bootstrap for sample mode."""
        if not sample_mode:
            return super().bootstrap(sample_mode=False, sample_size=sample_size)

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

        datasets = self._find_dataset_zips()
        if not datasets:
            stats["error_message"] = "No datasets found"
            stats["finished_at"] = datetime.now(timezone.utc).isoformat()
            return stats

        # Use most recent year first (most likely to have HACC data)
        for ds in reversed(datasets):
            if len(sample_records) >= sample_size:
                break

            logger.info(f"Sample mode: trying {ds['year']} dataset ({ds['size_mb']:.0f} MB)")
            zf = self._download_zip(ds["url"])
            if not zf:
                continue

            self._load_reference_tables(zf)
            rows = self._parse_documents_csv(zf, limit=50)
            logger.info(f"Year {ds['year']}: {len(rows)} HACC rows found")

            if not rows:
                continue

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
                    logger.info(
                        f"Sample {len(sample_records)}/{sample_size}: "
                        f"{record['title'][:60]} ({len(record['text'])} chars)"
                    )

                time.sleep(0.3)

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

    parser = argparse.ArgumentParser(description="UA/HACC Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = HACCScraper()

    if args.command == "test-api":
        logger.info("Testing data.gov.ua dataset discovery...")
        datasets = scraper._find_dataset_zips()
        logger.info(f"Found {len(datasets)} datasets (2019+):")
        for ds in datasets:
            logger.info(f"  {ds['year']}: {ds['size_mb']:.0f} MB")

        if datasets:
            # Test with most recent year
            ds = datasets[-1]
            logger.info(f"Testing HACC filter on {ds['year']}...")
            zf = scraper._download_zip(ds["url"])
            if zf:
                scraper._load_reference_tables(zf)
                rows = scraper._parse_documents_csv(zf, limit=5)
                logger.info(f"Found {len(rows)} HACC rows (limited to 5)")
                if rows:
                    doc_url = rows[0].get("doc_url", "").strip().strip('"')
                    text = scraper._fetch_full_text(doc_url)
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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
