#!/usr/bin/env python3
"""
UA/SupremeCourt -- Supreme Court of Ukraine Data Fetcher

Fetches decisions of the Supreme Court of Ukraine (Верховний Суд) and all
cassation courts from the Unified State Register of Court Decisions (ЄДРСР).

Strategy:
  - Download: Annual bulk CSV from data.gov.ua (daily updates for current year)
  - Filter: Cassation courts only (instance_code = 1)
  - Full text: Download RTF files from od.reyestr.court.gov.ua

Data Source:
  - Base: https://data.gov.ua/dataset/16ab7f06-7414-405f-8354-0a492475272d (2026)
  - RTF: http://od.reyestr.court.gov.ua/files/XX/<hash>.rtf

Court Codes (Cassation Instance):
  - 5001: Вищий господарський суд України (High Commercial Court)
  - 9901: Верховний Суд (Supreme Court)
  - 9911: Касаційний господарський суд ВС (Commercial Cassation Court)
  - 9921: Касаційний адміністративний суд ВС (Administrative Cassation Court)
  - 9931: Касаційний цивільний суд ВС (Civil Cassation Court)
  - 9941: Касаційний кримінальний суд ВС (Criminal Cassation Court)
  - 9951: Велика Палата Верховного Суду (Grand Chamber of SC)
  - 9991: Вищий адміністративний суд України (High Administrative Court)
  - 9992: Вищий спеціалізований суд (High Specialized Court)
  - 9999: Верховний Суд України (Supreme Court of Ukraine - old)

Rate Limits:
  - 2 requests/second conservative, no documented limits
  - Concurrent RTF downloads: 3 workers

Usage:
  python bootstrap.py bootstrap           # Full initial pull (current year)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick API connectivity test
"""

import csv
import io
import json
import logging
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from striprtf.striprtf import rtf_to_text

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UA.SupremeCourt")

# API configuration
DATA_GOV_BASE = "https://data.gov.ua"
RTF_BASE = "http://od.reyestr.court.gov.ua"

# Current year dataset (2026)
DATASET_2026_ZIP = (
    "https://data.gov.ua/dataset/16ab7f06-7414-405f-8354-0a492475272d/"
    "resource/b1a4ac1c-b17a-4988-8e6d-dedae8b2dd63/download/edrsr_data_2026.zip"
)

# Cassation court codes (Supreme Court level)
CASSATION_COURT_CODES = {
    "5001",  # Вищий господарський суд України
    "9901",  # Верховний Суд
    "9911",  # Касаційний господарський суд ВС
    "9921",  # Касаційний адміністративний суд ВС
    "9931",  # Касаційний цивільний суд ВС
    "9941",  # Касаційний кримінальний суд ВС
    "9951",  # Велика Палата Верховного Суду
    "9991",  # Вищий адміністративний суд України
    "9992",  # Вищий спеціалізований суд
    "9999",  # Верховний Суд України (old)
}

# Reference data mappings (from CSV files in archive)
JUDGMENT_FORMS = {
    "1": "Вирок (Verdict)",
    "2": "Постанова (Resolution)",
    "3": "Рішення (Decision)",
    "4": "Судовий наказ (Court Order)",
    "5": "Ухвала (Ruling)",
    "6": "Окрема ухвала (Separate Ruling)",
    "7": "Додаткове рішення (Additional Decision)",
    "10": "Окрема думка (Separate Opinion)",
}

JUSTICE_KINDS = {
    "1": "civil",
    "2": "criminal",
    "3": "commercial",
    "4": "administrative",
    "5": "administrative_offense",
}

JUSTICE_KINDS_UK = {
    "1": "Цивільне",
    "2": "Кримінальне",
    "3": "Господарське",
    "4": "Адміністративне",
    "5": "Адмінправопорушення",
}


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for UA/SupremeCourt -- Supreme Court of Ukraine.
    Country: UA
    URL: https://supreme.court.gov.ua

    Data types: case_law
    Auth: none (open data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "*/*",
        })
        self.last_request_time = 0

        # Cache for reference data
        self._courts_cache: Dict[str, str] = {}
        self._categories_cache: Dict[str, str] = {}

    def _rate_limit(self, delay: float = 0.5):
        """Enforce rate limiting with configurable delay."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time

        if elapsed < delay:
            time.sleep(delay - elapsed)

        self.last_request_time = time.time()

    def _download_zip(self, url: str) -> Optional[bytes]:
        """Download ZIP archive from data.gov.ua."""
        logger.info(f"Downloading archive from {url}...")
        try:
            resp = self.session.get(url, timeout=300, stream=True)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.error(f"Failed to download ZIP: {e}")
            return None

    def _load_reference_data(self, zip_content: bytes):
        """Load courts and categories reference tables from ZIP."""
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            # Load courts
            with zf.open("courts.csv") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8"),
                    delimiter="\t",
                )
                for row in reader:
                    code = row.get("court_code", "")
                    name = row.get("name", "").strip('"')
                    self._courts_cache[code] = name

            # Load categories
            with zf.open("cause_categories.csv") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8"),
                    delimiter="\t",
                )
                for row in reader:
                    code = row.get("category_code", "")
                    name = row.get("name", "").strip('"')
                    self._categories_cache[code] = name

        logger.info(
            f"Loaded reference data: {len(self._courts_cache)} courts, "
            f"{len(self._categories_cache)} categories"
        )

    def _fetch_rtf_text(self, url: str) -> str:
        """
        Download RTF file and extract plain text.

        Uses striprtf library to convert RTF to plain text.
        """
        if not url:
            return ""

        try:
            self._rate_limit(0.5)
            resp = self.session.get(url, timeout=60)

            if resp.status_code == 404:
                return ""

            resp.raise_for_status()

            # RTF files are typically CP1251 encoded
            try:
                content = resp.content.decode("cp1251")
            except UnicodeDecodeError:
                content = resp.content.decode("utf-8", errors="ignore")

            # Extract text from RTF
            text = rtf_to_text(content)
            return text.strip()

        except Exception as e:
            logger.warning(f"Failed to fetch RTF from {url}: {e}")
            return ""

    def _parse_date(self, date_str: str) -> str:
        """
        Parse date from CSV format to ISO 8601.

        Input: "2025-12-31 00:00:00+02" or similar
        Output: "2025-12-31"
        """
        if not date_str:
            return ""

        date_str = date_str.strip('"')

        try:
            # Try parsing with timezone
            if "+" in date_str or "-" in date_str[10:]:
                dt = datetime.fromisoformat(date_str.replace(" ", "T"))
            else:
                dt = datetime.fromisoformat(date_str)
            return dt.date().isoformat()
        except Exception:
            # Fallback: just take first 10 chars
            if len(date_str) >= 10:
                return date_str[:10]
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Supreme Court documents from current year dataset.

        Downloads the 2026 bulk CSV, filters to cassation courts,
        and yields each row as raw data.
        """
        logger.info("Starting Supreme Court data fetch...")

        # Download and extract ZIP
        zip_content = self._download_zip(DATASET_2026_ZIP)
        if not zip_content:
            logger.error("Failed to download dataset")
            return

        # Load reference data
        self._load_reference_data(zip_content)

        # Parse documents CSV
        fetched = 0
        skipped = 0

        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            with zf.open("documents.csv") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8"),
                    delimiter="\t",
                )

                for row in reader:
                    court_code = row.get("court_code", "")

                    # Filter to cassation courts only
                    if court_code not in CASSATION_COURT_CODES:
                        skipped += 1
                        continue

                    # Skip if no doc_url (no full text available)
                    doc_url = row.get("doc_url", "")
                    if not doc_url:
                        skipped += 1
                        continue

                    fetched += 1
                    yield row

                    if fetched % 500 == 0:
                        logger.info(
                            f"Yielded {fetched} Supreme Court decisions "
                            f"(skipped {skipped} non-cassation/no-text)"
                        )

        logger.info(
            f"Fetch complete: {fetched} Supreme Court decisions, "
            f"{skipped} skipped"
        )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given date.

        Uses date_publ field from CSV.
        """
        since_str = since.date().isoformat()
        logger.info(f"Checking for updates since {since_str}...")

        # Download current year data
        zip_content = self._download_zip(DATASET_2026_ZIP)
        if not zip_content:
            return

        self._load_reference_data(zip_content)

        count = 0
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            with zf.open("documents.csv") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8"),
                    delimiter="\t",
                )

                for row in reader:
                    court_code = row.get("court_code", "")

                    if court_code not in CASSATION_COURT_CODES:
                        continue

                    doc_url = row.get("doc_url", "")
                    if not doc_url:
                        continue

                    # Check publication date
                    date_publ = self._parse_date(row.get("date_publ", ""))
                    if date_publ and date_publ >= since_str:
                        count += 1
                        yield row

        logger.info(f"Found {count} decisions since {since_str}")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw CSV row into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from RTF endpoint.
        """
        doc_id = raw.get("doc_id", "")
        court_code = raw.get("court_code", "")
        cause_num = raw.get("cause_num", "")
        doc_url = raw.get("doc_url", "")

        # Create unique document ID
        unique_id = f"UA-SC-{doc_id}"

        # Get dates
        adjudication_date = self._parse_date(raw.get("adjudication_date", ""))
        receipt_date = self._parse_date(raw.get("receipt_date", ""))
        date_publ = self._parse_date(raw.get("date_publ", ""))

        # Get court name from cache
        court_name = self._courts_cache.get(court_code, f"Court {court_code}")

        # Get judgment form
        judgment_code = raw.get("judgment_code", "")
        judgment_form = JUDGMENT_FORMS.get(judgment_code, "Unknown")

        # Get justice kind
        justice_kind_code = raw.get("justice_kind", "")
        justice_kind = JUSTICE_KINDS.get(justice_kind_code, "other")
        justice_kind_uk = JUSTICE_KINDS_UK.get(justice_kind_code, "Інше")

        # Get category
        category_code = raw.get("category_code", "")
        category = self._categories_cache.get(category_code, "")

        # Get judge
        judge = raw.get("judge", "").strip('"')

        # Build public URL
        public_url = f"https://reyestr.court.gov.ua/Review/{doc_id}"

        # CRITICAL: Fetch full text from RTF
        full_text = self._fetch_rtf_text(doc_url)

        if not full_text:
            logger.warning(f"No full text for doc_id={doc_id}")

        # Build title from available data
        title_parts = []
        if judgment_form:
            title_parts.append(judgment_form.split(" (")[0])
        if cause_num:
            title_parts.append(f"у справі {cause_num}")
        if adjudication_date:
            title_parts.append(f"від {adjudication_date}")
        title = " ".join(title_parts) if title_parts else f"Рішення {doc_id}"

        return {
            # Required base fields
            "_id": unique_id,
            "_source": "UA/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": adjudication_date,
            "url": public_url,
            # Case law specific fields
            "court": court_name,
            "court_code": court_code,
            "case_number": cause_num,
            "judgment_type": judgment_form,
            "judgment_code": judgment_code,
            "jurisdiction": justice_kind,
            "jurisdiction_uk": justice_kind_uk,
            "category": category,
            "category_code": category_code,
            "judge": judge,
            # Dates
            "adjudication_date": adjudication_date,
            "receipt_date": receipt_date,
            "publication_date": date_publ,
            # Source fields
            "doc_id": doc_id,
            "doc_url": doc_url,
            "status": raw.get("status", ""),
            # Metadata
            "language": "uk",
            "country": "UA",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Ukraine Supreme Court data source...")

        # Test ZIP download (just headers)
        print("\n1. Testing data.gov.ua ZIP access...")
        try:
            resp = self.session.head(DATASET_2026_ZIP, timeout=30)
            size_mb = int(resp.headers.get("Content-Length", 0)) / (1024 * 1024)
            print(f"   Status: {resp.status_code}")
            print(f"   Size: {size_mb:.1f} MB")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Download and sample data
        print("\n2. Downloading and sampling data...")
        zip_content = self._download_zip(DATASET_2026_ZIP)
        if not zip_content:
            return

        self._load_reference_data(zip_content)

        # Count records
        total = 0
        cassation = 0
        with_url = 0
        sample_rows = []

        with zipfile.ZipFile(io.BytesIO(zip_content)) as zf:
            with zf.open("documents.csv") as f:
                reader = csv.DictReader(
                    io.TextIOWrapper(f, encoding="utf-8"),
                    delimiter="\t",
                )
                for row in reader:
                    total += 1
                    if row.get("court_code", "") in CASSATION_COURT_CODES:
                        cassation += 1
                        if row.get("doc_url", ""):
                            with_url += 1
                            if len(sample_rows) < 3:
                                sample_rows.append(row)

        print(f"   Total records in archive: {total}")
        print(f"   Cassation court records: {cassation}")
        print(f"   With RTF URL (full text): {with_url}")

        # Test RTF download
        print("\n3. Testing RTF full text download...")
        if sample_rows:
            row = sample_rows[0]
            doc_url = row.get("doc_url", "")
            print(f"   URL: {doc_url}")
            text = self._fetch_rtf_text(doc_url)
            print(f"   Text length: {len(text)} characters")
            if text:
                print(f"   Preview: {text[:300]}...")

        # Show sample records
        print("\n4. Sample records:")
        for i, row in enumerate(sample_rows[:2]):
            print(f"\n   Record {i+1}:")
            print(f"   doc_id: {row.get('doc_id')}")
            print(f"   court: {self._courts_cache.get(row.get('court_code', ''), '?')}")
            print(f"   case: {row.get('cause_num')}")
            print(f"   date: {row.get('adjudication_date')}")
            print(f"   judge: {row.get('judge', '')[:50]}")

        print("\nAPI test complete!")


def main():
    scraper = SupremeCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
