#!/usr/bin/env python3
"""
RU/PravoGovRu -- Russia Official Legal Portal Data Fetcher

Fetches Russian federal legislation from the RusLawOD dataset on HuggingFace.
The dataset contains 304,382 legislative texts with full text extracted from
pravo.gov.ru's IPS system (1991-2025).

Strategy:
  - Bootstrap: Streams parquet files from HuggingFace (irlspbru/RusLawOD).
    Uses fsspec + pyarrow for efficient remote parquet reads.
  - Update: Re-streams the dataset (no incremental API available).
  - Sample: Fetches 15 records from the smallest parquet file for validation.

Dataset: https://huggingface.co/datasets/irlspbru/RusLawOD
Paper: https://arxiv.org/html/2406.04855v2

Usage:
  python bootstrap.py bootstrap            # Full fetch (304K+ records)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Re-fetch (same as bootstrap)
"""

import sys
import re
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fsspec
import pyarrow.parquet as pq

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RU.PravoGovRu")

# HuggingFace dataset info
HF_DATASET = "irlspbru/RusLawOD"
HF_BASE_URL = "https://huggingface.co/datasets/irlspbru/RusLawOD/resolve/main"
PARQUET_FILES = [f"ruslawod_{i:02d}.parquet" for i in range(1, 12)]

# Columns we need from the parquet files
COLUMNS = [
    "pravogovruNd",
    "headingIPS",
    "textIPS",
    "docdateIPS",
    "docNumberIPS",
    "doc_typeIPS",
    "doc_author_normal_formIPS",
    "issuedByIPS",
    "signedIPS",
    "statusIPS",
    "classifierByIPS",
    "is_widely_used",
]

# Russian date format: DD.MM.YYYY
DATE_RE = re.compile(r"^(\d{2})\.(\d{2})\.(\d{4})$")


def parse_russian_date(date_str: str) -> str:
    """Convert DD.MM.YYYY to ISO 8601 (YYYY-MM-DD)."""
    if not date_str:
        return None
    m = DATE_RE.match(date_str.strip())
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return None


def clean_text(text: str) -> str:
    """Clean legislation text: strip IPS markup, normalize whitespace."""
    if not text:
        return ""
    # Strip IPS-specific markup: <ref nd="...">...</ref>, <table>...</table>, <1>, <2>, etc.
    text = re.sub(r"<ref[^>]*>", "", text)
    text = re.sub(r"</ref>", "", text)
    text = re.sub(r"</?table>", "", text)
    text = re.sub(r"<\d+>", "", text)
    # Strip any remaining HTML-like tags
    text = re.sub(r"<[^>]+>", "", text)
    # Normalize whitespace
    text = re.sub(r"\r\n", "\n", text)
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class PravoGovRuScraper(BaseScraper):
    """
    Scraper for RU/PravoGovRu -- Russian Federal Legislation.
    Country: RU
    URL: https://pravo.gov.ru/

    Data types: legislation
    Auth: none (open dataset on HuggingFace)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.fs = fsspec.filesystem("https")

    def _read_parquet_file(self, filename: str) -> Generator[dict, None, None]:
        """Read rows from a remote parquet file on HuggingFace."""
        url = f"{HF_BASE_URL}/{filename}"
        logger.info(f"Opening parquet file: {filename}")
        try:
            f = self.fs.open(url)
            pf = pq.ParquetFile(f)
            num_rg = pf.metadata.num_row_groups
            logger.info(f"  {filename}: {num_rg} row groups")

            for rg_idx in range(num_rg):
                table = pf.read_row_group(rg_idx, columns=COLUMNS)
                batch = table.to_pydict()
                num_rows = table.num_rows
                logger.info(f"  Row group {rg_idx}: {num_rows} rows")

                for i in range(num_rows):
                    row = {col: batch[col][i] for col in batch}
                    yield row

            f.close()
        except Exception as e:
            logger.error(f"Error reading {filename}: {e}")
            raise

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all parquet files."""
        total = 0
        for filename in PARQUET_FILES:
            for row in self._read_parquet_file(filename):
                total += 1
                yield row
                if total % 10000 == 0:
                    logger.info(f"  Progress: {total} records yielded")
        logger.info(f"Total records yielded: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all (no incremental endpoint available)."""
        logger.info("No incremental update available; re-fetching all data")
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform a raw dataset row into standardized schema."""
        nd = raw.get("pravogovruNd", "")
        if not nd:
            return None

        text = clean_text(raw.get("textIPS", ""))
        if not text:
            return None

        title = (raw.get("headingIPS") or "").strip()
        date_str = raw.get("docdateIPS", "")
        iso_date = parse_russian_date(date_str)

        return {
            "_id": f"RU-{nd}",
            "_source": "RU/PravoGovRu",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "pravogovruNd": nd,
            "title": title,
            "text": text,
            "date": iso_date,
            "url": f"https://pravo.gov.ru/proxy/ips/?docbody=&nd={nd}",
            "doc_type": raw.get("doc_typeIPS", ""),
            "doc_number": raw.get("docNumberIPS", ""),
            "author": raw.get("doc_author_normal_formIPS", ""),
            "issued_by": raw.get("issuedByIPS", ""),
            "signed": raw.get("signedIPS", ""),
            "status": raw.get("statusIPS", ""),
            "classifier": raw.get("classifierByIPS", ""),
            "is_widely_used": raw.get("is_widely_used", False),
        }


# ── CLI entry point ─────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="RU/PravoGovRu data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch only a small sample for validation",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=15,
        help="Number of sample records to fetch",
    )
    args = parser.parse_args()

    scraper = PravoGovRuScraper()

    if args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        print(f"\nBootstrap complete:")
        for k, v in stats.items():
            print(f"  {k}: {v}")

    elif args.command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete:")
        for k, v in stats.items():
            print(f"  {k}: {v}")

    elif args.command == "test":
        print("Testing RusLawOD dataset access...")
        # Just read first 3 records from smallest file
        for i, row in enumerate(scraper._read_parquet_file("ruslawod_11.parquet")):
            record = scraper.normalize(row)
            if record:
                print(f"\nRecord {i+1}:")
                print(f"  ID: {record['_id']}")
                print(f"  Title: {record['title'][:100]}")
                print(f"  Date: {record['date']}")
                print(f"  Type: {record['doc_type']}")
                print(f"  Author: {record['author']}")
                print(f"  Text length: {len(record['text'])} chars")
                print(f"  Text preview: {record['text'][:150]}...")
            if i >= 2:
                break
        print("\nTest complete.")


if __name__ == "__main__":
    main()
