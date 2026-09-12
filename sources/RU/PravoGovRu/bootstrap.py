#!/usr/bin/env python3
"""
RU/PravoGovRu -- Russia Official Legal Portal Data Fetcher

Fetches Russian federal legislation from the RusLawOD dataset on HuggingFace.
The dataset contains 304,382 legislative texts with full text extracted from
pravo.gov.ru's IPS system (1991-2025).

Strategy:
  - Bootstrap: Streams parquet files from HuggingFace (irlspbru/RusLawOD).
    Uses fsspec + pyarrow for efficient remote parquet reads.
  - Update: Short-circuits on an unchanged dataset commit sha; otherwise
    streams and emits only rows dated on or after the cutoff.
  - Sample: Fetches 15 records from the smallest parquet file for validation.

Dataset: https://huggingface.co/datasets/irlspbru/RusLawOD
Paper: https://arxiv.org/html/2406.04855v2

Usage:
  python bootstrap.py bootstrap            # Full fetch (304K+ records)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental refresh
"""

import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fsspec
import pyarrow.parquet as pq
import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RU.PravoGovRu")

# HuggingFace dataset info
HF_DATASET = "irlspbru/RusLawOD"
HF_DATASET_API = f"https://huggingface.co/api/datasets/{HF_DATASET}"
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
        # Read before the walk, so a dataset revised mid-crawl is not recorded
        # as already consumed — the next refresh should still pick it up.
        current_sha, _ = self._dataset_revision()

        total = 0
        for filename in PARQUET_FILES:
            for row in self._read_parquet_file(filename):
                total += 1
                yield row
                if total % 10000 == 0:
                    logger.info(f"  Progress: {total} records yielded")

        # Baseline for the incremental refresh, written only on a clean finish.
        if current_sha:
            self._save_checkpoint(current_sha)
        logger.info(f"Total records yielded: {total}")

    # ── Incremental refresh (#1502) ───────────────────────────────────

    def _checkpoint_path(self) -> Path:
        return self.source_dir / "data" / "hf_checkpoint.json"

    def _load_checkpoint(self) -> dict:
        try:
            with open(self._checkpoint_path(), encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _save_checkpoint(self, sha: str) -> None:
        path = self._checkpoint_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"sha": sha,
                       "updated_at": datetime.now(timezone.utc).isoformat()}, f, indent=2)
        tmp.replace(path)  # atomic: a half-written checkpoint would skip real rows

    def _dataset_revision(self):
        """(commit sha, lastModified) of the HF dataset, or (None, None)."""
        try:
            r = requests.get(HF_DATASET_API, timeout=60)
            r.raise_for_status()
            info = r.json()
            return info.get("sha"), info.get("lastModified")
        except Exception as e:
            logger.warning(f"Could not read dataset revision: {e}")
            return None, None

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield only documents dated on or after `since`.

        Two filters, cheapest first. The previous implementation was
        `yield from self.fetch_all()`, which re-streamed all 11 parquet files
        and re-yielded ~304K rows on every refresh slot (#1502).

        * the HF dataset commit `sha` — this is a static research corpus, so an
          unchanged sha means no new legislation exists, answered in one request
          instead of streaming the whole dataset
        * `docdateIPS` per row — when the sha *has* moved the files still have to
          be read (parquet row groups are not date-partitioned), but only rows at
          or after the cutoff are emitted, so the expensive half — text cleaning
          and the downstream normalize/dedup of 304K rows — is skipped

        A row whose date will not parse is emitted rather than dropped: an
        unparseable date is a reason to look at the record, not to hide it.
        """
        checkpoint = self._load_checkpoint()
        prev_sha = checkpoint.get("sha")
        current_sha, last_modified = self._dataset_revision()

        if current_sha and prev_sha and current_sha == prev_sha:
            logger.info(
                f"Dataset revision unchanged ({current_sha[:12]}) since last run — "
                f"no new legislation. Skipped re-streaming {len(PARQUET_FILES)} parquet files."
            )
            return

        cutoff = as_date_str(since)

        # No checkpoint yet (first refresh after this fix): fall back to the
        # dataset's own last-modified stamp before streaming anything.
        if not prev_sha and cutoff and last_modified:
            modified_day = as_date_str(last_modified)
            if modified_day and modified_day < cutoff:
                logger.info(
                    f"Dataset last modified {modified_day}, before the {cutoff} cutoff "
                    f"— no new legislation."
                )
                self._save_checkpoint(current_sha or "")
                return

        logger.info(
            f"Revision {str(prev_sha)[:12]} -> {str(current_sha)[:12]}: "
            f"streaming for documents dated >= {cutoff or '(no cutoff)'}"
        )

        emitted = scanned = 0
        for filename in PARQUET_FILES:
            for row in self._read_parquet_file(filename):
                scanned += 1
                if cutoff:
                    row_date = parse_russian_date(row.get("docdateIPS", ""))
                    if row_date and row_date < cutoff:
                        continue
                emitted += 1
                yield row

        self._save_checkpoint(current_sha or prev_sha or "")
        logger.info(
            f"Incremental refresh: {emitted} document(s) at or after {cutoff} "
            f"from {scanned} rows scanned"
        )

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
    parser.add_argument("--full", action="store_true", help="Fetch all records")
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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
