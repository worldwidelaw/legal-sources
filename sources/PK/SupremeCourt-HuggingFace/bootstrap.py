#!/usr/bin/env python3
"""
PK/SupremeCourt-HuggingFace -- Pakistan Supreme Court Judgments

Fetches Pakistan Supreme Court judgments from the
Ibtehaj10/supreme-court-of-pak-judgments dataset on HuggingFace.

Strategy:
  - Uses the HuggingFace datasets-server rows API (no auth needed)
  - Fetches in batches of 100 rows
  - Each record has full judgment text, case_details, and citation_number
  - 1,414 documents total

Data fields:
  - text: full judgment text
  - case_details: dict with id and url
  - citation_number: dict with id and url
  - embeddings: vector embeddings (ignored)

License: Open (academic dataset)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import hashlib
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PK.SupremeCourt-HuggingFace")

HF_ROWS_API = "https://datasets-server.huggingface.co/rows"
DATASET = "Ibtehaj10/supreme-court-of-pak-judgments"
CONFIG = "default"
SPLIT = "train"
BATCH_SIZE = 100


def _extract_case_id(case_details, citation_number) -> str:
    """Extract a case identifier from case_details or citation_number."""
    import ast
    for field in [citation_number, case_details]:
        if not field:
            continue
        # Field may be a dict or a string repr of a dict
        if isinstance(field, str):
            # Try to parse as Python dict literal
            try:
                field = ast.literal_eval(field)
            except (ValueError, SyntaxError):
                fid = field.strip()
                if fid:
                    return fid.replace(".pdf", "").replace(" ", "_")
                continue
        if isinstance(field, dict):
            fid = (field.get("id") or "").strip()
            if fid:
                return fid.replace(".pdf", "").replace(" ", "_")
    return ""


def _extract_title(text: str) -> str:
    """Extract a title from the judgment text."""
    lines = text.split("\n")
    # Look for case number pattern like "CIVIL APPEAL NO.10 OF 2021"
    for line in lines[:30]:
        line_s = line.strip()
        if re.search(r'(?:APPEAL|PETITION|CASE|REVIEW|REFERENCE)\s+NO', line_s, re.I):
            return line_s[:200]
    # Look for "Versus" pattern to get party names
    for i, line in enumerate(lines[:40]):
        if "versus" in line.lower() or "vs" in line.lower():
            # Try to get appellant from previous non-empty lines
            parties = []
            for j in range(max(0, i - 3), i):
                l = lines[j].strip()
                if l and len(l) > 3 and "appellant" not in l.lower() and "respondent" not in l.lower():
                    parties.append(l)
            if parties:
                return f"{parties[-1]} v. ..."[:200]
    # Fallback: first meaningful line
    for line in lines[:20]:
        line_s = line.strip()
        if len(line_s) > 20 and "SUPREME COURT" not in line_s.upper() and "PRESENT" not in line_s.upper():
            return line_s[:200]
    return "Pakistan Supreme Court Judgment"


def _extract_date(text: str) -> Optional[str]:
    """Extract date from judgment text."""
    # Common patterns: "dated 11.06.2019", "Dated: 2021-01-15", "January 15, 2021"
    patterns = [
        r'(\d{1,2})[./](\d{1,2})[./](\d{4})',
        r'(\d{4})-(\d{2})-(\d{2})',
        r'(\d{1,2})\s+(?:January|February|March|April|May|June|July|August|September|October|November|December),?\s+(\d{4})',
    ]
    for pattern in patterns:
        m = re.search(pattern, text[:3000])
        if m:
            groups = m.groups()
            try:
                if len(groups) == 3 and len(groups[0]) == 4:
                    # YYYY-MM-DD
                    return f"{groups[0]}-{groups[1]}-{groups[2]}"
                elif len(groups) == 3:
                    # DD/MM/YYYY or DD.MM.YYYY
                    day, month, year = groups
                    return f"{year}-{int(month):02d}-{int(day):02d}"
                elif len(groups) == 2:
                    # Day Month Year
                    months = {"January": 1, "February": 2, "March": 3, "April": 4,
                              "May": 5, "June": 6, "July": 7, "August": 8,
                              "September": 9, "October": 10, "November": 11, "December": 12}
                    day_str = groups[0]
                    year = groups[1]
                    # Need to find the month name
                    for mname, mnum in months.items():
                        if mname in text[:3000]:
                            return f"{year}-{mnum:02d}-{int(day_str):02d}"
            except (ValueError, IndexError):
                continue
    return None


class PKSupremeCourtHFScraper(BaseScraper):
    """
    Scraper for PK/SupremeCourt-HuggingFace -- Pakistan Supreme Court Judgments.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_batch(self, offset: int, length: int) -> dict:
        """Fetch a batch of rows from HuggingFace datasets API."""
        import requests

        params = {
            "dataset": DATASET,
            "config": CONFIG,
            "split": SPLIT,
            "offset": offset,
            "length": length,
        }
        resp = requests.get(HF_ROWS_API, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from the HuggingFace dataset."""
        offset = 0
        total = None

        while True:
            logger.info(f"Fetching rows {offset}..{offset + BATCH_SIZE}")
            data = self._fetch_batch(offset, BATCH_SIZE)

            if total is None:
                total = data.get("num_rows_total", 0)
                logger.info(f"Total rows in dataset: {total}")

            rows = data.get("rows", [])
            if not rows:
                break

            for item in rows:
                yield item.get("row", {})

            offset += len(rows)
            if offset >= total:
                break

            time.sleep(1)

        logger.info(f"Fetched {offset} rows total")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental updates for static dataset."""
        logger.info("Static dataset. No incremental updates.")
        return
        yield

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw HuggingFace row into standardized schema."""
        text = (raw.get("text") or "").strip()
        if not text:
            return None

        case_details = raw.get("case_details") or {}
        citation_number = raw.get("citation_number") or {}

        case_id = _extract_case_id(case_details, citation_number)
        if case_id:
            doc_id = f"PK-SC-{case_id}"
        else:
            text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
            doc_id = f"PK-SC-{text_hash}"

        title = _extract_title(text)
        date = _extract_date(text)

        return {
            "_id": doc_id,
            "_source": "PK/SupremeCourt-HuggingFace",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://huggingface.co/datasets/{DATASET}",
            "citation_number": case_id,
        }


if __name__ == "__main__":
    scraper = PKSupremeCourtHFScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        print("Testing HuggingFace datasets API connectivity...")
        try:
            data = scraper._fetch_batch(0, 1)
            total = data.get("num_rows_total", 0)
            rows = data.get("rows", [])
            if rows:
                row = rows[0].get("row", {})
                print(f"OK: Dataset has {total} rows")
                print(f"Sample fields: {list(row.keys())}")
                print(f"Text length: {len(row.get('text', ''))}")
            else:
                print("FAIL: No rows returned")
                sys.exit(1)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        stats = scraper.bootstrap(sample_mode="--sample" in sys.argv, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif cmd == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
