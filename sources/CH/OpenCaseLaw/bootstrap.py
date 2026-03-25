#!/usr/bin/env python3
"""
CH/OpenCaseLaw -- Swiss Court Decisions (OpenCaseLaw / Entscheidsuche)

Fetches Swiss court decisions from the voilaj/swiss-caselaw dataset on HuggingFace.
963K+ decisions from all cantons and federal courts, with full text.

Strategy:
  - Uses HuggingFace datasets-server rows API (no auth needed)
  - For sample: fetches small batches via rows API
  - For full: iterates through all rows in batches of 100

Data fields:
  - decision_id: unique decision identifier
  - court: court name
  - canton: Swiss canton code
  - docket_number: case reference number
  - decision_date: decision date
  - language: de/fr/it/rm
  - full_text: complete decision text
  - regeste: summary/headnote
  - legal_area: area of law
  - cited_decisions: JSON list of cited references

License: Open Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull (warning: 963K rows)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import hashlib
import logging
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
logger = logging.getLogger("legal-data-hunter.CH.OpenCaseLaw")

HF_ROWS_API = "https://datasets-server.huggingface.co/rows"
DATASET = "voilaj/swiss-caselaw"
CONFIG = "default"
SPLIT = "train"
BATCH_SIZE = 100


class OpenCaseLawScraper(BaseScraper):
    """Scraper for CH/OpenCaseLaw -- Swiss Court Decisions."""

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
        """No incremental updates via rows API."""
        logger.info("Use bootstrap for full refresh.")
        return
        yield

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw OpenCaseLaw row into standardized schema."""
        full_text = (raw.get("full_text") or "").strip()
        regeste = (raw.get("regeste") or "").strip()

        # Build text from full_text + regeste
        text_parts = []
        if regeste:
            text_parts.append(regeste)
        if full_text:
            text_parts.append(full_text)
        text = "\n\n".join(text_parts)

        if not text:
            return None

        decision_id = (raw.get("decision_id") or "").strip()
        if decision_id:
            doc_id = f"CH-OCL-{decision_id}"
        else:
            text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
            doc_id = f"CH-OCL-{text_hash}"

        # Parse date
        date = raw.get("decision_date") or raw.get("publication_date")

        # Title
        title = (raw.get("title") or "").strip()
        if not title:
            docket = (raw.get("docket_number") or "").strip()
            court = (raw.get("court") or "").strip()
            title = f"{court} — {docket}" if docket else court or "Swiss Court Decision"

        source_url = (raw.get("source_url") or "").strip()

        return {
            "_id": doc_id,
            "_source": "CH/OpenCaseLaw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": source_url or f"https://opencaselaw.ch/",
            "decision_id": decision_id,
            "court": (raw.get("court") or "").strip(),
            "canton": (raw.get("canton") or "").strip(),
            "docket_number": (raw.get("docket_number") or "").strip(),
            "language": (raw.get("language") or "").strip(),
            "legal_area": (raw.get("legal_area") or "").strip(),
            "decision_type": (raw.get("decision_type") or "").strip(),
            "judges": (raw.get("judges") or "").strip(),
            "outcome": (raw.get("outcome") or "").strip(),
            "cited_decisions": (raw.get("cited_decisions") or "").strip(),
        }


if __name__ == "__main__":
    scraper = OpenCaseLawScraper()

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
                has_text = bool(row.get("full_text", "").strip())
                print(f"OK: Dataset has {total} rows, full_text present: {has_text}")
            else:
                print("FAIL: No rows returned")
                sys.exit(1)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample else None

        for raw in scraper.fetch_all():
            normalized = scraper.normalize(raw)
            if normalized is None:
                continue

            count += 1
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            if count % 1000 == 0:
                logger.info(f"Processed {count} records")

            if limit and count >= limit:
                break

        print(f"Saved {count} records to {sample_dir}/")

    elif cmd == "update":
        print("Use bootstrap for full refresh.")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
