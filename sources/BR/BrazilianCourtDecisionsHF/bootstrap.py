#!/usr/bin/env python3
"""
BR/BrazilianCourtDecisionsHF -- Brazilian Court Decisions (HuggingFace)

Fetches Brazilian court decisions (TJAL - Tribunal de Justiça de Alagoas)
from the joelniklaus/brazilian_court_decisions dataset on HuggingFace.

Strategy:
  - Uses the HuggingFace datasets-server rows API (no auth needed)
  - Fetches in batches of 100 rows
  - Each record has ementa_text (headnote) and decision_description (full text)
  - ~3,234 labeled documents for case outcome prediction

Data fields:
  - process_number: case identifier
  - orgao_julgador: judging body
  - publish_date: publication date
  - judge_relator: reporting judge
  - ementa_text: headnote/summary (ementa)
  - decision_description: decision text
  - judgment_text: judgment outcome text
  - judgment_label: partial/yes/no
  - unanimity_text: unanimity text
  - unanimity_label: unanimity/not-unanimity

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
import time
import re
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
logger = logging.getLogger("legal-data-hunter.BR.BrazilianCourtDecisionsHF")

HF_ROWS_API = "https://datasets-server.huggingface.co/rows"
DATASET = "joelniklaus/brazilian_court_decisions"
CONFIG = "default"
SPLIT = "train"
BATCH_SIZE = 100


def _parse_date(date_str: str) -> Optional[str]:
    """Parse Brazilian date format DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return None


class BrazilianCourtDecisionsHFScraper(BaseScraper):
    """
    Scraper for BR/BrazilianCourtDecisionsHF -- Brazilian Court Decisions.
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
        ementa = (raw.get("ementa_text") or "").strip()
        decision_desc = (raw.get("decision_description") or "").strip()
        process_number = (raw.get("process_number") or "").strip()

        # Combine ementa and decision description for full text
        text_parts = []
        if ementa:
            text_parts.append(ementa)
        if decision_desc and decision_desc != ementa:
            text_parts.append(decision_desc)
        text = "\n\n".join(text_parts)

        if not text:
            return None

        # Generate stable ID from process number
        if process_number:
            doc_id = f"BR-TJAL-{process_number}"
        else:
            text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
            doc_id = f"BR-TJAL-{text_hash}"

        # Parse date
        iso_date = _parse_date(raw.get("publish_date", ""))

        # Title: first 150 chars of ementa or decision description
        title_src = ementa or decision_desc
        title = title_src[:150].rstrip() + ("..." if len(title_src) > 150 else "")

        return {
            "_id": doc_id,
            "_source": "BR/BrazilianCourtDecisionsHF",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": f"https://huggingface.co/datasets/{DATASET}",
            "process_number": process_number,
            "orgao_julgador": (raw.get("orgao_julgador") or "").strip(),
            "judge_relator": (raw.get("judge_relator") or "").strip(),
            "judgment_text": (raw.get("judgment_text") or "").strip(),
            "judgment_label": (raw.get("judgment_label") or "").strip(),
            "unanimity_text": (raw.get("unanimity_text") or "").strip(),
            "unanimity_label": (raw.get("unanimity_label") or "").strip(),
        }


if __name__ == "__main__":
    scraper = BrazilianCourtDecisionsHFScraper()

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
                print(f"Ementa length: {len(row.get('ementa_text', ''))}")
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

            if count % 100 == 0:
                logger.info(f"Processed {count} records")

            if limit and count >= limit:
                break

        print(f"Saved {count} records to {sample_dir}/")

    elif cmd == "update":
        print("Static dataset -- no updates available.")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
