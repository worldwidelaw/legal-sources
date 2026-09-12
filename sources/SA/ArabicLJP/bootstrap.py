#!/usr/bin/env python3
"""
SA/ArabicLJP -- Saudi commercial court judgments from HuggingFace dataset mbayan/Arabic-LJP

4,290 Saudi commercial court judgments with case facts and judgment text in Arabic.
Streams from HuggingFace Parquet files — no large downloads needed.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap-fast       # Alias for bootstrap
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SA.ArabicLJP")

DATASET_ID = "mbayan/Arabic-LJP"


class SAArabicLJPScraper(BaseScraper):
    """Scraper for SA/ArabicLJP — Saudi commercial court judgments from HuggingFace."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _clean_text(self, text: str) -> str:
        """Clean text: remove excessive whitespace."""
        if not text:
            return ""
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()

    def _extract_url(self, original_id: str) -> str:
        """Extract URL from original_id field if it contains one."""
        if not original_id:
            return ""
        url_match = re.search(r'https?://\S+', original_id)
        if url_match:
            return url_match.group(0)
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a HuggingFace record into standard schema."""
        record_id = raw.get("id", "")
        original_id = raw.get("original_id", "")
        case_facts = self._clean_text(raw.get("input", ""))
        judgment = self._clean_text(raw.get("output", ""))

        # Combine case facts and judgment as full text
        parts = []
        if case_facts:
            parts.append(case_facts)
        if judgment:
            parts.append(judgment)
        text = "\n\n".join(parts)
        if not text:
            return None

        url = self._extract_url(original_id)

        return {
            "_id": record_id,
            "_source": "SA/ArabicLJP",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": case_facts[:120].strip() if case_facts else "",
            "text": text,
            "date": None,
            "url": url,
            "original_id": original_id,
            "case_facts": case_facts,
            "judgment": judgment,
            "language": "ar",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Stream all RAW records from HuggingFace dataset (train + test splits).

        Per the BaseScraper contract, fetch_all yields RAW documents and the
        framework calls normalize(). Yielding already-normalized records here
        caused the VPS bootstrap to double-normalize (normalize reads raw keys
        like 'input'/'output' which are absent from a normalized dict) → empty
        text for every record. See issue #1204.
        """
        from datasets import load_dataset

        count = 0
        for split in ("train", "test"):
            logger.info("Loading dataset %s (split=%s) via streaming...", DATASET_ID, split)
            ds = load_dataset(DATASET_ID, split=split, streaming=True)

            for item in ds:
                yield dict(item)
                count += 1
                if count % 500 == 0:
                    logger.info("Yielded %d raw records so far...", count)

        logger.info("Finished: yielded %d raw records total", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Static dataset — fetch_updates returns all records."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            from datasets import load_dataset
            ds = load_dataset(DATASET_ID, split="train", streaming=True)
            item = next(iter(ds))
            text = item.get("input", "")
            logger.info("Test OK: got record with %d chars input text", len(text))
            return bool(text)
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    scraper = SAArabicLJPScraper()
    args = sys.argv[1:]

    if not args:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample] [--full]")
        sys.exit(1)

    command = args[0]
    sample_mode = "--sample" in args

    if command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    elif command in ("bootstrap", "bootstrap-fast"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        limit = 15 if sample_mode else None
        count = 0
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            if record is None:
                continue
            if sample_mode:
                out_file = sample_dir / f"{count:04d}.json"
                out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2))
                count += 1
                text_len = len(record.get("text", ""))
                logger.info("Sample %d: %s (%d chars)",
                            count, record.get("_id", "")[:40], text_len)
                if limit and count >= limit:
                    break
            else:
                print(json.dumps(record, ensure_ascii=False))
                count += 1

        logger.info("Done: %d records %s",
                     count, "(sample)" if sample_mode else "(full)")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
