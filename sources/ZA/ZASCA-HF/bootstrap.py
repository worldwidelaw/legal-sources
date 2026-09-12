#!/usr/bin/env python3
"""
ZA/ZASCA-HF -- South African Supreme Court of Appeal judgments from HuggingFace

~2,000+ SCA judgments with full English text from dsfsi/zasca-sum dataset.
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
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZA.ZASCA-HF")

DATASET_ID = "dsfsi/zasca-sum"
CONFIG = "without_summaries"
SPLIT = "all_data"

SOURCE_ID = "ZA/ZASCA-HF"


class ZASCAHFScraper(BaseScraper):
    """Scraper for ZA/ZASCA-HF — South Africa SCA judgments from HuggingFace."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _clean_text(self, text: str) -> str:
        """Clean judgment text: normalize whitespace."""
        if not text:
            return ""
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()

    def _extract_case_number(self, text: str) -> str:
        """Extract case number from judgment header."""
        for line in text.split('\n')[:30]:
            line_lower = line.lower().strip()
            if 'case no' in line_lower or 'case number' in line_lower:
                # Extract the actual number part
                match = re.search(r'(?:case\s*no\.?\s*:?\s*)([\d/\-\s]+)', line, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
                return line.strip()
        return ""

    def _extract_title(self, text: str) -> str:
        """Extract a title from the judgment — typically the parties."""
        lines = text.split('\n')
        # Skip header lines (court name, "JUDGMENT", "Reportable", "Case no:")
        skip_patterns = [
            'supreme court', 'republic of south', 'judgment', 'reportable',
            'case no', 'case number', 'in the matter', 'between', 'and',
            'appellant', 'respondent', 'coram', 'heard', 'delivered',
        ]
        candidates = []
        for line in lines[2:25]:
            stripped = line.strip()
            if not stripped or len(stripped) < 3:
                continue
            lower = stripped.lower()
            if any(p in lower for p in skip_patterns):
                continue
            # Party names are usually in caps
            if stripped.isupper() and len(stripped) > 5:
                candidates.append(stripped)
                if len(candidates) >= 2:
                    break

        if candidates:
            return ' v '.join(candidates[:2])

        # Fallback: use first substantial line after header
        for line in lines[3:15]:
            stripped = line.strip()
            if stripped and len(stripped) > 10:
                return stripped[:200]
        return "SCA Judgment"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a HuggingFace record into standard schema."""
        record_id = str(raw.get("id", "")).strip()
        year = str(raw.get("year", ""))
        case_type = raw.get("type", "")
        text = self._clean_text(raw.get("input", ""))

        case_number = self._extract_case_number(text)
        title = self._extract_title(text)

        date = f"{year}-01-01" if year else None

        # The dataset's "id" column is not surfaced by every `datasets` version,
        # which would otherwise make every _id identical ("ZA/ZASCA-HF:"). Fall
        # back to the extracted case number, then a content hash, so ids stay
        # unique on the ingest host (issue #932).
        if not record_id:
            record_id = (case_number or "").strip()
        if not record_id:
            record_id = hashlib.sha1((text or "").encode("utf-8")).hexdigest()[:16]

        return {
            "_id": f"{SOURCE_ID}:{record_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://huggingface.co/datasets/dsfsi/zasca-sum",
            "case_number": case_number,
            "case_type": case_type,
            "year": year,
            "court": "Supreme Court of Appeal",
            "jurisdiction": "ZA",
            "language": "en",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Stream RAW records from the HuggingFace dataset.

        Per the BaseScraper contract, fetch_all yields RAW items and the framework
        calls normalize(). Yielding normalized records here double-normalized on the
        ingest host: normalize then read the absent "input"/"id" keys, so every row
        got empty text and an identical _id ("ZA/ZASCA-HF:"), collapsing 2,053 rows
        to one empty record (issue #932).
        """
        from datasets import load_dataset

        logger.info("Loading dataset %s (config=%s, split=%s) via streaming...",
                     DATASET_ID, CONFIG, SPLIT)
        ds = load_dataset(DATASET_ID, CONFIG, split=SPLIT, streaming=True)

        count = 0
        for item in ds:
            raw = dict(item)
            if not raw.get("input"):
                continue
            yield raw
            count += 1
            if count % 500 == 0:
                logger.info("Yielded %d records so far...", count)

        logger.info("Finished: yielded %d raw records total", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch raw records whose normalized date is newer than `since`."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for raw in self.fetch_all():
            record = self.normalize(raw)
            if record.get("date") and record["date"] >= since:
                yield raw

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            from datasets import load_dataset
            ds = load_dataset(DATASET_ID, CONFIG, split=SPLIT, streaming=True)
            item = next(iter(ds))
            text = item.get("input", "")
            logger.info("Test OK: id=%s year=%s text=%d chars",
                        item.get("id"), item.get("year"), len(text))
            return bool(text)
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    scraper = ZASCAHFScraper()
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
        if sample_mode:
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            limit = 15
            count = 0
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                if not record.get("text"):
                    continue
                out_file = sample_dir / f"record_{count:04d}.json"
                out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2))
                count += 1
                text_len = len(record.get("text", ""))
                logger.info("Sample %d: %s (%d chars)",
                            count, record.get("title", "")[:60], text_len)
                if count >= limit:
                    break
            logger.info("Done: %d records (sample)", count)
        else:
            # Stream the full run to data/records.jsonl so the ingest pipeline
            # persists records (printing to stdout left nothing for the loader —
            # the no-persist half of issue #932).
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)
            jsonl_path = data_dir / "records.jsonl"
            count = 0
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for raw in scraper.fetch_all():
                    record = scraper.normalize(raw)
                    if not record.get("_id") or not record.get("text"):
                        continue
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 500 == 0:
                        logger.info("Wrote %d records to records.jsonl...", count)
            logger.info("Done: %d records (full) -> %s", count, jsonl_path)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
