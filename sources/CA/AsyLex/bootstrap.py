#!/usr/bin/env python3
"""
CA/AsyLex -- 59K Canadian refugee status decisions from HuggingFace

59,112 anonymized IRB/RPD decisions (1996-2022) via CanLII.
Streams from HuggingFace Parquet files — no auth required.

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
from typing import Generator, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.AsyLex")

DATASET_ID = "clairebarale/AsyLex"
CONFIG_NAME = "raw_documents"
SPLIT = "train"

OUTCOME_MAP = {
    0: "rejected",
    1: "granted",
    2: "uncertain",
}

# Patterns to extract metadata from the text header
DATE_PATTERN = re.compile(
    r'(?:DATE\s+(?:OF|DE\s+LA)\s+(?:DECISION|DÉCISION))\s*[:\n]*\s*'
    r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})',
    re.IGNORECASE
)
DATE_ISO_PATTERN = re.compile(r'(\d{4})-(\d{2})-(\d{2})')

CANLII_CITE_PATTERN = re.compile(r'(\d{4})\s+canlii\s+(\d+)\s+\(ca\s+irb\)', re.IGNORECASE)

MONTH_MAP = {
    'january': '01', 'february': '02', 'march': '03', 'april': '04',
    'may': '05', 'june': '06', 'july': '07', 'august': '08',
    'september': '09', 'october': '10', 'november': '11', 'december': '12',
}


def parse_text_date(text: str) -> Optional[str]:
    """Extract decision date from text and return ISO format."""
    m = DATE_PATTERN.search(text)
    if m:
        raw = m.group(1).strip()
        # Parse "November 17, 1999" → "1999-11-17"
        parts = re.match(r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})', raw)
        if parts:
            month = MONTH_MAP.get(parts.group(1).lower())
            if month:
                return f"{parts.group(3)}-{month}-{int(parts.group(2)):02d}"
    return None


def extract_canlii_id(key: str) -> str:
    """Extract CanLII case ID from __key__ field.

    e.g. "cases_anonymised_raw_text/1999canlii14666" -> "1999canlii14666".
    Returns "" if the key is empty or has no recognizable id.
    """
    if not key:
        return ""
    basename = key.rsplit("/", 1)[-1] if "/" in key else key
    # Extract year and number
    m = re.match(r'(\d{4})canlii(\d+)', basename)
    if m:
        return f"{m.group(1)}canlii{m.group(2)}"
    return basename.strip()


def canlii_id_from_text(text: str) -> str:
    """Derive the CanLII id from the citation embedded in the decision text.

    The body reliably contains "1999 CanLII 14666 (CA IRB)" -> "1999canlii14666".
    This is independent of the dataset's webdataset __key__ column, which is not
    surfaced by every `datasets` version (the root cause of the empty-_id bug).
    """
    m = CANLII_CITE_PATTERN.search(text or "")
    if m:
        return f"{m.group(1)}canlii{m.group(2)}"
    return ""


class CAAsyLexScraper(BaseScraper):
    """Scraper for CA/AsyLex — Canadian refugee decisions from HuggingFace."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _clean_text(self, text: str) -> str:
        """Clean decision text: normalize whitespace."""
        if not text:
            return ""
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a HuggingFace record into standard schema."""
        key = raw.get("__key__", "") or ""
        text = self._clean_text(raw.get("txt", ""))

        # Derive a stable, non-empty _id with fallbacks. The webdataset __key__
        # column is not exposed by every `datasets` version (it was absent on the
        # ingest host, producing 50.8K records with empty _id — issue #861), so
        # fall back to the CanLII citation in the body, then a content hash.
        canlii_id = extract_canlii_id(key) or canlii_id_from_text(text)
        if not canlii_id:
            canlii_id = "asylex-" + hashlib.sha1((text or "").encode("utf-8")).hexdigest()[:16]

        date = parse_text_date(text)

        # Try to extract year from key if date parsing failed
        if not date:
            m = re.match(r'(\d{4})', canlii_id)
            if m:
                date = f"{m.group(1)}-01-01"

        # Build CanLII URL
        m = re.match(r'(\d{4})canlii(\d+)', canlii_id)
        if m:
            year, num = m.group(1), m.group(2)
            url = f"https://www.canlii.org/en/ca/irb/doc/{year}/{year}canlii{num}/{year}canlii{num}.html"
        else:
            url = ""

        # Build title from the CanLII citation
        title = f"IRB Decision {canlii_id}"

        return {
            "_id": canlii_id,
            "_source": "CA/AsyLex",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "canlii_id": canlii_id,
            "tribunal": "Immigration and Refugee Board of Canada",
            "language": "en",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Stream RAW records from the HuggingFace dataset.

        Per the BaseScraper contract, fetch_all yields RAW items; the framework
        (bootstrap/bootstrap_fast) calls normalize() itself. Yielding normalized
        records here caused a double-normalize on the ingest host: normalize then
        read the absent "txt"/"__key__" keys, producing empty text and an identical
        content-hash _id for every record, so all 59K collapsed to one empty row
        (issue #931).
        """
        from datasets import load_dataset

        logger.info("Loading dataset %s (config=%s, split=%s) via streaming...",
                     DATASET_ID, CONFIG_NAME, SPLIT)
        ds = load_dataset(DATASET_ID, CONFIG_NAME, split=SPLIT, streaming=True)

        count = 0
        for item in ds:
            raw = dict(item)
            if not raw.get("txt"):
                continue
            yield raw
            count += 1
            if count % 1000 == 0:
                logger.info("Yielded %d records so far...", count)

        logger.info("Finished: yielded %d raw records total", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch raw records whose normalized decision date is newer than `since`."""
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
            ds = load_dataset(DATASET_ID, CONFIG_NAME, split=SPLIT, streaming=True)
            item = next(iter(ds))
            text = item.get("txt", "")
            logger.info("Test OK: key='%s', %d chars text",
                        item.get("__key__", "")[:60], len(text))
            return bool(text)
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    scraper = CAAsyLexScraper()
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
                out_file = sample_dir / f"{count:04d}.json"
                out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2))
                count += 1
                text_len = len(record.get("text", ""))
                logger.info("Sample %d: %s (%d chars)",
                            count, record.get("_id", "")[:40], text_len)
                if count >= limit:
                    break
            logger.info("Done: %d records (sample)", count)
        else:
            # Stream the full run to data/records.jsonl so the ingest pipeline
            # persists records regardless of how the host captures output (#861).
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)
            jsonl_path = data_dir / "records.jsonl"
            count = 0
            skipped_no_id = 0
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for raw in scraper.fetch_all():
                    record = scraper.normalize(raw)
                    if not record.get("_id") or not record.get("text"):
                        skipped_no_id += 1
                        continue
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 1000 == 0:
                        logger.info("Wrote %d records to records.jsonl...", count)
            logger.info("Done: %d records (full) -> %s (skipped %d without _id)",
                        count, jsonl_path, skipped_no_id)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
