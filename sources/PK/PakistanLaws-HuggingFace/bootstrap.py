#!/usr/bin/env python3
"""
PK/PakistanLaws-HuggingFace -- Pakistan Laws Dataset (federal legislation)

Fetches 967 federal laws and acts of Pakistan from the
AyeshaJadoon/Pakistan_Laws_Dataset dataset on HuggingFace.

The documents were collected from the Ministry of Law and Justice website
(pakistancode.gov.pk), originally as PDFs, and converted to a single JSON
array of {file_name, text} objects where `text` is the full extracted body
of each statute.

Strategy:
  - Download the data file `pdf_data.json` directly via the HuggingFace
    `resolve/main/` URL (the auto-converted parquet split is empty, so the
    datasets-server rows API does not work for this dataset).
  - Parse the JSON array and yield each record.
  - Derive a title and enactment year from the document text (the PDF
    file names are opaque hashes).
  - The canonical original document lives at
    pakistancode.gov.pk/pdffiles/{file_name}.

This is a usable mirror of PK/PakistanCode, which is blocked because
pakistancode.gov.pk is unreliable from datacenter IPs.

License: ODC-BY 1.0 (dataset card); underlying laws are Government of
Pakistan public legal texts.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # No-op (static dataset)
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import json
import hashlib
import logging
import tempfile
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
logger = logging.getLogger("legal-data-hunter.PK.PakistanLaws-HuggingFace")

DATASET = "AyeshaJadoon/Pakistan_Laws_Dataset"
DATA_URL = f"https://huggingface.co/datasets/{DATASET}/resolve/main/pdf_data.json"
DATASET_PAGE = f"https://huggingface.co/datasets/{DATASET}"
# Canonical origin of the source PDFs (Ministry of Law and Justice).
ORIGIN_PDF_BASE = "https://pakistancode.gov.pk/pdffiles/"

# ── Text / title / date extraction ────────────────────────────────
PAGE_RE = re.compile(r'(?i)^page\s+\d+\s+of\s+\d+\s*$')
YEAR_RE = re.compile(r'\b(1[6-9]\d{2}|20\d{2})\b')
LONE_RE = re.compile(r'^[A-Za-z0-9]$')          # stray single OCR char
NUM_RE = re.compile(r'^\d{1,3}$')               # lone page number
DIV_RE = re.compile(r'^[_\-–—=.*]+$')           # divider line
ACT_KW = re.compile(r'(?i)\b(ACT|ORDINANCE|RULES|REGULATIONS?|ORDER|BILL|CODE|CONSTITUTION)\b')
STOP_LINES = {
    'CONTENTS', 'SECTIONS:', 'SECTIONS', 'PREAMBLE.', 'PREAMBLE',
    'ARRANGEMENT OF SECTIONS', 'INDEX', 'CHAPTER I',
}


def _extract_title(text: str) -> str:
    """Derive a statute title from the leading lines of the document text."""
    lines = [l.strip() for l in text.split("\n")]
    collected, started = [], False
    for l in lines[:60]:
        if not l:
            if started and collected:
                break
            continue
        if PAGE_RE.match(l) or LONE_RE.match(l) or NUM_RE.match(l) or DIV_RE.match(l):
            continue
        if re.match(r'(?i)^updated till', l):
            continue
        up = l.upper()
        if up in STOP_LINES or re.match(r'^PART\b', up):
            break
        if re.match(r'^\d+\s*[\.\)]', l):  # numbered section heading
            break
        started = True
        collected.append(l)
        if YEAR_RE.search(l):
            break
        if len(collected) >= 5:
            break
    title = re.sub(r'\s+', ' ', ' '.join(collected)).strip(' .,-')
    if len(title) < 5 or not ACT_KW.search(title):
        # Fallback: first line that names a legal instrument
        for l in lines[:60]:
            if ACT_KW.search(l) and len(l.strip()) > 8 and 'GAZETTE' not in l.upper():
                title = re.sub(r'\s+', ' ', l).strip(' .,-')
                break
    if len(title) < 5:
        title = "Pakistan Federal Law"
    return title[:300]


def _extract_year(title: str, text: str) -> Optional[str]:
    """Extract the enactment year, preferring the year in the title."""
    m = YEAR_RE.findall(title)
    if m:
        return m[-1]
    m = YEAR_RE.findall(text[:1500])
    return m[0] if m else None


def _clean_text(text: str) -> str:
    """Light cleanup of PDF-extracted text — no HTML here, just whitespace."""
    text = text.replace("\x0c", "\n").replace("\xa0", " ")
    # Drop soft hyphens and zero-width characters (PDF extraction noise).
    text = re.sub("[\u00ad\u200b\u200c\u200d\ufeff]", "", text)
    out_lines = []
    for line in text.split("\n"):
        line = re.sub(r'[ \t]+', ' ', line).strip()
        if PAGE_RE.match(line):
            continue
        out_lines.append(line)
    cleaned = "\n".join(out_lines)
    cleaned = re.sub(r'\n{3,}', '\n\n', cleaned)
    return cleaned.strip()


class PKPakistanLawsHFScraper(BaseScraper):
    """Scraper for PK/PakistanLaws-HuggingFace — Pakistan federal laws."""

    def __init__(self):
        super().__init__(Path(__file__).parent)

    def _download_data(self) -> list:
        """Download and parse pdf_data.json (~47 MB) to a temp file."""
        import requests

        logger.info(f"Downloading dataset file: {DATA_URL}")
        with requests.get(DATA_URL, stream=True, timeout=180) as resp:
            resp.raise_for_status()
            with tempfile.NamedTemporaryFile(
                mode="wb", suffix=".json", delete=False
            ) as tmp:
                tmp_path = tmp.name
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    if chunk:
                        tmp.write(chunk)
        try:
            with open(tmp_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        if not isinstance(data, list):
            raise ValueError(f"Unexpected dataset shape: {type(data).__name__}")
        logger.info(f"Loaded {len(data)} records")
        return data

    def fetch_all(self) -> Generator[dict, None, None]:
        for item in self._download_data():
            if isinstance(item, dict):
                yield item

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Static dataset — no incremental updates."""
        logger.info("Static dataset. No incremental updates.")
        return
        yield

    def normalize(self, raw: dict) -> Optional[dict]:
        text = _clean_text((raw.get("text") or ""))
        if len(text) < 50:
            return None

        file_name = (raw.get("file_name") or "").strip()
        title = _extract_title(text)
        year = _extract_year(title, text)

        if file_name:
            stem = file_name[:-4] if file_name.lower().endswith(".pdf") else file_name
            doc_id = f"PK-LAW-{stem}"
            url = f"{ORIGIN_PDF_BASE}{file_name}"
        else:
            stem = hashlib.sha256(text.encode("utf-8")).hexdigest()[:24]
            doc_id = f"PK-LAW-{stem}"
            url = DATASET_PAGE

        return {
            "_id": doc_id,
            "_source": "PK/PakistanLaws-HuggingFace",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": year,  # enactment year (ISO 8601 reduced precision) or null
            "year": year,
            "url": url,
            "file_name": file_name,
            "jurisdiction": "PK",
            "source_dataset": DATASET,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = PKPakistanLawsHFScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        print("Testing HuggingFace dataset connectivity...")
        try:
            import requests
            r = requests.get(DATA_URL, stream=True, timeout=60)
            r.raise_for_status()
            head = next(r.iter_content(chunk_size=2048)).decode("utf-8", "ignore")
            r.close()
            ok = head.lstrip().startswith("[")
            print(f"OK: reachable, starts-with-array={ok}")
            if not ok:
                sys.exit(1)
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
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
