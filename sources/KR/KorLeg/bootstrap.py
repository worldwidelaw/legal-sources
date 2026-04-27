#!/usr/bin/env python3
"""
KR/KorLeg - South Korean Legal Judgments Dataset (Zenodo)

Fetches 85,887 Korean court judgments from the KorLeg academic dataset.
Source: https://zenodo.org/records/14542443
Format: Excel (.xlsx) with columns: text, category
License: CC-BY-4.0

Categories: Civil Law, Criminal Law, IP Law, Taxation, Administrative Law

Usage:
  python bootstrap.py bootstrap --sample   # Extract sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import hashlib
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

try:
    import openpyxl
    HAS_OPENPYXL = True
except ImportError:
    HAS_OPENPYXL = False

SOURCE_ID = "KR/KorLeg"
ZENODO_URL = "https://zenodo.org/api/records/14542443/files/KorLeg.xlsx/content"
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"


EXPECTED_MIN_SIZE = 200_000_000  # ~237MB expected


def download_dataset(dest_path: str, retries: int = 3) -> str:
    """Download the KorLeg Excel file from Zenodo with retries."""
    for attempt in range(retries):
        try:
            print(f"Downloading KorLeg dataset from Zenodo (attempt {attempt+1}/{retries})...")
            resp = requests.get(ZENODO_URL, stream=True, timeout=(30, 600))
            resp.raise_for_status()

            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            tmp_dest = dest_path + ".tmp"
            with open(tmp_dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total and downloaded % (10 * 1024 * 1024) < 65536:
                        pct = downloaded * 100 // total
                        print(f"  Downloaded {downloaded // (1024*1024)}MB / {total // (1024*1024)}MB ({pct}%)")

            file_size = os.path.getsize(tmp_dest)
            if file_size < EXPECTED_MIN_SIZE:
                print(f"  ERROR: Downloaded file too small ({file_size // (1024*1024)}MB), expected >{EXPECTED_MIN_SIZE // (1024*1024)}MB")
                os.remove(tmp_dest)
                continue

            os.rename(tmp_dest, dest_path)
            print(f"  Download complete: {file_size // (1024*1024)}MB")
            return dest_path
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            print(f"  Download failed: {e}")
            if os.path.exists(dest_path + ".tmp"):
                os.remove(dest_path + ".tmp")
            if attempt < retries - 1:
                wait = 30 * (attempt + 1)
                print(f"  Retrying in {wait}s...")
                time.sleep(wait)

    raise RuntimeError(f"Failed to download KorLeg dataset after {retries} attempts")


def make_id(text: str, index: int) -> str:
    """Generate a stable ID from text content hash."""
    text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
    return f"KR-KORLEG-{text_hash}"


def normalize(text: str, category: str, index: int) -> dict:
    """Normalize a record to the standard schema."""
    doc_id = make_id(text, index)
    # Create a title from the first meaningful sentence
    title_text = text[:200].strip()
    # Find first sentence break
    for sep in ["。", ".", "\n"]:
        pos = title_text.find(sep)
        if 10 < pos < 200:
            title_text = title_text[:pos + 1]
            break
    if len(title_text) > 150:
        title_text = title_text[:150] + "..."

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title_text,
        "text": text,
        "date": None,
        "url": "https://zenodo.org/records/14542443",
        "category": category,
        "language": "ko",
    }


def iter_records(xlsx_path: str, limit: int = 0):
    """Iterate over records from the Excel file."""
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb.active
    count = 0

    for row in ws.iter_rows(min_row=2, values_only=True):
        text = row[0] if row[0] else ""
        category = row[1] if len(row) > 1 and row[1] else "unknown"

        if not text or len(str(text).strip()) < 20:
            continue

        text = str(text).strip()
        category = str(category).strip()

        record = normalize(text, category, count)
        yield record
        count += 1

        if limit and count >= limit:
            break

    wb.close()
    print(f"Processed {count} records")


def test_connectivity():
    """Test connectivity to Zenodo."""
    print("Testing Zenodo connectivity...")
    resp = requests.head(ZENODO_URL, timeout=30, allow_redirects=True)
    print(f"  Status: {resp.status_code}")
    size = resp.headers.get("content-length", "unknown")
    print(f"  File size: {int(size) // (1024*1024)}MB" if size != "unknown" else f"  File size: {size}")
    print("Connectivity test PASSED")
    return True


def _get_xlsx_path():
    """Find or download the KorLeg Excel file."""
    cached_path = DATA_DIR / "KorLeg.xlsx"
    tmp_path = "/tmp/KorLeg.xlsx"

    if cached_path.exists() and os.path.getsize(str(cached_path)) >= EXPECTED_MIN_SIZE:
        print(f"Using cached dataset: {cached_path} ({os.path.getsize(str(cached_path)) // (1024*1024)}MB)")
        return str(cached_path)
    elif os.path.exists(tmp_path) and os.path.getsize(tmp_path) >= EXPECTED_MIN_SIZE:
        print(f"Using tmp dataset: {tmp_path} ({os.path.getsize(tmp_path) // (1024*1024)}MB)")
        return tmp_path
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        download_dataset(str(cached_path))
        return str(cached_path)


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    if not HAS_OPENPYXL:
        print("ERROR: openpyxl is required. Install with: pip install openpyxl")
        sys.exit(1)

    try:
        xlsx_path = _get_xlsx_path()
    except RuntimeError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    if sample:
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        saved = 0
        all_samples = []
        for record in iter_records(xlsx_path, limit=15):
            out_path = SAMPLE_DIR / f"record_{saved:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            all_samples.append(record)
            saved += 1

        all_path = SAMPLE_DIR / "all_samples.json"
        with open(all_path, "w", encoding="utf-8") as f:
            json.dump(all_samples, f, ensure_ascii=False, indent=2)

        print(f"\nSample complete: {saved} records saved to {SAMPLE_DIR}")
    else:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        jsonl_path = DATA_DIR / "records.jsonl"
        saved = 0
        with open(jsonl_path, "a", encoding="utf-8") as f:
            for record in iter_records(xlsx_path, limit=0):
                line = json.dumps(record, ensure_ascii=False, default=str)
                f.write(line + "\n")
                saved += 1
                if saved % 10000 == 0:
                    print(f"  Saved {saved} records...")
                    f.flush()
        print(f"\nBootstrap complete: {saved} records saved to {jsonl_path}")


def main():
    parser = argparse.ArgumentParser(description="KorLeg Korean Judgments Fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
    elif args.command in ("bootstrap", "bootstrap-fast"):
        sample = args.sample and not args.full and args.command != "bootstrap-fast"
        bootstrap(sample=sample)


if __name__ == "__main__":
    main()
