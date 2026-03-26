#!/usr/bin/env python3
"""
INTL/HFforLegalCaseLaw - HuggingFace for Legal Global Case Law Dataset

Fetches US state court decisions from the HFforLegal/case-law dataset on HuggingFace.
541,371 records across 37 US states with full text. Average document ~17K chars.

Data source: https://huggingface.co/datasets/HFforLegal/case-law
Method: HuggingFace datasets-server REST API (paginated JSON, no bulk download)
License: CC-BY-4.0

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap (streams all records)
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

DATASET = "HFforLegal/case-law"
CONFIG = "default"
SPLIT = "us"
API_BASE = "https://datasets-server.huggingface.co"
ROWS_URL = f"{API_BASE}/rows?dataset={DATASET}&config={CONFIG}&split={SPLIT}"

SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "INTL/HFforLegalCaseLaw"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
}

PAGE_SIZE = 100
RATE_LIMIT_DELAY = 0.5


def normalize(row: dict) -> dict:
    """Transform a HuggingFace row into standard schema."""
    row_data = row.get("row", row)
    doc_id = row_data.get("id", "")
    timestamp = row_data.get("timestamp", "")
    date = None
    if timestamp:
        try:
            date = timestamp[:10]  # Extract YYYY-MM-DD from ISO timestamp
        except Exception:
            date = None

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": row_data.get("title", "") or "",
        "text": row_data.get("document", "") or "",
        "date": date,
        "url": f"https://huggingface.co/datasets/{DATASET}",
        "citation": row_data.get("citation", "") or "",
        "docket_number": row_data.get("docket_number", "") or "",
        "state": row_data.get("state", "") or "",
        "issuer": row_data.get("issuer", "") or "",
        "hash": row_data.get("hash", "") or "",
    }


def fetch_page(offset: int, length: int, session: requests.Session) -> dict:
    """Fetch a page of rows from the datasets-server API."""
    url = f"{ROWS_URL}&offset={offset}&length={length}"
    resp = session.get(url, headers=HEADERS, timeout=120)
    resp.raise_for_status()
    return resp.json()


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records with full text."""
    session = requests.Session()

    if sample:
        # Fetch 15 sample records from different offsets for variety
        offsets = [0, 1000, 5000, 10000, 50000, 100000, 150000]
        count = 0
        for offset in offsets:
            if count >= 15:
                break
            try:
                data = fetch_page(offset, min(3, 15 - count), session)
                rows = data.get("rows", [])
                for row in rows:
                    if count >= 15:
                        break
                    record = normalize(row)
                    text_len = len(record.get("text", ""))
                    print(f"  [{count+1}/15] {record['state']}: "
                          f"{record['title'][:60]}... text={text_len} chars")
                    yield record
                    count += 1
            except Exception as e:
                print(f"  Error at offset {offset}: {e}")
            time.sleep(RATE_LIMIT_DELAY)
    else:
        # Stream all records in pages
        offset = 0
        total = None
        while True:
            try:
                data = fetch_page(offset, PAGE_SIZE, session)
            except requests.HTTPError as e:
                print(f"HTTP error at offset {offset}: {e}")
                break
            except Exception as e:
                print(f"Error at offset {offset}: {e}")
                break

            if total is None:
                total = data.get("num_rows_total", "?")
                print(f"Total rows: {total}")

            rows = data.get("rows", [])
            if not rows:
                break

            for row in rows:
                yield normalize(row)

            offset += len(rows)
            if offset % 1000 == 0:
                print(f"  Fetched {offset}/{total} records...")

            time.sleep(RATE_LIMIT_DELAY)


def test_connectivity():
    """Test that we can reach the HuggingFace datasets-server API."""
    print("Testing HuggingFace datasets-server connectivity...")
    session = requests.Session()

    # Test dataset info
    info_url = f"{API_BASE}/info?dataset={DATASET}&config={CONFIG}"
    resp = session.get(info_url, headers=HEADERS, timeout=30)
    print(f"Dataset info: {resp.status_code}")

    # Test rows endpoint
    data = fetch_page(0, 2, session)
    rows = data.get("rows", [])
    total = data.get("num_rows_total", "?")
    print(f"Rows endpoint: OK ({total} total rows)")

    if rows:
        row = rows[0].get("row", rows[0])
        doc = row.get("document", "")
        print(f"First record: {row.get('title', '?')}")
        print(f"Document length: {len(doc)} chars")
        print(f"Document preview: {doc[:200]}...")

    print("OK - connectivity working")


def main():
    parser = argparse.ArgumentParser(description="INTL/HFforLegalCaseLaw bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only sample records")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
        return

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    text_count = 0

    for record in fetch_all(sample=args.sample):
        if args.sample:
            safe_id = record["_id"].replace("/", "_")[:80]
            out_path = SAMPLE_DIR / f"{safe_id}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))

        count += 1
        if record.get("text"):
            text_count += 1

    if count:
        print(f"\nDone: {count} records, {text_count} with full text "
              f"({text_count/count*100:.0f}%)")
    else:
        print("\nNo records fetched")


if __name__ == "__main__":
    main()
