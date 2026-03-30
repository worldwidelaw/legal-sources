#!/usr/bin/env python3
"""
TH/Krisdika - Office of the Council of State (Krisdika) Thai Laws Fetcher

Fetches Thai legislation from the pythainlp/thailaw dataset on HuggingFace.
Coverage: 42,755 Acts, Royal Decrees, Ministerial Regulations, and ordinances.
Source: Office of the Council of State (Krisdika), Thailand.

Dataset: pythainlp/thailaw (HuggingFace)
License: CC0 1.0 (public domain)
Format: Parquet via HuggingFace datasets-server API

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

SOURCE_ID = "TH/Krisdika"
SAMPLE_DIR = Path(__file__).parent / "sample"
DATASET = "pythainlp/thailaw"
API_BASE = "https://datasets-server.huggingface.co"
PAGE_SIZE = 100


def http_get(url: str, retries: int = 3) -> bytes:
    """Fetch URL content with retries."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "LegalDataHunter/1.0"})
            with urlopen(req, timeout=60) as resp:
                return resp.read()
        except (HTTPError, URLError, TimeoutError) as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"Failed to fetch {url}: {e}")


def fetch_rows(offset: int, length: int) -> list[dict]:
    """Fetch rows from the HuggingFace datasets-server API."""
    url = (
        f"{API_BASE}/rows?dataset={DATASET}"
        f"&config=default&split=train&offset={offset}&length={length}"
    )
    data = json.loads(http_get(url))
    rows = data.get("rows", [])
    return [r.get("row", {}) for r in rows]


def normalize(raw: dict) -> Optional[dict]:
    """Normalize a raw record into standard schema."""
    text = raw.get("txt", "")
    if not text or len(text) < 50:
        return None

    title = raw.get("title", "")
    sysid = str(raw.get("sysid", ""))

    if not sysid:
        return None

    return {
        "_id": f"TH_KR_{sysid}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "sysid": sysid,
        "date": None,
        "text": text,
        "url": f"https://huggingface.co/datasets/{DATASET}",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all legislation records from the dataset."""
    if sample:
        print("  Fetching sample records...")
        rows = fetch_rows(0, 20)
        count = 0
        for raw in rows:
            record = normalize(raw)
            if record:
                yield record
                count += 1
                if count >= 15:
                    return
        return

    # Full fetch: paginate through all 42,755 records
    print("  Getting dataset info...")
    info_url = f"{API_BASE}/info?dataset={DATASET}"
    info = json.loads(http_get(info_url))
    total = info["dataset_info"]["default"]["splits"]["train"]["num_examples"]
    print(f"  Total records: {total}")

    offset = 0
    total_yielded = 0
    while offset < total:
        length = min(PAGE_SIZE, total - offset)
        print(f"  Fetching rows {offset}-{offset + length} of {total}...")
        try:
            rows = fetch_rows(offset, length)
        except Exception as e:
            print(f"    Error at offset {offset}: {e}")
            offset += length
            continue

        for raw in rows:
            record = normalize(raw)
            if record:
                yield record
                total_yielded += 1

        offset += length
        time.sleep(0.5)  # Rate limit

    print(f"  Total records with text: {total_yielded}")


def test_connection():
    """Test connectivity to the HuggingFace dataset."""
    print("Testing Krisdika/thailaw connectivity...")

    print("\n1. Checking dataset info...")
    try:
        info_url = f"{API_BASE}/info?dataset={DATASET}"
        info = json.loads(http_get(info_url))
        total = info["dataset_info"]["default"]["splits"]["train"]["num_examples"]
        print(f"   OK: {total} records in dataset")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n2. Fetching sample rows...")
    try:
        rows = fetch_rows(0, 5)
        print(f"   OK: Got {len(rows)} rows")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n3. Checking data quality...")
    for i, raw in enumerate(rows[:3]):
        record = normalize(raw)
        if record:
            print(f"   [{i}] sysid={record['sysid']}, title={record['title'][:60]}")
            print(f"        text_length={len(record['text'])} chars")
            if i == 0:
                print(f"        text_preview: {record['text'][:150]}...")

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="TH/Krisdika Thai Laws Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connection()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            filename = re.sub(r'[^\w\-]', '_', record["_id"]) + ".json"
            filepath = SAMPLE_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  Saved: {filepath.name} ({len(record['text'])} chars)")

        print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
