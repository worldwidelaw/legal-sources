#!/usr/bin/env python3
"""
INTL/FAOLEX - FAO Legislation Database Fetcher

Fetches legislation from the FAO FAOLEX database via bulk CSV open data download.
218,607 records across 200+ countries. Agriculture, food, land, water, environment law.

Data source: https://www.fao.org/faolex/opendata/en/
Method: Bulk CSV download + per-record .txt full text fetch from faolex.fao.org
License: CC BY-NC-SA 3.0 IGO

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import csv
import io
import json
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

# Use the smaller Constitutions dataset for sample mode, full dataset otherwise
CSV_URL_ALL = "https://faolex.fao.org/docs/opendata/FAOLEX_All.zip"
CSV_URL_CONST = "https://faolex.fao.org/docs/opendata/FAOLEX_Const.zip"

SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "INTL/FAOLEX"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
}

RATE_LIMIT_DELAY = 1.0


def download_csv(url: str) -> list[dict]:
    """Download ZIP, extract CSV, return list of row dicts."""
    print(f"Downloading {url} ...")
    resp = requests.get(url, headers=HEADERS, timeout=300)
    resp.raise_for_status()
    print(f"Downloaded {len(resp.content) / 1024 / 1024:.1f} MB")

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise RuntimeError(f"No CSV found in ZIP: {zf.namelist()}")
        csv_name = csv_names[0]
        print(f"Extracting {csv_name} ...")
        raw = zf.read(csv_name).decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(raw))
    rows = list(reader)
    print(f"Parsed {len(rows)} records from CSV")
    return rows


def fetch_full_text(text_url: str, session: requests.Session) -> str:
    """Fetch plain text content from a FAOLEX .txt URL."""
    if not text_url or not text_url.strip():
        return ""
    # Some records have multiple URLs separated by semicolons
    urls = [u.strip() for u in text_url.split(";") if u.strip()]
    texts = []
    for url in urls:
        try:
            resp = session.get(url, headers=HEADERS, timeout=60)
            if resp.status_code == 200:
                texts.append(resp.text.strip())
            else:
                print(f"  Text fetch {resp.status_code}: {url}")
        except Exception as e:
            print(f"  Text fetch error: {url} - {e}")
    return "\n\n---\n\n".join(texts)


def normalize_date(date_str: str) -> Optional[str]:
    """Parse FAOLEX date formats to ISO 8601."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


def normalize(row: dict, full_text: str) -> dict:
    """Transform a CSV row + fetched text into standard schema."""
    record_id = row.get("Record Id", "").strip()
    return {
        "_id": record_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": row.get("Title", "").strip(),
        "original_title": row.get("Original title", "").strip(),
        "text": full_text,
        "abstract": row.get("Abstract", "").strip(),
        "date": normalize_date(row.get("Date of original text", "")),
        "last_amended_date": normalize_date(row.get("Last amended date", "")),
        "url": row.get("Record URL", "").strip(),
        "document_url": row.get("Document URL", "").strip(),
        "text_url": row.get("Text URL", "").strip(),
        "country_territory": row.get("Country/Territory", "").strip(),
        "language": row.get("Language of document", "").strip(),
        "repealed": row.get("Repealed", "").strip(),
        "keywords": row.get("Keywords", "").strip(),
        "record_id": record_id,
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records with full text."""
    if sample:
        # Use Constitutions dataset (small, ~270 records) for sample
        rows = download_csv(CSV_URL_CONST)
        # Pick first 15 that have a Text URL
        rows_with_text = [r for r in rows if r.get("Text URL", "").strip()]
        rows = rows_with_text[:15]
        print(f"Sample mode: fetching full text for {len(rows)} records")
    else:
        rows = download_csv(CSV_URL_ALL)

    session = requests.Session()
    for i, row in enumerate(rows):
        text_url = row.get("Text URL", "").strip()
        full_text = fetch_full_text(text_url, session)

        if not full_text:
            # Try abstract as fallback indicator, but still yield
            pass

        record = normalize(row, full_text)

        if sample:
            print(f"  [{i+1}/{len(rows)}] {record['country_territory']}: "
                  f"{record['title'][:60]}... text={len(full_text)} chars")

        yield record

        if i < len(rows) - 1:
            time.sleep(RATE_LIMIT_DELAY)


def test_connectivity():
    """Test that we can reach the FAOLEX open data endpoint."""
    print("Testing FAOLEX open data connectivity...")
    # Test CSV download endpoint
    resp = requests.head(CSV_URL_CONST, headers=HEADERS, timeout=30)
    print(f"Constitutions ZIP: {resp.status_code} "
          f"({resp.headers.get('Content-Length', '?')} bytes)")

    # Test a text URL
    resp2 = requests.get("https://faolex.fao.org/txt/afg72553.txt",
                         headers=HEADERS, timeout=30)
    print(f"Sample .txt: {resp2.status_code} ({len(resp2.text)} chars)")
    print(f"Text preview: {resp2.text[:200]}...")
    print("OK - connectivity working")


def main():
    parser = argparse.ArgumentParser(description="INTL/FAOLEX bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
        return

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    text_count = 0

    for record in fetch_all(sample=args.sample):
        if args.sample:
            out_path = SAMPLE_DIR / f"{record['_id']}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))

        count += 1
        if record.get("text"):
            text_count += 1

    print(f"\nDone: {count} records, {text_count} with full text "
          f"({text_count/count*100:.0f}%)" if count else "\nNo records fetched")


if __name__ == "__main__":
    main()
