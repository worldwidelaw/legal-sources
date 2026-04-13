#!/usr/bin/env python3
"""
MA/AdalaJustice - Morocco Adala Justice Portal

Fetches 7,532 Moroccan legal texts from the Ministry of Justice Adala portal
via REST API + PDF text extraction.

Content: Dahirs, Decrees, Laws, Circulars, Royal Speeches, Conventions.
Languages: Arabic and French.
License: Open access, no authentication required.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import hashlib
import html
import io
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "MA/AdalaJustice"
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"

API_BASE = "https://adala.justice.gov.ma/api/files"
SEARCH_URL = f"{API_BASE}/search"
UPLOAD_BASE = "https://adala.justice.gov.ma/api"

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
})


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="MA/AdalaJustice",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""

def fetch_law_types() -> dict:
    """Fetch law type mappings."""
    try:
        resp = SESSION.get(f"{API_BASE}/law-types", timeout=30)
        resp.raise_for_status()
        types = resp.json()
        return {t["id"]: t.get("name", "") for t in types}
    except Exception:
        return {}


def fetch_themes() -> dict:
    """Fetch theme mappings."""
    try:
        resp = SESSION.get(f"{API_BASE}/themes", timeout=30)
        resp.raise_for_status()
        themes = resp.json()
        return {t["id"]: t.get("name", "") for t in themes}
    except Exception:
        return {}


def normalize(item: dict) -> dict:
    """Normalize an API result item to standard schema.

    API structure: {type, language, name, path, fileMeta: {object, LawType, theme, ...}}
    """
    file_meta = item.get("fileMeta", {}) or {}
    title = file_meta.get("object", "") or item.get("name", "")
    file_path = item.get("path", "")
    lang = item.get("language", "AR").lower()

    law_type_obj = file_meta.get("LawType", {}) or {}
    theme_obj = file_meta.get("theme", {}) or {}
    law_number = file_meta.get("lawNumber", "") or ""
    keywords = file_meta.get("keywords", "") or ""
    date_str = file_meta.get("gregorianDate", "") or ""

    # Generate stable ID from file path
    import hashlib
    path_hash = hashlib.sha256(file_path.encode()).hexdigest()[:12]
    doc_id = f"MA-ADALA-{path_hash}"

    # Build full title
    if law_number and law_number not in title:
        title = f"{title} ({law_number})"

    # Parse date
    date_val = None
    if date_str:
        date_val = date_str[:10]

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title.strip(),
        "text": "",  # Will be filled by PDF extraction
        "date": date_val,
        "url": "https://adala.justice.gov.ma/",
        "language": lang,
        "law_number": law_number,
        "law_type": law_type_obj.get("name", ""),
        "theme": theme_obj.get("name", ""),
        "keywords": keywords,
        "file_path": file_path,
    }


def download_and_extract_text(file_path: str) -> str:
    """Download a PDF and extract its text."""
    if not file_path:
        return ""
    url = f"{UPLOAD_BASE}/{file_path}"
    try:
        resp = SESSION.get(url, timeout=60)
        if resp.status_code != 200:
            return ""
        return extract_text_from_pdf(resp.content)
    except Exception as e:
        return ""


def iter_laws(limit: int = 0):
    """Iterate over all laws from the API.

    API returns: {meta: {totalItems, totalPages, ...}, items: {results: [...], filters: ...}}
    """
    page = 0
    count = 0
    total = None

    while True:
        url = f"{SEARCH_URL}?term=&page={page}"
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  Error fetching page {page}: {e}")
            break

        meta = data.get("meta", {})
        items_obj = data.get("items", {})
        results = items_obj.get("results", []) if isinstance(items_obj, dict) else []

        if total is None:
            total = meta.get("totalItems", 0)
            total_pages = meta.get("totalPages", 0)
            print(f"  Total: {total} documents across {total_pages} pages")

        if not results:
            break

        for item in results:
            record = normalize(item)
            file_path = record.pop("file_path", "")

            # Download PDF and extract text
            if file_path:
                text = download_and_extract_text(file_path)
                if text and len(text) > 50:
                    record["text"] = text

            if not record["text"] or len(record["text"]) < 50:
                continue

            yield record
            count += 1
            time.sleep(0.3)

            if limit and count >= limit:
                print(f"  Reached sample limit of {limit}")
                return

        page += 1
        time.sleep(0.5)

    print(f"  Processed {count} laws with full text")


def test_connectivity():
    """Test connectivity to the Adala API."""
    print("Testing Adala Justice API connectivity...")

    resp = SESSION.get(f"{SEARCH_URL}?term=&page=0", timeout=30)
    data = resp.json()
    meta = data.get("meta", {})
    total = meta.get("totalItems", 0)
    items_obj = data.get("items", {})
    results = items_obj.get("results", []) if isinstance(items_obj, dict) else []
    print(f"  Search API: HTTP {resp.status_code}, {total} total documents")
    print(f"  Items per page: {len(results)}")

    if results:
        item = results[0]
        title = (item.get("fileMeta", {}) or {}).get("object", "")[:60]
        file_path = item.get("path", "")
        print(f"  Sample title: {title}")
        print(f"  Sample file path: {file_path[:80]}")

        if file_path and HAS_PYMUPDF:
            text = download_and_extract_text(file_path)
            print(f"  PDF text extraction: {len(text)} chars")
            if text:
                print(f"  First 150 chars: {text[:150]}...")

    # Test law types
    lt_resp = SESSION.get(f"{API_BASE}/law-types", timeout=30)
    print(f"  Law types: HTTP {lt_resp.status_code}, {len(lt_resp.json())} types")

    print("Connectivity test complete")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    if not HAS_PYMUPDF:
        print("ERROR: PyMuPDF required for PDF text extraction. Install with: pip install pymupdf")
        sys.exit(1)

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    limit = 15 if sample else 0
    all_records = []
    saved = 0

    for record in iter_laws(limit=limit):
        out_path = SAMPLE_DIR / f"record_{saved:04d}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        all_records.append(record)
        saved += 1

    all_path = SAMPLE_DIR / "all_samples.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    print(f"\nBootstrap complete: {saved} records saved to {SAMPLE_DIR}")

    text_count = sum(1 for r in all_records if r.get("text") and len(r["text"]) > 100)
    print(f"  Records with substantial text: {text_count}/{saved}")

    if saved > 0 and text_count < saved * 0.5:
        print("WARNING: Less than 50% of records have substantial text")


def main():
    parser = argparse.ArgumentParser(description="MA/AdalaJustice Morocco Legal Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
