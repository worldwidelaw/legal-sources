#!/usr/bin/env python3
"""
TH/HuggingFaceRG - Thailand Royal Gazette OCR Dataset Fetcher

Fetches Thai Royal Gazette documents from obbzung/soc-ratchakitcha on HuggingFace.
Coverage: Royal Gazette pages 1884-present; OCR full text available 2018-2025.

Dataset: obbzung/soc-ratchakitcha (HuggingFace)
License: CC-BY 4.0
Format: JSONL files (meta/ for metadata, ocr/iapp/ for OCR text)

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

SOURCE_ID = "TH/HuggingFaceRG"
SAMPLE_DIR = Path(__file__).parent / "sample"
DATASET = "obbzung/soc-ratchakitcha"
BASE_URL = f"https://huggingface.co/datasets/{DATASET}/resolve/main"
API_URL = f"https://huggingface.co/api/datasets/{DATASET}"


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


def list_ocr_files() -> list[str]:
    """List all OCR JSONL files from the dataset API."""
    data = json.loads(http_get(API_URL))
    siblings = data.get("siblings", [])
    ocr_files = sorted(
        s["rfilename"] for s in siblings
        if s["rfilename"].startswith("ocr/iapp/") and s["rfilename"].endswith(".jsonl")
    )
    return ocr_files


def list_meta_files() -> list[str]:
    """List all meta JSONL files from the dataset API."""
    data = json.loads(http_get(API_URL))
    siblings = data.get("siblings", [])
    meta_files = sorted(
        s["rfilename"] for s in siblings
        if s["rfilename"].startswith("meta/") and s["rfilename"].endswith(".jsonl")
    )
    return meta_files


def load_meta_index(meta_path: str) -> dict:
    """Load a meta JSONL file and index by pdf_file."""
    url = f"{BASE_URL}/{meta_path}"
    content = http_get(url).decode("utf-8")
    index = {}
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
            pdf_file = rec.get("pdf_file", "")
            if pdf_file:
                index[pdf_file] = rec
        except json.JSONDecodeError:
            continue
    return index


def extract_ocr_text(ocr_rec: dict) -> str:
    """Extract full text from an OCR record, handling both data formats."""
    data = ocr_rec.get("data", {})
    if not data:
        return ""

    # Format 1 (2018-2024): data.ocr_results[].markdown_output
    ocr_results = data.get("ocr_results")
    if ocr_results:
        pages = sorted(ocr_results, key=lambda p: p.get("page_num", 0))
        parts = []
        for page in pages:
            md = page.get("markdown_output", "")
            if md:
                parts.append(md.strip())
        return "\n\n".join(parts)

    # Format 2 (2025+): data.formatted_result.formatted_output
    formatted = data.get("formatted_result")
    if isinstance(formatted, dict):
        output = formatted.get("formatted_output", "")
        if output:
            return output.strip()

    return ""


def normalize(ocr_rec: dict, meta_rec: Optional[dict]) -> Optional[dict]:
    """Normalize a raw OCR+meta record into standard schema."""
    # Skip error records
    if ocr_rec.get("status") == "error" or ocr_rec.get("success") is False:
        return None

    text = extract_ocr_text(ocr_rec)

    if not text or len(text) < 50:
        return None

    data = ocr_rec.get("data", {})
    pdf_file = ocr_rec.get("pdf_file", data.get("file_name", ""))

    # Get metadata fields
    title = ""
    publish_date = None
    category = ""
    book_no = ""
    section = ""
    page_no = ""

    if meta_rec:
        title = meta_rec.get("doctitle", "")
        publish_date = meta_rec.get("publishDate")
        category = meta_rec.get("category", "")
        book_no = meta_rec.get("bookNo", "")
        section = meta_rec.get("section", "")
        page_no = meta_rec.get("pageNo", "")

    # Build ID from pdf_file
    doc_id = pdf_file.replace("/", "_").replace(".pdf", "")
    if not doc_id:
        return None

    if not title:
        title = doc_id

    return {
        "_id": f"TH_RG_{doc_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "date": publish_date,
        "category": category,
        "book_no": book_no,
        "section": section,
        "page_no": page_no,
        "text": text,
        "url": f"https://huggingface.co/datasets/{DATASET}",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all Royal Gazette records with OCR text."""
    print("  Listing OCR files...")
    ocr_files = list_ocr_files()
    print(f"  Found {len(ocr_files)} OCR files")

    if sample:
        # Use 2 recent months for sample
        ocr_files = ocr_files[-2:]

    total_yielded = 0
    for ocr_path in ocr_files:
        print(f"  Processing: {ocr_path}")

        # Derive corresponding meta path: ocr/iapp/2024/2024-01.jsonl -> meta/2024/2024-01.jsonl
        meta_path = ocr_path.replace("ocr/iapp/", "meta/")

        # Load meta index for this month
        try:
            meta_index = load_meta_index(meta_path)
            print(f"    Loaded {len(meta_index)} meta records")
        except Exception as e:
            print(f"    Warning: could not load meta {meta_path}: {e}")
            meta_index = {}

        # Load OCR data
        try:
            url = f"{BASE_URL}/{ocr_path}"
            content = http_get(url).decode("utf-8")
        except Exception as e:
            print(f"    Error downloading {ocr_path}: {e}")
            continue

        time.sleep(1)  # Rate limit

        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                ocr_rec = json.loads(line)
            except json.JSONDecodeError:
                continue

            pdf_file = ocr_rec.get("pdf_file", "")
            meta_rec = meta_index.get(pdf_file)

            record = normalize(ocr_rec, meta_rec)
            if record:
                yield record
                total_yielded += 1

                if sample and total_yielded >= 15:
                    return

    print(f"  Total records with text: {total_yielded}")


def test_connection():
    """Test connectivity to the HuggingFace dataset."""
    print("Testing Thailand Royal Gazette HuggingFace connectivity...")

    print("\n1. Checking dataset API...")
    try:
        data = json.loads(http_get(API_URL))
        print(f"   OK: Dataset found - {data.get('id', 'unknown')}")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n2. Listing OCR files...")
    try:
        ocr_files = list_ocr_files()
        print(f"   OK: Found {len(ocr_files)} OCR files")
        if ocr_files:
            print(f"   Range: {ocr_files[0]} to {ocr_files[-1]}")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n3. Downloading sample OCR file...")
    sample_ocr = ocr_files[-1] if ocr_files else "ocr/iapp/2024/2024-01.jsonl"
    try:
        url = f"{BASE_URL}/{sample_ocr}"
        content = http_get(url).decode("utf-8")
        lines = [l for l in content.splitlines() if l.strip()]
        print(f"   OK: {len(lines)} records in {sample_ocr}")
    except Exception as e:
        print(f"   FAIL: {e}")
        return False

    print("\n4. Checking OCR text quality...")
    first_line = lines[0] if lines else None
    if first_line:
        rec = json.loads(first_line)
        record = normalize(rec, None)
        if record:
            print(f"   Title: {record['title'][:80]}")
            print(f"   Text length: {len(record['text'])} chars")
            print(f"   Text preview: {record['text'][:150]}...")
        else:
            print("   Warning: first record had no usable text")

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="TH/HuggingFaceRG Thailand Royal Gazette Fetcher")
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
