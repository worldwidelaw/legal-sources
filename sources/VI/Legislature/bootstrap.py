#!/usr/bin/env python3
"""
VI/Legislature - US Virgin Islands Legislature Acts

Fetches enacted legislation from the USVI Bill Tracking System JSON API.
Full text is extracted from act PDFs (where available — ~46% have text layers,
remainder are scanned images requiring OCR and are skipped).

Data source: https://billtracking.legvi.org/
Format: JSON API with pagination + PDF attachments
License: Public government data
Rate limit: 1 req/sec (courtesy)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap --full      # Full bootstrap
  python bootstrap.py test                  # Test API connectivity
"""

import base64
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

API_URL = "https://billtracking.legvi.org/"
PDF_URL = "https://billtracking.legvi.org/view-pdf/"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "VI/Legislature"
PAGE_SIZE = 9  # API returns HTML (not JSON) when pgl >= 10; always returns 10 bills regardless

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json, text/html",
}


def fetch_bill_page(page: int, page_size: int = PAGE_SIZE) -> Optional[dict]:
    """Fetch a page of bills from the AJAX API."""
    url = f"{API_URL}?ajax=1&pageno={page}&pgl={page_size}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        raw = resp.content.decode("utf-8-sig")
        return json.loads(raw)
    except (requests.RequestException, json.JSONDecodeError) as e:
        print(f"  Error fetching page {page}: {e}")
        return None


def extract_pdf_text(pdf_b64: str, doc_id: str) -> Optional[str]:
    """Download and extract text from an act PDF via view-pdf endpoint."""
    url = f"{PDF_URL}?pdf_path={pdf_b64}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        pdf_bytes = resp.content

        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            return None

        # Try common.pdf_extract first
        try:
            from common.pdf_extract import extract_pdf_markdown
            text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=doc_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
                force=True,
            )
            if text and len(text.strip()) > 50:
                return text.strip()
        except (ImportError, Exception):
            pass

        # Fallback: pdfplumber
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(pages).strip()
                if len(text) > 50:
                    return text
        except Exception:
            pass

        # Fallback: pypdf
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = [p.extract_text() or "" for p in reader.pages]
            text = "\n\n".join(pages).strip()
            if len(text) > 50:
                return text
        except Exception:
            pass

        return None

    except requests.RequestException as e:
        print(f"    PDF download failed for {doc_id}: {e}")
        return None


def parse_date(date_str: str) -> Optional[str]:
    """Parse MM-DD-YYYY to ISO YYYY-MM-DD."""
    if not date_str:
        return None
    for fmt in ["%m-%d-%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def normalize(bill: dict, text: str) -> dict:
    """Build normalized record from bill API data + extracted text."""
    act_no = bill.get("ACTNO", "")
    bill_no = bill.get("BILLNUMBER", "")
    leg = bill.get("LEGINUM", "")

    doc_id = f"act-{act_no}" if act_no else f"bill-{bill_no}" if bill_no else f"doc-{bill['DocEntry']}"

    title = bill.get("SUBJCT", "").strip()
    if not title:
        title = f"Act No. {act_no}" if act_no else f"Bill No. {bill_no}"

    date = parse_date(bill.get("LASTACTDT")) or parse_date(bill.get("INTRODT"))

    detail_url = f"https://billtracking.legvi.org/bill_detail/{bill['DocEntry']}/"

    sponsors = []
    if bill.get("PRIMARYSPON"):
        sponsors.append(bill["PRIMARYSPON"])
    if bill.get("SPONSOR"):
        sponsors.extend(s.strip() for s in bill["SPONSOR"].split("|"))

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": detail_url,
        "act_number": act_no or None,
        "bill_number": bill_no or None,
        "legislature": f"{leg}th" if leg else None,
        "status": bill.get("LASTSTATUS"),
        "request_type": bill.get("REQUESTTYPE"),
        "sponsors": sponsors or None,
        "language": "en",
    }


def fetch_all(max_records: int = None, max_pages: int = None) -> Generator[dict, None, None]:
    """Fetch all enacted acts with full text from PDFs."""
    # First page to get total count
    first = fetch_bill_page(1)
    if not first or not first.get("bills"):
        print("ERROR: Could not fetch first page")
        return

    total_pages = first.get("totalPages", 1)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    print(f"Total pages: {total_pages}")
    total_yielded = 0
    skipped_no_pdf = 0
    skipped_no_text = 0

    for page in range(1, total_pages + 1):
        if max_records and total_yielded >= max_records:
            break

        if page == 1:
            data = first
        else:
            data = fetch_bill_page(page)
            time.sleep(1)

        if not data or not data.get("bills"):
            continue

        for bill in data["bills"]:
            if max_records and total_yielded >= max_records:
                break

            # Only process bills with enacted act PDFs
            acb64 = bill.get("ACPATH_B64", "")
            if not acb64:
                skipped_no_pdf += 1
                continue

            doc_id = f"act-{bill.get('ACTNO', bill['DocEntry'])}"

            # Extract text from PDF
            time.sleep(1)
            text = extract_pdf_text(acb64, doc_id)
            if not text:
                skipped_no_text += 1
                continue

            record = normalize(bill, text)
            yield record
            total_yielded += 1

            if total_yielded % 10 == 0:
                print(f"  Progress: {total_yielded} records yielded, "
                      f"{skipped_no_pdf} no-pdf, {skipped_no_text} no-text")

    print(f"\nCompleted: {total_yielded} records yielded")
    print(f"  Skipped (no act PDF): {skipped_no_pdf}")
    print(f"  Skipped (image/no text): {skipped_no_text}")


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records by scanning across page ranges to find text-extractable acts."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample USVI Legislature acts...")
    print("=" * 60)

    records = []
    # Scan wider range: many recent acts are scanned images, born-digital PDFs
    # appear more frequently in the 34th legislature (~pages 70-130)
    for record in fetch_all(max_records=sample_count, max_pages=200):
        records.append(record)
        idx = len(records)
        filename = SAMPLE_DIR / f"record_{idx:03d}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        date = record.get("date", "??")
        act = record.get("act_number", "??")
        print(f"  [{idx:02d}] Act {act} ({date}) — {text_len:,} chars")

    print("\n" + "=" * 60)
    print(f"Saved {len(records)} records to {SAMPLE_DIR}")

    if records:
        avg_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_len:,.0f} chars/doc")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty = sum(1 for r in records if not r.get("text"))
    if empty > 0:
        print(f"WARNING: {empty} records have empty text!")
        return False

    print(f"\nVALIDATION PASSED: {len(records)} records with text.")
    return True


def bootstrap_full():
    """Full bootstrap."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    print("Starting full bootstrap of USVI Legislature acts...")
    count = 0
    for record in fetch_all():
        count += 1
        filename = SAMPLE_DIR / f"record_{count:04d}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        if count % 50 == 0:
            print(f"  Written {count} records...")
    print(f"\nFull bootstrap complete: {count} records")


def test_api():
    """Test API connectivity."""
    print("Testing USVI Bill Tracking API...")
    data = fetch_bill_page(1, page_size=3)
    if not data:
        print("FAIL: Could not fetch API")
        return False

    bills = data.get("bills", [])
    total = data.get("totalPages", 0)
    print(f"OK: {len(bills)} bills on page 1, {total} total pages")

    # Test PDF access
    for bill in bills:
        acb64 = bill.get("ACPATH_B64", "")
        if acb64:
            text = extract_pdf_text(acb64, "test")
            if text:
                print(f"OK: PDF text extraction works ({len(text)} chars)")
            else:
                print("INFO: PDF is image-based (no text layer)")
            break

    return True


def main():
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample|--full]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        success = test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        if "--sample" in sys.argv:
            count = 15
            if "--sample-size" in sys.argv:
                idx = sys.argv.index("--sample-size")
                count = int(sys.argv[idx + 1])
            success = bootstrap_sample(count)
            sys.exit(0 if success else 1)
        elif "--full" in sys.argv:
            bootstrap_full()
        else:
            bootstrap_sample()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
