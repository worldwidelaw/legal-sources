#!/usr/bin/env python3
"""
RW/Amategeko -- Rwanda Official Laws Portal Data Fetcher

Fetches legislation from amategeko.gov.rw, operated by the Rwanda Law Reform
Commission (RLRC). Uses the public REST JSON API to list documents, then
downloads PDFs and extracts full text via pdfplumber.

Sections:
  - 1.1: Legislation in force (~1,400+ laws)
  - 1.2: Historical/other legislation (~1,200+)

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import io
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    import pdfplumber
except ImportError:
    print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
    sys.exit(1)

SOURCE_ID = "RW/Amategeko"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RW.Amategeko")

API_BASE = "https://apis.amategeko.gov.rw/v1/site"
SITE_BASE = "https://www.amategeko.gov.rw"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Legislation sections to fetch
SECTIONS = ["1.1", "1.2"]
PAGE_SIZE = 200


def list_documents(section: str, start: int = 0, length: int = PAGE_SIZE) -> dict:
    """List documents from a section via the API."""
    url = f"{API_BASE}/documents/table"
    resp = SESSION.get(url, params={
        "start": start,
        "length": length,
        "section": section,
    }, timeout=60)
    resp.raise_for_status()
    return resp.json()


def get_file_info(document_id: int) -> Optional[dict]:
    """Get file metadata for a document."""
    url = f"{API_BASE}/documents-files/table"
    resp = SESSION.get(url, params={"document": document_id}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    files = data.get("data", {}).get("data", [])
    if files:
        return files[0]
    return None


def download_pdf(file_path: str) -> Optional[bytes]:
    """Download a PDF file from the API."""
    url = f"{API_BASE}/files/download"
    try:
        resp = SESSION.post(url, data={"path": file_path}, timeout=120)
        resp.raise_for_status()
        if resp.headers.get("Content-Type", "").startswith("application/pdf") or len(resp.content) > 100:
            return resp.content
    except Exception as e:
        logger.warning(f"Failed to download PDF {file_path}: {e}")
    return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
        pages = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        pdf.close()
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"Failed to extract PDF text: {e}")
        return ""


def fetch_document(record: dict) -> Optional[dict]:
    """Fetch a single document with full text from PDF."""
    src = record["_source"]
    doc_id = src["document_id"]
    file_id = src["id"]
    name = src.get("document_name", "")
    date = src.get("document_date", "")
    section = src.get("document_section", "")
    case_no = src.get("document_case_no", "")

    # Get file info
    file_info = get_file_info(doc_id)
    if not file_info:
        logger.warning(f"No file found for document {doc_id}: {name}")
        return None

    file_obj = file_info.get("file", {})
    file_path = file_obj.get("path", "")
    if not file_path:
        logger.warning(f"No file path for document {doc_id}: {name}")
        return None

    # Download PDF
    pdf_bytes = download_pdf(file_path)
    if not pdf_bytes:
        logger.warning(f"Failed to download PDF for document {doc_id}: {name}")
        return None

    # Extract text
    text = extract_text_from_pdf(pdf_bytes)
    if not text or len(text) < 50:
        logger.warning(f"Insufficient text extracted for document {doc_id}: {name} ({len(text)} chars)")
        return None

    # Determine language from the file info
    languages = file_info.get("languages", "en")

    # Build category info
    category = src.get("document_category_name") or src.get("document_category_name_rw") or ""
    sub_category = src.get("document_sub_category_name") or src.get("document_sub_category_name_rw") or ""

    return {
        "_id": f"RW-amategeko-{doc_id}-{file_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": name,
        "text": text,
        "date": date,
        "url": f"{SITE_BASE}/view/toc/doc/{doc_id}/{file_id}",
        "document_code": case_no,
        "category": category,
        "sub_category": sub_category,
        "section": section,
        "language": languages,
        "file_size": file_obj.get("size", 0),
        "page_count": len(text.split("\n\n")),
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all legislation documents with full text."""
    for section in SECTIONS:
        logger.info(f"Fetching section {section}...")
        offset = 0
        total = None
        while True:
            data = list_documents(section, start=offset, length=PAGE_SIZE)
            inner = data.get("data", {})
            if total is None:
                total = inner.get("recordsFiltered", 0)
                logger.info(f"Section {section}: {total} documents")

            records = inner.get("data", [])
            if not records:
                break

            for record in records:
                doc = fetch_document(record)
                if doc:
                    yield doc
                time.sleep(1.5)

            offset += PAGE_SIZE
            if offset >= total:
                break
            time.sleep(1)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a date (re-fetches all, filters by date)."""
    for doc in fetch_all():
        if doc["date"] and doc["date"] >= since:
            yield doc


def normalize(raw: dict) -> dict:
    """Already normalized during fetch."""
    return raw


def test_api():
    """Test API access and PDF extraction."""
    print("Testing RW/Amategeko API...")

    # Test document listing
    data = list_documents("1.1", start=0, length=3)
    inner = data.get("data", {})
    total = inner.get("recordsFiltered", 0)
    print(f"  Section 1.1: {total} documents")

    records = inner.get("data", [])
    for rec in records[:2]:
        src = rec["_source"]
        print(f"  - {src['document_name'][:80]} ({src['document_date']})")

    # Test PDF download and extraction
    if records:
        print("\n  Testing PDF extraction...")
        doc = fetch_document(records[0])
        if doc:
            print(f"  Title: {doc['title'][:80]}")
            print(f"  Text length: {len(doc['text'])} chars")
            print(f"  Text preview: {doc['text'][:300]}...")
        else:
            print("  FAILED to fetch document")

    # Test section 1.2
    data2 = list_documents("1.2", start=0, length=1)
    inner2 = data2.get("data", {})
    total2 = inner2.get("recordsFiltered", 0)
    print(f"\n  Section 1.2: {total2} documents")

    print("\nTest complete.")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    if sample:
        count = 0
        target = 15
        for section in SECTIONS:
            if count >= target:
                break
            data = list_documents(section, start=0, length=20)
            records = data.get("data", {}).get("data", [])
            for record in records:
                if count >= target:
                    break
                doc = fetch_document(record)
                if doc:
                    count += 1
                    out_file = SAMPLE_DIR / f"{doc['_id']}.json"
                    out_file.write_text(
                        json.dumps(doc, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                    logger.info(
                        f"[{count}/{target}] Saved {doc['_id']} "
                        f"({len(doc['text'])} chars)"
                    )
                time.sleep(1.5)
        logger.info(f"Sample complete: {count} documents saved to {SAMPLE_DIR}")
    else:
        count = 0
        for doc in fetch_all():
            count += 1
            out_file = SAMPLE_DIR / f"{doc['_id']}.json"
            out_file.write_text(
                json.dumps(doc, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            if count % 25 == 0:
                logger.info(f"Progress: {count} documents fetched")
        logger.info(f"Bootstrap complete: {count} documents saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RW/Amategeko fetcher")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("test-api", help="Test API access")

    boot = sub.add_parser("bootstrap", help="Run bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")

    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)
    else:
        parser.print_help()
