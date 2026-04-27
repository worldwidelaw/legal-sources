#!/usr/bin/env python3
"""
RW/Courts -- Rwanda Courts Decisions Fetcher

Fetches case law from amategeko.gov.rw, the Portal of Rwandan Laws and Case Laws
operated by the Rwanda Law Reform Commission (RLRC). Uses the public REST API
to list court decisions, downloads PDFs, and extracts full text via pdfplumber.

Sections:
  - 2.1:   Unreported decisions (~3,465 individual judgments)
  - 2.2.1: Reported decisions (Rwanda Law Reports volumes)
  - 2.2.2: Reported decisions
  - 2.2.3: Reported decisions

Courts covered: Supreme Court, Court of Appeal, High Court, Commercial High Court,
Intermediate Court, Commercial Court, ICTR.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap [--full]
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

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "RW/Courts"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RW.Courts")

API_BASE = "https://apis.amategeko.gov.rw/v1/site"
SITE_BASE = "https://www.amategeko.gov.rw"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CASE_LAW_SECTIONS = ["2.1", "2.2.1", "2.2.2", "2.2.3"]
PAGE_SIZE = 200


def list_documents(section: str, start: int = 0, length: int = PAGE_SIZE) -> dict:
    """List documents from a section via the search API with retry."""
    url = f"{API_BASE}/documents/search"
    for attempt in range(3):
        try:
            resp = SESSION.get(url, params={
                "start": start,
                "length": length,
                "section": section,
            }, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"list_documents attempt {attempt+1}/3 failed "
                           f"(section={section}, start={start}): {e}")
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                raise


def download_pdf(file_path: str) -> Optional[bytes]:
    """Download a PDF file from the API."""
    url = f"{API_BASE}/files/download"
    try:
        resp = SESSION.post(url, data={"path": file_path}, timeout=120)
        resp.raise_for_status()
        if len(resp.content) > 100:
            return resp.content
    except Exception as e:
        logger.warning(f"Failed to download PDF {file_path}: {e}")
    return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="RW/Courts",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def fetch_document(record: dict) -> Optional[dict]:
    """Fetch a single court decision with full text from PDF."""
    src = record.get("_source", record)
    doc_id = src.get("document_id", src.get("id"))
    file_id = src.get("file_id", src.get("id"))
    name = src.get("document_name", "")
    date = src.get("document_date", "")
    section = src.get("document_section", "")
    case_no = src.get("document_case_no", "")

    # Get PDF path from file_info in the record itself
    file_info = src.get("file_info", {})
    file_path = file_info.get("path", "")
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
        logger.warning(f"Insufficient text for document {doc_id}: {name} ({len(text)} chars)")
        return None

    # Court name
    court = src.get("document_collection_name", "")
    court_rw = src.get("document_collection_name_rw", "")
    court_fr = src.get("document_collection_name_fr", "")

    # Decision type and status
    decision_type = src.get("document_decisions_types_name", "")
    decision_status = src.get("document_decisions_status_name", "")
    judgment_type = src.get("document_relations_name", "")

    # Language
    language = src.get("file_languages", "rw")

    # Keywords and references
    keywords = src.get("document_keywords", "")
    if keywords:
        keywords = [k.strip() for k in keywords.split(",") if k.strip()]
    else:
        keywords = []

    references = src.get("file_references_selected", "") or src.get("file_references", "")
    if references:
        references = [r.strip() for r in references.split(",") if r.strip()]
    else:
        references = []

    # URL to view on the site
    if case_no:
        view_url = f"{SITE_BASE}/view/doc_case/{case_no}"
    else:
        view_url = f"{SITE_BASE}/view/toc/doc/{doc_id}/{file_id}"

    return {
        "_id": f"RW-courts-{doc_id}-{file_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": name,
        "text": text,
        "date": date,
        "url": view_url,
        "document_case_no": case_no or None,
        "court": court,
        "court_rw": court_rw,
        "court_fr": court_fr,
        "decision_type": decision_type,
        "decision_status": decision_status,
        "judgment_type": judgment_type,
        "section": section,
        "language": language,
        "keywords": keywords,
        "references": references,
        "file_size": file_info.get("size", 0),
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all case law documents with full text.

    Paginates through every section, advancing by the actual number of
    records the API returned (not by PAGE_SIZE) so that a server-side cap
    smaller than PAGE_SIZE cannot cause pages to be silently skipped.
    Pagination stops only when the API returns an empty page.
    """
    for section in CASE_LAW_SECTIONS:
        logger.info(f"Fetching section {section}...")
        offset = 0
        page_num = 0
        while True:
            data = list_documents(section, start=offset, length=PAGE_SIZE)
            inner = data.get("data", {})

            if page_num == 0:
                total = inner.get("recordsFiltered", "?")
                logger.info(f"Section {section}: {total} documents reported by API")

            records = inner.get("data", [])
            if not records:
                logger.info(f"Section {section}: empty page at offset {offset}, done.")
                break

            page_num += 1
            got = len(records)
            logger.info(f"Section {section} page {page_num}: "
                         f"got {got} records (offset {offset})")

            for record in records:
                doc = fetch_document(record)
                if doc:
                    yield doc
                time.sleep(1.5)

            # Advance by actual records received, not PAGE_SIZE, so a
            # server-side cap (e.g. max 100) doesn't skip records.
            offset += got

            # If we got fewer records than requested, we've reached the end.
            if got < PAGE_SIZE:
                logger.info(f"Section {section}: last page ({got} < {PAGE_SIZE}), done.")
                break
            time.sleep(1)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a date."""
    for doc in fetch_all():
        if doc["date"] and doc["date"] >= since:
            yield doc


def normalize(raw: dict) -> dict:
    """Already normalized during fetch."""
    return raw


def test_api():
    """Test API access and PDF extraction."""
    print("Testing RW/Courts API...")

    for section in CASE_LAW_SECTIONS:
        data = list_documents(section, start=0, length=1)
        inner = data.get("data", {})
        total = inner.get("recordsFiltered", 0)
        print(f"  Section {section}: {total} documents")

    # Test PDF extraction on first unreported decision
    data = list_documents("2.1", start=0, length=3)
    records = data.get("data", {}).get("data", [])
    if records:
        print("\n  Testing PDF extraction...")
        doc = fetch_document(records[0])
        if doc:
            print(f"  Title: {doc['title'][:80]}")
            print(f"  Court: {doc['court']}")
            print(f"  Case No: {doc['document_case_no']}")
            print(f"  Date: {doc['date']}")
            print(f"  Text length: {len(doc['text'])} chars")
            print(f"  Text preview: {doc['text'][:300]}...")
        else:
            print("  FAILED to fetch document")

    print("\nTest complete.")


def bootstrap(sample: bool = False, full: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    if sample:
        count = 0
        target = 15
        for section in CASE_LAW_SECTIONS:
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
                        f"({len(doc['text'])} chars) - {doc['court']}"
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
    parser = argparse.ArgumentParser(description="RW/Courts fetcher")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("test-api", help="Test API access")

    boot = sub.add_parser("bootstrap", help="Run bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only (15 docs)")
    boot.add_argument("--full", action="store_true", help="Fetch all documents")

    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, full=getattr(args, "full", False))
    else:
        parser.print_help()
