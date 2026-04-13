#!/usr/bin/env python3
"""
PK/FBR-TaxDoctrine -- Pakistan Federal Board of Revenue Tax Doctrine

Fetches tax circulars, SROs (Statutory Regulatory Orders), and general orders
from the FBR website. Full text extracted from PDFs hosted on download1.fbr.gov.pk.

Data types covered:
  - Income Tax Circulars (~731 docs, 1979-2025)
  - Sales Tax Circulars (~141 docs, 1989-2025)
  - Customs General Orders (~159 docs, 1999-2026)
  - Sales Tax General Orders (~1316 docs, 1991-2026)
  - SROs by department: Income Tax, Sales Tax, Customs, Federal Excise (~4763 docs)

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
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

SOURCE_ID = "PK/FBR-TaxDoctrine"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PK.FBR-TaxDoctrine")

BASE_URL = "https://www.fbr.gov.pk"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research project)",
    "Accept": "application/json",
    "X-Requested-With": "XMLHttpRequest",
}

REQUEST_DELAY = 1.5  # seconds between requests
MIN_TEXT_LENGTH = 50  # skip scanned PDFs with only watermark text

# Circular category pages: (order_type, page_url_suffix, category_id_for_main_page)
# We'll discover year-specific CategoryIDs by scraping the dropdown
CIRCULAR_PAGES = [
    ("Income Tax Circulars", "/Orders/Income-Tax-Circulars/231", 231),
    ("Sales Tax Circulars", "/Orders/Sales-Tax-Circulars/180", 180),
    ("Customs General Orders", "/Orders/Customs-General-Orders/131", 131),
    ("Sales Tax General Orders", "/Orders/Sales-Tax-General-Orders/151", 151),
    ("Federal Excise Circulars", "/Orders/Federal-Excise-Circulars/345", 345),
    ("Federal Excise General Orders", "/Orders/Federal-Excise-General-Orders/179", 179),
    ("Customs Circulars", "/Orders/Customs-Circulars/623", 623),
]

SRO_DEPARTMENTS = [
    "Income Tax",
    "Sales Tax",
    "Customs",
    "Federal Excise",
]


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="PK/FBR-TaxDoctrine",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

def download_pdf_text(url: str) -> str:
    """Download PDF and extract text."""
    if not url:
        return ""
    try:
        resp = requests.get(url, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=60)
        resp.raise_for_status()
        if "pdf" not in resp.headers.get("content-type", "").lower() and len(resp.content) < 100:
            return ""
        return extract_text_from_pdf(resp.content)
    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to download PDF {url}: {e}")
        return ""


def parse_dotnet_date(date_str: str) -> Optional[str]:
    """Parse .NET /Date(timestamp)/ format to ISO 8601."""
    if not date_str:
        return None
    m = re.search(r"/Date\((\d+)\)/", str(date_str))
    if m:
        ts = int(m.group(1)) / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    return None


def clean_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def get_category_ids(page_url: str) -> list:
    """Scrape year-specific CategoryIDs from the dropdown on a circulars page."""
    try:
        resp = requests.get(
            f"{BASE_URL}{page_url}",
            headers={"User-Agent": HEADERS["User-Agent"]},
            timeout=30,
        )
        resp.raise_for_status()
        # Parse <select id="Categories"> options
        pattern = r'<option\s+value="(\d+)"[^>]*>([^<]+)</option>'
        matches = re.findall(pattern, resp.text)
        ids = []
        for cat_id, label in matches:
            cat_id = int(cat_id)
            if cat_id > 0:
                ids.append((cat_id, label.strip()))
        return ids
    except Exception as e:
        logger.error(f"Failed to get categories from {page_url}: {e}")
        return []


def fetch_circulars_for_category(category_id: int) -> list:
    """Fetch all circulars for a given year-category ID."""
    try:
        resp = requests.post(
            f"{BASE_URL}/Home/ShowOrdersFiltered",
            data={
                "draw": 1,
                "start": 0,
                "length": 500,
                "CategoryID": category_id,
                "Parent": "false",
            },
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except Exception as e:
        logger.error(f"Failed to fetch circulars for category {category_id}: {e}")
        return []


def fetch_all_circulars(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all circular/general order documents."""
    total = 0
    for order_type, page_url, _ in CIRCULAR_PAGES:
        logger.info(f"Fetching categories for: {order_type}")
        categories = get_category_ids(page_url)
        if not categories:
            logger.warning(f"  No categories found for {order_type}")
            continue

        logger.info(f"  Found {len(categories)} year-categories")

        for cat_id, cat_label in categories:
            time.sleep(REQUEST_DELAY)
            docs = fetch_circulars_for_category(cat_id)
            if not docs:
                continue

            for doc in docs:
                pdf_url = doc.get("UploadedFile1") or ""
                if not pdf_url:
                    continue

                time.sleep(REQUEST_DELAY)
                text = download_pdf_text(pdf_url)
                if not text or len(text) < MIN_TEXT_LENGTH:
                    logger.debug(f"  Insufficient text from {pdf_url} ({len(text)} chars)")
                    continue

                doc_id = doc.get("DocumentID", "")
                title = doc.get("DocumentTitle") or ""
                doc_number = doc.get("DocumentNumber") or ""
                date = parse_dotnet_date(doc.get("CreationDate"))

                record = {
                    "_id": f"PK-FBR-DOC-{doc_id}",
                    "_source": SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": title.strip(),
                    "text": text,
                    "date": date,
                    "url": pdf_url,
                    "document_number": doc_number.strip(),
                    "category": order_type,
                    "year": cat_label,
                }
                yield record
                total += 1

                if limit and total >= limit:
                    return

        logger.info(f"  {order_type}: fetched {total} total so far")


def fetch_sros(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch SROs from all departments."""
    total = 0

    for dept in SRO_DEPARTMENTS:
        logger.info(f"Fetching SROs for department: {dept}")
        offset = 0
        page_size = 100
        dept_count = 0

        while True:
            time.sleep(REQUEST_DELAY)
            # Build full DataTables request
            data = {
                "draw": str(offset // page_size + 1),
                "start": str(offset),
                "length": str(page_size),
                "department": dept,
                "columns[0][data]": "SROID",
                "columns[0][name]": "",
                "columns[0][searchable]": "true",
                "columns[0][orderable]": "true",
                "columns[0][search][value]": "",
                "columns[0][search][regex]": "false",
                "columns[1][data]": "SRONumber",
                "columns[1][name]": "",
                "columns[1][searchable]": "true",
                "columns[1][orderable]": "true",
                "columns[1][search][value]": "",
                "columns[1][search][regex]": "false",
                "columns[2][data]": "Title",
                "columns[2][name]": "",
                "columns[2][searchable]": "true",
                "columns[2][orderable]": "true",
                "columns[2][search][value]": "",
                "columns[2][search][regex]": "false",
                "columns[3][data]": "CreationDate",
                "columns[3][name]": "",
                "columns[3][searchable]": "true",
                "columns[3][orderable]": "true",
                "columns[3][search][value]": "",
                "columns[3][search][regex]": "false",
                "columns[4][data]": "UploadedFile1",
                "columns[4][name]": "",
                "columns[4][searchable]": "true",
                "columns[4][orderable]": "true",
                "columns[4][search][value]": "",
                "columns[4][search][regex]": "false",
                "order[0][column]": "3",
                "order[0][dir]": "desc",
                "search[value]": "",
                "search[regex]": "false",
            }

            try:
                resp = requests.post(
                    f"{BASE_URL}/Home/LoadSROs",
                    data=data,
                    headers=HEADERS,
                    timeout=30,
                )
                resp.raise_for_status()
                result = resp.json()
            except Exception as e:
                logger.error(f"Failed to fetch SROs at offset {offset}: {e}")
                break

            sros = result.get("data", [])
            if not sros:
                break

            for sro in sros:
                pdf_url = sro.get("UploadedFile1") or ""
                if not pdf_url:
                    continue

                time.sleep(REQUEST_DELAY)
                text = download_pdf_text(pdf_url)

                # Also try Detail field as fallback
                detail = clean_html(sro.get("Detail") or "")
                if (not text or len(text) < MIN_TEXT_LENGTH) and detail and len(detail) > MIN_TEXT_LENGTH:
                    text = detail
                elif not text or len(text) < MIN_TEXT_LENGTH:
                    logger.debug(f"  Insufficient text for SRO {sro.get('SROID')}")
                    continue

                sro_id = sro.get("SROID", "")
                title = sro.get("Title") or sro.get("SRONumber") or ""
                sro_number = sro.get("SRONumber") or ""
                date = parse_dotnet_date(sro.get("CreationDate"))

                record = {
                    "_id": f"PK-FBR-SRO-{sro_id}",
                    "_source": SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": title.strip(),
                    "text": text,
                    "date": date,
                    "url": pdf_url,
                    "document_number": sro_number.strip(),
                    "category": f"SRO - {dept}",
                    "department": dept,
                }
                yield record
                total += 1
                dept_count += 1

                if limit and total >= limit:
                    return

            offset += page_size
            records_total = int(result.get("recordsTotal", 0))
            if offset >= records_total:
                break

        logger.info(f"  {dept} SROs: {dept_count} records")


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all FBR tax doctrine documents."""
    count = 0

    # Circulars and general orders first
    for record in fetch_circulars_for_sample(limit):
        yield record
        count += 1
        if limit and count >= limit:
            return

    # Then SROs
    remaining = (limit - count) if limit else None
    for record in fetch_sros(limit=remaining):
        yield record
        count += 1
        if limit and count >= limit:
            return


def fetch_circulars_for_sample(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch circulars, optimized: tries most productive categories first."""
    total = 0
    # Start with Income Tax Circulars (most docs) for sample
    priority_pages = [
        ("Income Tax Circulars", "/Orders/Income-Tax-Circulars/231", 231),
        ("Sales Tax General Orders", "/Orders/Sales-Tax-General-Orders/151", 151),
    ]
    pages = priority_pages if limit else CIRCULAR_PAGES

    for order_type, page_url, _ in pages:
        logger.info(f"Fetching categories for: {order_type}")
        categories = get_category_ids(page_url)
        if not categories:
            continue

        # For sample, just use latest year
        cats_to_use = categories[:3] if limit else categories

        for cat_id, cat_label in cats_to_use:
            time.sleep(REQUEST_DELAY)
            docs = fetch_circulars_for_category(cat_id)
            if not docs:
                continue

            for doc in docs:
                pdf_url = doc.get("UploadedFile1") or ""
                if not pdf_url:
                    continue

                time.sleep(REQUEST_DELAY)
                text = download_pdf_text(pdf_url)
                if not text or len(text) < MIN_TEXT_LENGTH:
                    continue

                doc_id = doc.get("DocumentID", "")
                title = doc.get("DocumentTitle") or ""
                doc_number = doc.get("DocumentNumber") or ""
                date = parse_dotnet_date(doc.get("CreationDate"))

                record = {
                    "_id": f"PK-FBR-DOC-{doc_id}",
                    "_source": SOURCE_ID,
                    "_type": "doctrine",
                    "_fetched_at": datetime.now(timezone.utc).isoformat(),
                    "title": title.strip(),
                    "text": text,
                    "date": date,
                    "url": pdf_url,
                    "document_number": doc_number.strip(),
                    "category": order_type,
                    "year": cat_label,
                }
                yield record
                total += 1

                if limit and total >= limit:
                    return


def test_api():
    """Test FBR API connectivity."""
    logger.info("Testing FBR API...")

    # Test circulars endpoint
    logger.info("\n1. Testing Income Tax Circulars page...")
    categories = get_category_ids("/Orders/Income-Tax-Circulars/231")
    logger.info(f"   Found {len(categories)} year-categories")
    if categories:
        cat_id, cat_label = categories[0]
        logger.info(f"   Latest: {cat_label} (ID: {cat_id})")
        docs = fetch_circulars_for_category(cat_id)
        logger.info(f"   Documents in latest year: {len(docs)}")
        if docs:
            doc = docs[0]
            logger.info(f"   Sample: {doc.get('DocumentTitle', '')[:80]}")
            pdf_url = doc.get("UploadedFile1", "")
            if pdf_url:
                logger.info(f"   PDF URL: {pdf_url}")
                text = download_pdf_text(pdf_url)
                logger.info(f"   Text extracted: {len(text)} chars")
                if text:
                    logger.info(f"   Preview: {text[:200]}...")

    # Test SROs endpoint
    time.sleep(REQUEST_DELAY)
    logger.info("\n2. Testing SROs endpoint...")
    data = {
        "draw": "1",
        "start": "0",
        "length": "2",
        "department": "Income Tax",
        "columns[0][data]": "SROID",
        "columns[0][name]": "",
        "columns[0][searchable]": "true",
        "columns[0][orderable]": "true",
        "columns[0][search][value]": "",
        "columns[0][search][regex]": "false",
        "columns[1][data]": "SRONumber",
        "columns[1][name]": "",
        "columns[1][searchable]": "true",
        "columns[1][orderable]": "true",
        "columns[1][search][value]": "",
        "columns[1][search][regex]": "false",
        "columns[2][data]": "Title",
        "columns[2][name]": "",
        "columns[2][searchable]": "true",
        "columns[2][orderable]": "true",
        "columns[2][search][value]": "",
        "columns[2][search][regex]": "false",
        "columns[3][data]": "CreationDate",
        "columns[3][name]": "",
        "columns[3][searchable]": "true",
        "columns[3][orderable]": "true",
        "columns[3][search][value]": "",
        "columns[3][search][regex]": "false",
        "columns[4][data]": "UploadedFile1",
        "columns[4][name]": "",
        "columns[4][searchable]": "true",
        "columns[4][orderable]": "true",
        "columns[4][search][value]": "",
        "columns[4][search][regex]": "false",
        "order[0][column]": "3",
        "order[0][dir]": "desc",
        "search[value]": "",
        "search[regex]": "false",
    }
    try:
        resp = requests.post(
            f"{BASE_URL}/Home/LoadSROs",
            data=data,
            headers=HEADERS,
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        total = result.get("recordsTotal", 0)
        logger.info(f"   Income Tax SROs total: {total}")
        sros = result.get("data", [])
        if sros:
            sro = sros[0]
            logger.info(f"   Sample: {sro.get('Title', '')[:80]}")
            logger.info(f"   Number: {sro.get('SRONumber', '')}")
    except Exception as e:
        logger.error(f"   SROs test failed: {e}")


def bootstrap(sample: bool = False, full: bool = False):
    """Run bootstrap: fetch and save records."""
    limit = 15 if sample else None
    out_dir = SAMPLE_DIR if sample else SOURCE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    records = []
    text_lengths = []

    gen = fetch_all(limit=limit) if not sample else fetch_circulars_for_sample(limit=limit)

    for record in gen:
        records.append(record)
        text_lengths.append(len(record.get("text", "")))

        safe_id = record["_id"].replace("/", "_").replace(" ", "_")
        filepath = out_dir / f"{safe_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {record['_id']} ({len(record.get('text', '')):,} chars)")

    if records:
        avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0
        logger.info(f"\n{'='*60}")
        logger.info(f"Total records: {len(records)}")
        logger.info(f"Avg text length: {avg_text:,.0f} chars")
        logger.info(f"Min text length: {min(text_lengths):,} chars")
        logger.info(f"Max text length: {max(text_lengths):,} chars")
        logger.info(f"Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
        logger.info(f"Output directory: {out_dir}")
    else:
        logger.warning("No records fetched!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PK/FBR-TaxDoctrine data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, full=args.full)
