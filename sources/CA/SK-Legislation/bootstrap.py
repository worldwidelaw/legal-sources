#!/usr/bin/env python3
"""
CA/SK-Legislation -- Saskatchewan Publications Centre Fetcher

Fetches Saskatchewan consolidated statutes and regulations from the official
Publications Saskatchewan REST API. Full text extracted from free PDF downloads.

Data source: https://publications.saskatchewan.ca/
License: Crown Copyright (Saskatchewan King's Printer)

Strategy:
  - List statutes via API (productSubTypeId=2)
  - List regulations via API (productSubTypeId=15)
  - For each product, fetch detail to get digital format ID
  - Download PDF and extract text with PyMuPDF
"""

import json
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


API_BASE = "https://publications.saskatchewan.ca/api/v1"
STATUTES_SUBTYPE = 2
REGS_SUBTYPE = 15
RATE_LIMIT_DELAY = 1.5
CURL_TIMEOUT = 120

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


def fetch_json(url: str, retries: int = 2):
    """Fetch JSON from the API using curl."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-H", "Accept: application/json",
                 "-w", "\n%{http_code}", url],
                capture_output=True, text=True, timeout=CURL_TIMEOUT + 10
            )
            parts = result.stdout.rsplit("\n", 1)
            if len(parts) == 2:
                body, status = parts[0], parts[1].strip()
            else:
                body, status = result.stdout, "000"

            if not status.startswith("2"):
                if attempt == retries:
                    print(f"HTTP {status} for {url}", file=sys.stderr)
                    return None
                time.sleep(3)
                continue
            if body:
                return json.loads(body)
            if attempt == retries:
                return None
            time.sleep(3)
        except Exception as e:
            if attempt == retries:
                print(f"Failed to fetch {url}: {e}", file=sys.stderr)
                return None
            time.sleep(3)
    return None


def download_pdf(url: str) -> Optional[bytes]:
    """Download a PDF file, following redirects."""
    try:
        result = subprocess.run(
            ["curl", "-sL", "-m", str(CURL_TIMEOUT), url],
            capture_output=True, timeout=CURL_TIMEOUT + 10
        )
        if result.returncode == 0 and result.stdout and result.stdout[:5] == b"%PDF-":
            return result.stdout
        return None
    except Exception as e:
        print(f"Failed to download PDF {url}: {e}", file=sys.stderr)
        return None


def extract_text_from_pdf(pdf_bytes: bytes, source_id: str = "") -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="CA/SK-Legislation",
        source_id=source_id,
        pdf_bytes=pdf_bytes,
        table="legislation",
        force=True,
    ) or ""

def get_product_detail(product_id: int) -> Optional[dict]:
    """Get product detail including digital format and long description."""
    detail = fetch_json(f"{API_BASE}/products/{product_id}")
    if not detail:
        return None
    formats = detail.get("productFormats", [])
    digital_fmt = None
    for fmt in formats:
        if (fmt.get("productFormatMediumType") == "DIGITAL"
                and fmt.get("price", 999) == 0.0):
            digital_fmt = {
                "format_id": fmt["productFormatId"],
                "file_name": fmt.get("digitalAttributes", {}).get("fileName", ""),
                "file_size": fmt.get("digitalAttributes", {}).get("fileSize", 0),
                "page_count": fmt.get("pageCount", 0),
            }
            break
    if not digital_fmt:
        return None
    return {
        "format": digital_fmt,
        "long_description": detail.get("longDescriptionEnglish", "") or "",
        "active_timestamp": detail.get("activeTimestamp", "") or "",
    }


def extract_date_from_description(desc: str) -> str:
    """Extract last update date from product description HTML."""
    if not desc:
        return ""
    # Pattern: "Last update posted: 7 Aug 2024" or similar
    m = re.search(r'Last\s+update\s+posted[:\s]*(\d{1,2}\s+\w+\s+\d{4})', desc, re.IGNORECASE)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%d %b %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    # Try ISO-ish date
    m = re.search(r'(\d{4}-\d{2}-\d{2})', desc)
    if m:
        return m.group(1)
    return ""


def normalize(product: dict, text: str, doc_type: str, detail: dict = None) -> dict:
    """Normalize a product record."""
    product_id = product["productId"]
    custom_id = product.get("customIdentifier", str(product_id))
    long_desc = (detail or {}).get("long_description", "")
    desc = long_desc or product.get("shortDescriptionEnglish", "") or ""
    date = extract_date_from_description(desc)
    if not date and detail:
        ts = detail.get("active_timestamp", "")
        if ts:
            date = ts[:10]

    return {
        "_id": f"CA/SK-Legislation/{doc_type}/{custom_id}",
        "_source": "CA/SK-Legislation",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": product.get("name", ""),
        "text": text,
        "date": date,
        "url": f"https://publications.saskatchewan.ca/#/products/{product_id}",
        "doc_type": doc_type,
        "custom_identifier": custom_id,
        "jurisdiction": "CA-SK",
    }


def fetch_products(subtype_id: int, doc_type: str, sample: bool = False,
                   sample_limit: int = 10) -> Generator[dict, None, None]:
    """Fetch and yield normalized records for a product subtype."""
    products = fetch_json(f"{API_BASE}/products?productSubTypeId={subtype_id}")
    if not products:
        print(f"ERROR: Could not fetch product list for subtype {subtype_id}", file=sys.stderr)
        return

    # Only process active products
    active = [p for p in products if p.get("productStatusType") == "ACTIVE"]
    print(f"Found {len(active)} active {doc_type}s (of {len(products)} total)", file=sys.stderr)

    count = 0
    for product in active:
        pid = product["productId"]

        # Get product detail (format + description)
        detail = get_product_detail(pid)
        if not detail:
            print(f"  No free digital format for product {pid}: {product.get('name', '?')}", file=sys.stderr)
            continue
        fmt = detail["format"]

        # Download PDF
        pdf_url = f"{API_BASE}/products/{pid}/formats/{fmt['format_id']}/download"
        pdf_bytes = download_pdf(pdf_url)
        if not pdf_bytes:
            print(f"  Failed to download PDF for {pid}", file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)
            continue

        # Extract text
        custom_id = product.get("customIdentifier", str(pid))
        source_id = f"CA/SK-Legislation/{doc_type}/{custom_id}"
        text = extract_text_from_pdf(pdf_bytes, source_id=source_id)
        if not text or len(text) < 100:
            print(f"  Insufficient text for {pid}: {len(text)} chars", file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)
            continue

        record = normalize(product, text, doc_type, detail)
        yield record
        count += 1
        if count % 25 == 0:
            print(f"  Fetched {count} {doc_type}s...", file=sys.stderr)
        if sample and count >= sample_limit:
            break

        time.sleep(RATE_LIMIT_DELAY)

    print(f"Total {doc_type}s fetched: {count}", file=sys.stderr)


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all Saskatchewan statutes and regulations."""
    total = 0

    # Fetch statutes
    print("Fetching statutes...", file=sys.stderr)
    for record in fetch_products(STATUTES_SUBTYPE, "statute", sample=sample, sample_limit=10):
        yield record
        total += 1

    # Fetch regulations
    print("Fetching regulations...", file=sys.stderr)
    reg_limit = 5 if sample else 99999
    for record in fetch_products(REGS_SUBTYPE, "regulation", sample=sample, sample_limit=reg_limit):
        yield record
        total += 1

    print(f"Grand total: {total} records", file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Saskatchewan legislation fetcher")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch documents")
    boot.add_argument("--sample", action="store_true", help="Fetch ~15 sample records")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            out_path = SAMPLE_DIR / f"{count:04d}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
        print(f"Done. Saved {count} records to {SAMPLE_DIR}/", file=sys.stderr)


if __name__ == "__main__":
    main()
