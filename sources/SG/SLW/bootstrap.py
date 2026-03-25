#!/usr/bin/env python3
"""
SG/SLW - Singapore Law Watch Judgments Fetcher

Fetches Singapore court judgments from Singapore Law Watch (SAL).
Covers Court of Appeal, High Court, IPOS, and PDPC decisions.

Data source: https://www.singaporelawwatch.sg/Judgments
Method: HTML listing scrape + PDF download + PyMuPDF text extraction
License: Singapore Government / Singapore Academy of Law
Rate limit: ~2 seconds between requests

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all ~12K judgments)
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
from urllib.parse import quote

import fitz  # PyMuPDF
import requests

SOURCE_ID = "SG/SLW"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://www.singaporelawwatch.sg"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Court listing pages with their pagination parameters
# Each court has: (url_path, pgr_id, max_pages, entries_per_page)
COURTS = {
    "Court of Appeal": {
        "url": "/Judgments/Court-of-Appeal",
        "pgr_id": 399,
        "per_page": 60,
    },
    "High Court": {
        "url": "/Judgments/High-Court",
        "pgr_id": 401,
        "per_page": 60,
    },
    "IPOS": {
        "url": "/Judgments",
        "pgr_id": 451,
        "per_page": 5,
    },
    "PDPC": {
        "url": "/Judgments",
        "pgr_id": 452,
        "per_page": 5,
    },
}


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using PyMuPDF."""
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text = ""
        for page in doc:
            text += page.get_text()
        doc.close()
        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()
    except Exception as e:
        print(f"  Error extracting PDF text: {e}")
        return ""


def parse_listing_page(html: str) -> list[dict]:
    """Parse a listing page and extract judgment entries."""
    entries = []

    # Pattern: each entry is an h4 with a PDF link, followed by metadata div
    # Match blocks: <h4>...<a href="PDF_URL">TITLE</a>...</h4>\n<div class="...judgmentsMetaDetails">CATEGORIES<br/>Decision Date : DATE</div>
    pattern = re.compile(
        r'<h4>\s*<a\s+href="([^"]*\.pdf)"[^>]*>\s*(.*?)\s*</a>.*?</h4>\s*'
        r'<div\s+class="[^"]*judgmentsMetaDetails[^"]*">\s*(.*?)\s*</div>',
        re.DOTALL
    )

    for match in pattern.finditer(html):
        pdf_url = match.group(1)
        title = match.group(2).strip()
        meta_block = match.group(3)

        # Clean title
        title = re.sub(r'\s+', ' ', title).strip()

        # Extract decision date
        date_match = re.search(r'Decision Date\s*:\s*(\d{1,2}\s+\w+\s+\d{4})', meta_block)
        decision_date = None
        if date_match:
            try:
                decision_date = datetime.strptime(date_match.group(1), "%d %b %Y").strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Extract categories (everything before <br/>Decision Date)
        categories_text = re.sub(r'<br\s*/?>.*', '', meta_block, flags=re.DOTALL).strip()
        categories_text = re.sub(r'<[^>]+>', '', categories_text).strip()
        categories = [c.strip() for c in categories_text.split(',') if c.strip()] if categories_text else []

        # Extract citation from title (e.g., [2026] SGHC 62)
        citation_match = re.search(r'\[(\d{4})\]\s+(SG\w+)\s+(\d+)', title)
        citation = citation_match.group(0) if citation_match else None
        court_code = citation_match.group(2) if citation_match else None

        # Make PDF URL absolute
        if pdf_url and not pdf_url.startswith("http"):
            pdf_url = BASE_URL + pdf_url

        entries.append({
            "title": title,
            "pdf_url": pdf_url,
            "date": decision_date,
            "categories": categories,
            "citation": citation,
            "court_code": court_code,
        })

    return entries


def fetch_listing_page(court_name: str, page: int = 1) -> Optional[str]:
    """Fetch a court listing page."""
    court = COURTS[court_name]
    if page == 1:
        url = f"{BASE_URL}{court['url']}"
    else:
        url = f"{BASE_URL}{court['url']}/PgrID/{court['pgr_id']}/PageID/{page}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"  Error fetching {court_name} page {page}: {e}")
        return None


def get_max_page(court_name: str) -> int:
    """Get the maximum page number for a court listing."""
    html = fetch_listing_page(court_name, 1)
    if not html:
        return 1
    pgr_id = COURTS[court_name]["pgr_id"]
    pages = re.findall(rf'PgrID/{pgr_id}/PageID/(\d+)', html)
    if not pages:
        return 1
    return max(int(p) for p in pages)


def download_pdf(pdf_url: str) -> Optional[bytes]:
    """Download a PDF file."""
    try:
        resp = requests.get(pdf_url, headers={
            "User-Agent": HEADERS["User-Agent"],
            "Accept": "application/pdf,*/*",
        }, timeout=120)
        resp.raise_for_status()
        if len(resp.content) < 100:
            print(f"  Warning: PDF too small ({len(resp.content)} bytes)")
            return None
        return resp.content
    except requests.RequestException as e:
        print(f"  Error downloading PDF: {e}")
        return None


def normalize(entry: dict, text: str) -> dict:
    """Normalize a judgment entry into the standard schema."""
    citation = entry.get("citation") or entry.get("title", "unknown")
    return {
        "_id": citation.replace(" ", "_").replace("[", "").replace("]", ""),
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": entry["title"],
        "citation": citation,
        "court": entry.get("court_code", ""),
        "date": entry.get("date"),
        "categories": entry.get("categories", []),
        "text": text,
        "url": entry.get("pdf_url", ""),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all judgments from all courts."""
    seen = set()

    if sample:
        # For sample mode, just get first page of Court of Appeal
        courts_to_fetch = [("Court of Appeal", 1)]
    else:
        # Discover page counts for each court
        courts_to_fetch = []
        for court_name in COURTS:
            max_page = get_max_page(court_name)
            print(f"  {court_name}: {max_page} pages")
            for page in range(1, max_page + 1):
                courts_to_fetch.append((court_name, page))
            time.sleep(1)

    total_fetched = 0
    for court_name, page in courts_to_fetch:
        print(f"  Fetching {court_name} page {page}...")
        html = fetch_listing_page(court_name, page)
        if not html:
            continue

        entries = parse_listing_page(html)
        if not entries:
            print(f"    No entries found on page {page}")
            continue

        for entry in entries:
            citation = entry.get("citation")
            if not citation or citation in seen:
                continue
            seen.add(citation)

            # Download and extract PDF text
            print(f"    Downloading: {citation}")
            pdf_bytes = download_pdf(entry["pdf_url"])
            if not pdf_bytes:
                print(f"    Skipping {citation}: PDF download failed")
                continue

            text = extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 200:
                print(f"    Skipping {citation}: insufficient text ({len(text)} chars)")
                continue

            record = normalize(entry, text)
            yield record
            total_fetched += 1

            if sample and total_fetched >= 15:
                return

            time.sleep(1.5)

        time.sleep(1)


def test_connection():
    """Test connectivity to Singapore Law Watch."""
    print("Testing Singapore Law Watch connectivity...")

    # Test listing page
    print("\n1. Testing listing page...")
    html = fetch_listing_page("Court of Appeal", 1)
    if html:
        entries = parse_listing_page(html)
        print(f"   OK: Found {len(entries)} entries on first page")
    else:
        print("   FAIL: Could not fetch listing page")
        return False

    # Test PDF download
    if entries:
        entry = entries[0]
        print(f"\n2. Testing PDF download: {entry['citation']}...")
        pdf_bytes = download_pdf(entry["pdf_url"])
        if pdf_bytes:
            print(f"   OK: Downloaded {len(pdf_bytes)} bytes")
            text = extract_text_from_pdf(pdf_bytes)
            print(f"   OK: Extracted {len(text)} chars of text")
            if len(text) > 200:
                print(f"   Text preview: {text[:200]}...")
            else:
                print("   WARN: Text seems short")
        else:
            print("   FAIL: Could not download PDF")
            return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="SG/SLW Singapore Law Watch Fetcher")
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
            # Save to sample directory
            filename = re.sub(r'[^\w\-]', '_', record["_id"]) + ".json"
            filepath = SAMPLE_DIR / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            print(f"  Saved: {filepath.name} ({len(record['text'])} chars)")

        print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
