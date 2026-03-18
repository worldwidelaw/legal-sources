#!/usr/bin/env python3
"""
UK/FCA - Financial Conduct Authority Fetcher

Fetches decision notices and final notices from the FCA website.
These are enforcement actions, bans, fines, and regulatory decisions
against firms and individuals in the UK financial services sector.

Data source: https://www.fca.org.uk
Method: Sitemap parsing + PDF text extraction
License: Open Government Licence v3.0
Rate limit: 1 request per second

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test API connectivity
"""

import argparse
import io
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List, Optional, Tuple
from urllib.parse import unquote

import pdfplumber
import requests

SITEMAP_URL = "https://www.fca.org.uk/sitemap.xml?page=2"
BASE_URL = "https://www.fca.org.uk"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UK/FCA"

HEADERS = {
    "User-Agent": "WorldWideLaw/1.0 (Open Data Research; github.com/worldwidelaw/legal-sources)",
    "Accept": "*/*",
}


def fetch_sitemap() -> List[Tuple[str, str, str]]:
    """
    Fetch and parse the FCA sitemap to extract notice URLs.

    Returns:
        List of tuples: (url, lastmod, notice_type)
    """
    resp = requests.get(SITEMAP_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    root = ET.fromstring(resp.content)
    ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    notices = []

    for url_elem in root.findall(".//ns:url", ns):
        loc = url_elem.find("ns:loc", ns)
        lastmod = url_elem.find("ns:lastmod", ns)

        if loc is None:
            continue

        url = loc.text
        mod_date = lastmod.text if lastmod is not None else None

        # Filter for decision notices and final notices PDFs
        if "/publication/decision-notices/" in url and url.endswith(".pdf"):
            notices.append((url, mod_date, "decision_notice"))
        elif "/publication/final-notices/" in url and url.endswith(".pdf"):
            notices.append((url, mod_date, "final_notice"))

    return notices


def extract_text_from_pdf(url: str) -> Optional[str]:
    """
    Download a PDF and extract its text content.

    Args:
        url: URL of the PDF file

    Returns:
        Extracted text or None on error
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120)
        resp.raise_for_status()

        pdf_bytes = io.BytesIO(resp.content)

        with pdfplumber.open(pdf_bytes) as pdf:
            text_parts = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)

            return "\n\n".join(text_parts)

    except requests.RequestException as e:
        print(f"  Error downloading PDF: {e}")
        return None
    except Exception as e:
        print(f"  Error extracting PDF text: {e}")
        return None


def parse_notice_metadata(url: str, text: str, notice_type: str, lastmod: str) -> dict:
    """
    Parse metadata from notice URL and text content.

    Args:
        url: Notice PDF URL
        text: Extracted text content
        notice_type: "decision_notice" or "final_notice"
        lastmod: Last modification date from sitemap

    Returns:
        Metadata dict
    """
    # Extract filename to derive subject name
    filename = url.split("/")[-1].replace(".pdf", "")
    filename = unquote(filename)

    # Try to extract subject name from filename (format: name-year.pdf)
    name_match = re.match(r"(.+?)-(\d{4})$", filename)
    if name_match:
        subject_name = name_match.group(1).replace("-", " ").title()
        year = name_match.group(2)
    else:
        # Handle other formats
        subject_name = filename.replace("-", " ").title()
        year = None

    # Try to extract date from text
    date = None

    # Look for "Date: DD Month YYYY" pattern
    date_pattern = r"Date:\s*(\d{1,2}\s+\w+\s+\d{4})"
    date_match = re.search(date_pattern, text[:2000])
    if date_match:
        try:
            date_str = date_match.group(1)
            date = datetime.strptime(date_str, "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Fallback to lastmod from sitemap
    if not date and lastmod:
        date = lastmod[:10] if "T" in lastmod else lastmod

    # Try to extract reference number
    ref_pattern = r"(?:Reference\s*Number|IRN|FRN):\s*([A-Z0-9]+)"
    ref_match = re.search(ref_pattern, text[:3000], re.IGNORECASE)
    reference_number = ref_match.group(1) if ref_match else None

    # Extract title from text (first meaningful line)
    title = None
    lines = text.split("\n")
    for line in lines[:20]:
        line = line.strip()
        if line and len(line) > 10 and not line.startswith("Page "):
            if "NOTICE" in line.upper() or "To:" in line:
                continue
            title = line[:200]
            break

    if not title:
        # Construct title from notice type and subject
        type_display = "Final Notice" if notice_type == "final_notice" else "Decision Notice"
        title = f"{type_display}: {subject_name}"

    return {
        "subject_name": subject_name,
        "reference_number": reference_number,
        "date": date,
        "year": year,
        "title": title,
    }


def parse_notice(url: str, text: str, notice_type: str, lastmod: str) -> dict:
    """
    Parse a notice into a normalized record.

    Args:
        url: Notice PDF URL
        text: Extracted text content
        notice_type: "decision_notice" or "final_notice"
        lastmod: Last modification date from sitemap

    Returns:
        Normalized record dict
    """
    metadata = parse_notice_metadata(url, text, notice_type, lastmod)

    # Generate ID from URL path
    doc_id = url.replace(BASE_URL + "/publication/", "").replace("/", "_").replace(".pdf", "")

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "regulatory_decisions",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": metadata["title"],
        "text": text,
        "date": metadata["date"],
        "url": url,
        "notice_type": notice_type,
        "subject_name": metadata["subject_name"],
        "reference_number": metadata["reference_number"],
        "language": "en",
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """
    Fetch all FCA notices.

    Args:
        max_records: Maximum total records to yield

    Yields:
        Normalized notice records
    """
    print("Fetching FCA sitemap...")
    notices = fetch_sitemap()
    print(f"Found {len(notices)} notices in sitemap")

    # Sort by lastmod (newest first)
    notices.sort(key=lambda x: x[1] or "", reverse=True)

    total_yielded = 0

    for url, lastmod, notice_type in notices:
        if max_records and total_yielded >= max_records:
            return

        print(f"\n[{total_yielded + 1}] Fetching: {url.split('/')[-1]}")

        text = extract_text_from_pdf(url)
        if not text or len(text) < 100:
            print(f"  Skipping: insufficient text ({len(text) if text else 0} chars)")
            continue

        record = parse_notice(url, text, notice_type, lastmod)

        yield record
        total_yielded += 1

        if total_yielded % 10 == 0:
            print(f"  Progress: {total_yielded} records fetched...")

        time.sleep(1.0)  # Rate limiting

    print(f"\nCompleted: {total_yielded} total records")


def normalize(raw: dict) -> dict:
    """Validate and normalize a record."""
    required = ["_id", "_source", "_type", "_fetched_at", "title", "text", "url"]
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    if not raw.get("text") or len(raw["text"]) < 100:
        raise ValueError(f"Document has insufficient text content ({len(raw.get('text', ''))} chars)")

    return raw


def bootstrap_sample(sample_count: int = 15):
    """
    Fetch sample records showing variety of FCA notices.
    """
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from UK/FCA...")
    print("=" * 60)

    print("\nFetching FCA sitemap...")
    notices = fetch_sitemap()
    print(f"Found {len(notices)} notices in sitemap")

    # Get a mix of decision notices and final notices
    decision_notices = [n for n in notices if n[2] == "decision_notice"]
    final_notices = [n for n in notices if n[2] == "final_notice"]

    # Sort each by lastmod (newest first)
    decision_notices.sort(key=lambda x: x[1] or "", reverse=True)
    final_notices.sort(key=lambda x: x[1] or "", reverse=True)

    # Interleave to get a mix
    selected = []
    di, fi = 0, 0
    while len(selected) < sample_count * 2:  # Fetch extra in case some fail
        if di < len(decision_notices):
            selected.append(decision_notices[di])
            di += 1
        if fi < len(final_notices):
            selected.append(final_notices[fi])
            fi += 1
        if di >= len(decision_notices) and fi >= len(final_notices):
            break

    records = []
    notice_types_seen = {"decision_notice": 0, "final_notice": 0}

    for url, lastmod, notice_type in selected:
        if len(records) >= sample_count:
            break

        filename = url.split("/")[-1]
        print(f"\n[{len(records) + 1}] Fetching: {filename}")

        text = extract_text_from_pdf(url)
        if not text or len(text) < 100:
            print(f"  Skipping: insufficient text ({len(text) if text else 0} chars)")
            continue

        record = parse_notice(url, text, notice_type, lastmod)

        try:
            normalized = normalize(record)
            records.append(normalized)
            notice_types_seen[notice_type] += 1

            # Save individual record
            idx = len(records)
            filename_safe = re.sub(r'[^\w\-]', '_', normalized["_id"])[:50]
            json_filename = SAMPLE_DIR / f"record_{idx:03d}_{filename_safe}.json"
            with open(json_filename, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            text_len = len(normalized.get("text", ""))
            nt = notice_type[:15]
            title = normalized["title"][:40]
            print(f"  [{idx:02d}] {nt}: {title}... ({text_len:,} chars)")

        except ValueError as e:
            print(f"  Skipping: {e}")

        time.sleep(1.0)  # Rate limiting

    print("\n" + "=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")
        print(f"Notice types: {notice_types_seen}")

        # Show date range
        dates = [r.get("date") for r in records if r.get("date")]
        if dates:
            dates.sort()
            print(f"Date range: {dates[0]} to {dates[-1]}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get("text"))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print(f"\nVALIDATION PASSED: {len(records)} records with full text.")
    return True


def test_api():
    """Test API connectivity and PDF extraction."""
    print("Testing UK FCA data source...")

    # Test sitemap fetch
    print("\n1. Testing sitemap fetch...")
    try:
        notices = fetch_sitemap()
        decision_count = sum(1 for n in notices if n[2] == "decision_notice")
        final_count = sum(1 for n in notices if n[2] == "final_notice")
        print(f"   OK: Found {len(notices)} notices ({decision_count} decision, {final_count} final)")
    except Exception as e:
        print(f"   FAILED: {e}")
        return False

    # Test PDF download and extraction
    print("\n2. Testing PDF extraction...")
    if notices:
        test_url = notices[0][0]
        test_filename = test_url.split("/")[-1]
        print(f"   Testing: {test_filename}")

        text = extract_text_from_pdf(test_url)
        if text and len(text) > 100:
            print(f"   OK: Extracted {len(text):,} characters")

            # Test parsing
            record = parse_notice(test_url, text, notices[0][2], notices[0][1])
            if record:
                print(f"   OK: Parsed record")
                print(f"       Title: {record['title'][:60]}...")
                print(f"       Subject: {record.get('subject_name', 'N/A')}")
                print(f"       Date: {record.get('date', 'N/A')}")
            else:
                print("   FAILED: Could not parse record")
                return False
        else:
            print(f"   FAILED: Insufficient text extracted ({len(text) if text else 0} chars)")
            return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="UK/FCA fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")

    args = parser.parse_args()

    if args.command == "test":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)
        else:
            print("Full bootstrap not implemented yet. Use --sample flag.")
            sys.exit(1)

    elif args.command == "update":
        print("Update command not implemented yet.")
        sys.exit(1)


if __name__ == "__main__":
    main()
