#!/usr/bin/env python3
"""
GU/SupremeCourt -- Supreme Court of Guam Opinions Data Fetcher

Fetches published opinions from guamcourts.gov via year-filtered POST form.
Each opinion is a PDF with citation format YYYYGuamNN.

Covers 1990-present (~600+ opinions).

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap --full     # Full bootstrap (all years)
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Generator

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

# Setup
SOURCE_ID = "GU/SupremeCourt"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GU.SupremeCourt")

BASE_URL = "https://guamcourts.gov"
OPINIONS_URL = f"{BASE_URL}/Supreme-Court-Opinions/Supreme-Court-Opinions.asp"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    text_parts = []
    try:
        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        logger.warning(f"PDF extraction error: {e}")
    return "\n\n".join(text_parts)


def get_available_years() -> list[int]:
    """Get all available years from the dropdown."""
    resp = session.get(OPINIONS_URL, timeout=30)
    resp.raise_for_status()
    years = [int(y) for y in re.findall(r'<option value="(\d+)">', resp.text)]
    years.sort(reverse=True)
    return years


def fetch_opinions_for_year(year: int) -> list[dict]:
    """Fetch opinion list for a given year via POST form."""
    resp = session.post(OPINIONS_URL, data={"Year": str(year)}, timeout=30)
    resp.raise_for_status()
    html = resp.text

    opinions = []
    seen = set()

    # Extract PDF links and case names
    # Pattern: <a href="/Supreme-Court-Opinions/images/YYYYGuamNN.pdf" ...>Case Name</a>
    for m in re.finditer(
        r'<a\s+href="(/Supreme-Court-Opinions/images/(\d{4})Guam(\d+)\.pdf)"[^>]*>([^<]+)',
        html, re.IGNORECASE
    ):
        path, op_year, op_num, case_name = m.group(1), m.group(2), m.group(3), m.group(4).strip()

        # Only include opinions for the requested year
        if int(op_year) != year:
            continue

        citation = f"{op_year} Guam {int(op_num)}"
        if citation in seen:
            continue
        seen.add(citation)

        pdf_url = BASE_URL + path
        opinions.append({
            "year": int(op_year),
            "number": int(op_num),
            "citation": citation,
            "case_name": case_name,
            "pdf_url": pdf_url,
        })

    # Try to extract posted dates from the "recent" section
    # Pattern: Posted: M/D/YYYY followed by a PDF link
    date_pattern = re.finditer(
        r'Posted:\s*(\d{1,2}/\d{1,2}/\d{4})\s*.*?'
        r'href="(/Supreme-Court-Opinions/images/(\d{4})Guam(\d+)\.pdf)"',
        html, re.DOTALL
    )
    date_map = {}
    for dm in date_pattern:
        date_str, _, dy, dn = dm.group(1), dm.group(2), dm.group(3), dm.group(4)
        cit = f"{dy} Guam {int(dn)}"
        try:
            dt = datetime.strptime(date_str, "%m/%d/%Y")
            date_map[cit] = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Attach dates where available
    for op in opinions:
        op["date"] = date_map.get(op["citation"])

    opinions.sort(key=lambda x: x["number"])
    logger.info(f"Year {year}: found {len(opinions)} opinions")
    return opinions


def normalize(raw: dict) -> dict:
    """Normalize a raw opinion record into standard schema."""
    return {
        "_id": f"GU-SC-{raw['year']}-{raw['number']:02d}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("case_name", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("pdf_url", ""),
        "citation": raw.get("citation", ""),
        "year": raw.get("year"),
        "jurisdiction": "GU",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records for all opinions."""
    years = get_available_years()
    count = 0
    sample_limit = 15 if sample else None

    # For sample mode, just do recent years
    if sample:
        years = years[:3]

    for year in years:
        if sample_limit and count >= sample_limit:
            break

        opinions = fetch_opinions_for_year(year)
        time.sleep(1)

        for op in opinions:
            if sample_limit and count >= sample_limit:
                break

            logger.info(f"Downloading {op['citation']}: {op['case_name'][:60]}...")
            try:
                resp = session.get(op["pdf_url"], timeout=60)
                resp.raise_for_status()
                text = extract_pdf_text(resp.content)
                if not text.strip():
                    logger.warning(f"No text from {op['pdf_url']}")
                    continue
                op["text"] = text
                yield normalize(op)
                count += 1
            except Exception as e:
                logger.error(f"Failed {op['citation']}: {e}")

            time.sleep(1)

    logger.info(f"Total records yielded: {count}")


def save_records(records: list[dict], output_dir: Path) -> int:
    """Save records as individual JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    for rec in records:
        fname = f"{rec['_id']}.json"
        fpath = output_dir / fname
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1
    return saved


def cmd_test_api():
    """Quick connectivity test."""
    print(f"Testing connection to {OPINIONS_URL}...")
    resp = session.get(OPINIONS_URL, timeout=15)
    print(f"Status: {resp.status_code}")

    years = get_available_years()
    print(f"Available years: {years[0]}-{years[-1]} ({len(years)} years)")

    # Test one year
    opinions = fetch_opinions_for_year(years[0])
    print(f"Year {years[0]}: {len(opinions)} opinions")

    if opinions:
        op = opinions[0]
        print(f"Testing PDF: {op['pdf_url']}")
        resp = session.get(op["pdf_url"], timeout=30)
        print(f"PDF status: {resp.status_code}, size: {len(resp.content)} bytes")
        text = extract_pdf_text(resp.content)
        print(f"Text length: {len(text)} chars")
        if text:
            print(f"First 200 chars: {text[:200]}")

    print("\nConnectivity test PASSED")


def cmd_bootstrap(sample: bool = False, full: bool = False):
    """Bootstrap the data source."""
    mode = "sample" if sample else "full"
    logger.info(f"Starting bootstrap in {mode} mode")

    records = list(fetch_all(sample=sample))
    logger.info(f"Fetched {len(records)} records")

    if not records:
        logger.error("No records fetched!")
        sys.exit(1)

    output_dir = SAMPLE_DIR if sample else SOURCE_DIR / "data"
    saved = save_records(records, output_dir)
    logger.info(f"Saved {saved} records to {output_dir}")

    texts_ok = sum(1 for r in records if r.get("text", "").strip())
    print(f"\n{'='*60}")
    print(f"Bootstrap complete ({mode} mode)")
    print(f"Records: {len(records)}")
    print(f"With full text: {texts_ok}/{len(records)}")
    print(f"Output: {output_dir}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="GU/SupremeCourt bootstrapper")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Bootstrap data")
    boot.add_argument("--sample", action="store_true", help="Sample mode (~15 records)")
    boot.add_argument("--full", action="store_true", help="Full bootstrap")

    sub.add_parser("test-api", help="Test API connectivity")

    args = parser.parse_args()

    if args.command == "test-api":
        cmd_test_api()
    elif args.command == "bootstrap":
        cmd_bootstrap(sample=args.sample, full=args.full)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
