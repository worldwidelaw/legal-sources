#!/usr/bin/env python3
"""
GU/SuperiorCourt -- Guam Superior Court Decisions & Orders Data Fetcher

Fetches full text of Superior Court decisions from guamcourts.gov.
The site uses a POST form with year parameter to list decisions per year
(1998-present). Each entry links to a PDF decision document.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap --full     # Full bootstrap (all years)
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import html as html_mod
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
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

# Setup
SOURCE_ID = "GU/SuperiorCourt"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GU.SuperiorCourt")

BASE_URL = "https://guamcourts.gov"
DECISIONS_URL = f"{BASE_URL}/Superior-Court-Decision-and-Orders/Superior-Court-Decision-and-Orders.asp"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)

# Years available (1998-2026)
YEARS = list(range(2026, 1997, -1))


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


def parse_decision_date(date_str: str) -> Optional[str]:
    """Parse a date like '04-03-2026' or '1-9-2026' to ISO 8601."""
    if not date_str:
        return None
    for fmt in ("%m-%d-%Y", "%m-%d-%y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def fetch_decisions_for_year(year: int) -> list[dict]:
    """Fetch all decision entries for a given year via POST form."""
    logger.info(f"Fetching decisions for year {year}")
    resp = session.post(
        DECISIONS_URL,
        data={"Year": str(year), "Submit": "Submit"},
        timeout=30,
    )
    resp.raise_for_status()
    page_html = resp.text

    # Parse entries: <span class="CaseNumber">CASE</span>, <a href="PDF">Name</a><br> description, date
    entry_pattern = re.compile(
        r'<span class="CaseNumber">([^<]+)</span>[^<]*'
        r'<a href="([^"]+\.pdf)"[^>]*>([^<]+)</a>'
        r'(?:<br>\s*(?:&nbsp;)*([^<]*?)(?:</td>|<br>))?',
        re.DOTALL | re.IGNORECASE,
    )

    entries = []
    for m in entry_pattern.finditer(page_html):
        case_num = m.group(1).strip()
        pdf_path = m.group(2).strip()
        case_name = html_mod.unescape(m.group(3).strip())
        desc_raw = m.group(4).strip() if m.group(4) else ""
        desc = re.sub(r"&nbsp;", " ", desc_raw).strip()

        # Make URL absolute
        if not pdf_path.startswith("http"):
            pdf_url = f"{BASE_URL}{pdf_path}"
        else:
            pdf_url = pdf_path

        # Extract date from description (MM-DD-YYYY at end)
        date_match = re.search(r"(\d{1,2}-\d{1,2}-\d{4})", desc)
        decision_date = date_match.group(1) if date_match else None

        # Extract decision type from description (text before date)
        decision_type = desc
        if date_match:
            decision_type = desc[: date_match.start()].strip().rstrip(",")

        # Build unique ID from filename (includes case + date)
        filename = pdf_path.split("/")[-1].replace(".pdf", "")

        entries.append({
            "case_number": case_num,
            "case_name": case_name,
            "decision_type": decision_type,
            "decision_date": decision_date,
            "year": year,
            "pdf_url": pdf_url,
            "filename": filename,
        })

    logger.info(f"Year {year}: found {len(entries)} decisions")
    return entries


def normalize(raw: dict) -> dict:
    """Normalize a raw decision record into standard schema."""
    case_num = raw.get("case_number", "unknown")
    filename = raw.get("filename", case_num)
    doc_id = f"GU-SC-{filename}"

    case_name = raw.get("case_name", "")
    decision_type = raw.get("decision_type", "")
    title = case_name
    if decision_type:
        title = f"{case_name} - {decision_type}"

    iso_date = parse_decision_date(raw.get("decision_date"))

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": iso_date,
        "url": raw.get("pdf_url", ""),
        "case_number": case_num,
        "case_name": case_name,
        "decision_type": decision_type,
        "jurisdiction": "GU",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records for all decisions."""
    count = 0
    sample_limit = 15 if sample else None
    errors = 0
    max_errors = 20

    # In sample mode, only fetch 2025
    years = [2025] if sample else YEARS

    for year in years:
        if sample_limit and count >= sample_limit:
            break

        try:
            entries = fetch_decisions_for_year(year)
        except Exception as e:
            logger.error(f"Failed to fetch year {year}: {e}")
            errors += 1
            continue

        time.sleep(1)

        for entry in entries:
            if sample_limit and count >= sample_limit:
                break
            if errors >= max_errors:
                logger.error("Too many errors, stopping")
                break

            pdf_url = entry["pdf_url"]
            logger.info(f"Downloading {entry['case_number']} ({entry['case_name'][:40]})")

            try:
                resp = None
                for attempt in range(3):
                    try:
                        resp = session.get(pdf_url, timeout=60)
                        resp.raise_for_status()
                        break
                    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as retry_e:
                        if attempt < 2:
                            logger.warning(f"Retry {attempt+1}: {retry_e}")
                            time.sleep(2)
                        else:
                            raise

                text = extract_pdf_text(resp.content)

                if not text.strip():
                    logger.warning(f"No text from {pdf_url} (may be scanned)")
                    errors += 1
                    continue

                entry["text"] = text
                yield normalize(entry)
                count += 1

            except requests.exceptions.HTTPError as e:
                logger.error(f"HTTP error for {pdf_url}: {e}")
                errors += 1
            except Exception as e:
                logger.error(f"Failed to fetch {pdf_url}: {e}")
                errors += 1

            time.sleep(1)

    logger.info(f"Total records yielded: {count} (errors: {errors})")


def save_records(records: list[dict], output_dir: Path) -> int:
    """Save records as individual JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    for rec in records:
        doc_id = rec["_id"].replace("/", "_")
        fname = f"record_{doc_id}.json"
        fpath = output_dir / fname
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1
    return saved


def cmd_test_api():
    """Quick connectivity test."""
    print(f"Testing connection to {DECISIONS_URL}...")
    resp = session.get(DECISIONS_URL, timeout=15)
    print(f"Status: {resp.status_code}")

    entries = fetch_decisions_for_year(2025)
    print(f"Found {len(entries)} decisions for 2025")

    if entries:
        entry = entries[0]
        print(f"\nFirst entry:")
        print(f"  Case: {entry['case_number']}")
        print(f"  Name: {entry['case_name']}")
        print(f"  Type: {entry['decision_type']}")
        print(f"  Date: {entry['decision_date']}")
        print(f"  URL: {entry['pdf_url']}")

        print(f"\nTesting PDF download...")
        resp = session.get(entry["pdf_url"], timeout=30)
        print(f"PDF status: {resp.status_code}, size: {len(resp.content)} bytes")
        text = extract_pdf_text(resp.content)
        print(f"Extracted text: {len(text)} chars")
        if text:
            print(f"First 300 chars: {text[:300]}")

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

    # Validate
    texts_ok = sum(1 for r in records if len(r.get("text", "").strip()) > 100)
    print(f"\n{'='*60}")
    print(f"Bootstrap complete ({mode} mode)")
    print(f"Records: {len(records)}")
    print(f"With substantial text: {texts_ok}/{len(records)}")
    print(f"Output: {output_dir}")
    print(f"{'='*60}")


def main():
    parser = argparse.ArgumentParser(description="GU/SuperiorCourt bootstrapper")
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
