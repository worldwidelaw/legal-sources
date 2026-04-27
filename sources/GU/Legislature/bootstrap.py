#!/usr/bin/env python3
"""
GU/Legislature -- Guam Legislature Public Laws Data Fetcher

Fetches full text of Guam public laws from guamlegislature.gov (38th) and
archives.guamlegislature.gov (34th-37th) via WordPress REST API.

Each legislature's public laws page is a WordPress page containing HTML with
PDF links, law titles, bill numbers, sponsors, and signing dates.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap --full     # Full bootstrap (all legislatures)
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import html
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

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
SOURCE_ID = "GU/Legislature"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GU.Legislature")

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

session = requests.Session()
session.headers.update(HEADERS)

# WordPress page IDs for each legislature's public laws
LEGISLATURE_PAGES = [
    {
        "legislature": 38,
        "wp_api": "https://guamlegislature.gov/wp-json/wp/v2/pages/94",
        "base_url": "https://guamlegislature.gov/",
    },
    {
        "legislature": 37,
        "wp_api": "https://archives.guamlegislature.gov/wp-json/wp/v2/pages/825",
        "base_url": "https://archives.guamlegislature.gov/",
    },
    {
        "legislature": 36,
        "wp_api": "https://archives.guamlegislature.gov/wp-json/wp/v2/pages/1619",
        "base_url": "https://archives.guamlegislature.gov/",
    },
    {
        "legislature": 35,
        "wp_api": "https://archives.guamlegislature.gov/wp-json/wp/v2/pages/1700",
        "base_url": "https://archives.guamlegislature.gov/",
    },
    {
        "legislature": 34,
        "wp_api": "https://archives.guamlegislature.gov/wp-json/wp/v2/pages/1747",
        "base_url": "https://archives.guamlegislature.gov/",
    },
]


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


def parse_date(date_str: str) -> Optional[str]:
    """Parse a date like '4/12/25' or '1/5/2024' to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            dt = datetime.strptime(date_str, fmt)
            # Fix 2-digit years: 00-30 -> 2000s, 31-99 -> 1900s
            if dt.year > 2050:
                dt = dt.replace(year=dt.year - 100)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_legislature_page(wp_api_url: str, base_url: str, legislature_num: int) -> list[dict]:
    """Parse a WordPress page to extract public law entries."""
    logger.info(f"Fetching {legislature_num}th legislature page from {wp_api_url}")
    resp = session.get(wp_api_url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    content = data.get("content", {}).get("rendered", "")

    if not content:
        logger.warning(f"Empty content for {legislature_num}th legislature")
        return []

    entries = []
    # Split content by PDF links to public laws
    # Pattern: <a href="...pdf">Public Law XX-YY</a> or <a href="...pdf"><strong>Public Law XX-YY</strong></a>
    pdf_pattern = re.compile(
        r'<a\s+href="([^"]*\.pdf)"[^>]*>\s*(?:<strong>)?\s*((?:Public Law|P\.L\.)[^<]*?)(?:</strong>)?\s*</a>',
        re.IGNORECASE,
    )

    matches = list(pdf_pattern.finditer(content))
    logger.info(f"Found {len(matches)} PDF links for {legislature_num}th legislature")

    for i, m in enumerate(matches):
        pdf_url = m.group(1).strip()
        law_label = re.sub(r"\s+", " ", m.group(2)).strip()

        # Make URL absolute
        if not pdf_url.startswith("http"):
            pdf_url = urljoin(base_url, pdf_url)

        # Extract law number (e.g., "38-1" from "Public Law 38-1")
        law_num_match = re.search(r"(\d+-\d+)", law_label)
        law_number = law_num_match.group(1) if law_num_match else law_label

        # Get the text block after this match until the next PDF link
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(content)
        block = content[start:end]

        # Strip HTML tags for text parsing
        block_text = re.sub(r"<[^>]+>", " ", block)
        block_text = re.sub(r"\s+", " ", block_text).strip()

        # Extract description (the "AN ACT TO..." part)
        desc = ""
        desc_match = re.search(
            r"(AN\s+ACT\s+(?:TO\s+)?.*?)(?:\d+-\d+\s*\(COR\)|$)",
            block_text,
            re.IGNORECASE | re.DOTALL,
        )
        if desc_match:
            desc = desc_match.group(1).strip()
            # Clean up trailing whitespace/punctuation
            desc = re.sub(r"\s+", " ", desc).strip()

        # Extract bill number
        bill_match = re.search(r"(\d+-\d+)\s*\(COR\)", block_text)
        bill_number = bill_match.group(1) if bill_match else None

        # Extract signed date
        date_match = re.search(
            r"(?:SIGNED|LAPSED|OVERRIDDEN)[:\s]*(\d{1,2}/\d{1,2}/\d{2,4})",
            block_text,
            re.IGNORECASE,
        )
        signed_date = date_match.group(1) if date_match else None

        # Extract status
        status = "signed"
        if re.search(r"VETOED", block_text, re.IGNORECASE):
            status = "vetoed"
        elif re.search(r"LAPSED", block_text, re.IGNORECASE):
            status = "lapsed"
        elif re.search(r"OVERRIDDEN", block_text, re.IGNORECASE):
            status = "overridden"

        # Extract sponsor
        sponsor = None
        # Sponsor is typically listed before "SIGNED:" on the same line
        sponsor_match = re.search(
            r"(?:\(COR\)[^A-Z]*)((?:[A-Z][a-z]+\.?\s+)+(?:[A-Z][a-z]+))\s*\|?\s*(?:SIGNED|LAPSED|OVERRIDDEN|VETOED)",
            block_text,
        )
        if sponsor_match:
            sponsor = sponsor_match.group(1).strip()

        entries.append({
            "legislature_number": legislature_num,
            "law_number": law_number,
            "law_label": law_label,
            "description": desc,
            "bill_number": bill_number,
            "signed_date": signed_date,
            "status": status,
            "sponsor": sponsor,
            "pdf_url": pdf_url,
        })

    return entries


def normalize(raw: dict) -> dict:
    """Normalize a raw public law record into standard schema."""
    leg_num = raw.get("legislature_number", 0)
    law_num = raw.get("law_number", "unknown")

    doc_id = f"GU-PL-{law_num}"
    title = f"Public Law {law_num}"
    desc = html.unescape(raw.get("description", "")).strip()
    if desc:
        title = f"{title}: {desc[:200]}"

    iso_date = parse_date(raw.get("signed_date"))

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get("text", ""),
        "date": iso_date,
        "url": raw.get("pdf_url", ""),
        "legislature_number": leg_num,
        "law_number": law_num,
        "bill_number": raw.get("bill_number"),
        "sponsor": raw.get("sponsor"),
        "enactment_status": raw.get("status"),
        "jurisdiction": "GU",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield normalized records for all public laws."""
    count = 0
    sample_limit = 15 if sample else None
    errors = 0
    max_errors = 10

    # In sample mode, only fetch 38th legislature
    pages = LEGISLATURE_PAGES if not sample else LEGISLATURE_PAGES[:1]

    for page_info in pages:
        if sample_limit and count >= sample_limit:
            break

        leg_num = page_info["legislature"]
        try:
            entries = parse_legislature_page(
                page_info["wp_api"], page_info["base_url"], leg_num
            )
        except Exception as e:
            logger.error(f"Failed to parse {leg_num}th legislature: {e}")
            errors += 1
            continue

        for entry in entries:
            if sample_limit and count >= sample_limit:
                break
            if errors >= max_errors:
                logger.error("Too many errors, stopping")
                break

            pdf_url = entry["pdf_url"]
            logger.info(f"Downloading PL {entry['law_number']}...")

            try:
                resp = None
                for attempt in range(3):
                    try:
                        resp = session.get(pdf_url, timeout=60)
                        resp.raise_for_status()
                        break
                    except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as retry_e:
                        if attempt < 2:
                            logger.warning(f"Retry {attempt+1} for {pdf_url}: {retry_e}")
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
    page = LEGISLATURE_PAGES[0]
    print(f"Testing {page['legislature']}th legislature WP API...")
    resp = session.get(page["wp_api"], timeout=15)
    print(f"Status: {resp.status_code}")

    entries = parse_legislature_page(page["wp_api"], page["base_url"], page["legislature"])
    print(f"Found {len(entries)} public law entries")

    if entries:
        entry = entries[0]
        print(f"\nFirst entry:")
        print(f"  Law: {entry['law_label']}")
        print(f"  Bill: {entry['bill_number']}")
        print(f"  Date: {entry['signed_date']}")
        print(f"  URL: {entry['pdf_url']}")
        print(f"  Desc: {entry['description'][:150]}")

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

    if texts_ok < len(records):
        logger.warning(f"{len(records) - texts_ok} records with insufficient text")


def main():
    parser = argparse.ArgumentParser(description="GU/Legislature bootstrapper")
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
