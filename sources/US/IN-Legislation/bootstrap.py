#!/usr/bin/env python3
"""
US/IN-Legislation -- Indiana Code (iga.in.gov)

Fetches all Indiana Code sections with full text from official per-title
PDF downloads on iga.in.gov.

Strategy:
  1. Download per-title PDFs from iga.in.gov/ic/{year}/Title_{N}.pdf
  2. Extract text using PyMuPDF (fitz)
  3. Split into individual sections using IC X-X-X-X header pattern
  4. Normalize into standard schema

Data: Public domain (Indiana government works). No auth required.
Requires PyMuPDF for PDF text extraction.

Usage:
  python bootstrap.py bootstrap            # Full pull (all 36 titles)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import io
from pathlib import Path
from datetime import datetime, timezone
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

SOURCE_ID = "US/IN-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IN-Legislation")

BASE_URL = "https://iga.in.gov/ic"
YEAR = "2024"

# All 36 titles; Title 7 is repealed (returns HTML not PDF)
ALL_TITLES = list(range(1, 37))
REPEALED_TITLES = {7}

# For --sample mode: fetch these titles and limit sections
SAMPLE_TITLES = [1, 35, 36]
SAMPLE_MAX_SECTIONS = 6

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/pdf,*/*",
}

# Section header pattern: IC X-X-X-X followed by description
SECTION_RE = re.compile(
    r'^(IC \d+-\d+-\d+-\d+(?:\.\d+)?)\s+(.+?)(?:\n|$)',
    re.MULTILINE
)

# Indiana Code title names (for metadata)
TITLE_NAMES = {
    1: "General Provisions",
    2: "General Assembly",
    3: "Administrative Law",
    4: "State Administration",
    5: "State and Local Administration",
    6: "Local Government",
    7: "Probate (Repealed)",
    8: "Commercial Law and Consumer Protection",
    9: "Motor Vehicles",
    10: "Children and Families",
    11: "Criminal Law and Procedure",
    12: "Human Services",
    13: "Environment",
    14: "Natural and Cultural Resources",
    15: "Education",
    16: "Health",
    17: "Railroads (Repealed chapters)",
    18: "Highways (Repealed chapters)",
    19: "Conservation and Resource Development (Repealed chapters)",
    20: "Tax and Finance",
    21: "Higher Education",
    22: "Courts and Court Officers",
    23: "Family Law",
    24: "Trade and Commerce",
    25: "Professions and Occupations",
    26: "Commercial Code",
    27: "Financial Institutions",
    28: "Insurance",
    29: "Military and Veterans",
    30: "State Officers",
    31: "Utilities and Transportation",
    32: "Real Property",
    33: "Courts and Judicial Officers",
    34: "Civil Law and Procedure",
    35: "Criminal Law and Procedure",
    36: "Local Government",
}


def download_title_pdf(title_num: int) -> Optional[bytes]:
    """Download a title PDF from iga.in.gov."""
    url = f"{BASE_URL}/{YEAR}/Title_{title_num}.pdf"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        # Check it's actually a PDF (Title 7 returns HTML)
        if not resp.content[:4] == b'%PDF':
            logger.warning(f"Title {title_num}: not a PDF (likely repealed)")
            return None
        return resp.content
    except Exception as e:
        logger.error(f"Failed to download Title {title_num}: {e}")
        return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/IN-Legislation",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""

def parse_sections(full_text: str, title_num: int) -> list:
    """Parse individual sections from extracted PDF text.

    Indiana Code sections follow the pattern:
      IC X-X-X-X Section heading
      Sec. N. Section text...
    """
    matches = list(SECTION_RE.finditer(full_text))
    if not matches:
        return []

    sections = []
    for i, m in enumerate(matches):
        ic_num = m.group(1).strip()      # e.g., "IC 1-1-1-1"
        heading = m.group(2).strip()     # e.g., "Citation"

        # Section text runs from this match to the next
        start = m.start()
        if i + 1 < len(matches):
            end = matches[i + 1].start()
        else:
            end = len(full_text)

        text = full_text[start:end].strip()

        # Skip very short entries (table of contents lines, etc.)
        if len(text) < 50:
            continue

        # Clean the section number for use as ID
        section_id = ic_num.replace("IC ", "IC-").replace(" ", "")

        sections.append({
            "section_id": section_id,
            "ic_num": ic_num,
            "heading": heading,
            "title_num": title_num,
            "text": text,
        })

    return sections


def normalize(raw: dict) -> dict:
    """Transform raw section data into standard schema."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    title_name = TITLE_NAMES.get(raw["title_num"], f"Title {raw['title_num']}")

    return {
        "_id": raw["section_id"],
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": now,
        "title": f"{raw['ic_num']} - {raw['heading']}",
        "text": raw["text"],
        "date": today,
        "url": f"https://iga.in.gov/laws/{YEAR}/ic/titles/{raw['title_num']}",
        "title_num": raw["title_num"],
        "title_name": title_name,
    }


def process_title(title_num: int, max_sections: int = 0) -> Generator[dict, None, None]:
    """Download and process a single title."""
    if title_num in REPEALED_TITLES:
        logger.info(f"Title {title_num}: skipped (repealed)")
        return

    logger.info(f"Downloading Title {title_num}...")
    pdf_bytes = download_title_pdf(title_num)
    if not pdf_bytes:
        return

    logger.info(f"Title {title_num}: {len(pdf_bytes)} bytes, extracting text...")
    full_text = extract_text_from_pdf(pdf_bytes)
    sections = parse_sections(full_text, title_num)
    logger.info(f"Title {title_num}: {len(sections)} sections found")

    count = 0
    for sec in sections:
        yield normalize(sec)
        count += 1
        if max_sections > 0 and count >= max_sections:
            break

    # Rate limit between titles
    time.sleep(2.0)


def fetch_all() -> Generator[dict, None, None]:
    """Yield all sections across all Indiana Code titles."""
    total = 0
    for title_num in ALL_TITLES:
        for record in process_title(title_num):
            yield record
            total += 1
            if total % 500 == 0:
                logger.info(f"  Progress: {total} sections fetched")
    logger.info(f"Total sections fetched: {total}")


def fetch_sample() -> Generator[dict, None, None]:
    """Fetch a small sample from a few titles."""
    logger.info(f"Fetching sample from titles {SAMPLE_TITLES}...")
    count = 0
    for title_num in SAMPLE_TITLES:
        for record in process_title(title_num, max_sections=SAMPLE_MAX_SECTIONS):
            yield record
            count += 1
    logger.info(f"Sample complete: {count} sections fetched")


def test_api() -> bool:
    """Test connectivity to iga.in.gov PDF downloads."""
    logger.info("Testing Indiana General Assembly PDF downloads...")
    try:
        url = f"{BASE_URL}/{YEAR}/Title_1.pdf"
        resp = requests.get(url, headers=HEADERS, timeout=30, stream=True)
        resp.raise_for_status()
        chunk = resp.raw.read(1024)
        resp.close()
        if chunk[:4] == b'%PDF':
            logger.info("  Title 1 PDF: OK")
            logger.info("API test PASSED")
            return True
        else:
            logger.error("API test FAILED: not a PDF")
            return False
    except Exception as e:
        logger.error(f"API test FAILED: {e}")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/IN-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        SAMPLE_DIR.mkdir(exist_ok=True)

        if args.sample:
            gen = fetch_sample()
        else:
            gen = fetch_all()

        count = 0
        for record in gen:
            fname = f"sample_{count+1:02d}_{SOURCE_ID.replace('/', '_')}_{record['_id']}.json"
            out_path = SAMPLE_DIR / fname
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            if count <= 20 or count % 100 == 0:
                logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
