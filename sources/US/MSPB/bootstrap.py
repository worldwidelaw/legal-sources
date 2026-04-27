#!/usr/bin/env python3
"""
US/MSPB -- Merit Systems Protection Board Decisions

Fetches MSPB decisions from official static JSON manifests at mspb.gov.
The SEARCH_MANIFESTS contain full text of all precedential and
nonprecedential decisions (1975-present).

Strategy:
  - Download JSON manifest files that contain DOCUMENT_CONTENT (full text)
  - Parse each record and normalize to standard schema
  - No pagination or scraping needed — manifests are static JSON files

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample decisions
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, List

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/MSPB"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MSPB")

BASE_URL = "https://www.mspb.gov"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# Full-text manifest URLs (SEARCH_MANIFESTS contain DOCUMENT_CONTENT)
PRECEDENTIAL_MANIFESTS = [
    f"{BASE_URL}/decisions/precedential/SEARCH_MANIFESTS/PrecedentialDecisions_Manifest_{r}.json"
    for r in [
        "2020-PRESENT", "2015-2019", "2010-2014", "2005-2009",
        "2000-2004", "1995-1999", "1990-1994", "1985-1989",
        "1980-1984", "1975-1979",
    ]
]

NONPRECEDENTIAL_MANIFESTS = [
    f"{BASE_URL}/decisions/nonprecedential/SEARCH_MANIFESTS/NonPrecedentialDecisions_Manifest_{r}.json"
    for r in [
        "2020-PRESENT", "2015-2019", "2010-2014",
    ]
]


def download_manifest(url: str) -> List[dict]:
    """Download a JSON manifest and return the data array."""
    logger.info(f"Downloading manifest: {url.split('/')[-1]}...")
    try:
        resp = SESSION.get(url, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        records = data.get("data", [])
        logger.info(f"  Got {len(records)} records")
        return records
    except Exception as e:
        logger.warning(f"Failed to download manifest {url}: {e}")
        return []


def normalize(record: dict, doc_type: str) -> dict:
    """Normalize an MSPB record into standard schema."""
    # Extract fields
    decision_number = (record.get("DECISION_NUMBER") or "").strip()
    docket = (record.get("DOCKET_NBR") or "").strip()
    issued_date = (record.get("ISSUED_DATE") or "").strip()
    first_name = (record.get("APL_FIRST_NAME") or "").strip()
    last_name = (record.get("APL_LAST_NAME") or "").strip()
    agency = (record.get("AGENCY") or "").strip()
    doc_title = (record.get("DOCTITLE") or "").strip()
    citation = (record.get("LEGAL_CITATION") or "").strip()
    file_name = (record.get("FILE_NAME") or "").strip()
    text = (record.get("DOCUMENT_CONTENT") or "").strip()

    # Build title
    applicant = f"{first_name} {last_name}".strip()
    if applicant and agency:
        title = f"{applicant} v. {agency}"
    elif applicant:
        title = applicant
    elif docket:
        title = docket
    else:
        title = decision_number or "MSPB Decision"

    if doc_title:
        title = f"{title} - {doc_title}"

    # Parse date to ISO format
    iso_date = None
    if issued_date:
        try:
            dt = datetime.strptime(issued_date, "%Y/%m/%d")
            iso_date = dt.strftime("%Y-%m-%d")
        except ValueError:
            iso_date = issued_date

    # Build URL to PDF
    if file_name:
        pdf_url = f"{BASE_URL}/decisions/{doc_type}/{file_name}"
    else:
        pdf_url = f"{BASE_URL}/decisions/decisions.htm"

    # Build unique ID
    doc_id = docket or decision_number or file_name or str(hash(text))[:12]
    safe_id = re.sub(r'[^\w\-.]', '_', doc_id)

    return {
        "_id": f"US/MSPB/{safe_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": iso_date,
        "url": pdf_url,
        "decision_number": decision_number,
        "docket_number": docket,
        "citation": citation,
        "agency": agency,
        "applicant": applicant,
        "doc_type": doc_title,
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all MSPB decisions with full text."""
    total = 0

    for url in PRECEDENTIAL_MANIFESTS:
        records = download_manifest(url)
        for rec in records:
            text = (rec.get("DOCUMENT_CONTENT") or "").strip()
            if len(text) < 50:
                continue
            result = normalize(rec, "precedential")
            total += 1
            yield result
        time.sleep(1)

    for url in NONPRECEDENTIAL_MANIFESTS:
        records = download_manifest(url)
        for rec in records:
            text = (rec.get("DOCUMENT_CONTENT") or "").strip()
            if len(text) < 50:
                continue
            result = normalize(rec, "nonprecedential")
            total += 1
            yield result
        time.sleep(1)

    logger.info(f"Total decisions with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from the most recent manifest."""
    # Use only the most recent precedential manifest for sampling
    url = PRECEDENTIAL_MANIFESTS[0]  # 2020-PRESENT
    records = download_manifest(url)

    results = []
    for rec in records:
        text = (rec.get("DOCUMENT_CONTENT") or "").strip()
        if len(text) < 50:
            continue
        result = normalize(rec, "precedential")
        results.append(result)
        if len(results) >= count:
            break

    return results


def test_api():
    """Test connectivity to MSPB manifests."""
    logger.info("Testing MSPB manifest connectivity...")

    url = PRECEDENTIAL_MANIFESTS[0]
    records = download_manifest(url)
    if not records:
        logger.error("Failed to download manifest")
        return False

    # Check for full text
    with_text = sum(1 for r in records[:100] if len((r.get("DOCUMENT_CONTENT") or "").strip()) > 50)
    logger.info(f"First 100 records: {with_text} have full text")

    if with_text == 0:
        logger.error("No records with full text found")
        return False

    # Show a sample
    for rec in records[:3]:
        text = (rec.get("DOCUMENT_CONTENT") or "").strip()
        if len(text) > 50:
            result = normalize(rec, "precedential")
            logger.info(f"Sample: {result['title']}")
            logger.info(f"  Date: {result['date']}")
            logger.info(f"  Text ({len(text)} chars): {text[:150]}...")
            break

    return True


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r"[^\w\-]", "_", record["_id"])[:80]
        filename = f"sample_{i:02d}_{safe_id}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="US/MSPB Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode")
            count = 0
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            for record in fetch_all():
                count += 1
                safe_id = re.sub(r"[^\w\-]", "_", record["_id"])[:80]
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
