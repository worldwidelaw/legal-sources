#!/usr/bin/env python3
"""
EG/SCC -- Egypt Supreme Constitutional Court

Fetches decisions from the SCC's Django REST API at sccourt.gov.eg.
Covers ~9,190 decisions (1970-present) across three case types:
  1 = Constitutional (dostoriya)
  2 = Legislative interpretation (tafseer)
  3 = Jurisdictional conflict (tanazua)

Strategy:
  - Enumerate cases by type via get-cases-basedon-casestatuscode
  - Fetch full details per case via get-rule-details-by-case-id
  - Combine subject, requests, ruleWarding, caseDetails, briefSubject,
    disagreeReason, caseInfo into a composite full-text field

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick API test
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

try:
    import requests
except ImportError:
    print("ERROR: requests library required. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "EG/SCC"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EG.SCC")

BASE_URL = "https://www.sccourt.gov.eg/DjangoPortal"
CASE_TYPES = {1: "constitutional", 2: "interpretation", 3: "jurisdictional_conflict"}
PAGE_SIZE = 25
DELAY = 1.5  # seconds between requests


def _get(path: str, params: dict = None) -> dict:
    """GET request to the SCC API."""
    url = f"{BASE_URL}{path}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "LegalDataHunter/1.0 (legal research)",
    }
    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, body: dict) -> dict:
    """POST request to the SCC API."""
    url = f"{BASE_URL}{path}"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "LegalDataHunter/1.0 (legal research)",
    }
    resp = requests.post(url, json=body, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_date(date_str: str) -> str:
    """Parse date string to ISO 8601. Returns empty string on failure."""
    if not date_str:
        return ""
    # API returns dates like "2024-01-15T00:00:00" or "15/01/2024"
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            continue
    return date_str.strip() if isinstance(date_str, str) else ""


def _build_text(details: dict) -> str:
    """Combine all text fields into a composite full-text document."""
    parts = []
    field_map = [
        ("subject", "الموضوع"),  # Subject
        ("briefSubject", "ملخص الموضوع"),  # Brief subject
        ("requests", "الطلبات"),  # Requests
        ("caseDetails", "تفاصيل القضية"),  # Case details
        ("caseInfo", "معلومات القضية"),  # Case info
        ("disagreeReason", "أسباب الاعتراض"),  # Disagreement reasons
        ("ruleWarding", "منطوق الحكم"),  # Disposition/ruling text
        ("law", "القانون"),  # Law reference
    ]
    for field, label in field_map:
        val = details.get(field, "")
        if val and str(val).strip():
            text = str(val).strip()
            parts.append(f"## {label}\n{text}")
    return "\n\n".join(parts)


def normalize(raw: dict) -> dict:
    """Transform raw API record to standard schema."""
    details = raw.get("details", raw)
    case_id = str(details.get("caseId", ""))
    rule_number = str(details.get("ruleNumber", ""))
    court_year = str(details.get("courtYear", ""))

    # Build doc ID
    doc_id = f"EG-SCC-{case_id}" if case_id else f"EG-SCC-R{rule_number}-Y{court_year}"

    # Build title
    subject = details.get("subject", "")
    title = subject[:200] if subject else f"قرار رقم {rule_number} لسنة {court_year}"

    text = _build_text(details)

    # Dates
    acceptance_date = _parse_date(details.get("acceptanceDate", ""))
    submission_date = _parse_date(details.get("submissionDate", ""))
    rule_date = _parse_date(details.get("ruleDate", ""))
    date = acceptance_date or rule_date or submission_date

    # Case type
    case_type_code = details.get("ruleSubjectTypeCode", details.get("caseStatusCode", ""))
    case_type = CASE_TYPES.get(case_type_code, str(case_type_code))

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": f"https://www.sccourt.gov.eg/newportal/",
        "case_id": case_id,
        "rule_number": rule_number,
        "court_year": court_year,
        "case_type": case_type,
        "disposition": str(details.get("ruleWarding", "")).strip(),
        "plaintiff": str(details.get("claimed", "")).strip(),
        "defendant": str(details.get("defendent", "")).strip(),
        "law_reference": str(details.get("law", "")).strip(),
    }


def fetch_case_ids_by_type(case_status_code: int, max_pages: int = None) -> Generator[str, None, None]:
    """Enumerate case IDs by case type with pagination."""
    page = 1
    seen = set()
    while True:
        if max_pages and page > max_pages:
            break
        try:
            data = _get(
                "/api/Cases/get-cases-basedon-casestatuscode",
                params={
                    "caseStatusCode": case_status_code,
                    "PageNumber": page,
                    "PageSize": PAGE_SIZE,
                },
            )
        except Exception as e:
            logger.warning(f"Error fetching page {page} for type {case_status_code}: {e}")
            break

        items = data.get("data", [])
        if not items:
            break

        new_count = 0
        for item in items:
            cid = str(item.get("caseId", ""))
            if cid and cid not in seen:
                seen.add(cid)
                yield cid
                new_count += 1

        if new_count == 0:
            break

        logger.info(f"Type {case_status_code}, page {page}: {len(items)} items, {new_count} new")
        page += 1
        time.sleep(DELAY)


def fetch_case_details(case_id: str) -> dict:
    """Fetch full details for a single case."""
    data = _get(
        "/api/RulesInsert/get-rule-details-by-case-id",
        params={"CaseiD": case_id},
    )
    items = data.get("data", [])
    if items:
        return items[0] if isinstance(items, list) else items
    return data.get("data", {})


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all decisions. If sample=True, fetch ~15 records."""
    max_pages_per_type = 2 if sample else None
    total = 0

    for type_code, type_name in CASE_TYPES.items():
        logger.info(f"Fetching case type: {type_name} (code={type_code})")
        for case_id in fetch_case_ids_by_type(type_code, max_pages=max_pages_per_type):
            try:
                details = fetch_case_details(case_id)
                if not details:
                    logger.warning(f"No details for case {case_id}")
                    continue
                record = normalize({"details": details})
                if record.get("text", "").strip():
                    yield record
                    total += 1
                else:
                    logger.warning(f"Empty text for case {case_id}, skipping")
            except Exception as e:
                logger.warning(f"Error fetching case {case_id}: {e}")
            time.sleep(DELAY)

            if sample and total >= 15:
                return

    logger.info(f"Total records fetched: {total}")


def test_api():
    """Quick API connectivity test."""
    print("Testing SCC API...")

    # Test chart endpoint for total counts
    try:
        data = _get("/api/Cases/get-the-chart-from-database")
        print(f"✓ Chart endpoint: {json.dumps(data.get('data', {}), indent=2)[:500]}")
    except Exception as e:
        print(f"✗ Chart endpoint failed: {e}")
        return False

    # Test case listing
    try:
        data = _get(
            "/api/Cases/get-cases-basedon-casestatuscode",
            params={"caseStatusCode": 1, "PageNumber": 1, "PageSize": 5},
        )
        items = data.get("data", [])
        print(f"✓ Case listing: {len(items)} items returned")
        if items:
            print(f"  First case ID: {items[0].get('caseId')}")
    except Exception as e:
        print(f"✗ Case listing failed: {e}")
        return False

    # Test case details
    if items:
        cid = items[0].get("caseId")
        try:
            details = fetch_case_details(str(cid))
            text = _build_text(details)
            print(f"✓ Case details for {cid}: {len(text)} chars of text")
            print(f"  Fields present: {[k for k, v in details.items() if v]}")
        except Exception as e:
            print(f"✗ Case details failed: {e}")
            return False

    print("\nAll API tests passed!")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    out_dir = SAMPLE_DIR if sample else SOURCE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        fname = f"{record['_id']}.json"
        # Sanitize filename
        fname = fname.replace("/", "_").replace("\\", "_")
        out_path = out_dir / fname
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        if count % 10 == 0:
            logger.info(f"Saved {count} records")

    logger.info(f"Bootstrap complete: {count} records saved to {out_dir}")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EG/SCC Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        print(f"Done: {count} records")
