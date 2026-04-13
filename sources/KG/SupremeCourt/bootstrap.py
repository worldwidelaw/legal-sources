#!/usr/bin/env python3
"""
KG/SupremeCourt - Kyrgyzstan Supreme Court Decisions (Digital Justice Portal)

Data source: https://portal.sot.kg
Format: REST API (JSON) with inline HTML full text
License: Public Domain (government decisions)
Records: ~5,000+ Supreme Court decisions (all chambers)

The Digital Justice Portal (GRSA) is Kyrgyzstan's State Registry of Judicial Acts.
It provides a fully open REST API at portal.sot.kg/api/v1/ with no authentication.

Full text is available as inline HTML in the file_html field of each case_act entry.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, Generator, List, Optional

import requests
from bs4 import BeautifulSoup

# Configuration
SOURCE_ID = "KG/SupremeCourt"
BASE_URL = "https://portal.sot.kg"
API_URL = f"{BASE_URL}/api/v1/cc_court_case/"
COURT_LIST_URL = f"{BASE_URL}/api/v1/court/"
SUPREME_COURT_ID = 87  # Supreme Court of Kyrgyzstan
REQUEST_DELAY = 1.5  # seconds between requests
PER_PAGE = 100


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
        "Accept-Language": "ru-RU,ru;q=0.9,ky;q=0.8,en;q=0.7",
    })
    return session


def clean_html_text(html_content: str) -> str:
    """Extract clean text from HTML content."""
    if not html_content:
        return ""
    soup = BeautifulSoup(html_content, "html.parser")
    text = soup.get_text(separator="\n")
    text = unescape(text)
    # Normalize whitespace
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def fetch_cases_page(session: requests.Session, page: int = 1,
                     court_id: Optional[int] = None,
                     start_date: Optional[str] = None,
                     end_date: Optional[str] = None) -> Optional[Dict]:
    """Fetch a page of court cases from the API."""
    params = {
        "page": page,
        "per_page": PER_PAGE,
        "case_act_exist": "true",
    }
    if court_id is not None:
        params["court_id"] = court_id
    if start_date:
        params["start_date"] = start_date
    if end_date:
        params["end_date"] = end_date

    try:
        resp = session.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  Error fetching page {page}: {e}", file=sys.stderr)
        return None


def extract_full_text(case_data: Dict) -> str:
    """Extract full text from a case's judicial acts."""
    text_parts = []
    case_acts = case_data.get("case_act") or []

    for act in case_acts:
        html = act.get("file_html") or ""
        if html:
            cleaned = clean_html_text(html)
            if cleaned:
                text_parts.append(cleaned)

    return "\n\n---\n\n".join(text_parts)


def normalize(raw: Dict) -> Dict:
    """Transform raw case data into standard schema."""
    case_id = raw.get("id", "")
    case_number = (raw.get("case_number")
                   or raw.get("third_instance_number")
                   or raw.get("second_instance_number")
                   or "")

    # Parse date — prefer act_date from case_act, then in_date
    date_str = ""
    case_acts = raw.get("case_act") or []
    if case_acts:
        date_str = case_acts[0].get("act_date") or case_acts[0].get("create_date") or ""
    if not date_str:
        date_str = raw.get("in_date") or ""
    parsed_date = None
    if date_str:
        match = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        if match:
            parsed_date = match.group(1)

    # Court info
    court = raw.get("court") or {}
    court_name = court.get("name") or court.get("name_kg") or "Unknown Court"

    # Judge info
    judge = raw.get("judge") or {}
    judge_name = ""
    if isinstance(judge, dict):
        parts = [judge.get("name_first", ""), judge.get("name_second", "")]
        judge_name = " ".join(p for p in parts if p).strip()

    # Production type
    production_type = raw.get("production_type") or {}
    if isinstance(production_type, dict):
        prod_name = production_type.get("name_ru") or production_type.get("name") or ""
    else:
        prod_name = str(production_type) if production_type else ""

    # Build title
    title_parts = []
    if case_number:
        title_parts.append(case_number)
    if parsed_date:
        title_parts.append(parsed_date)
    if court_name and court_name != "Unknown Court":
        title_parts.append(court_name)
    title = " — ".join(title_parts) if title_parts else f"Case {case_id}"

    # Extract full text from case acts
    full_text = extract_full_text(raw)

    # Parties
    parties = raw.get("cc_take_parties") or []
    party_names = []
    for p in parties:
        if isinstance(p, dict):
            name = p.get("name") or p.get("full_name") or ""
            if name:
                party_names.append(name)

    # Case URL
    url = f"{BASE_URL}/cases/{case_id}"

    return {
        "_id": f"KG-SC-{case_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": parsed_date,
        "url": url,
        "case_number": case_number,
        "court": court_name,
        "judge": judge_name,
        "production_type": prod_name,
        "parties": party_names,
    }


def fetch_all(court_id: Optional[int] = SUPREME_COURT_ID) -> Generator[Dict, None, None]:
    """Fetch all available decisions with full text."""
    session = get_session()

    page = 1
    total_yielded = 0

    while True:
        print(f"  Fetching page {page}...")
        data = fetch_cases_page(session, page=page, court_id=court_id)

        if not data:
            break

        # Handle paginated response
        items = data.get("items") or []
        if not items:
            break

        for case in items:
            record = normalize(case)
            if record.get("text") and len(record["text"]) >= 100:
                yield record
                total_yielded += 1

        # Check if more pages
        total_count = data.get("total") or 0
        if page * PER_PAGE >= total_count or not items:
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    print(f"  Total records yielded: {total_yielded}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch decisions modified since a given date."""
    since_str = since.strftime("%Y-%m-%d")
    session = get_session()
    page = 1

    while True:
        data = fetch_cases_page(session, page=page, court_id=SUPREME_COURT_ID,
                                start_date=since_str)
        if not data:
            break

        items = data.get("items") or []
        if not items:
            break

        for case in items:
            record = normalize(case)
            if record.get("text") and len(record["text"]) >= 100:
                yield record

        total_count = data.get("total") or 0
        if page * PER_PAGE >= total_count or not items:
            break

        page += 1
        time.sleep(REQUEST_DELAY)


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    session = get_session()

    print("Fetching Supreme Court cases with judicial acts...")
    data = fetch_cases_page(session, page=1, court_id=SUPREME_COURT_ID)

    if not data:
        print("ERROR: Could not fetch cases from API")
        return

    results = data.get("items") or []

    total_available = data.get("total") or len(results)
    print(f"Total available: {total_available} cases with acts")
    print(f"First page returned: {len(results)} cases")

    if not results:
        print("ERROR: No cases found!")
        return

    sample_dir.mkdir(parents=True, exist_ok=True)

    records_saved = 0
    records_attempted = 0
    total_text_chars = 0

    for case in results:
        if records_saved >= count:
            break

        records_attempted += 1
        record = normalize(case)

        text_len = len(record.get("text", ""))
        if text_len < 100:
            print(f"  Skipping case {record.get('case_number', '?')} — text too short ({text_len} chars)")
            continue

        total_text_chars += text_len
        records_saved += 1

        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
        filename = f"{safe_name}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  [{records_saved}/{count}] {filename}")
        print(f"    Case: {record.get('case_number', '?')} | Court: {record.get('court', '?')}")
        print(f"    Text: {text_len:,} chars | Date: {record.get('date', '?')}")

    # If first page wasn't enough, fetch more pages
    page = 2
    while records_saved < count:
        print(f"\n  Fetching page {page} for more samples...")
        time.sleep(REQUEST_DELAY)
        data = fetch_cases_page(session, page=page, court_id=SUPREME_COURT_ID)

        if not data:
            break
        results = data.get("items") or []
        if not results:
            break

        for case in results:
            if records_saved >= count:
                break

            records_attempted += 1
            record = normalize(case)

            text_len = len(record.get("text", ""))
            if text_len < 100:
                continue

            total_text_chars += text_len
            records_saved += 1

            safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
            filename = f"{safe_name}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  [{records_saved}/{count}] {filename}")
            print(f"    Case: {record.get('case_number', '?')} | Text: {text_len:,} chars")

        page += 1

    # Summary
    print("\n" + "=" * 60)
    print("SAMPLE SUMMARY")
    print("=" * 60)
    print(f"Records attempted: {records_attempted}")
    print(f"Records saved: {records_saved}")
    if records_saved > 0:
        avg_chars = total_text_chars // records_saved
        print(f"Total text chars: {total_text_chars:,}")
        print(f"Average text length: {avg_chars:,} chars/doc")
    print(f"Sample directory: {sample_dir}")

    if records_saved >= 10:
        print("\nSUCCESS: 10+ sample records with full text")
    else:
        print(f"\nWARNING: Only {records_saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(description="Kyrgyzstan Supreme Court Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true",
                        help="Run full bootstrap (all records)")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Running full bootstrap...")
            sample_dir.mkdir(parents=True, exist_ok=True)
            records_saved = 0
            for record in fetch_all():
                safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
                filename = f"{safe_name}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                records_saved += 1
                if records_saved % 50 == 0:
                    print(f"  Saved {records_saved} records...")

            print(f"\nFull bootstrap complete: {records_saved} records saved")

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
