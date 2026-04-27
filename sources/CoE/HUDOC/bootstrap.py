#!/usr/bin/env python3
"""
CoE/HUDOC - European Court of Human Rights Case Law

Fetches case law from HUDOC (https://hudoc.echr.coe.int).
Uses the internal JSON API for search and HTML conversion endpoint for full text.

Data coverage:
- ~58K judgments (HEJUD/HFJUD + language variants) from 1960-present
- ~170K decisions (HEDEC/HFDEC)
- Total: 227K+ documents

Historical data: The API provides full coverage from 1960 to present.
Use --chronological flag to fetch oldest records first for historical bootstrap.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from urllib.parse import urlencode

import requests

# Configuration
SEARCH_URL = "https://hudoc.echr.coe.int/app/query/results"
FULL_TEXT_URL = "https://hudoc.echr.coe.int/app/conversion/docx/html/body"
RATE_LIMIT_DELAY = 1.0  # seconds between requests

# Document type filters - all judgment types across all languages
# HEJUD/HFJUD = English/French, HJUD* = other languages (RUS, GER, SPA, TUR, etc.)
ALL_JUDGMENT_TYPES = [
    "HEJUD", "HFJUD",  # English and French judgments
    "HJUDGER", "HJUDRUM", "HJUDSLV", "HJUDGEO", "HJUDRUS",
    "HJUDALB", "HJUDSPA", "HJUDTUR", "HJUDSRP", "HJUDEST",
    "HJUDHRV", "HJUDSLO", "HJUDARM", "HJUDBUL", "HJUDBOS",
    "HJUDMAC", "HJUDCZE",
]

# Also fetch decisions (important case law)
ALL_DECISION_TYPES = ["HEDEC", "HFDEC"]

# Default: fetch both judgments and decisions for comprehensive coverage
DEFAULT_DOC_TYPES = ALL_JUDGMENT_TYPES + ALL_DECISION_TYPES


class HTMLTextExtractor(HTMLParser):
    """Extract clean text from HTML content."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.in_style = False

    def handle_starttag(self, tag, attrs):
        if tag == "style":
            self.in_style = True

    def handle_endtag(self, tag):
        if tag == "style":
            self.in_style = False

    def handle_data(self, data):
        if not self.in_style:
            text = data.strip()
            if text:
                self.text_parts.append(text)

    def get_text(self) -> str:
        return " ".join(self.text_parts)


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from HTML, removing CSS and tags."""
    parser = HTMLTextExtractor()
    parser.feed(html_content)
    text = parser.get_text()
    # Clean up CSS that might have leaked through
    text = re.sub(r"\.s[A-F0-9]+\s*\{[^}]+\}", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def search_cases(
    query: str = "contentsitename:ECHR",
    doc_types: Optional[list] = None,
    start: int = 0,
    length: int = 50,
    sort: str = "kpdate Descending",
) -> Dict[str, Any]:
    """Search HUDOC for cases."""
    if doc_types:
        type_filter = " OR ".join([f'doctype="{t}"' for t in doc_types])
        query = f"{query} AND ({type_filter})"

    params = {
        "query": query,
        "select": "itemid,docname,doctype,conclusion,kpdate,languageisocode,application,ecli,importance,respondent,representedby,separateopinion",
        "sort": sort,
        "start": start,
        "length": length,
    }

    url = f"{SEARCH_URL}?{urlencode(params)}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def fetch_full_text(item_id: str) -> Optional[str]:
    """Fetch the full text HTML of a case and extract clean text."""
    url = f"{FULL_TEXT_URL}?library=ECHR&id={item_id}"
    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        return extract_text_from_html(response.text)
    except Exception as e:
        print(f"  Warning: Failed to fetch full text for {item_id}: {e}")
        return None


def normalize(raw: Dict[str, Any], full_text: Optional[str] = None) -> Dict[str, Any]:
    """Normalize a HUDOC record to standard schema."""
    columns = raw.get("columns", raw)

    item_id = columns.get("itemid", "")
    doc_name = columns.get("docname", "")
    doc_type = columns.get("doctype", "")
    kp_date = columns.get("kpdate", "")
    lang = columns.get("languageisocode", "ENG")
    conclusion = columns.get("conclusion", "")
    ecli = columns.get("ecli", "")
    application = columns.get("application", "")
    respondent = columns.get("respondent", "")
    importance = columns.get("importance", "")

    # Parse date
    date_iso = None
    if kp_date:
        try:
            dt = datetime.fromisoformat(kp_date.replace("Z", "+00:00"))
            date_iso = dt.strftime("%Y-%m-%d")
        except ValueError:
            date_iso = kp_date[:10] if len(kp_date) >= 10 else kp_date

    # Determine document type
    type_mapping = {
        "HEJUD": "judgment",
        "HFJUD": "judgment",
        "HEDEC": "decision",
        "HFDEC": "decision",
        "HECOMOLD": "commission_report",
        "HFCOMOLD": "commission_report",
    }
    case_type = type_mapping.get(doc_type, "judgment")

    return {
        "_id": item_id,
        "_source": "CoE/HUDOC",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc_name,
        "text": full_text or "",
        "date": date_iso,
        "url": f"https://hudoc.echr.coe.int/eng?i={item_id}",
        "ecli": ecli,
        "application_number": application,
        "respondent_state": respondent,
        "conclusion": conclusion,
        "language": lang.lower() if lang else "eng",
        "document_type": doc_type,
        "case_type": case_type,
        "importance_level": importance,
    }


def fetch_year_range(
    start_year: int,
    end_year: int,
    doc_types: Optional[list] = None,
    max_records: Optional[int] = None,
) -> Generator[Dict[str, Any], None, None]:
    """Fetch cases within a specific year range.

    The HUDOC API limits pagination to ~10K records. This function fetches
    data for a specific date range to stay within that limit.
    """
    if doc_types is None:
        doc_types = DEFAULT_DOC_TYPES

    type_filter = " OR ".join([f'doctype="{t}"' for t in doc_types])
    query = f"contentsitename:ECHR AND ({type_filter}) AND kpdate>={start_year}-01-01 AND kpdate<={end_year}-12-31"

    batch_size = 50
    start = 0
    fetched = 0

    while True:
        params = {
            "query": query,
            "select": "itemid,docname,doctype,conclusion,kpdate,languageisocode,application,ecli,importance,respondent,representedby,separateopinion",
            "sort": "kpdate Descending",
            "start": start,
            "length": batch_size,
        }

        url = f"{SEARCH_URL}?{urlencode(params)}"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        result = response.json()

        total = result.get("resultcount", 0)
        results = result.get("results", [])

        if not results:
            break

        for item in results:
            if max_records and fetched >= max_records:
                return

            item_id = item.get("columns", {}).get("itemid", "")
            if not item_id:
                continue

            time.sleep(RATE_LIMIT_DELAY)
            full_text = fetch_full_text(item_id)
            record = normalize(item, full_text)

            if record.get("text"):
                yield record
                fetched += 1

        start += batch_size

        # HUDOC API limits offset to ~10K
        if start >= min(total, 9500):
            break


def fetch_all(
    max_records: Optional[int] = None,
    doc_types: Optional[list] = None,
    chronological: bool = False,
) -> Generator[Dict[str, Any], None, None]:
    """Fetch all cases from HUDOC using date-range pagination.

    The HUDOC API limits pagination offset to ~10K records. To fetch the full
    catalog of 130K+ documents, we iterate year by year and paginate within
    each year (all years have <10K records).

    Args:
        max_records: Optional limit on number of records to fetch
        doc_types: Document types to fetch (defaults to all judgments + decisions)
        chronological: If True, fetch oldest first (for historical bootstrap)
    """
    if doc_types is None:
        doc_types = DEFAULT_DOC_TYPES

    # Get current year for year range
    current_year = datetime.now().year

    # Generate year ranges - process year by year
    # Most years have <10K records, but busy years (2000s-2020s) may need smaller chunks
    if chronological:
        years = list(range(1955, current_year + 1))
    else:
        years = list(range(current_year, 1954, -1))

    fetched = 0

    for year in years:
        if max_records and fetched >= max_records:
            return

        remaining = max_records - fetched if max_records else None
        print(f"\n=== Fetching year {year} ===")

        for record in fetch_year_range(year, year, doc_types, max_records=remaining):
            yield record
            fetched += 1

            if max_records and fetched >= max_records:
                return


def fetch_updates(since: datetime) -> Generator[Dict[str, Any], None, None]:
    """Fetch cases updated since a given date."""
    since_str = since.strftime("%Y-%m-%d")
    query = f"contentsitename:ECHR AND kpdate>={since_str}"

    batch_size = 50
    start = 0

    while True:
        result = search_cases(
            query=query,
            doc_types=DEFAULT_DOC_TYPES,
            start=start,
            length=batch_size,
            sort="kpdate Descending",
        )

        results = result.get("results", [])
        total = result.get("resultcount", 0)

        if not results:
            break

        for item in results:
            item_id = item.get("columns", {}).get("itemid", "")
            if not item_id:
                continue

            time.sleep(RATE_LIMIT_DELAY)
            full_text = fetch_full_text(item_id)
            record = normalize(item, full_text)

            if record.get("text"):
                yield record

        start += batch_size
        if start >= total:
            break


def bootstrap_sample(sample_dir: Path, count: int = 15, historical: bool = True) -> int:
    """Fetch sample records and save to sample directory.

    Args:
        sample_dir: Directory to save sample records
        count: Total number of records to fetch
        historical: If True, fetch a mix of old and recent records to verify
                   full historical coverage (earliest from 1960)
    """
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    if historical:
        # Fetch half from oldest records (1960s) and half from recent
        half = count // 2
        print(f"Fetching {half} OLDEST records (historical verification)...")
        for record in fetch_all(max_records=half, chronological=True):
            item_id = record["_id"]
            filename = f"{item_id.replace('/', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            date = record.get("date", "N/A")
            print(f"  Saved {filename} (date={date}, {text_len:,} chars)")
            saved += 1

        print(f"\nFetching {count - half} RECENT records...")
        for record in fetch_all(max_records=count - half, chronological=False):
            item_id = record["_id"]
            filename = f"{item_id.replace('/', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            date = record.get("date", "N/A")
            print(f"  Saved {filename} (date={date}, {text_len:,} chars)")
            saved += 1
    else:
        print(f"Fetching {count} sample records from HUDOC...")
        for record in fetch_all(max_records=count):
            item_id = record["_id"]
            filename = f"{item_id.replace('/', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            print(f"  Saved {filename} ({text_len:,} chars)")
            saved += 1

    return saved


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records meet requirements."""
    samples = list(sample_dir.glob("*.json"))

    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need at least 10")
        return False

    total_text_len = 0
    all_valid = True

    for sample_path in samples:
        with open(sample_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        text = record.get("text", "")
        if not text:
            print(f"FAIL: {sample_path.name} has no text")
            all_valid = False
        elif len(text) < 500:
            print(f"WARN: {sample_path.name} has short text ({len(text)} chars)")

        total_text_len += len(text)

        # Check required fields
        for field in ["_id", "_source", "_type", "title", "date"]:
            if not record.get(field):
                print(f"WARN: {sample_path.name} missing {field}")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="CoE/HUDOC data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "validate", "fetch"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of records to fetch",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Fetch records since date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--historical",
        action="store_true",
        default=True,
        help="Fetch mix of oldest and newest records (default: True)",
    )
    parser.add_argument(
        "--chronological",
        action="store_true",
        help="Fetch oldest records first (for historical bootstrap)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            saved = bootstrap_sample(sample_dir, args.count, historical=args.historical)
            print(f"\nSaved {saved} sample records to {sample_dir}")
            sys.exit(0 if saved >= 10 else 1)
        else:
            print("Use --sample for bootstrap mode")
            sys.exit(1)

    elif args.command == "validate":
        valid = validate_samples(sample_dir)
        sys.exit(0 if valid else 1)

    elif args.command == "fetch":
        if args.since:
            since = datetime.fromisoformat(args.since)
            for record in fetch_updates(since):
                print(json.dumps(record, ensure_ascii=False))
        else:
            for record in fetch_all(max_records=args.count, chronological=args.chronological):
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
