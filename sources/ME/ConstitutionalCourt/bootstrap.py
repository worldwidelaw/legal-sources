#!/usr/bin/env python3
"""
Montenegro Constitutional Court (Ustavni sud Crne Gore) - Case Law Scraper

Fetches Constitutional Court decisions from the official database at ustavnisud.me.
Uses the DataTables server-side API (upit.php) for paginated access.

API Details:
- Endpoint: http://www.ustavnisud.me/ustavnisud/upit.php
- Method: POST
- Response: JSON with DataTables format
- Full text: Available in sadrzaj_fajlova field
- Records: ~18,000+ decisions from 1964 onwards
"""

import argparse
import json
import os
import sys
import time
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional, Dict, Any

import requests
import yaml


BASE_URL = "http://www.ustavnisud.me/ustavnisud"
LIST_ENDPOINT = f"{BASE_URL}/upit.php"
PAGE_SIZE = 100
RATE_LIMIT_DELAY = 1.5

HEADERS = {
    "Content-Type": "application/x-www-form-urlencoded",
    "Referer": f"{BASE_URL}/arhiva.php",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}


def load_config() -> dict:
    """Load source configuration from config.yaml."""
    config_path = Path(__file__).parent / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def clean_text(text: str) -> str:
    """Clean text content, removing HTML tags and normalizing whitespace."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def normalize_date(date_str: str) -> Optional[str]:
    """Normalize date to ISO 8601 format."""
    if not date_str:
        return None

    # Handle various date formats
    for fmt in ["%Y-%m-%d", "%Y.%m.%d", "%d.%m.%Y", "%Y-%m-%d %H:%M:%S"]:
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_case_number(djel_broj: str) -> Dict[str, Any]:
    """Extract structured case number components."""
    result = {"raw": djel_broj}

    if not djel_broj:
        return result

    # Pattern: U-III br.383/25 or U-I br. 7/17
    match = re.match(r'(U-[IVX]+)\s*br\.?\s*(\d+)/(\d+)', djel_broj)
    if match:
        result["type"] = match.group(1)
        result["number"] = int(match.group(2))
        year = int(match.group(3))
        # Convert 2-digit year to 4-digit
        if year < 50:
            result["year"] = 2000 + year
        else:
            result["year"] = 1900 + year

    return result


def normalize(raw: dict) -> dict:
    """Transform raw API data into normalized schema."""
    # Get the full text
    full_text = raw.get("sadrzaj_fajlova", "") or raw.get("8", "")
    full_text = clean_text(full_text)

    # Get case number
    case_number = raw.get("djelovodni_broj", "") or raw.get("3", "")
    case_info = extract_case_number(case_number)

    # Get date
    date_str = raw.get("datum", "") or raw.get("2", "")
    decision_date = normalize_date(date_str)

    session_date_str = raw.get("datum_sjednice", "") or raw.get("10", "")
    session_date = normalize_date(session_date_str)

    # Get document ID
    iddok = raw.get("iddok", "") or raw.get("0", "")

    # Get document type
    doc_type = raw.get("vrsta_dokumenta", "") or raw.get("4", "")

    # Get challenged act
    challenged_act = raw.get("osporeni_akt", "") or raw.get("9", "")
    challenged_act = clean_text(challenged_act)

    # Get keywords
    keywords = raw.get("kljucne_rijeci_tagovi", "") or raw.get("5", "")

    # Get constitutional articles
    const_articles = raw.get("clan_ustava_cg_atr19", "") or raw.get("6", "")

    # Get convention articles (ECHR)
    conv_articles = raw.get("clan_konvencije_atr20", "") or raw.get("7", "")

    # Get applicant info
    applicant = raw.get("komitent", "") or raw.get("1", "")

    # Build title from case number and type
    title = case_number
    if doc_type:
        title = f"{case_number} - {doc_type}"

    # Build URL
    url = f"{BASE_URL}/arhiva.php"  # Archive search page

    return {
        "_id": f"ME/ConstitutionalCourt/{iddok}",
        "_source": "ME/ConstitutionalCourt",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": decision_date or session_date,
        "url": url,
        "case_number": case_number,
        "case_type": case_info.get("type"),
        "case_year": case_info.get("year"),
        "document_type": doc_type,
        "session_date": session_date,
        "challenged_act": challenged_act,
        "keywords": keywords,
        "constitutional_articles": const_articles,
        "convention_articles": conv_articles,
        "applicant": applicant,
        "internal_id": iddok,
        "language": "sr",
    }


def fetch_page(start: int = 0, length: int = PAGE_SIZE, session: requests.Session = None) -> dict:
    """Fetch a page of decisions from the API."""
    if session is None:
        session = requests.Session()

    # DataTables server-side request format
    data = {
        "start": start,
        "length": length,
        "draw": 1,
        "order[0][column]": "1",  # Order by date
        "order[0][dir]": "desc",  # Most recent first
        "columns[0][data]": "iddok",
        "columns[1][data]": "datum",
        "columns[2][data]": "djelovodni_broj",
        "columns[3][data]": "vrsta_dokumenta",
        "columns[4][data]": "komitent",
        "columns[5][data]": "kljucne_rijeci_tagovi",
        "columns[6][data]": "clan_ustava_cg_atr19",
        "columns[7][data]": "clan_konvencije_atr20",
        "columns[8][data]": "sadrzaj_fajlova",
        "columns[9][data]": "osporeni_akt",
        "columns[10][data]": "datum_sjednice",
    }

    resp = session.post(LIST_ENDPOINT, data=data, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    return resp.json()


def fetch_all(sample: bool = False) -> Iterator[dict]:
    """Fetch all decisions from the database."""
    session = requests.Session()

    # First request to get total count
    result = fetch_page(start=0, length=1, session=session)
    total = result.get("recordsTotal", 0)
    print(f"Total records in database: {total}", file=sys.stderr)

    if sample:
        # For sample, fetch first 15 records
        limit = 15
        print(f"Sample mode: fetching {limit} records", file=sys.stderr)
    else:
        limit = total

    fetched = 0
    while fetched < limit:
        batch_size = min(PAGE_SIZE, limit - fetched)
        print(f"Fetching records {fetched+1} to {fetched+batch_size}...", file=sys.stderr)

        result = fetch_page(start=fetched, length=batch_size, session=session)
        records = result.get("data", [])

        if not records:
            break

        for raw in records:
            yield normalize(raw)

        fetched += len(records)

        if fetched < limit:
            time.sleep(RATE_LIMIT_DELAY)

    print(f"Fetched {fetched} records", file=sys.stderr)


def fetch_updates(since: str) -> Iterator[dict]:
    """Fetch decisions modified since a given date."""
    # Parse the since date
    since_date = datetime.fromisoformat(since.replace("Z", "+00:00"))
    since_str = since_date.strftime("%Y-%m-%d")

    print(f"Fetching updates since {since_str}...", file=sys.stderr)

    session = requests.Session()

    # Fetch in date order (most recent first), stop when we hit older records
    fetched = 0
    start = 0

    while True:
        result = fetch_page(start=start, length=PAGE_SIZE, session=session)
        records = result.get("data", [])

        if not records:
            break

        found_older = False
        for raw in records:
            date_str = raw.get("datum", "") or raw.get("2", "")
            record_date = normalize_date(date_str)

            if record_date and record_date < since_str:
                found_older = True
                break

            yield normalize(raw)
            fetched += 1

        if found_older:
            break

        start += len(records)
        time.sleep(RATE_LIMIT_DELAY)

    print(f"Fetched {fetched} updated records", file=sys.stderr)


def save_samples(records: list, output_dir: Path):
    """Save sample records to JSON files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filename = f"sample_{i+1:03d}.json"
        filepath = output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved {filepath}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Montenegro Constitutional Court case law scraper"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser(
        "bootstrap", help="Run initial data collection"
    )
    bootstrap_parser.add_argument(
        "--sample", action="store_true",
        help="Only fetch a small sample for testing"
    )
    bootstrap_parser.add_argument(
        "--output", type=str, default="sample",
        help="Output directory for sample files"
    )

    # Updates command
    updates_parser = subparsers.add_parser(
        "updates", help="Fetch records since a given date"
    )
    updates_parser.add_argument(
        "--since", type=str, required=True,
        help="ISO date to fetch updates from (e.g., 2024-01-01)"
    )
    updates_parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        records = list(fetch_all(sample=args.sample))

        if args.sample:
            output_dir = Path(__file__).parent / args.output
            save_samples(records, output_dir)

            # Print validation summary
            print("\n=== Validation Summary ===", file=sys.stderr)
            print(f"Records fetched: {len(records)}", file=sys.stderr)

            text_lengths = [len(r.get("text", "")) for r in records]
            with_text = sum(1 for t in text_lengths if t > 100)
            avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0

            print(f"Records with substantial text: {with_text}/{len(records)}", file=sys.stderr)
            print(f"Average text length: {avg_len:.0f} characters", file=sys.stderr)

            if text_lengths:
                print(f"Min text length: {min(text_lengths)}", file=sys.stderr)
                print(f"Max text length: {max(text_lengths)}", file=sys.stderr)
        else:
            # Output records as JSON lines for pipeline
            for record in records:
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        for record in fetch_updates(args.since):
            print(json.dumps(record, ensure_ascii=False))

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
