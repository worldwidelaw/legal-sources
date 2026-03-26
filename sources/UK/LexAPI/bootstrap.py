#!/usr/bin/env python3
"""
UK Lex API — UK legislation via the i.AI Government Lab REST API.

Fetches full text of UK legislation (Acts, Statutory Instruments, etc.)
from the Lex API built by i.AI (UK Government's AI incubator).

Data source: https://lex.lab.i.ai.gov.uk
License: Open Government Licence v3.0
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

SOURCE_ID = "UK/LexAPI"
BASE_URL = "https://lex.lab.i.ai.gov.uk"
RATE_LIMIT = 1.0

# Major UK legislation types to enumerate
LEGISLATION_TYPES = [
    "ukpga",   # UK Public General Acts
    "uksi",    # UK Statutory Instruments
    "asp",     # Acts of the Scottish Parliament
    "asc",     # Acts of Senedd Cymru (Welsh)
    "anaw",    # Acts of the National Assembly for Wales (pre-2020)
    "wsi",     # Wales Statutory Instruments
    "ssi",     # Scottish Statutory Instruments
    "nia",     # Northern Ireland Acts
    "nisr",    # Northern Ireland Statutory Rules
    "eur",     # EU Retained legislation
    "eudr",    # EU Retained Directives
    "eudn",    # EU Retained Decisions
    "ukla",    # UK Local Acts
    "ukmo",    # UK Ministerial Orders
]

# Year ranges by type (approximate starts)
TYPE_YEAR_RANGES = {
    "ukpga": (1801, 2025),
    "uksi": (1948, 2025),
    "asp": (1999, 2025),
    "asc": (2020, 2025),
    "anaw": (2012, 2020),
    "wsi": (1999, 2025),
    "ssi": (1999, 2025),
    "nia": (2000, 2025),
    "nisr": (2000, 2025),
    "eur": (1958, 2025),
    "eudr": (1977, 2025),
    "eudn": (1980, 2025),
    "ukla": (1800, 2025),
    "ukmo": (1992, 2025),
}


def api_post(endpoint: str, body: dict, timeout: int = 30) -> Optional[dict]:
    """Make a POST request to the Lex API."""
    url = f"{BASE_URL}{endpoint}"
    data = json.dumps(body).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'User-Agent': 'Legal Data Hunter/1.0 (EU Legal Research)',
    })
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        if e.code == 404 or e.code == 422:
            return None
        print(f"  API error {e.code} for {endpoint}: {e.reason}")
        return None
    except Exception as e:
        print(f"  Request error for {endpoint}: {e}")
        return None


def api_get(endpoint: str, timeout: int = 30) -> Optional[dict]:
    """Make a GET request to the Lex API."""
    url = f"{BASE_URL}{endpoint}"
    req = urllib.request.Request(url, headers={
        'Accept': 'application/json',
        'User-Agent': 'Legal Data Hunter/1.0 (EU Legal Research)',
    })
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        print(f"  GET error for {endpoint}: {e}")
        return None


def lookup_legislation(leg_type: str, year: int, number: int) -> Optional[dict]:
    """Look up a specific piece of legislation by type/year/number."""
    return api_post('/legislation/lookup', {
        'legislation_type': leg_type,
        'year': year,
        'number': number,
    })


def get_full_text(legislation_id: str) -> Optional[str]:
    """Get full text of a legislation document."""
    result = api_post('/legislation/text', {
        'legislation_id': legislation_id,
        'include_schedules': True,
    }, timeout=60)
    if result and result.get('full_text'):
        return result['full_text']
    return None


def normalize(raw: dict, full_text: str) -> dict:
    """Transform raw legislation data into standard schema."""
    leg_type = raw.get('type', '')
    year = raw.get('year', 0)
    number = raw.get('number', 0)
    leg_id = f"{leg_type}/{year}/{number}"
    doc_id = f"UK_LEX_{leg_type}_{year}_{number}"

    title = raw.get('title', '')
    date = raw.get('enactment_date') or (f"{year}-01-01" if year else None)

    uri = raw.get('uri', '')
    url = uri.replace('http://www.legislation.gov.uk/id/', 'https://www.legislation.gov.uk/') if uri else ''

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": url,
        "legislation_id": leg_id,
        "legislation_type": leg_type,
        "year": year,
        "number": number,
        "category": raw.get('category', ''),
        "status": raw.get('status', ''),
        "extent": raw.get('extent', []),
        "description": raw.get('description', ''),
        "number_of_provisions": raw.get('number_of_provisions', 0),
    }


def enumerate_year(leg_type: str, year: int, max_number: int = 5000) -> Iterator[dict]:
    """Enumerate all legislation of a given type for a given year."""
    consecutive_misses = 0
    max_consecutive_misses = 10  # Stop after 10 consecutive misses

    for number in range(1, max_number + 1):
        meta = lookup_legislation(leg_type, year, number)
        if meta is None:
            consecutive_misses += 1
            if consecutive_misses >= max_consecutive_misses:
                break
            continue

        consecutive_misses = 0
        yield meta
        time.sleep(0.3)  # Light rate limit for metadata lookups


def fetch_all() -> Iterator[dict]:
    """Fetch all available legislation with full text."""
    for leg_type in LEGISLATION_TYPES:
        year_start, year_end = TYPE_YEAR_RANGES.get(leg_type, (2000, 2025))
        print(f"\n=== Type: {leg_type} ({year_start}-{year_end}) ===")

        for year in range(year_end, year_start - 1, -1):  # Newest first
            print(f"  Year: {year}")
            count = 0

            for meta in enumerate_year(leg_type, year):
                leg_id = f"{leg_type}/{year}/{meta.get('number', 0)}"
                text = get_full_text(leg_id)
                if text and len(text) >= 50:
                    record = normalize(meta, text)
                    yield record
                    count += 1
                else:
                    print(f"    No text for {leg_id}")
                time.sleep(RATE_LIMIT)

            if count > 0:
                print(f"    → {count} documents fetched")


def fetch_updates(since: datetime) -> Iterator[dict]:
    """Fetch legislation modified since a given date."""
    since_year = since.year
    current_year = datetime.now().year

    for leg_type in LEGISLATION_TYPES:
        for year in range(current_year, since_year - 1, -1):
            for meta in enumerate_year(leg_type, year):
                modified = meta.get('modified_date', '')
                if modified and modified >= since.isoformat()[:10]:
                    leg_id = f"{leg_type}/{year}/{meta.get('number', 0)}"
                    text = get_full_text(leg_id)
                    if text and len(text) >= 50:
                        yield normalize(meta, text)
                    time.sleep(RATE_LIMIT)


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    records_saved = 0
    total_text_chars = 0
    types_sampled = set()

    # Sample from diverse legislation types and years
    sample_targets = [
        ("ukpga", 2024, 1),   # Recent Act
        ("ukpga", 2024, 13),  # Digital Markets Act
        ("ukpga", 2018, 12),  # Data Protection Act
        ("ukpga", 2010, 15),  # Equality Act
        ("ukpga", 2000, 36),  # Freedom of Information Act
        ("uksi", 2024, 500),  # Recent SI
        ("uksi", 2018, 506),  # GDPR SI
        ("uksi", 2023, 100),  # 2023 SI
        ("asp", 2024, 1),     # Scottish Act
        ("asp", 2023, 1),     # Scottish Act
        ("asc", 2024, 1),     # Welsh Act
        ("nia", 2023, 1),     # NI Act
        ("eur", 2016, 679),   # GDPR (retained)
        ("ukpga", 2023, 1),   # 2023 Act
        ("ukpga", 2022, 1),   # 2022 Act
    ]

    for leg_type, year, number in sample_targets:
        if records_saved >= count:
            break

        leg_id = f"{leg_type}/{year}/{number}"
        print(f"\n  Looking up {leg_id}...")

        meta = lookup_legislation(leg_type, year, number)
        if not meta:
            print(f"    Not found, skipping")
            time.sleep(0.5)
            continue

        print(f"    Title: {meta.get('title', '')[:70]}")

        text = get_full_text(leg_id)
        if not text or len(text) < 50:
            print(f"    No full text available, skipping")
            time.sleep(0.5)
            continue

        record = normalize(meta, text)
        text_len = len(record.get('text', ''))
        total_text_chars += text_len

        filename = f"record_{records_saved:04d}.json"
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    Saved: {filename}")
        print(f"    Type: {leg_type}, Year: {year}, Number: {number}")
        print(f"    Text: {text_len:,} chars")
        types_sampled.add(leg_type)
        records_saved += 1

        time.sleep(RATE_LIMIT)

    # Print summary
    print("\n" + "=" * 60)
    print("SAMPLE SUMMARY")
    print("=" * 60)
    print(f"Records saved: {records_saved}")
    print(f"Types sampled: {len(types_sampled)} ({', '.join(sorted(types_sampled))})")
    if records_saved > 0:
        avg_chars = total_text_chars // records_saved
        print(f"Total text chars: {total_text_chars:,}")
        print(f"Average text length: {avg_chars:,} chars/doc")
    print(f"Sample directory: {sample_dir}")

    if records_saved >= 10:
        print("\n✓ SUCCESS: 10+ sample records with full text")
    else:
        print(f"\n✗ WARNING: Only {records_saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(
        description="UK Lex API Legislation Fetcher"
    )
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Running full bootstrap...")
            records_saved = 0
            for record in fetch_all():
                safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', record['_id'])[:100]
                filepath = sample_dir / f"{safe_id}.json"
                filepath.parent.mkdir(parents=True, exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                records_saved += 1
                if records_saved % 100 == 0:
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
