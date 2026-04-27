#!/usr/bin/env python3
"""
CADA (Commission d'accès aux documents administratifs) Data Fetcher

Fetches administrative opinions on document access requests from the CADA
open data portal at cada.data.gouv.fr.

The CADA is an independent French administrative authority that issues opinions
when citizens are denied access to administrative documents.

Data source:
- https://cada.data.gouv.fr/
- JSON API at /api/search and /api/<id>/
- 60,000+ opinions since 1984

License: Open Licence Etalab
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests

# Constants
API_BASE = "https://cada.data.gouv.fr"
SEARCH_ENDPOINT = f"{API_BASE}/api/search"
RATE_LIMIT_DELAY = 0.3  # seconds between API calls
PAGE_SIZE = 100  # API supports up to 100


def clean_text(text: str) -> str:
    """Clean up opinion text."""
    if not text:
        return ""
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse CADA date string to ISO format (YYYY-MM-DD)."""
    if not date_str:
        return None

    # Handle RFC 2822 format: "Thu, 15 Dec 2016 00:00:00 GMT"
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        return dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass

    # Try ISO format
    if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]

    return None


def normalize(raw: dict) -> dict:
    """Transform raw CADA opinion data into normalized schema."""
    opinion_id = raw.get('id', '')

    # Parse session date
    session_date = parse_date(raw.get('session', ''))

    # Get full text content
    text = clean_text(raw.get('content', ''))

    # Get subject/request description
    subject = raw.get('subject', '')

    # Get administration name
    administration = raw.get('administration', '')

    # Get meanings (favorable, defavorable, etc.)
    meanings = raw.get('meanings', [])

    # Get topics/themes
    topics = raw.get('topics', [])

    # Get tags
    tags = raw.get('tags', [])

    # Build title from subject or administration
    title = subject if subject else f"Avis {opinion_id} - {administration}"
    if len(title) > 200:
        title = title[:197] + "..."

    # Build URL
    url = f"https://cada.data.gouv.fr/{opinion_id}/"

    # Build normalized record
    return {
        '_id': f"FR/CADA/{opinion_id}",
        '_source': 'FR/CADA',
        '_type': 'doctrine',  # CADA issues opinions/doctrine, not binding decisions
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'opinion_id': opinion_id,
        'title': title,
        'date': session_date,
        'url': url,
        'text': text,
        'subject': subject,
        'administration': administration,
        'meanings': meanings,
        'topics': topics,
        'tags': tags,
        'part': raw.get('part'),  # Part of session (1-4)
        'type': raw.get('type', 'Avis'),
    }


def fetch_page(page: int = 1, page_size: int = PAGE_SIZE, **filters) -> dict:
    """Fetch a single page of results from the CADA API."""
    params = {
        'page': page,
        'page_size': page_size,
    }
    params.update(filters)

    response = requests.get(SEARCH_ENDPOINT, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def _get_session_dates() -> list[str]:
    """
    Discover all unique session dates by combining:
    1. Session dates from first 10K records (sorted by session asc)
    2. Session dates from last 10K records (sorted by session desc)
    3. Enumeration of gap years where we don't have coverage

    The API returns HTTP 500 beyond page 101, so we cannot paginate past 10K records.
    But by sorting ascending/descending we can get dates from both ends, then
    fill in the middle by querying individual dates.
    """
    from email.utils import parsedate_to_datetime
    from datetime import timedelta

    all_dates: set[str] = set()

    # Step 1: Collect dates from first 10K (oldest records)
    print("Collecting session dates from first 10K records (session asc)...", file=sys.stderr)
    for page in range(1, 101):
        try:
            data = fetch_page(page=page, page_size=PAGE_SIZE, sort='session asc')
        except requests.RequestException:
            break
        advices = data.get('advices', [])
        if not advices:
            break
        for advice in advices:
            session = advice.get('session')
            if session:
                try:
                    dt = parsedate_to_datetime(session)
                    all_dates.add(dt.strftime('%Y-%m-%d'))
                except (ValueError, TypeError):
                    pass
        time.sleep(0.05)

    asc_count = len(all_dates)
    print(f"  Found {asc_count} unique dates from first 10K", file=sys.stderr)

    # Step 2: Collect dates from last 10K (newest records)
    print("Collecting session dates from last 10K records (session desc)...", file=sys.stderr)
    for page in range(1, 101):
        try:
            data = fetch_page(page=page, page_size=PAGE_SIZE, sort='session desc')
        except requests.RequestException:
            break
        advices = data.get('advices', [])
        if not advices:
            break
        for advice in advices:
            session = advice.get('session')
            if session:
                try:
                    dt = parsedate_to_datetime(session)
                    all_dates.add(dt.strftime('%Y-%m-%d'))
                except (ValueError, TypeError):
                    pass
        time.sleep(0.05)

    desc_count = len(all_dates) - asc_count
    print(f"  Found {desc_count} new dates from last 10K", file=sys.stderr)

    # Step 3: Find gaps in coverage and enumerate missing years
    sorted_dates = sorted(all_dates)
    if sorted_dates:
        # Find years with no coverage
        years_covered = {int(d[:4]) for d in sorted_dates}
        min_year = min(years_covered)
        max_year = max(years_covered)

        gap_years = []
        for year in range(min_year, max_year + 1):
            if year not in years_covered:
                gap_years.append(year)

        if gap_years:
            print(f"Filling in gap years: {gap_years}", file=sys.stderr)
            for year in gap_years:
                # Enumerate all days in the gap year and check which have data
                start = datetime(year, 1, 1)
                end = datetime(year, 12, 31)
                current = start
                year_found = 0
                while current <= end:
                    date_str = current.strftime('%Y-%m-%d')
                    try:
                        data = fetch_page(page=1, page_size=1, session=date_str)
                        if data.get('total', 0) > 0:
                            all_dates.add(date_str)
                            year_found += 1
                    except requests.RequestException:
                        pass
                    current += timedelta(days=1)
                    time.sleep(0.02)  # Fast rate for discovery
                print(f"  {year}: {year_found} sessions discovered", file=sys.stderr)

    final_dates = sorted(all_dates)
    print(f"Total: {len(final_dates)} unique session dates discovered", file=sys.stderr)
    return final_dates


def _fetch_session(session_date: str) -> Generator[dict, None, None]:
    """Fetch all opinions for a single session date, paginating within it."""
    page = 1
    fetched = 0

    while True:
        try:
            data = fetch_page(page=page, page_size=PAGE_SIZE, session=session_date)
        except requests.RequestException as e:
            print(f"  Error on session={session_date} page {page}: {e}", file=sys.stderr)
            break

        advices = data.get('advices', [])
        if not advices:
            break

        for raw in advices:
            yield normalize(raw)
            fetched += 1

        total = data.get('total', 0)
        if fetched >= total:
            break

        page += 1
        time.sleep(RATE_LIMIT_DELAY)


def fetch_all() -> Generator[dict, None, None]:
    """
    Fetch all CADA opinions using session-date windowing.

    The API returns HTTP 500 beyond page 101 (~10K records).  To work
    around this, we iterate over every session date (via the facets
    endpoint) and paginate within each session.  Each session typically
    has only a few hundred opinions, so pagination never hits the cap.
    """
    session_dates = _get_session_dates()
    if not session_dates:
        print("No session dates found — falling back to simple pagination", file=sys.stderr)
        # Fallback: simple pagination up to page 100
        page = 1
        while page <= 100:
            try:
                data = fetch_page(page=page, page_size=PAGE_SIZE)
            except requests.RequestException:
                break
            advices = data.get('advices', [])
            if not advices:
                break
            for raw in advices:
                yield normalize(raw)
            page += 1
            time.sleep(RATE_LIMIT_DELAY)
        return

    total_fetched = 0
    for i, session_date in enumerate(session_dates):
        before = total_fetched
        for doc in _fetch_session(session_date):
            yield doc
            total_fetched += 1

        session_count = total_fetched - before
        if session_count > 0 and (i + 1) % 50 == 0:
            print(
                f"  Sessions processed: {i+1}/{len(session_dates)} — "
                f"total opinions: {total_fetched:,}",
                file=sys.stderr,
            )
        time.sleep(RATE_LIMIT_DELAY)

    print(f"\nTotal fetched: {total_fetched:,} opinions across {len(session_dates)} sessions", file=sys.stderr)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch opinions from sessions after a given date."""
    # CADA API supports session date filtering
    # Sort by session descending to get most recent first
    page = 1

    while True:
        print(f"Fetching updates page {page}...", file=sys.stderr)

        try:
            data = fetch_page(page=page, page_size=PAGE_SIZE, sort='session desc')
        except requests.RequestException as e:
            print(f"Error fetching page {page}: {e}", file=sys.stderr)
            break

        advices = data.get('advices', [])
        if not advices:
            break

        found_old = False
        for raw in advices:
            doc = normalize(raw)
            doc_date = doc.get('date')

            if doc_date:
                try:
                    doc_dt = datetime.fromisoformat(doc_date)
                    if doc_dt.replace(tzinfo=timezone.utc) < since.replace(tzinfo=timezone.utc):
                        found_old = True
                        continue
                except ValueError:
                    pass

            yield doc

        # If we found old records, we can stop
        if found_old:
            break

        page += 1
        time.sleep(RATE_LIMIT_DELAY)


def bootstrap_sample(limit: int = 15) -> None:
    """Fetch sample records for testing."""
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)

    # Clear existing samples
    for f in sample_dir.glob('*.json'):
        f.unlink()

    print("Fetching CADA sample data...", file=sys.stderr)

    # Get diverse sample from recent sessions
    # Sort by session descending to get recent opinions
    samples = []
    topic_counts = {}

    page = 1
    while len(samples) < limit:
        print(f"Fetching page {page}...", file=sys.stderr)

        data = fetch_page(page=page, page_size=50, sort='session desc')
        advices = data.get('advices', [])

        if not advices:
            break

        for raw in advices:
            if len(samples) >= limit:
                break

            text = raw.get('content', '')

            # Skip if no or very short text
            if not text or len(text) < 200:
                continue

            # Get topic for diversity
            topics = raw.get('topics', ['Other'])
            topic = topics[0] if topics else 'Other'

            # Limit per topic to ensure diversity
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
            if topic_counts[topic] > (limit // 4 + 1):
                continue

            samples.append(raw)

        page += 1
        time.sleep(RATE_LIMIT_DELAY)

        if page > 10:  # Safety limit
            break

    # Normalize and save
    count = 0
    total_chars = 0

    for raw in samples:
        doc = normalize(raw)

        # Save to sample directory
        safe_id = doc['opinion_id'].replace('/', '-').replace('\\', '-')
        filename = f"{safe_id}.json"
        filepath = sample_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        text_len = len(doc.get('text', ''))
        total_chars += text_len
        print(f"Saved {filename} ({text_len:,} chars)", file=sys.stderr)
        count += 1

    avg_chars = total_chars // count if count > 0 else 0
    print(f"\nSaved {count} sample records to {sample_dir}", file=sys.stderr)
    print(f"Average text length: {avg_chars:,} chars", file=sys.stderr)

    # Print topic breakdown
    print("\nTopic breakdown:", file=sys.stderr)
    for topic, c in sorted(topic_counts.items(), key=lambda x: -x[1]):
        if c > 0:
            print(f"  - {topic}: {c}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description='CADA (Commission d\'accès aux documents administratifs) Data Fetcher')
    subparsers = parser.add_subparsers(dest='command')

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser('bootstrap', help='Fetch sample data')
    bootstrap_parser.add_argument('--sample', action='store_true', help='Fetch sample records')
    bootstrap_parser.add_argument('--limit', type=int, default=15, help='Number of records to fetch')
    bootstrap_parser.add_argument("--full", action="store_true", help="Fetch all records")

    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show dataset statistics')

    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(args.limit)
        else:
            # Full bootstrap — stream all opinions to stdout as JSONL
            data_dir = Path(__file__).parent / 'data'
            data_dir.mkdir(parents=True, exist_ok=True)
            jsonl_path = data_dir / 'records.jsonl'

            count = 0
            with open(jsonl_path, 'w', encoding='utf-8') as out:
                for doc in fetch_all():
                    line = json.dumps(doc, ensure_ascii=False)
                    out.write(line + '\n')
                    print(line)
                    count += 1

            print(f"\nBootstrap complete: {count} records written to {jsonl_path}", file=sys.stderr)
    elif args.command == 'stats':
        data = fetch_page(page=1, page_size=1)
        total = data.get('total', 0)
        facets = data.get('facets', {})

        print(f"Total opinions: {total:,}")

        # Top administrations
        if 'administration' in facets:
            print("\nTop 10 administrations:")
            for admin, count, _ in facets['administration'][:10]:
                print(f"  {admin}: {count:,}")

        # Meaning breakdown
        if 'meaning' in facets:
            print("\nBy meaning (outcome):")
            for meaning, count, _ in facets['meaning'][:10]:
                print(f"  {meaning}: {count:,}")

        # Topic breakdown
        if 'topic' in facets:
            print("\nTop 10 topics:")
            for topic, count, _ in facets['topic'][:10]:
                print(f"  {topic}: {count:,}")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
