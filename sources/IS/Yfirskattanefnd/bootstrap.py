#!/usr/bin/env python3
"""
IS/Yfirskattanefnd - Icelandic Tax Appeals Board Rulings Fetcher

Fetches rulings from the Yfirskattanefnd (Internal Revenue Board),
Iceland's supreme administrative appeals authority for tax matters.

Data source: https://yskn.is/urskurdir/
License: Public Domain (official government decisions)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://yskn.is"
LISTING_URL = f"{BASE_URL}/urskurdir/"
RULING_URL = f"{BASE_URL}/urskurdir/skoda-urskurd/"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/Yfirskattanefnd"

YEAR_START = 1973
YEAR_END = datetime.now().year

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0"
}
REQUEST_DELAY = 2.0


MONTH_MAP = {
    'janúar': '01', 'febrúar': '02', 'mars': '03', 'apríl': '04',
    'maí': '05', 'júní': '06', 'júlí': '07', 'ágúst': '08',
    'september': '09', 'október': '10', 'nóvember': '11', 'desember': '12'
}


def parse_icelandic_date(text: str) -> Optional[str]:
    """Extract date from Icelandic text like 'föstudaginn 10. apríl 2026'."""
    match = re.search(r'(\d{1,2})\.\s*(\w+)\s+(\d{4})', text)
    if match:
        day = match.group(1).zfill(2)
        month_name = match.group(2).lower()
        year = match.group(3)
        month = MONTH_MAP.get(month_name)
        if month:
            return f"{year}-{month}-{day}"
    return None


def get_ruling_ids_for_year(year: int, session: requests.Session) -> list[tuple[int, str]]:
    """Fetch the listing page for a year and return list of (internal_id, ruling_number) tuples."""
    try:
        resp = session.get(LISTING_URL, params={"year": year}, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching year {year}: {e}")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    results = []

    for link in soup.find_all('a', href=True):
        href = link['href']
        nr_match = re.search(r'nr=(\d+)', href)
        if nr_match and 'skoda-urskurd' in href:
            internal_id = int(nr_match.group(1))
            link_text = link.get_text(strip=True)
            # Extract ruling number like "202/2025"
            num_match = re.search(r'(\d+/\d{4})', link_text)
            ruling_number = num_match.group(1) if num_match else f"{internal_id}"
            results.append((internal_id, ruling_number))

    return results


def fetch_ruling(internal_id: int, session: requests.Session) -> Optional[dict]:
    """Fetch and parse a single ruling via AJAX endpoint."""
    try:
        resp = session.get(
            RULING_URL,
            params={"nr": internal_id, "altTemplate": "SkodaurskurdAjax"},
            timeout=30
        )
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error fetching ruling {internal_id}: {e}")
        return None

    return parse_ruling_html(resp.text, internal_id)


def parse_ruling_html(html: str, internal_id: int) -> Optional[dict]:
    """Parse ruling HTML and extract structured data."""
    soup = BeautifulSoup(html, 'html.parser')

    # Extract ruling number from h2
    ruling_number = None
    h2 = soup.find('h2')
    if h2:
        h2_text = h2.get_text(strip=True)
        num_match = re.search(r'(\d+/\d{4})', h2_text)
        if num_match:
            ruling_number = num_match.group(1)

    # Extract tax years (e.g., "Gjaldár 2017-2022")
    tax_years = None
    tax_match = re.search(r'Gjaldár\s+([\d\-–, ]+)', soup.get_text())
    if tax_match:
        tax_years = tax_match.group(1).strip()

    # Extract keywords from initial tag-like elements
    keywords = []
    # Keywords appear as text before the ruling number, often in specific elements
    for elem in soup.find_all(['span', 'div', 'a']):
        text = elem.get_text(strip=True)
        # Short keyword-like items before the main content
        if text and 5 < len(text) < 80 and not re.search(r'\d{4}', text):
            parent_text = elem.parent.get_text(strip=True) if elem.parent else ""
            if len(parent_text) < 200:
                keywords.append(text)
        if len(keywords) >= 10:
            break

    # Extract date from body text
    body_text = soup.get_text()
    date = None
    date_match = re.search(
        r'(?:Ár|ár)\s+(\d{4}),?\s+\w+\s+(\d{1,2})\.\s+(\w+)',
        body_text
    )
    if date_match:
        year = date_match.group(1)
        day = date_match.group(2).zfill(2)
        month_name = date_match.group(3).lower()
        month = MONTH_MAP.get(month_name)
        if month:
            date = f"{year}-{month}-{day}"

    if not date:
        date = parse_icelandic_date(body_text)

    if not date and ruling_number:
        # Fallback: extract year from ruling number
        year_match = re.search(r'/(\d{4})', ruling_number)
        if year_match:
            date = f"{year_match.group(1)}-01-01"

    # Extract full text — clean HTML
    # Remove script/style
    for element in soup(['script', 'style']):
        element.decompose()

    full_text = soup.get_text(separator='\n', strip=True)
    # Clean up whitespace
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)
    full_text = full_text.strip()

    if len(full_text) < 100:
        return None

    # Build title
    title = f"Úrskurður nr. {ruling_number}" if ruling_number else f"Úrskurður (ID {internal_id})"

    doc_id = ruling_number if ruling_number else str(internal_id)
    url = f"{RULING_URL}?nr={internal_id}"

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.utcnow().isoformat() + 'Z',
        'title': title,
        'text': full_text,
        'date': date,
        'url': url,
        'language': 'isl',
        'ruling_number': ruling_number,
        'internal_id': internal_id,
        'tax_years': tax_years,
        'keywords': keywords[:10] if keywords else [],
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all rulings year-by-year with checkpoint support."""
    checkpoint_file = Path(__file__).parent / ".checkpoint"
    completed_ids = set()

    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r') as f:
                completed_ids = set(int(line.strip()) for line in f if line.strip())
            print(f"Loaded checkpoint: {len(completed_ids)} already processed")
        except Exception as e:
            print(f"Warning: Could not load checkpoint: {e}")

    session = requests.Session()
    session.headers.update(HEADERS)

    count = 0
    for year in range(YEAR_END, YEAR_START - 1, -1):
        if max_records and count >= max_records:
            break

        print(f"  Year {year}...")
        entries = get_ruling_ids_for_year(year, session)
        time.sleep(REQUEST_DELAY)

        if not entries:
            continue

        print(f"    Found {len(entries)} rulings")

        for internal_id, ruling_number in entries:
            if max_records and count >= max_records:
                break

            if internal_id in completed_ids:
                continue

            record = fetch_ruling(internal_id, session)
            time.sleep(REQUEST_DELAY)

            if record and len(record.get('text', '')) >= 100:
                yield record
                count += 1

                if not max_records:
                    try:
                        with open(checkpoint_file, 'a') as f:
                            f.write(f"{internal_id}\n")
                    except Exception:
                        pass

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch rulings updated since a given date."""
    for record in fetch_all():
        if record.get('date'):
            try:
                doc_date = datetime.fromisoformat(record['date'])
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def normalize(raw: dict) -> dict:
    """Validate and normalize the record."""
    required = ['_id', '_source', '_type', '_fetched_at', 'title', 'text', 'date', 'url']
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    if not raw.get('text') or len(raw['text']) < 50:
        raise ValueError("Document has insufficient text content")

    return raw


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []
    for i, record in enumerate(fetch_all(max_records=sample_count)):
        try:
            normalized = normalize(record)
            records.append(normalized)

            filename = SAMPLE_DIR / f"record_{i+1:03d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            text_len = len(normalized.get('text', ''))
            print(f"  [{i+1:02d}] {normalized['_id']}: {normalized['title'][:50]} ({text_len:,} chars)")

        except ValueError as e:
            print(f"  Skipping record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get('text'))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="IS/Yfirskattanefnd tax appeals fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'info'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=15,
                       help="Number of sample records to fetch")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'info':
        print(f"{SOURCE_ID} - Icelandic Tax Appeals Board Rulings")
        print(f"Source URL: {BASE_URL}")
        print(f"Rulings URL: {LISTING_URL}")

    elif args.command == 'bootstrap':
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
