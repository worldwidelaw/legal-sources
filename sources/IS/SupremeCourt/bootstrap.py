#!/usr/bin/env python3
"""
IS/SupremeCourt - Icelandic Supreme Court (Hæstiréttur) Case Law Fetcher

Fetches court decisions from the Icelandic Supreme Court website.

Data source: https://www.haestirettur.is/domar/
License: Public Domain (official court decisions)
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

BASE_URL = "https://www.haestirettur.is"
DOMAR_URL = f"{BASE_URL}/domar/"
DOMUR_URL = f"{BASE_URL}/domar/_domur/"
PAGINATION_URL = f"{BASE_URL}/default.aspx"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/SupremeCourt"

# Pagination settings - discovered from site JavaScript
# The moreVer button uses pageitemid parameter for AJAX loading
PAGEITEM_ID = "4468cca6-a82f-11e5-9402-005056bc2afe"
PAGE_SIZE = 50  # Items per request

# Request settings
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0"
}
REQUEST_DELAY = 1.5  # Seconds between requests


def extract_case_ids_from_listing(html: str) -> list[str]:
    """Extract case UUID IDs from the domar listing page."""
    # Pattern: /domar/_domur/?id=UUID
    pattern = r'id=([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})'
    return list(set(re.findall(pattern, html)))


def parse_date(date_str: str) -> Optional[str]:
    """Parse Icelandic date string to ISO format."""
    # Example: "Fimmtudaginn 5. febrúar 2026"
    month_map = {
        'janúar': '01', 'febrúar': '02', 'mars': '03', 'apríl': '04',
        'maí': '05', 'júní': '06', 'júlí': '07', 'ágúst': '08',
        'september': '09', 'október': '10', 'nóvember': '11', 'desember': '12'
    }

    try:
        # Try to extract date components
        match = re.search(r'(\d{1,2})\.\s*(\w+)\s*(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month_name = match.group(2).lower()
            year = match.group(3)
            month = month_map.get(month_name)
            if month:
                return f"{year}-{month}-{day}"
    except Exception:
        pass

    return None


def extract_text_from_html(soup: BeautifulSoup) -> str:
    """Extract clean text from the decision body."""
    # Find the verdict body
    body = soup.find('div', class_='verdict__body')
    if not body:
        body = soup.find('div', class_='verdict')

    if not body:
        return ""

    # Remove script and style elements
    for element in body(['script', 'style']):
        element.decompose()

    # Get text, preserving structure
    text = body.get_text(separator='\n', strip=True)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def parse_decision(html: str, case_id: str) -> Optional[dict]:
    """Parse a court decision HTML page and extract metadata and text."""
    try:
        soup = BeautifulSoup(html, 'html.parser')

        # Extract case number from h2 subtitle
        case_number = None
        subtitle = soup.find('h2', class_='verdict-head__subtitle')
        if subtitle:
            case_text = subtitle.get_text(strip=True)
            match = re.search(r'(\d+/\d{4})', case_text)
            if match:
                case_number = match.group(1)

        # Extract date
        date = None
        time_elem = soup.find('time', class_='verdict-head__time')
        if time_elem:
            date_str = time_elem.get_text(strip=True)
            date = parse_date(date_str)
            if not date and time_elem.get('datetime'):
                # Try datetime attribute format "5.2.2026 00:00:00"
                dt_str = time_elem.get('datetime')
                match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', dt_str)
                if match:
                    day = match.group(1).zfill(2)
                    month = match.group(2).zfill(2)
                    year = match.group(3)
                    date = f"{year}-{month}-{day}"

        # Extract parties
        appellants = []
        plaintiffs = []

        appellant_div = soup.find('div', class_='appelants')
        if appellant_div:
            appellants = [s.get_text(strip=True) for s in appellant_div.find_all('strong')]

        plaintiff_div = soup.find('div', class_='plaintiffs')
        if plaintiff_div:
            plaintiffs = [s.get_text(strip=True) for s in plaintiff_div.find_all('strong')]

        # Extract keywords
        keywords = []
        keyword_section = soup.find('div', class_='verdict__keywords')
        if keyword_section:
            keywords = [li.get_text(strip=True) for li in keyword_section.find_all('li')]

        # Extract summary (reifun)
        summary = None
        reifun_section = soup.find('div', class_='verdict__reifun')
        if reifun_section:
            summary_div = reifun_section.find('div', class_='text-justify')
            if summary_div:
                summary = summary_div.get_text(strip=True)

        # Extract full text
        text = extract_text_from_html(soup)

        # Combine summary and body text
        full_text = ""
        if summary:
            full_text = f"REIFUN (Summary):\n{summary}\n\n"
        if text:
            full_text += f"DÓMUR (Judgment):\n{text}"

        if not full_text or len(full_text) < 100:
            return None

        # Build title
        title_parts = []
        if case_number:
            title_parts.append(f"Mál nr. {case_number}")
        if appellants and plaintiffs:
            title_parts.append(f"{appellants[0]} gegn {plaintiffs[0]}")
        elif appellants:
            title_parts.append(appellants[0])

        title = " - ".join(title_parts) if title_parts else f"Hæstiréttur {case_id[:8]}"

        # Build document ID
        doc_id = case_number if case_number else case_id

        url = f"{DOMUR_URL}?id={case_id}"

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
            'court': 'Hæstiréttur Íslands',
            'case_number': case_number,
            'uuid': case_id,
            'keywords': keywords,
            'appellants': appellants,
            'plaintiffs': plaintiffs,
            'summary': summary,
        }
    except Exception as e:
        print(f"  Error parsing decision {case_id}: {e}")
        return None


def fetch_decision(case_id: str) -> Optional[dict]:
    """Fetch and parse a single court decision."""
    url = f"{DOMUR_URL}?id={case_id}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return parse_decision(resp.text, case_id)
    except requests.RequestException as e:
        print(f"  Error fetching {case_id}: {e}")
        return None


def get_all_case_ids(max_ids: int = None) -> list[str]:
    """
    Get all available case IDs using AJAX pagination.

    The Icelandic Supreme Court website uses AJAX pagination via
    /default.aspx?pageitemid=<id>&offset=<offset>&count=<count>

    Args:
        max_ids: Maximum number of IDs to return (for sampling/testing)

    Returns:
        List of unique case UUIDs
    """
    all_ids = set()
    offset = 0
    consecutive_empty = 0

    print(f"Fetching case listings from {DOMAR_URL}...")

    # First, get IDs from the main page (first 10 decisions shown by default)
    try:
        resp = requests.get(DOMAR_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        ids = extract_case_ids_from_listing(resp.text)
        all_ids.update(ids)
        print(f"  Found {len(ids)} cases on main listing")
    except requests.RequestException as e:
        print(f"  Error fetching main listing: {e}")

    # Now paginate through all decisions using AJAX endpoint
    print(f"  Paginating through archive (batch size: {PAGE_SIZE})...")

    while True:
        if max_ids and len(all_ids) >= max_ids:
            print(f"  Reached max_ids limit ({max_ids})")
            break

        try:
            params = {
                'pageitemid': PAGEITEM_ID,
                'offset': offset,
                'count': PAGE_SIZE
            }
            resp = requests.get(PAGINATION_URL, params=params, headers=HEADERS, timeout=30)
            resp.raise_for_status()

            ids = extract_case_ids_from_listing(resp.text)
            new_ids = [id for id in ids if id not in all_ids]

            if not new_ids:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    print(f"  No new IDs found for 3 consecutive pages, stopping at offset {offset}")
                    break
            else:
                consecutive_empty = 0
                all_ids.update(new_ids)

            if offset % 500 == 0:
                print(f"    Offset {offset}: {len(all_ids)} unique IDs collected")

            offset += PAGE_SIZE
            time.sleep(0.5)  # Brief delay between pagination requests

            # Safety limit - should be ~12K decisions
            if offset > 15000:
                print(f"  Safety limit reached at offset {offset}")
                break

        except requests.RequestException as e:
            print(f"  Error at offset {offset}: {e}")
            consecutive_empty += 1
            if consecutive_empty >= 3:
                break
            offset += PAGE_SIZE
            time.sleep(1)

    result = list(all_ids)
    print(f"  Total unique case IDs discovered: {len(result)}")
    return result


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """
    Fetch all court decisions with checkpoint/resume support.

    Args:
        max_records: Maximum number of records to yield (for sampling)

    Yields:
        Normalized document records
    """
    checkpoint_file = Path(__file__).parent / ".checkpoint"
    completed_ids = set()

    # Load checkpoint if exists
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r') as f:
                completed_ids = set(line.strip() for line in f if line.strip())
            print(f"Loaded checkpoint: {len(completed_ids)} already processed")
        except Exception as e:
            print(f"Warning: Could not load checkpoint: {e}")

    # Get all case IDs (for sample mode, limit discovery)
    if max_records:
        case_ids = get_all_case_ids(max_ids=max_records + 20)
    else:
        case_ids = get_all_case_ids()

    # Filter out already completed
    pending_ids = [id for id in case_ids if id not in completed_ids]
    if max_records:
        pending_ids = pending_ids[:max_records + 5]

    print(f"Processing {len(pending_ids)} pending cases (of {len(case_ids)} total)...")

    count = 0
    for i, case_id in enumerate(pending_ids):
        if max_records and count >= max_records:
            break

        print(f"  [{i+1}/{len(pending_ids)}] Fetching {case_id}...")

        record = fetch_decision(case_id)

        if record and len(record.get('text', '')) >= 100:
            yield record
            count += 1

            # Update checkpoint for full fetches
            if not max_records:
                try:
                    with open(checkpoint_file, 'a') as f:
                        f.write(f"{case_id}\n")
                except Exception:
                    pass

        time.sleep(REQUEST_DELAY)

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
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


def bootstrap_sample(sample_count: int = 12):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from IS/SupremeCourt...")
    print("=" * 60)

    records = []
    for i, record in enumerate(fetch_all(max_records=sample_count)):
        try:
            normalized = normalize(record)
            records.append(normalized)

            # Save individual record
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

    # Validate
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
    parser = argparse.ArgumentParser(description="IS/SupremeCourt case law fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'info'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'info':
        print(f"IS/SupremeCourt - Icelandic Supreme Court Case Law")
        print(f"Source URL: {BASE_URL}")
        print(f"Decisions URL: {DOMAR_URL}")

    elif args.command == 'bootstrap':
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
