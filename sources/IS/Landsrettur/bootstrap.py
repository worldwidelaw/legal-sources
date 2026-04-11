#!/usr/bin/env python3
"""
IS/Landsrettur - Icelandic Court of Appeals (Landsréttur) Case Law Fetcher

Fetches court decisions from the Icelandic Court of Appeals website.

Data source: https://www.landsrettur.is/
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

BASE_URL = "https://www.landsrettur.is"
DOMAR_URL = f"{BASE_URL}/domar-og-urskurdir/"
DOMUR_URL = f"{BASE_URL}/domar-og-urskurdir/domur-urskurdur/"
PAGINATION_URL = f"{BASE_URL}/default.aspx"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/Landsrettur"

# Pagination settings - AJAX endpoint
# The site uses pageitemid parameter for AJAX load-more pagination
PAGEITEM_ID = "landsrettur-domar-listing"
PAGE_SIZE = 50  # Items per AJAX request

# Request settings
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalSourcesBot/1.0; worldwidelaw/legal-sources)"
}
REQUEST_DELAY = 1.5  # Seconds between requests


def extract_case_ids_from_listing(html: str) -> list[str]:
    """Extract case UUID IDs from the domar listing page."""
    # Pattern: /domar-og-urskurdir/domur-urskurdur/?Id=UUID
    pattern = r'[Ii]d=([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})'
    return list(set(re.findall(pattern, html)))


def parse_date(date_str: str) -> Optional[str]:
    """Parse Icelandic date string to ISO format."""
    month_map = {
        'janúar': '01', 'febrúar': '02', 'mars': '03', 'apríl': '04',
        'maí': '05', 'júní': '06', 'júlí': '07', 'ágúst': '08',
        'september': '09', 'október': '10', 'nóvember': '11', 'desember': '12'
    }

    try:
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

    # Try d.m.yyyy format
    try:
        match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month = match.group(2).zfill(2)
            year = match.group(3)
            return f"{year}-{month}-{day}"
    except Exception:
        pass

    return None


def extract_text_from_html(soup: BeautifulSoup) -> str:
    """Extract clean text from the decision body."""
    # The verdict text is in a sr-only div on landsrettur.is
    body = soup.find('div', class_='sr-only')
    if not body:
        body = soup.find('div', class_='verdict__body')
    if not body:
        body = soup.find('div', class_='verdict')

    if not body:
        return ""

    # Remove script and style elements
    for element in body(['script', 'style']):
        element.decompose()

    text = body.get_text(separator='\n', strip=True)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def parse_decision(html: str, case_id: str) -> Optional[dict]:
    """Parse a Court of Appeals decision HTML page."""
    try:
        soup = BeautifulSoup(html, 'html.parser')

        # Extract case number - format "Mál nr. N/YYYY"
        case_number = None
        heading = soup.find('h1') or soup.find('h2')
        if heading:
            heading_text = heading.get_text(strip=True)
            match = re.search(r'(\d+/\d{4})', heading_text)
            if match:
                case_number = match.group(1)

        # Try subtitle
        if not case_number:
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
                dt_str = time_elem.get('datetime')
                date = parse_date(dt_str)

        # Extract parties
        parties = None
        parties_div = soup.find('div', class_='verdict-head__parties')
        if parties_div:
            parties = parties_div.get_text(strip=True)

        # Also try from the text header
        if not parties:
            for tag in soup.find_all(['h2', 'h3']):
                tag_text = tag.get_text(strip=True)
                if 'gegn' in tag_text and ('lögmaður' in tag_text or 'ehf' in tag_text):
                    parties = tag_text
                    break

        # Extract keywords
        keywords = []
        keyword_section = soup.find('div', class_='verdict__keywords')
        if keyword_section:
            keywords = [li.get_text(strip=True) for li in keyword_section.find_all('li')]
        if not keywords and keyword_section:
            keywords_text = keyword_section.get_text(strip=True)
            if keywords_text:
                # Split by period or comma
                keywords = [k.strip().rstrip('.') for k in re.split(r'[.,]', keywords_text) if k.strip()]

        # Also try lykilorð section
        if not keywords:
            for tag in soup.find_all(['h3', 'h4', 'strong']):
                if 'lykilorð' in tag.get_text(strip=True).lower() or 'lykilor' in tag.get_text(strip=True).lower():
                    next_elem = tag.find_next_sibling()
                    if next_elem:
                        kw_text = next_elem.get_text(strip=True)
                        keywords = [k.strip().rstrip('.') for k in re.split(r'[.,]', kw_text) if k.strip()]
                    break

        # Extract abstract/summary (útdráttur)
        abstract = None
        abstract_section = soup.find('div', class_='verdict__reifun')
        if abstract_section:
            abstract = abstract_section.get_text(strip=True)

        if not abstract:
            for tag in soup.find_all(['h3', 'h4', 'strong']):
                tag_text = tag.get_text(strip=True).lower()
                if 'útdráttur' in tag_text or 'reifun' in tag_text:
                    next_div = tag.find_next_sibling('div')
                    if next_div:
                        abstract = next_div.get_text(strip=True)
                    else:
                        # Get text until next heading
                        parts = []
                        for sibling in tag.next_siblings:
                            if sibling.name in ['h3', 'h4', 'h2']:
                                break
                            text = sibling.get_text(strip=True) if hasattr(sibling, 'get_text') else str(sibling).strip()
                            if text:
                                parts.append(text)
                        if parts:
                            abstract = ' '.join(parts)
                    break

        # Extract full text from sr-only div
        text = extract_text_from_html(soup)

        if not text or len(text) < 100:
            return None

        # Build title
        title = f"Landsréttur - Mál nr. {case_number}" if case_number else f"Landsréttur {case_id[:8]}"

        # Build document ID
        doc_id = case_number if case_number else case_id

        url = f"{DOMUR_URL}?Id={case_id}"

        record = {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': text,
            'date': date,
            'url': url,
            'language': 'isl',
            'court': 'Landsréttur',
            'case_number': case_number,
        }

        if parties:
            record['parties'] = parties
        if keywords:
            record['keywords'] = keywords
        if abstract:
            record['abstract'] = abstract

        return record

    except Exception as e:
        print(f"  Error parsing decision {case_id}: {e}")
        return None


def fetch_decision(case_id: str) -> Optional[dict]:
    """Fetch and parse a single Court of Appeals decision."""
    url = f"{DOMUR_URL}?Id={case_id}"

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

    The Landsréttur website uses AJAX load-more pagination via
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

    # First, get IDs from the main listing page
    try:
        resp = requests.get(DOMAR_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        ids = extract_case_ids_from_listing(resp.text)
        all_ids.update(ids)
        print(f"  Found {len(ids)} cases on main listing")
    except requests.RequestException as e:
        print(f"  Error fetching main listing: {e}")

    # Paginate through all decisions using AJAX endpoint
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
            time.sleep(0.5)

            # Safety limit - ~6,000 decisions expected
            if offset > 8000:
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
    Fetch all Court of Appeals decisions with checkpoint/resume support.

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

    # Get all case IDs
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
    parser = argparse.ArgumentParser(description="IS/Landsrettur case law fetcher")
    parser.add_argument('command', choices=['bootstrap', 'update', 'fetch', 'info'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument('--since', type=str, default=None,
                       help="Fetch updates since date (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.command == 'info':
        print(f"IS/Landsrettur - Icelandic Court of Appeals Case Law")
        print(f"Source URL: {BASE_URL}")
        print(f"Decisions URL: {DOMAR_URL}")
        print(f"Expected records: ~6,000 decisions (2018-present)")

    elif args.command == 'bootstrap':
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == 'update':
        since = datetime.fromisoformat(args.since) if args.since else datetime(2024, 1, 1)
        print(f"Fetching updates since {since.isoformat()}...")
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
