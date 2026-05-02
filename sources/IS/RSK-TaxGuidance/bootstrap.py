#!/usr/bin/env python3
"""
IS/RSK-TaxGuidance - Skatturinn Binding Tax Opinions (Bindandi Álit)

Fetches binding tax opinions from the Icelandic Director of Internal Revenue
(Ríkisskattstjóri / Skatturinn).

Data source: https://www.skatturinn.is/fagadilar/bindandi-alit/
License: Public Domain (official government guidance)
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

BASE_URL = "https://www.skatturinn.is"
LISTING_URL = f"{BASE_URL}/fagadilar/bindandi-alit/"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/RSK-TaxGuidance"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0"
}
REQUEST_DELAY = 2.0

MONTH_MAP = {
    'janúar': '01', 'febrúar': '02', 'mars': '03', 'apríl': '04',
    'maí': '05', 'júní': '06', 'júlí': '07', 'ágúst': '08',
    'september': '09', 'október': '10', 'nóvember': '11', 'desember': '12'
}


def parse_date(text: str) -> Optional[str]:
    """Parse date from text like '12.4.2007' or '8. apríl 2025'."""
    # Try d.m.yyyy format
    match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text)
    if match:
        day = match.group(1).zfill(2)
        month = match.group(2).zfill(2)
        year = match.group(3)
        return f"{year}-{month}-{day}"

    # Try d. month_name yyyy
    match = re.search(r'(\d{1,2})\.\s*(\w+)\s+(\d{4})', text)
    if match:
        day = match.group(1).zfill(2)
        month_name = match.group(2).lower()
        year = match.group(3)
        month = MONTH_MAP.get(month_name)
        if month:
            return f"{year}-{month}-{day}"

    return None


def discover_opinion_links(session: requests.Session) -> list[tuple[str, str]]:
    """Discover all binding opinion links from the main listing page.
    Returns list of (url_path, link_text) tuples."""
    try:
        resp = session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching listing: {e}")
        return []

    soup = BeautifulSoup(resp.text, 'html.parser')
    seen = set()
    results = []

    for a in soup.find_all('a', href=True):
        href = a['href']
        text = a.get_text(strip=True)

        # Match opinion links — both slug and nr formats
        if '/bindandi-alit/' not in href:
            continue
        if href == '/fagadilar/bindandi-alit/':
            continue
        # Skip year-only links
        if re.match(r'^/fagadilar/bindandi-alit/\d{4}$', href):
            continue
        if 'bindandi' not in text.lower() and 'Lesa meira' not in text:
            continue

        full_url = BASE_URL + href if href.startswith('/') else href
        if full_url not in seen:
            seen.add(full_url)
            results.append((full_url, text))

    return results


def fetch_opinion(url: str, session: requests.Session) -> Optional[dict]:
    """Fetch and parse a single binding opinion."""
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error fetching {url}: {e}")
        return None

    return parse_opinion_html(resp.text, url)


def parse_opinion_html(html: str, url: str) -> Optional[dict]:
    """Parse opinion HTML and extract structured data."""
    soup = BeautifulSoup(html, 'html.parser')

    # Extract title from h1
    h1 = soup.find('h1')
    title = h1.get_text(strip=True) if h1 else None

    # Extract opinion number (e.g., "1/2025", "4/07")
    opinion_number = None
    if title:
        match = re.search(r'(\d+/\d{2,4})', title)
        if match:
            opinion_number = match.group(1)

    # Extract subject from the first h2 after h1
    subject = None
    if h1:
        next_h2 = h1.find_next('h2')
        if next_h2:
            subject_text = next_h2.get_text(strip=True)
            # Avoid navigation h2s
            if subject_text and 'Síðuvalmynd' not in subject_text and 'Leita' not in subject_text:
                subject = subject_text

    # Find content in .article container
    article = soup.select_one('.article')
    if not article:
        article = soup.find('article')

    if not article:
        return None

    # Remove nav elements
    for nav in article.find_all(['nav', 'script', 'style']):
        nav.decompose()

    # Extract text
    full_text = article.get_text(separator='\n', strip=True)

    # Clean: remove year navigation lines at the top
    lines = full_text.split('\n')
    cleaned_lines = []
    content_started = False
    for line in lines:
        if not content_started:
            # Skip year-only lines and "Bindandi álit" header before the title
            if re.match(r'^\d{4}$', line.strip()):
                continue
            if line.strip() == 'Bindandi álit':
                continue
            content_started = True
        cleaned_lines.append(line)

    full_text = '\n'.join(cleaned_lines)
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)
    full_text = full_text.strip()

    if len(full_text) < 100:
        return None

    # Extract date from the text
    date = parse_date(full_text[:500])

    if not date and opinion_number:
        year_match = re.search(r'/(\d{2,4})$', opinion_number)
        if year_match:
            yr = year_match.group(1)
            if len(yr) == 2:
                yr = '20' + yr if int(yr) < 50 else '19' + yr
            date = f"{yr}-01-01"

    if not title:
        title = f"Bindandi álit {opinion_number}" if opinion_number else "Bindandi álit"

    doc_id = f"BAL-{opinion_number}" if opinion_number else url.split('/')[-1]

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.utcnow().isoformat() + 'Z',
        'title': title,
        'text': full_text,
        'date': date,
        'url': url,
        'language': 'isl',
        'opinion_number': opinion_number,
        'subject': subject,
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all binding opinions."""
    session = requests.Session()
    session.headers.update(HEADERS)

    print(f"Discovering binding opinions from {LISTING_URL}...")
    links = discover_opinion_links(session)
    print(f"  Found {len(links)} opinion links")

    count = 0
    for i, (url, link_text) in enumerate(links):
        if max_records and count >= max_records:
            break

        print(f"  [{i+1}/{len(links)}] {link_text[:50]}...")
        record = fetch_opinion(url, session)
        time.sleep(REQUEST_DELAY)

        if record and len(record.get('text', '')) >= 100:
            yield record
            count += 1

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch opinions updated since a given date."""
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
    parser = argparse.ArgumentParser(description="IS/RSK-TaxGuidance binding opinions fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'info'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=15,
                       help="Number of sample records to fetch")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'info':
        print(f"{SOURCE_ID} - Skatturinn Binding Tax Opinions")
        print(f"Source URL: {LISTING_URL}")

    elif args.command == 'bootstrap':
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
