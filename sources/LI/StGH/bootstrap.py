#!/usr/bin/env python3
"""
LI/StGH - Liechtenstein Constitutional Court (Staatsgerichtshof) Case Law Fetcher

Fetches court decisions from the Liechtenstein Constitutional Court.

Data source: https://www.gerichtsentscheidungen.li (via https://www.stgh.li/urteile)
License: Open Government Data
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.stgh.li"
URTEILE_URL = f"{BASE_URL}/urteile"
GERICHTSENTSCHEIDUNGEN_URL = "https://www.gerichtsentscheidungen.li"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "LI/StGH"

# Request settings
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0"
}
REQUEST_DELAY = 1.5  # Seconds between requests


def extract_decision_urls_from_stgh(html: str) -> list[str]:
    """Extract gerichtsentscheidungen.li URLs from the stgh.li/urteile page."""
    # Pattern: https://www.gerichtsentscheidungen.li/default.aspx?z=...
    # Judgment URLs have long encoded strings (70+ chars for the z parameter)
    # Navigation links are shorter (20-30 chars) - we filter those out
    pattern = r'https://www\.gerichtsentscheidungen\.li/default\.aspx\?z=[A-Za-z0-9_-]+'
    urls = re.findall(pattern, html)
    # Filter to only keep longer URLs (actual judgment links)
    # Navigation links have z=... of ~25 chars, judgment links have z=... of 70+ chars
    return list(set(url for url in urls if len(url.split('z=')[-1]) > 40))


def parse_date(date_str: str) -> Optional[str]:
    """Parse German date string to ISO format."""
    # Example: "21.10.2025"
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
    """Extract clean text from the decision HTML."""
    # Find the main content div
    content = soup.find('div', class_='eintrag')
    if not content:
        content = soup.find('table', border='0')
    if not content:
        content = soup.body

    if not content:
        return ""

    # Remove script and style elements
    for element in content(['script', 'style']):
        element.decompose()

    # Get text, preserving structure
    text = content.get_text(separator='\n', strip=True)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def parse_decision(html: str, url: str, require_stgh: bool = True) -> Optional[dict]:
    """Parse a court decision HTML page and extract metadata and text.

    Args:
        html: Raw HTML content
        url: Source URL
        require_stgh: If True, only return StGH (Constitutional Court) decisions
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')

        # Extract case number (e.g., "StGH 2025/100")
        case_number = None
        aktenzeichen = soup.find('div', class_='aktenzeichen')
        if aktenzeichen:
            case_number = aktenzeichen.get_text(strip=True)
        else:
            # Try to find in page content
            match = re.search(r'(StGH\s+\d{4}/\d+)', html)
            if match:
                case_number = match.group(1)

        # Filter for StGH (Constitutional Court) only if required
        if require_stgh:
            if not case_number or not case_number.startswith('StGH'):
                return None

        # Extract date from metadata
        date = None
        date_match = re.search(r"<div class='fL hEIItem'>(\d{1,2}\.\d{1,2}\.\d{4})</div>", html)
        if date_match:
            date = parse_date(date_match.group(1))

        # Extract court type
        court_type = None
        court_match = re.search(r"<div class='fL hEIItem'>(StGH|OGH|LG|OG)</div>", html)
        if court_match:
            court_type = court_match.group(1)

        # Extract decision type (Urteil, Beschluss, etc.)
        decision_type = None
        type_match = re.search(r"<div class='fL hEIItem'>(Urteil|Beschluss|Entscheidung)</div>", html)
        if type_match:
            decision_type = type_match.group(1)

        # Extract full text
        text = extract_text_from_html(soup)

        if not text or len(text) < 100:
            return None

        # Build title
        title_parts = []
        if case_number:
            title_parts.append(case_number)
        if decision_type:
            title_parts.append(decision_type)

        title = " - ".join(title_parts) if title_parts else f"StGH Decision"

        # Build document ID
        doc_id = case_number if case_number else url.split('z=')[-1][:20]

        # Map court type to full name
        court_names = {
            'StGH': 'Staatsgerichtshof (Constitutional Court)',
            'OGH': 'Oberster Gerichtshof (Supreme Court)',
            'LG': 'Landgericht (District Court)',
            'OG': 'Obergericht (Higher Court)'
        }
        court = court_names.get(court_type, 'Staatsgerichtshof')

        return {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': text,
            'date': date,
            'url': url,
            'language': 'deu',
            'court': court,
            'case_number': case_number,
            'decision_type': decision_type,
            'country': 'LI',
        }
    except Exception as e:
        print(f"  Error parsing decision: {e}")
        return None


def fetch_decision(url: str) -> Optional[dict]:
    """Fetch and parse a single court decision."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return parse_decision(resp.text, url)
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return None


def search_decisions(query: str) -> list[str]:
    """Search for decisions using the API and return URLs from results page."""
    try:
        # Use the search API to get a results page URL
        api_url = f"{GERICHTSENTSCHEIDUNGEN_URL}/methods.aspx/GetUrl"
        payload = {
            "u": "",
            "e": ["mode", "txt"],
            "d": ["suche", query]
        }
        resp = requests.post(
            api_url,
            json=payload,
            headers={**HEADERS, "Content-Type": "application/json"},
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()

        if 'd' in data:
            results_url = f"{GERICHTSENTSCHEIDUNGEN_URL}/{data['d']}"
            time.sleep(REQUEST_DELAY)

            # Fetch the results page and extract decision links
            results_resp = requests.get(results_url, headers=HEADERS, timeout=30)
            results_resp.raise_for_status()

            # Extract decision URLs from onclick handlers
            pattern = r"window\.location='(default\.aspx\?z=[A-Za-z0-9_-]+)'"
            urls = re.findall(pattern, results_resp.text)
            # Filter for longer URLs (actual decisions, not navigation)
            return [f"{GERICHTSENTSCHEIDUNGEN_URL}/{url}" for url in urls if len(url.split('z=')[-1]) > 40]
    except Exception as e:
        print(f"  Search error: {e}")
    return []


def get_all_decision_urls() -> list[str]:
    """Get all available decision URLs from the StGH urteile page and via search."""
    print(f"Fetching decision listings from {URTEILE_URL}...")

    urls = set()

    # Method 1: Get from stgh.li/urteile page
    try:
        resp = requests.get(URTEILE_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        page_urls = extract_decision_urls_from_stgh(resp.text)
        urls.update(page_urls)
        print(f"  Found {len(page_urls)} URLs from urteile page")
    except requests.RequestException as e:
        print(f"  Error fetching listing: {e}")

    # Method 2: Search for StGH decisions by year
    current_year = datetime.now().year
    for year in range(current_year, current_year - 3, -1):
        time.sleep(REQUEST_DELAY)
        print(f"  Searching for StGH {year} decisions...")
        search_urls = search_decisions(f"StGH {year}")
        new_urls = len(set(search_urls) - urls)
        urls.update(search_urls)
        if new_urls > 0:
            print(f"    Found {new_urls} new URLs")

    print(f"  Total unique URLs: {len(urls)}")
    return list(urls)


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """
    Fetch all court decisions.

    Args:
        max_records: Maximum number of records to yield (for sampling)

    Yields:
        Normalized document records
    """
    urls = get_all_decision_urls()

    if max_records:
        urls = urls[:max_records * 3]  # Get extra to account for non-StGH and duplicates

    print(f"Processing {len(urls)} decisions...")

    count = 0
    seen_case_numbers = set()  # Track case numbers to avoid duplicates

    for i, url in enumerate(urls):
        if max_records and count >= max_records:
            break

        print(f"  [{i+1}/{len(urls)}] Fetching {url[:60]}...")

        record = fetch_decision(url)

        if record and len(record.get('text', '')) >= 100:
            case_num = record.get('case_number', '')
            if case_num and case_num in seen_case_numbers:
                continue  # Skip duplicate
            seen_case_numbers.add(case_num)
            yield record
            count += 1

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

    print(f"Fetching {sample_count} sample records from LI/StGH...")
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
    parser = argparse.ArgumentParser(description="LI/StGH case law fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'info'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")

    args = parser.parse_args()

    if args.command == 'info':
        print(f"LI/StGH - Liechtenstein Constitutional Court Case Law")
        print(f"Source URL: {BASE_URL}")
        print(f"Decisions URL: {URTEILE_URL}")
        print(f"Data Portal: {GERICHTSENTSCHEIDUNGEN_URL}")

    elif args.command == 'bootstrap':
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
