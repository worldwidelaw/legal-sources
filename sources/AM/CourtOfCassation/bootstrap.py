#!/usr/bin/env python3
"""
AM/CourtOfCassation - Armenian Court of Cassation (Vchrabek)

Data source: https://cassationcourt.am
Format: PDF/DOC documents via API, text extracted via pdfplumber
License: Public Domain (government decisions)
Records: ~1,300+ decisions (civil, criminal, administrative, anti-corruption)

The Court of Cassation is the highest court in Armenia's judicial system.
It reviews decisions from lower courts on questions of law.

API endpoint discovered: /api/precedent-single-decision/{chamber}/{id}
Chamber types: civil-cases, criminal-cases, administrative-cases,
               administrative-cases-intermediate, corruption-civil-cases,
               corruption-crimes-cases
"""

import argparse
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional, List, Dict
from html import unescape

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Configuration
SOURCE_ID = "AM/CourtOfCassation"
BASE_URL = "https://cassationcourt.am"
API_URL = f"{BASE_URL}/api/precedent-single-decision"
DECISIONS_URL = f"{BASE_URL}/en/decisions/"
REQUEST_DELAY = 1.5  # seconds between requests


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,hy;q=0.8",
    })
    return session


def get_decision_ids_from_page(session: requests.Session, page: int = 1) -> List[Dict]:
    """Fetch decision IDs and metadata from a pagination page."""
    url = f"{DECISIONS_URL}?page={page}"

    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        html = resp.text

        soup = BeautifulSoup(html, 'html.parser')
        decisions = []

        # Find all decision links with data attributes
        for link in soup.find_all('a', attrs={'data-id': True, 'data-chamber': True}):
            decision_id = link.get('data-id')
            chamber = link.get('data-chamber')

            if decision_id and chamber:
                # Extract metadata from the link content
                divs = link.find_all('div')
                metadata = {}

                if len(divs) >= 6:
                    metadata['date'] = divs[0].get_text(strip=True)
                    metadata['case_number'] = divs[1].get_text(strip=True)
                    metadata['case_type'] = divs[2].get_text(strip=True)
                    # divs[3] is usually '-'
                    metadata['claimant'] = divs[4].get_text(strip=True) if divs[4].get_text(strip=True) != '-' else None
                    metadata['respondent'] = divs[5].get_text(strip=True) if divs[5].get_text(strip=True) != '-' else None

                decisions.append({
                    'id': decision_id,
                    'chamber': chamber,
                    'metadata': metadata
                })

        return decisions

    except Exception as e:
        print(f"  Error fetching page {page}: {e}", file=sys.stderr)
        return []


def get_all_decision_ids(session: requests.Session, max_pages: int = 100) -> List[Dict]:
    """Fetch all decision IDs by paginating through the decisions list."""
    all_decisions = []

    for page in range(1, max_pages + 1):
        print(f"  Scanning page {page}...")
        decisions = get_decision_ids_from_page(session, page)

        if not decisions:
            print(f"  No more decisions found at page {page}")
            break

        all_decisions.extend(decisions)
        time.sleep(0.5)  # Rate limiting

    return all_decisions


def fetch_decision_detail(session: requests.Session, chamber: str, decision_id: str) -> Optional[Dict]:
    """Fetch decision details from the API."""
    url = f"{API_URL}/{chamber}/{decision_id}"

    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return None

        data = resp.json()
        if data.get('status') != 200:
            return None

        return {
            'content_html': data.get('content', ''),
            'chamber': chamber,
            'decision_id': decision_id
        }

    except Exception as e:
        print(f"  Error fetching decision {decision_id}: {e}", file=sys.stderr)
        return None


def extract_pdf_url_from_html(html_content: str) -> Optional[str]:
    """Extract PDF URL from the decision HTML content."""
    soup = BeautifulSoup(html_content, 'html.parser')

    # Look for PDF link
    pdf_link = soup.find('a', href=lambda x: x and '.pdf' in x.lower())
    if pdf_link:
        href = pdf_link.get('href', '')
        if not href.startswith('http'):
            href = BASE_URL + href
        return href

    # Look for iframe with PDF
    iframe = soup.find('iframe', src=lambda x: x and '.pdf' in x.lower())
    if iframe:
        src = iframe.get('src', '')
        if not src.startswith('http'):
            src = BASE_URL + src
        return src

    return None


def extract_metadata_from_html(html_content: str) -> Dict:
    """Extract metadata fields from the decision HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    metadata = {}

    # Find info divs with title/value pairs
    for info_div in soup.find_all('div', class_='decisionInfo'):
        title_div = info_div.find('div', class_='title')
        if title_div:
            title = title_div.get_text(strip=True).rstrip(':')
            # Get next sibling div with the value
            value_divs = info_div.find_all('div')
            if len(value_divs) >= 2:
                value = value_divs[1].get_text(strip=True)
                metadata[title] = value

    return metadata


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="AM/CourtOfCassation",
        source_id="",
        pdf_bytes=pdf_content,
        table="case_law",
    ) or ""

def clean_text(text: str) -> str:
    """Clean and normalize extracted text."""
    if not text:
        return ""

    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)

    return text.strip()


def fetch_full_decision(session: requests.Session, chamber: str, decision_id: str,
                        list_metadata: Dict = None) -> Optional[Dict]:
    """Fetch a complete decision with full text from PDF."""
    # Get decision detail from API
    detail = fetch_decision_detail(session, chamber, decision_id)
    if not detail:
        return None

    html_content = detail.get('content_html', '')

    # Extract PDF URL
    pdf_url = extract_pdf_url_from_html(html_content)
    if not pdf_url:
        print(f"    No PDF found for decision {decision_id}")
        return None

    # Download and extract text from PDF
    try:
        time.sleep(0.5)  # Small delay before PDF download
        pdf_resp = session.get(pdf_url, timeout=60)
        pdf_resp.raise_for_status()

        full_text = extract_text_from_pdf(pdf_resp.content)
        if not full_text or len(full_text) < 100:
            print(f"    PDF text extraction failed or too short for {decision_id}")
            return None

    except Exception as e:
        print(f"    Error downloading PDF: {e}", file=sys.stderr)
        return None

    # Extract metadata from HTML content
    html_metadata = extract_metadata_from_html(html_content)

    # Merge with list metadata
    metadata = list_metadata or {}
    metadata.update(html_metadata)

    return {
        'decision_id': decision_id,
        'chamber': chamber,
        'pdf_url': pdf_url,
        'text': clean_text(full_text),
        'metadata': metadata
    }


def normalize(raw: Dict) -> Dict:
    """Transform raw decision data into standard schema."""
    metadata = raw.get('metadata', {})

    # Build case number - check various fields
    case_number = metadata.get('case_number', '')
    
    # Check Armenian field names
    for key in metadata:
        key_lower = key.lower()
        if 'gord' in key_lower or 'hamar' in key_lower:
            case_number = metadata[key]
            break

    # Parse date
    date_str = metadata.get('date', '')
    parsed_date = None
    if date_str:
        # Try DD.MM.YYYY format
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            day, month, year = match.groups()
            parsed_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

    # Build unique ID
    decision_id = raw.get('decision_id', '')
    chamber = raw.get('chamber', 'unknown')
    _id = f"AM-CASS-{chamber}-{decision_id}"

    # Build title
    title_parts = []
    if case_number:
        title_parts.append(case_number)
    if parsed_date:
        title_parts.append(parsed_date)
    title = " - ".join(title_parts) if title_parts else f"Decision {decision_id}"

    # Map chamber to type
    case_type = "case_law"
    if 'criminal' in chamber:
        case_type = "criminal_case"
    elif 'civil' in chamber:
        case_type = "civil_case"
    elif 'administrative' in chamber:
        case_type = "administrative_case"
    elif 'corruption' in chamber:
        case_type = "anti_corruption_case"

    return {
        "_id": _id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get('text', ''),
        "date": parsed_date,
        "url": raw.get('pdf_url', ''),
        "case_number": case_number,
        "chamber": chamber,
        "case_type": case_type,
        "claimant": metadata.get('claimant'),
        "respondent": metadata.get('respondent'),
    }


def fetch_all() -> Generator[Dict, None, None]:
    """Fetch all available decisions with full text."""
    session = get_session()

    print("Discovering all decision IDs...")
    all_decisions = get_all_decision_ids(session)
    print(f"Found {len(all_decisions)} total decisions")

    for i, decision_info in enumerate(all_decisions):
        print(f"\nProcessing {i+1}/{len(all_decisions)}: {decision_info['id']}...")

        raw = fetch_full_decision(
            session,
            decision_info['chamber'],
            decision_info['id'],
            decision_info.get('metadata')
        )

        if raw and raw.get('text'):
            yield normalize(raw)

        time.sleep(REQUEST_DELAY)


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch decisions modified since a given date."""
    for record in fetch_all():
        if record.get('date'):
            try:
                decision_date = datetime.fromisoformat(record['date'])
                if decision_date >= since:
                    yield record
            except:
                pass


def bootstrap_sample(sample_dir: Path, count: int = 12):
    """Fetch sample records for validation."""
    session = get_session()

    print("Discovering decision IDs...")
    # Only scan first few pages for sample
    all_decisions = []
    for page in range(1, 10):
        print(f"  Scanning page {page}...")
        decisions = get_decision_ids_from_page(session, page)
        if not decisions:
            break
        all_decisions.extend(decisions)
        time.sleep(0.5)

    print(f"Found {len(all_decisions)} decisions from first pages")

    if not all_decisions:
        print("ERROR: No decisions found!")
        return

    sample_dir.mkdir(parents=True, exist_ok=True)

    # Sample from different chamber types
    chamber_counts = {}
    sampled = []

    for decision in all_decisions:
        chamber = decision['chamber']
        if chamber_counts.get(chamber, 0) < 3:  # Max 3 per chamber type
            chamber_counts[chamber] = chamber_counts.get(chamber, 0) + 1
            sampled.append(decision)
        if len(sampled) >= count:
            break

    # Fill remaining slots if needed
    if len(sampled) < count:
        for decision in all_decisions:
            if decision not in sampled:
                sampled.append(decision)
            if len(sampled) >= count:
                break

    total_text_chars = 0
    records_saved = 0
    records_attempted = 0

    for i, decision_info in enumerate(sampled[:count]):
        records_attempted += 1
        print(f"\nProcessing {i+1}/{count}: {decision_info['chamber']}/{decision_info['id']}...")

        raw = fetch_full_decision(
            session,
            decision_info['chamber'],
            decision_info['id'],
            decision_info.get('metadata')
        )

        if not raw:
            print("  Failed to fetch")
            time.sleep(REQUEST_DELAY)
            continue

        record = normalize(raw)

        text_len = len(record.get("text", ""))
        if text_len < 100:
            print(f"  Text too short ({text_len} chars), skipping")
            time.sleep(REQUEST_DELAY)
            continue

        total_text_chars += text_len
        records_saved += 1

        # Save to sample directory
        safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', record['_id'])[:100]
        filename = f"{safe_name}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  Saved: {filename}")
        print(f"  Chamber: {record.get('chamber', 'unknown')}")
        print(f"  Case: {record.get('case_number', 'unknown')}")
        print(f"  Text: {text_len:,} chars")

        time.sleep(REQUEST_DELAY)

    # Print summary
    print("\n" + "="*60)
    print("SAMPLE SUMMARY")
    print("="*60)
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
    parser = argparse.ArgumentParser(description="Armenian Court of Cassation Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=12,
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
            sample_dir.mkdir(parents=True, exist_ok=True)
            records_saved = 0
            for record in fetch_all():
                safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', record['_id'])[:100]
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
