#!/usr/bin/env python3
"""
Hungarian Supreme Court (Kúria) Data Fetcher

Extracts court decisions from the Kúria website using the XML sitemap.
Covers uniformity decisions, principled decisions, electoral cases,
municipal cases, and other Supreme Court rulings.

Data source: https://kuria-birosag.hu
License: Public Domain
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional, List
from html import unescape

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

SOURCE_ID = "HU/FelsoBirosag"
BASE_URL = "https://kuria-birosag.hu"

# Sitemap URLs
SITEMAP_INDEX_URL = f"{BASE_URL}/hu/sitemap.xml"

# Decision URL patterns to include
DECISION_PATTERNS = [
    '/hu/valhat/',      # Electoral decisions (választási határozatok)
    '/hu/joghat/',      # Legal unity decisions
    '/hu/onkugy/',      # Municipal cases (önkormányzati ügyek)
    '/hu/nepszavugy/',  # Referendum cases
    '/hu/ejeb/',        # ECHR cases
    '/hu/alkotmbir-hat/', # Constitutional court cases
    '/hu/jogegysegi-panasz/',  # Legal unity complaints
    '/hu/kuriai-dontesek/',    # Curia decisions (monthly compilations)
    '/hu/gyulhat/',     # Assembly cases
    '/hu/kollvel/',     # Collegial opinions
]


def curl_get(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL content using curl (workaround for SSL issues)."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout), url],
            capture_output=True,
            text=True,
            timeout=timeout + 5
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except Exception as e:
        print(f"  curl error for {url}: {e}")
        return None


def clean_text(text: str) -> str:
    """Clean HTML entities and normalize whitespace."""
    if not text:
        return ""
    text = unescape(text)
    # Normalize whitespace but preserve paragraph breaks
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def extract_text_from_html(soup: BeautifulSoup) -> str:
    """Extract full text from decision HTML page."""
    content_div = soup.find('div', class_='node__content')
    if not content_div:
        return ""

    text_parts = []

    # Find the body field with the decision text
    body_field = content_div.find('div', class_='field--name-body')
    if body_field:
        # Get all text blocks
        for elem in body_field.find_all(['p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5']):
            text = elem.get_text(separator=' ', strip=True)
            # Filter out CSS/style artifacts
            if text and not text.startswith(('background:', 'height:', 'margin:', 'padding:')):
                text_parts.append(text)

    return clean_text('\n\n'.join(text_parts))


def parse_sitemap_urls(sitemap_xml: str) -> List[str]:
    """Parse URLs from a sitemap XML."""
    urls = []
    try:
        # Remove namespace for easier parsing
        sitemap_xml = re.sub(r'\s+xmlns[^"]*"[^"]*"', '', sitemap_xml)
        root = ET.fromstring(sitemap_xml)

        # Check if this is a sitemap index
        for sitemap in root.findall('.//sitemap'):
            loc = sitemap.find('loc')
            if loc is not None and loc.text:
                urls.append(loc.text)

        # Or if it's a regular sitemap with URLs
        for url_elem in root.findall('.//url'):
            loc = url_elem.find('loc')
            if loc is not None and loc.text:
                urls.append(loc.text)

    except ET.ParseError as e:
        print(f"  XML parse error: {e}")

    return urls


def get_decision_urls_from_sitemap() -> List[str]:
    """Fetch all decision URLs from the sitemap."""
    print("Fetching sitemap index...")

    sitemap_index = curl_get(SITEMAP_INDEX_URL)
    if not sitemap_index:
        print("ERROR: Could not fetch sitemap index")
        return []

    sitemap_urls = parse_sitemap_urls(sitemap_index)
    print(f"Found {len(sitemap_urls)} sitemap pages")

    decision_urls = []

    for sitemap_url in sitemap_urls:
        print(f"  Fetching {sitemap_url}...")
        sitemap_content = curl_get(sitemap_url)
        if not sitemap_content:
            continue

        urls = parse_sitemap_urls(sitemap_content)

        # Filter for decision URLs only
        for url in urls:
            for pattern in DECISION_PATTERNS:
                if pattern in url:
                    decision_urls.append(url)
                    break

        time.sleep(0.5)  # Rate limiting

    # Remove duplicates while preserving order
    seen = set()
    unique_urls = []
    for url in decision_urls:
        if url not in seen:
            seen.add(url)
            unique_urls.append(url)

    return unique_urls


def fetch_decision(url: str) -> Optional[dict]:
    """Fetch a single decision page and extract data."""
    html = curl_get(url)
    if not html:
        return None

    try:
        soup = BeautifulSoup(html, 'html.parser')

        # Extract title
        title_elem = soup.find('h1', class_='page-title')
        title = title_elem.get_text(strip=True) if title_elem else ""

        # Extract full text
        full_text = extract_text_from_html(soup)

        if not full_text or len(full_text) < 100:
            return None

        # Determine decision type from URL
        decision_type = "court_decision"
        if '/valhat/' in url:
            decision_type = "electoral_decision"
        elif '/joghat/' in url:
            decision_type = "uniformity_decision"
        elif '/onkugy/' in url:
            decision_type = "municipal_case"
        elif '/nepszavugy/' in url:
            decision_type = "referendum_case"
        elif '/ejeb/' in url:
            decision_type = "echr_case"
        elif '/alkotmbir-hat/' in url:
            decision_type = "constitutional_case"
        elif '/jogegysegi-panasz/' in url:
            decision_type = "uniformity_complaint"
        elif '/kuriai-dontesek/' in url:
            decision_type = "curia_decision"
        elif '/gyulhat/' in url:
            decision_type = "assembly_case"
        elif '/kollvel/' in url:
            decision_type = "collegial_opinion"

        # Parse decision/case number from title or URL
        decision_number = ""
        case_number = ""

        # Try to extract from title (pattern: "Kvk.I.37.141/2010/2. számú határozat")
        title_match = re.search(r'([A-Za-z]+\.?[IVX]*\.?\d+[./]\d+/\d+[./]?\d*)', title)
        if title_match:
            case_number = title_match.group(1)

        # Try pattern like "16/2025. JEH"
        decision_match = re.search(r'(\d+/\d{4})\.\s*(JEH|BJE|PJE|KJE|MJE|EBH|EBD)', title)
        if decision_match:
            decision_number = f"{decision_match.group(1)} {decision_match.group(2)}"

        # Extract date
        date_str = None

        # Try to find date in content (Budapest, YYYY. month DD.)
        date_pattern = re.search(
            r'Budapest,?\s*(\d{4})\.\s*(január|február|március|április|május|június|'
            r'július|augusztus|szeptember|október|november|december)\s*(\d{1,2})\.?',
            full_text, re.IGNORECASE
        )
        if date_pattern:
            months = {
                'január': '01', 'február': '02', 'március': '03', 'április': '04',
                'május': '05', 'június': '06', 'július': '07', 'augusztus': '08',
                'szeptember': '09', 'október': '10', 'november': '11', 'december': '12'
            }
            month = months.get(date_pattern.group(2).lower(), '01')
            date_str = f"{date_pattern.group(1)}-{month}-{date_pattern.group(3).zfill(2)}"
        else:
            # Fallback: extract year from URL
            year_match = re.search(r'/(\d{4})/', url)
            if year_match:
                date_str = f"{year_match.group(1)}-01-01"

        return {
            'url': url,
            'title': title,
            'text': full_text,
            'decision_number': decision_number,
            'case_number': case_number,
            'date': date_str,
            'decision_type': decision_type,
        }

    except Exception as e:
        print(f"  Error parsing {url}: {e}")
        return None


def normalize(raw: dict) -> dict:
    """Transform raw decision data into standard schema."""
    # Generate unique ID from case number or URL
    if raw.get('case_number'):
        doc_id = re.sub(r'[^a-zA-Z0-9]', '_', raw['case_number'])
    elif raw.get('decision_number'):
        doc_id = re.sub(r'[^a-zA-Z0-9]', '_', raw['decision_number'])
    else:
        # Use URL path as ID
        path = raw.get('url', '').replace(BASE_URL, '').strip('/')
        doc_id = re.sub(r'[^a-zA-Z0-9]', '_', path)

    doc_id = f"HU_KURIA_{doc_id}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get('title', ''),
        "text": raw.get('text', ''),
        "date": raw.get('date'),
        "url": raw.get('url', ''),
        "decision_number": raw.get('decision_number', ''),
        "case_number": raw.get('case_number', ''),
        "decision_type": raw.get('decision_type', ''),
    }


def fetch_all() -> Iterator[dict]:
    """Fetch all available decisions with full text."""
    print("Discovering decision URLs from sitemap...")
    all_urls = get_decision_urls_from_sitemap()
    print(f"Total decisions found: {len(all_urls)}")

    for i, url in enumerate(all_urls):
        print(f"Processing {i+1}/{len(all_urls)}: {url[-60:]}...")

        raw_data = fetch_decision(url)
        if raw_data and raw_data.get('text'):
            yield normalize(raw_data)

        time.sleep(1.5)  # Rate limiting


def fetch_updates(since: datetime) -> Iterator[dict]:
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
    print("Discovering decision URLs from sitemap...")
    all_urls = get_decision_urls_from_sitemap()

    if not all_urls:
        print("ERROR: No decision URLs found!")
        return

    print(f"Found {len(all_urls)} total decisions")
    print(f"Fetching {min(count, len(all_urls))} sample records...")

    sample_dir.mkdir(parents=True, exist_ok=True)

    total_text_chars = 0
    records_saved = 0
    records_attempted = 0

    # Sample from different decision types
    type_counts = {}
    sampled_urls = []

    for url in all_urls:
        for pattern in DECISION_PATTERNS:
            if pattern in url:
                dtype = pattern.strip('/')
                if type_counts.get(dtype, 0) < 2:  # Max 2 per type
                    type_counts[dtype] = type_counts.get(dtype, 0) + 1
                    sampled_urls.append(url)
                break
        if len(sampled_urls) >= count:
            break

    # If we didn't get enough, just take from the start
    if len(sampled_urls) < count:
        for url in all_urls:
            if url not in sampled_urls:
                sampled_urls.append(url)
            if len(sampled_urls) >= count:
                break

    for i, url in enumerate(sampled_urls[:count]):
        records_attempted += 1
        print(f"\nProcessing {i+1}/{count}: {url[-60:]}...")

        raw_data = fetch_decision(url)
        if not raw_data:
            print("  Failed to fetch or parse")
            continue

        record = normalize(raw_data)

        text_len = len(record.get("text", ""))
        if text_len < 100:
            print(f"  Text too short ({text_len} chars), skipping")
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
        print(f"  Type: {record.get('decision_type', 'unknown')}")
        print(f"  Title: {record.get('title', '')[:60]}...")
        print(f"  Text: {text_len:,} chars")

        time.sleep(1.5)  # Rate limiting

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
        print("\n✓ SUCCESS: 10+ sample records with full text")
    else:
        print(f"\n✗ WARNING: Only {records_saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(description="Hungarian Supreme Court (Kúria) Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                       help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            # Full bootstrap: process all decisions
            print("Running full bootstrap (no limit)...")
            records_saved = 0
            for record in fetch_all():
                # Save each record
                safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', record['_id'])[:100]
                filename = f"{safe_name}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
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
