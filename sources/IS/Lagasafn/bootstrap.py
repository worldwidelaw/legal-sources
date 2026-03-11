#!/usr/bin/env python3
"""
IS/Lagasafn - Icelandic Consolidated Legislation Fetcher

Fetches all Icelandic statutes from the official Lagasafn (Law Collection)
published by the Althingi (Parliament of Iceland).

Data source: https://www.althingi.is/lagasafn/
License: Public Domain (official government publications)
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.althingi.is/lagasafn"
CURRENT_VERSION = "156b"  # September 2025 compilation
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/Lagasafn"

# Browser-like headers to avoid 403 Forbidden from datacenter IPs
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}


def download_zip(version: str = CURRENT_VERSION) -> BytesIO:
    """Download the law collection ZIP file."""
    url = f"{BASE_URL}/zip/{version}/allt.zip"
    print(f"Downloading Icelandic law collection from {url}...")

    resp = requests.get(url, headers=HEADERS, timeout=120, stream=True)
    resp.raise_for_status()

    data = BytesIO()
    total = int(resp.headers.get('content-length', 0))
    downloaded = 0

    for chunk in resp.iter_content(chunk_size=8192):
        data.write(chunk)
        downloaded += len(chunk)
        if total:
            pct = (downloaded / total) * 100
            print(f"\r  Progress: {downloaded:,} / {total:,} bytes ({pct:.1f}%)", end="", flush=True)

    print()
    data.seek(0)
    return data


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from Icelandic law HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style elements
    for element in soup(['script', 'style', 'nav', 'header', 'footer', 'meta', 'link']):
        element.decompose()

    # Get text, preserving structure
    text = soup.get_text(separator='\n', strip=True)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def parse_law_html(content: str, filename: str) -> Optional[dict]:
    """Parse Icelandic law HTML document and extract metadata and text."""
    try:
        soup = BeautifulSoup(content, 'html.parser')

        # Parse filename: format is YYYYNNN.html (year + law number)
        # e.g., 1944033.html = Law 33 of 1944 (The Constitution)
        match = re.match(r'(\d{4})(\d{3})\.html?', filename)
        if match:
            year = match.group(1)
            law_num = str(int(match.group(2)))  # Remove leading zeros
            doc_id = f"{year}/{law_num}"
        else:
            doc_id = filename.replace('.html', '')
            year = None
            law_num = None

        # Extract title - typically in <title> or first <h1>
        title = None
        title_elem = soup.find('title')
        if title_elem:
            title = title_elem.get_text(strip=True)

        if not title:
            h1 = soup.find('h1')
            if h1:
                title = h1.get_text(strip=True)

        if not title:
            title = f"Lög nr. {law_num}/{year}" if year and law_num else doc_id

        # Extract full text
        text = extract_text_from_html(content)

        if not text or len(text) < 100:
            return None

        # Try to extract date from content
        date = None
        if year:
            # Look for specific date pattern in the document
            date_pattern = re.search(r'(\d{1,2})\.\s*(janúar|febrúar|mars|apríl|maí|júní|júlí|ágúst|september|október|nóvember|desember)\s*(\d{4})', text[:2000], re.IGNORECASE)
            if date_pattern:
                day = date_pattern.group(1).zfill(2)
                month_name = date_pattern.group(2).lower()
                month_map = {
                    'janúar': '01', 'febrúar': '02', 'mars': '03', 'apríl': '04',
                    'maí': '05', 'júní': '06', 'júlí': '07', 'ágúst': '08',
                    'september': '09', 'október': '10', 'nóvember': '11', 'desember': '12'
                }
                month = month_map.get(month_name, '01')
                date = f"{date_pattern.group(3)}-{month}-{day}"
            else:
                # Default to January 1 of the law's year
                date = f"{year}-01-01"

        # Build URL
        url = f"https://www.althingi.is/lagas/{CURRENT_VERSION}/{filename}"

        return {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': text,
            'date': date,
            'url': url,
            'language': 'isl',
            'year': year,
            'law_number': law_num,
            'version': CURRENT_VERSION,
            'file_name': filename,
        }
    except Exception as e:
        print(f"  Error parsing {filename}: {e}")
        return None


def fetch_all(max_records: int = None, prioritize_recent: bool = True) -> Generator[dict, None, None]:
    """
    Fetch all Icelandic legislation from the ZIP archive.

    Args:
        max_records: Maximum number of records to yield (for sampling)
        prioritize_recent: If True, prioritize recent/significant laws for sampling

    Yields:
        Normalized document records
    """
    # Download ZIP
    data = download_zip()

    count = 0

    # Extract and parse ZIP
    with zipfile.ZipFile(data, 'r') as zf:
        # Get list of HTML files with law format (YYYYNNN.html)
        law_files = []
        for f in zf.namelist():
            basename = os.path.basename(f)
            if re.match(r'\d{7}\.html?$', basename):
                # Get file size for prioritization
                info = zf.getinfo(f)
                law_files.append((f, basename, info.file_size))

        # Sort by file size (larger = more content) for sampling
        if prioritize_recent and max_records:
            law_files.sort(key=lambda x: x[2], reverse=True)

        print(f"Found {len(law_files)} law files in archive")

        for filepath, filename, filesize in law_files:
            if max_records and count >= max_records:
                break

            try:
                content = zf.read(filepath).decode('iso-8859-1')  # Icelandic encoding

                record = parse_law_html(content, filename)
                if record and len(record.get('text', '')) >= 100:
                    yield record
                    count += 1
                    if count % 50 == 0:
                        print(f"  Processed {count} records...")
            except Exception as e:
                print(f"  Error extracting {filepath}: {e}")

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """
    Fetch documents updated since a given date.

    Note: The ZIP download is a snapshot - no incremental updates available.
    """
    for record in fetch_all():
        if record.get('date'):
            try:
                doc_date = datetime.fromisoformat(record['date'])
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def normalize(raw: dict) -> dict:
    """
    Transform raw data into standard schema.
    Already normalized in parse_law_html, so just validate.
    """
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

    print(f"Fetching {sample_count} sample records from IS/Lagasafn...")
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
    parser = argparse.ArgumentParser(description="IS/Lagasafn legislation fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'info'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")

    args = parser.parse_args()

    if args.command == 'info':
        print(f"IS/Lagasafn - Icelandic Consolidated Legislation")
        print(f"Source URL: {BASE_URL}")
        print(f"Current version: {CURRENT_VERSION}")
        print(f"ZIP download: {BASE_URL}/zip/{CURRENT_VERSION}/allt.zip")

    elif args.command == 'bootstrap':
        if args.sample or True:  # Always sample for bootstrap
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
