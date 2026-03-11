#!/usr/bin/env python3
"""
NO/Lovdata - Norwegian Legislation Fetcher

Fetches Norwegian laws and regulations from the Lovdata API.
Uses free bulk download endpoint - no authentication required.

Data source: https://api.lovdata.no
License: Norwegian Licence for Open Government Data (NLOD) 2.0
"""

import argparse
import json
import os
import re
import sys
import tarfile
import tempfile
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://api.lovdata.no"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "NO/Lovdata"


def fetch_dataset_list() -> list:
    """Get list of available bulk download datasets."""
    url = f"{BASE_URL}/v1/publicData/list"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def download_bulk_file(filename: str) -> BytesIO:
    """Download a bulk dataset file."""
    url = f"{BASE_URL}/v1/publicData/get/{filename}"
    print(f"Downloading {filename}...")
    resp = requests.get(url, timeout=300, stream=True)
    resp.raise_for_status()

    # Stream to BytesIO
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
    """Extract clean text from HTML content."""
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove script and style elements
    for element in soup(['script', 'style', 'nav', 'header', 'footer']):
        element.decompose()

    # Get text
    text = soup.get_text(separator='\n', strip=True)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def parse_lovdata_xml(content: str, filename: str) -> Optional[dict]:
    """Parse Lovdata XML/HTML document and extract metadata and text."""
    try:
        soup = BeautifulSoup(content, 'html.parser')

        # Try to extract document metadata
        doc_id = filename.replace('.html', '').replace('.xml', '')

        # Extract title - try multiple possible locations
        title = None
        title_elem = soup.find('h1') or soup.find('title') or soup.find('meta', attrs={'name': 'title'})
        if title_elem:
            title = title_elem.get_text(strip=True) if hasattr(title_elem, 'get_text') else title_elem.get('content', '')

        if not title:
            title = doc_id

        # Extract full text
        text = extract_text_from_html(content)

        if not text or len(text) < 100:
            return None

        # Try to extract date from content or metadata
        date = None
        date_elem = soup.find('meta', attrs={'name': 'DC.Date'}) or soup.find('meta', attrs={'name': 'date'})
        if date_elem:
            date = date_elem.get('content', '')

        # Look for date patterns in the text (Norwegian format: DD.MM.YYYY or YYYY-MM-DD)
        if not date:
            date_match = re.search(r'(\d{4}-\d{2}-\d{2})', text[:500])
            if date_match:
                date = date_match.group(1)
            else:
                date_match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', text[:500])
                if date_match:
                    date = f"{date_match.group(3)}-{date_match.group(2).zfill(2)}-{date_match.group(1).zfill(2)}"

        # Determine document type from filename
        doc_type = "legislation"
        if 'forskrift' in filename.lower():
            doc_type = "regulation"
        elif 'lov' in filename.lower():
            doc_type = "law"

        # Build ELI URI if possible
        eli_uri = f"https://lovdata.no/eli/{doc_id}"

        return {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': doc_type,
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': text,
            'date': date,
            'url': eli_uri,
            'language': 'nob',  # Norwegian Bokmål
            'file_name': filename,
        }
    except Exception as e:
        print(f"  Error parsing {filename}: {e}")
        return None


def process_tar_file(filename: str, max_records: int = None, current_count: int = 0) -> Generator[dict, None, None]:
    """
    Process a single tar.bz2 file and yield records.

    Args:
        filename: Name of the bulk file to download
        max_records: Maximum total records to yield (None = unlimited)
        current_count: Current record count (for limit tracking)

    Yields:
        Normalized document records
    """
    data = download_bulk_file(filename)
    count = 0

    with tarfile.open(fileobj=data, mode='r:bz2') as tar:
        members = [m for m in tar.getmembers() if m.isfile() and (m.name.endswith('.html') or m.name.endswith('.xml'))]
        print(f"Found {len(members)} document files in {filename}")

        for member in members:
            if max_records and (current_count + count) >= max_records:
                break

            try:
                f = tar.extractfile(member)
                if f:
                    content = f.read().decode('utf-8', errors='replace')
                    record = parse_lovdata_xml(content, os.path.basename(member.name))
                    if record and len(record.get('text', '')) >= 100:
                        yield record
                        count += 1
                        if count % 100 == 0:
                            print(f"  Processed {count} records from {filename}...")
            except Exception as e:
                print(f"  Error extracting {member.name}: {e}")

    print(f"Yielded {count} records from {filename}")


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """
    Fetch all Norwegian legislation from bulk downloads.

    Downloads both current laws (gjeldende-lover) AND current regulations
    (gjeldende-sentrale-forskrifter) for comprehensive coverage.

    Args:
        max_records: Maximum number of records to yield (for sampling)

    Yields:
        Normalized document records
    """
    # Get dataset list
    datasets = fetch_dataset_list()
    print(f"Available datasets: {len(datasets)}")
    for ds in datasets:
        size_mb = int(ds['sizeBytes']) / (1024 * 1024)
        print(f"  - {ds['filename']}: {size_mb:.1f} MB - {ds['description']}")

    count = 0

    # Files to process (both current laws and regulations)
    bulk_files = [
        "gjeldende-lover.tar.bz2",           # Current laws (~775 records)
        "gjeldende-sentrale-forskrifter.tar.bz2",  # Current regulations (~2,500+ records)
    ]

    for bulk_file in bulk_files:
        print(f"\n--- Processing {bulk_file} ---")
        for record in process_tar_file(bulk_file, max_records, count):
            yield record
            count += 1

        if max_records and count >= max_records:
            break

    print(f"\nTotal records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """
    Fetch documents updated since a given date.

    Note: The bulk download doesn't support incremental updates,
    so this fetches all and filters by date.
    """
    for record in fetch_all():
        if record.get('date'):
            try:
                doc_date = datetime.fromisoformat(record['date'].replace('Z', '+00:00'))
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                # If date parsing fails, include the record
                yield record


def normalize(raw: dict) -> dict:
    """
    Transform raw data into standard schema.
    Already normalized in parse_lovdata_xml, so just validate.
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

    print(f"Fetching {sample_count} sample records from NO/Lovdata...")
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
            print(f"  [{i+1:02d}] {normalized['_id'][:50]}: {text_len:,} chars")

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
    parser = argparse.ArgumentParser(description="NO/Lovdata legislation fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'list'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")

    args = parser.parse_args()

    if args.command == 'list':
        datasets = fetch_dataset_list()
        print("Available datasets:")
        for ds in datasets:
            size_mb = int(ds['sizeBytes']) / (1024 * 1024)
            print(f"  {ds['filename']}: {size_mb:.1f} MB - {ds['description']}")

    elif args.command == 'bootstrap':
        if args.sample or True:  # Always sample for bootstrap
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
