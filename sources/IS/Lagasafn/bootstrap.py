#!/usr/bin/env python3
"""
IS/Lagasafn - Icelandic Consolidated Legislation Fetcher

Fetches all Icelandic statutes from the official Lagasafn (Law Collection)
published by the Althingi (Parliament of Iceland).

Data source: https://www.althingi.is/lagasafn/
Fallback: https://github.com/althingi-net/lagasafn-xml (XML format)
License: Public Domain (official government publications)
"""

import argparse
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.althingi.is/lagasafn"
CURRENT_VERSION = "156b"  # September 2025 compilation
GITHUB_XML_BASE = "https://raw.githubusercontent.com/althingi-net/lagasafn-xml/master/data/xml"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/Lagasafn"

# Browser-like headers to avoid 403 Forbidden from datacenter IPs
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': 'https://www.althingi.is/lagasafn/',
}


def download_zip(version: str = CURRENT_VERSION) -> Optional[BytesIO]:
    """Download the law collection ZIP file from althingi.is."""
    url = f"{BASE_URL}/zip/{version}/allt.zip"
    print(f"Downloading Icelandic law collection from {url}...")

    try:
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
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 403:
            print(f"  403 Forbidden from althingi.is (datacenter IP blocked)")
            return None
        raise


def clone_github_repo(version: str = CURRENT_VERSION) -> Optional[Path]:
    """Clone GitHub repository with sparse checkout to get XML files."""
    import subprocess
    import tempfile

    clone_dir = Path(tempfile.mkdtemp(prefix="lagasafn_"))
    print(f"Cloning GitHub repository to {clone_dir}...")

    try:
        # Shallow clone with sparse checkout
        subprocess.run([
            'git', 'clone', '--depth', '1', '--filter=blob:none', '--sparse',
            'https://github.com/althingi-net/lagasafn-xml.git',
            str(clone_dir / 'repo')
        ], check=True, capture_output=True, timeout=120)

        repo_dir = clone_dir / 'repo'

        # Initialize sparse checkout
        subprocess.run(
            ['git', 'sparse-checkout', 'init', '--cone'],
            cwd=repo_dir, check=True, capture_output=True, timeout=30
        )

        # Check which versions are available
        subprocess.run(
            ['git', 'sparse-checkout', 'set', f'data/xml/{version}'],
            cwd=repo_dir, check=True, capture_output=True, timeout=120
        )

        xml_dir = repo_dir / 'data' / 'xml' / version
        if xml_dir.exists():
            return xml_dir

        # Try fallback version
        print(f"  Version {version} not found, trying 155...")
        subprocess.run(
            ['git', 'sparse-checkout', 'set', 'data/xml/155'],
            cwd=repo_dir, check=True, capture_output=True, timeout=120
        )
        xml_dir = repo_dir / 'data' / 'xml' / '155'
        if xml_dir.exists():
            return xml_dir

        return None
    except subprocess.TimeoutExpired:
        print("  Git clone timed out")
        return None
    except subprocess.CalledProcessError as e:
        print(f"  Git error: {e.stderr.decode() if e.stderr else str(e)}")
        return None


def parse_law_xml(xml_content: str, filename: str) -> Optional[dict]:
    """Parse Icelandic law XML document and extract metadata and text."""
    try:
        root = ET.fromstring(xml_content)

        # Extract basic metadata from attributes
        year = root.get('year')
        law_num = root.get('nr')
        doc_id = f"{year}/{law_num}" if year and law_num else filename.replace('.xml', '')

        # Extract title from <name> element
        name_elem = root.find('name')
        title = name_elem.text.strip() if name_elem is not None and name_elem.text else f"Lög nr. {law_num}/{year}"

        # Extract date from <num-and-date>/<date>
        date_elem = root.find('.//num-and-date/date')
        date = date_elem.text.strip() if date_elem is not None and date_elem.text else None
        if not date and year:
            date = f"{year}-01-01"

        # Extract all text content from the document
        def extract_text(elem, depth=0):
            """Recursively extract text from XML elements."""
            parts = []

            # Add element's direct text
            if elem.text and elem.text.strip():
                parts.append(elem.text.strip())

            # Process children
            for child in elem:
                # Skip certain elements
                if child.tag in ['minister-clause', 'meta']:
                    continue

                # Add section headers
                if child.tag in ['chapter', 'section']:
                    nr_title = child.find('nr-title')
                    if nr_title is not None and nr_title.text:
                        parts.append(f"\n{nr_title.text.strip()}\n")
                    name_elem = child.find('name')
                    if name_elem is not None and name_elem.text:
                        parts.append(name_elem.text.strip())

                # Add article headers
                if child.tag == 'art':
                    nr_title = child.find('nr-title')
                    if nr_title is not None and nr_title.text:
                        parts.append(f"\n{nr_title.text.strip()}")

                # Recursively extract from child
                child_text = extract_text(child, depth + 1)
                if child_text:
                    parts.append(child_text)

                # Add tail text
                if child.tail and child.tail.strip():
                    parts.append(child.tail.strip())

            return ' '.join(parts)

        text = extract_text(root)

        # Clean up text
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r' ([.,;:])', r'\1', text)
        text = text.strip()

        if not text or len(text) < 100:
            return None

        # Build URL to official source
        url = f"https://www.althingi.is/lagas/{CURRENT_VERSION}/{year}{law_num.zfill(3)}.html"

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
            'data_source': 'github_xml',
        }
    except Exception as e:
        print(f"  Error parsing XML {filename}: {e}")
        return None


def fetch_from_github(max_records: int = None, prioritize_recent: bool = True) -> Generator[dict, None, None]:
    """Fetch Icelandic legislation from GitHub XML repository via sparse checkout."""
    import shutil

    print("Using GitHub XML fallback (althingi-net/lagasafn-xml)...")

    xml_dir = clone_github_repo()
    if xml_dir is None:
        print("  Failed to clone GitHub repository")
        return

    try:
        # Get all XML files
        xml_files = list(xml_dir.glob('*.xml'))
        print(f"  Found {len(xml_files)} XML files")

        # Sort by filename to get recent laws first (newer laws have higher numbers)
        if prioritize_recent and max_records:
            def sort_key(f):
                match = re.match(r'(\d{4})\.(\d+)\.xml', f.name)
                if match:
                    return (int(match.group(1)), int(match.group(2)))
                return (0, 0)
            xml_files.sort(key=sort_key, reverse=True)
        else:
            # For full fetch, sort by size (larger files first for sampling)
            xml_files.sort(key=lambda f: f.stat().st_size, reverse=True)

        count = 0
        for xml_file in xml_files:
            if max_records and count >= max_records:
                break

            try:
                content = xml_file.read_text(encoding='utf-8')
                record = parse_law_xml(content, xml_file.name)
                if record and len(record.get('text', '')) >= 100:
                    yield record
                    count += 1
                    if count % 50 == 0:
                        print(f"  Processed {count} records from GitHub...")
            except Exception as e:
                print(f"  Error parsing {xml_file.name}: {e}")

        print(f"Total records yielded from GitHub: {count}")
    finally:
        # Clean up cloned repo
        try:
            shutil.rmtree(xml_dir.parent.parent)
        except Exception:
            pass


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
            'data_source': 'althingi_zip',
        }
    except Exception as e:
        print(f"  Error parsing {filename}: {e}")
        return None


def fetch_from_zip(max_records: int = None, prioritize_recent: bool = True) -> Generator[dict, None, None]:
    """Fetch Icelandic legislation from the official ZIP archive."""
    data = download_zip()

    if data is None:
        return  # Caller should fall back to GitHub

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


def fetch_all(max_records: int = None, prioritize_recent: bool = True) -> Generator[dict, None, None]:
    """
    Fetch all Icelandic legislation.

    Tries official althingi.is ZIP first, falls back to GitHub XML if blocked.

    Args:
        max_records: Maximum number of records to yield (for sampling)
        prioritize_recent: If True, prioritize recent/significant laws for sampling

    Yields:
        Normalized document records
    """
    # Try official source first
    records_yielded = False
    try:
        for record in fetch_from_zip(max_records, prioritize_recent):
            records_yielded = True
            yield record
    except Exception as e:
        print(f"ZIP download failed: {e}")
        records_yielded = False

    # Fall back to GitHub XML if ZIP failed (403 or other error)
    if not records_yielded:
        print("Falling back to GitHub XML repository...")
        for record in fetch_from_github(max_records, prioritize_recent):
            yield record


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
    Already normalized in parse_law_html/parse_law_xml, so just validate.
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
    parser.add_argument('--github', action='store_true',
                       help="Force use of GitHub XML fallback")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'info':
        print(f"IS/Lagasafn - Icelandic Consolidated Legislation")
        print(f"Source URL: {BASE_URL}")
        print(f"Current version: {CURRENT_VERSION}")
        print(f"ZIP download: {BASE_URL}/zip/{CURRENT_VERSION}/allt.zip")
        print(f"GitHub fallback: {GITHUB_XML_BASE}/{CURRENT_VERSION}/")

    elif args.command == 'bootstrap':
        if args.github:
            # Force GitHub fallback for testing
            print("Forcing GitHub XML fallback...")
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            records = []
            for i, record in enumerate(fetch_from_github(max_records=args.count)):
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
            print(f"Saved {len(records)} records")
            sys.exit(0 if len(records) >= 10 else 1)
        else:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
