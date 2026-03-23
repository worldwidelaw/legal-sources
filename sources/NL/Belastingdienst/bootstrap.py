#!/usr/bin/env python3
"""
NL/Belastingdienst - Dutch Tax Administration doctrine

Fetches tax policy decisions (beleidsbesluiten) and policy rules (beleidsregels)
from the Ministry of Finance via the SRU API at zoek.officielebekendmakingen.nl.
Full text is downloaded as structured XML from the Staatscourant.

Data source:
- https://zoek.officielebekendmakingen.nl (SRU Search/Retrieve API)

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all documents
"""

import argparse
import json
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

# Constants
SOURCE_ID = "NL/Belastingdienst"
SRU_BASE = "https://zoek.officielebekendmakingen.nl/sru/Search"
FULL_TEXT_BASE = "https://zoek.officielebekendmakingen.nl"
MAX_RECORDS = 100

# SRU queries for tax doctrine
SRU_QUERIES = [
    # Beleidsbesluiten (policy decisions) - ~2354 docs
    'creator="ministerie van financiën" AND type="ander besluit van algemene strekking"',
    # Beleidsregels (policy rules) - ~34 docs
    'creator="ministerie van financiën" AND type="beleidsregel"',
]

RATE_LIMIT_DELAY = 1.5
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"

# XML namespaces used in SRU responses
NS = {
    'srw': 'http://www.loc.gov/zing/srw/',
    'gzd': 'http://standaarden.overheid.nl/sru',
    'dcterms': 'http://purl.org/dc/terms/',
    'overheid': 'http://standaarden.overheid.nl/owms/terms/',
    'overheidop': 'http://standaarden.overheid.nl/product/terms/',
}


def curl_fetch(url: str) -> Optional[str]:
    """Fetch URL using curl subprocess."""
    try:
        cmd = [
            "curl", "-s", "-L",
            "-H", f"User-Agent: {USER_AGENT}",
            "-H", "Accept: application/xml,text/xml,text/html;q=0.9,*/*;q=0.8",
            "--max-time", "60",
            url
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=90)
        if result.returncode != 0:
            print(f"curl error for {url}: {result.stderr.decode()}", file=sys.stderr)
            return None
        return result.stdout.decode("utf-8", errors="replace")
    except subprocess.TimeoutExpired:
        print(f"Timeout fetching {url}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def sru_search(query: str, start_record: int = 1, max_records: int = MAX_RECORDS) -> Optional[ET.Element]:
    """Execute an SRU search query and return parsed XML."""
    url = (
        f"{SRU_BASE}?version=1.2&operation=searchRetrieve"
        f"&query={quote(query)}"
        f"&startRecord={start_record}"
        f"&maximumRecords={max_records}"
        f"&recordSchema=gzd"
    )
    content = curl_fetch(url)
    if not content:
        return None
    try:
        return ET.fromstring(content)
    except ET.ParseError as e:
        print(f"XML parse error in SRU response: {e}", file=sys.stderr)
        return None


def parse_sru_records(root: ET.Element) -> list[dict]:
    """Parse SRU response to extract document metadata."""
    records = []
    for rec in root.findall('.//srw:record', NS):
        data = rec.find('.//srw:recordData', NS)
        if data is None:
            continue

        # Search for dcterms fields anywhere in the record (nested structure)
        meta = {}
        for field in ['identifier', 'title', 'type', 'modified', 'issued', 'subject', 'creator', 'date']:
            els = data.findall(f'.//{{{NS["dcterms"]}}}{field}')
            if els:
                values = [el.text.strip() for el in els if el.text and el.text.strip()]
                if values:
                    meta[field] = values[0] if len(values) == 1 else values

        if meta.get('identifier'):
            records.append(meta)

    return records


def get_total_results(root: ET.Element) -> int:
    """Get total number of results from SRU response."""
    el = root.find('.//srw:numberOfRecords', NS)
    if el is not None and el.text:
        return int(el.text)
    return 0


def fetch_full_text_xml(identifier: str) -> str:
    """Fetch and extract full text from document XML."""
    url = f"{FULL_TEXT_BASE}/{identifier}.xml"
    content = curl_fetch(url)
    if not content:
        return ""

    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return ""

    # Extract text from all <al> (alinea/paragraph) elements
    text_parts = []

    # Try multiple paths for text content
    for el in root.iter():
        tag = el.tag.split('}')[-1] if '}' in el.tag else el.tag

        if tag == 'al':
            # Paragraph text
            text = ''.join(el.itertext()).strip()
            if text:
                text_parts.append(text)
        elif tag == 'tussenkop' or tag == 'kop':
            # Section headings
            text = ''.join(el.itertext()).strip()
            if text:
                text_parts.append(f"\n## {text}")
        elif tag == 'table-entry':
            text = ''.join(el.itertext()).strip()
            if text:
                text_parts.append(text)

    full_text = '\n\n'.join(text_parts)

    # If structured extraction got nothing, try raw text
    if not full_text or len(full_text) < 100:
        raw_text = ''.join(root.itertext())
        raw_text = re.sub(r'\s+', ' ', raw_text).strip()
        if len(raw_text) > len(full_text):
            full_text = raw_text

    # Clean up
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    return full_text.strip()


def normalize(meta: dict, full_text: str) -> dict:
    """Normalize a record into the standard schema."""
    identifier = meta.get('identifier', '')

    # Date: prefer issued, then modified
    date = None
    for field in ['issued', 'modified']:
        val = meta.get(field)
        if val:
            if isinstance(val, list):
                val = val[0]
            match = re.match(r'\d{4}-\d{2}-\d{2}', str(val))
            if match:
                date = match.group(0)
                break

    # Subjects
    subjects = meta.get('subject', [])
    if isinstance(subjects, str):
        subjects = [subjects]

    return {
        '_id': identifier,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': meta.get('title', identifier),
        'text': full_text,
        'date': date,
        'url': f"{FULL_TEXT_BASE}/{identifier}",
        'doc_type': meta.get('type', ''),
        'subjects': subjects,
        'creator': meta.get('creator', ''),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all tax doctrine documents."""
    for qi, query in enumerate(SRU_QUERIES):
        print(f"\nQuery {qi+1}/{len(SRU_QUERIES)}: {query[:80]}...", file=sys.stderr)

        # First request to get total count
        root = sru_search(query, start_record=1, max_records=MAX_RECORDS)
        if root is None:
            print("  SRU search failed, skipping query", file=sys.stderr)
            continue

        total = get_total_results(root)
        print(f"  Total results: {total}", file=sys.stderr)

        records = parse_sru_records(root)
        all_records = list(records)

        # Paginate if not sample mode
        if not sample:
            start = 1 + MAX_RECORDS
            while start <= total:
                time.sleep(1.0)
                root = sru_search(query, start_record=start)
                if root is None:
                    break
                page_records = parse_sru_records(root)
                if not page_records:
                    break
                all_records.extend(page_records)
                print(f"  Fetched metadata: {len(all_records)}/{total}", file=sys.stderr)
                start += MAX_RECORDS

        limit = 8 if sample else len(all_records)
        print(f"  Processing {limit} documents...", file=sys.stderr)

        for i, meta in enumerate(all_records[:limit]):
            identifier = meta.get('identifier', '')
            print(f"  [{i+1}/{limit}] Fetching full text: {identifier}", file=sys.stderr)

            full_text = fetch_full_text_xml(identifier)
            if not full_text or len(full_text) < 50:
                print(f"    Skipping: insufficient text ({len(full_text)} chars)", file=sys.stderr)
                continue

            record = normalize(meta, full_text)
            yield record

            time.sleep(RATE_LIMIT_DELAY)


def bootstrap(sample: bool = True):
    """Bootstrap the data source."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', record['_id'])
        fname = SAMPLE_DIR / f"{safe_id[:100]}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        print(f"    Saved: {record['title'][:80]} ({len(record['text'])} chars)", file=sys.stderr)

    print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}", file=sys.stderr)
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NL/Belastingdienst Bootstrap')
    sub = parser.add_subparsers(dest='command')
    boot = sub.add_parser('bootstrap')
    boot.add_argument('--sample', action='store_true', default=True)
    boot.add_argument('--full', action='store_true')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        sample = not args.full
        bootstrap(sample=sample)
    else:
        parser.print_help()
