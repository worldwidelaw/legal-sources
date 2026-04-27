#!/usr/bin/env python3
"""
Swedish Supreme Administrative Court (Högsta förvaltningsdomstolen - HFD) Data Fetcher

Data source: Domstolsverket's Open Data API
API: https://rattspraxis.etjanst.domstol.se/api/v1
API docs: https://rattspraxis.etjanst.domstol.se/openapi/puh-openapi.yaml

This fetcher uses the official API to retrieve HFD decisions with full text content.
The API provides:
- Full text in HTML format via the 'innehall' field
- PDF attachments for official document versions
- Comprehensive metadata including case numbers, dates, keywords, legal provisions

Coverage: ~1,300+ decisions from the API (March 2025+ new decisions, plus historical referat)
Legacy archive (2008-2025) may require the RSS feed fallback.

No authentication required. Public domain court decisions.
"""

import argparse
import html
import io
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional, List, Dict

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


API_BASE = "https://rattspraxis.etjanst.domstol.se/api/v1"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "SE/SupremeAdministrativeCourt"
COURT_CODE = "HFD"
COURT_NAME = "Högsta förvaltningsdomstolen"

PUB_TYPES = {
    'DOM_ELLER_BESLUT': 'Judgment or decision',
    'RATTSFALL': 'Case report (RÅ)',
    'PROVNINGSTILLSTAND': 'Leave to appeal decision',
    'FORHANDSAVGORANDE': 'Preliminary ruling request',
}


def get_session() -> requests.Session:
    """Create a requests session with proper headers."""
    session = requests.Session()
    session.headers.update({
        'Accept': 'application/json',
        'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
    })
    return session


def html_to_text(html_content: str) -> str:
    """Convert HTML to plain text, preserving paragraph structure."""
    if not html_content:
        return ""

    # Replace common block elements with newlines
    text = re.sub(r'</(p|div|h[1-6]|li|tr)>', '\n', html_content, flags=re.IGNORECASE)
    text = re.sub(r'<(br|hr)\s*/?>', '\n', text, flags=re.IGNORECASE)

    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities
    text = html.unescape(text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)

    return text.strip()


def fetch_publications(
    page: int = 0,
    pagesize: int = 100,
    pub_types: str = None,
    sort_asc: bool = False,
) -> List[Dict]:
    """
    Fetch HFD publications from the API.

    Args:
        page: Page number (0-indexed)
        pagesize: Results per page (max 100)
        pub_types: Comma-separated publication types
        sort_asc: Sort ascending by date (default False = newest first)

    Returns:
        List of publication records
    """
    session = get_session()
    params = {
        'domstolkod': COURT_CODE,
        'page': page,
        'pagesize': pagesize,
        'sortorder': 'avgorandedatum',
        'asc': 'true' if sort_asc else 'false',
    }
    if pub_types:
        params['publiceringstyper'] = pub_types

    resp = session.get(f"{API_BASE}/publiceringar", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def download_attachment(storage_id: str) -> bytes:
    """Download an attachment (PDF) from the API."""
    from urllib.parse import quote
    session = requests.Session()
    session.headers.update({
        'Accept': 'application/octet-stream',
        'User-Agent': 'LegalDataHunter/1.0',
    })
    encoded_id = quote(storage_id, safe='')
    url = f"{API_BASE}/bilagor/{encoded_id}"

    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="SE/SupremeAdministrativeCourt",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def process_publication(pub: Dict) -> Optional[Dict]:
    """
    Process a publication and extract full text.

    First tries the HTML 'innehall' field, then falls back to PDF extraction.
    """
    pub_id = pub.get('id', '')
    case_numbers = pub.get('malNummerLista', [])
    case_num_str = case_numbers[0] if case_numbers else pub_id[:20]

    # Try HTML content first (most cases have this)
    html_content = pub.get('innehall', '')
    if html_content:
        text = html_to_text(html_content)
        if text and len(text) >= 200:
            pub['text'] = text
            pub['text_source'] = 'html'
            return pub

    # Fall back to PDF extraction
    attachments = pub.get('bilagaLista', [])
    if attachments:
        attachment = attachments[0]
        storage_id = attachment.get('fillagringId', '')
        filename = attachment.get('filnamn', '')

        if storage_id:
            try:
                print(f"    -> Downloading PDF: {filename}")
                pdf_bytes = download_attachment(storage_id)
                text = extract_pdf_text(pdf_bytes)

                if text and len(text) >= 200:
                    pub['text'] = text
                    pub['text_source'] = 'pdf'
                    pub['pdf_filename'] = filename
                    return pub
            except Exception as e:
                print(f"    -> PDF download/extraction error: {e}")

    # Fall back to summary if available
    summary = pub.get('sammanfattning', '')
    if summary and len(summary) >= 200:
        pub['text'] = summary
        pub['text_source'] = 'summary'
        return pub

    return None


def normalize(raw: Dict) -> Dict:
    """Transform raw API data into standard schema."""
    pub_id = raw.get('id', '')

    # Get case numbers
    case_numbers = raw.get('malNummerLista', [])
    primary_case = case_numbers[0] if case_numbers else pub_id

    # Get dates
    decision_date = raw.get('avgorandedatum', '')
    pub_time = raw.get('publiceringstid', '')

    # Build title
    title_parts = []
    if raw.get('benamning'):
        title_parts.append(raw['benamning'].strip())
    if primary_case:
        title_parts.append(f"Mål: {primary_case}")
    title = ' - '.join(title_parts) if title_parts else f"Mål: {primary_case}"

    # Get reference numbers (RÅ etc.)
    ref_numbers = raw.get('referatNummerLista', [])

    # Get legal provisions
    provisions = raw.get('lagrumLista', [])
    sfs_refs = [p.get('referens', '') for p in provisions if p.get('referens')]
    sfs_numbers = [p.get('sfsNummer', '') for p in provisions if p.get('sfsNummer')]

    # Get keywords
    keywords = raw.get('nyckelordLista', [])

    # Determine document type
    pub_type = raw.get('typ', '')
    is_precedent = raw.get('arVagledande', False)
    doc_type = 'precedent' if is_precedent else 'decision'

    # Create document ID
    doc_id = f"HFD-{primary_case}" if primary_case else f"HFD-{pub_id[:12]}"

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.utcnow().isoformat() + 'Z',
        'title': title,
        'text': raw.get('text', ''),
        'date': decision_date,
        'url': f"https://rattspraxis.etjanst.domstol.se/sok/?id={pub_id}",
        'court': COURT_NAME,
        'court_code': COURT_CODE,
        'case_numbers': case_numbers,
        'case_number': primary_case,
        'publication_type': pub_type,
        'publication_type_label': PUB_TYPES.get(pub_type, pub_type),
        'is_precedent': is_precedent,
        'document_type': doc_type,
        'published_at': pub_time,
        'reference_numbers': ref_numbers,
        'legal_provisions': sfs_refs,
        'sfs_numbers': sfs_numbers,
        'keywords': keywords,
        'summary': raw.get('sammanfattning', ''),
        'text_source': raw.get('text_source', ''),
        'pdf_filename': raw.get('pdf_filename', ''),
        'language': 'sv',
    }


def fetch_all(max_records: int = None) -> Generator[Dict, None, None]:
    """
    Fetch all HFD decisions from the API.

    Args:
        max_records: Maximum number of records to yield (None = all)

    Yields:
        Normalized document records with full text
    """
    page = 0
    pagesize = 100
    count = 0
    empty_pages = 0

    while True:
        if max_records and count >= max_records:
            break

        print(f"Fetching page {page} (records {page*pagesize}-{(page+1)*pagesize})...")

        try:
            publications = fetch_publications(page=page, pagesize=pagesize)
        except requests.HTTPError as e:
            print(f"Error fetching page {page}: {e}")
            break

        if not publications:
            empty_pages += 1
            if empty_pages >= 3:
                print("No more publications (3 empty pages)")
                break
            page += 1
            continue

        empty_pages = 0

        for pub in publications:
            if max_records and count >= max_records:
                break

            case_numbers = pub.get('malNummerLista', [])
            case_num = case_numbers[0] if case_numbers else pub.get('id', '')[:12]
            print(f"  [{count+1}] Processing {case_num}...")

            # Process to get full text
            pub_with_text = process_publication(pub)

            if pub_with_text and pub_with_text.get('text'):
                try:
                    normalized = normalize(pub_with_text)
                    text_len = len(normalized.get('text', ''))

                    if text_len >= 200:
                        source = normalized.get('text_source', 'unknown')
                        print(f"      -> {text_len:,} chars ({source})")
                        yield normalized
                        count += 1
                    else:
                        print(f"      -> Skipping: text too short ({text_len} chars)")
                except Exception as e:
                    print(f"      -> Error normalizing: {e}")
            else:
                print(f"      -> Skipping: no text extracted")

            # Rate limiting
            time.sleep(0.5)

        page += 1
        time.sleep(1.0)

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch documents updated since a given date."""
    since_str = since.strftime('%Y-%m-%d')

    for record in fetch_all():
        if record.get('date'):
            if record['date'] >= since_str:
                yield record


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print(f"Court: {COURT_CODE} ({COURT_NAME})")
    print("=" * 60)

    records = []

    for record in fetch_all(max_records=sample_count + 10):
        if len(records) >= sample_count:
            break

        try:
            text_len = len(record.get('text', ''))
            if text_len < 200:
                print(f"  Skipping {record.get('case_number')}: Text too short ({text_len} chars)")
                continue

            records.append(record)

            # Save individual record
            doc_id = record['_id'].replace('/', '_').replace(':', '-')
            filename = SAMPLE_DIR / f"{doc_id}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            source = record.get('text_source', 'unknown')
            title_preview = record.get('title', '')[:50]
            print(f"  [{len(records):02d}] {record['case_number']}: {text_len:,} chars ({source})")
            print(f"       {title_preview}...")

        except Exception as e:
            print(f"  Error saving record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        # Statistics
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        # Count by text source
        source_counts = {}
        for r in records:
            source = r.get('text_source', 'unknown')
            source_counts[source] = source_counts.get(source, 0) + 1

        print("Text sources:")
        for source, count in sorted(source_counts.items()):
            print(f"  {source}: {count}")

        # Count precedent vs regular
        precedent_count = sum(1 for r in records if r.get('is_precedent'))
        print(f"Precedent decisions: {precedent_count}/{len(records)}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    insufficient_text = sum(1 for r in records if not r.get('text') or len(r['text']) < 200)
    if insufficient_text > 0:
        print(f"WARNING: {insufficient_text} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="SE/SupremeAdministrativeCourt case law fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'test', 'count'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only (12 records)")
    parser.add_argument('--count', type=int, default=15,
                       help="Number of sample records to fetch")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'count':
        # Count total available records
        print("Counting total HFD records...")
        page = 0
        total = 0
        while True:
            try:
                pubs = fetch_publications(page=page, pagesize=100)
                if not pubs:
                    break
                total += len(pubs)
                print(f"  Page {page}: {len(pubs)} records (total: {total})")
                page += 1
                time.sleep(0.5)
            except Exception as e:
                print(f"Error at page {page}: {e}")
                break
        print(f"Total HFD records available: {total}")

    elif args.command == 'test':
        # Test a single fetch
        print("Testing API connection...")
        pubs = fetch_publications(page=0, pagesize=5)
        print(f"Found {len(pubs)} publications")
        if pubs:
            pub = pubs[0]
            print(f"First publication:")
            print(f"  ID: {pub.get('id')}")
            print(f"  Case numbers: {pub.get('malNummerLista', [])}")
            print(f"  Date: {pub.get('avgorandedatum')}")
            print(f"  Type: {pub.get('typ')}")
            print(f"  Has HTML content: {bool(pub.get('innehall'))}")
            print(f"  Attachments: {len(pub.get('bilagaLista', []))}")

    elif args.command == 'bootstrap':
        count = 12 if args.sample else args.count
        success = bootstrap_sample(count)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
