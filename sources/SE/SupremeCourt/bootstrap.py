#!/usr/bin/env python3
"""
SE/SupremeCourt - Swedish Supreme Court Case Law Fetcher

Fetches Swedish court decisions from Domstolsverket's open data API.
Covers all Swedish courts that publish precedent decisions:
- Högsta domstolen (HDO) - Supreme Court
- Högsta förvaltningsdomstolen (HFD) - Supreme Administrative Court
- Hovrätter (courts of appeal)
- Kammarrätter (administrative courts of appeal)

Data source: https://rattspraxis.etjanst.domstol.se/
API docs: https://rattspraxis.etjanst.domstol.se/openapi/puh-openapi.yaml
License: Open data (public domain court decisions)
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

import pdfplumber
import requests

API_BASE = "https://rattspraxis.etjanst.domstol.se/api/v1"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "SE/SupremeCourt"

# Court codes and names
COURT_CODES = {
    'HDO': 'Högsta domstolen',           # Supreme Court (civil/criminal)
    'HFD': 'Högsta förvaltningsdomstolen', # Supreme Administrative Court
    'ADO': 'Arbetsdomstolen',             # Labour Court
    'HSV': 'Svea hovrätt',                # Svea Court of Appeal
    'HGO': 'Göta hovrätt',                # Göta Court of Appeal
    'HVS': 'Hovrätten för Västra Sverige', # Court of Appeal Western Sweden
    'HON': 'Hovrätten för Övre Norrland',  # Court of Appeal Upper Norrland
    'HNN': 'Hovrätten för Nedre Norrland', # Court of Appeal Lower Norrland
    'HSB': 'Hovrätten över Skåne och Blekinge', # Court of Appeal Skåne/Blekinge
    'KST': 'Kammarrätten i Stockholm',     # Admin Court of Appeal Stockholm
    'KGG': 'Kammarrätten i Göteborg',      # Admin Court of Appeal Gothenburg
    'KJO': 'Kammarrätten i Jönköping',     # Admin Court of Appeal Jönköping
    'KSU': 'Kammarrätten i Sundsvall',     # Admin Court of Appeal Sundsvall
    'MMOD': 'Mark- och miljööverdomstolen', # Land & Environment Court of Appeal
    'MIOD': 'Migrationsöverdomstolen',     # Migration Court of Appeal
    'PMOD': 'Patent- och marknadsöverdomstolen', # Patent & Market Court of Appeal
}

# Publication types
PUB_TYPES = {
    'DOM_ELLER_BESLUT': 'Judgment or decision',
    'RATTSFALL': 'Case report (NJA/RÅ)',
    'PROVNINGSTILLSTAND': 'Leave to appeal decision',
    'FORHANDSAVGORANDE': 'Preliminary ruling request',
}


def get_session() -> requests.Session:
    """Create a requests session with proper headers."""
    session = requests.Session()
    session.headers.update({
        'Accept': 'application/json',
        'User-Agent': 'WorldWideLaw/1.0 (research; https://github.com/worldwidelaw/legal-sources)',
    })
    return session


def fetch_courts() -> list:
    """Fetch list of available courts from API."""
    session = get_session()
    resp = session.get(f"{API_BASE}/domstolar", timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_publications(
    court_code: str = None,
    page: int = 0,
    pagesize: int = 50,
    pub_types: str = None,
) -> dict:
    """
    Fetch publications from the API.

    Args:
        court_code: Court code filter (e.g., 'HDO' for Supreme Court)
        page: Page number (0-indexed)
        pagesize: Results per page
        pub_types: Comma-separated publication types

    Returns:
        API response with list of publications
    """
    session = get_session()
    params = {
        'page': page,
        'pagesize': pagesize,
        'sortorder': 'avgorandedatum',
        'asc': 'false',  # Most recent first
    }
    if court_code:
        params['domstolkod'] = court_code
    if pub_types:
        params['publiceringstyper'] = pub_types

    resp = session.get(f"{API_BASE}/publiceringar", params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_publication(pub_id: str) -> dict:
    """Fetch a single publication by ID."""
    session = get_session()
    resp = session.get(f"{API_BASE}/publiceringar/{pub_id}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def download_attachment(storage_id: str) -> bytes:
    """
    Download an attachment (PDF) from the API.

    Args:
        storage_id: The fillagringId from the publication

    Returns:
        Raw PDF bytes
    """
    session = requests.Session()
    session.headers.update({
        # Must use octet-stream for binary attachments
        'Accept': 'application/octet-stream',
        'User-Agent': 'WorldWideLaw/1.0 (research; https://github.com/worldwidelaw/legal-sources)',
    })
    # URL-encode the storage ID (contains slashes)
    encoded_id = quote(storage_id, safe='')
    url = f"{API_BASE}/bilagor/{encoded_id}"

    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """
    Extract text from a PDF document.

    Args:
        pdf_bytes: Raw PDF content

    Returns:
        Extracted text
    """
    text_parts = []

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        print(f"    -> PDF extraction error: {e}")
        return ''

    full_text = '\n\n'.join(text_parts)

    # Clean up common artifacts
    # Remove excessive whitespace
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)

    # Clean up page headers/footers patterns
    full_text = re.sub(r'Sida \d+ \(\d+\)\n', '', full_text)

    return full_text.strip()


def fetch_decision_with_text(publication: dict) -> Optional[dict]:
    """
    Fetch full text for a publication by downloading its PDF attachment.

    Args:
        publication: Publication data from API

    Returns:
        Publication with 'text' field added, or None if no text available
    """
    pub_id = publication.get('id', '')
    case_numbers = publication.get('malNummerLista', [])
    case_num_str = case_numbers[0] if case_numbers else pub_id[:20]

    print(f"  Processing {case_num_str}...")

    attachments = publication.get('bilagaLista', [])
    if not attachments:
        # No PDF attachment - use summary as text if available
        summary = publication.get('sammanfattning', '')
        if summary and len(summary) > 200:
            print(f"    -> No PDF, using summary ({len(summary)} chars)")
            publication['text'] = summary
            publication['text_source'] = 'summary'
            return publication
        else:
            print(f"    -> No PDF attachment and no sufficient summary")
            return None

    # Download and extract text from first PDF
    attachment = attachments[0]
    storage_id = attachment.get('fillagringId', '')
    filename = attachment.get('filnamn', '')

    if not storage_id:
        print(f"    -> No storage ID for attachment")
        return None

    try:
        print(f"    -> Downloading {filename}")
        pdf_bytes = download_attachment(storage_id)

        print(f"    -> Extracting text from PDF ({len(pdf_bytes):,} bytes)")
        text = extract_pdf_text(pdf_bytes)

        if not text or len(text) < 200:
            print(f"    -> Insufficient text extracted ({len(text) if text else 0} chars)")
            return None

        publication['text'] = text
        publication['text_source'] = 'pdf'
        publication['pdf_filename'] = filename
        print(f"    -> Extracted {len(text):,} chars")
        return publication

    except requests.HTTPError as e:
        print(f"    -> HTTP error downloading PDF: {e}")
        return None
    except Exception as e:
        print(f"    -> Error processing PDF: {e}")
        return None


def normalize(raw: dict) -> dict:
    """Transform raw API data into standard schema."""
    pub_id = raw.get('id', '')

    # Get case numbers
    case_numbers = raw.get('malNummerLista', [])
    primary_case = case_numbers[0] if case_numbers else pub_id

    # Get court info
    court_data = raw.get('domstol', {})
    court_code = court_data.get('domstolKod', '')
    court_name = court_data.get('domstolNamn', COURT_CODES.get(court_code, court_code))

    # Get dates
    decision_date = raw.get('avgorandedatum', '')
    pub_time = raw.get('publiceringstid', '')

    # Build title
    title_parts = []
    if raw.get('benamning'):
        title_parts.append(raw['benamning'].strip())
    if primary_case:
        title_parts.append(f"({primary_case})")
    title = ' '.join(title_parts) if title_parts else primary_case

    # Get reference numbers (NJA etc.)
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

    return {
        '_id': pub_id,
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.utcnow().isoformat() + 'Z',
        'title': title,
        'text': raw.get('text', ''),
        'date': decision_date,
        'url': f"https://rattspraxis.etjanst.domstol.se/sok/?id={pub_id}",
        'court': court_name,
        'court_code': court_code,
        'case_numbers': case_numbers,
        'primary_case_number': primary_case,
        'publication_type': pub_type,
        'publication_type_label': PUB_TYPES.get(pub_type, pub_type),
        'is_precedent': is_precedent,
        'document_type': doc_type,
        'published_at': pub_time,
        'reference_numbers': ref_numbers,  # NJA etc.
        'legal_provisions': sfs_refs,
        'sfs_numbers': sfs_numbers,
        'keywords': keywords,
        'summary': raw.get('sammanfattning', ''),
        'text_source': raw.get('text_source', ''),
        'pdf_filename': raw.get('pdf_filename', ''),
        'language': 'swe',
    }


def fetch_all(
    max_records: int = None,
    court_code: str = 'HDO',
    include_all_types: bool = True,
) -> Generator[dict, None, None]:
    """
    Fetch court decisions from the API.

    Args:
        max_records: Maximum number of records to yield
        court_code: Court code filter (default: HDO = Supreme Court)
        include_all_types: Include all publication types (default: True for full archive)

    Yields:
        Normalized document records with full text
    """
    page = 0
    pagesize = 50
    count = 0
    empty_pages = 0  # Track consecutive empty pages

    # Fetch all types by default for full archive coverage
    # Types: DOM_ELLER_BESLUT (judgments), PROVNINGSTILLSTAND (leave to appeal), REFERAT (case reports)
    pub_types = None
    if not include_all_types:
        # Only filter if explicitly requested - DOM_ELLER_BESLUT covers main judgments
        pub_types = 'DOM_ELLER_BESLUT'

    while True:
        if max_records and count >= max_records:
            break

        print(f"Fetching page {page} ({court_code or 'all courts'})... [{count} records so far]")

        try:
            publications = fetch_publications(
                court_code=court_code,
                page=page,
                pagesize=pagesize,
                pub_types=pub_types,
            )
        except requests.HTTPError as e:
            print(f"Error fetching publications: {e}")
            break

        if not publications:
            empty_pages += 1
            if empty_pages >= 3:
                print("No more publications (3 consecutive empty pages)")
                break
            page += 1
            continue

        empty_pages = 0  # Reset counter when we get results

        for pub in publications:
            if max_records and count >= max_records:
                break

            # Fetch full text
            pub_with_text = fetch_decision_with_text(pub)

            if pub_with_text and pub_with_text.get('text'):
                try:
                    normalized = normalize(pub_with_text)

                    # Validate text length
                    text_len = len(normalized.get('text', ''))
                    if text_len >= 500:
                        yield normalized
                        count += 1
                    else:
                        print(f"    -> Skipping: text too short ({text_len} chars)")

                except Exception as e:
                    print(f"    -> Error normalizing: {e}")

            # Rate limiting
            time.sleep(1.0)

        page += 1

        # Safety check - allow up to 500 pages (25K records at 50/page)
        if page > 500:
            print("Reached page limit (500)")
            break

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    for record in fetch_all():
        if record.get('date'):
            try:
                doc_date = datetime.strptime(record['date'], '%Y-%m-%d')
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def bootstrap_sample(sample_count: int = 12, court_code: str = 'HDO'):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print(f"Court filter: {court_code or 'all courts'}")
    print("=" * 60)

    # First, show available courts
    print("Available courts:")
    try:
        courts = fetch_courts()
        for court in courts:
            code = court.get('domstolKod', '')
            name = court.get('domstolNamn', '')
            marker = ' <-- target' if code == court_code else ''
            print(f"  {code}: {name}{marker}")
        print()
    except Exception as e:
        print(f"Could not fetch courts: {e}")

    records = []
    # Fetch extra in case some fail PDF extraction
    for record in fetch_all(max_records=sample_count + 5, court_code=court_code):
        if len(records) >= sample_count:
            break

        try:
            # Validate record has full text
            text_len = len(record.get('text', ''))
            if text_len < 500:
                print(f"  Skipping {record.get('primary_case_number')}: Text too short ({text_len} chars)")
                continue

            records.append(record)

            # Save individual record
            filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            title_preview = record.get('title', '')[:50]
            source = record.get('text_source', 'unknown')
            print(f"  [{len(records):02d}] {record['primary_case_number']}: {text_len:,} chars ({source})")
            print(f"       {title_preview}...")

        except Exception as e:
            print(f"  Error saving record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        # Statistics
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        # Count by publication type
        type_counts = {}
        for r in records:
            ptype = r.get('publication_type_label', 'Unknown')
            type_counts[ptype] = type_counts.get(ptype, 0) + 1

        print("Publication types:")
        for ptype, count in sorted(type_counts.items()):
            print(f"  {ptype}: {count}")

        # Count precedent vs regular
        precedent_count = sum(1 for r in records if r.get('is_precedent'))
        print(f"Precedent decisions: {precedent_count}/{len(records)}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    insufficient_text = sum(1 for r in records if not r.get('text') or len(r['text']) < 500)
    if insufficient_text > 0:
        print(f"WARNING: {insufficient_text} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="SE/SupremeCourt case law fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'courts', 'test'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument('--court', type=str, default='HDO',
                       help="Court code filter (e.g., HDO, HFD). Use '' for all courts")
    parser.add_argument('--all-types', action='store_true',
                       help="Include all publication types")

    args = parser.parse_args()

    if args.command == 'courts':
        # List available courts
        courts = fetch_courts()
        print("Available courts:")
        for court in courts:
            print(f"  {court['domstolKod']}: {court['domstolNamn']}")

    elif args.command == 'test':
        # Test a single fetch
        print("Testing API connection...")
        pubs = fetch_publications(court_code='HDO', page=0, pagesize=3)
        print(f"Found {len(pubs)} publications")
        if pubs:
            pub = pubs[0]
            print(f"First publication: {pub.get('malNummerLista', [])}")
            print(f"  Type: {pub.get('typ')}")
            print(f"  Date: {pub.get('avgorandedatum')}")
            print(f"  Attachments: {len(pub.get('bilagaLista', []))}")

            # Try to fetch full text
            pub_with_text = fetch_decision_with_text(pub)
            if pub_with_text:
                text_len = len(pub_with_text.get('text', ''))
                print(f"  Text: {text_len:,} chars")

    elif args.command == 'bootstrap':
        court = args.court if args.court else None
        success = bootstrap_sample(args.count, court_code=court)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        court = args.court if args.court else None
        for record in fetch_all(court_code=court, include_all_types=args.all_types):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
