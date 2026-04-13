#!/usr/bin/env python3
"""
SE/Domstolverket - Swedish Courts Case Law (All Courts)

Fetches case law from ALL Swedish courts via Domstolsverket's Open Data API.
This complements SE/SupremeCourt (HDO only) and SE/SupremeAdministrativeCourt (HFD only)
by covering all other courts including:

- Hovrätter (Courts of Appeal): HVS, HGO, HON, HNN, HSV, HSB
- Kammarrätter (Administrative Courts of Appeal): KST, KGG, KJO, KSU
- Specialized Courts: ADO, MMOD, MIOD, PMOD, MDO, etc.

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
from typing import Generator, Optional, List, Dict
from urllib.parse import quote

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


API_BASE = "https://rattspraxis.etjanst.domstol.se/api/v1"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "SE/Domstolverket"

# Court codes and names (all courts from the API)
COURT_CODES = {
    # Supreme Courts (covered separately but included for completeness)
    'HDO': 'Högsta domstolen',                 # Supreme Court (civil/criminal)
    'HFD': 'Högsta förvaltningsdomstolen',     # Supreme Administrative Court
    'REGR': 'Regeringsrätten',                  # Government Court (historical)

    # Courts of Appeal (hovrätter)
    'HSV': 'Svea hovrätt',                      # Svea Court of Appeal
    'HGO': 'Göta hovrätt',                      # Göta Court of Appeal
    'HVS': 'Hovrätten för Västra Sverige',     # Court of Appeal Western Sweden
    'HON': 'Hovrätten för Övre Norrland',      # Court of Appeal Upper Norrland
    'HNN': 'Hovrätten för Nedre Norrland',     # Court of Appeal Lower Norrland
    'HSB': 'Hovrätten över Skåne och Blekinge', # Court of Appeal Skåne/Blekinge
    'HYOD': 'Svea hovrätts hyresrättsliga avgöranden', # Svea housing law decisions

    # Administrative Courts of Appeal (kammarrätter)
    'KST': 'Kammarrätten i Stockholm',          # Admin Court of Appeal Stockholm
    'KGG': 'Kammarrätten i Göteborg',           # Admin Court of Appeal Gothenburg
    'KJO': 'Kammarrätten i Jönköping',          # Admin Court of Appeal Jönköping
    'KSU': 'Kammarrätten i Sundsvall',          # Admin Court of Appeal Sundsvall

    # Specialized Courts
    'ADO': 'Arbetsdomstolen',                   # Labour Court
    'MMOD': 'Mark- och miljööverdomstolen',    # Land & Environment Court of Appeal
    'MOD': 'Miljööverdomstolen',                # Environment Court of Appeal (historical)
    'MIOD': 'Migrationsöverdomstolen',          # Migration Court of Appeal
    'PMOD': 'Patent- och marknadsöverdomstolen', # Patent & Market Court of Appeal
    'PBR': 'Patentbesvärsrätten',               # Patent Appeals Court (historical)
    'MDO': 'Marknadsdomstolen',                 # Market Court (historical)
    'RHN': 'Rättshjälpsnämnden',                # Legal Aid Board
    'DOV': 'Domstolsverket',                    # Courts Administration
}

# Courts to EXCLUDE (covered by separate sources)
EXCLUDED_COURTS = {'HDO', 'HFD'}

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
        'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
    })
    return session


def fetch_courts() -> List[Dict]:
    """Fetch list of available courts from API."""
    session = get_session()
    resp = session.get(f"{API_BASE}/domstolar", timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_publications_page(
    page: int = 0,
    pagesize: int = 100,
    pub_types: str = None,
    exclude_courts: List[str] = None,
) -> tuple[List[Dict], int]:
    """
    Fetch a single page of publications from all courts.

    Args:
        page: Page number (0-indexed)
        pagesize: Results per page (max 100)
        pub_types: Comma-separated publication types
        exclude_courts: Court codes to exclude from results

    Returns:
        Tuple of (list of publication records, total count)
    """
    session = get_session()
    params = {
        'page': page,
        'pagesize': min(pagesize, 100),  # API max is 100
        'sortorder': 'avgorandedatum',
        'asc': 'false',  # Most recent first
    }
    if pub_types:
        params['publiceringstyper'] = pub_types

    resp = session.get(f"{API_BASE}/publiceringar", params=params, timeout=30)
    resp.raise_for_status()

    total_count = int(resp.headers.get('x-total-count', 0))
    publications = resp.json()

    # Filter out excluded courts if specified
    if exclude_courts:
        publications = [
            p for p in publications
            if p.get('domstol', {}).get('domstolKod') not in exclude_courts
        ]

    return publications, total_count


def fetch_publication(pub_id: str) -> Dict:
    """Fetch a single publication by ID."""
    session = get_session()
    resp = session.get(f"{API_BASE}/publiceringar/{pub_id}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def download_attachment(storage_id: str) -> bytes:
    """Download an attachment (PDF) from the API."""
    session = requests.Session()
    session.headers.update({
        'Accept': 'application/octet-stream',
        'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
    })
    encoded_id = quote(storage_id, safe='')
    url = f"{API_BASE}/bilagor/{encoded_id}"

    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    return resp.content


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="SE/Domstolverket",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def fetch_decision_with_text(publication: Dict) -> Optional[Dict]:
    """Fetch full text for a publication by downloading its PDF attachment."""
    pub_id = publication.get('id', '')
    case_numbers = publication.get('malNummerLista', [])
    case_num_str = case_numbers[0] if case_numbers else pub_id[:20]

    print(f"  Processing {case_num_str}...")

    attachments = publication.get('bilagaLista', [])
    if not attachments:
        # No PDF - use summary if available
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


def normalize(raw: Dict) -> Dict:
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
        'reference_numbers': ref_numbers,
        'legal_provisions': sfs_refs,
        'sfs_numbers': sfs_numbers,
        'keywords': keywords,
        'summary': raw.get('sammanfattning', ''),
        'text_source': raw.get('text_source', ''),
        'pdf_filename': raw.get('pdf_filename', ''),
        'language': 'swe',
    }


def get_target_courts(include_supreme: bool = False) -> List[str]:
    """Get list of court codes to fetch (excluding supreme courts by default)."""
    all_courts = list(COURT_CODES.keys())
    if include_supreme:
        return all_courts
    return [c for c in all_courts if c not in EXCLUDED_COURTS]


def fetch_all(
    max_records: int = None,
    include_supreme: bool = False,
    include_all_types: bool = False,
) -> Generator[Dict, None, None]:
    """
    Fetch court decisions from all courts using proper pagination.

    Args:
        max_records: Maximum number of records to yield
        include_supreme: Include supreme courts (HDO, HFD) - normally excluded
        include_all_types: Include all publication types (not just judgments)

    Yields:
        Normalized document records with full text
    """
    # Courts to exclude (handled by separate sources)
    exclude_courts = None if include_supreme else list(EXCLUDED_COURTS)

    page = 0
    pagesize = 100  # Maximum page size for efficiency
    count = 0
    total_available = None

    # Include all publication types by default to maximize coverage
    # The API has 10k+ records but only 920 are DOM_ELLER_BESLUT
    # Other types (PROVNINGSTILLSTAND, etc.) are also valuable case law
    pub_types = None  # All types

    # Track which courts we've fetched from
    courts_fetched = set()
    errors = 0
    max_errors = 5

    while True:
        if max_records and count >= max_records:
            break

        try:
            publications, total = fetch_publications_page(
                page=page,
                pagesize=pagesize,
                pub_types=pub_types,
                exclude_courts=exclude_courts,
            )

            if total_available is None:
                total_available = total
                print(f"Total available records: {total_available}")

        except requests.HTTPError as e:
            print(f"Error fetching page {page}: {e}")
            errors += 1
            if errors >= max_errors:
                print(f"Too many errors ({errors}), stopping")
                break
            time.sleep(5)
            continue

        if not publications:
            print(f"No more publications at page {page}")
            break

        print(f"Page {page}: {len(publications)} publications (yielded so far: {count})")

        for pub in publications:
            if max_records and count >= max_records:
                break

            court_code = pub.get('domstol', {}).get('domstolKod', '')
            courts_fetched.add(court_code)

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

        # Progress check
        if page % 10 == 0:
            print(f"Progress: {count} records from {len(courts_fetched)} courts")

    print(f"Total records yielded: {count}")
    print(f"Courts covered: {', '.join(sorted(courts_fetched))}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch documents updated since a given date."""
    for record in fetch_all():
        if record.get('date'):
            try:
                doc_date = datetime.strptime(record['date'], '%Y-%m-%d')
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def bootstrap_sample(sample_count: int = 15, include_supreme: bool = False):
    """Fetch sample records from various courts and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    target_courts = get_target_courts(include_supreme=include_supreme)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print(f"Target courts: {len(target_courts)} (excluding supreme courts: {not include_supreme})")
    print("=" * 60)

    # Show available courts
    print("Available courts for this source:")
    for code in sorted(target_courts):
        name = COURT_CODES.get(code, code)
        print(f"  {code}: {name}")
    print()

    records = []
    courts_covered = set()

    # Try to get variety across courts
    for record in fetch_all(max_records=sample_count + 10, include_supreme=include_supreme):
        if len(records) >= sample_count:
            break

        try:
            # Validate record has full text
            text_len = len(record.get('text', ''))
            if text_len < 500:
                print(f"  Skipping {record.get('primary_case_number')}: Text too short ({text_len} chars)")
                continue

            records.append(record)
            courts_covered.add(record.get('court_code', ''))

            # Save individual record
            filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            court = record.get('court_code', 'UNK')
            title_preview = record.get('title', '')[:40]
            source = record.get('text_source', 'unknown')
            print(f"  [{len(records):02d}] {court} - {record['primary_case_number']}: {text_len:,} chars ({source})")
            print(f"       {title_preview}...")

        except Exception as e:
            print(f"  Error saving record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        # Statistics
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        # Count by court
        court_counts = {}
        for r in records:
            court = r.get('court_code', 'Unknown')
            court_counts[court] = court_counts.get(court, 0) + 1

        print(f"Courts covered: {len(courts_covered)}")
        for court, count in sorted(court_counts.items()):
            name = COURT_CODES.get(court, court)
            print(f"  {court}: {count} ({name})")

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
    parser = argparse.ArgumentParser(description="SE/Domstolverket case law fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'courts', 'test'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=15,
                       help="Number of sample records to fetch")
    parser.add_argument('--include-supreme', action='store_true',
                       help="Include supreme courts (HDO, HFD) in fetch")
    parser.add_argument('--all-types', action='store_true',
                       help="Include all publication types")

    args = parser.parse_args()

    if args.command == 'courts':
        # List available courts
        courts = fetch_courts()
        print("Available courts from API:")
        for court in courts:
            code = court['domstolKod']
            name = court['domstolNamn']
            excluded = ' (excluded - has separate source)' if code in EXCLUDED_COURTS else ''
            print(f"  {code}: {name}{excluded}")

    elif args.command == 'test':
        # Test a single fetch
        print("Testing API connection...")
        court_codes = get_target_courts()[:3]  # First 3 courts
        print(f"Testing courts: {court_codes}")

        pubs = fetch_publications(court_codes=court_codes, page=0, pagesize=5)
        print(f"Found {len(pubs)} publications")
        if pubs:
            pub = pubs[0]
            court = pub.get('domstol', {}).get('domstolKod', 'UNK')
            print(f"First publication from {court}: {pub.get('malNummerLista', [])}")
            print(f"  Type: {pub.get('typ')}")
            print(f"  Date: {pub.get('avgorandedatum')}")
            print(f"  Attachments: {len(pub.get('bilagaLista', []))}")

    elif args.command == 'bootstrap':
        success = bootstrap_sample(args.count, include_supreme=args.include_supreme)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all(include_supreme=args.include_supreme, include_all_types=args.all_types):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
