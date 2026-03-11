#!/usr/bin/env python3
"""
UK/CaseLaw - National Archives Find Case Law Fetcher

Fetches UK case law from the Find Case Law service at caselaw.nationalarchives.gov.uk.
Covers UK Supreme Court, Court of Appeal, High Court, and tribunals.

Data source: https://caselaw.nationalarchives.gov.uk
API: Atom feed + Akoma Ntoso XML documents
License: Open Justice Licence
Rate limit: 1000 requests per 5 minutes

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test API connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from xml.etree import ElementTree as ET

import requests

BASE_URL = "https://caselaw.nationalarchives.gov.uk"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UK/CaseLaw"

# User agent required by API
HEADERS = {
    "User-Agent": "WorldWideLaw/1.0 (Open Data Research; github.com/worldwidelaw/legal-sources)",
    "Accept": "application/xml, application/atom+xml",
}

# XML namespaces
NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "tna": "https://caselaw.nationalarchives.gov.uk",
    "akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0",
    "uk": "https://caselaw.nationalarchives.gov.uk/akn",
}

# All available courts and tribunals
ALL_COURTS = [
    # Supreme & Appeals Courts
    "uksc",           # UK Supreme Court
    "ukpc",           # Privy Council
    "ewca/civ",       # Court of Appeal (Civil Division)
    "ewca/crim",      # Court of Appeal (Criminal Division)
    # High Court Divisions
    "ewhc/admin",     # Administrative Court
    "ewhc/admlty",    # Admiralty Court
    "ewhc/ch",        # Chancery Division
    "ewhc/comm",      # Commercial Court
    "ewhc/fam",       # Family Division
    "ewhc/ipec",      # Intellectual Property Enterprise Court
    "ewhc/kb",        # King's/Queen's Bench Division
    "ewhc/qb",        # Queen's Bench (historical)
    "ewhc/mercantile",# Mercantile Court
    "ewhc/pat",       # Patents Court
    "ewhc/scco",      # Senior Courts Costs Office
    "ewhc/tcc",       # Technology and Construction Court
    # Other Courts
    "ewcr",           # Crown Court
    "ewcc",           # County Court
    "ewfc",           # Family Court
    "ewcop",          # Court of Protection
    # Tribunals
    "ukiptrib",       # Investigatory Powers Tribunal
    "siac",           # Special Immigration Appeals Commission
    "eat",            # Employment Appeal Tribunal
    "ukut/aac",       # Upper Tribunal (Administrative Appeals)
    "ukut/iac",       # Upper Tribunal (Immigration & Asylum)
    "ukut/lc",        # Upper Tribunal (Lands Chamber)
    "ukut/tcc",       # Upper Tribunal (Tax & Chancery)
    "ukftt/grc",      # First-tier Tribunal (General Regulatory)
    "ukftt/tc",       # First-tier Tribunal (Tax)
]

# Courts to sample from (subset for faster testing)
COURTS_TO_SAMPLE = [
    "uksc",       # UK Supreme Court
    "ewca/civ",   # Court of Appeal Civil
    "ewca/crim",  # Court of Appeal Criminal
    "ewhc/ch",    # High Court Chancery
    "ewhc/kb",    # High Court King's Bench
    "ewhc/admin", # High Court Administrative
]

# Year range for historical data (service started in 2001)
EARLIEST_YEAR = 2003  # First year with reliable data
CURRENT_YEAR = datetime.now().year


def fetch_atom_feed(
    page: int = 1,
    per_page: int = 50,
    court: str = None,
    from_year: int = None,
    to_year: int = None,
) -> Optional[ET.Element]:
    """
    Fetch Atom feed listing case law documents.

    Args:
        page: Page number (1-indexed)
        per_page: Results per page
        court: Court code to filter by
        from_year: Start year for filtering (inclusive)
        to_year: End year for filtering (inclusive)

    Returns parsed XML root element or None on error.
    """
    url = f"{BASE_URL}/atom.xml"
    params = {"page": page, "per_page": per_page}
    if court:
        params["court"] = court
    # API uses from_date_2 and to_date_2 for year filtering
    if from_year:
        params["from_date_2"] = str(from_year)
    if to_year:
        params["to_date_2"] = str(to_year)

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        return ET.fromstring(resp.content)
    except requests.RequestException as e:
        print(f"  Error fetching feed: {e}")
        return None
    except ET.ParseError as e:
        print(f"  Error parsing feed: {e}")
        return None


def fetch_document_xml(doc_url: str) -> Optional[str]:
    """
    Fetch Akoma Ntoso XML for a single case law document.
    
    Args:
        doc_url: Document URL (e.g., https://caselaw.nationalarchives.gov.uk/uksc/2024/1)
    
    Returns:
        Raw XML content string or None on error.
    """
    xml_url = doc_url.rstrip("/") + "/data.xml"
    
    try:
        resp = requests.get(xml_url, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"    Error fetching {xml_url}: {e}")
        return None


def extract_text_from_akn(xml_content: str) -> str:
    """
    Extract full text from Akoma Ntoso XML.
    
    The judgment text is in <judgmentBody> with <p> elements.
    """
    try:
        root = ET.fromstring(xml_content.encode('utf-8'))
    except ET.ParseError:
        return ""
    
    text_parts = []
    
    # Extract from all paragraph elements
    for elem in root.iter():
        local_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
        
        # Skip metadata, header, and style elements
        if local_name in ['meta', 'style', 'references', 'proprietary', 'presentation']:
            continue
        
        # Extract text from content elements
        if local_name in ['p', 'docTitle', 'docDate', 'neutralCitation', 'party', 'judge']:
            # Get all text including nested elements
            text = ''.join(elem.itertext()).strip()
            if text:
                text_parts.append(text)
    
    full_text = '\n'.join(text_parts)
    
    # Clean up
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)
    
    return full_text.strip()


def parse_document_xml(xml_content: str, doc_url: str) -> Optional[dict]:
    """
    Parse Akoma Ntoso XML and extract normalized record.
    """
    try:
        root = ET.fromstring(xml_content.encode('utf-8'))
    except ET.ParseError as e:
        print(f"    XML parse error: {e}")
        return None
    
    # Find FRBRWork for metadata
    frbr_work = root.find('.//{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}FRBRWork')
    frbr_name = root.find('.//{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}FRBRname')
    frbr_date = root.find('.//{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}FRBRdate')
    
    # Extract title
    title = None
    if frbr_name is not None:
        title = frbr_name.get('value')
    
    # Fallback to docTitle
    if not title:
        doc_title = root.find('.//{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}docTitle')
        if doc_title is not None:
            title = ''.join(doc_title.itertext()).strip()
    
    if not title:
        title = doc_url.split('/')[-1]
    
    # Extract date
    date = None
    if frbr_date is not None:
        date = frbr_date.get('date')
    
    # Extract court from proprietary metadata
    court = None
    court_elem = root.find('.//{https://caselaw.nationalarchives.gov.uk/akn}court')
    if court_elem is not None and court_elem.text:
        court = court_elem.text
    
    # Extract citation
    citation = None
    cite_elem = root.find('.//{https://caselaw.nationalarchives.gov.uk/akn}cite')
    if cite_elem is not None and cite_elem.text:
        citation = cite_elem.text
    
    # Fallback to neutralCitation in header
    if not citation:
        neutral = root.find('.//{http://docs.oasis-open.org/legaldocml/ns/akn/3.0}neutralCitation')
        if neutral is not None:
            citation = ''.join(neutral.itertext()).strip()
    
    # Extract case number
    case_number = None
    case_elem = root.find('.//{https://caselaw.nationalarchives.gov.uk/akn}caseNumber')
    if case_elem is not None and case_elem.text:
        case_number = case_elem.text
    
    # Extract full text
    text = extract_text_from_akn(xml_content)
    
    if not text or len(text) < 100:
        print(f"    Warning: Document has insufficient text ({len(text)} chars)")
        return None
    
    # Parse document ID from URL
    uri_parts = doc_url.replace(BASE_URL + '/', '').split('/')
    doc_id = '/'.join(uri_parts)
    
    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': text,
        'date': date,
        'url': doc_url,
        'court': court,
        'citation': citation,
        'case_number': case_number,
        'language': 'en',
    }


def get_entries_from_feed(feed_root: ET.Element) -> list:
    """
    Extract document entries from Atom feed.
    
    Returns list of dicts with uri, title, published, xml_url.
    """
    entries = []
    
    for entry in feed_root.findall('.//{http://www.w3.org/2005/Atom}entry'):
        # Get document link
        link = None
        for l in entry.findall('{http://www.w3.org/2005/Atom}link'):
            if l.get('rel') == 'alternate' and not l.get('type'):
                link = l.get('href')
                break
        
        if not link:
            continue
        
        # Get title
        title_elem = entry.find('{http://www.w3.org/2005/Atom}title')
        title = title_elem.text if title_elem is not None else ""
        
        # Get published date
        published_elem = entry.find('{http://www.w3.org/2005/Atom}published')
        published = published_elem.text if published_elem is not None else ""
        
        # Get XML link
        xml_url = None
        for l in entry.findall('{http://www.w3.org/2005/Atom}link'):
            if l.get('type') == 'application/akn+xml':
                xml_url = l.get('href')
                break
        
        entries.append({
            'uri': link,
            'title': title,
            'published': published,
            'xml_url': xml_url,
        })
    
    return entries


def get_total_pages(feed_root: ET.Element) -> int:
    """Extract total pages from feed's last link."""
    for link in feed_root.findall('{http://www.w3.org/2005/Atom}link'):
        if link.get('rel') == 'last':
            href = link.get('href', '')
            match = re.search(r'page=(\d+)', href)
            if match:
                return int(match.group(1))
    return 1


def fetch_all(
    max_records: int = None,
    max_pages: int = None,
    courts: list = None,
    from_year: int = None,
    to_year: int = None,
) -> Generator[dict, None, None]:
    """
    Fetch all UK case law documents.

    Args:
        max_records: Maximum total records to yield
        max_pages: Maximum pages to fetch per court/year
        courts: List of court codes to filter by (default: ALL_COURTS)
        from_year: Start year for filtering (default: EARLIEST_YEAR)
        to_year: End year for filtering (default: CURRENT_YEAR)

    Yields:
        Normalized document records
    """
    total_yielded = 0
    courts = courts or ALL_COURTS
    from_year = from_year or EARLIEST_YEAR
    to_year = to_year or CURRENT_YEAR

    print(f"Fetching UK case law from {from_year} to {to_year}")
    print(f"Courts: {len(courts)}")

    # Iterate by year for better progress tracking
    for year in range(to_year, from_year - 1, -1):  # newest first
        if max_records and total_yielded >= max_records:
            return

        print(f"\n=== Year {year} ===")

        for court in courts:
            if max_records and total_yielded >= max_records:
                return

            print(f"\nFetching from court: {court} ({year})")

            for record in _fetch_court(court, max_records=None, max_pages=max_pages, from_year=year, to_year=year):
                yield record
                total_yielded += 1

                if total_yielded % 100 == 0:
                    print(f"  Total fetched: {total_yielded} records...")

                if max_records and total_yielded >= max_records:
                    return

    print(f"\nCompleted: {total_yielded} total records")


def _fetch_court(
    court: str,
    max_records: int = None,
    max_pages: int = None,
    from_year: int = None,
    to_year: int = None,
) -> Generator[dict, None, None]:
    """Fetch documents for a specific court, optionally filtered by year range."""
    feed = fetch_atom_feed(page=1, per_page=20, court=court, from_year=from_year, to_year=to_year)
    if feed is None:
        return

    total_pages = get_total_pages(feed)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    year_info = f" ({from_year}-{to_year})" if from_year or to_year else ""
    print(f"  Total pages for {court}{year_info}: {total_pages}")

    count = 0
    for page in range(1, total_pages + 1):
        if max_records and count >= max_records:
            return

        if page > 1:
            feed = fetch_atom_feed(page=page, per_page=20, court=court, from_year=from_year, to_year=to_year)
            if feed is None:
                continue
            time.sleep(0.5)

        entries = get_entries_from_feed(feed)

        for entry in entries:
            if max_records and count >= max_records:
                return

            xml_content = fetch_document_xml(entry['uri'])
            if not xml_content:
                continue

            record = parse_document_xml(xml_content, entry['uri'])
            if record:
                yield record
                count += 1

            time.sleep(0.5)


def normalize(raw: dict) -> dict:
    """Validate and normalize a record."""
    required = ['_id', '_source', '_type', '_fetched_at', 'title', 'text', 'url']
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")
    
    if not raw.get('text') or len(raw['text']) < 50:
        raise ValueError(f"Document has insufficient text content ({len(raw.get('text', ''))} chars)")
    
    return raw


def bootstrap_sample(sample_count: int = 15):
    """
    Fetch sample records from multiple courts across different years.

    Demonstrates the expanded coverage by fetching from:
    - Multiple courts (Supreme, Appeal, High Court divisions)
    - Multiple years (recent + historical: 2026, 2020, 2015, 2010, 2005)
    """
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from UK/CaseLaw...")
    print("Demonstrating expanded year coverage (2003-present)")
    print("=" * 60)

    # Sample from different years to show historical coverage
    sample_years = [CURRENT_YEAR, 2020, 2015, 2010, 2005]
    sample_courts = ["uksc", "ewca/civ", "ewhc/ch"]

    records = []
    year_counts = {}

    for year in sample_years:
        if len(records) >= sample_count:
            break

        print(f"\n--- Fetching from year {year} ---")

        for court in sample_courts:
            if len(records) >= sample_count:
                break

            print(f"\nFetching from {court} ({year})...")

            count_for_this = 0
            for record in _fetch_court(court, max_records=2, max_pages=1, from_year=year, to_year=year):
                if len(records) >= sample_count:
                    break

                try:
                    normalized = normalize(record)
                    records.append(normalized)
                    count_for_this += 1
                    year_counts[year] = year_counts.get(year, 0) + 1

                    # Save individual record
                    idx = len(records)
                    filename = SAMPLE_DIR / f"record_{idx:03d}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)

                    text_len = len(normalized.get('text', ''))
                    doc_date = normalized.get('date', 'Unknown')[:10]
                    court_name = normalized.get('court', 'Unknown')[:25]
                    print(f"  [{idx:02d}] {doc_date} {court_name}: {normalized['title'][:35]}... ({text_len:,} chars)")

                except ValueError as e:
                    print(f"    Skipping record: {e}")

    print("\n" + "=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        # Show year distribution
        print(f"Year distribution: {dict(sorted(year_counts.items()))}")

        # Show court distribution
        courts = {}
        for r in records:
            c = r.get('court', 'Unknown')
            courts[c] = courts.get(c, 0) + 1
        print(f"Courts: {dict(sorted(courts.items()))}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get('text'))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    # Check year coverage
    years_covered = len(year_counts)
    if years_covered < 2:
        print(f"WARNING: Only {years_covered} year(s) covered. Expected multiple years.")
        return False

    print(f"\nVALIDATION PASSED: {len(records)} records with full text across {years_covered} different years.")
    return True


def test_api():
    """Test API connectivity and year filtering."""
    print("Testing UK Case Law API...")

    # Test feed access
    print("\n1. Testing Atom feed endpoint (current)...")
    feed = fetch_atom_feed(page=1, per_page=5)
    if feed is not None:
        entries = get_entries_from_feed(feed)
        total_pages = get_total_pages(feed)
        print(f"   OK: Got {len(entries)} entries from feed (total pages: {total_pages})")
    else:
        print("   FAILED: Could not fetch feed")
        return False

    # Test year filtering
    print("\n2. Testing year filter (2010)...")
    feed_2010 = fetch_atom_feed(page=1, per_page=5, from_year=2010, to_year=2010)
    if feed_2010 is not None:
        entries_2010 = get_entries_from_feed(feed_2010)
        total_pages_2010 = get_total_pages(feed_2010)
        print(f"   OK: Got {len(entries_2010)} entries from 2010 (total pages: {total_pages_2010})")
    else:
        print("   FAILED: Could not fetch 2010 feed")
        return False

    # Test document XML access
    print("\n3. Testing document XML endpoint...")
    if entries:
        test_url = entries[0]['uri']
        print(f"   Fetching: {test_url}")
        xml_content = fetch_document_xml(test_url)
        if xml_content:
            print(f"   OK: Got {len(xml_content):,} bytes of XML")

            # Try parsing
            record = parse_document_xml(xml_content, test_url)
            if record:
                print(f"   OK: Parsed document '{record['title'][:50]}...'")
                print(f"       Court: {record.get('court', 'N/A')}")
                print(f"       Citation: {record.get('citation', 'N/A')}")
                print(f"       Text length: {len(record['text']):,} chars")
            else:
                print("   FAILED: Could not parse document")
                return False
        else:
            print("   FAILED: Could not fetch document XML")
            return False

    print("\n4. Testing historical document (2010)...")
    if entries_2010:
        test_url_2010 = entries_2010[0]['uri']
        print(f"   Fetching: {test_url_2010}")
        xml_content_2010 = fetch_document_xml(test_url_2010)
        if xml_content_2010:
            record_2010 = parse_document_xml(xml_content_2010, test_url_2010)
            if record_2010:
                print(f"   OK: Historical document '{record_2010['title'][:50]}...'")
                print(f"       Date: {record_2010.get('date', 'N/A')}")
                print(f"       Text length: {len(record_2010['text']):,} chars")
            else:
                print("   FAILED: Could not parse historical document")
                return False
        else:
            print("   FAILED: Could not fetch historical document XML")
            return False

    print("\nAll tests passed!")
    print(f"Year coverage: {EARLIEST_YEAR} - {CURRENT_YEAR}")
    print(f"Courts available: {len(ALL_COURTS)}")
    return True


def main():
    parser = argparse.ArgumentParser(description="UK/CaseLaw fetcher")
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=15,
                       help="Number of sample records to fetch")
    
    args = parser.parse_args()
    
    if args.command == 'test':
        success = test_api()
        sys.exit(0 if success else 1)
    
    elif args.command == 'bootstrap':
        if args.sample:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)
        else:
            print("Full bootstrap not implemented yet. Use --sample flag.")
            sys.exit(1)
    
    elif args.command == 'update':
        print("Update command not implemented yet.")
        sys.exit(1)


if __name__ == '__main__':
    main()
