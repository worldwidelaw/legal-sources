#!/usr/bin/env python3
"""
UK/JCPC-Crown - Judicial Committee of the Privy Council Fetcher

Fetches JCPC (Privy Council) case law from the National Archives Find Case Law
service. The JCPC is the final court of appeal for Crown Dependencies (Jersey,
Guernsey, Isle of Man) and many overseas territories.

Data source: https://caselaw.nationalarchives.gov.uk (court code: ukpc)
API: Atom feed + Akoma Ntoso XML documents
License: Open Justice Licence
Rate limit: 1000 requests per 5 minutes

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap --full      # Full bootstrap
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
COURT_CODE = "ukpc"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UK/JCPC-Crown"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/xml, application/atom+xml",
}

NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "akn": "http://docs.oasis-open.org/legaldocml/ns/akn/3.0",
    "uk": "https://caselaw.nationalarchives.gov.uk/akn",
}

EARLIEST_YEAR = 2003
CURRENT_YEAR = datetime.now().year


def fetch_atom_feed(page: int = 1, per_page: int = 50,
                    from_year: int = None, to_year: int = None) -> Optional[ET.Element]:
    """Fetch Atom feed listing JCPC decisions."""
    url = f"{BASE_URL}/atom.xml"
    params = {"page": page, "per_page": per_page, "court": COURT_CODE}
    if from_year:
        params["from_date_2"] = str(from_year)
    if to_year:
        params["to_date_2"] = str(to_year)

    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        return ET.fromstring(resp.content)
    except (requests.RequestException, ET.ParseError) as e:
        print(f"  Error fetching feed: {e}")
        return None


def fetch_document_xml(doc_url: str) -> Optional[str]:
    """Fetch Akoma Ntoso XML for a single JCPC decision."""
    xml_url = doc_url.rstrip("/") + "/data.xml"
    try:
        resp = requests.get(xml_url, headers=HEADERS, timeout=120)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"    Error fetching {xml_url}: {e}")
        return None


def extract_text_from_akn(xml_content: str) -> str:
    """Extract full text from Akoma Ntoso XML judgment body."""
    try:
        root = ET.fromstring(xml_content.encode('utf-8'))
    except ET.ParseError:
        return ""

    text_parts = []
    skip_tags = {'meta', 'style', 'references', 'proprietary', 'presentation'}

    for elem in root.iter():
        local_name = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
        if local_name in skip_tags:
            continue
        if local_name in ['p', 'docTitle', 'docDate', 'neutralCitation', 'party', 'judge']:
            text = ''.join(elem.itertext()).strip()
            if text:
                text_parts.append(text)

    full_text = '\n'.join(text_parts)
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)
    return full_text.strip()


def extract_jurisdiction_from_title(title: str) -> Optional[str]:
    """Try to identify the originating jurisdiction from the case title."""
    jurisdictions = {
        "Jersey": "JE", "Guernsey": "GG", "Isle of Man": "IM",
        "Cayman Islands": "KY", "Bermuda": "BM", "Gibraltar": "GI",
        "Jamaica": "JM", "Trinidad and Tobago": "TT", "Trinidad & Tobago": "TT",
        "Bahamas": "BS", "Mauritius": "MU", "Belize": "BZ",
        "Antigua and Barbuda": "AG", "Antigua & Barbuda": "AG",
        "British Virgin Islands": "VG", "BVI": "VG",
        "St Lucia": "LC", "Saint Lucia": "LC",
        "St Vincent": "VC", "Saint Vincent": "VC",
        "St Kitts": "KN", "Saint Kitts": "KN", "St Christopher": "KN",
        "Dominica": "DM", "Grenada": "GD", "Montserrat": "MS",
        "Turks and Caicos": "TC", "Turks & Caicos": "TC",
        "Anguilla": "AI", "Falkland Islands": "FK",
        "Solomon Islands": "SB", "Kiribati": "KI", "Tuvalu": "TV",
        "Cook Islands": "CK", "Pitcairn": "PN",
        "New Zealand": "NZ", "Brunei": "BN",
    }
    title_lower = title.lower()
    for name, code in jurisdictions.items():
        if name.lower() in title_lower:
            return code
    return None


def parse_document_xml(xml_content: str, doc_url: str) -> Optional[dict]:
    """Parse Akoma Ntoso XML and extract normalized record."""
    try:
        root = ET.fromstring(xml_content.encode('utf-8'))
    except ET.ParseError as e:
        print(f"    XML parse error: {e}")
        return None

    akn = "http://docs.oasis-open.org/legaldocml/ns/akn/3.0"
    uk_ns = "https://caselaw.nationalarchives.gov.uk/akn"

    # Title
    frbr_name = root.find(f'.//{{{akn}}}FRBRname')
    title = frbr_name.get('value') if frbr_name is not None else None
    if not title:
        doc_title = root.find(f'.//{{{akn}}}docTitle')
        if doc_title is not None:
            title = ''.join(doc_title.itertext()).strip()
    if not title:
        title = doc_url.split('/')[-1]

    # Date
    frbr_date = root.find(f'.//{{{akn}}}FRBRdate')
    date = frbr_date.get('date') if frbr_date is not None else None

    # Court
    court_elem = root.find(f'.//{{{uk_ns}}}court')
    court = court_elem.text if court_elem is not None and court_elem.text else "UKPC"

    # Citation
    cite_elem = root.find(f'.//{{{uk_ns}}}cite')
    citation = cite_elem.text if cite_elem is not None and cite_elem.text else None
    if not citation:
        neutral = root.find(f'.//{{{akn}}}neutralCitation')
        if neutral is not None:
            citation = ''.join(neutral.itertext()).strip()

    # Case number
    case_elem = root.find(f'.//{{{uk_ns}}}caseNumber')
    case_number = case_elem.text if case_elem is not None and case_elem.text else None

    # Full text
    text = extract_text_from_akn(xml_content)
    if not text or len(text) < 100:
        print(f"    Warning: Insufficient text ({len(text)} chars)")
        return None

    # Document ID from URL
    uri_parts = doc_url.replace(BASE_URL + '/', '').split('/')
    doc_id = '/'.join(uri_parts)

    # Jurisdiction from title
    jurisdiction = extract_jurisdiction_from_title(title)

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
        'jurisdiction': jurisdiction,
        'language': 'en',
    }


def get_entries_from_feed(feed_root: ET.Element) -> list:
    """Extract document entries from Atom feed."""
    entries = []
    atom = "http://www.w3.org/2005/Atom"
    for entry in feed_root.findall(f'.//{{{atom}}}entry'):
        link = None
        for l in entry.findall(f'{{{atom}}}link'):
            if l.get('rel') == 'alternate' and not l.get('type'):
                link = l.get('href')
                break
        if not link:
            continue
        title_elem = entry.find(f'{{{atom}}}title')
        title = title_elem.text if title_elem is not None else ""
        entries.append({'uri': link, 'title': title})
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


def fetch_all(max_records: int = None, max_pages: int = None,
              from_year: int = None, to_year: int = None) -> Generator[dict, None, None]:
    """Fetch all JCPC decisions."""
    total_yielded = 0
    from_year = from_year or EARLIEST_YEAR
    to_year = to_year or CURRENT_YEAR

    print(f"Fetching JCPC decisions from {from_year} to {to_year}")

    for year in range(to_year, from_year - 1, -1):
        if max_records and total_yielded >= max_records:
            return

        print(f"\n=== Year {year} ===")
        feed = fetch_atom_feed(page=1, per_page=50, from_year=year, to_year=year)
        if feed is None:
            continue

        total_pages = get_total_pages(feed)
        if max_pages:
            total_pages = min(total_pages, max_pages)
        print(f"  Pages: {total_pages}")

        for page in range(1, total_pages + 1):
            if max_records and total_yielded >= max_records:
                return
            if page > 1:
                feed = fetch_atom_feed(page=page, per_page=50, from_year=year, to_year=year)
                if feed is None:
                    continue
                time.sleep(0.5)

            entries = get_entries_from_feed(feed)
            for entry in entries:
                if max_records and total_yielded >= max_records:
                    return
                xml_content = fetch_document_xml(entry['uri'])
                if not xml_content:
                    continue
                record = parse_document_xml(xml_content, entry['uri'])
                if record:
                    yield record
                    total_yielded += 1
                    if total_yielded % 50 == 0:
                        print(f"  Total: {total_yielded} records...")
                time.sleep(0.5)

    print(f"\nCompleted: {total_yielded} total records")


def normalize(raw: dict) -> dict:
    """Validate and normalize a record."""
    required = ['_id', '_source', '_type', '_fetched_at', 'title', 'text', 'url']
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")
    if not raw.get('text') or len(raw['text']) < 50:
        raise ValueError(f"Insufficient text ({len(raw.get('text', ''))} chars)")
    return raw


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample JCPC records across multiple years."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample JCPC records...")
    print("=" * 60)

    records = []
    year_counts = {}

    for record in fetch_all(max_records=sample_count, max_pages=1):
        try:
            normalized = normalize(record)
            records.append(normalized)
            year = normalized.get('date', '')[:4]
            year_counts[year] = year_counts.get(year, 0) + 1

            idx = len(records)
            filename = SAMPLE_DIR / f"record_{idx:03d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            text_len = len(normalized.get('text', ''))
            doc_date = normalized.get('date', 'Unknown')[:10]
            jurisdiction = normalized.get('jurisdiction', '??')
            print(f"  [{idx:02d}] {doc_date} [{jurisdiction}] {normalized['title'][:50]}... ({text_len:,} chars)")

        except ValueError as e:
            print(f"    Skipping: {e}")

    print("\n" + "=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")
        print(f"Year distribution: {dict(sorted(year_counts.items()))}")

        jurisdictions = {}
        for r in records:
            j = r.get('jurisdiction') or 'Unknown'
            jurisdictions[j] = jurisdictions.get(j, 0) + 1
        print(f"Jurisdictions: {dict(sorted(jurisdictions.items()))}")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get('text'))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print(f"\nVALIDATION PASSED: {len(records)} records with full text.")
    return True


def bootstrap_full():
    """Full bootstrap of all JCPC decisions."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print("Starting full bootstrap of JCPC decisions...")
    count = 0
    for record in fetch_all():
        try:
            normalized = normalize(record)
            count += 1
            filename = SAMPLE_DIR / f"record_{count:04d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
        except ValueError as e:
            print(f"  Skipping: {e}")

    print(f"\nFull bootstrap complete: {count} records saved.")
    return count > 0


def test_api():
    """Test API connectivity."""
    print("Testing JCPC (ukpc) API access...")

    print("\n1. Testing Atom feed for ukpc...")
    feed = fetch_atom_feed(page=1, per_page=5)
    if feed is None:
        print("   FAILED")
        return False
    entries = get_entries_from_feed(feed)
    total_pages = get_total_pages(feed)
    print(f"   OK: {len(entries)} entries, {total_pages} total pages")

    print("\n2. Testing document XML...")
    if entries:
        xml = fetch_document_xml(entries[0]['uri'])
        if xml:
            record = parse_document_xml(xml, entries[0]['uri'])
            if record:
                print(f"   OK: '{record['title'][:50]}...' ({len(record['text']):,} chars)")
                print(f"   Jurisdiction: {record.get('jurisdiction', 'N/A')}")
            else:
                print("   FAILED: parse error")
                return False
        else:
            print("   FAILED: fetch error")
            return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="UK/JCPC-Crown fetcher")
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'])
    parser.add_argument('--sample', action='store_true')
    parser.add_argument('--count', type=int, default=15)
    parser.add_argument('--full', action='store_true')

    args = parser.parse_args()

    if args.command == 'test':
        sys.exit(0 if test_api() else 1)
    elif args.command == 'bootstrap':
        if args.sample:
            sys.exit(0 if bootstrap_sample(args.count) else 1)
        elif args.full:
            sys.exit(0 if bootstrap_full() else 1)
        else:
            print("Use --sample or --full flag.")
            sys.exit(1)
    elif args.command == 'update':
        print("Update not implemented.")
        sys.exit(1)


if __name__ == '__main__':
    main()
