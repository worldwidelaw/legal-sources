#!/usr/bin/env python3
"""
French Constitutional Council (Conseil constitutionnel) Data Fetcher

Fetches decisions from the Conseil constitutionnel via DILA CONSTIT bulk open data.
Covers QPC (since 2010), DC (since 1958), electoral matters, and administrative decisions.

Data source: https://echanges.dila.gouv.fr/OPENDATA/CONSTIT/
License: Open Licence 2.0
"""

import json
import os
import re
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Generator, Optional
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

# Constants
BASE_URL = "https://echanges.dila.gouv.fr/OPENDATA/CONSTIT/"
RATE_LIMIT_DELAY = 1  # seconds between requests


def get_text(element: Optional[ET.Element], default: str = "") -> str:
    """Extract text from an XML element, stripping whitespace."""
    if element is not None and element.text:
        return element.text.strip()
    return default


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    # Remove HTML tags
    soup = BeautifulSoup(html_text, 'html.parser')
    text = soup.get_text(separator='\n')
    # Clean up whitespace
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


def list_archives() -> list[dict]:
    """List available archives from the CONSTIT endpoint."""
    response = requests.get(BASE_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    archives = []

    for link in soup.find_all('a'):
        href = link.get('href', '')
        if href.endswith('.tar.gz'):
            # Extract filename and determine if it's a full archive or incremental
            filename = href.split('/')[-1]
            is_full = 'Freemium_constit_global' in filename

            archives.append({
                'filename': filename,
                'url': BASE_URL + filename,
                'is_full': is_full
            })

    return archives


def parse_decision_xml(xml_content: bytes) -> Optional[dict]:
    """Parse a CONSTIT decision XML file."""
    try:
        root = ET.fromstring(xml_content)

        # Extract metadata
        meta = root.find('.//META')
        if meta is None:
            return None

        meta_commun = meta.find('META_COMMUN')
        meta_juri = meta.find('.//META_JURI')
        meta_juri_constit = meta.find('.//META_JURI_CONSTIT')

        if meta_commun is None:
            return None

        doc_id = get_text(meta_commun.find('ID'))
        nature = get_text(meta_commun.find('NATURE'))

        # Get decision metadata
        title = ""
        date_dec = None
        numero = ""
        solution = ""
        juridiction = ""

        if meta_juri is not None:
            title = get_text(meta_juri.find('TITRE'))
            date_dec = get_text(meta_juri.find('DATE_DEC'))
            numero = get_text(meta_juri.find('NUMERO'))
            solution = get_text(meta_juri.find('SOLUTION'))
            juridiction = get_text(meta_juri.find('JURIDICTION'))

        # Get Constitutional-specific metadata
        ecli = ""
        nature_qualifiee = ""
        url_cc = ""
        nor = ""
        titre_jo = ""

        if meta_juri_constit is not None:
            ecli = get_text(meta_juri_constit.find('ECLI'))
            nature_qualifiee = get_text(meta_juri_constit.find('NATURE_QUALIFIEE'))
            url_cc = get_text(meta_juri_constit.find('URL_CC'))
            nor = get_text(meta_juri_constit.find('NOR'))
            titre_jo = get_text(meta_juri_constit.find('TITRE_JO'))

        # Extract full text content
        contenu = root.find('.//BLOC_TEXTUEL/CONTENU')
        full_text = ""
        if contenu is not None:
            # Get raw HTML content
            raw_html = ET.tostring(contenu, encoding='unicode', method='xml')
            # Extract just the inner content
            inner_match = re.search(r'<CONTENU>(.*?)</CONTENU>', raw_html, re.DOTALL)
            if inner_match:
                full_text = clean_html(inner_match.group(1))

        if not full_text:
            return None

        return {
            '_id': doc_id,
            'nature': nature,
            'nature_qualifiee': nature_qualifiee or nature,
            'title': title,
            'date': date_dec,
            'numero': numero,
            'solution': solution,
            'juridiction': juridiction,
            'ecli': ecli,
            'nor': nor,
            'titre_jo': titre_jo,
            'url_cc': url_cc,
            'text': full_text
        }

    except ET.ParseError as e:
        print(f"XML parse error: {e}", file=sys.stderr)
        return None


def fetch_all(max_docs: Optional[int] = None, use_full_archive: bool = True) -> Generator[dict, None, None]:
    """
    Fetch all Constitutional Council decisions.

    Args:
        max_docs: Maximum number of documents to fetch (None for all)
        use_full_archive: If True, download full archive; if False, use incrementals only
    """
    archives = list_archives()

    if use_full_archive:
        # Find the full archive
        full_archives = [a for a in archives if a['is_full']]
        if full_archives:
            # Use the most recent full archive
            archives_to_process = [full_archives[-1]]
        else:
            # Fall back to incremental archives
            archives_to_process = [a for a in archives if not a['is_full']]
    else:
        # Use only incremental archives
        archives_to_process = [a for a in archives if not a['is_full']]

    doc_count = 0
    seen_ids = set()

    for archive in archives_to_process:
        print(f"Processing archive: {archive['filename']}", file=sys.stderr)

        try:
            response = requests.get(archive['url'], timeout=300)
            response.raise_for_status()

            with tarfile.open(fileobj=BytesIO(response.content), mode='r:gz') as tar:
                for member in tar.getmembers():
                    if not member.name.endswith('.xml'):
                        continue

                    # Extract and parse XML
                    f = tar.extractfile(member)
                    if f is None:
                        continue

                    xml_content = f.read()
                    decision = parse_decision_xml(xml_content)

                    if decision is None:
                        continue

                    # Skip duplicates
                    if decision['_id'] in seen_ids:
                        continue
                    seen_ids.add(decision['_id'])

                    yield decision
                    doc_count += 1

                    if max_docs and doc_count >= max_docs:
                        return

        except Exception as e:
            print(f"Error processing archive {archive['filename']}: {e}", file=sys.stderr)
            continue

        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """
    Fetch decisions updated since a given date.

    Uses incremental archives only.
    """
    archives = list_archives()
    incremental_archives = [a for a in archives if not a['is_full']]

    # Filter by date in filename (YYYYMMDD format)
    since_str = since.strftime('%Y%m%d')

    for archive in incremental_archives:
        # Extract date from filename (e.g., CONSTIT_20260205-212615.tar.gz)
        match = re.search(r'CONSTIT_(\d{8})', archive['filename'])
        if not match:
            continue

        archive_date = match.group(1)
        if archive_date < since_str:
            continue

        # Process this archive
        yield from fetch_all(max_docs=None, use_full_archive=False)


def normalize(raw: dict) -> dict:
    """Transform raw decision data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    # Build URL
    url = raw.get('url_cc', '')
    if not url and raw.get('numero') and raw.get('date'):
        # Construct URL from decision number and date
        year = raw['date'][:4] if raw.get('date') else ''
        nature = raw.get('nature_qualifiee', 'DC').upper()
        numero = raw.get('numero', '')
        if year and numero:
            url = f"https://www.conseil-constitutionnel.fr/decision/{year}/{year}{numero}{nature}.htm"

    # Construct a meaningful title if missing
    title = raw.get('title', '')
    if not title:
        nature = raw.get('nature_qualifiee', raw.get('nature', 'Decision'))
        numero = raw.get('numero', '')
        title = f"Décision n° {numero} {nature}" if numero else f"Décision {nature}"

    return {
        '_id': raw['_id'],
        '_source': 'FR/ConseilConstitutionnel',
        '_type': 'case_law',
        '_fetched_at': now,
        'title': title,
        'text': raw['text'],
        'date': raw.get('date'),
        'url': url,
        'ecli': raw.get('ecli'),
        'numero': raw.get('numero'),
        'nature': raw.get('nature'),
        'nature_qualifiee': raw.get('nature_qualifiee'),
        'solution': raw.get('solution'),
        'juridiction': raw.get('juridiction'),
        'nor': raw.get('nor'),
        'titre_jo': raw.get('titre_jo'),
        'language': 'fr'
    }


def bootstrap_sample(sample_dir: Path, count: int = 100) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count, use_full_archive=True):
        record = normalize(raw)
        samples.append(record)

        # Save individual sample
        filename = f"{record['_id']}.json"
        with open(sample_dir / filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text'])} chars)", file=sys.stderr)

    # Save combined samples
    if samples:
        with open(sample_dir / 'all_samples.json', 'w', encoding='utf-8') as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        # Calculate statistics
        text_lengths = [len(s['text']) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)

        # Count by type
        by_nature = {}
        for s in samples:
            nature = s.get('nature_qualifiee') or s.get('nature') or 'Unknown'
            by_nature[nature] = by_nature.get(nature, 0) + 1

        print(f"\nBy decision type:", file=sys.stderr)
        for nature, count in sorted(by_nature.items(), key=lambda x: -x[1]):
            print(f"  {nature}: {count}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='French Constitutional Council fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Generate sample data only')
    parser.add_argument('--count', type=int, default=100,
                       help='Number of samples to generate')
    parser.add_argument('--since', type=str,
                       help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / 'sample'

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            # Full bootstrap
            for raw in fetch_all():
                record = normalize(raw)
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'fetch':
        for raw in fetch_all(max_docs=args.count if args.sample else None):
            record = normalize(raw)
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for raw in fetch_updates(since):
            record = normalize(raw)
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
