#!/usr/bin/env python3
"""
CNIL (Commission Nationale de l'Informatique et des Libertés) Data Fetcher

Fetches deliberations from CNIL (French data protection authority) via DILA open data.
Covers all deliberations since 1979: opinions, recommendations, sanctions, decisions.

Data source: https://echanges.dila.gouv.fr/OPENDATA/CNIL/
License: Licence Ouverte (Open Licence 2.0)
"""

import json
import os
import re
import sys
import tarfile
import time
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Generator, Optional
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

# Constants
BASE_URL = "https://echanges.dila.gouv.fr/OPENDATA/CNIL/"
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
    """List available archives from the CNIL endpoint."""
    response = requests.get(BASE_URL, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    archives = []

    for link in soup.find_all('a'):
        href = link.get('href', '')
        if href.endswith('.tar.gz'):
            filename = href.split('/')[-1]
            is_full = 'Freemium_cnil_global' in filename

            archives.append({
                'filename': filename,
                'url': BASE_URL + filename,
                'is_full': is_full
            })

    return archives


def parse_deliberation_xml(xml_content: bytes) -> Optional[dict]:
    """Parse a CNIL deliberation XML file."""
    try:
        root = ET.fromstring(xml_content)

        # The root is TEXTE_CNIL
        if root.tag != 'TEXTE_CNIL':
            return None

        # Extract metadata
        meta = root.find('.//META')
        if meta is None:
            return None

        meta_commun = meta.find('META_COMMUN')
        meta_cnil = meta.find('.//META_CNIL')

        if meta_commun is None:
            return None

        doc_id = get_text(meta_commun.find('ID'))
        nature = get_text(meta_commun.find('NATURE'))

        # Get CNIL-specific metadata
        titre = ""
        titre_full = ""
        numero = ""
        nature_delib = ""
        date_texte = None
        date_publi = None
        etat_juridique = ""
        nor = ""

        if meta_cnil is not None:
            titre = get_text(meta_cnil.find('TITRE'))
            titre_full = get_text(meta_cnil.find('TITREFULL'))
            numero = get_text(meta_cnil.find('NUMERO'))
            nature_delib = get_text(meta_cnil.find('NATURE_DELIB'))
            date_texte = get_text(meta_cnil.find('DATE_TEXTE'))
            date_publi = get_text(meta_cnil.find('DATE_PUBLI'))
            etat_juridique = get_text(meta_cnil.find('ETAT_JURIDIQUE'))
            nor = get_text(meta_cnil.find('NOR'))

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
            'nature_delib': nature_delib,
            'titre': titre,
            'titre_full': titre_full,
            'numero': numero,
            'date_texte': date_texte,
            'date_publi': date_publi,
            'etat_juridique': etat_juridique,
            'nor': nor,
            'text': full_text
        }

    except ET.ParseError as e:
        print(f"XML parse error: {e}", file=sys.stderr)
        return None


def fetch_all(max_docs: Optional[int] = None, use_full_archive: bool = True) -> Generator[dict, None, None]:
    """
    Fetch all CNIL deliberations.

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
            response = requests.get(archive['url'], timeout=600)
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
                    deliberation = parse_deliberation_xml(xml_content)

                    if deliberation is None:
                        continue

                    # Skip duplicates
                    if deliberation['_id'] in seen_ids:
                        continue
                    seen_ids.add(deliberation['_id'])

                    yield deliberation
                    doc_count += 1

                    if max_docs and doc_count >= max_docs:
                        return

        except Exception as e:
            print(f"Error processing archive {archive['filename']}: {e}", file=sys.stderr)
            continue

        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """
    Fetch deliberations updated since a given date.

    Uses incremental archives only.
    """
    archives = list_archives()
    incremental_archives = [a for a in archives if not a['is_full']]

    # Filter by date in filename (YYYYMMDD format)
    since_str = since.strftime('%Y%m%d')
    seen_ids = set()

    for archive in incremental_archives:
        # Extract date from filename (e.g., CNIL_20260113-211022.tar.gz)
        match = re.search(r'CNIL_(\d{8})', archive['filename'])
        if not match:
            continue

        archive_date = match.group(1)
        if archive_date < since_str:
            continue

        print(f"Processing archive: {archive['filename']}", file=sys.stderr)

        try:
            response = requests.get(archive['url'], timeout=300)
            response.raise_for_status()

            with tarfile.open(fileobj=BytesIO(response.content), mode='r:gz') as tar:
                for member in tar.getmembers():
                    if not member.name.endswith('.xml'):
                        continue

                    f = tar.extractfile(member)
                    if f is None:
                        continue

                    xml_content = f.read()
                    deliberation = parse_deliberation_xml(xml_content)

                    if deliberation is None:
                        continue

                    # Skip duplicates
                    if deliberation['_id'] in seen_ids:
                        continue
                    seen_ids.add(deliberation['_id'])

                    yield deliberation

        except Exception as e:
            print(f"Error processing archive {archive['filename']}: {e}", file=sys.stderr)
            continue

        time.sleep(RATE_LIMIT_DELAY)


def normalize(raw: dict) -> dict:
    """Transform raw deliberation data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    # Build URL to Légifrance
    doc_id = raw['_id']
    url = f"https://www.legifrance.gouv.fr/cnil/id/{doc_id}"

    # Use full title if available, otherwise short title
    title = raw.get('titre_full') or raw.get('titre', '')
    if not title:
        numero = raw.get('numero', '')
        nature = raw.get('nature_delib', raw.get('nature', 'Délibération'))
        title = f"Délibération {nature} n° {numero}" if numero else f"Délibération {nature}"

    return {
        '_id': doc_id,
        '_source': 'FR/CNIL',
        '_type': 'doctrine',  # CNIL deliberations are regulatory doctrine
        '_fetched_at': now,
        'title': title,
        'text': raw['text'],
        'date': raw.get('date_texte'),
        'date_publi': raw.get('date_publi'),
        'url': url,
        'numero': raw.get('numero'),
        'nature': raw.get('nature'),
        'nature_delib': raw.get('nature_delib'),
        'etat_juridique': raw.get('etat_juridique'),
        'nor': raw.get('nor'),
        'language': 'fr'
    }


def bootstrap_sample(sample_dir: Path, count: int = 100) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count, use_full_archive=False):
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
            nature = s.get('nature_delib') or s.get('nature') or 'Unknown'
            by_nature[nature] = by_nature.get(nature, 0) + 1

        print(f"\nBy deliberation type:", file=sys.stderr)
        for nature, count in sorted(by_nature.items(), key=lambda x: -x[1]):
            print(f"  {nature}: {count}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='CNIL deliberations fetcher')
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
