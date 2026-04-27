#!/usr/bin/env python3
"""
French Competition Authority (Autorité de la Concurrence) Data Fetcher

Fetches competition law decisions from the ADLC open data portal.
Data includes merger control decisions, antitrust decisions, opinions, and interim measures.

Data source:
- https://www.data.gouv.fr/datasets/decisions-publiees-par-lautorite-de-la-concurrence-depuis-1988/
- JSON file with full text of all decisions since 1988

License: Open Licence 2.0 (Etalab)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests

# Constants
DATASET_URL = "https://www.data.gouv.fr/api/1/datasets/decisions-publiees-par-lautorite-de-la-concurrence-depuis-1988/"
RATE_LIMIT_DELAY = 0.5  # seconds between API calls


def get_json_resource_url() -> Optional[str]:
    """Get the URL of the French JSON resource from data.gouv.fr API."""
    try:
        response = requests.get(DATASET_URL, timeout=30)
        response.raise_for_status()
        data = response.json()

        # Find the French JSON resource (larger file)
        for resource in data.get('resources', []):
            if resource.get('format', '').lower() == 'json':
                title = resource.get('title', '').lower()
                # Prefer the French version (larger file, not '-en' suffix)
                if 'en.json' not in title and resource.get('filesize', 0) > 100000000:
                    return resource.get('url')

        # Fallback: just find any JSON resource
        for resource in data.get('resources', []):
            if resource.get('format', '').lower() == 'json' and 'en.json' not in resource.get('title', '').lower():
                return resource.get('url')

        return None
    except Exception as e:
        print(f"Error fetching dataset metadata: {e}", file=sys.stderr)
        return None


def clean_text(text: str) -> str:
    """Clean up decision text."""
    if not text:
        return ""
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse French date string to ISO format."""
    if not date_str:
        return None

    # Already in ISO format (YYYY-MM-DD)
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str

    # French month names
    months = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
        'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
        'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12'
    }

    # Try "DD month YYYY" format
    match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str)
    if match:
        day, month, year = match.groups()
        month_num = months.get(month.lower())
        if month_num:
            return f"{year}-{month_num}-{day.zfill(2)}"

    return None


def determine_decision_type(type_code) -> str:
    """Map decision type code to description."""
    # Handle list types (some records have multiple types)
    if isinstance(type_code, list):
        type_code = type_code[0] if type_code else ''

    types = {
        'D': 'antitrust_decision',           # Décisions contentieuses
        'Décision': 'antitrust_decision',    # French spelling
        'A': 'opinion',                       # Avis
        'Avis': 'opinion',                    # French spelling
        'MC': 'interim_measures',             # Mesures conservatoires
        'DCC': 'merger_control',              # Décisions de contrôle des concentrations
        'DEX': 'merger_exemption',            # Décision d'exemption
        'SOA': 'sector_opinion',              # Avis sectoriel
    }
    return types.get(type_code, 'other')


def normalize(raw: dict) -> dict:
    """Transform raw decision data into normalized schema."""
    decision_id = raw.get('id_decision', '')

    # Determine document type
    type_code = raw.get('type_decision', '')
    doc_type = determine_decision_type(type_code)

    # Normalize type_code for storage
    type_code_str = type_code[0] if isinstance(type_code, list) else type_code

    # Parse date
    date = raw.get('date_decision_datetime') or parse_date(raw.get('date_decision', ''))

    # Get full text
    text = clean_text(raw.get('texte_complet_decision', ''))

    # Build normalized record
    return {
        '_id': f"FR/ADLC/{decision_id}",
        '_source': 'FR/ADLC',
        '_type': 'case_law',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'decision_id': decision_id,
        'title': raw.get('titre_decision', ''),
        'date': date,
        'year': raw.get('date_decision_year'),
        'decision_type': doc_type,
        'decision_type_code': type_code_str,
        'url': raw.get('url_site', ''),
        'text': text,
        'companies_involved': raw.get('entreprises_concernees', []),
        'notifying_party': raw.get('partie_notifiante', ''),
        'sector': raw.get('secteur_activite', []),
        'simplified_procedure': raw.get('decision_simplifiee', '') == 'Oui',
        'merger_phase': raw.get('phase_decision_concentration', ''),
        'merger_disposition': raw.get('dispositifs_dcc', ''),
        'operation_type': raw.get('type_operation_concentration', ''),
    }


def fetch_all_data() -> list:
    """Download and parse the full JSON dataset."""
    print("Finding JSON resource URL...", file=sys.stderr)
    json_url = get_json_resource_url()

    if not json_url:
        raise ValueError("Could not find JSON resource URL")

    print(f"Downloading dataset from {json_url}...", file=sys.stderr)

    response = requests.get(json_url, timeout=300, stream=True)
    response.raise_for_status()

    # Parse JSON
    data = response.json()
    print(f"Loaded {len(data)} decisions", file=sys.stderr)

    return data


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all available ADLC decisions."""
    data = fetch_all_data()

    for raw in data:
        yield normalize(raw)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date."""
    for doc in fetch_all():
        doc_date = doc.get('date')
        if doc_date:
            try:
                doc_dt = datetime.fromisoformat(doc_date)
                if doc_dt.replace(tzinfo=timezone.utc) >= since.replace(tzinfo=timezone.utc):
                    yield doc
            except ValueError:
                yield doc
        else:
            yield doc


def bootstrap_sample(limit: int = 15) -> None:
    """Fetch sample records for testing."""
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)

    # Clear existing samples
    for f in sample_dir.glob('*.json'):
        f.unlink()

    print("Fetching ADLC sample data...", file=sys.stderr)

    # Download full dataset
    data = fetch_all_data()

    # Get diverse sample: some from each type
    type_counts = {}
    samples = []

    # Sort by date descending to get recent decisions
    sorted_data = sorted(data, key=lambda x: x.get('date_decision_datetime') or '', reverse=True)

    for raw in sorted_data:
        if len(samples) >= limit:
            break

        type_code = raw.get('type_decision', 'other')
        text = raw.get('texte_complet_decision', '')

        # Skip if no text
        if not text or len(text) < 100:
            continue

        # Limit per type to ensure diversity
        type_counts[type_code] = type_counts.get(type_code, 0) + 1
        if type_counts[type_code] > (limit // 3 + 1):
            continue

        samples.append(raw)

    # Normalize and save
    count = 0
    for raw in samples:
        doc = normalize(raw)

        # Save to sample directory
        safe_id = doc['decision_id'].replace('/', '-').replace('\\', '-')
        filename = f"{safe_id}.json"
        filepath = sample_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        text_len = len(doc.get('text', ''))
        print(f"Saved {filename} ({text_len:,} chars)", file=sys.stderr)
        count += 1

    print(f"\nSaved {count} sample records to {sample_dir}", file=sys.stderr)

    # Print type breakdown
    type_breakdown = {}
    for raw in samples:
        t = raw.get('type_decision', 'other')
        type_breakdown[t] = type_breakdown.get(t, 0) + 1

    print("Type breakdown:", file=sys.stderr)
    for t, c in sorted(type_breakdown.items()):
        print(f"  - {t}: {c}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description='ADLC (Autorité de la Concurrence) Data Fetcher')
    subparsers = parser.add_subparsers(dest='command')

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser('bootstrap', help='Fetch sample data')
    bootstrap_parser.add_argument('--sample', action='store_true', help='Fetch sample records')
    bootstrap_parser.add_argument('--limit', type=int, default=15, help='Number of records to fetch')
    bootstrap_parser.add_argument("--full", action="store_true", help="Fetch all records")

    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show dataset statistics')

    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(args.limit)
        else:
            print("Use --sample to fetch sample records", file=sys.stderr)
    elif args.command == 'stats':
        data = fetch_all_data()
        print(f"Total decisions: {len(data)}")

        # Count by type
        types = {}
        years = {}
        for d in data:
            t = d.get('type_decision', 'unknown')
            y = d.get('date_decision_year', 'unknown')
            types[t] = types.get(t, 0) + 1
            years[y] = years.get(y, 0) + 1

        print("\nBy type:")
        for t, c in sorted(types.items(), key=lambda x: -x[1]):
            print(f"  {t}: {c}")

        print(f"\nYears covered: {min(y for y in years.keys() if isinstance(y, int))} - {max(y for y in years.keys() if isinstance(y, int))}")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
