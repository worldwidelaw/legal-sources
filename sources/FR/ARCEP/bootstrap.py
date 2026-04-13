#!/usr/bin/env python3
"""
ARCEP (Autorité de régulation des communications électroniques) Data Fetcher

Fetches regulatory decisions ("Avis et décisions") from ARCEP, the French telecom
and postal regulatory authority.

Data source: https://www.arcep.fr/la-regulation/avis-et-decisions-de-larcep.html
License: Open Government License (Licence Ouverte)

Coverage:
- 46,000+ decisions from 1997 to present
- Categories: frequency authorizations, network access, universal service,
  numbering, postal services, etc.

Data access:
- CSV export endpoint provides metadata (decision number, date, category, description)
- Direct PDF links at https://www.arcep.fr/uploads/tx_gsavis/{decision_number}.pdf
- Full text extracted from PDFs using pdfplumber
"""

import csv
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
BASE_URL = "https://www.arcep.fr"
CSV_URL = f"{BASE_URL}/la-regulation/avis-et-decisions-de-larcep/download.csv?tx_arcepavis_results%5Bformat%5D=csv"
PDF_BASE_URL = f"{BASE_URL}/uploads/tx_gsavis/"
RATE_LIMIT_DELAY = 1.0  # seconds between PDF downloads


def safe_get(row: dict, key: str, default: str = '') -> str:
    """Safely get a value from a dict row, handling None values."""
    value = row.get(key)
    if value is None:
        return default
    return str(value).strip()


def fetch_csv(session: requests.Session) -> list[dict]:
    """Fetch the CSV export with all decision metadata."""
    response = session.get(CSV_URL, timeout=60)
    response.raise_for_status()

    # CSV is in ISO-8859-1 (latin-1) encoding
    content = response.content.decode('iso-8859-1')

    # Parse CSV (semicolon-delimited) with positional access
    # Columns: Numéro;Date d'adoption;Date de publication;Catégorie;Description;Lien vers le document;Liens vers les annexes
    reader = csv.reader(io.StringIO(content), delimiter=';')

    records = []
    header = next(reader, None)  # Skip header row

    for row in reader:
        if len(row) < 6:
            continue
        record = {
            'number': row[0].strip() if row[0] else '',
            'adoption_date': row[1].strip() if len(row) > 1 and row[1] else '',
            'publication_date': row[2].strip() if len(row) > 2 and row[2] else '',
            'category': row[3].strip() if len(row) > 3 and row[3] else '',
            'description': row[4].strip() if len(row) > 4 and row[4] else '',
            'pdf_url': row[5].strip() if len(row) > 5 and row[5] else '',
            'annexes': row[6].strip() if len(row) > 6 and row[6] else ''
        }
        if record['number'] and record['pdf_url']:
            records.append(record)

    return records


def parse_date(date_str: str) -> Optional[str]:
    """Parse French date format (DD/MM/YYYY) to ISO format (YYYY-MM-DD)."""
    if not date_str:
        return None
    try:
        # Handle DD/MM/YYYY format
        dt = datetime.strptime(date_str, '%d/%m/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return None


def extract_pdf_text(pdf_url: str, session: requests.Session) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="FR/ARCEP",
        source_id="",
        pdf_url=pdf_url,
        table="doctrine",
    ) or ""

def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all ARCEP decisions with full text."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Legal-Data-Hunter/1.0 (Open Data Collection for Research)',
        'Accept': 'text/html,application/xhtml+xml,application/pdf',
        'Accept-Language': 'fr,en;q=0.9'
    })

    # Fetch CSV with all metadata
    print("Fetching decision catalog from CSV...", file=sys.stderr)
    records = fetch_csv(session)
    print(f"Found {len(records):,} decisions in catalog", file=sys.stderr)

    if max_docs:
        records = records[:max_docs]

    doc_count = 0
    for record in records:
        pdf_url = record['pdf_url']

        if not pdf_url or not pdf_url.startswith('http'):
            continue

        time.sleep(RATE_LIMIT_DELAY)

        # Download and extract PDF text
        text = extract_pdf_text(pdf_url, session)

        if not text or len(text) < 100:
            print(f"  Skipping {record['number']}: no text or too short", file=sys.stderr)
            continue

        # Build document with metadata + text
        doc = {
            '_id': f"ARCEP-{record['number']}",
            'number': record['number'],
            'title': record['description'],
            'category': record['category'],
            'adoption_date': parse_date(record['adoption_date']),
            'publication_date': parse_date(record['publication_date']),
            'pdf_url': pdf_url,
            'annexes_url': record.get('annexes'),
            'text': text
        }

        doc_count += 1
        print(f"  [{doc_count}] {doc['_id']}: {len(text):,} chars", file=sys.stderr)

        yield doc


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date."""
    since_str = since.strftime('%Y-%m-%d')

    for doc in fetch_all():
        pub_date = doc.get('publication_date')
        if pub_date and pub_date >= since_str:
            yield doc


def normalize(raw: dict) -> dict:
    """Transform raw document data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    # Use adoption date as the primary date, fallback to publication date
    date = raw.get('adoption_date') or raw.get('publication_date')

    return {
        '_id': raw['_id'],
        '_source': 'FR/ARCEP',
        '_type': 'doctrine',  # Regulatory decisions are doctrine
        '_fetched_at': now,
        'title': raw.get('title', ''),
        'text': raw['text'],
        'date': date,
        'url': raw.get('pdf_url'),
        'pdf_url': raw.get('pdf_url'),
        'number': raw.get('number'),
        'category': raw.get('category'),
        'adoption_date': raw.get('adoption_date'),
        'publication_date': raw.get('publication_date'),
        'language': 'fr'
    }


def bootstrap_sample(sample_dir: Path, count: int = 12) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count):
        record = normalize(raw)
        samples.append(record)

        # Save individual sample
        filename = f"{record['_id']}.json"
        # Sanitize filename
        filename = re.sub(r'[^\w\-.]', '_', filename)

        with open(sample_dir / filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text']):,} chars)", file=sys.stderr)

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

        # Count by category
        by_cat = {}
        for s in samples:
            cat = s.get('category', 'Unknown')
            by_cat[cat] = by_cat.get(cat, 0) + 1

        print(f"\nBy category:", file=sys.stderr)
        for cat, count in sorted(by_cat.items(), key=lambda x: -x[1]):
            print(f"  {cat}: {count}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='ARCEP decisions fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Generate sample data only')
    parser.add_argument('--count', type=int, default=12,
                       help='Number of samples to generate')
    parser.add_argument('--since', type=str,
                       help='Fetch updates since date (YYYY-MM-DD)')

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
