#!/usr/bin/env python3
"""
EC/Contraloria - Ecuador Comptroller General Audit Reports Fetcher

Fetches approved audit reports (Informes Aprobados) from Ecuador's
Contraloria General del Estado via their DataTables JSON API.

Data source: https://www.contraloria.gob.ec/Consultas/InformesAprobados
License: Open data (public government audit reports)
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

import pdfplumber
import requests

BASE_URL = "https://www.contraloria.gob.ec"
LISTING_URL = f"{BASE_URL}/WFResultados.aspx"
DOWNLOAD_URL = f"{BASE_URL}/WFDescarga.aspx"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "EC/Contraloria"

EXAM_TYPES = {
    'EE': 'Examen Especial',
    'AG': 'Auditoría Gubernamental',
    'AF': 'Auditoría Financiera',
    'AOP': 'Auditoría de Obras Públicas',
    'EEI': 'Examen Especial Interno',
    'ACOO': 'Auditoría Coordinada',
    'SFP': 'Seguimiento de Fiscalización Posterior',
}


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
        'X-Requested-With': 'XMLHttpRequest',
    })
    return session


def parse_date(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY to YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), '%d/%m/%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return None


def fetch_listing(year: int, start: int = 0, length: int = 100) -> dict:
    """Fetch a page of approved audit reports for a given year."""
    session = get_session()
    params = {
        'tipo': 'ia',
        'ne': '',
        'ui': '',
        'uc': '',
        'ex': '',
        'an': str(year),
        'ni': '',
        'draw': '1',
        'start': str(start),
        'length': str(length),
    }
    resp = session.get(LISTING_URL, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()


def download_pdf(file_id: str) -> bytes:
    """Download a report PDF by file ID."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
        'Referer': f'{BASE_URL}/Consultas/InformesAprobados',
    })
    resp = session.get(DOWNLOAD_URL, params={'id': file_id, 'tipo': 'inf'}, timeout=120)
    resp.raise_for_status()
    content_type = resp.headers.get('Content-Type', '')
    if 'pdf' not in content_type and 'octet-stream' not in content_type:
        raise ValueError(f"Unexpected content type: {content_type}")
    return resp.content


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF document using pdfplumber."""
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
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)
    return full_text.strip()


def normalize(raw: dict) -> dict:
    """Transform raw listing data into standard schema."""
    return {
        '_id': raw['file_id'],
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.utcnow().isoformat() + 'Z',
        'title': raw.get('description', ''),
        'text': raw.get('text', ''),
        'date': raw.get('approval_date'),
        'url': f"{DOWNLOAD_URL}?id={raw['file_id']}&tipo=inf",
        'report_number': raw.get('report_number', ''),
        'control_unit': raw.get('control_unit', ''),
        'exam_type': raw.get('exam_type', ''),
        'exam_type_label': EXAM_TYPES.get(raw.get('exam_type', ''), raw.get('exam_type', '')),
        'audited_entity': raw.get('entity', ''),
        'year': raw.get('year'),
        'period_from': raw.get('period_from'),
        'period_to': raw.get('period_to'),
        'language': 'spa',
    }


def parse_row(row: list) -> dict:
    """Parse a DataTables row array into a structured dict."""
    return {
        'control_unit': (row[0] or '').strip(),
        'exam_type': (row[1] or '').strip(),
        'entity': (row[2] or '').strip(),
        'year': int(row[3]) if row[3] else None,
        'period_from': parse_date(row[4]),
        'period_to': parse_date(row[5]),
        'approval_date': parse_date(row[6]),
        'report_number': (row[7] or '').strip(),
        'description': (row[8] or '').strip(),
        'file_id': (row[9] or '').strip(),
        'synthesis_id': (row[10] or '').strip() if len(row) > 10 else '',
    }


def fetch_report_with_text(row_data: dict) -> Optional[dict]:
    """Download PDF and extract text for a report."""
    file_id = row_data.get('file_id', '')
    report_num = row_data.get('report_number', file_id[:20])

    if not file_id:
        print(f"    -> No file ID for {report_num}")
        return None

    print(f"  Processing {report_num}...")

    try:
        print(f"    -> Downloading PDF (file_id={file_id})")
        pdf_bytes = download_pdf(file_id)
        print(f"    -> Extracting text from PDF ({len(pdf_bytes):,} bytes)")
        text = extract_pdf_text(pdf_bytes)

        if not text or len(text) < 2000:
            print(f"    -> Insufficient text extracted ({len(text) if text else 0} chars, need 2000+)")
            return None

        row_data['text'] = text
        print(f"    -> Extracted {len(text):,} chars")
        return row_data

    except requests.HTTPError as e:
        print(f"    -> HTTP error downloading PDF: {e}")
        return None
    except Exception as e:
        print(f"    -> Error processing PDF: {e}")
        return None


def fetch_all(
    max_records: int = None,
    year: int = None,
) -> Generator[dict, None, None]:
    """
    Fetch audit reports with full text.

    Args:
        max_records: Maximum number of records to yield
        year: Specific year to fetch (default: most recent years)

    Yields:
        Normalized document records with full text
    """
    count = 0
    current_year = datetime.now().year

    if year:
        years = [year]
    else:
        years = list(range(current_year, 2002, -1))

    for yr in years:
        if max_records and count >= max_records:
            break

        start = 0
        length = 100

        while True:
            if max_records and count >= max_records:
                break

            print(f"Fetching year {yr}, offset {start}... [{count} records so far]")

            try:
                result = fetch_listing(yr, start=start, length=length)
            except requests.HTTPError as e:
                print(f"Error fetching listing: {e}")
                break

            data = result.get('data', [])
            if not data:
                break

            for row in data:
                if max_records and count >= max_records:
                    break

                row_data = parse_row(row)

                report = fetch_report_with_text(row_data)
                if report and report.get('text'):
                    try:
                        normalized = normalize(report)
                        text_len = len(normalized.get('text', ''))
                        if text_len >= 500:
                            yield normalized
                            count += 1
                        else:
                            print(f"    -> Skipping: text too short ({text_len} chars)")
                    except Exception as e:
                        print(f"    -> Error normalizing: {e}")

                time.sleep(1.5)

            total = result.get('recordsFiltered', 0)
            start += length
            if start >= total:
                break

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    current_year = datetime.now().year
    for record in fetch_all(year=current_year):
        if record.get('date'):
            try:
                doc_date = datetime.strptime(record['date'], '%Y-%m-%d')
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def bootstrap_sample(sample_count: int = 12):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    # Find the most recent year with data
    current_year = datetime.now().year
    sample_year = None
    for yr in range(current_year, 2002, -1):
        try:
            result = fetch_listing(yr, start=0, length=1)
            total = result.get('recordsFiltered', 0)
            print(f"Reports for {yr}: {total}")
            if total > 0 and sample_year is None:
                sample_year = yr
                break
        except Exception:
            continue

    if not sample_year:
        print("No reports found in any year!")
        return False

    print()

    records = []
    for record in fetch_all(max_records=sample_count + 5, year=sample_year):
        if len(records) >= sample_count:
            break

        text_len = len(record.get('text', ''))
        if text_len < 500:
            print(f"  Skipping {record.get('report_number')}: Text too short ({text_len} chars)")
            continue

        records.append(record)

        filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        title_preview = record.get('title', '')[:60]
        print(f"  [{len(records):02d}] {record['report_number']}: {text_len:,} chars")
        print(f"       {title_preview}...")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        type_counts = {}
        for r in records:
            etype = r.get('exam_type_label', 'Unknown')
            type_counts[etype] = type_counts.get(etype, 0) + 1
        print("Exam types:")
        for etype, cnt in sorted(type_counts.items()):
            print(f"  {etype}: {cnt}")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    insufficient = sum(1 for r in records if not r.get('text') or len(r['text']) < 500)
    if insufficient > 0:
        print(f"WARNING: {insufficient} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="EC/Contraloria audit reports fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'test'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument('--year', type=int, default=None,
                       help="Specific year to fetch")

    args = parser.parse_args()

    if args.command == 'test':
        print("Testing API connection...")
        result = fetch_listing(2024, start=0, length=3)
        total = result.get('recordsFiltered', 0)
        data = result.get('data', [])
        print(f"Found {total} total reports for 2024, got {len(data)} in this page")
        if data:
            row = parse_row(data[0])
            print(f"First report: {row['report_number']}")
            print(f"  Entity: {row['entity']}")
            print(f"  Type: {row['exam_type']} ({EXAM_TYPES.get(row['exam_type'], '?')})")
            print(f"  Date: {row['approval_date']}")
            print(f"  File ID: {row['file_id']}")

    elif args.command == 'bootstrap':
        if args.sample or True:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all(year=args.year):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
