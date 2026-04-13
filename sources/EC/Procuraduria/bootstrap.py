#!/usr/bin/env python3
"""
EC/Procuraduria - Ecuador Attorney General Legal Pronouncements Fetcher

Fetches binding legal pronouncements (pronunciamientos) from Ecuador's
Procuraduria General del Estado via monthly extract PDFs.

Data source: https://www.pge.gob.ec/index.php/servicios/consultas/extractos-de-pronunciamientos
License: Open data (public government legal opinions)
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

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


BASE_URL = "https://www.pge.gob.ec"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "EC/Procuraduria"

MONTHS_ES = [
    'Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio',
    'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'
]

# URL patterns by year range (inconsistent naming across years)
URL_PATTERNS = {
    (2026, 2026): '/images/{year}/ROTATIVOS/PRONUNCIAMIENTOS/Extractos_Pronunciamientos_{month_lower}_{year}.pdf',
    (2025, 2025): '/images/{year}/ROTATIVOS/pronunciamientos/Extracto_Pronunciamientos_{month_title}_{year}.pdf',
    (2024, 2024): '/images/{year}/PRONUNCIAMIENTOS/EP EXTRACTO DE PRONUNCIAMIENTOS DE {month_upper} DE {year}.pdf',
    (2017, 2023): '/images/{year}/extractos/EP_EXTRACTO_DE_PRONUNCIAMIENTOS_{month_upper}_{year}.pdf',
    (2016, 2016): '/images/{year}/extractos/EP_EXTRACTO_DE_PRONUNCIAMIENTOS_{month_upper}_{year}.pdf',
}


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
    })
    return session


def parse_date_dmy(date_str: str) -> Optional[str]:
    """Parse DD-MM-YYYY to YYYY-MM-DD."""
    if not date_str or not date_str.strip():
        return None
    try:
        dt = datetime.strptime(date_str.strip(), '%d-%m-%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        return None


def build_pdf_url(year: int, month: int) -> list:
    """Build candidate PDF URLs for a given year/month."""
    month_title = MONTHS_ES[month - 1]
    month_lower = month_title.lower()
    month_upper = month_title.upper()

    candidates = []
    for (y_min, y_max), pattern in URL_PATTERNS.items():
        if y_min <= year <= y_max:
            url = pattern.format(
                year=year,
                month_title=month_title,
                month_lower=month_lower,
                month_upper=month_upper,
            )
            candidates.append(f"{BASE_URL}{url}")

    # Also try common alternative patterns
    alts = [
        f'/images/{year}/extractos/EP_EXTRACTO_DE_PRONUNCIAMIENTOS_{month_upper}_{year}.pdf',
        f'/images/{year}/Pronunciamientos/EP_EXTRACTO_DE_PRONUNCIAMIENTOS_{month_upper}_{year}.pdf',
        f'/images/{year}/ROTATIVOS/pronunciamientos/Extracto_Pronunciamientos_{month_title}_{year}.pdf',
        f'/images/{year}/ROTATIVOS/PRONUNCIAMIENTOS/Extractos_Pronunciamientos_{month_lower}_{year}.pdf',
        f'/images/{year}/PRONUNCIAMIENTOS/EP EXTRACTO DE PRONUNCIAMIENTOS DE {month_upper} DE {year}.pdf',
    ]
    for alt in alts:
        full = f"{BASE_URL}{alt}"
        if full not in candidates:
            candidates.append(full)

    return candidates


def download_extract_pdf(year: int, month: int, session: requests.Session) -> Optional[bytes]:
    """Download an extract PDF for a given year/month, trying multiple URL patterns."""
    candidates = build_pdf_url(year, month)

    for url in candidates:
        try:
            resp = session.get(url, timeout=30)
            if resp.status_code == 200:
                ct = resp.headers.get('Content-Type', '')
                if 'pdf' in ct or 'octet-stream' in ct or len(resp.content) > 5000:
                    # Verify it's actually a PDF
                    if resp.content[:4] == b'%PDF':
                        return resp.content
            time.sleep(0.3)
        except requests.RequestException:
            continue

    return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="EC/Procuraduria",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

def parse_pronouncements(full_text: str) -> list:
    """Parse individual pronouncements from extract PDF text."""
    # Split on pattern: Oficio number + OF. PGE No.:
    records = re.split(r'(?=\d{4,6}\s+OF\.\s*PGE\s*No\.\s*:)', full_text)
    records = [r for r in records if re.match(r'\d{4,6}\s+OF\.\s*PGE', r.strip())]

    parsed = []
    for rec_text in records:
        rec = parse_single_pronouncement(rec_text)
        if rec:
            parsed.append(rec)

    return parsed


def parse_single_pronouncement(text: str) -> Optional[dict]:
    """Parse a single pronouncement from extract text."""
    # Oficio number and date
    m = re.match(r'(\d{4,6})\s+OF\.\s*PGE\s*No\.\s*:\s*(\d{2}-\d{2}-\d{4})', text)
    if not m:
        return None

    oficio = m.group(1)
    date_raw = m.group(2)

    # Consultante: entity name after date line, before CONSULTANTE:
    cm = re.search(r'de\n(.+?)CONSULTANTE:', text, re.DOTALL)
    consultante = cm.group(1).strip().replace('\n', ' ') if cm else ''

    # Sector
    sm = re.search(r'CONSULTANTE:\s*\n?(.*?)SECTOR:', text, re.DOTALL)
    sector = sm.group(1).strip().replace('\n', ' ') if sm else ''

    # Materia
    mm = re.search(r'MATERIA:\s*\n?(.*?)(?:Submateria|Consulta)', text, re.DOTALL)
    materia = mm.group(1).strip().replace('\n', ' ') if mm else ''

    # Submateria
    subm = re.search(r'Submateria\s*/\s*Tema:\s*\n?(.*?)(?:Consulta|\n[A-Z])', text, re.DOTALL)
    submateria = ''
    if subm:
        submateria = subm.group(1).strip().replace('\n', ' ')

    # Extract consultation question and pronouncement answer
    # The text before "Consulta(s)" marker is the question
    # The text after "Consulta(s)" marker is the answer/pronouncement
    consulta_text = ''
    pronunciamiento_text = ''

    # Find the question text (between Submateria/Tema or MATERIA and Consulta(s))
    qm = re.search(r'(?:Submateria\s*/\s*Tema:\s*\n?|MATERIA:\s*\n?[^\n]*\n)(.*?)Consulta\(s\)', text, re.DOTALL)
    if qm:
        consulta_text = qm.group(1).strip()
        # Clean up leading submateria text that gets captured
        consulta_text = re.sub(r'^[A-ZÁÉÍÓÚÑ\s/]+(?:Submateria\s*/\s*Tema:)?\s*', '', consulta_text).strip()

    # Get pronunciamiento text (after "Consulta(s)" to end, minus boilerplate)
    pm = re.search(r'Consulta\(s\)\s*\n?(.*?)(?:Enlace\s+Lexis|PROCURADURÍA\s+GENERAL|$)', text, re.DOTALL)
    if pm:
        pronunciamiento_text = pm.group(1).strip()

    # If we couldn't split cleanly, use everything after the metadata
    if not consulta_text and not pronunciamiento_text:
        body_m = re.search(r'(?:Submateria.*?:|MATERIA:.*?\n)\s*(.*?)(?:PROCURADURÍA|$)', text, re.DOTALL)
        if body_m:
            pronunciamiento_text = body_m.group(1).strip()

    # Build combined text
    text_parts = []
    if consulta_text:
        text_parts.append(f"CONSULTA:\n{consulta_text}")
    if pronunciamiento_text:
        text_parts.append(f"PRONUNCIAMIENTO:\n{pronunciamiento_text}")

    combined_text = '\n\n'.join(text_parts)

    if len(combined_text) < 100:
        return None

    return {
        'oficio': oficio,
        'date': parse_date_dmy(date_raw),
        'date_raw': date_raw,
        'consultante': consultante,
        'sector': sector,
        'materia': materia,
        'submateria': submateria if submateria != materia else '',
        'text': combined_text,
    }


def normalize(raw: dict, year: int, month: int) -> dict:
    """Transform raw pronouncement data into standard schema."""
    oficio = raw['oficio']
    doc_id = f"PGE-{oficio}-{raw.get('date_raw', f'{year}-{month:02d}')}"

    title_parts = []
    if raw.get('materia'):
        title_parts.append(raw['materia'])
    if raw.get('consultante'):
        title_parts.append(f"({raw['consultante'][:80]})")
    title = ' - '.join(title_parts) if title_parts else f"Pronunciamiento OF.PGE {oficio}"

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.utcnow().isoformat() + 'Z',
        'title': title,
        'text': raw['text'],
        'date': raw.get('date'),
        'url': f"{BASE_URL}/index.php/servicios/consultas/extractos-de-pronunciamientos",
        'oficio_number': oficio,
        'consultante': raw.get('consultante', ''),
        'sector': raw.get('sector', ''),
        'materia': raw.get('materia', ''),
        'submateria': raw.get('submateria', ''),
        'year': year,
        'month': month,
        'language': 'spa',
    }


def fetch_all(
    max_records: int = None,
    year: int = None,
) -> Generator[dict, None, None]:
    """Fetch pronouncements from extract PDFs."""
    count = 0
    current_year = datetime.now().year
    session = get_session()

    if year:
        years = [year]
    else:
        years = list(range(current_year, 2016, -1))

    for yr in years:
        if max_records and count >= max_records:
            break

        for month in range(1, 13):
            if max_records and count >= max_records:
                break

            month_name = MONTHS_ES[month - 1]
            print(f"Fetching {yr}/{month_name}... [{count} records so far]")

            pdf_bytes = download_extract_pdf(yr, month, session)
            if not pdf_bytes:
                print(f"  -> No PDF found for {yr}/{month_name}")
                continue

            print(f"  -> PDF: {len(pdf_bytes):,} bytes")
            full_text = extract_text_from_pdf(pdf_bytes)
            if not full_text:
                print(f"  -> Could not extract text")
                continue

            pronouncements = parse_pronouncements(full_text)
            print(f"  -> Found {len(pronouncements)} pronouncements")

            for raw in pronouncements:
                if max_records and count >= max_records:
                    break

                normalized = normalize(raw, yr, month)
                text_len = len(normalized.get('text', ''))
                if text_len >= 100:
                    yield normalized
                    count += 1

            time.sleep(1.5)

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch pronouncements from the current year."""
    current_year = datetime.now().year
    for record in fetch_all(year=current_year):
        if record.get('date'):
            try:
                doc_date = datetime.strptime(record['date'], '%Y-%m-%d')
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def bootstrap_sample(sample_count: int = 15):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []
    for record in fetch_all(max_records=sample_count + 5):
        if len(records) >= sample_count:
            break

        text_len = len(record.get('text', ''))
        if text_len < 100:
            continue

        records.append(record)

        filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  [{len(records):02d}] OF.PGE {record['oficio_number']}: {text_len:,} chars")
        print(f"       {record.get('title', '')[:60]}...")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    insufficient = sum(1 for r in records if not r.get('text') or len(r['text']) < 100)
    if insufficient > 0:
        print(f"WARNING: {insufficient} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="EC/Procuraduria legal pronouncements fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'test'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=15,
                       help="Number of sample records to fetch")
    parser.add_argument('--year', type=int, default=None,
                       help="Specific year to fetch")

    args = parser.parse_args()

    if args.command == 'test':
        print("Testing PDF download...")
        session = get_session()
        pdf = download_extract_pdf(2025, 1, session)
        if pdf:
            print(f"Downloaded PDF: {len(pdf):,} bytes")
            text = extract_text_from_pdf(pdf)
            records = parse_pronouncements(text)
            print(f"Found {len(records)} pronouncements")
            if records:
                r = records[0]
                print(f"First: OF.PGE {r['oficio']} ({r['date']})")
                print(f"  Materia: {r['materia']}")
                print(f"  Text: {len(r['text'])} chars")
        else:
            print("Failed to download PDF")

    elif args.command == 'bootstrap':
        if args.sample or True:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all(year=args.year):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
