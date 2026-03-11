#!/usr/bin/env python3
"""
FR/AMF_Sanctions - AMF Enforcement Committee Decisions Fetcher

Fetches AMF (Autorité des Marchés Financiers) enforcement committee decisions
(Décisions de la Commission des sanctions) using the official REST API.

Data source: https://www.amf-france.org/fr/sanction-transaction/Decisions-de-la-commission-des-sanctions
REST API: https://www.amf-france.org/fr/rest/listing_sanction/{tids}/all/all
License: Licence Ouverte Etalab 2.0

Usage:
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import requests

SOURCE_ID = "FR/AMF_Sanctions"
BASE_URL = "https://www.amf-france.org"

# REST API endpoint for sanctions decisions
# IDs are taxonomy term IDs for different sanction themes
SANCTION_API_URL = "https://www.amf-france.org/fr/rest/listing_sanction/91,184,183,325,90,461,89,242,86,462,181/all/all"

HEADERS = {
    "User-Agent": "World Wide Law/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "application/json,text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

SAMPLE_DIR = Path(__file__).parent / "sample"


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean text."""
    if not html_text:
        return ""
    text = unescape(html_text)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_pdf_text(pdf_content: bytes) -> str:
    """Extract text from PDF content using pdfplumber or PyPDF2."""
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            text_parts = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            return '\n\n'.join(text_parts)
    except ImportError:
        pass
    except Exception as e:
        print(f"    pdfplumber error: {e}")

    # Fallback to PyPDF2
    try:
        from PyPDF2 import PdfReader
        reader = PdfReader(io.BytesIO(pdf_content))
        text_parts = []
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
        return '\n\n'.join(text_parts)
    except ImportError:
        pass
    except Exception as e:
        print(f"    PyPDF2 error: {e}")

    return ""


def fetch_sanctions_api(session: requests.Session) -> list:
    """Fetch all sanctions decisions from the REST API."""
    print(f"Fetching sanctions from REST API: {SANCTION_API_URL}")

    try:
        resp = session.get(SANCTION_API_URL, headers=HEADERS, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching API: {e}")
        return []

    try:
        data = resp.json()
        items = data.get('data', [])
        print(f"Found {len(items)} sanctions decisions from API")
        return items
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}")
        return []


def fetch_pdf_text(pdf_url: str, session: requests.Session) -> str:
    """Download and extract text from a PDF."""
    if not pdf_url:
        return ""

    full_url = urljoin(BASE_URL, pdf_url)

    try:
        resp = session.get(full_url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return extract_pdf_text(resp.content)
    except requests.RequestException as e:
        print(f"    Error fetching PDF: {e}")
        return ""


def normalize(item: dict, pdf_text: str = "") -> dict:
    """Transform API item into standard schema."""
    infos = item.get('infos', {})

    # Extract decision number (e.g., SAN-2026-02)
    title = infos.get('title', '')
    decision_number = title if title.startswith('SAN-') else ''

    # Parse Unix timestamp to date
    date = ""
    try:
        ts = int(item.get('date', 0))
        if ts > 0:
            dt = datetime.fromtimestamp(ts, tz=timezone.utc)
            date = dt.strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        pass

    # Get URL
    url = infos.get('link', {}).get('url', '')
    if url and not url.startswith('http'):
        url = urljoin(BASE_URL, url)

    # Get PDF URL
    pdf_url = infos.get('download', {}).get('sanction', {}).get('links', {}).get('url', '')

    # Extract parties involved
    parties = clean_html(infos.get('text_egard', ''))

    # Extract sanction summary
    sanction_summary = clean_html(infos.get('text', ''))

    # Theme/category
    theme = item.get('theme', '')

    # Recours (appeal)
    recours = infos.get('recours', '')

    # Generate document ID
    if decision_number:
        doc_id = f"FR_AMF_{decision_number.replace('-', '_')}"
    else:
        doc_id = f"FR_AMF_SANC_{hash(url) % 1000000:06d}"

    # Clean PDF text
    full_text = pdf_text.strip() if pdf_text else ""

    # If no PDF text, use available metadata as minimal text
    if not full_text:
        text_parts = []
        if title:
            text_parts.append(f"Décision: {title}")
        if parties:
            text_parts.append(f"À l'égard de: {parties}")
        if sanction_summary:
            text_parts.append(f"Sanction: {sanction_summary}")
        if theme:
            text_parts.append(f"Thème: {theme}")
        full_text = '\n'.join(text_parts)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"{title} - {parties}" if parties else title,
        "text": full_text,
        "date": date,
        "url": url,
        "decision_number": decision_number,
        "parties": parties,
        "sanction_summary": sanction_summary,
        "theme": theme,
        "appeal": recours,
        "pdf_url": urljoin(BASE_URL, pdf_url) if pdf_url else "",
    }


def fetch_all(max_records: int = None, fetch_pdfs: bool = True) -> Generator[dict, None, None]:
    """Fetch all sanctions decisions from the API."""
    session = requests.Session()

    items = fetch_sanctions_api(session)

    count = 0
    errors = 0
    for item in items:
        if max_records and count >= max_records:
            break

        infos = item.get('infos', {})
        title = infos.get('title', 'Unknown')
        print(f"  Processing: {title}...")

        pdf_text = ""
        if fetch_pdfs:
            pdf_url = infos.get('download', {}).get('sanction', {}).get('links', {}).get('url', '')
            if pdf_url:
                pdf_text = fetch_pdf_text(pdf_url, session)
                if pdf_text:
                    print(f"    -> PDF: {len(pdf_text):,} chars")
                else:
                    print(f"    -> No PDF text extracted")
                    errors += 1
            else:
                print(f"    -> No PDF URL available")

        record = normalize(item, pdf_text)
        if record['text'] and len(record['text']) >= 50:
            yield record
            count += 1
        else:
            print(f"    -> Skipped (insufficient text)")
            errors += 1

        time.sleep(1.5)  # Rate limiting

    print(f"Total records yielded: {count}, errors: {errors}")


def fetch_updates(since: datetime, fetch_pdfs: bool = True) -> Generator[dict, None, None]:
    """Fetch decisions updated since the given date."""
    session = requests.Session()

    items = fetch_sanctions_api(session)

    for item in items:
        try:
            ts = int(item.get('date', 0))
            if ts > 0:
                item_date = datetime.fromtimestamp(ts, tz=timezone.utc)
                if item_date.replace(tzinfo=None) < since.replace(tzinfo=None):
                    continue
        except (ValueError, TypeError):
            pass

        infos = item.get('infos', {})
        title = infos.get('title', 'Unknown')
        print(f"  Processing: {title}...")

        pdf_text = ""
        if fetch_pdfs:
            pdf_url = infos.get('download', {}).get('sanction', {}).get('links', {}).get('url', '')
            if pdf_url:
                pdf_text = fetch_pdf_text(pdf_url, session)

        record = normalize(item, pdf_text)
        if record['text'] and len(record['text']) >= 50:
            yield record

        time.sleep(1.5)


def bootstrap_sample(sample_count: int = 15) -> bool:
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    session = requests.Session()
    items = fetch_sanctions_api(session)

    if not items:
        print("ERROR: No sanctions decisions found in API!")
        return False

    records = []
    errors = 0

    # Get most recent items for sample
    for item in items[:sample_count + 5]:  # Fetch a few extra in case some fail
        if len(records) >= sample_count:
            break

        infos = item.get('infos', {})
        title = infos.get('title', 'Unknown')
        print(f"  Fetching: {title}...")

        pdf_url = infos.get('download', {}).get('sanction', {}).get('links', {}).get('url', '')
        pdf_text = ""

        if pdf_url:
            pdf_text = fetch_pdf_text(pdf_url, session)
            if pdf_text:
                print(f"    -> PDF: {len(pdf_text):,} chars")
            else:
                print(f"    -> Failed to extract PDF text")
                errors += 1
        else:
            print(f"    -> No PDF URL")

        record = normalize(item, pdf_text)

        if record['text'] and len(record['text']) >= 100:
            records.append(record)

            # Save individual record
            filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"    -> Saved: {len(record['text']):,} chars")
        else:
            print(f"    -> Skipped (insufficient text: {len(record.get('text', ''))} chars)")
            errors += 1

        time.sleep(1.5)

    # Print summary
    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")
    print(f"Errors: {errors}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        # Count by year
        year_counts = {}
        for r in records:
            year = r.get('date', '')[:4] if r.get('date') else 'Unknown'
            year_counts[year] = year_counts.get(year, 0) + 1

        print(f"Year distribution: {year_counts}")

        # Check for PDF vs non-PDF text
        has_pdf = sum(1 for r in records if len(r.get('text', '')) > 500)
        print(f"Records with substantial text (>500 chars): {has_pdf}/{len(records)}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get('text') or len(r['text']) < 100)
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description=f"{SOURCE_ID} sanctions fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=15,
                       help="Number of sample records to fetch")
    parser.add_argument('--since', type=str,
                       help="Fetch updates since date (ISO format)")
    parser.add_argument('--no-pdf', action='store_true',
                       help="Skip PDF downloads (faster, less text)")

    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)
        else:
            for record in fetch_all(fetch_pdfs=not args.no_pdf):
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'fetch':
        for record in fetch_all(fetch_pdfs=not args.no_pdf):
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'updates':
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since, fetch_pdfs=not args.no_pdf):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
