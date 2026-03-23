#!/usr/bin/env python3
"""
European Medicines Agency (EMA) Data Fetcher

Data source: EMA Open Data JSON Files
API: https://www.ema.europa.eu/en/about-us/about-website/download-website-data-json-data-format

This fetcher retrieves:
- Medicines metadata from the medicines JSON endpoint
- EPAR documents (summaries for the public) as PDFs
- Extracts full text from PDF summaries

The data includes:
- Medicine name, active substance, therapeutic area
- Authorization dates and status
- Therapeutic indications (already in JSON)
- Full EPAR summaries (extracted from PDFs)

Coverage: ~2,600+ medicines authorized via EU centralized procedure
No authentication required. Public data updated twice daily.
"""

import argparse
import html
import io
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional, Dict, List

import requests

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    try:
        from pypdf import PdfReader
        HAS_PYPDF = True
    except ImportError:
        try:
            from PyPDF2 import PdfReader
            HAS_PYPDF = True
        except ImportError:
            HAS_PYPDF = False

BASE_URL = "https://www.ema.europa.eu"
MEDICINES_ENDPOINT = "/en/documents/report/medicines-output-medicines_json-report_en.json"
EPAR_DOCS_ENDPOINT = "/en/documents/report/documents-output-epar_documents_json-report_en.json"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "EU/EMA"


def get_session() -> requests.Session:
    """Create a requests session with proper headers."""
    session = requests.Session()
    session.headers.update({
        'Accept': 'application/json',
        'User-Agent': 'LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)',
    })
    return session


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from a PDF document."""
    text_parts = []

    if HAS_PDFPLUMBER:
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
        except Exception as e:
            print(f"    -> pdfplumber extraction error: {e}")
            return ''
    elif HAS_PYPDF:
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
        except Exception as e:
            print(f"    -> pypdf extraction error: {e}")
            return ''
    else:
        print("    -> No PDF library available (install pdfplumber or pypdf)")
        return ''

    full_text = '\n\n'.join(text_parts)

    # Clean up common artifacts
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)
    # Remove page numbers like "1 of 10"
    full_text = re.sub(r'\d+\s+of\s+\d+\s*\n', '\n', full_text)

    return full_text.strip()


def html_to_text(html_content: str) -> str:
    """Convert HTML to plain text, preserving paragraph structure."""
    if not html_content:
        return ""

    # Replace common block elements with newlines
    text = re.sub(r'</(p|div|h[1-6]|li|tr)>', '\n', html_content, flags=re.IGNORECASE)
    text = re.sub(r'<(br|hr)\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'&nbsp;', ' ', text)

    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities
    text = html.unescape(text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)
    text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)

    return text.strip()


def fetch_medicines_json() -> Dict:
    """Fetch the medicines JSON data."""
    session = get_session()
    url = BASE_URL + MEDICINES_ENDPOINT
    print(f"Fetching medicines data from {url}...")

    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    print(f"  -> Found {data['meta']['total_records']} medicines")
    return data


def fetch_epar_documents_json() -> Dict:
    """Fetch the EPAR documents JSON data."""
    session = get_session()
    url = BASE_URL + EPAR_DOCS_ENDPOINT
    print(f"Fetching EPAR documents data from {url}...")

    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    print(f"  -> Found {data['meta']['total_records']} EPAR documents")
    return data


def build_medicine_doc_index(epar_docs: List[Dict]) -> Dict[str, List[Dict]]:
    """
    Build an index of EPAR documents by medicine name.

    Returns:
        Dict mapping medicine name (lowercase) to list of documents
    """
    index = {}

    for doc in epar_docs:
        name = doc.get('name', '')
        # Extract medicine name from document name (e.g., "Voranigo : EPAR - Summary for the public")
        if ':' in name:
            medicine_name = name.split(':')[0].strip().lower()
        else:
            # Try extracting from the name pattern
            medicine_name = name.split(' ')[0].strip().lower()

        if medicine_name:
            if medicine_name not in index:
                index[medicine_name] = []
            index[medicine_name].append(doc)

    return index


def download_pdf(url: str) -> Optional[bytes]:
    """Download a PDF from the given URL."""
    session = requests.Session()
    session.headers.update({
        'Accept': 'application/pdf',
        'User-Agent': 'LegalDataHunter/1.0',
    })

    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        print(f"    -> PDF download error: {e}")
        return None


def get_overview_doc_for_medicine(medicine_name: str, doc_index: Dict[str, List[Dict]]) -> Optional[Dict]:
    """Find the overview/summary document for a medicine."""
    medicine_key = medicine_name.lower().strip()

    docs = doc_index.get(medicine_key, [])
    if not docs:
        return None

    # Prefer "overview" type (summary for public)
    for doc in docs:
        if doc.get('type') == 'overview':
            return doc

    # Fall back to scientific-discussion
    for doc in docs:
        if doc.get('type') == 'scientific-discussion':
            return doc

    # Return first available
    return docs[0] if docs else None


def process_medicine(medicine: Dict, doc_index: Dict[str, List[Dict]]) -> Optional[Dict]:
    """
    Process a medicine record and extract full text.

    First uses the therapeutic_indication field (already text),
    then optionally enhances with EPAR summary PDF content.
    """
    medicine_name = medicine.get('name_of_medicine', '')
    product_number = medicine.get('ema_product_number', '')

    # Start with therapeutic indication as base text
    indication = medicine.get('therapeutic_indication', '')
    indication_text = html_to_text(indication) if indication else ''

    # Try to get EPAR summary PDF
    overview_doc = get_overview_doc_for_medicine(medicine_name, doc_index)
    epar_text = ''
    pdf_url = ''

    if overview_doc:
        pdf_url = overview_doc.get('document_url', '')
        if pdf_url:
            print(f"    -> Downloading EPAR summary: {overview_doc.get('name', '')[:50]}...")
            pdf_bytes = download_pdf(pdf_url)
            if pdf_bytes:
                epar_text = extract_pdf_text(pdf_bytes)
                if epar_text:
                    print(f"    -> Extracted {len(epar_text):,} chars from PDF")

    # Combine texts
    text_parts = []

    if epar_text and len(epar_text) >= 500:
        # EPAR summary is comprehensive, use it as main text
        text_parts.append(epar_text)
        text_source = 'epar_pdf'
    elif indication_text and len(indication_text) >= 100:
        # Use therapeutic indication as fallback
        text_parts.append(f"Therapeutic Indication:\n{indication_text}")
        text_source = 'indication'
    else:
        return None

    # Add additional context
    if medicine.get('pharmacotherapeutic_group_human'):
        text_parts.append(f"\nPharmacotherapeutic Group: {medicine['pharmacotherapeutic_group_human']}")

    if medicine.get('therapeutic_area_mesh'):
        text_parts.append(f"\nTherapeutic Area: {medicine['therapeutic_area_mesh']}")

    medicine['text'] = '\n'.join(text_parts)
    medicine['text_source'] = text_source
    medicine['epar_pdf_url'] = pdf_url

    return medicine


def normalize(raw: Dict) -> Dict:
    """Transform raw medicine data into standard schema."""
    product_number = raw.get('ema_product_number', '')
    medicine_name = raw.get('name_of_medicine', '')

    # Build document ID
    doc_id = product_number.replace('/', '-') if product_number else medicine_name.replace(' ', '-')

    # Get dates
    auth_date = raw.get('marketing_authorisation_date', '')
    ec_date = raw.get('european_commission_decision_date', '')
    date = auth_date or ec_date or ''

    # Parse date to ISO format
    if date and len(date) == 10 and '/' in date:
        try:
            # Format: DD/MM/YYYY
            parts = date.split('/')
            date = f"{parts[2]}-{parts[1]}-{parts[0]}"
        except:
            pass

    # Build title
    title_parts = [medicine_name]
    if raw.get('active_substance'):
        title_parts.append(f"({raw['active_substance']})")
    if raw.get('medicine_status'):
        title_parts.append(f"- {raw['medicine_status']}")
    title = ' '.join(title_parts)

    # Build URL
    medicine_url = raw.get('medicine_url', '')
    if not medicine_url and medicine_name:
        slug = medicine_name.lower().replace(' ', '-')
        medicine_url = f"https://www.ema.europa.eu/en/medicines/human/EPAR/{slug}"

    # Determine document subtype
    doc_subtype = 'medicine_authorization'
    if raw.get('orphan_medicine') == 'Yes':
        doc_subtype = 'orphan_designation'

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.utcnow().isoformat() + 'Z',
        'title': title,
        'text': raw.get('text', ''),
        'date': date,
        'url': medicine_url,
        'document_subtype': doc_subtype,
        'text_source': raw.get('text_source', ''),
        'epar_pdf_url': raw.get('epar_pdf_url', ''),

        # Medicine-specific fields
        'ema_product_number': product_number,
        'medicine_name': medicine_name,
        'active_substance': raw.get('active_substance', ''),
        'international_name': raw.get('international_non_proprietary_name_common_name', ''),
        'therapeutic_area': raw.get('therapeutic_area_mesh', ''),
        'atc_code': raw.get('atc_code_human', ''),
        'pharmacotherapeutic_group': raw.get('pharmacotherapeutic_group_human', ''),
        'medicine_status': raw.get('medicine_status', ''),
        'marketing_authorisation_holder': raw.get('marketing_authorisation_developer_applicant_holder', ''),
        'category': raw.get('category', ''),

        # Authorization flags
        'is_orphan': raw.get('orphan_medicine') == 'Yes',
        'is_biosimilar': raw.get('biosimilar') == 'Yes',
        'is_generic': raw.get('generic') == 'Yes',
        'conditional_approval': raw.get('conditional_approval') == 'Yes',
        'exceptional_circumstances': raw.get('exceptional_circumstances') == 'Yes',
        'accelerated_assessment': raw.get('accelerated_assessment') == 'Yes',
        'additional_monitoring': raw.get('additional_monitoring') == 'Yes',
        'advanced_therapy': raw.get('advanced_therapy') == 'Yes',
        'prime_medicine': raw.get('prime_priority_medicine') == 'Yes',

        'language': 'en',
    }


def fetch_all(max_records: int = None) -> Generator[Dict, None, None]:
    """
    Fetch all authorized medicines with EPAR summaries.

    Args:
        max_records: Maximum number of records to yield (None = all)

    Yields:
        Normalized medicine records with full text
    """
    # Load data
    medicines_data = fetch_medicines_json()
    epar_docs_data = fetch_epar_documents_json()

    medicines = medicines_data.get('data', [])
    epar_docs = epar_docs_data.get('data', [])

    # Build document index
    print("Building document index...")
    doc_index = build_medicine_doc_index(epar_docs)
    print(f"  -> Indexed {len(doc_index)} medicine names")

    # Filter to authorized human medicines only
    authorized = [m for m in medicines if
                  m.get('category') == 'Human' and
                  m.get('medicine_status') == 'Authorised']
    print(f"Processing {len(authorized)} authorized human medicines...")

    count = 0

    for medicine in authorized:
        if max_records and count >= max_records:
            break

        medicine_name = medicine.get('name_of_medicine', '')
        print(f"  [{count+1}] Processing {medicine_name}...")

        # Process medicine to get full text
        medicine_with_text = process_medicine(medicine, doc_index)

        if medicine_with_text and medicine_with_text.get('text'):
            try:
                normalized = normalize(medicine_with_text)
                text_len = len(normalized.get('text', ''))

                if text_len >= 200:
                    source = normalized.get('text_source', 'unknown')
                    print(f"      -> {text_len:,} chars ({source})")
                    yield normalized
                    count += 1
                else:
                    print(f"      -> Skipping: text too short ({text_len} chars)")
            except Exception as e:
                print(f"      -> Error normalizing: {e}")
        else:
            print(f"      -> Skipping: no text extracted")

        # Rate limiting
        time.sleep(2.0)

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch documents updated since a given date."""
    since_str = since.strftime('%Y-%m-%d')

    for record in fetch_all():
        last_updated = record.get('date', '')
        if last_updated and last_updated >= since_str:
            yield record


def bootstrap_sample(sample_count: int = 12):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []

    for record in fetch_all(max_records=sample_count + 5):
        if len(records) >= sample_count:
            break

        try:
            text_len = len(record.get('text', ''))
            if text_len < 200:
                print(f"  Skipping {record.get('medicine_name')}: Text too short ({text_len} chars)")
                continue

            records.append(record)

            # Save individual record
            doc_id = record['_id'].replace('/', '_').replace(':', '-')
            filename = SAMPLE_DIR / f"{doc_id}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            source = record.get('text_source', 'unknown')
            title_preview = record.get('title', '')[:50]
            print(f"  [{len(records):02d}] {record['medicine_name']}: {text_len:,} chars ({source})")

        except Exception as e:
            print(f"  Error saving record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        # Statistics
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        # Count by text source
        source_counts = {}
        for r in records:
            source = r.get('text_source', 'unknown')
            source_counts[source] = source_counts.get(source, 0) + 1

        print("Text sources:")
        for source, count in sorted(source_counts.items()):
            print(f"  {source}: {count}")

        # Count orphan medicines
        orphan_count = sum(1 for r in records if r.get('is_orphan'))
        print(f"Orphan medicines: {orphan_count}/{len(records)}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    insufficient_text = sum(1 for r in records if not r.get('text') or len(r['text']) < 200)
    if insufficient_text > 0:
        print(f"WARNING: {insufficient_text} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="EU/EMA medicines data fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'test', 'count'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only (12 records)")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")

    args = parser.parse_args()

    if args.command == 'count':
        # Count total available records
        data = fetch_medicines_json()
        medicines = data.get('data', [])
        authorized = [m for m in medicines if
                      m.get('category') == 'Human' and
                      m.get('medicine_status') == 'Authorised']
        print(f"Total authorized human medicines: {len(authorized)}")
        print(f"Total medicines (all status): {len(medicines)}")

    elif args.command == 'test':
        # Test a single fetch
        print("Testing API connection...")
        data = fetch_medicines_json()
        medicines = data.get('data', [])
        if medicines:
            med = medicines[0]
            print(f"First medicine:")
            print(f"  Name: {med.get('name_of_medicine')}")
            print(f"  Product Number: {med.get('ema_product_number')}")
            print(f"  Status: {med.get('medicine_status')}")
            print(f"  Active Substance: {med.get('active_substance')}")
            print(f"  Has indication: {bool(med.get('therapeutic_indication'))}")

    elif args.command == 'bootstrap':
        count = 12 if args.sample else args.count
        success = bootstrap_sample(count)
        sys.exit(0 if success else 1)

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
