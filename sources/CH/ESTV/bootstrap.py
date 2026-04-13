#!/usr/bin/env python3
"""
CH/ESTV - Swiss Federal Tax Administration (ESTV)

Fetches tax circulars and administrative directives from ESTV.
Documents are published as PDFs on the admin.ch website.

Data sources:
- Kreisschreiben Direkte Bundessteuer: https://www.estv.admin.ch/de/kreisschreiben-direkten-bundessteuer
- Rundschreiben Direkte Bundessteuer: https://www.estv.admin.ch/de/rundschreiben-direkten-bundessteuer
- Kreisschreiben Verrechnungssteuer: https://www.estv.admin.ch/de/kreisschreiben-verrechnungssteuer
- Rundschreiben Verrechnungssteuer: https://www.estv.admin.ch/de/rundschreiben-verrechnungssteuer

Document types:
- Kreisschreiben: Interpretive guidance / tax circulars
- Rundschreiben: Administrative notices / directives

Languages: German (de), French (fr), Italian (it)
License: Public domain (Swiss federal government)

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all circulars
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
SOURCE_ID = "CH/ESTV"
BASE_URL = "https://www.estv.admin.ch"

# Publication pages to scrape
PUBLICATION_PAGES = [
    {
        'url': f"{BASE_URL}/de/kreisschreiben-direkten-bundessteuer",
        'type': 'kreisschreiben',
        'tax_area': 'direkte_bundessteuer',
        'name': 'Kreisschreiben Direkte Bundessteuer',
    },
    {
        'url': f"{BASE_URL}/de/rundschreiben-direkten-bundessteuer",
        'type': 'rundschreiben',
        'tax_area': 'direkte_bundessteuer',
        'name': 'Rundschreiben Direkte Bundessteuer',
    },
    {
        'url': f"{BASE_URL}/de/kreisschreiben-verrechnungssteuer",
        'type': 'kreisschreiben',
        'tax_area': 'verrechnungssteuer',
        'name': 'Kreisschreiben Verrechnungssteuer',
    },
    {
        'url': f"{BASE_URL}/de/rundschreiben-verrechnungssteuer",
        'type': 'rundschreiben',
        'tax_area': 'verrechnungssteuer',
        'name': 'Rundschreiben Verrechnungssteuer',
    },
]

RATE_LIMIT_DELAY = 2.0
MAX_PDF_SIZE_MB = 25.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"

# Month names for date parsing
MONTHS_DE = {
    "januar": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8, "september": 9,
    "oktober": 10, "november": 11, "dezember": 12,
}
MONTHS_FR = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}
MONTHS_IT = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
    "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8, "settembre": 9,
    "ottobre": 10, "novembre": 11, "dicembre": 12,
}


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "de-CH,de;q=0.9,en;q=0.8",
    })
    return session


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="CH/ESTV",
        source_id="",
        pdf_bytes=pdf_content,
        table="doctrine",
    ) or ""

def parse_nuxt_data(html: str) -> list[dict]:
    """Parse NUXT data from admin.ch pages to extract document list."""
    match = re.search(
        r'<script type="application/json" data-nuxt-data="nuxt-app"[^>]*>([^<]+)</script>',
        html
    )
    if not match:
        return []

    try:
        arr = json.loads(match.group(1))
    except json.JSONDecodeError as e:
        print(f"Error parsing NUXT data: {e}", file=sys.stderr)
        return []

    documents = []
    seen_urls = set()

    for i, item in enumerate(arr):
        if isinstance(item, dict) and 'asset' in item and 'text' in item:
            asset_idx = item.get('asset')
            if not isinstance(asset_idx, int) or asset_idx >= len(arr):
                continue

            asset = arr[asset_idx]
            if not isinstance(asset, dict) or 'url' not in asset:
                continue

            url_idx = asset.get('url')
            filename_idx = asset.get('filename')
            text_idx = item.get('text')

            url = arr[url_idx] if isinstance(url_idx, int) and url_idx < len(arr) else ''
            filename = arr[filename_idx] if isinstance(filename_idx, int) and filename_idx < len(arr) else ''
            title = arr[text_idx] if isinstance(text_idx, int) and text_idx < len(arr) else ''

            if not filename.endswith('.pdf'):
                continue

            # Skip appendix files
            if '-anhang' in filename.lower() or '_anhang' in filename.lower():
                continue

            # Skip duplicates
            if url in seen_urls:
                continue
            seen_urls.add(url)

            # Extract document ID from filename
            doc_id = filename.replace('.pdf', '')

            documents.append({
                'url': url,
                'filename': filename,
                'title': title,
                'doc_id': doc_id,
            })

    return documents


def fetch_documents_from_page(session: requests.Session, page_info: dict) -> list[dict]:
    """Fetch document list from a single publication page."""
    url = page_info['url']
    print(f"Fetching: {page_info['name']}...", file=sys.stderr)

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        html = response.text
    except requests.RequestException as e:
        print(f"Error fetching page: {e}", file=sys.stderr)
        return []

    documents = parse_nuxt_data(html)

    # Add metadata from page_info
    for doc in documents:
        doc['circular_type'] = page_info['type']
        doc['tax_area'] = page_info['tax_area']
        doc['page_name'] = page_info['name']

    print(f"  Found {len(documents)} documents", file=sys.stderr)
    return documents


def fetch_all_documents(session: requests.Session) -> list[dict]:
    """Fetch document list from all publication pages."""
    all_docs = []

    for page_info in PUBLICATION_PAGES:
        docs = fetch_documents_from_page(session, page_info)
        all_docs.extend(docs)
        time.sleep(1.0)  # Small delay between pages

    print(f"Total: {len(all_docs)} documents across all pages", file=sys.stderr)
    return all_docs


def download_pdf(session: requests.Session, url: str) -> Optional[bytes]:
    """Download PDF and return content."""
    try:
        # First check PDF size with HEAD request
        head_resp = session.head(url, timeout=20, allow_redirects=True)
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > MAX_PDF_SIZE_MB:
                print(f"Skipping large PDF ({size_mb:.1f} MB)", file=sys.stderr)
                return None

        response = session.get(url, timeout=180)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        print(f"Error downloading PDF: {e}", file=sys.stderr)
        return None


def parse_date_from_text(text: str) -> Optional[str]:
    """Extract date from text content."""
    # Priority order: most specific patterns first
    patterns = [
        # Swiss format with city: "Bern, 05. Dezember 2023"
        (r'Bern,\s+(\d{1,2})\.?\s+([A-Za-zäöüéèû]+)\s+(\d{4})', 'month_name'),
        # German: "vom 25. März 2025" or "vom 25 März 2025"
        (r'vom\s+(\d{1,2})\.?\s+([A-Za-zäöüéèû]+)\s+(\d{4})', 'month_name'),
        # French: "du 25 mars 2025"
        (r'du\s+(\d{1,2})\s+([A-Za-zéèûô]+)\s+(\d{4})', 'month_name'),
        # Italian: "del 25 marzo 2025"
        (r'del\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', 'month_name'),
        # Generic German date: "05. Dezember 2023"
        (r'(\d{1,2})\.?\s+([A-Za-zäöüéè]+)\s+(\d{4})', 'month_name'),
        # Generic: DD.MM.YYYY or DD/MM/YYYY
        (r'(\d{1,2})[./](\d{1,2})[./](\d{4})', 'numeric'),
        # Filename pattern: ks-2020-1-050 (fallback)
        (r'ks-(\d{4})-\d+-\d+', 'year_only'),
        (r'rs-(\d{4})-\d+', 'year_only'),
    ]

    for pattern, ptype in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            groups = match.groups()

            if ptype == 'month_name' and len(groups) == 3:
                day = int(groups[0])
                month_name = groups[1].lower()
                year = int(groups[2])

                month = MONTHS_DE.get(month_name) or MONTHS_FR.get(month_name) or MONTHS_IT.get(month_name)
                if month and 1 <= day <= 31:
                    return f"{year}-{month:02d}-{day:02d}"

            elif ptype == 'numeric' and len(groups) == 3:
                day = int(groups[0])
                month = int(groups[1])
                year = int(groups[2])
                if 1 <= month <= 12 and 1 <= day <= 31:
                    return f"{year}-{month:02d}-{day:02d}"

            elif ptype == 'year_only' and len(groups) == 1:
                year = groups[0]
                return f"{year}-01-01"

    return None


def extract_circular_number(title: str, filename: str) -> Optional[str]:
    """Extract circular number from title or filename."""
    # Pattern: "Nr. 50a" or "Nr 50" or "Kreisschreiben Nr. 45"
    patterns = [
        r'Nr\.?\s*(\d+[a-z]?)',
        r'n[°o]\.?\s*(\d+[a-z]?)',
        r'numero\s*(\d+[a-z]?)',
    ]

    for pattern in patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            return match.group(1)

    # Try filename: dbst-ks-2020-1-050a-d-de.pdf
    match = re.search(r'ks-\d+-\d+-(\d+[a-z]?)', filename)
    if match:
        return match.group(1)

    return None


def detect_language(filename: str, title: str, text: str) -> str:
    """Detect document language."""
    # First check filename suffix (-de, -fr, -it)
    if '-de.pdf' in filename.lower():
        return 'de'
    if '-fr.pdf' in filename.lower():
        return 'fr'
    if '-it.pdf' in filename.lower():
        return 'it'

    combined = (title + " " + text[:500]).lower()

    # French indicators
    fr_words = ['du', 'décision', 'impôt fédéral', 'concernant', 'cette']
    fr_count = sum(1 for w in fr_words if w in combined)

    # German indicators
    de_words = ['vom', 'verfügung', 'bundessteuer', 'betreffend', 'diese']
    de_count = sum(1 for w in de_words if w in combined)

    # Italian indicators
    it_words = ['del', 'decisione', 'imposta federale', 'concernente', 'questa']
    it_count = sum(1 for w in it_words if w in combined)

    counts = {'de': de_count, 'fr': fr_count, 'it': it_count}
    return max(counts, key=counts.get)


def normalize(raw: dict, text: str) -> dict:
    """Transform raw document data into normalized schema."""
    title = raw.get('title', '')
    doc_id = raw.get('doc_id', '')
    url = raw.get('url', '')
    filename = raw.get('filename', '')
    circular_type = raw.get('circular_type', '')
    tax_area = raw.get('tax_area', '')

    # Parse date from title first, then from text body, then from filename
    date_str = parse_date_from_text(title)
    if not date_str:
        date_str = parse_date_from_text(filename)
    if not date_str and text:
        date_str = parse_date_from_text(text[:1500])

    # Extract circular number
    circular_number = extract_circular_number(title, filename)

    # Detect language
    language = detect_language(filename, title, text)

    # Clean up title
    clean_title = title.strip()

    # Create unique ID
    unique_id = f"{SOURCE_ID}/{doc_id}"

    return {
        '_id': unique_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': clean_title,
        'text': text,
        'date': date_str,
        'url': url,
        'language': language,
        'circular_type': circular_type,
        'circular_number': circular_number,
        'tax_area': tax_area,
        'filename': filename,
        'authority': 'Eidgenössische Steuerverwaltung (ESTV)',
        'country': 'CH',
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all ESTV circulars."""
    print("Fetching all ESTV circulars...", file=sys.stderr)

    documents = fetch_all_documents(session)

    for i, doc in enumerate(documents):
        print(f"[{i+1}/{len(documents)}] {doc['title'][:60]}...", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)

        pdf_content = download_pdf(session, doc['url'])
        if not pdf_content:
            continue

        text = extract_text_from_pdf(pdf_content)
        if not text or len(text) < 200:
            print(f"  Warning: Could not extract text", file=sys.stderr)
            continue

        record = normalize(doc, text)
        print(f"  Extracted {len(text)} chars", file=sys.stderr)
        yield record


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of documents."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching sample of {count} documents...", file=sys.stderr)

    documents = fetch_all_documents(session)

    # Sample from different categories
    kreisschreiben = [d for d in documents if d['circular_type'] == 'kreisschreiben']
    rundschreiben = [d for d in documents if d['circular_type'] == 'rundschreiben']

    # Take some from each
    sample_docs = kreisschreiben[:count//2] + rundschreiben[:count//2]
    if len(sample_docs) < count:
        sample_docs = documents[:count * 2]

    for doc in sample_docs:
        if len(records) >= count:
            break

        print(f"Processing: {doc['title'][:60]}...", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)

        pdf_content = download_pdf(session, doc['url'])
        if not pdf_content:
            continue

        text = extract_text_from_pdf(pdf_content)
        if not text or len(text) < 200:
            print(f"  Could not extract text, skipping", file=sys.stderr)
            continue

        record = normalize(doc, text)
        records.append(record)

        # Save incrementally
        filepath = save_dir / f"record_{len(records)-1:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  Saved {len(text)} chars to {filepath.name}", file=sys.stderr)

    return records


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="CH/ESTV Swiss Federal Tax Administration Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if not PDF_AVAILABLE:
        print("ERROR: pdfplumber or pypdf library required for PDF text extraction", file=sys.stderr)
        sys.exit(1)

    session = get_session()

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample records...", file=sys.stderr)
            records = fetch_sample(session, args.count)

            if records:
                save_samples(records)

                # Print summary
                text_lengths = [len(r.get('text', '')) for r in records]
                types = {}
                areas = {}
                for r in records:
                    t = r.get('circular_type', 'unknown')
                    types[t] = types.get(t, 0) + 1
                    area = r.get('tax_area', 'unknown')
                    areas[area] = areas.get(area, 0) + 1

                print(f"\n=== Sample Statistics ===", file=sys.stderr)
                print(f"  Records: {len(records)}", file=sys.stderr)
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars", file=sys.stderr)
                print(f"  Min text length: {min(text_lengths)} chars", file=sys.stderr)
                print(f"  Max text length: {max(text_lengths)} chars", file=sys.stderr)
                print(f"\nBy type:", file=sys.stderr)
                for ctype, cnt in sorted(types.items(), key=lambda x: -x[1]):
                    print(f"  {ctype}: {cnt}", file=sys.stderr)
                print(f"\nBy tax area:", file=sys.stderr)
                for area, cnt in sorted(areas.items(), key=lambda x: -x[1]):
                    print(f"  {area}: {cnt}", file=sys.stderr)
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch...", file=sys.stderr)
            count = 0
            for record in fetch_all(session):
                count += 1
                print(f"Fetched: {record.get('title', 'unknown')[:50]}", file=sys.stderr)
            print(f"Total: {count} circulars", file=sys.stderr)


if __name__ == "__main__":
    main()
