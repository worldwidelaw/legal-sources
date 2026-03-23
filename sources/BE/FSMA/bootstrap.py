#!/usr/bin/env python3
"""
BE/FSMA - Belgian Financial Services and Markets Authority

Fetches administrative sanctions and settlement agreements from the FSMA website.
Decisions are published as PDFs in French and Dutch.

Data source:
- French: https://www.fsma.be/fr/reglements-transactionnels
- Dutch: https://www.fsma.be/nl/minnelijke-schikkingen
- English: https://www.fsma.be/en/administrative-sanctions

The FSMA publishes:
- Settlement agreements (règlements transactionnels / minnelijke schikkingen)
- Sanctions Committee decisions (décisions de la Commission des sanctions)
- Historical decisions

License: Belgian Federal Government Open Data

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all decisions
"""

import argparse
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
from bs4 import BeautifulSoup

try:
    import pdfplumber
    PDF_AVAILABLE = True
except ImportError:
    try:
        import pypdf
        PDF_AVAILABLE = True
        USE_PYPDF = True
    except ImportError:
        PDF_AVAILABLE = False
        print("Warning: Neither pdfplumber nor pypdf available, PDF text extraction disabled", file=sys.stderr)
else:
    USE_PYPDF = False

# Constants
SOURCE_ID = "BE/FSMA"
BASE_URL = "https://www.fsma.be"

# Sanctions page URLs
SANCTIONS_URL_FR = f"{BASE_URL}/fr/reglements-transactionnels"
SANCTIONS_URL_NL = f"{BASE_URL}/nl/minnelijke-schikkingen"
SANCTIONS_URL_EN = f"{BASE_URL}/en/administrative-sanctions"

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-BE,fr;q=0.9,nl-BE;q=0.8,nl;q=0.7,en;q=0.6",
    })
    return session


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF content."""
    if not PDF_AVAILABLE:
        return ""

    try:
        if USE_PYPDF:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(pdf_content))
            text_parts = []
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            full_text = "\n\n".join(text_parts)
        else:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                text_parts = []
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                full_text = "\n\n".join(text_parts)

        # Clean up the text
        full_text = re.sub(r'[ \t]+', ' ', full_text)
        full_text = re.sub(r'\n\s*\n\s*\n+', '\n\n', full_text)

        return full_text.strip()
    except Exception as e:
        print(f"Error extracting text from PDF: {e}", file=sys.stderr)
        return ""


def parse_date_from_filename(filename: str) -> Optional[str]:
    """Parse date from PDF filename like 2026-03-09_minnelijkeschikking.pdf"""
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})_', filename)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return None


def parse_date_from_table(date_str: str) -> Optional[str]:
    """Parse date from table format like 09.03.26"""
    match = re.match(r'(\d{2})\.(\d{2})\.(\d{2})', date_str.strip())
    if match:
        day, month, year_short = match.groups()
        # Assume 20XX for years
        year = f"20{year_short}"
        return f"{year}-{month}-{day}"
    return None


def detect_language(text: str) -> str:
    """Detect language from text content."""
    # Dutch indicators
    nl_words = ['van', 'het', 'een', 'niet', 'wordt', 'zijn', 'heeft', 'door', 'minnelijke', 'schikking']
    # French indicators
    fr_words = ['de', 'la', 'le', 'les', 'une', 'dans', 'règlement', 'transactionnel', 'par']

    text_lower = text.lower()
    nl_count = sum(1 for w in nl_words if f' {w} ' in text_lower)
    fr_count = sum(1 for w in fr_words if f' {w} ' in text_lower)

    if nl_count > fr_count:
        return 'nl'
    return 'fr'


def classify_decision_type(title: str, text: str) -> str:
    """Classify the type of decision based on title and content."""
    title_lower = title.lower()
    text_lower = text.lower()[:1000]

    # Settlement agreements
    if 'règlement transactionnel' in title_lower or 'minnelijke schikking' in title_lower:
        return 'settlement'
    if 'transactionnel' in title_lower or 'schikking' in title_lower:
        return 'settlement'

    # Formal sanctions
    if 'sanction' in title_lower or 'sanctie' in title_lower:
        return 'sanction'
    if 'commission des sanctions' in text_lower or 'sanctiecommissie' in text_lower:
        return 'sanction'

    # Decisions
    if 'décision' in title_lower or 'beslissing' in title_lower:
        return 'decision'

    return 'regulatory_decision'


def fetch_pdf_links_from_page(session: requests.Session, url: str) -> list[dict]:
    """Fetch all PDF links from a sanctions page."""
    pdfs = []

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching page {url}: {e}", file=sys.stderr)
        return pdfs

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all table rows with PDF links
    for row in soup.find_all('tr'):
        cells = row.find_all('td')
        if len(cells) >= 2:
            date_cell = cells[0]
            link_cell = cells[1]

            # Get date
            date_text = date_cell.get_text().strip()
            date = parse_date_from_table(date_text)

            # Get PDF link
            link = link_cell.find('a', href=True)
            if link and link['href'].lower().endswith('.pdf'):
                href = link['href']
                if href.startswith('/'):
                    pdf_url = BASE_URL + href
                else:
                    pdf_url = href

                title = link.get_text().strip()

                # Check for language notes
                is_dutch_only = 'néerlandais' in link_cell.get_text().lower() or 'nederlands' in link_cell.get_text().lower()

                filename = href.split('/')[-1]

                pdfs.append({
                    'pdf_url': pdf_url,
                    'title': title,
                    'filename': filename,
                    'date': date or parse_date_from_filename(filename),
                    'is_dutch_only': is_dutch_only,
                })

    return pdfs


def fetch_all_pdf_links(session: requests.Session) -> list[dict]:
    """Fetch all PDF links from all language versions of the sanctions page."""
    all_pdfs = []
    seen_urls = set()

    # Fetch from French page (main source, most complete)
    print("Fetching sanctions page...", file=sys.stderr)
    pdfs = fetch_pdf_links_from_page(session, SANCTIONS_URL_FR)

    for pdf in pdfs:
        if pdf['pdf_url'] not in seen_urls:
            seen_urls.add(pdf['pdf_url'])
            all_pdfs.append(pdf)

    print(f"Found {len(all_pdfs)} unique PDF documents", file=sys.stderr)
    return all_pdfs


def fetch_decision_with_text(session: requests.Session, decision: dict, max_pdf_size_mb: float = 10.0) -> Optional[dict]:
    """Download PDF and extract text for a decision."""
    pdf_url = decision.get('pdf_url')
    if not pdf_url:
        return None

    try:
        # First check PDF size with HEAD request
        head_resp = session.head(pdf_url, timeout=20, allow_redirects=True)
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > max_pdf_size_mb:
                print(f"Skipping large PDF ({size_mb:.1f} MB > {max_pdf_size_mb} MB): {pdf_url}", file=sys.stderr)
                return None

        response = session.get(pdf_url, timeout=180)
        response.raise_for_status()
        pdf_content = response.content
    except requests.RequestException as e:
        print(f"Error downloading PDF {pdf_url}: {e}", file=sys.stderr)
        return None

    # Extract text
    text = extract_text_from_pdf(pdf_content)
    if not text or len(text) < 100:
        print(f"Warning: Could not extract meaningful text from {pdf_url}", file=sys.stderr)
        return None

    decision['text'] = text
    decision['pdf_size'] = len(pdf_content)
    decision['language'] = detect_language(text)
    decision['decision_type'] = classify_decision_type(decision.get('title', ''), text)

    # Try to extract fine/settlement amount
    amount_patterns = [
        r'(?:montant|bedrag|amende|boete|som)\s*(?:de|van)?\s*€?\s*([\d\s.,]+)\s*(?:euros?|EUR)?',
        r'€\s*([\d\s.,]+)',
        r'([\d\s.,]+)\s*(?:euros?|EUR)',
    ]
    for pattern in amount_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            amount_str = match.group(1).replace(' ', '').replace('.', '').replace(',', '.')
            try:
                amount = float(amount_str)
                if 100 < amount < 100_000_000:  # Reasonable range for fines
                    decision['amount'] = amount
                    break
            except ValueError:
                pass

    # Extract company/entity name from title
    title = decision.get('title', '')
    if 'accord de' in title.lower():
        match = re.search(r"accord de\s+(.+?)(?:\s*\(|$)", title, re.IGNORECASE)
        if match:
            decision['entity_name'] = match.group(1).strip()
    elif "l'accord de" in title.lower():
        match = re.search(r"l'accord de\s+(.+?)(?:\s*\(|$)", title, re.IGNORECASE)
        if match:
            decision['entity_name'] = match.group(1).strip()

    return decision


def normalize(raw: dict) -> dict:
    """Transform raw FSMA decision data into normalized schema."""
    # Create unique ID from filename
    filename = raw.get('filename', 'unknown')
    slug = filename.replace('.pdf', '')
    doc_id = f"{SOURCE_ID}/{slug}"

    # Get date
    date = raw.get('date')
    if not date:
        # Try to extract from filename
        date = parse_date_from_filename(filename)

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': date,
        'url': raw.get('pdf_url', ''),
        'language': raw.get('language'),
        'decision_type': raw.get('decision_type'),
        'entity_name': raw.get('entity_name'),
        'amount': raw.get('amount'),
        'pdf_size': raw.get('pdf_size'),
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all FSMA decisions."""
    print("Fetching all FSMA decisions...", file=sys.stderr)
    pdf_links = fetch_all_pdf_links(session)

    for i, pdf_info in enumerate(pdf_links):
        print(f"Processing {i+1}/{len(pdf_links)}: {pdf_info['filename']}", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, pdf_info)
        if full_decision and full_decision.get('text'):
            record = normalize(full_decision)
            yield record


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of decisions. Saves incrementally to avoid data loss."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching sample of {count} decisions...", file=sys.stderr)
    pdf_links = fetch_all_pdf_links(session)

    for pdf_info in pdf_links[:count + 5]:
        if len(records) >= count:
            break

        print(f"Fetching {pdf_info['filename']}...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, pdf_info)
        if full_decision and full_decision.get('text'):
            record = normalize(full_decision)
            records.append(record)

            # Save incrementally
            filepath = save_dir / f"record_{len(records)-1:04d}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  Extracted {len(full_decision['text'])} chars, saved to {filepath.name}", file=sys.stderr)

    return records


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="BE/FSMA Financial Services Authority Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
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
                amounts = [r.get('amount') for r in records if r.get('amount')]
                types = {}
                for r in records:
                    dt = r.get('decision_type', 'unknown')
                    types[dt] = types.get(dt, 0) + 1

                print(f"\n=== Sample Statistics ===", file=sys.stderr)
                print(f"  Records: {len(records)}", file=sys.stderr)
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars", file=sys.stderr)
                print(f"  Min text length: {min(text_lengths)} chars", file=sys.stderr)
                print(f"  Max text length: {max(text_lengths)} chars", file=sys.stderr)
                if amounts:
                    print(f"  Settlement amounts found: {len(amounts)} (total €{sum(amounts):,.0f})", file=sys.stderr)
                print(f"\nBy decision type:", file=sys.stderr)
                for dtype, cnt in sorted(types.items(), key=lambda x: -x[1]):
                    print(f"  {dtype}: {cnt}", file=sys.stderr)
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch...", file=sys.stderr)
            count = 0
            for record in fetch_all(session):
                count += 1
                print(f"Fetched: {record.get('title', 'unknown')[:60]}", file=sys.stderr)
            print(f"Total: {count} decisions", file=sys.stderr)


if __name__ == "__main__":
    main()
