#!/usr/bin/env python3
"""
BE/BCA - Belgian Competition Authority (Autorité belge de la concurrence / Belgische Mededingingsautoriteit)

Fetches competition/antitrust decisions from the Belgian Competition Authority.
Decisions are published as PDFs on their website.

Data source:
- https://www.belgiancompetition.be/en/decisions

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
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
SOURCE_ID = "BE/BCA"
BASE_URL = "https://www.belgiancompetition.be"
DECISIONS_URL = f"{BASE_URL}/en/decisions"

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-BE,en;q=0.9,fr-BE;q=0.8,fr;q=0.7,nl-BE;q=0.6,nl;q=0.5",
    })
    return session


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="BE/BCA",
        source_id="",
        pdf_bytes=pdf_content,
        table="doctrine",
    ) or ""

def parse_case_number(case_str: str) -> dict:
    """Parse BCA case number and type from string."""
    metadata = {
        'case_number': case_str,
        'case_type': None,
        'year': None,
    }

    # Pattern: YY-TYPE-NN (e.g., 26-CC-01, 25-RPR-40)
    match = re.match(r'(\d{2})-([A-Z]+)-(\d+)(?:-([A-Z]+))?', case_str, re.IGNORECASE)
    if match:
        year_short = match.group(1)
        case_type = match.group(2).upper()
        seq_num = match.group(3)
        suffix = match.group(4)

        # Convert 2-digit year
        year_num = int(year_short)
        year = f"20{year_short}" if year_num < 50 else f"19{year_short}"
        metadata['year'] = year

        # Map case types
        type_map = {
            'CC': 'concentration',
            'CCS': 'concentration_simplified',
            'RPR': 'restrictive_practices',
            'C': 'concentration',
            'ABC': 'antitrust',
            'BMA': 'antitrust',
        }
        metadata['case_type'] = type_map.get(case_type, case_type.lower())

        if suffix:
            metadata['case_number'] = f"{case_str}"
            metadata['decision_type'] = suffix.lower()  # e.g., 'AUD' for audit

    return metadata


def get_total_pages(session: requests.Session) -> int:
    """Get total number of pages from decisions list."""
    try:
        response = session.get(DECISIONS_URL, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for pagination links
        pagination = soup.find_all('a', href=re.compile(r'\?page=\d+'))
        if pagination:
            max_page = 0
            for link in pagination:
                href = link.get('href', '')
                match = re.search(r'page=(\d+)', href)
                if match:
                    page_num = int(match.group(1))
                    max_page = max(max_page, page_num)
            return max_page + 1  # pages are 0-indexed
    except Exception as e:
        print(f"Error getting total pages: {e}", file=sys.stderr)

    return 155  # Current known count


def fetch_decisions_from_page(session: requests.Session, page: int) -> list[dict]:
    """Fetch all decision entries from a single page."""
    decisions = []
    url = f"{DECISIONS_URL}?page={page}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching page {page}: {e}", file=sys.stderr)
        return decisions

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all decision entries
    # The structure appears to be views-row elements with case info and PDF links
    rows = soup.find_all('div', class_=re.compile(r'views-row'))

    if not rows:
        # Try alternative structures
        rows = soup.find_all('article')

    if not rows:
        # Try table structure
        rows = soup.find_all('tr')

    for row in rows:
        decision = {}

        # Find links - look for PDF links and decision page links
        links = row.find_all('a', href=True)

        for link in links:
            href = link['href']
            text = link.get_text().strip()

            # Decision detail page link
            if '/decisions/' in href and not href.endswith('.pdf'):
                if href.startswith('/'):
                    decision['detail_url'] = urljoin(BASE_URL, href)
                else:
                    decision['detail_url'] = href
                if text:
                    decision['title'] = text

            # PDF link
            if href.lower().endswith('.pdf'):
                if href.startswith('/'):
                    decision['pdf_url'] = urljoin(BASE_URL, href)
                else:
                    decision['pdf_url'] = href

        # Extract case number from title or any text
        row_text = row.get_text()
        case_match = re.search(r'\b(\d{2}-[A-Z]+-\d+(?:-[A-Z]+)?)\b', row_text, re.IGNORECASE)
        if case_match:
            decision['case_number'] = case_match.group(1)
            decision.update(parse_case_number(case_match.group(1)))

        # Extract date
        date_match = re.search(r'(\d{1,2}[./]\d{1,2}[./]\d{4}|\d{4}-\d{2}-\d{2})', row_text)
        if date_match:
            date_str = date_match.group(1)
            # Normalize date format
            if '/' in date_str or '.' in date_str:
                parts = re.split(r'[./]', date_str)
                if len(parts) == 3:
                    if len(parts[2]) == 4:  # DD/MM/YYYY
                        decision['date'] = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                    else:  # YYYY.MM.DD
                        decision['date'] = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
            else:
                decision['date'] = date_str

        # Only add if we have a PDF URL or detail URL
        if decision.get('pdf_url') or decision.get('detail_url'):
            decisions.append(decision)

    return decisions


def fetch_decision_detail(session: requests.Session, decision: dict) -> dict:
    """Fetch additional details from decision detail page."""
    detail_url = decision.get('detail_url')
    if not detail_url:
        return decision

    try:
        response = session.get(detail_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching detail page: {e}", file=sys.stderr)
        return decision

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract title if not already set
    if not decision.get('title'):
        title_elem = soup.find(['h1', 'h2'], class_=re.compile(r'title|heading'))
        if title_elem:
            decision['title'] = title_elem.get_text().strip()

    # Find PDF links on detail page
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.lower().endswith('.pdf'):
            if href.startswith('/'):
                decision['pdf_url'] = urljoin(BASE_URL, href)
            else:
                decision['pdf_url'] = href
            break

    # Extract parties/companies involved
    content = soup.find('article') or soup.find('main') or soup.find('div', class_='content')
    if content:
        text = content.get_text()

        # Extract date if not found
        if not decision.get('date'):
            date_match = re.search(r'(\d{1,2}[./]\d{1,2}[./]\d{4}|\d{4}-\d{2}-\d{2})', text)
            if date_match:
                date_str = date_match.group(1)
                if '/' in date_str or '.' in date_str:
                    parts = re.split(r'[./]', date_str)
                    if len(parts) == 3:
                        if len(parts[2]) == 4:
                            decision['date'] = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
                        else:
                            decision['date'] = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                else:
                    decision['date'] = date_str

    return decision


def fetch_decision_with_text(session: requests.Session, decision: dict, max_pdf_size_mb: float = 15.0) -> Optional[dict]:
    """Download PDF and extract text for a decision."""
    # First try to get detail page for PDF URL
    if not decision.get('pdf_url') and decision.get('detail_url'):
        decision = fetch_decision_detail(session, decision)

    pdf_url = decision.get('pdf_url')
    if not pdf_url:
        print(f"No PDF URL for decision: {decision.get('case_number', 'unknown')}", file=sys.stderr)
        return None

    try:
        # Check PDF size with HEAD request
        head_resp = session.head(pdf_url, timeout=20, allow_redirects=True)
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > max_pdf_size_mb:
                print(f"Skipping large PDF ({size_mb:.1f} MB): {pdf_url}", file=sys.stderr)
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

    # Try to extract additional info from text
    # Extract fine amount
    fine_patterns = [
        r'(?:amende|boete|fine|geldboete)\s+(?:van\s+)?(?:de\s+)?€?\s*([\d\s.,]+)\s*(?:euros?|EUR)?',
        r'€\s*([\d\s.,]+)',
    ]
    for pattern in fine_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            amount_str = match.group(1).replace(' ', '').replace('.', '').replace(',', '.')
            try:
                amount = float(amount_str)
                if amount > 1000:  # Likely a real fine
                    decision['fine_amount'] = amount
                    break
            except ValueError:
                pass

    # Extract sector/industry
    sector_keywords = {
        'retail': ['retail', 'detailhandel', 'distribution', 'supermarkt', 'supermarket'],
        'telecom': ['telecom', 'mobile', 'internet', 'broadband'],
        'energy': ['energie', 'energy', 'gas', 'electricity', 'électricité'],
        'pharma': ['pharma', 'médicament', 'geneesmiddel', 'drug'],
        'automotive': ['automotive', 'automobile', 'car', 'voiture', 'auto'],
        'transport': ['transport', 'logistics', 'logistiek'],
        'financial': ['bank', 'insurance', 'assurance', 'verzekering', 'financ'],
    }
    text_lower = text.lower()
    for sector, keywords in sector_keywords.items():
        if any(kw in text_lower for kw in keywords):
            decision['sector'] = sector
            break

    return decision


def extract_case_info_from_filename(filename: str) -> dict:
    """Extract case number and metadata from PDF filename."""
    info = {
        'case_number': None,
        'case_type': None,
        'year': None,
        'decision_type': None,
    }

    # Pattern: BMA-YYYY-TYPE-NN or ABC-YYYY-TYPE-NN
    match = re.search(r'(?:BMA|ABC)-(\d{4})-([A-Z]+)-(\d+)(?:-([A-Z]+))?', filename, re.IGNORECASE)
    if match:
        year = match.group(1)
        case_type_code = match.group(2).upper()
        seq_num = match.group(3)
        suffix = match.group(4)

        info['year'] = year
        info['case_number'] = f"{year[-2:]}-{case_type_code}-{seq_num}"

        # Map case types
        type_map = {
            'CC': 'concentration',
            'CCS': 'concentration_simplified',
            'RPR': 'restrictive_practices',
            'C': 'concentration',
        }
        info['case_type'] = type_map.get(case_type_code, case_type_code.lower())

        if suffix:
            info['decision_type'] = suffix.lower()

    return info


def extract_title_from_text(text: str) -> str:
    """Extract a meaningful title from the decision text."""
    # Look for "Zaak nr." pattern followed by case name
    match = re.search(r'Zaak\s+n[r°]\.\s*([^:]+?)(?:\n|$)', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()[:200]

    # Look for "Affaire" pattern (French)
    match = re.search(r'Affaire\s+n[°o]\.\s*([^:]+?)(?:\n|$)', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()[:200]

    # Look for parties mentioned after case reference
    match = re.search(r'(?:MEDE-[A-Z]+-\d+[/-]\d+)[:\s]+([^\n]+)', text)
    if match:
        return match.group(1).strip()[:200]

    return ""


def normalize(raw: dict) -> dict:
    """Transform raw BCA decision data into normalized schema."""
    # Extract info from PDF filename
    pdf_url = raw.get('pdf_url', '')
    filename = pdf_url.split('/')[-1].replace('.pdf', '') if pdf_url else 'unknown'
    file_info = extract_case_info_from_filename(filename)

    # Use extracted info or fallback to raw values
    case_num = raw.get('case_number') or file_info.get('case_number')
    case_type = raw.get('case_type') or file_info.get('case_type')
    year = raw.get('year') or file_info.get('year')
    decision_type = raw.get('decision_type') or file_info.get('decision_type')

    # Create unique ID
    if case_num:
        doc_id = f"{SOURCE_ID}/{case_num}"
    else:
        doc_id = f"{SOURCE_ID}/{filename}"

    # Extract title from text if not set
    title = raw.get('title', '')
    if not title:
        title = extract_title_from_text(raw.get('text', ''))
    if not title:
        title = case_num or filename

    # Build date
    date = raw.get('date')
    if not date and year:
        date = f"{year}-01-01"

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': raw.get('text', ''),
        'date': date,
        'url': raw.get('pdf_url') or raw.get('detail_url', ''),
        'case_number': case_num,
        'case_type': case_type,
        'decision_type': decision_type,
        'sector': raw.get('sector'),
        'fine_amount': raw.get('fine_amount'),
        'pdf_size': raw.get('pdf_size'),
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all BCA decisions."""
    total_pages = get_total_pages(session)
    print(f"Fetching decisions from {total_pages} pages...", file=sys.stderr)

    seen_ids = set()

    for page in range(total_pages):
        print(f"  Page {page + 1}/{total_pages}...", file=sys.stderr)
        decisions = fetch_decisions_from_page(session, page)

        for decision in decisions:
            time.sleep(RATE_LIMIT_DELAY)

            full_decision = fetch_decision_with_text(session, decision)
            if full_decision and full_decision.get('text'):
                record = normalize(full_decision)

                # Skip duplicates
                if record['_id'] in seen_ids:
                    continue
                seen_ids.add(record['_id'])

                yield record


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of decisions. Saves incrementally."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching sample of {count} decisions...", file=sys.stderr)

    # Fetch from first few pages
    all_decisions = []
    for page in range(3):  # First 3 pages should have enough
        decisions = fetch_decisions_from_page(session, page)
        all_decisions.extend(decisions)
        time.sleep(RATE_LIMIT_DELAY)
        if len(all_decisions) >= count + 10:
            break

    print(f"Found {len(all_decisions)} decision entries, fetching PDFs...", file=sys.stderr)

    for decision in all_decisions:
        if len(records) >= count:
            break

        case_num = decision.get('case_number', 'unknown')
        print(f"Fetching {case_num}...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, decision)
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

    # Save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="BE/BCA Competition Authority Fetcher")
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
        print("ERROR: pypdf library required for PDF text extraction", file=sys.stderr)
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
                fines = [r.get('fine_amount') for r in records if r.get('fine_amount')]
                types = {}
                for r in records:
                    ct = r.get('case_type', 'unknown')
                    types[ct] = types.get(ct, 0) + 1

                print(f"\n=== Sample Statistics ===", file=sys.stderr)
                print(f"  Records: {len(records)}", file=sys.stderr)
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars", file=sys.stderr)
                print(f"  Min text length: {min(text_lengths)} chars", file=sys.stderr)
                print(f"  Max text length: {max(text_lengths)} chars", file=sys.stderr)
                if fines:
                    print(f"  Fines found: {len(fines)} (total EUR {sum(fines):,.0f})", file=sys.stderr)
                print(f"\nBy case type:", file=sys.stderr)
                for ctype, cnt in sorted(types.items(), key=lambda x: -x[1]):
                    print(f"  {ctype}: {cnt}", file=sys.stderr)
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
