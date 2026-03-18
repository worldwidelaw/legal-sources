#!/usr/bin/env python3
"""
CH/CAS - Court of Arbitration for Sport (CAS / TAS)

Fetches arbitration awards from the CAS Jurisprudence Database.
The CAS API provides case metadata, and full award texts are available as PDFs.

Data source:
- API: https://jurisprudence.tas-cas.org/CaseLawDocument/SearchCaseLawDocument
- PDFs: https://jurisprudence.tas-cas.org/pdf/{filename}

The CAS publishes non-confidential arbitration awards since 1986:
- Appeal Procedures (A)
- Ordinary Procedures (O)
- Ad Hoc Division (AHD)
- Anti-Doping Division (ADD)

License: Public access for non-confidential awards

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all awards
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

try:
    import pdfplumber
    PDF_AVAILABLE = True
    USE_PYPDF = False
except ImportError:
    try:
        import pypdf
        PDF_AVAILABLE = True
        USE_PYPDF = True
    except ImportError:
        PDF_AVAILABLE = False
        print("Warning: Neither pdfplumber nor pypdf available, PDF text extraction disabled", file=sys.stderr)

# Constants
SOURCE_ID = "CH/CAS"
API_BASE = "https://jurisprudence.tas-cas.org"
SEARCH_ENDPOINT = f"{API_BASE}/CaseLawDocument/SearchCaseLawDocument"
DETAIL_ENDPOINT = f"{API_BASE}/CaseLawDocument"
PDF_BASE = f"{API_BASE}/pdf"

RATE_LIMIT_DELAY = 1.5
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
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


def fetch_case_list(session: requests.Session, page: int = 1, page_size: int = 50) -> dict:
    """Fetch a page of cases from the CAS API."""
    params = {
        "pageNumber": page,
        "pageSize": page_size,
    }

    try:
        response = session.get(SEARCH_ENDPOINT, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching case list page {page}: {e}", file=sys.stderr)
        return {}


def fetch_case_detail(session: requests.Session, guid: str) -> Optional[dict]:
    """Fetch detailed case information."""
    url = f"{DETAIL_ENDPOINT}/{guid}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching case detail {guid}: {e}", file=sys.stderr)
        return None


def download_pdf_text(session: requests.Session, filename: str, max_pdf_size_mb: float = 15.0) -> Optional[str]:
    """Download PDF and extract text."""
    if not filename:
        return None

    pdf_url = f"{PDF_BASE}/{filename}"

    try:
        # First check PDF size with HEAD request
        head_resp = session.head(pdf_url, timeout=20, allow_redirects=True)
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > max_pdf_size_mb:
                print(f"Skipping large PDF ({size_mb:.1f} MB > {max_pdf_size_mb} MB): {filename}", file=sys.stderr)
                return None

        response = session.get(pdf_url, timeout=180)
        response.raise_for_status()
        pdf_content = response.content
    except requests.RequestException as e:
        print(f"Error downloading PDF {filename}: {e}", file=sys.stderr)
        return None

    text = extract_text_from_pdf(pdf_content)
    if not text or len(text) < 100:
        print(f"Warning: Could not extract meaningful text from {filename}", file=sys.stderr)
        return None

    return text


def normalize(case_detail: dict, text: str) -> dict:
    """Transform raw CAS case data into normalized schema."""
    # Create unique ID from title (case number)
    title = case_detail.get('title', '')
    case_number = title.replace('/', '_')
    doc_id = f"{SOURCE_ID}/{case_number}"

    # Parse decision date
    decision_date = case_detail.get('decisionDate')
    if decision_date:
        # Format: "2024-02-29T00:00:00Z"
        try:
            dt = datetime.fromisoformat(decision_date.replace('Z', '+00:00'))
            date_str = dt.strftime('%Y-%m-%d')
        except:
            date_str = decision_date[:10] if len(decision_date) >= 10 else None
    else:
        date_str = None

    # Get procedure type
    procedure = case_detail.get('caseLawProcedure', {})
    procedure_type = procedure.get('nameEn', '')

    # Get matter type
    matter = case_detail.get('matter', {})
    matter_type = matter.get('nameEn', '')

    # Get outcome
    outcome_data = case_detail.get('outcome', {})
    outcome = outcome_data.get('decision', '')

    # Get sport
    sport = case_detail.get('sport', {})
    sport_name = sport.get('nameEn', '')

    # Get category
    category = case_detail.get('caseLawCategory', {})
    category_type = category.get('nameEn', '')

    # Get parties
    appellants = case_detail.get('appellants', [])
    if isinstance(appellants, list):
        appellants_str = "; ".join(appellants)
    else:
        appellants_str = appellants or ''

    respondents = case_detail.get('respondents', [])
    if isinstance(respondents, list):
        respondents_str = "; ".join(respondents)
    else:
        respondents_str = respondents or ''

    # Get arbitrators
    arbitrators = []
    president = case_detail.get('president')
    if president:
        arbitrators.append(f"President: {president}")
    arb1 = case_detail.get('arbitrator1')
    if arb1:
        arbitrators.append(arb1)
    arb2 = case_detail.get('arbitrator2')
    if arb2:
        arbitrators.append(arb2)

    # Get keywords
    keywords = case_detail.get('keywords', [])
    keyword_strs = [kw.get('nameEn', '') for kw in keywords if kw.get('nameEn')]

    # Get language
    language = case_detail.get('language', {})
    lang_code = language.get('isoCode', 'En')

    # Build URL
    filename = case_detail.get('fileName', '')
    pdf_url = f"{PDF_BASE}/{filename}" if filename else f"https://jurisprudence.tas-cas.org"

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': text,
        'date': date_str,
        'url': pdf_url,
        'language': lang_code.lower() if lang_code else None,
        'case_number': title,
        'year': case_detail.get('year'),
        'procedure_type': procedure_type,
        'matter_type': matter_type,
        'outcome': outcome,
        'sport': sport_name,
        'category': category_type,
        'appellants': appellants_str,
        'respondents': respondents_str,
        'arbitrators': "; ".join(arbitrators) if arbitrators else None,
        'keywords': keyword_strs if keyword_strs else None,
        'follow_up': case_detail.get('followUp') or None,
        'follow_up_outcome': case_detail.get('followUpOutcome') or None,
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all CAS awards."""
    print("Fetching all CAS awards...", file=sys.stderr)

    # Get first page to determine total
    result = fetch_case_list(session, page=1, page_size=50)
    if not result:
        return

    total_count = result.get('totalCount', 0)
    total_pages = result.get('totalPages', 0)
    print(f"Total awards: {total_count}, pages: {total_pages}", file=sys.stderr)

    page = 1
    while page <= total_pages:
        result = fetch_case_list(session, page=page, page_size=50)
        items = result.get('items', [])

        for item in items:
            guid = item.get('guid')
            if not guid:
                continue

            time.sleep(RATE_LIMIT_DELAY)

            # Fetch detailed case info
            case_detail = fetch_case_detail(session, guid)
            if not case_detail:
                continue

            # Download and extract PDF text
            filename = case_detail.get('fileName')
            if not filename:
                print(f"No PDF for case {item.get('title')}", file=sys.stderr)
                continue

            time.sleep(RATE_LIMIT_DELAY)
            text = download_pdf_text(session, filename)
            if not text:
                continue

            record = normalize(case_detail, text)
            yield record

        page += 1
        time.sleep(RATE_LIMIT_DELAY)


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of awards. Saves incrementally to avoid data loss."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching sample of {count} awards...", file=sys.stderr)

    # Fetch more than needed to account for missing PDFs
    result = fetch_case_list(session, page=1, page_size=min(count * 2, 100))
    items = result.get('items', [])

    print(f"Found {len(items)} cases in first page, extracting {count} with full text...", file=sys.stderr)

    for item in items:
        if len(records) >= count:
            break

        guid = item.get('guid')
        title = item.get('title', 'unknown')
        if not guid:
            continue

        print(f"Processing: {title}...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        # Fetch detailed case info
        case_detail = fetch_case_detail(session, guid)
        if not case_detail:
            continue

        # Download and extract PDF text
        filename = case_detail.get('fileName')
        if not filename:
            print(f"  No PDF available", file=sys.stderr)
            continue

        time.sleep(RATE_LIMIT_DELAY)
        text = download_pdf_text(session, filename)
        if not text:
            continue

        record = normalize(case_detail, text)
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

    # Save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="CH/CAS Court of Arbitration for Sport Fetcher")
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
                sports = {}
                procedures = {}
                for r in records:
                    s = r.get('sport', 'unknown')
                    sports[s] = sports.get(s, 0) + 1
                    p = r.get('procedure_type', 'unknown')
                    procedures[p] = procedures.get(p, 0) + 1

                print(f"\n=== Sample Statistics ===", file=sys.stderr)
                print(f"  Records: {len(records)}", file=sys.stderr)
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars", file=sys.stderr)
                print(f"  Min text length: {min(text_lengths)} chars", file=sys.stderr)
                print(f"  Max text length: {max(text_lengths)} chars", file=sys.stderr)
                print(f"\nBy sport:", file=sys.stderr)
                for sport, cnt in sorted(sports.items(), key=lambda x: -x[1])[:5]:
                    print(f"  {sport}: {cnt}", file=sys.stderr)
                print(f"\nBy procedure:", file=sys.stderr)
                for proc, cnt in sorted(procedures.items(), key=lambda x: -x[1]):
                    print(f"  {proc}: {cnt}", file=sys.stderr)
            else:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch...", file=sys.stderr)
            count = 0
            for record in fetch_all(session):
                count += 1
                print(f"Fetched: {record.get('title', 'unknown')}", file=sys.stderr)
            print(f"Total: {count} awards", file=sys.stderr)


if __name__ == "__main__":
    main()
