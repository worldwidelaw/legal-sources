#!/usr/bin/env python3
"""
IE/DPC - Irish Data Protection Commission

Fetches DPC enforcement decisions from the official website.
Ireland hosts EU headquarters for Meta, Google, Apple, Microsoft, TikTok etc.,
making DPC decisions highly significant for EU-wide GDPR enforcement.

Data source:
- https://www.dataprotection.ie/en/dpc-guidance/law/decisions-made-under-data-protection-act-2018
- Decisions published as PDFs with full text

License: Irish Public Sector Open Licence

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

try:
    import pypdf
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Warning: pypdf not available, PDF text extraction disabled", file=sys.stderr)

# Constants
SOURCE_ID = "IE/DPC"
BASE_URL = "https://www.dataprotection.ie"
DECISIONS_URL = f"{BASE_URL}/en/dpc-guidance/law/decisions-made-under-data-protection-act-2018"
RATE_LIMIT_DELAY = 2.0
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"


def get_session() -> requests.Session:
    """Create requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-IE,en;q=0.9",
    })
    return session


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF content using pypdf."""
    if not PDF_AVAILABLE:
        return ""

    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_content))
        text_parts = []
        for page in reader.pages:
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


def fetch_decision_page_urls(session: requests.Session) -> list[dict]:
    """Fetch all decision page URLs from the listing page."""
    decisions = []

    print("Fetching decisions listing page...", file=sys.stderr)
    try:
        response = session.get(DECISIONS_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching listing page: {e}", file=sys.stderr)
        return decisions

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all links to decision pages - multiple URL patterns exist
    # The page uses various paths including:
    # - /en/dpc-guidance/law/decisions/...
    # - /en/dpc-guidance/law/decisions-made-under-data-protection-act-2018/...
    # - /en/resources/law/decisions/...
    # - /en/dpc-guidance/resources/law/decisions/...
    # - /en/treoir-ccs/law/decisions/... (Irish language)
    decision_patterns = [
        '/decisions/',
        '/law/decisions/',
        '/decisions-made-under-data-protection-act-2018/'
    ]

    for link in soup.find_all('a', href=True):
        href = link['href']

        # Check if it's a decision link (not a PDF directly)
        # Look for decision-related URL patterns, but exclude the main listing page itself
        is_decision = any(pattern in href for pattern in decision_patterns)
        is_listing_page = href.rstrip('/').endswith('decisions-made-under-data-protection-act-2018')
        if is_decision and not href.endswith('.pdf') and not is_listing_page:
            # Normalize URL
            if href.startswith('http'):
                full_url = href
            else:
                full_url = urljoin(BASE_URL, href)

            # Extract title from link text
            title = link.get_text().strip()
            if title and full_url not in [d['url'] for d in decisions]:
                decisions.append({
                    'url': full_url,
                    'title': title,
                })

    print(f"Found {len(decisions)} decision pages", file=sys.stderr)
    return decisions


def fetch_decision_details(session: requests.Session, decision_info: dict) -> Optional[dict]:
    """Fetch decision page and extract PDF link and metadata."""
    url = decision_info['url']

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching decision page {url}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find PDF links
    pdf_urls = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.lower().endswith('.pdf'):
            pdf_url = urljoin(BASE_URL, href) if not href.startswith('http') else href
            pdf_urls.append(pdf_url)

    if not pdf_urls:
        print(f"No PDF found for {url}", file=sys.stderr)
        return None

    # Use the first PDF (usually the main decision)
    pdf_url = pdf_urls[0]

    # Extract metadata from the page
    title = decision_info.get('title', '')

    # Try to get better title from page
    h1 = soup.find('h1')
    if h1:
        title = h1.get_text().strip()

    # Extract date from title or page content
    date = None
    date_patterns = [
        r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        r'(\d{4})-(\d{2})-(\d{2})',
    ]

    months = {
        'January': '01', 'February': '02', 'March': '03', 'April': '04',
        'May': '05', 'June': '06', 'July': '07', 'August': '08',
        'September': '09', 'October': '10', 'November': '11', 'December': '12'
    }

    page_text = soup.get_text()
    for pattern in date_patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            groups = match.groups()
            if len(groups) == 3 and groups[0].isdigit():
                # Pattern: DD Month YYYY
                day = groups[0].zfill(2)
                month = months.get(groups[1].title(), '01')
                year = groups[2]
                date = f"{year}-{month}-{day}"
            elif len(groups) == 2:
                # Pattern: Month YYYY
                month = months.get(groups[0].title(), '01')
                year = groups[1]
                date = f"{year}-{month}-01"
            break

    # Try to extract organization name from title
    organization = None
    org_patterns = [
        r'(?:Inquiry\s+(?:into|concerning)\s+)([\w\s,]+?)(?:\s*[-–—]\s*|\s+\d)',
        r'^([\w\s,]+?)(?:\s*[-–—]\s*|\s+(?:January|February|March|April|May|June|July|August|September|October|November|December))',
    ]
    for pattern in org_patterns:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            organization = match.group(1).strip()
            break

    return {
        'url': url,
        'pdf_url': pdf_url,
        'title': title,
        'date': date,
        'organization': organization,
    }


def fetch_decision_with_text(session: requests.Session, decision: dict, max_pdf_size_mb: float = 15.0) -> Optional[dict]:
    """Download PDF and extract text for a decision."""
    pdf_url = decision.get('pdf_url')
    if not pdf_url:
        return None

    try:
        # First check PDF size with HEAD request
        head_resp = session.head(pdf_url, timeout=20)
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

    # Try to extract fine amount from text
    fine_amount = None
    fine_patterns = [
        r'€\s*([\d,]+(?:\.\d{2})?)\s*(?:million|m)',
        r'([\d,]+(?:\.\d{2})?)\s*(?:million|m)\s*(?:euro|EUR|€)',
        r'fine\s+of\s+€?\s*([\d,]+(?:\.\d{2})?)',
        r'administrative\s+fine[s]?\s+(?:of\s+)?(?:totaling\s+)?€?\s*([\d,]+(?:\.\d{2})?)',
    ]
    for pattern in fine_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            amount_str = match.group(1).replace(',', '')
            try:
                amount = float(amount_str)
                if 'million' in match.group(0).lower() or 'm' in match.group(0).lower():
                    amount *= 1_000_000
                fine_amount = amount
                break
            except ValueError:
                pass

    # Try to extract GDPR articles mentioned
    gdpr_articles = []
    article_matches = re.findall(r'Article\s+(\d+(?:\(\d+\))?(?:\([a-z]\))?)', text, re.IGNORECASE)
    gdpr_articles = list(set(article_matches))[:10]  # Limit to 10 unique articles

    decision['text'] = text
    decision['pdf_size'] = len(pdf_content)
    decision['fine_amount'] = fine_amount
    decision['gdpr_articles'] = gdpr_articles

    return decision


def normalize(raw: dict) -> dict:
    """Transform raw DPC decision data into normalized schema."""
    # Create unique ID from URL path
    url_path = urlparse(raw.get('url', '')).path
    slug = url_path.rstrip('/').split('/')[-1] or 'unknown'
    doc_id = f"{SOURCE_ID}/{slug}"

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'case_law',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': raw.get('date'),
        'url': raw.get('url', ''),
        'pdf_url': raw.get('pdf_url'),
        'organization': raw.get('organization'),
        'fine_amount': raw.get('fine_amount'),
        'gdpr_articles': raw.get('gdpr_articles', []),
        'pdf_size': raw.get('pdf_size'),
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all DPC decisions."""
    decision_pages = fetch_decision_page_urls(session)

    for decision_info in decision_pages:
        time.sleep(RATE_LIMIT_DELAY)

        details = fetch_decision_details(session, decision_info)
        if not details:
            continue

        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, details)
        if full_decision and full_decision.get('text'):
            yield normalize(full_decision)


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of decisions. Saves incrementally to avoid data loss."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    decision_pages = fetch_decision_page_urls(session)

    # Sort to get most recent decisions (usually at top of list)
    for decision_info in decision_pages[:count + 10]:
        if len(records) >= count:
            break

        print(f"Fetching {decision_info['url']}...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        details = fetch_decision_details(session, decision_info)
        if not details:
            continue

        time.sleep(RATE_LIMIT_DELAY)

        full_decision = fetch_decision_with_text(session, details)
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

    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="IE/DPC Data Protection Commission Fetcher")
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
                print(f"\nSummary:")
                print(f"  Records: {len(records)}")
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars")
                print(f"  Min text length: {min(text_lengths)} chars")
                print(f"  Max text length: {max(text_lengths)} chars")
                if fines:
                    print(f"  Fines found: {len(fines)} (total €{sum(fines):,.0f})")
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
