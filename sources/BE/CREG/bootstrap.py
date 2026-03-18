#!/usr/bin/env python3
"""
BE/CREG - Belgian Energy Regulator (Commission de Régulation de l'Électricité et du Gaz)

Fetches regulatory decisions and publications from CREG official website.
Decisions are published as PDFs in French and Dutch.

Data source:
- French: https://www.creg.be/fr/publications
- Dutch: https://www.creg.be/nl/publications
- Filter for decisions: ?f[0]=type_de_publication:15

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
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    PDF_AVAILABLE = True
    USE_PDFPLUMBER = True
except ImportError:
    try:
        import pypdf
        PDF_AVAILABLE = True
        USE_PDFPLUMBER = False
    except ImportError:
        PDF_AVAILABLE = False
        print("Warning: pdfplumber/pypdf not available, PDF text extraction disabled", file=sys.stderr)

# Constants
SOURCE_ID = "BE/CREG"
BASE_URL = "https://www.creg.be"

# Publications pages with type filters
# Type 15 = Décisions, Type 6 = Études, Type 16 = Notes
PUBLICATIONS_URL_FR = "https://www.creg.be/fr/publications"
PUBLICATIONS_URL_NL = "https://www.creg.be/nl/publications"

# Filter for decisions specifically
DECISIONS_FILTER = "type_de_publication:15"  # Décisions

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
        if USE_PDFPLUMBER:
            with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                text_parts = []
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                full_text = "\n\n".join(text_parts)
        else:
            import pypdf
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


def parse_publication_list(session: requests.Session, url: str) -> list[dict]:
    """Parse publication list page and extract publication metadata."""
    publications = []

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return publications

    soup = BeautifulSoup(response.text, 'html.parser')

    # Find all publication links - they follow pattern /fr/publications/{type}-{id}
    # Look for article or teaser elements
    for link in soup.find_all('a', href=re.compile(r'/(?:fr|nl|en)/publications/\w+-\w+')):
        href = link.get('href', '')
        if not href:
            continue

        # Build full URL
        full_url = urljoin(BASE_URL, href)

        # Skip if already seen (avoid duplicates from multiple link occurrences)
        if any(p['url'] == full_url for p in publications):
            continue

        # Get title from link text
        title = link.get_text().strip()
        if not title or len(title) < 5:
            # Try to find title in parent elements
            parent = link.find_parent(['article', 'div', 'li'])
            if parent:
                h_tag = parent.find(['h2', 'h3', 'h4', 'strong'])
                if h_tag:
                    title = h_tag.get_text().strip()

        if not title or len(title) < 5:
            continue

        # Extract publication type and ID from URL
        # Format: /fr/publications/decision-b3164 or /fr/publications/etude-f3020
        url_parts = href.rstrip('/').split('/')[-1]
        pub_type = None
        pub_id = None

        type_match = re.match(r'^(\w+)-(\w+)$', url_parts)
        if type_match:
            pub_type = type_match.group(1)  # decision, etude, note, rapport, etc.
            pub_id = type_match.group(2)    # b3164, f3020, etc.

        # Try to find date from nearby elements
        date = None
        parent = link.find_parent(['article', 'div', 'li', 'tr'])
        if parent:
            # Look for date patterns
            date_elem = parent.find(['time', 'span'], class_=re.compile(r'date|time', re.I))
            if date_elem:
                date_text = date_elem.get('datetime') or date_elem.get_text().strip()
                date = parse_date_string(date_text)

        publications.append({
            'url': full_url,
            'title': title,
            'publication_type': pub_type,
            'publication_id': pub_id,
            'date': date,
        })

    return publications


def parse_date_string(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO format."""
    if not date_str:
        return None

    date_str = date_str.strip()

    # Already ISO format
    if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
        return date_str[:10]

    # French month names
    fr_months = {
        'janvier': '01', 'février': '02', 'mars': '03', 'avril': '04',
        'mai': '05', 'juin': '06', 'juillet': '07', 'août': '08',
        'septembre': '09', 'octobre': '10', 'novembre': '11', 'décembre': '12'
    }

    # Dutch month names
    nl_months = {
        'januari': '01', 'februari': '02', 'maart': '03', 'april': '04',
        'mei': '05', 'juni': '06', 'juli': '07', 'augustus': '08',
        'september': '09', 'oktober': '10', 'november': '11', 'december': '12'
    }

    # Try French date format: "12 février 2026"
    for month_name, month_num in fr_months.items():
        pattern = rf'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})'
        match = re.search(pattern, date_str.lower())
        if match:
            day = match.group(1).zfill(2)
            year = match.group(2)
            return f"{year}-{month_num}-{day}"

    # Try Dutch date format: "12 februari 2026"
    for month_name, month_num in nl_months.items():
        pattern = rf'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})'
        match = re.search(pattern, date_str.lower())
        if match:
            day = match.group(1).zfill(2)
            year = match.group(2)
            return f"{year}-{month_num}-{day}"

    # Try DD/MM/YYYY or DD-MM-YYYY
    match = re.match(r'(\d{2})[/-](\d{2})[/-](\d{4})', date_str)
    if match:
        return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"

    return None


def get_pagination_count(session: requests.Session, url: str) -> int:
    """Get total number of pages from publications list."""
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error getting pagination: {e}", file=sys.stderr)
        return 1

    soup = BeautifulSoup(response.text, 'html.parser')

    # Look for pager items
    pager = soup.find('nav', class_=re.compile(r'pager'))
    if pager:
        # Find highest page number
        max_page = 0
        for link in pager.find_all('a', href=re.compile(r'page=\d+')):
            href = link.get('href', '')
            match = re.search(r'page=(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)

        # Also check pager__item--last
        last_item = pager.find('li', class_=re.compile(r'pager__item--last'))
        if last_item:
            link = last_item.find('a', href=True)
            if link:
                match = re.search(r'page=(\d+)', link.get('href', ''))
                if match:
                    max_page = max(max_page, int(match.group(1)))

        return max_page + 1  # page is 0-indexed

    return 1


def fetch_all_publication_metadata(session: requests.Session, lang: str = 'fr',
                                    pub_type_filter: str = None,
                                    max_pages: int = None) -> list[dict]:
    """Fetch all publication metadata from paginated results."""
    if lang == 'fr':
        base_url = PUBLICATIONS_URL_FR
    else:
        base_url = PUBLICATIONS_URL_NL

    # Build filter URL
    if pub_type_filter:
        filter_url = f"{base_url}?f[0]={pub_type_filter}"
    else:
        filter_url = base_url

    # Get total pages
    total_pages = get_pagination_count(session, filter_url)
    if max_pages:
        total_pages = min(total_pages, max_pages)

    print(f"Fetching {lang.upper()} publications from {total_pages} pages...", file=sys.stderr)

    all_publications = []
    seen_urls = set()

    for page in range(total_pages):
        page_url = f"{filter_url}&page={page}" if '?' in filter_url else f"{filter_url}?page={page}"

        print(f"  Page {page + 1}/{total_pages}...", file=sys.stderr)
        publications = parse_publication_list(session, page_url)

        for pub in publications:
            if pub['url'] not in seen_urls:
                seen_urls.add(pub['url'])
                all_publications.append(pub)

        if page < total_pages - 1:
            time.sleep(RATE_LIMIT_DELAY)

    return all_publications


def fetch_publication_page(session: requests.Session, url: str) -> dict:
    """Fetch publication detail page and extract metadata and PDF links."""
    result = {
        'summary': '',
        'pdf_url': None,
        'reference_number': None,
        'approval_date': None,
        'themes': [],
        'annexes': [],
    }

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return result

    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract summary/description from field--body
    body_div = soup.find('div', class_=re.compile(r'field--name-body'))
    if body_div:
        # Get the actual content, not the label
        content_div = body_div.find('div', class_='field__item')
        if content_div:
            result['summary'] = content_div.get_text(separator=' ', strip=True)

    # Extract main PDF link from field--documents
    docs_div = soup.find('div', class_=re.compile(r'field--documents'))
    if docs_div:
        pdf_link = docs_div.find('a', href=re.compile(r'\.pdf$', re.I))
        if pdf_link:
            result['pdf_url'] = urljoin(BASE_URL, pdf_link.get('href'))

    # Extract reference number
    ref_div = soup.find('div', class_=re.compile(r'field--creg-publication-number'))
    if ref_div:
        ref_item = ref_div.find('div', class_='field__item')
        if ref_item:
            result['reference_number'] = ref_item.get_text(strip=True)

    # Extract approval/publication date
    date_div = soup.find('div', class_=re.compile(r'field--publication-date'))
    if date_div:
        time_elem = date_div.find('time')
        if time_elem:
            date_str = time_elem.get('datetime') or time_elem.get_text(strip=True)
            result['approval_date'] = parse_date_string(date_str)

    # Extract themes/categories
    for tag in soup.find_all(['a', 'span'], class_=re.compile(r'tag|theme|category', re.I)):
        theme = tag.get_text(strip=True)
        if theme and len(theme) < 100 and theme not in result['themes']:
            result['themes'].append(theme)

    # Extract annexes
    annexes_list = soup.find('ul', class_=re.compile(r'field--publication-file'))
    if annexes_list:
        for annex_link in annexes_list.find_all('a', href=True):
            result['annexes'].append({
                'name': annex_link.get_text(strip=True),
                'url': urljoin(BASE_URL, annex_link.get('href'))
            })

    return result


def fetch_pdf_text(session: requests.Session, pdf_url: str, max_size_mb: float = 15.0) -> Optional[str]:
    """Download and extract text from a PDF."""
    try:
        # Check size first
        head_resp = session.head(pdf_url, timeout=20, allow_redirects=True)
        content_length = head_resp.headers.get('Content-Length')
        if content_length:
            size_mb = int(content_length) / (1024 * 1024)
            if size_mb > max_size_mb:
                print(f"Skipping large PDF ({size_mb:.1f} MB): {pdf_url}", file=sys.stderr)
                return None

        response = session.get(pdf_url, timeout=180)
        response.raise_for_status()

        text = extract_text_from_pdf(response.content)
        return text if text else None

    except requests.RequestException as e:
        print(f"Error downloading PDF {pdf_url}: {e}", file=sys.stderr)
        return None


def fetch_publication_with_text(session: requests.Session, publication: dict) -> Optional[dict]:
    """Fetch full publication with text from PDF."""
    page_url = publication.get('url')
    if not page_url:
        return None

    # Fetch publication page
    page_data = fetch_publication_page(session, page_url)

    # Update publication with page data
    publication.update({
        'summary': page_data['summary'],
        'reference_number': page_data['reference_number'],
        'themes': page_data['themes'],
        'annexes': page_data['annexes'],
    })

    # Use approval date if we didn't have one
    if not publication.get('date') and page_data['approval_date']:
        publication['date'] = page_data['approval_date']

    # Get PDF text
    pdf_url = page_data.get('pdf_url')
    if not pdf_url:
        print(f"No PDF found for: {publication.get('title', 'unknown')[:60]}", file=sys.stderr)
        return None

    publication['pdf_url'] = pdf_url

    time.sleep(RATE_LIMIT_DELAY)
    text = fetch_pdf_text(session, pdf_url)

    if not text or len(text) < 100:
        print(f"Could not extract text from: {publication.get('title', 'unknown')[:60]}", file=sys.stderr)
        return None

    publication['text'] = text
    return publication


def normalize(raw: dict) -> dict:
    """Transform raw CREG publication data into normalized schema."""
    # Create unique ID from URL slug or reference number
    url = raw.get('url', '')
    slug = url.rstrip('/').split('/')[-1] if url else 'unknown'

    # Use reference number if available for ID
    ref_num = raw.get('reference_number', '')
    if ref_num:
        # Clean reference number: "Décision (B)3164" -> "B3164"
        ref_clean = re.sub(r'[^A-Za-z0-9]', '', ref_num)
        doc_id = f"{SOURCE_ID}/{ref_clean}"
    else:
        doc_id = f"{SOURCE_ID}/{slug}"

    # Determine document type
    pub_type = raw.get('publication_type', 'unknown')
    doc_type = 'regulatory_decision'
    if pub_type in ['etude', 'study']:
        doc_type = 'study'
    elif pub_type in ['note']:
        doc_type = 'note'
    elif pub_type in ['rapport', 'report']:
        doc_type = 'report'

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': doc_type,
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': raw.get('date'),
        'url': raw.get('url', ''),
        'reference_number': raw.get('reference_number'),
        'summary': raw.get('summary', ''),
        'themes': raw.get('themes', []),
        'annexes': raw.get('annexes', []),
        'pdf_url': raw.get('pdf_url'),
        'publication_type': pub_type,
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all CREG publications."""
    print("Fetching all publication metadata...", file=sys.stderr)

    # Fetch decisions from French site
    publications = fetch_all_publication_metadata(
        session,
        lang='fr',
        pub_type_filter=DECISIONS_FILTER
    )
    print(f"Found {len(publications)} decisions", file=sys.stderr)

    for i, pub in enumerate(publications):
        print(f"[{i+1}/{len(publications)}] {pub.get('title', 'unknown')[:60]}...", file=sys.stderr)

        full_pub = fetch_publication_with_text(session, pub)
        if full_pub and full_pub.get('text'):
            record = normalize(full_pub)
            yield record

        time.sleep(RATE_LIMIT_DELAY)


def fetch_sample(session: requests.Session, count: int = 15, save_dir: Path = None) -> list[dict]:
    """Fetch a sample of publications."""
    records = []
    save_dir = save_dir or SAMPLE_DIR
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching sample of {count} decisions...", file=sys.stderr)

    # Fetch first few pages of decisions
    publications = fetch_all_publication_metadata(
        session,
        lang='fr',
        pub_type_filter=DECISIONS_FILTER,
        max_pages=3
    )
    print(f"Got {len(publications)} publication metadata entries", file=sys.stderr)

    for i, pub in enumerate(publications[:count + 5]):
        if len(records) >= count:
            break

        print(f"\n[{i+1}] Fetching: {pub.get('title', 'unknown')[:60]}...", file=sys.stderr)

        full_pub = fetch_publication_with_text(session, pub)
        if full_pub and full_pub.get('text'):
            record = normalize(full_pub)
            records.append(record)

            # Save incrementally
            filepath = save_dir / f"record_{len(records)-1:04d}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  -> Extracted {len(full_pub['text'])} chars", file=sys.stderr)
        else:
            print(f"  -> Skipped (no text)", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)

    return records


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def main():
    parser = argparse.ArgumentParser(description="BE/CREG Energy Regulator Fetcher")
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
                types = {}
                for r in records:
                    pt = r.get('publication_type', 'unknown')
                    types[pt] = types.get(pt, 0) + 1

                print(f"\n=== Sample Statistics ===", file=sys.stderr)
                print(f"  Records: {len(records)}", file=sys.stderr)
                print(f"  Avg text length: {sum(text_lengths) / len(text_lengths):.0f} chars", file=sys.stderr)
                print(f"  Min text length: {min(text_lengths)} chars", file=sys.stderr)
                print(f"  Max text length: {max(text_lengths)} chars", file=sys.stderr)

                # Show publication types
                print(f"\nBy publication type:", file=sys.stderr)
                for ptype, cnt in sorted(types.items(), key=lambda x: -x[1]):
                    print(f"  {ptype}: {cnt}", file=sys.stderr)

                # Check for common issues
                refs = [r.get('reference_number') for r in records if r.get('reference_number')]
                print(f"\nReference numbers found: {len(refs)}/{len(records)}", file=sys.stderr)
                dates = [r.get('date') for r in records if r.get('date')]
                print(f"Dates found: {len(dates)}/{len(records)}", file=sys.stderr)
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
