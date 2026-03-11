#!/usr/bin/env python3
"""
CRE (Commission de régulation de l'énergie) Data Fetcher

Fetches deliberations and CoRDiS decisions from CRE (French energy regulatory authority).
Covers all deliberations since 1999 and CoRDiS dispute resolution decisions.

Data source: https://www.cre.fr/documents/
License: Open Government License (Licence Ouverte)

Document types:
- Délibérations: Regulatory decisions on tariffs, network access, markets (~3,500 docs)
- CoRDiS decisions: Dispute resolution and sanctions (~470 docs)
"""

import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional
from urllib.parse import urljoin

import pdfplumber
import requests
from bs4 import BeautifulSoup

# Constants
BASE_URL = "https://www.cre.fr"
DELIBERATIONS_URL = f"{BASE_URL}/documents/deliberations.html"
CORDIS_URL = f"{BASE_URL}/documents/decisions-du-cordis.html"
RATE_LIMIT_DELAY = 1.5  # seconds between requests


def fetch_page(url: str, session: requests.Session) -> str:
    """Fetch a page with proper error handling."""
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def parse_listing_page(html: str, url_pattern: str) -> list[dict]:
    """Parse a document listing page and extract document URLs."""
    soup = BeautifulSoup(html, 'html.parser')
    documents = []

    # Find all document links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if url_pattern in href and href.endswith('.html'):
            full_url = urljoin(BASE_URL, href)
            documents.append({'url': full_url})

    return documents


def get_pagination_urls(html: str, base_url: str) -> list[str]:
    """Extract pagination URLs from a listing page."""
    soup = BeautifulSoup(html, 'html.parser')
    urls = [base_url]  # Include page 1

    # Find pagination links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if 'tx_solr%5Bpage%5D=' in href or 'tx_solr[page]=' in href:
            full_url = urljoin(BASE_URL, href.split('#')[0])  # Remove anchor
            if full_url not in urls:
                urls.append(full_url)

    return urls


def extract_pdf_text(pdf_url: str, session: requests.Session) -> str:
    """Download PDF and extract text content."""
    try:
        response = session.get(pdf_url, timeout=60)
        response.raise_for_status()

        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            text_parts = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            return '\n\n'.join(text_parts)
    except Exception as e:
        print(f"Error extracting PDF {pdf_url}: {e}", file=sys.stderr)
        return ""


def parse_detail_page(html: str, url: str, doc_type: str, session: requests.Session) -> Optional[dict]:
    """Parse a document detail page and extract metadata + full text."""
    soup = BeautifulSoup(html, 'html.parser')

    # Extract title
    title = ""
    h1 = soup.find('h1')
    if h1:
        title = h1.get_text(strip=True)
    if not title:
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.text.split('|')[0].strip()

    # Extract date
    date = None
    time_tag = soup.find('time')
    if time_tag and time_tag.get('datetime'):
        date_str = time_tag['datetime']
        # Parse ISO format date
        try:
            date = date_str.split('T')[0]
        except:
            pass

    # Find PDF link
    pdf_url = None
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '.pdf' in href.lower():
            pdf_url = urljoin(BASE_URL, href)
            break

    if not pdf_url:
        print(f"No PDF found for {url}", file=sys.stderr)
        return None

    # Extract document number from URL or content
    doc_number = ""
    # Try to extract from content (e.g., "Délibérations N° : 2026-34")
    number_match = re.search(r'N[°º]\s*:\s*([\d\-]+)', html)
    if number_match:
        doc_number = number_match.group(1)
    else:
        # Try from PDF filename
        pdf_filename = pdf_url.split('/')[-1]
        num_match = re.search(r'(\d{4}-\d+)', pdf_filename)
        if num_match:
            doc_number = num_match.group(1)

    # Download PDF and extract text
    time.sleep(RATE_LIMIT_DELAY)
    text = extract_pdf_text(pdf_url, session)

    if not text:
        print(f"No text extracted from {pdf_url}", file=sys.stderr)
        return None

    # Generate unique ID
    if doc_number:
        doc_id = f"CRE-{doc_type.upper()}-{doc_number}"
    else:
        # Use URL slug as fallback
        slug = url.split('/')[-1].replace('.html', '')
        doc_id = f"CRE-{doc_type.upper()}-{slug[:50]}"

    return {
        '_id': doc_id,
        'url': url,
        'pdf_url': pdf_url,
        'title': title,
        'date': date,
        'doc_number': doc_number,
        'doc_type': doc_type,
        'text': text
    }


def fetch_documents(listing_url: str, doc_type: str, url_path: str, max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all documents from a listing page category."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'WorldWideLaw/1.0 (Open Data Collection for Research)',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'fr,en;q=0.9'
    })

    # Get first page to discover pagination
    html = fetch_page(listing_url, session)
    pagination_urls = get_pagination_urls(html, listing_url)

    print(f"Found {len(pagination_urls)} pages for {doc_type}", file=sys.stderr)

    seen_urls = set()
    doc_count = 0

    for page_url in pagination_urls:
        if max_docs and doc_count >= max_docs:
            break

        print(f"Processing page: {page_url}", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

        try:
            html = fetch_page(page_url, session)
            documents = parse_listing_page(html, f'/documents/{url_path}/')

            for doc_info in documents:
                if max_docs and doc_count >= max_docs:
                    break

                if doc_info['url'] in seen_urls:
                    continue
                seen_urls.add(doc_info['url'])

                time.sleep(RATE_LIMIT_DELAY)

                try:
                    detail_html = fetch_page(doc_info['url'], session)
                    document = parse_detail_page(detail_html, doc_info['url'], doc_type, session)

                    if document and document.get('text'):
                        yield document
                        doc_count += 1
                        print(f"  [{doc_count}] {document['_id']}: {len(document['text'])} chars", file=sys.stderr)

                except Exception as e:
                    print(f"Error fetching {doc_info['url']}: {e}", file=sys.stderr)
                    continue

        except Exception as e:
            print(f"Error fetching page {page_url}: {e}", file=sys.stderr)
            continue


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all CRE documents (deliberations + CoRDiS decisions)."""
    docs_per_type = max_docs // 2 if max_docs else None

    # Fetch deliberations
    print("=== Fetching Délibérations ===", file=sys.stderr)
    for doc in fetch_documents(DELIBERATIONS_URL, 'deliberations', 'deliberations', docs_per_type):
        yield doc

    # Fetch CoRDiS decisions
    print("\n=== Fetching CoRDiS Decisions ===", file=sys.stderr)
    for doc in fetch_documents(CORDIS_URL, 'cordis', 'decisions-du-cordis', docs_per_type):
        yield doc


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    since_str = since.strftime('%Y-%m-%d')

    for doc in fetch_all():
        if doc.get('date') and doc['date'] >= since_str:
            yield doc


def normalize(raw: dict) -> dict:
    """Transform raw document data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    # Determine type based on doc_type
    if raw.get('doc_type') == 'cordis':
        doc_type = 'case_law'  # CoRDiS is quasi-judicial
    else:
        doc_type = 'doctrine'  # Deliberations are regulatory doctrine

    return {
        '_id': raw['_id'],
        '_source': 'FR/CRE',
        '_type': doc_type,
        '_fetched_at': now,
        'title': raw.get('title', ''),
        'text': raw['text'],
        'date': raw.get('date'),
        'url': raw.get('url'),
        'pdf_url': raw.get('pdf_url'),
        'doc_number': raw.get('doc_number'),
        'doc_type': raw.get('doc_type'),
        'language': 'fr'
    }


def bootstrap_sample(sample_dir: Path, count: int = 12) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count):
        record = normalize(raw)
        samples.append(record)

        # Save individual sample
        filename = f"{record['_id']}.json"
        # Sanitize filename
        filename = re.sub(r'[^\w\-.]', '_', filename)

        with open(sample_dir / filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text'])} chars)", file=sys.stderr)

    # Save combined samples
    if samples:
        with open(sample_dir / 'all_samples.json', 'w', encoding='utf-8') as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        # Calculate statistics
        text_lengths = [len(s['text']) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)

        # Count by type
        by_type = {}
        for s in samples:
            dtype = s.get('doc_type', 'Unknown')
            by_type[dtype] = by_type.get(dtype, 0) + 1

        print(f"\nBy document type:", file=sys.stderr)
        for dtype, count in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"  {dtype}: {count}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='CRE documents fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Generate sample data only')
    parser.add_argument('--count', type=int, default=12,
                       help='Number of samples to generate')
    parser.add_argument('--since', type=str,
                       help='Fetch updates since date (YYYY-MM-DD)')

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / 'sample'

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            # Full bootstrap
            for raw in fetch_all():
                record = normalize(raw)
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'fetch':
        for raw in fetch_all(max_docs=args.count if args.sample else None):
            record = normalize(raw)
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for raw in fetch_updates(since):
            record = normalize(raw)
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
