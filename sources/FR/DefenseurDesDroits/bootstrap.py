#!/usr/bin/env python3
"""
Défenseur des Droits (French Ombudsman) Data Fetcher

Fetches decisions from the French Ombudsman's documentation portal.
Covers recommendations, formal notices, amicable settlements, and court observations.

Data source: https://juridique.defenseurdesdroits.fr
License: Licence Ouverte 2.0

Document types:
- Recommandations: Recommendations to public services
- Rappels à la loi: Formal notices
- Règlements amiables: Amicable settlements
- Observations devant les juridictions: Court observations
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
from urllib.parse import urljoin, unquote

import pdfplumber
import requests
from bs4 import BeautifulSoup

# Constants
BASE_URL = "https://juridique.defenseurdesdroits.fr"
DECISIONS_SHELF_URL = f"{BASE_URL}/index.php?lvl=etagere_see&id=33"
RATE_LIMIT_DELAY = 2.0  # seconds between requests


def fetch_page(url: str, session: requests.Session) -> str:
    """Fetch a page with proper error handling."""
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.text


def get_pagination_urls(base_url: str, session: requests.Session) -> list[str]:
    """Get all pagination URLs from the shelf listing."""
    html = fetch_page(base_url, session)
    soup = BeautifulSoup(html, 'html.parser')
    
    urls = [base_url]  # Page 1
    
    # Find total pages from pagination
    # Look for pattern like "(1 - 10 / 2928)"
    page_info = soup.find(string=re.compile(r'\d+\s*-\s*\d+\s*/\s*\d+'))
    if page_info:
        match = re.search(r'/\s*(\d+)', page_info)
        if match:
            total_items = int(match.group(1))
            items_per_page = 10
            total_pages = (total_items + items_per_page - 1) // items_per_page
            
            for page in range(2, total_pages + 1):
                page_url = f"{base_url}&page={page}&nbr_lignes={total_items}"
                urls.append(page_url)
    
    return urls


def extract_documents_from_listing(html: str) -> list[dict]:
    """Extract document info from a listing page."""
    soup = BeautifulSoup(html, 'html.parser')
    documents = []
    
    # Find all notice links
    for link in soup.find_all('a', href=True):
        href = link['href']
        if 'lvl=notice_display&id=' in href or 'lvl=notice_display&amp;id=' in href:
            # Extract notice ID
            match = re.search(r'id=(\d+)', href)
            if match:
                notice_id = match.group(1)
                doc_url = f"{BASE_URL}/index.php?lvl=notice_display&id={notice_id}"
                
                # Also look for PDF link in same context
                explnum_match = re.search(r'explnum_id=(\d+)', str(link.parent))
                explnum_id = explnum_match.group(1) if explnum_match else None
                
                documents.append({
                    'notice_id': notice_id,
                    'url': doc_url,
                    'explnum_id': explnum_id
                })
    
    # Deduplicate by notice_id
    seen = set()
    unique_docs = []
    for doc in documents:
        if doc['notice_id'] not in seen:
            seen.add(doc['notice_id'])
            unique_docs.append(doc)
    
    return unique_docs


def parse_z3988_metadata(html: str) -> dict:
    """Extract metadata from Z3988 COinS span."""
    soup = BeautifulSoup(html, 'html.parser')
    z3988 = soup.find('span', class_='Z3988')
    
    metadata = {}
    if z3988 and z3988.get('title'):
        title_attr = z3988['title']
        
        # Parse title field
        title_match = re.search(r'rft\.btitle=([^&]+)', title_attr)
        if title_match:
            metadata['title'] = unquote(title_match.group(1))
        
        # Parse date field (format: DD/MM/YYYY)
        date_match = re.search(r'rft\.date=([^&]+)', title_attr)
        if date_match:
            date_str = unquote(date_match.group(1))
            # Convert DD/MM/YYYY to ISO format
            try:
                parts = date_str.split('/')
                if len(parts) == 3:
                    metadata['date'] = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                pass
        
        # Parse decision number (isbn field often has it)
        isbn_match = re.search(r'rft\.isbn=([^&]+)', title_attr)
        if isbn_match:
            metadata['decision_number'] = unquote(isbn_match.group(1))
    
    return metadata


def get_explnum_id_from_page(html: str) -> Optional[str]:
    """Extract explnum_id (PDF ID) from document page."""
    match = re.search(r'explnum_id=(\d+)', html)
    return match.group(1) if match else None


def extract_pdf_text(explnum_id: str, session: requests.Session) -> str:
    """Download PDF and extract text content."""
    pdf_url = f"{BASE_URL}/doc_num.php?explnum_id={explnum_id}"
    
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


def categorize_decision(title: str) -> str:
    """Categorize decision type based on title."""
    title_lower = title.lower()
    
    if 'recommandation' in title_lower:
        return 'recommendation'
    elif 'rappel à la loi' in title_lower or 'rappel au droit' in title_lower:
        return 'formal_notice'
    elif 'règlement amiable' in title_lower or 'reglement amiable' in title_lower:
        return 'settlement'
    elif 'observation' in title_lower:
        return 'court_observation'
    else:
        return 'decision'


def fetch_document(doc_info: dict, session: requests.Session) -> Optional[dict]:
    """Fetch a single document with full text."""
    url = doc_info['url']
    notice_id = doc_info['notice_id']
    
    try:
        html = fetch_page(url, session)
        
        # Extract metadata
        metadata = parse_z3988_metadata(html)
        
        # Get explnum_id if not already known
        explnum_id = doc_info.get('explnum_id') or get_explnum_id_from_page(html)
        
        if not explnum_id:
            print(f"No PDF found for notice {notice_id}", file=sys.stderr)
            return None
        
        # Extract PDF text
        time.sleep(RATE_LIMIT_DELAY)
        text = extract_pdf_text(explnum_id, session)
        
        if not text:
            print(f"No text extracted for notice {notice_id}", file=sys.stderr)
            return None
        
        # Extract decision number from title if not in metadata
        decision_number = metadata.get('decision_number', '')
        if not decision_number and metadata.get('title'):
            match = re.search(r'Décision\s+(\d{4}-\d+)', metadata['title'])
            if match:
                decision_number = match.group(1)
        
        # Generate ID
        if decision_number:
            doc_id = f"DDD-{decision_number}"
        else:
            doc_id = f"DDD-{notice_id}"
        
        return {
            '_id': doc_id,
            'notice_id': notice_id,
            'explnum_id': explnum_id,
            'url': url,
            'pdf_url': f"{BASE_URL}/doc_num.php?explnum_id={explnum_id}",
            'title': metadata.get('title', ''),
            'date': metadata.get('date'),
            'decision_number': decision_number,
            'decision_type': categorize_decision(metadata.get('title', '')),
            'text': text
        }
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all decisions from the Défenseur des Droits."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Legal-Data-Hunter/1.0 (Open Data Collection for Research)',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'fr,en;q=0.9'
    })
    
    # Get all pagination URLs
    print("Discovering pagination...", file=sys.stderr)
    pagination_urls = get_pagination_urls(DECISIONS_SHELF_URL, session)
    print(f"Found {len(pagination_urls)} pages", file=sys.stderr)
    
    doc_count = 0
    seen_notices = set()
    
    for page_num, page_url in enumerate(pagination_urls, 1):
        if max_docs and doc_count >= max_docs:
            break
        
        print(f"Processing page {page_num}/{len(pagination_urls)}", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)
        
        try:
            html = fetch_page(page_url, session)
            documents = extract_documents_from_listing(html)
            
            for doc_info in documents:
                if max_docs and doc_count >= max_docs:
                    break
                
                if doc_info['notice_id'] in seen_notices:
                    continue
                seen_notices.add(doc_info['notice_id'])
                
                time.sleep(RATE_LIMIT_DELAY)
                document = fetch_document(doc_info, session)
                
                if document and document.get('text'):
                    yield document
                    doc_count += 1
                    print(f"  [{doc_count}] {document['_id']}: {len(document['text'])} chars", file=sys.stderr)
        
        except Exception as e:
            print(f"Error processing page {page_url}: {e}", file=sys.stderr)
            continue


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    since_str = since.strftime('%Y-%m-%d')
    
    for doc in fetch_all():
        if doc.get('date') and doc['date'] >= since_str:
            yield doc


def normalize(raw: dict) -> dict:
    """Transform raw document data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()
    
    # Determine type: court observations are case_law-adjacent, others are doctrine
    doc_type = raw.get('decision_type', 'decision')
    if doc_type == 'court_observation':
        _type = 'case_law'
    else:
        _type = 'doctrine'
    
    return {
        '_id': raw['_id'],
        '_source': 'FR/DefenseurDesDroits',
        '_type': _type,
        '_fetched_at': now,
        'title': raw.get('title', ''),
        'text': raw['text'],
        'date': raw.get('date'),
        'url': raw.get('url'),
        'pdf_url': raw.get('pdf_url'),
        'decision_number': raw.get('decision_number'),
        'decision_type': raw.get('decision_type'),
        'notice_id': raw.get('notice_id'),
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
            dtype = s.get('decision_type', 'Unknown')
            by_type[dtype] = by_type.get(dtype, 0) + 1
        
        print(f"\nBy decision type:", file=sys.stderr)
        for dtype, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"  {dtype}: {cnt}", file=sys.stderr)


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Défenseur des Droits decisions fetcher')
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
            # Full bootstrap — write records to JSONL and stdout
            data_dir = script_dir / 'data'
            data_dir.mkdir(parents=True, exist_ok=True)
            jsonl_path = data_dir / 'records.jsonl'

            count = 0
            with open(jsonl_path, 'w', encoding='utf-8') as out:
                for raw in fetch_all():
                    record = normalize(raw)
                    line = json.dumps(record, ensure_ascii=False)
                    out.write(line + '\n')
                    print(line)  # also to stdout for pipeline compat
                    count += 1

            print(f"\nBootstrap complete: {count} records written to {jsonl_path}", file=sys.stderr)
    
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
