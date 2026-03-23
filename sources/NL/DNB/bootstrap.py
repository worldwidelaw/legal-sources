#!/usr/bin/env python3
"""
NL/DNB - Dutch Central Bank Supervision Doctrine

Fetches regulatory guidance (Open Boek Toezicht) and enforcement decisions
from De Nederlandsche Bank (DNB) via sitemap + HTML scraping.

Data source:
- https://www.dnb.nl/sitemap.xml (5000+ URLs)
- Open Boek Toezicht: ~917 pages of regulatory guidance
- Enforcement decisions: ~178 pages with formal decisions

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all documents
"""

import argparse
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

from bs4 import BeautifulSoup

# Constants
SOURCE_ID = "NL/DNB"
BASE_URL = "https://www.dnb.nl"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"

# URL patterns for doctrine content
URL_PATTERNS = [
    "/voor-de-sector/open-boek-toezicht/",
    "/algemeen-nieuws/handhavingsmaatregel-",
]

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def curl_fetch(url: str) -> Optional[str]:
    """Fetch URL using curl subprocess (default UA to avoid WAF blocks)."""
    try:
        cmd = [
            "curl", "-s", "-L",
            "--max-time", "60",
            url
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=90)
        if result.returncode != 0:
            print(f"curl error for {url}: {result.stderr.decode()}", file=sys.stderr)
            return None
        return result.stdout.decode("utf-8", errors="replace")
    except subprocess.TimeoutExpired:
        print(f"Timeout fetching {url}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def get_sitemap_urls() -> list[dict]:
    """Parse sitemap.xml and return URLs matching our patterns."""
    print("Fetching sitemap...", file=sys.stderr)
    content = curl_fetch(SITEMAP_URL)
    if not content:
        return []

    urls = []
    # Extract all <loc> URLs from sitemap
    all_locs = re.findall(r'<loc>([^<]+)</loc>', content)
    for url in all_locs:
        url = url.strip()
        for pattern in URL_PATTERNS:
            if pattern in url:
                doc_type = "enforcement" if "handhavingsmaatregel" in url else "guidance"
                urls.append({
                    'url': url,
                    'doc_type': doc_type,
                })
                break

    print(f"Found {len(urls)} doctrine URLs in sitemap", file=sys.stderr)
    return urls


def parse_page(url: str) -> Optional[dict]:
    """Parse a single DNB page and extract full text."""
    html = curl_fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')

    # Title
    h1 = soup.find('h1')
    title = h1.get_text(strip=True) if h1 else None
    if not title:
        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else url.split('/')[-1]

    # Content type from header
    type_span = soup.find('span', class_=re.compile(r'content-header__type'))
    content_type = type_span.get_text(strip=True) if type_span else None

    # Date
    date = None
    date_p = soup.find('p', class_=re.compile(r'page-meta__published'))
    if date_p:
        date_text = date_p.get_text(strip=True)
        # Try to parse Dutch date format
        date_match = re.search(r'(\d{1,2})\s+(januari|februari|maart|april|mei|juni|juli|augustus|september|oktober|november|december)\s+(\d{4})', date_text)
        if date_match:
            day = int(date_match.group(1))
            month_map = {
                'januari': 1, 'februari': 2, 'maart': 3, 'april': 4,
                'mei': 5, 'juni': 6, 'juli': 7, 'augustus': 8,
                'september': 9, 'oktober': 10, 'november': 11, 'december': 12
            }
            month = month_map.get(date_match.group(2), 1)
            year = int(date_match.group(3))
            date = f"{year:04d}-{month:02d}-{day:02d}"

    # Try JSON-LD for date
    if not date:
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                ld = json.loads(script.string)
                for field in ['datePublished', 'dateModified']:
                    if field in ld:
                        match = re.match(r'\d{4}-\d{2}-\d{2}', str(ld[field]))
                        if match:
                            date = match.group(0)
                            break
            except (json.JSONDecodeError, TypeError):
                pass
            if date:
                break

    # Full text from content area
    text_parts = []

    # Primary: rs-content div with text-block/rte
    content_div = soup.find('div', id='rs-content')
    if content_div:
        for block in content_div.find_all('div', class_='rte'):
            block_text = block.get_text(separator='\n', strip=True)
            if block_text:
                text_parts.append(block_text)

    # Fallback: text-block divs
    if not text_parts:
        for block in soup.find_all('div', class_='text-block'):
            block_text = block.get_text(separator='\n', strip=True)
            if block_text and len(block_text) > 50:
                text_parts.append(block_text)

    # Fallback: main content
    if not text_parts:
        main = soup.find('main') or soup.find('article')
        if main:
            main_text = main.get_text(separator='\n', strip=True)
            if len(main_text) > 100:
                text_parts.append(main_text)

    full_text = '\n\n'.join(text_parts)
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = full_text.strip()

    if not full_text or len(full_text) < 50:
        return None

    # Extract PDF links (for enforcement decisions)
    pdf_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.endswith('.pdf'):
            pdf_links.append(BASE_URL + href if href.startswith('/') else href)

    return {
        'title': title,
        'date': date,
        'text': full_text,
        'url': url,
        'content_type': content_type,
        'pdf_urls': pdf_links if pdf_links else None,
    }


def normalize(raw: dict, doc_type: str) -> dict:
    """Normalize a raw record into the standard schema."""
    url = raw.get('url', '')
    slug = url.replace(BASE_URL, '').strip('/')
    _id = re.sub(r'[^a-zA-Z0-9_-]', '_', slug)

    return {
        '_id': _id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': raw.get('date'),
        'url': url,
        'doc_type': doc_type,
        'content_type': raw.get('content_type'),
        'pdf_urls': raw.get('pdf_urls'),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all DNB doctrine documents."""
    sitemap_urls = get_sitemap_urls()

    if sample:
        # Take a mix of guidance and enforcement
        guidance = [u for u in sitemap_urls if u['doc_type'] == 'guidance'][:10]
        enforcement = [u for u in sitemap_urls if u['doc_type'] == 'enforcement'][:5]
        sitemap_urls = guidance + enforcement

    total = len(sitemap_urls)
    fetched = 0

    for i, entry in enumerate(sitemap_urls):
        url = entry['url']
        print(f"[{i+1}/{total}] Fetching: {url}", file=sys.stderr)

        raw = parse_page(url)
        if raw:
            record = normalize(raw, entry['doc_type'])
            yield record
            fetched += 1
            print(f"  Saved: {record['title'][:70]} ({len(record['text'])} chars)", file=sys.stderr)
        else:
            print(f"  Skipped: no text", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)

    print(f"Fetched {fetched}/{total} documents", file=sys.stderr)


def bootstrap(sample: bool = True):
    """Bootstrap the data source."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        safe_id = record['_id'][:100]
        fname = SAMPLE_DIR / f"{safe_id}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1

    print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}", file=sys.stderr)
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NL/DNB Bootstrap')
    sub = parser.add_subparsers(dest='command')
    boot = sub.add_parser('bootstrap')
    boot.add_argument('--sample', action='store_true', default=True)
    boot.add_argument('--full', action='store_true')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        sample = not args.full
        bootstrap(sample=sample)
    else:
        parser.print_help()
