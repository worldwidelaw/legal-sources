#!/usr/bin/env python3
"""
NL/AFM - Dutch Financial Markets Authority (Autoriteit Financiële Markten)

Fetches regulatory news, decisions, guidance and other doctrine from AFM.
Uses the Sitecore PagedNews API to enumerate articles, then extracts full
text from individual article HTML pages.

Data source:
- https://www.afm.nl

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all articles
"""

import argparse
import io
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
SOURCE_ID = "NL/AFM"
BASE_URL = "https://www.afm.nl"
PAGED_NEWS_URL = f"{BASE_URL}/api/sitecore/News/PagedNews"
CONTEXT_ITEM_ID = "{E28FA557-8A42-43B1-AB89-E05C563C9423}"
PAGE_SIZE = 12

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def curl_fetch(url: str, binary: bool = False):
    """Fetch URL using curl subprocess."""
    try:
        cmd = [
            "curl", "-s", "-L",
            "-H", f"User-Agent: {USER_AGENT}",
            "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "-H", "Accept-Language: nl-NL,nl;q=0.9,en;q=0.5",
            "--max-time", "60",
            url
        ]
        result = subprocess.run(cmd, capture_output=True, timeout=90)
        if result.returncode != 0:
            print(f"curl error for {url}: {result.stderr.decode()}", file=sys.stderr)
            return None
        if binary:
            return result.stdout
        return result.stdout.decode("utf-8", errors="replace")
    except subprocess.TimeoutExpired:
        print(f"Timeout fetching {url}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def extract_text_from_pdf(pdf_content: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="NL/AFM",
        source_id="",
        pdf_bytes=pdf_content,
        table="doctrine",
    ) or ""

def get_article_urls_from_api(max_pages: Optional[int] = None) -> list[dict]:
    """Enumerate all article URLs using the Sitecore PagedNews API."""
    articles = []
    page = 1

    while True:
        if max_pages and page > max_pages:
            break

        url = (
            f"{PAGED_NEWS_URL}?pageNumber={page}&pageSize={PAGE_SIZE}"
            f"&contextItemId={CONTEXT_ITEM_ID}&lang=nl-NL"
        )
        print(f"  Fetching API page {page}...", file=sys.stderr)
        html = curl_fetch(url)
        if not html or not html.strip():
            break

        soup = BeautifulSoup(html, 'html.parser')
        links = soup.find_all('a', href=True)

        page_articles = []
        for link in links:
            href = link['href']
            if '/nl-nl/' in href and href not in [a['url'] for a in articles]:
                full_url = urljoin(BASE_URL, href)
                # Extract title from link text
                title_el = link.find(['h2', 'h3', 'span'])
                title = title_el.get_text(strip=True) if title_el else link.get_text(strip=True)
                # Extract type tag if present
                tag_el = link.find('span', class_=re.compile(r'tag'))
                doc_type = tag_el.get_text(strip=True) if tag_el else None

                if title and len(title) > 5:
                    page_articles.append({
                        'url': full_url,
                        'title': title,
                        'doc_type': doc_type
                    })

        if not page_articles:
            print(f"  No articles found on page {page}, stopping.", file=sys.stderr)
            break

        articles.extend(page_articles)
        print(f"  Found {len(page_articles)} articles on page {page} (total: {len(articles)})", file=sys.stderr)

        page += 1
        time.sleep(1.0)

    # Deduplicate by URL
    seen = set()
    unique = []
    for a in articles:
        if a['url'] not in seen:
            seen.add(a['url'])
            unique.append(a)

    print(f"Total unique articles: {len(unique)}", file=sys.stderr)
    return unique


def parse_article_page(url: str) -> Optional[dict]:
    """Parse a single article page and extract full text."""
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

    # Date - try multiple sources
    date = None
    # From URL pattern /YYYY/mmm/
    date_match = re.search(r'/(\d{4})/(jan|feb|mrt|apr|mei|jun|jul|aug|sep|okt|nov|dec)/', url)
    if date_match:
        year = date_match.group(1)
        month_map = {
            'jan': '01', 'feb': '02', 'mrt': '03', 'apr': '04',
            'mei': '05', 'jun': '06', 'jul': '07', 'aug': '08',
            'sep': '09', 'okt': '10', 'nov': '11', 'dec': '12'
        }
        month = month_map.get(date_match.group(2), '01')
        date = f"{year}-{month}-01"

    # Try meta tags for more precise date
    for meta_name in ['dcterms.issued', 'dcterms.date', 'article:published_time']:
        meta = soup.find('meta', attrs={'name': meta_name}) or soup.find('meta', attrs={'property': meta_name})
        if meta and meta.get('content'):
            raw = meta['content'].strip()
            if re.match(r'\d{4}-\d{2}-\d{2}', raw):
                date = raw[:10]
                break

    # From visible date elements
    if not date:
        time_el = soup.find('time', attrs={'datetime': True})
        if time_el:
            raw = time_el['datetime']
            if re.match(r'\d{4}-\d{2}-\d{2}', raw):
                date = raw[:10]

    # Full text from content blocks
    text_parts = []

    # Primary: cc-content-text blocks
    content_blocks = soup.find_all('div', class_='cc-content-text')
    for block in content_blocks:
        block_text = block.get_text(separator='\n', strip=True)
        if block_text:
            text_parts.append(block_text)

    # Fallback: article body or main content
    if not text_parts:
        for selector in ['article', 'main', '.content', '#content']:
            el = soup.select_one(selector)
            if el:
                el_text = el.get_text(separator='\n', strip=True)
                if len(el_text) > 200:
                    text_parts.append(el_text)
                    break

    # Extract PDF links for additional text
    pdf_links = []
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.endswith('.pdf'):
            pdf_url = urljoin(BASE_URL, href)
            pdf_links.append(pdf_url)

    # Download first PDF if HTML text is thin
    html_text = '\n\n'.join(text_parts)
    if len(html_text) < 500 and pdf_links:
        print(f"  HTML text short ({len(html_text)} chars), trying PDF: {pdf_links[0]}", file=sys.stderr)
        pdf_content = curl_fetch(pdf_links[0], binary=True)
        if pdf_content:
            pdf_text = extract_text_from_pdf(pdf_content)
            if pdf_text:
                text_parts.append(f"\n--- PDF Content ---\n{pdf_text}")

    full_text = '\n\n'.join(text_parts)
    # Clean up
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = full_text.strip()

    if not full_text or len(full_text) < 50:
        print(f"  Skipping {url}: insufficient text ({len(full_text)} chars)", file=sys.stderr)
        return None

    return {
        'title': title,
        'date': date,
        'text': full_text,
        'url': url,
        'pdf_urls': pdf_links if pdf_links else None,
    }


def normalize(raw: dict) -> dict:
    """Normalize a raw record into the standard schema."""
    # Generate ID from URL
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
        'url': raw.get('url', ''),
        'doc_type': raw.get('doc_type'),
        'pdf_urls': raw.get('pdf_urls'),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all AFM articles."""
    max_pages = 2 if sample else None
    articles = get_article_urls_from_api(max_pages=max_pages)

    limit = 15 if sample else len(articles)
    fetched = 0

    for article_meta in articles[:limit]:
        url = article_meta['url']
        print(f"Fetching [{fetched+1}/{limit}]: {url}", file=sys.stderr)

        raw = parse_article_page(url)
        if raw:
            # Merge API metadata
            if article_meta.get('doc_type') and not raw.get('doc_type'):
                raw['doc_type'] = article_meta['doc_type']
            yield normalize(raw)
            fetched += 1

        time.sleep(RATE_LIMIT_DELAY)

    print(f"Fetched {fetched} articles total", file=sys.stderr)


def bootstrap(sample: bool = True):
    """Bootstrap the data source."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        fname = SAMPLE_DIR / f"{record['_id'][:100]}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        print(f"  Saved: {record['title'][:80]} ({len(record['text'])} chars)", file=sys.stderr)

    print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}", file=sys.stderr)
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NL/AFM Bootstrap')
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
