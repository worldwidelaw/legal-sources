#!/usr/bin/env python3
"""
FR/AMF_Doctrine - AMF Regulatory Doctrine Fetcher

Fetches AMF (Autorité des Marchés Financiers) regulatory doctrine documents.
- Instructions (DOC-YYYY-NN)
- Positions and recommendations
- Professional rules (règles professionnelles approuvées)

Data source: https://www.amf-france.org/fr/reglementation/doctrine
RSS feed: https://www.amf-france.org/fr/flux-rss/display/31
License: Licence Ouverte Etalab 2.0

Usage:
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

SOURCE_ID = "FR/AMF_Doctrine"
BASE_URL = "https://www.amf-france.org"
RSS_FEED_URL = "https://www.amf-france.org/fr/flux-rss/display/31"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

SAMPLE_DIR = Path(__file__).parent / "sample"


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean text."""
    if not html_text:
        return ""
    text = unescape(html_text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def clean_url(url: str) -> str:
    """Remove tracking parameters from URL."""
    if not url:
        return ""
    parsed = urlparse(url)
    # Remove tracking parameters (xts, xtor, etc.)
    clean_path = parsed.path
    return urljoin(BASE_URL, clean_path)


def extract_doc_reference(url: str, title: str) -> str:
    """Extract document reference (DOC-YYYY-NN) from URL or title."""
    # Try URL first
    match = re.search(r'doc-(\d{4}-\d+)', url, re.IGNORECASE)
    if match:
        return f"DOC-{match.group(1).upper()}"

    # Try title
    match = re.search(r'DOC-(\d{4}-\d+)', title, re.IGNORECASE)
    if match:
        return f"DOC-{match.group(1).upper()}"

    # Try extracting from URL path
    path_parts = urlparse(url).path.split('/')
    for part in reversed(path_parts):
        if part and not part.startswith('#'):
            return part.replace('-', '_').upper()[:50]

    return ""


def parse_rss_feed(session: requests.Session) -> list:
    """Parse the AMF doctrine RSS feed and return items."""
    print(f"Fetching RSS feed: {RSS_FEED_URL}")

    try:
        resp = session.get(RSS_FEED_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching RSS feed: {e}")
        return []

    items = []
    try:
        root = ET.fromstring(resp.content)

        for item in root.findall('.//item'):
            title_elem = item.find('title')
            link_elem = item.find('link')
            description_elem = item.find('description')
            pubdate_elem = item.find('pubDate')
            guid_elem = item.find('guid')

            if title_elem is not None and link_elem is not None:
                items.append({
                    'title': title_elem.text.strip() if title_elem.text else '',
                    'url': clean_url(link_elem.text.strip() if link_elem.text else ''),
                    'description': description_elem.text.strip() if description_elem is not None and description_elem.text else '',
                    'pub_date': pubdate_elem.text.strip() if pubdate_elem is not None and pubdate_elem.text else '',
                    'guid': guid_elem.text.strip() if guid_elem is not None and guid_elem.text else '',
                })

        print(f"Found {len(items)} items in RSS feed")

    except ET.ParseError as e:
        print(f"Error parsing RSS feed: {e}")
        return []

    return items


def fetch_document_page(url: str, session: requests.Session) -> dict:
    """Fetch a doctrine document page and extract content."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return {}

    soup = BeautifulSoup(resp.text, 'html.parser')

    # Extract title
    title = ""
    h1 = soup.find('h1')
    if h1:
        title = h1.get_text(strip=True)

    # Extract document type/category
    doc_type = ""
    breadcrumb = soup.find('nav', {'aria-label': 'Fil d\'Ariane'})
    if breadcrumb:
        links = breadcrumb.find_all('a')
        if len(links) >= 2:
            doc_type = links[-1].get_text(strip=True)

    # Extract main content from content-accordion divs
    text_parts = []
    content_sections = soup.find_all('div', class_='content-accordion')
    for section in content_sections:
        section_text = section.get_text(separator='\n', strip=True)
        if section_text:
            text_parts.append(section_text)

    # Also try regular content divs if no accordion content found
    if not text_parts:
        content_div = soup.find('div', class_='content')
        if content_div:
            text_parts.append(content_div.get_text(separator='\n', strip=True))

    # Extract PDF links
    pdf_urls = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if '.pdf' in href.lower():
            pdf_url = urljoin(BASE_URL, href)
            if pdf_url not in pdf_urls:
                pdf_urls.append(pdf_url)

    # Extract date from page metadata or content
    date = ""
    date_meta = soup.find('meta', {'property': 'article:published_time'})
    if date_meta and date_meta.get('content'):
        date = date_meta['content'][:10]

    # Try finding date in page content
    if not date:
        date_span = soup.find('span', class_='date')
        if date_span:
            date_text = date_span.get_text(strip=True)
            # Try to parse French date format
            match = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_text)
            if match:
                date = f"{match.group(3)}-{match.group(2).zfill(2)}-{match.group(1).zfill(2)}"

    # Clean and combine text
    full_text = "\n\n".join(text_parts)
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)

    return {
        'title': title,
        'text': full_text.strip(),
        'doc_type': doc_type,
        'pdf_urls': pdf_urls,
        'date': date,
    }


def normalize(item: dict, page_data: dict) -> dict:
    """Transform RSS item and page data into standard schema."""
    url = item.get('url', '')
    title = page_data.get('title') or item.get('title', '')

    # Generate document reference
    doc_ref = extract_doc_reference(url, title)

    # Generate unique ID
    doc_id = f"FR_AMF_{doc_ref}" if doc_ref else f"FR_AMF_{hash(url) % 1000000}"

    # Parse date from RSS or page
    date = page_data.get('date', '')
    if not date:
        pub_date = item.get('pub_date', '')
        if pub_date:
            # Parse RFC 2822 date format (e.g., "Wed, 18 Feb 2026 00:00:00 +0100")
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(pub_date)
                date = dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                # Try manual parsing
                match = re.search(r'(\d{1,2}) (\w+) (\d{4})', pub_date)
                if match:
                    months = {'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                             'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                             'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
                    day, month, year = match.groups()
                    month_num = months.get(month[:3], '01')
                    date = f"{year}-{month_num}-{day.zfill(2)}"

    # Get full text from page or fallback to RSS description
    text = page_data.get('text', '')
    if not text or len(text) < 100:
        text = clean_html(item.get('description', ''))

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": url,
        "doc_reference": doc_ref,
        "doc_type": page_data.get('doc_type', ''),
        "summary": clean_html(item.get('description', '')),
        "pdf_urls": page_data.get('pdf_urls', []),
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all doctrine documents from RSS feed."""
    session = requests.Session()

    items = parse_rss_feed(session)

    count = 0
    for item in items:
        if max_records and count >= max_records:
            break

        url = item.get('url', '')
        if not url:
            continue

        print(f"  Fetching: {item.get('title', url)[:60]}...")

        page_data = fetch_document_page(url, session)

        if page_data.get('text') and len(page_data['text']) >= 50:
            record = normalize(item, page_data)
            yield record
            count += 1
            print(f"    -> {len(record['text']):,} chars")
        else:
            print(f"    -> Skipped (insufficient text)")

        time.sleep(2.0)  # Rate limiting

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since the given date."""
    session = requests.Session()

    items = parse_rss_feed(session)

    for item in items:
        pub_date_str = item.get('pub_date', '')
        if pub_date_str:
            try:
                from email.utils import parsedate_to_datetime
                pub_date = parsedate_to_datetime(pub_date_str)
                if pub_date.replace(tzinfo=None) < since.replace(tzinfo=None):
                    continue
            except (ValueError, TypeError):
                pass

        url = item.get('url', '')
        if not url:
            continue

        page_data = fetch_document_page(url, session)

        if page_data.get('text') and len(page_data['text']) >= 50:
            yield normalize(item, page_data)

        time.sleep(2.0)


def bootstrap_sample(sample_count: int = 12) -> bool:
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    session = requests.Session()
    items = parse_rss_feed(session)

    records = []
    doc_types_seen = set()

    for item in items:
        if len(records) >= sample_count:
            break

        url = item.get('url', '')
        if not url:
            continue

        # Try to get variety in document types
        print(f"  Fetching: {item.get('title', url)[:50]}...")

        page_data = fetch_document_page(url, session)

        text = page_data.get('text', '')
        if text and len(text) >= 100:
            record = normalize(item, page_data)
            records.append(record)

            # Save individual record
            filename = SAMPLE_DIR / f"record_{len(records):03d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            doc_type = page_data.get('doc_type', 'unknown')
            doc_types_seen.add(doc_type)

            print(f"    -> {len(record['text']):,} chars ({doc_type})")
        else:
            print(f"    -> Skipped (insufficient text: {len(text)} chars)")

        time.sleep(2.0)

    # Print summary
    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")
        print(f"Document types: {', '.join(sorted(doc_types_seen))}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get('text') or len(r['text']) < 100)
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have insufficient text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description=f"{SOURCE_ID} doctrine fetcher")
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument('--since', type=str,
                       help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)
        else:
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'updates':
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
