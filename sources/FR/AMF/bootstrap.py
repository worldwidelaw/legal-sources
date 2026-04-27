#!/usr/bin/env python3
"""
French Financial Markets Authority (AMF) Data Fetcher

Fetches sanctions decisions via REST API and doctrine via sitemap scraping.
Full text extracted from PDF documents using pdfplumber.

Data sources:
- Sanctions: REST API at /fr/rest/listing_sanction/... (449+ records)
- Doctrine: Sitemap scraping at /sitemap.xml (343+ records) - replaces RSS feed

License: Public regulatory documents
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Generator, Optional
from xml.etree import ElementTree as ET

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Constants
BASE_URL = "https://www.amf-france.org"
# REST API for sanctions - returns JSON with all historical records
# Categories: 91,184,183,325,90,461,89,242,86,462,181 cover all sanction types
REST_SANCTIONS = f"{BASE_URL}/fr/rest/listing_sanction/91,184,183,325,90,461,89,242,86,462,181/all/all"
# Sitemap for doctrine - contains 343+ doctrine document URLs
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
RATE_LIMIT_DELAY = 1.5  # seconds between requests


def clean_text(text: str) -> str:
    """Clean up extracted text."""
    if not text:
        return ""
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


def clean_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = unescape(text)
    # Clean up whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_pdf_text(pdf_url: str) -> Optional[str]:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="FR/AMF",
        source_id="",
        pdf_url=pdf_url,
        table="case_law",
    ) or ""

def find_pdf_url(page_url: str) -> Optional[str]:
    """Find PDF download URL from an AMF page."""
    try:
        # Clean URL of RSS tracking params
        clean_url = page_url.split('#')[0]

        response = requests.get(clean_url, timeout=30)
        response.raise_for_status()

        # Look for PDF links in the page
        pdf_pattern = r'/sites/institutionnel/files/[^"\']+\.pdf'
        matches = re.findall(pdf_pattern, response.text)

        if matches:
            # Prefer 'private' folder PDFs (full documents) over 'pdf' folder (summaries)
            private_pdfs = [m for m in matches if '/private/' in m]
            if private_pdfs:
                return BASE_URL + private_pdfs[0]
            return BASE_URL + matches[0]

        return None

    except Exception as e:
        print(f"Error finding PDF at {page_url}: {e}", file=sys.stderr)
        return None


def fetch_doctrine_urls_from_sitemap() -> list[str]:
    """Fetch all doctrine document URLs from the sitemap."""
    response = requests.get(SITEMAP_URL, timeout=60)
    response.raise_for_status()

    # Parse sitemap XML
    root = ET.fromstring(response.content)
    namespace = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

    urls = []
    for url_elem in root.findall('.//sm:url/sm:loc', namespace):
        url = url_elem.text
        if url and '/fr/reglementation/doctrine/doc-' in url:
            urls.append(url)

    print(f"Found {len(urls)} doctrine URLs in sitemap", file=sys.stderr)
    return urls


def scrape_doctrine_page(url: str) -> Optional[dict]:
    """Scrape metadata and PDF URL from a doctrine page."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        html = response.text

        # Extract title from og:title or title tag
        title_match = re.search(r'<meta property="og:title" content="([^"]+)"', html)
        title = title_match.group(1) if title_match else ''

        # Extract publication date from data-published-date
        date_match = re.search(r'data-published-date="(\d+)"', html)
        pub_date = None
        if date_match:
            date_str = date_match.group(1)
            # Format: YYYYMMDDHHMI
            if len(date_str) >= 8:
                try:
                    pub_date = datetime.strptime(date_str[:8], '%Y%m%d').replace(tzinfo=timezone.utc).isoformat()
                except ValueError:
                    pass

        # Find PDF URLs - prefer private folder (full document)
        pdf_matches = re.findall(r'/sites/institutionnel/files/[^"]+\.pdf', html)
        pdf_url = None
        if pdf_matches:
            # Prefer private folder PDFs (full documents)
            private_pdfs = [p for p in pdf_matches if '/private/' in p]
            if private_pdfs:
                pdf_url = BASE_URL + private_pdfs[0]
            else:
                pdf_url = BASE_URL + pdf_matches[0]

        # Extract doc ID from URL
        doc_id_match = re.search(r'/doc-(\d{4}-\d+)$', url)
        doc_id = f"DOC-{doc_id_match.group(1).upper()}" if doc_id_match else url.split('/')[-1]

        return {
            'title': clean_html(title),
            'link': url,
            'pub_date': pub_date,
            'pdf_url': pdf_url,
            'doc_id': doc_id,
        }

    except Exception as e:
        print(f"Error scraping {url}: {e}", file=sys.stderr)
        return None


def fetch_sanctions_rest() -> list[dict]:
    """Fetch sanctions from REST API - returns all historical records."""
    timestamp = int(time.time() * 1000)
    url = f"{REST_SANCTIONS}?t={timestamp}&_={timestamp - 200}"

    print(f"Fetching sanctions from REST API...", file=sys.stderr)
    response = requests.get(url, timeout=60, headers={'Accept': 'application/json'})
    response.raise_for_status()

    data = response.json()
    items = []

    for record in data.get('data', []):
        infos = record.get('infos', {})
        download = infos.get('download', {})
        sanction = download.get('sanction', {})
        links = sanction.get('links', {})

        # Extract PDF URL
        pdf_path = links.get('url', '')
        pdf_url = BASE_URL + pdf_path if pdf_path else ''

        # Parse date (Unix timestamp)
        date_ts = record.get('date', '')
        try:
            date_iso = datetime.fromtimestamp(int(date_ts), tz=timezone.utc).isoformat() if date_ts else None
        except (ValueError, TypeError):
            date_iso = None

        items.append({
            'title': infos.get('title', ''),
            'theme': record.get('theme', ''),
            'entities': clean_html(infos.get('text_egard', '')),
            'summary': clean_html(infos.get('text', '')),
            'link': infos.get('link', {}).get('url', ''),
            'pdf_url': pdf_url,
            'date': date_iso,
            'recours': infos.get('recours', ''),
            'source': 'rest_api',
        })

    print(f"Found {len(items)} sanctions from REST API", file=sys.stderr)
    return items


def extract_document_id(title: str, link: str) -> str:
    """Extract document ID from title or link."""
    # Try to extract SAN-YYYY-NN or DOC-YYYY-NN pattern from title
    for pattern in [r'(SAN-\d{4}-\d+)', r'(DOC-\d{4}-\d+)']:
        match = re.search(pattern, title, re.IGNORECASE)
        if match:
            return match.group(1).upper()

    # Clean URL of tracking params before extracting ID
    clean_url = link.split('#')[0].split('?')[0]

    # Try to extract doc ID from URL path (e.g., /doctrine/doc-2019-05)
    doc_match = re.search(r'/(doc-\d{4}-\d+)$', clean_url, re.IGNORECASE)
    if doc_match:
        return doc_match.group(1).upper()

    # Try san-YYYY-NN
    san_match = re.search(r'/(san-\d{4}-\d+)$', clean_url, re.IGNORECASE)
    if san_match:
        return san_match.group(1).upper()

    # Fall back to last path segment
    url_match = re.search(r'/([^/]+)$', clean_url)
    if url_match:
        return url_match.group(1)

    # Last resort: hash of title
    return f"amf-{abs(hash(title)) % 10**8}"


def parse_date(date_str: str) -> Optional[str]:
    """Parse RSS date to ISO format."""
    if not date_str:
        return None

    try:
        # RFC 822 format: Thu, 22 Jan 2026 11:57:29 +0100
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        return dt.isoformat()
    except Exception:
        return None


def determine_document_type(title: str, link: str) -> str:
    """Determine the type of AMF document."""
    title_lower = title.lower()
    link_lower = link.lower()

    if 'san-' in title_lower or 'sanction' in link_lower:
        return 'sanction_decision'
    if 'transaction' in title_lower or 'homologuée' in title_lower:
        return 'approved_transaction'
    if 'doc-' in title_lower or 'doctrine' in link_lower:
        return 'doctrine'
    if 'règles-professionnelles' in link_lower or 'regles-professionnelles' in link_lower:
        return 'professional_rules'
    if 'arrêté' in title_lower or 'arrete' in link_lower:
        return 'arrete_homologation'

    return 'other'


def normalize_sanction(raw: dict) -> dict:
    """Transform raw REST API sanction record into normalized schema."""
    doc_id = extract_document_id(raw['title'], raw.get('link', ''))

    return {
        '_id': f"FR/AMF/{doc_id}",
        '_source': 'FR/AMF',
        '_type': 'case_law',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'document_id': doc_id,
        'title': raw['title'],
        'date': raw.get('date'),
        'document_type': 'sanction_decision',
        'theme': raw.get('theme', ''),
        'entities': raw.get('entities', ''),
        'summary': raw.get('summary', ''),
        'recours': raw.get('recours', ''),
        'url': raw.get('link', ''),
        'text': raw.get('text', ''),
        'pdf_url': raw.get('pdf_url', ''),
    }


def normalize_doctrine(raw: dict) -> dict:
    """Transform raw doctrine item into normalized schema."""
    doc_id = raw.get('doc_id') or extract_document_id(raw['title'], raw['link'])
    doc_type = determine_document_type(raw['title'], raw['link'])

    # Handle both ISO date strings and RFC 822 date strings
    pub_date = raw.get('pub_date', '')
    if pub_date and not pub_date.startswith('20'):
        # It's an RFC 822 date, parse it
        pub_date = parse_date(pub_date)

    return {
        '_id': f"FR/AMF/{doc_id}",
        '_source': 'FR/AMF',
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'document_id': doc_id,
        'title': raw['title'],
        'date': pub_date,
        'document_type': doc_type,
        'description': raw.get('description', ''),
        'url': raw['link'].split('#')[0],  # Clean URL
        'text': raw.get('text', ''),
        'pdf_url': raw.get('pdf_url', ''),
    }


def fetch_all() -> Generator[dict, None, None]:
    """Fetch all available AMF documents from REST API and sitemap scraping."""
    seen_ids = set()

    # 1. Fetch sanctions from REST API (449+ records)
    sanctions = fetch_sanctions_rest()
    for item in sanctions:
        doc_id = extract_document_id(item['title'], item.get('link', ''))

        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)

        # Extract PDF text if URL available
        pdf_url = item.get('pdf_url', '')
        if pdf_url:
            text = extract_pdf_text(pdf_url)
            if text:
                item['text'] = clean_text(text)
            time.sleep(RATE_LIMIT_DELAY)

        yield normalize_sanction(item)

    # 2. Fetch doctrine from sitemap (343+ records)
    print(f"Fetching doctrine from sitemap...", file=sys.stderr)
    doctrine_urls = fetch_doctrine_urls_from_sitemap()

    for url in doctrine_urls:
        time.sleep(RATE_LIMIT_DELAY)

        item = scrape_doctrine_page(url)
        if not item:
            continue

        doc_id = item.get('doc_id', '')
        if doc_id in seen_ids:
            continue
        seen_ids.add(doc_id)

        # Extract PDF text
        pdf_url = item.get('pdf_url')
        if pdf_url:
            time.sleep(RATE_LIMIT_DELAY)
            text = extract_pdf_text(pdf_url)
            if text:
                item['text'] = clean_text(text)

        yield normalize_doctrine(item)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    for doc in fetch_all():
        doc_date = doc.get('date')
        if doc_date:
            try:
                doc_dt = datetime.fromisoformat(doc_date.replace('Z', '+00:00'))
                if doc_dt >= since:
                    yield doc
            except ValueError:
                yield doc
        else:
            yield doc


def bootstrap_sample(limit: int = 15) -> None:
    """Fetch sample records for testing."""
    sample_dir = Path(__file__).parent / 'sample'
    sample_dir.mkdir(exist_ok=True)

    # Clear existing samples
    for f in sample_dir.glob('*.json'):
        f.unlink()

    count = 0
    sanctions_count = 0
    doctrine_count = 0
    max_sanctions = (limit * 2) // 3  # More sanctions since they're the main content
    max_doctrine = limit - max_sanctions

    print("Fetching AMF sample data...", file=sys.stderr)

    # 1. Fetch sanctions from REST API
    print("Fetching sanctions from REST API...", file=sys.stderr)
    sanctions = fetch_sanctions_rest()

    for item in sanctions:
        if sanctions_count >= max_sanctions:
            break

        doc_id = extract_document_id(item['title'], item.get('link', ''))

        # Extract PDF text
        pdf_url = item.get('pdf_url', '')
        if not pdf_url:
            print(f"Skipping {doc_id} - no PDF URL", file=sys.stderr)
            continue

        time.sleep(RATE_LIMIT_DELAY)
        text = extract_pdf_text(pdf_url)

        if not text:
            print(f"Skipping {doc_id} - no text extracted", file=sys.stderr)
            continue

        item['text'] = clean_text(text)

        # Normalize
        doc = normalize_sanction(item)

        # Save to sample directory
        filename = f"{doc['document_id'].replace('/', '-')}.json"
        filepath = sample_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        text_len = len(doc.get('text', ''))
        print(f"Saved {filename} ({text_len:,} chars) - sanction", file=sys.stderr)
        sanctions_count += 1
        count += 1

    # 2. Fetch doctrine from sitemap
    if doctrine_count < max_doctrine:
        print("Fetching doctrine from sitemap...", file=sys.stderr)
        doctrine_urls = fetch_doctrine_urls_from_sitemap()

        for url in doctrine_urls:
            if doctrine_count >= max_doctrine:
                break

            time.sleep(RATE_LIMIT_DELAY)
            item = scrape_doctrine_page(url)
            if not item:
                print(f"Skipping {url} - could not scrape page", file=sys.stderr)
                continue

            doc_id = item.get('doc_id', '')
            pdf_url = item.get('pdf_url')

            if not pdf_url:
                print(f"Skipping {doc_id} - no PDF found", file=sys.stderr)
                continue

            time.sleep(RATE_LIMIT_DELAY)
            text = extract_pdf_text(pdf_url)
            if not text:
                print(f"Skipping {doc_id} - no text extracted", file=sys.stderr)
                continue

            item['text'] = clean_text(text)

            # Normalize
            doc = normalize_doctrine(item)

            # Save to sample directory
            filename = f"{doc['document_id'].replace('/', '-')}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

            text_len = len(doc.get('text', ''))
            print(f"Saved {filename} ({text_len:,} chars) - doctrine", file=sys.stderr)
            doctrine_count += 1
            count += 1

    print(f"\nSaved {count} sample records to {sample_dir}", file=sys.stderr)
    print(f"  - Sanctions: {sanctions_count}", file=sys.stderr)
    print(f"  - Doctrine: {doctrine_count}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description='AMF Data Fetcher')
    subparsers = parser.add_subparsers(dest='command')

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser('bootstrap', help='Fetch sample data')
    bootstrap_parser.add_argument('--sample', action='store_true', help='Fetch sample records')
    bootstrap_parser.add_argument('--limit', type=int, default=15, help='Number of records to fetch')

    # List command
    list_parser = subparsers.add_parser('list', help='List available documents')
    list_parser.add_argument('--source', choices=['sanctions', 'doctrine', 'all'], default='all')
    list_parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(args.limit)
        else:
            print("Use --sample to fetch sample records", file=sys.stderr)
    elif args.command == 'list':
        if args.source in ['sanctions', 'all']:
            sanctions = fetch_sanctions_rest()
            print(f"\nSanctions ({len(sanctions)} items from REST API):")
            for item in sanctions[:5]:
                print(f"  - {item['title']}: {item.get('entities', '')[:60]}...")

        if args.source in ['doctrine', 'all']:
            urls = fetch_doctrine_urls_from_sitemap()
            print(f"\nDoctrine ({len(urls)} URLs from sitemap):")
            for url in urls[:5]:
                print(f"  - {url}")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
