#!/usr/bin/env python3
"""
FR/ASN — French Nuclear Safety Authority (ASNR) Incident Notices

Fetches nuclear safety incident notices from the Autorité de sûreté nucléaire
et de radioprotection (ASNR) website via HTML scraping.

Covers ~3,100+ incident notices for nuclear installations with full text,
INES classifications, facility info, and publication dates.

Data source: https://reglementation-controle.asnr.fr
License: Open data (French public authority)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://reglementation-controle.asnr.fr"
LISTING_PATH = "/controle/actualites-du-controle/installations-nucleaires/avis-d-incident-des-installations-nucleaires/"
RATE_LIMIT_DELAY = 1.5

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (academic research; legal data collection)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
})


def parse_french_date(date_str: str) -> Optional[str]:
    """Parse French date 'DD/MM/YYYY' to ISO format."""
    if not date_str:
        return None
    match = re.search(r'(\d{2})/(\d{2})/(\d{4})', date_str)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"
    return None


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, 'html.parser')
    for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
        tag.decompose()
    text = soup.get_text(separator='\n')
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()


def fetch_page(url: str, params: dict = None) -> Optional[BeautifulSoup]:
    """Fetch a page and return parsed BeautifulSoup."""
    try:
        resp = SESSION.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, 'html.parser')
    except requests.RequestException as e:
        print(f"  [WARN] Failed to fetch {url}: {e}", file=sys.stderr)
        return None


def get_max_page(soup: BeautifulSoup) -> int:
    """Extract max page number from pagination."""
    pagination = soup.select_one('ul.pagination')
    if not pagination:
        return 1
    max_p = 1
    for link in pagination.select('a'):
        href = link.get('href', '')
        match = re.search(r'page=(\d+)', href)
        if match:
            max_p = max(max_p, int(match.group(1)))
    return max_p


def scrape_listing_page(page: int = 1) -> list[dict]:
    """Scrape one page of incident notice listings."""
    url = BASE_URL + LISTING_PATH
    soup = fetch_page(url, params={"page": page})
    if not soup:
        return []

    items = []
    # Find incident items — look for h3 links within listing
    for heading in soup.select('h3 a[href]'):
        href = heading.get('href', '')
        if '/avis-d-incident' not in href:
            continue

        title = heading.get_text(strip=True)
        if not title:
            continue

        # Build full URL
        full_url = href if href.startswith('http') else BASE_URL + href
        slug = href.rstrip('/').split('/')[-1]

        # Try to find date and facility from surrounding elements
        parent = heading.find_parent()
        # Walk up to find the container
        container = heading.find_parent('div') or heading.find_parent('li') or heading.find_parent('article')

        date_str = None
        facility = None
        description = None

        if container:
            text_content = container.get_text(separator='\n')
            # Look for date pattern "Publié le DD/MM/YYYY"
            date_match = re.search(r'Publi[ée]\s+le\s+(\d{2}/\d{2}/\d{4})', text_content)
            if date_match:
                date_str = parse_french_date(date_match.group(1))

            # Look for facility name (usually in a separate paragraph before title)
            paragraphs = container.find_all('p')
            for p in paragraphs:
                p_text = p.get_text(strip=True)
                if re.match(r'Publi[ée]\s+le', p_text):
                    continue
                if p_text and p_text != title and len(p_text) < 200:
                    if not facility:
                        facility = p_text
                    elif not description:
                        description = p_text

        items.append({
            'url': full_url,
            'slug': slug,
            'title': title,
            'date': date_str,
            'facility': facility,
            'description': description,
        })

    return items


def scrape_detail_page(url: str) -> dict:
    """Fetch detail page and extract full text and metadata."""
    soup = fetch_page(url)
    if not soup:
        return {'text': '', 'ines_level': None, 'facility': None}

    result = {'text': '', 'ines_level': None, 'facility': None}

    # Extract INES level
    page_text = soup.get_text()
    ines_match = re.search(r'[Nn]iveau\s+(\d)\s', page_text)
    if not ines_match:
        ines_match = re.search(r'INES\s*[:\s]+(\d)', page_text)
    if ines_match:
        result['ines_level'] = int(ines_match.group(1))

    # Extract main content
    # Try article tag first
    main = soup.select_one('article') or soup.select_one('main') or soup.select_one('div.content')
    if main:
        # Remove nav, breadcrumbs, share links, pagination, footer
        for tag in main.select('nav, .breadcrumb, .pagination, .share-links, .social-share, footer, header, aside, .sidebar'):
            tag.decompose()
        # Remove any INES scale legend/table (it's boilerplate)
        for tag in main.select('table'):
            tag.decompose()

        text = clean_html(str(main))
        # Remove common boilerplate
        text = re.sub(r'(?s)Échelle INES.*?$', '', text).strip()
        text = re.sub(r'(?s)Partager sur.*?\n', '', text).strip()
        result['text'] = text
    else:
        # Fallback: get body text
        body = soup.select_one('body')
        if body:
            for tag in body.select('nav, footer, header, aside, script, style'):
                tag.decompose()
            result['text'] = clean_html(str(body))

    return result


def normalize(raw: dict) -> dict:
    """Transform raw data into normalized schema."""
    slug = raw.get('slug', '')
    doc_id = f"asn-{slug}" if slug else f"asn-{hash(raw.get('title', ''))}"

    return {
        '_id': doc_id,
        '_source': 'FR/ASN',
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': raw.get('date'),
        'url': raw.get('url', ''),
        'facility': raw.get('facility', ''),
        'ines_level': raw.get('ines_level'),
        'description': raw.get('description', ''),
    }


def fetch_all(max_items: int = None) -> Generator[dict, None, None]:
    """Fetch all ASN incident notices."""
    count = 0
    print("Fetching ASN incident notices...", file=sys.stderr)

    # Get first page to determine pagination
    first_soup = fetch_page(BASE_URL + LISTING_PATH)
    if not first_soup:
        print("  [ERROR] Could not load listing page", file=sys.stderr)
        return
    max_page = get_max_page(first_soup)
    print(f"  Found {max_page} pages of incident notices", file=sys.stderr)

    for page in range(1, max_page + 1):
        if max_items and count >= max_items:
            return

        items = scrape_listing_page(page)
        if not items:
            print(f"  [WARN] No items on page {page}, stopping", file=sys.stderr)
            break

        for item in items:
            if max_items and count >= max_items:
                return

            time.sleep(RATE_LIMIT_DELAY)
            detail = scrape_detail_page(item['url'])
            item['text'] = detail['text']
            item['ines_level'] = detail['ines_level']
            if detail['facility'] and not item.get('facility'):
                item['facility'] = detail['facility']

            if item['text']:
                yield normalize(item)
                count += 1
                if count % 10 == 0:
                    print(f"  Fetched {count} incident notices...", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)

    print(f"  Total: {count} incident notices fetched", file=sys.stderr)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch incident notices published since a given date."""
    since_date = datetime.fromisoformat(since).date()
    print(f"Fetching ASN notices since {since_date}...", file=sys.stderr)

    for page in range(1, 300):
        items = scrape_listing_page(page)
        if not items:
            break

        all_older = True
        for item in items:
            if item.get('date'):
                item_date = datetime.fromisoformat(item['date']).date()
                if item_date < since_date:
                    continue
                all_older = False

            time.sleep(RATE_LIMIT_DELAY)
            detail = scrape_detail_page(item['url'])
            item['text'] = detail['text']
            item['ines_level'] = detail['ines_level']
            if detail['facility'] and not item.get('facility'):
                item['facility'] = detail['facility']

            if item['text']:
                yield normalize(item)

        if all_older:
            break
        time.sleep(RATE_LIMIT_DELAY)


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Download sample records for testing."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    for record in fetch_all(max_items=count):
        if not record.get('text'):
            continue

        filename = re.sub(r'[^\w\-]', '_', record['_id'])[:80] + '.json'
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        saved += 1
        print(f"  Saved {saved}/{count}: {record['title'][:60]}...", file=sys.stderr)

    print(f"\nSample complete: {saved} records saved to {sample_dir}", file=sys.stderr)
    return saved


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='FR/ASN Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Download sample records only')
    parser.add_argument('--count', type=int, default=15,
                        help='Number of sample records')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (ISO format)')
    parser.add_argument('--output', type=str, default='output',
                        help='Output directory')

    args = parser.parse_args()
    source_dir = Path(__file__).parent

    if args.command == 'bootstrap':
        if args.sample:
            sample_dir = source_dir / 'sample'
            bootstrap_sample(sample_dir, args.count)
        else:
            output_dir = source_dir / args.output
            output_dir.mkdir(parents=True, exist_ok=True)
            count = 0
            for record in fetch_all():
                filename = re.sub(r'[^\w\-]', '_', record['_id'])[:80] + '.json'
                with open(output_dir / filename, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
            print(f"Fetched {count} records to {output_dir}", file=sys.stderr)

    elif args.command == 'updates':
        if not args.since:
            print("--since is required for updates command", file=sys.stderr)
            sys.exit(1)
        for record in fetch_updates(args.since):
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))
