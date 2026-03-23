#!/usr/bin/env python3
"""
ANSM (Agence nationale de sécurité du médicament) Data Fetcher

Fetches regulatory decisions, safety notices, and official publications from
the French medicines agency via HTML scraping of ansm.sante.fr.

Covers:
- Informations de sécurité (safety notices for medical products)
- Regulatory decisions (injonctions, sanctions, AMM decisions, etc.)

Data source: https://ansm.sante.fr
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

# Constants
BASE_URL = "https://ansm.sante.fr"
RATE_LIMIT_DELAY = 1.5

# Decision category IDs to scrape from /actualites/
DECISION_CATEGORIES = {
    19: "Injonctions",
    18: "Décisions - Médicaments",
    94: "Décisions - DM & DMDIV",
    95: "Décisions - Autres produits",
    69: "Sanctions financières",
    45: "Retraits de lots",
    13: "AMM",
    43: "AMM - Autorisations/Modifications",
    44: "AMM - Suspensions/Retraits",
    70: "Bonnes pratiques / Avis de non conformité",
    80: "Autorisations / Agréments",
    108: "Accès dérogatoire",
    61: "Recommandations",
    93: "Classement substances vénéneuses",
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "WorldWideLaw/1.0 (academic research; legal data collection)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
})


def parse_french_date(date_str: str) -> Optional[str]:
    """Parse French date format 'PUBLIE LE DD/MM/YYYY' to ISO format."""
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
    # Remove script and style elements
    for tag in soup(['script', 'style', 'nav', 'footer', 'header']):
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
    pagination = soup.select_one('div.pagination')
    if not pagination:
        return 1
    # Check for last page link
    last_link = pagination.select_one('span.last a')
    if last_link and last_link.get('href'):
        match = re.search(r'page=(\d+)', last_link['href'])
        if match:
            return int(match.group(1))
    # Check all page links
    max_p = 1
    for link in pagination.select('span.page a'):
        href = link.get('href', '')
        match = re.search(r'page=(\d+)', href)
        if match:
            max_p = max(max_p, int(match.group(1)))
    current = pagination.select_one('span.current')
    if current and current.text.strip().isdigit():
        max_p = max(max_p, int(current.text.strip()))
    return max_p


def scrape_security_listing(page: int = 1) -> list[dict]:
    """Scrape one page of security notices listing."""
    url = f"{BASE_URL}/informations-de-securite/"
    soup = fetch_page(url, params={"page": page})
    if not soup:
        return []

    items = []
    for article in soup.select('article.article-item'):
        link = article.select_one('a[href]')
        if not link:
            continue

        href = link.get('href', '')
        if not href.startswith('/'):
            continue

        title_el = link.select_one('span.article-title')
        date_el = link.select_one('span.article-date')
        cat_el = link.select_one('span.article-category')
        product_el = link.select_one('span.article-health-product')
        desc_el = link.select_one('span.article-content')

        items.append({
            'url': BASE_URL + href,
            'slug': href.split('/')[-1],
            'title': title_el.get_text(strip=True) if title_el else '',
            'date': parse_french_date(date_el.get_text(strip=True) if date_el else ''),
            'category': cat_el.get_text(strip=True) if cat_el else '',
            'product_type': product_el.get_text(strip=True) if product_el else '',
            'description': desc_el.get_text(strip=True) if desc_el else '',
            'document_type': 'information_securite',
        })

    return items


def scrape_actualites_listing(category_id: int, page: int = 1) -> list[dict]:
    """Scrape one page of actualites for a specific category."""
    url = f"{BASE_URL}/actualites/"
    params = {"filter[categories][]": category_id, "page": page}
    soup = fetch_page(url, params=params)
    if not soup:
        return []

    items = []
    for link in soup.select('a.news-item'):
        href = link.get('href', '')
        if not href.startswith('/'):
            continue

        title_el = link.select_one('span.ni-text')
        date_el = link.select_one('div.ni-date')
        cat_el = link.select_one('span.nif-l')
        subcat_el = link.select_one('span.nif-r')

        items.append({
            'url': BASE_URL + href,
            'slug': href.split('/')[-1],
            'title': title_el.get_text(strip=True) if title_el else '',
            'date': parse_french_date(date_el.get_text(strip=True) if date_el else ''),
            'category': cat_el.get_text(strip=True) if cat_el else '',
            'subcategory': subcat_el.get_text(strip=True) if subcat_el else '',
            'document_type': 'decision',
        })

    return items


def scrape_detail_page(url: str) -> str:
    """Fetch detail page and extract full text content."""
    soup = fetch_page(url)
    if not soup:
        return ""

    # Try security notice format
    content_divs = soup.select('div.wysiwyg-content.zoom-area')
    if content_divs:
        texts = []
        for div in content_divs:
            text = clean_html(str(div))
            if text:
                texts.append(text)
        if texts:
            return '\n\n'.join(texts)

    # Try actualites format - look for main content area
    main_content = soup.select_one('div.content') or soup.select_one('article') or soup.select_one('main')
    if main_content:
        # Remove navigation, sidebar, etc.
        for tag in main_content.select('nav, .breadcrumb, .pagination, .sidebar, .share-links, footer'):
            tag.decompose()
        text = clean_html(str(main_content))
        if text:
            return text

    return ""


def normalize(raw: dict) -> dict:
    """Transform raw ANSM data into normalized schema."""
    slug = raw.get('slug', '')
    doc_type = raw.get('document_type', 'doctrine')

    doc_id = f"ansm-{slug}" if slug else f"ansm-{hash(raw.get('title', ''))}"

    return {
        '_id': doc_id,
        '_source': 'FR/ANSM',
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': raw.get('title', ''),
        'text': raw.get('text', ''),
        'date': raw.get('date'),
        'url': raw.get('url', ''),
        'category': raw.get('category', ''),
        'subcategory': raw.get('subcategory', ''),
        'product_type': raw.get('product_type', ''),
        'document_type': doc_type,
        'description': raw.get('description', ''),
    }


def fetch_all(max_items: int = None) -> Generator[dict, None, None]:
    """Fetch all ANSM documents."""
    count = 0

    # 1. Fetch security notices
    print("Fetching security notices...", file=sys.stderr)
    first_page_soup = fetch_page(f"{BASE_URL}/informations-de-securite/")
    max_page = get_max_page(first_page_soup) if first_page_soup else 1
    print(f"  Found {max_page} pages of security notices", file=sys.stderr)

    for page in range(1, max_page + 1):
        if max_items and count >= max_items:
            return
        items = scrape_security_listing(page)
        if not items:
            break
        for item in items:
            if max_items and count >= max_items:
                return
            # Fetch full text
            time.sleep(RATE_LIMIT_DELAY)
            item['text'] = scrape_detail_page(item['url'])
            if item['text']:
                yield normalize(item)
                count += 1
                if count % 10 == 0:
                    print(f"  Fetched {count} security notices...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

    # 2. Fetch decisions by category
    for cat_id, cat_name in DECISION_CATEGORIES.items():
        if max_items and count >= max_items:
            return
        print(f"Fetching decisions: {cat_name} (ID={cat_id})...", file=sys.stderr)

        first_soup = fetch_page(f"{BASE_URL}/actualites/",
                                params={"filter[categories][]": cat_id})
        cat_max_page = get_max_page(first_soup) if first_soup else 1
        print(f"  Found {cat_max_page} pages", file=sys.stderr)

        for page in range(1, cat_max_page + 1):
            if max_items and count >= max_items:
                return
            items = scrape_actualites_listing(cat_id, page)
            if not items:
                break
            for item in items:
                if max_items and count >= max_items:
                    return
                item['subcategory'] = item.get('subcategory', '') or cat_name
                time.sleep(RATE_LIMIT_DELAY)
                item['text'] = scrape_detail_page(item['url'])
                if item['text']:
                    yield normalize(item)
                    count += 1
                    if count % 10 == 0:
                        print(f"  Fetched {count} total documents...", file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    since_date = datetime.fromisoformat(since).date() if since else None

    for doc in fetch_all():
        if since_date and doc.get('date'):
            try:
                doc_date = datetime.fromisoformat(doc['date']).date()
                if doc_date < since_date:
                    continue
            except (ValueError, TypeError):
                pass
        yield doc


def bootstrap(sample: bool = False):
    """Bootstrap the data source with sample or full data."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    max_items = 15 if sample else None
    count = 0

    for doc in fetch_all(max_items=max_items):
        count += 1
        filename = re.sub(r'[^\w\-]', '_', doc['_id'])[:100] + '.json'
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        print(f"  [{count}] {doc['title'][:80]}", file=sys.stderr)

    print(f"\nTotal: {count} documents saved to {sample_dir}", file=sys.stderr)
    if count == 0:
        print("[ERROR] No records written!", file=sys.stderr)
        sys.exit(1)

    # Validate
    has_text = 0
    for f in sample_dir.glob('*.json'):
        with open(f) as fh:
            rec = json.load(fh)
            if rec.get('text') and len(rec['text']) > 50:
                has_text += 1
    print(f"Records with full text: {has_text}/{count}", file=sys.stderr)
    if has_text < count * 0.8:
        print("[WARN] Less than 80% of records have full text!", file=sys.stderr)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ANSM Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch only sample data (15 records)')
    parser.add_argument('--since', type=str, default=None,
                        help='Fetch updates since date (ISO format)')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        bootstrap(sample=args.sample)
    elif args.command == 'updates':
        if not args.since:
            print("Error: --since required for updates", file=sys.stderr)
            sys.exit(1)
        for doc in fetch_updates(args.since):
            print(json.dumps(doc, ensure_ascii=False))
