#!/usr/bin/env python3
"""
Hungary NAV Tax Guidance (Adózási kérdés) Data Fetcher

Extracts official tax guidance questions from nav.gov.hu.
These are authoritative NAV interpretations on tax law issues.

Strategy:
- Listing pages use Aurelia SPA but embed article metadata in data-content attrs
- Individual article content is available via /print/ prefix (server-rendered)
- Szabalyzok PDFs available via /pfile/file?path= endpoint

Data source: https://nav.gov.hu/ado/adozasi_kerdes
License: Public Domain (Hungarian government publication)
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Iterator, Optional, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

SOURCE_ID = "HU/NAV-TaxGuidance"
BASE_URL = "https://nav.gov.hu"
REQUEST_DELAY = 1.5

# Pagination parameter for adozasi_kerdes listing
LISTING_PATH = "/ado/adozasi_kerdes"
PAGE_PARAM = "$rppid0x1484150x112_pageNumber"
MAX_PAGES = 8

# Szabalyzok PDF sections
SZABALYZOK_SECTIONS = {
    "tajekoztatasok": "/szabalyzok/tajekoztatasok",
    "utmutatok": "/szabalyzok/utmutatok",
}


def curl_get(url: str, timeout: int = 45) -> Optional[str]:
    """Fetch URL content using curl."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout), '-H',
             'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)', url],
            capture_output=True, text=True, timeout=timeout + 10
        )
        if result.returncode == 0 and result.stdout:
            return result.stdout
        return None
    except Exception as e:
        print(f"  curl error for {url}: {e}")
        return None


def clean_html(html_content: str) -> str:
    """Strip HTML tags and clean up text."""
    text = re.sub(r'<br\s*/?>', '\n', html_content)
    text = re.sub(r'</?p[^>]*>', '\n', text)
    text = re.sub(r'</?li[^>]*>', '\n- ', text)
    text = re.sub(r'</?(?:ul|ol)[^>]*>', '\n', text)
    text = re.sub(r'</?h[1-6][^>]*>', '\n\n', text)
    text = re.sub(r'</?(?:table|tr)[^>]*>', '\n', text)
    text = re.sub(r'</?t[dh][^>]*>', ' | ', text)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    lines = [line.strip() for line in text.split('\n')]
    return '\n'.join(lines).strip()


def extract_articles_from_listing(html: str) -> List[Dict]:
    """Extract article metadata from data-content attributes."""
    articles = []
    matches = re.findall(r'data-content="([^"]+)"', html)
    for m in matches:
        decoded = unescape(m)
        try:
            obj = json.loads(decoded)
            if obj.get('@class') == 'hu.ponte.dokk.portal.content.Article':
                node = obj.get('node', {})
                slug = node.get('name', '')
                display_name = obj.get('displayName', '')
                mod_date = obj.get('modificationDate')
                if slug:
                    articles.append({
                        'slug': slug,
                        'title': display_name,
                        'modification_ts': mod_date,
                        'id': obj.get('id'),
                    })
        except (json.JSONDecodeError, KeyError):
            continue
    return articles


def fetch_all_article_slugs() -> List[Dict]:
    """Fetch all article metadata from paginated listing."""
    all_articles = []
    seen_slugs = set()

    for page in range(1, MAX_PAGES + 1):
        if page == 1:
            url = f"{BASE_URL}{LISTING_PATH}"
        else:
            url = f"{BASE_URL}{LISTING_PATH}/{PAGE_PARAM}/{page}"

        print(f"  Fetching listing page {page}...")
        html = curl_get(url)
        if not html:
            break

        articles = extract_articles_from_listing(html)
        new_count = 0
        for art in articles:
            if art['slug'] not in seen_slugs:
                seen_slugs.add(art['slug'])
                all_articles.append(art)
                new_count += 1

        print(f"    Found {new_count} new articles (total: {len(all_articles)})")
        if new_count == 0 and page > 2:
            break

        time.sleep(REQUEST_DELAY)

    return all_articles


def fetch_article_content(slug: str) -> Optional[Dict]:
    """Fetch full text of an article using the /print/ prefix."""
    url = f"{BASE_URL}/print/ado/adozasi_kerdes/{slug}"
    html = curl_get(url)
    if not html:
        return None

    # Extract from <article> tag
    article_match = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)
    if not article_match:
        return None

    article_html = article_match.group(1)

    # Extract title
    title_match = re.search(r'<h1[^>]*>(.*?)</h1>', article_html, re.DOTALL)
    title = clean_html(title_match.group(1)) if title_match else slug

    # Extract date
    date_match = re.search(r'class="article-date"[^>]*>.*?<span>(.*?)</span>', article_html, re.DOTALL)
    date_str = None
    if date_match:
        raw_date = date_match.group(1).strip()
        # Parse Hungarian date format: "2026. jan. 28."
        date_str = parse_hungarian_date(raw_date)

    # Extract body
    body_match = re.search(r'class="article-body"[^>]*>(.*?)(?:</div>\s*</article>|$)', article_html, re.DOTALL)
    if not body_match:
        # Try broader extraction - everything after the header
        body_match = re.search(r'</header>(.*)', article_html, re.DOTALL)

    if not body_match:
        return None

    text = clean_html(body_match.group(1))
    if len(text) < 50:
        return None

    return {
        'title': title,
        'text': text,
        'date': date_str,
    }


def parse_hungarian_date(raw: str) -> Optional[str]:
    """Parse Hungarian date like '2026. jan. 28.' to ISO format."""
    months = {
        'jan': '01', 'feb': '02', 'márc': '03', 'ápr': '04',
        'máj': '05', 'jún': '06', 'júl': '07', 'aug': '08',
        'szept': '09', 'okt': '10', 'nov': '11', 'dec': '12',
    }
    m = re.search(r'(\d{4})\.\s*(\w+)\.?\s*(\d{1,2})\.?', raw)
    if m:
        year = m.group(1)
        month_str = m.group(2).lower().rstrip('.')
        day = m.group(3)
        for key, val in months.items():
            if month_str.startswith(key):
                return f"{year}-{val}-{int(day):02d}"
    # Try just year
    m = re.search(r'(\d{4})', raw)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def extract_year_number(title: str, slug: str) -> tuple:
    """Extract year and question number from title or slug."""
    # Pattern: "2026/1." or "2025/10."
    m = re.search(r'(\d{4})/(\d+)', title)
    if m:
        return m.group(1), m.group(2)
    # From slug: "20261." or "2025_10_"
    m = re.search(r'^(\d{4})[\._-]?(\d+)', slug)
    if m:
        return m.group(1), m.group(2)
    return None, None


def normalize_article(slug: str, meta: Dict, content: Dict) -> Dict:
    """Normalize an article to standard schema."""
    title = content.get('title') or meta.get('title') or slug
    year, number = extract_year_number(title, slug)

    doc_id = f"HU_NAV_TG_{year or 'unknown'}_{number or slug[:40]}"
    doc_id = re.sub(r'[^\w-]', '_', doc_id)

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': content['text'],
        'date': content.get('date'),
        'url': f"{BASE_URL}/ado/adozasi_kerdes/{slug}",
        'year': year,
        'question_number': number,
        'section': 'adozasi_kerdes',
        'language': 'hu',
    }


def fetch_szabalyzok_pdfs(section: str, path: str, max_items: int = 0) -> Iterator[Dict]:
    """Fetch PDF documents from szabalyzok sections."""
    print(f"\n--- Fetching szabalyzok/{section} ---")
    # The listing page content is in data-content attrs too
    html = curl_get(f"{BASE_URL}{path}")
    if not html:
        print("  Failed to fetch listing")
        return

    # Look for pfile links in the page
    pdf_links = re.findall(
        r'href="(/pfile/file\?path=/szabalyzok/[^"]+)"', html
    )
    # Also look in data-content attributes for article slugs
    articles = extract_articles_from_listing(html)

    # If we found articles, fetch their print versions for PDF links
    if articles and not pdf_links:
        for art in articles[:max_items if max_items else len(articles)]:
            print_url = f"{BASE_URL}/print/szabalyzok/{section}/{art['slug']}"
            print_html = curl_get(print_url)
            if print_html:
                found = re.findall(r'href="(/pfile/file\?path=[^"]+\.pdf)"', print_html)
                pdf_links.extend(found)
            time.sleep(REQUEST_DELAY)

    pdf_links = list(dict.fromkeys(pdf_links))  # dedupe preserving order
    print(f"  Found {len(pdf_links)} PDF links")

    count = 0
    for pdf_path in pdf_links:
        if max_items and count >= max_items:
            break

        pdf_url = f"{BASE_URL}{pdf_path}"
        filename = pdf_path.split('/')[-1]
        doc_id = f"HU_NAV_TG_{section}_{filename[:60]}"
        doc_id = re.sub(r'[^\w-]', '_', doc_id)

        title = filename.replace('.pdf', '').replace('-', ' ').replace('_', ' ')
        title = re.sub(r'^\d+[._-]+', '', title)
        title = re.sub(r'\s+', ' ', title).strip()

        text = extract_pdf_markdown(
            source="HU/NAV-TaxGuidance",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
            force=True,
        ) or ""

        if len(text) < 50:
            print(f"  SKIP (no text): {filename[:60]}")
            time.sleep(REQUEST_DELAY)
            continue

        yield {
            '_id': doc_id,
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': text,
            'date': None,
            'url': pdf_url,
            'section': f"szabalyzok/{section}",
            'category': section,
            'language': 'hu',
        }
        count += 1
        print(f"  [{count}] {title[:60]}... ({len(text)} chars)")
        time.sleep(REQUEST_DELAY)


def fetch_all() -> Iterator[Dict]:
    """Yield all NAV tax guidance documents."""
    # 1. Adozasi kerdes (main tax guidance questions)
    print("\n=== Fetching Adózási kérdés articles ===")
    articles = fetch_all_article_slugs()
    print(f"Total articles found: {len(articles)}")

    for i, meta in enumerate(articles):
        print(f"\n  [{i+1}/{len(articles)}] {meta['title'][:60]}...")
        content = fetch_article_content(meta['slug'])
        if content:
            yield normalize_article(meta['slug'], meta, content)
        else:
            print(f"    SKIP (no content)")
        time.sleep(REQUEST_DELAY)

    # 2. Szabalyzok PDFs
    for section, path in SZABALYZOK_SECTIONS.items():
        yield from fetch_szabalyzok_pdfs(section, path)


def bootstrap_sample() -> List[Dict]:
    """Fetch sample records."""
    samples = []

    # Fetch all listing slugs
    print("\n=== Fetching article listing ===")
    articles = fetch_all_article_slugs()
    print(f"Found {len(articles)} articles total")

    # Fetch content for up to 12 articles
    for meta in articles[:12]:
        print(f"\n  Fetching: {meta['title'][:60]}...")
        content = fetch_article_content(meta['slug'])
        if content:
            record = normalize_article(meta['slug'], meta, content)
            samples.append(record)
            print(f"    OK: {len(content['text'])} chars")
        else:
            print(f"    SKIP (no content)")
        time.sleep(REQUEST_DELAY)

    return samples


def main():
    parser = argparse.ArgumentParser(description='HU/NAV-TaxGuidance Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only')
    parser.add_argument('--since', type=str, help='Fetch updates since date')
    parser.add_argument('--output', type=str, help='Output directory')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    output_dir = args.output or str(Path(__file__).parent / 'sample')
    os.makedirs(output_dir, exist_ok=True)

    if args.command == 'bootstrap':
        if args.sample:
            records = bootstrap_sample()
        elif args.full:
            records = list(fetch_all())
        else:
            records = bootstrap_sample()

        saved = 0
        for record in records:
            filename = re.sub(r'[^\w-]', '_', record['_id'])[:100] + '.json'
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            saved += 1

        print(f"\n=== Summary ===")
        print(f"Records saved: {saved}")
        print(f"Output directory: {output_dir}")
        if records:
            texts = [r.get('text', '') for r in records]
            avg_len = sum(len(t) for t in texts) / len(texts)
            print(f"Average text length: {avg_len:.0f} chars")
            non_empty = sum(1 for t in texts if len(t) > 100)
            print(f"Records with substantial text: {non_empty}/{len(records)}")


if __name__ == '__main__':
    main()
