#!/usr/bin/env python3
"""
Irish Revenue Commissioners Data Fetcher

Extracts Tax and Duty Manuals (TDMs) from revenue.ie.
Recursively crawls TDM category/part index pages to find PDF documents,
downloads current versions, and extracts full text.

Data source: https://www.revenue.ie
License: Public Domain
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional, Dict, List, Set

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


SOURCE_ID = "IE/Revenue"
BASE_URL = "https://www.revenue.ie"
TDM_ROOT = "/en/tax-professionals/tdm/index.aspx"
REQUEST_DELAY = 0.8


def curl_get(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL content using curl."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout), '-H',
             'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)', url],
            capture_output=True, text=True, timeout=timeout + 5
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except Exception as e:
        print(f"  curl error: {e}")
        return None


def curl_download(url: str, output_path: str, timeout: int = 60) -> bool:
    """Download a file using curl."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout), '-H',
             'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
             '-o', output_path, url],
            capture_output=True, text=True, timeout=timeout + 5
        )
        return result.returncode == 0 and os.path.getsize(output_path) > 0
    except Exception:
        return False


def extract_pdf_text(pdf_path: str) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="IE/Revenue",
        source_id="",
        pdf_bytes=pdf_path,
        table="doctrine",
    ) or ""

def crawl_index_page(path: str) -> tuple:
    """Crawl an index page and return (sub_pages, pdf_links)."""
    url = f"{BASE_URL}{path}"
    html = curl_get(url)
    if not html:
        return [], []

    all_links = re.findall(r'href="([^"]+)"', html)
    tdm_links = [l for l in all_links if '/tdm/' in l]

    sub_pages = []
    pdf_links = []
    for link in tdm_links:
        if link.endswith('index.aspx') and link != path:
            sub_pages.append(link)
        elif link.endswith('.pdf'):
            # Only current versions (no timestamp like -20230531121115)
            if not re.search(r'-\d{14}', link):
                pdf_links.append(link)

    return list(set(sub_pages)), list(set(pdf_links))


def crawl_all_tdm_pdfs(max_depth: int = 3) -> List[Dict]:
    """Recursively crawl all TDM index pages to find PDF links."""
    # Start from TDM root
    print("Crawling TDM index pages...")
    root_html = curl_get(f"{BASE_URL}{TDM_ROOT}")
    if not root_html:
        return []

    categories = re.findall(
        r'href="(/en/tax-professionals/tdm/[^"]+/index\.aspx)"', root_html
    )
    categories = list(set(categories))
    print(f"  Found {len(categories)} top-level categories")

    all_pdfs = []  # List of {path, category, part}
    visited = set()

    def crawl_recursive(pages: List[str], category: str, depth: int):
        if depth > max_depth:
            return
        for page in pages:
            if page in visited:
                continue
            visited.add(page)
            time.sleep(REQUEST_DELAY)
            sub_pages, pdf_links = crawl_index_page(page)

            # Extract part name from URL
            parts = page.split('/tdm/')[1].split('/')
            part = '/'.join(parts[1:-1]) if len(parts) > 2 else ''

            for pdf in pdf_links:
                all_pdfs.append({
                    'path': pdf,
                    'category': category,
                    'part': part,
                })

            if sub_pages:
                crawl_recursive(sub_pages, category, depth + 1)

    for cat_path in categories:
        cat_name = cat_path.split('/tdm/')[1].split('/')[0]
        print(f"  Crawling category: {cat_name}")
        time.sleep(REQUEST_DELAY)
        sub_pages, pdf_links = crawl_index_page(cat_path)
        visited.add(cat_path)

        for pdf in pdf_links:
            all_pdfs.append({
                'path': pdf,
                'category': cat_name,
                'part': '',
            })

        crawl_recursive(sub_pages, cat_name, 1)

    print(f"  Total PDFs found: {len(all_pdfs)}")
    return all_pdfs


def extract_title_from_path(path: str) -> str:
    """Generate a readable title from the PDF path."""
    filename = path.split('/')[-1].replace('.pdf', '')
    # Clean up the filename
    title = filename.replace('-', ' ').replace('_', ' ')
    return title


def download_and_extract(path: str) -> Optional[str]:
    """Download PDF and extract text."""
    url = f"{BASE_URL}{path}" if path.startswith('/') else path
    tmp_path = f"/tmp/revenue_{hash(path) & 0xFFFFFFFF}.pdf"
    try:
        if not curl_download(url, tmp_path):
            return None
        # Skip very large files (> 50MB)
        if os.path.getsize(tmp_path) > 50 * 1024 * 1024:
            print(f"    Skipping large file: {os.path.getsize(tmp_path)} bytes")
            return None
        text = extract_pdf_text(tmp_path)
        return text if text else None
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def normalize(pdf_info: Dict, text: str) -> Dict:
    """Normalize a TDM entry to standard schema."""
    path = pdf_info['path']
    filename = path.split('/')[-1].replace('.pdf', '')
    doc_id = re.sub(r'[^\w-]', '_', filename)

    # Try to extract title from first line of PDF text
    title = None
    if text:
        first_lines = text[:500].split('\n')
        for line in first_lines:
            line = line.strip()
            if len(line) > 10 and not line.startswith('Tax and Duty Manual'):
                title = line[:200]
                break
    if not title:
        title = extract_title_from_path(path)

    return {
        '_id': f"IE_Revenue_{doc_id}",
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': text,
        'date': None,
        'url': f"{BASE_URL}{path}",
        'category': pdf_info.get('category', ''),
        'part': pdf_info.get('part', ''),
        'filename': path.split('/')[-1],
        'language': 'en',
    }


def fetch_all() -> Iterator[Dict]:
    """Yield all TDM documents with full text."""
    pdfs = crawl_all_tdm_pdfs()
    for i, pdf_info in enumerate(pdfs):
        print(f"  [{i+1}/{len(pdfs)}] {pdf_info['path'].split('/')[-1]}")
        text = download_and_extract(pdf_info['path'])
        if text:
            yield normalize(pdf_info, text)
        time.sleep(REQUEST_DELAY)


def bootstrap_sample(max_records: int = 12) -> List[Dict]:
    """Fetch sample records from different categories."""
    pdfs = crawl_all_tdm_pdfs()
    if not pdfs:
        print("ERROR: No PDFs found")
        return []

    # Sample from different categories
    by_category = {}
    for pdf in pdfs:
        cat = pdf['category']
        if cat not in by_category:
            by_category[cat] = []
        by_category[cat].append(pdf)

    samples = []
    cats = list(by_category.keys())
    idx = 0
    while len(samples) < max_records and idx < len(pdfs):
        cat = cats[idx % len(cats)]
        if by_category[cat]:
            pdf_info = by_category[cat].pop(0)
            print(f"  Downloading: {pdf_info['category']}/{pdf_info['path'].split('/')[-1]}")
            text = download_and_extract(pdf_info['path'])
            if text:
                record = normalize(pdf_info, text)
                samples.append(record)
                print(f"    OK: {len(text)} chars")
            else:
                print(f"    SKIP: no text")
            time.sleep(REQUEST_DELAY)
        idx += 1

    return samples


def main():
    parser = argparse.ArgumentParser(description='IE/Revenue Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'])
    parser.add_argument('--sample', action='store_true')
    parser.add_argument('--since', type=str)
    parser.add_argument('--output', type=str)
    args = parser.parse_args()

    output_dir = args.output or str(Path(__file__).parent / 'sample')
    os.makedirs(output_dir, exist_ok=True)

    if args.command == 'bootstrap':
        records = bootstrap_sample() if args.sample else list(fetch_all())

        saved = 0
        for record in records:
            filename = re.sub(r'[^\w-]', '_', record['_id'])[:100] + '.json'
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            saved += 1

        print(f"\n=== Summary ===")
        print(f"Records saved: {saved}")
        if records:
            texts = [r['text'] for r in records if r.get('text')]
            avg_len = sum(len(t) for t in texts) / len(texts) if texts else 0
            print(f"Average text length: {avg_len:.0f} chars")
            print(f"All have text: {all(r.get('text') for r in records)}")

    elif args.command == 'fetch':
        count = 0
        for record in fetch_all():
            filename = re.sub(r'[^\w-]', '_', record['_id'])[:100] + '.json'
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Fetched {count} records")

    elif args.command == 'updates':
        print("NOTE: TDMs do not have reliable modification dates. Use 'fetch' for full refresh.")


if __name__ == '__main__':
    main()
