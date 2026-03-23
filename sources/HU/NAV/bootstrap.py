#!/usr/bin/env python3
"""
Hungarian Tax Authority (NAV) Data Fetcher

Extracts tax information booklets (információs füzetek) from nav.gov.hu.
PDF documents with full text extraction via pypdf.

Data source: https://nav.gov.hu
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
from typing import Iterator, Optional, Dict, List

SOURCE_ID = "HU/NAV"
BASE_URL = "https://nav.gov.hu"

# Year listing pages - 2024 has a different URL slug
YEAR_PAGES = {
    2026: "/ugyfeliranytu/nezzen-utana/inf_fuz/2026",
    2025: "/ugyfeliranytu/nezzen-utana/inf_fuz/2025",
    2024: "/ugyfeliranytu/nezzen-utana/inf_fuz/informacios-fuzetek---2024",
    2023: "/ugyfeliranytu/nezzen-utana/inf_fuz/2023",
    2022: "/ugyfeliranytu/nezzen-utana/inf_fuz/2022",
    2021: "/ugyfeliranytu/nezzen-utana/inf_fuz/2021",
}

REQUEST_DELAY = 1.5


def curl_get(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL content using curl."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout), '-H',
             'User-Agent: Mozilla/5.0 (compatible; WorldWideLaw/1.0)', url],
            capture_output=True, text=True, timeout=timeout + 5
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except Exception as e:
        print(f"  curl error for {url}: {e}")
        return None


def curl_download(url: str, output_path: str, timeout: int = 60) -> bool:
    """Download a file using curl."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout), '-H',
             'User-Agent: Mozilla/5.0 (compatible; WorldWideLaw/1.0)',
             '-o', output_path, url],
            capture_output=True, text=True, timeout=timeout + 5
        )
        return result.returncode == 0 and os.path.getsize(output_path) > 0
    except Exception:
        return False


def extract_pdf_text(pdf_path: str) -> str:
    """Extract text from PDF using pypdf."""
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        return text.strip()
    except ImportError:
        pass
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        return text.strip()
    except ImportError:
        print("ERROR: Neither pypdf nor PyPDF2 available. Install: pip3 install pypdf")
        return ""


def parse_year_page(html: str, year: int) -> List[Dict]:
    """Parse a year listing page to extract PDF links and titles."""
    entries = []
    # Find all pfile links pointing to info booklets
    pattern = r'href="(/pfile/file\?path=/ugyfeliranytu/nezzen-utana/inf_fuz/[^"]+)"'
    pdf_links = re.findall(pattern, html)

    # Also extract titles - they are typically in adjacent text or link text
    # Pattern: <a href="/pfile/...">TITLE</a>
    full_pattern = r'<a[^>]*href="(/pfile/file\?path=/ugyfeliranytu/nezzen-utana/inf_fuz/[^"]+)"[^>]*>([^<]*)</a>'
    matches = re.findall(full_pattern, html)

    seen_paths = set()
    for path, link_text in matches:
        if path in seen_paths:
            continue
        seen_paths.add(path)

        link_text = link_text.strip()

        # Extract booklet number from filename start (e.g., "09.-Title" or "97._Title")
        filename = path.split('/')[-1]
        num_m = re.match(r'(\d+)', filename)
        booklet_number = num_m.group(1) if num_m else None

        # Extract date from path slug (e.g., "2026.02.04" or "2026.-02.-03" or "2025.-02.-03")
        date_m = re.search(r'(\d{4})\.?\s*-?\s*(\d{2})\.?\s*-?\s*(\d{2})', path)
        date_iso = None
        if date_m:
            y, m, d = date_m.group(1), date_m.group(2), date_m.group(3)
            # Only use if year is plausible (not a booklet-internal number)
            if 2019 <= int(y) <= 2030:
                date_iso = f"{y}-{m}-{d}"

        # Use link text as title, or extract from path
        if link_text and link_text != 'Letöltés':
            title = link_text
        else:
            # Extract from path slug
            slug = path.split('/')[-1]
            title = re.sub(r'^\d+[._-]', '', slug)
            title = title.replace('-', ' ').replace('_', ' ')
            # Remove date suffix
            title = re.sub(r'\d{4}[._]\d{2}[._]\d{2}$', '', title).strip()

        entries.append({
            'path': path,
            'title': title,
            'booklet_number': booklet_number,
            'date': date_iso,
            'year': year,
        })

    # Deduplicate by path (some links appear twice)
    return entries


def download_and_extract(path: str) -> Optional[str]:
    """Download PDF and extract text."""
    url = f"{BASE_URL}{path}"
    tmp_path = f"/tmp/nav_{hash(path) & 0xFFFFFFFF}.pdf"
    try:
        if not curl_download(url, tmp_path):
            return None
        text = extract_pdf_text(tmp_path)
        return text if text else None
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def normalize(entry: Dict, text: str) -> Dict:
    """Normalize a NAV entry to standard schema."""
    num = entry.get('booklet_number', 'unknown')
    year = entry['year']
    doc_id = f"HU_NAV_{year}_{num}"
    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': entry['title'],
        'text': text,
        'date': entry.get('date'),
        'url': f"{BASE_URL}{entry['path']}",
        'year': year,
        'booklet_number': entry.get('booklet_number'),
        'language': 'hu',
    }


def fetch_all() -> Iterator[Dict]:
    """Yield all NAV info booklets with full text."""
    for year, page_path in sorted(YEAR_PAGES.items()):
        print(f"\n--- Year: {year} ---")
        url = f"{BASE_URL}{page_path}"
        html = curl_get(url)
        if not html:
            print(f"  Failed to fetch {url}")
            continue

        entries = parse_year_page(html, year)
        print(f"  Found {len(entries)} booklets")

        for i, entry in enumerate(entries):
            print(f"  [{i+1}/{len(entries)}] {entry['title'][:60]}...")
            text = download_and_extract(entry['path'])
            if text:
                yield normalize(entry, text)
            time.sleep(REQUEST_DELAY)


def bootstrap_sample(max_per_year: int = 2) -> List[Dict]:
    """Fetch sample records."""
    samples = []
    for year in [2026, 2025, 2024, 2023, 2022, 2021]:
        page_path = YEAR_PAGES.get(year)
        if not page_path:
            continue

        print(f"\n--- Sampling year {year} ---")
        url = f"{BASE_URL}{page_path}"
        html = curl_get(url)
        if not html:
            continue

        entries = parse_year_page(html, year)
        count = 0
        for entry in entries[:max_per_year * 2]:
            if count >= max_per_year:
                break
            print(f"  Downloading: {entry['title'][:60]}...")
            text = download_and_extract(entry['path'])
            if text:
                record = normalize(entry, text)
                samples.append(record)
                count += 1
                print(f"    OK: {len(text)} chars")
            else:
                print(f"    SKIP: no text")
            time.sleep(REQUEST_DELAY)
        print(f"  Sampled {count} from {year}")
    return samples


def main():
    parser = argparse.ArgumentParser(description='HU/NAV Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only')
    parser.add_argument('--since', type=str, help='Fetch updates since date')
    parser.add_argument('--output', type=str, help='Output directory')
    args = parser.parse_args()

    output_dir = args.output or str(Path(__file__).parent / 'sample')
    os.makedirs(output_dir, exist_ok=True)

    if args.command == 'bootstrap':
        if args.sample:
            records = bootstrap_sample()
        else:
            records = list(fetch_all())

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
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        count = 0
        for record in fetch_all():
            if record.get('date') and record['date'] >= args.since:
                filename = re.sub(r'[^\w-]', '_', record['_id'])[:100] + '.json'
                filepath = os.path.join(output_dir, filename)
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
        print(f"Fetched {count} updates since {args.since}")


if __name__ == '__main__':
    main()
