#!/usr/bin/env python3
"""
Hungarian Data Protection Authority (NAIH) Data Fetcher

Extracts GDPR decisions, privacy opinions, and recommendations from naih.hu.
Uses Joomla PhocaDownload PDF links with pypdf text extraction.

Data source: https://www.naih.hu
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
from typing import Iterator, Optional, Dict, List, Tuple

SOURCE_ID = "HU/NAIH"
BASE_URL = "https://www.naih.hu"

# Sections to scrape: (url_path, section_label, doc_type)
SECTIONS = [
    ("/hatarozatok-vegzesek", "GDPR Decisions", "decision"),
    ("/adatvedelmi-allasfoglalasok", "Privacy Opinions", "opinion"),
    ("/adatvedelmi-ajanlasok", "Recommendations", "recommendation"),
]

PAGINATION_STEP = 50
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


def parse_hungarian_date(date_str: str) -> Optional[str]:
    """Parse Hungarian date format '2025. december 02.' to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip().rstrip('.')
    hu_months = {
        'január': '01', 'február': '02', 'március': '03', 'április': '04',
        'május': '05', 'június': '06', 'július': '07', 'augusztus': '08',
        'szeptember': '09', 'október': '10', 'november': '11', 'december': '12'
    }
    m = re.match(r'(\d{4})\.\s*(\w+)\s+(\d{1,2})', date_str)
    if m:
        year, month_name, day = m.group(1), m.group(2).lower(), m.group(3)
        month = hu_months.get(month_name)
        if month:
            return f"{year}-{month}-{int(day):02d}"
    return None


def parse_listing_page(html: str) -> List[Dict]:
    """Parse a PhocaDownload listing page to extract entries."""
    entries = []
    blocks = re.findall(
        r'<div class="pd-filebox[^"]*">(.*?)(?=<div class="pd-filebox|<div class="pd-subcategory|<div class="pagination|$)',
        html, re.DOTALL
    )

    for block in blocks:
        # Extract download link (first non-"Letöltés" title)
        downloads = re.findall(
            r'download=(\d+):([^"&]*)[^>]*>([^<]+)</a>', block
        )
        if not downloads:
            continue

        # Get the meaningful title (not "Letöltés" which means "Download")
        download_id = downloads[0][0]
        slug = downloads[0][1]
        title = None
        for d_id, d_slug, d_title in downloads:
            if d_title.strip() != 'Letöltés':
                title = d_title.strip()
                download_id = d_id
                slug = d_slug
                break
        if not title:
            # Use slug as fallback
            title = slug.replace('-', ' ').title()

        # Extract case number
        case_m = re.search(r'(NAIH-[0-9/.,-]+)', block)
        case_number = case_m.group(1).rstrip('.,') if case_m else None

        # Extract date - "Dátum:" and date value are in separate elements
        date_m = re.search(r'D.tum:.*?(\d{4}\.\s*\w+\s+\d{1,2}\.?)', block, re.DOTALL)
        date_str = date_m.group(1).strip() if date_m else None
        date_iso = parse_hungarian_date(date_str)

        # Extract tags
        tags = re.findall(r'tagid=\d+[^>]*>([^<]+)</a>', block)

        entries.append({
            'download_id': download_id,
            'slug': slug,
            'title': title,
            'case_number': case_number,
            'date': date_iso,
            'date_raw': date_str,
            'tags': tags,
        })

    return entries


def fetch_section_entries(section_path: str) -> List[Dict]:
    """Fetch all entries from a section, handling pagination."""
    all_entries = []
    seen_ids = set()
    start = 0

    while True:
        url = f"{BASE_URL}{section_path}"
        if start > 0:
            url += f"?start={start}"

        print(f"  Fetching {url}")
        html = curl_get(url)
        if not html:
            print(f"  Failed to fetch {url}")
            break

        entries = parse_listing_page(html)
        new_count = 0
        for entry in entries:
            if entry['download_id'] not in seen_ids:
                seen_ids.add(entry['download_id'])
                all_entries.append(entry)
                new_count += 1

        print(f"    Found {new_count} new entries (total: {len(all_entries)})")

        if new_count == 0:
            break

        # Check for more pages
        has_next = f"start={start + PAGINATION_STEP}" in html
        if not has_next:
            break

        start += PAGINATION_STEP
        time.sleep(REQUEST_DELAY)

    return all_entries


def download_and_extract(entry: Dict, section_path: str) -> Optional[str]:
    """Download PDF for an entry and extract text."""
    download_url = (
        f"{BASE_URL}{section_path}?download="
        f"{entry['download_id']}:{entry['slug']}"
    )

    tmp_path = f"/tmp/naih_{entry['download_id']}.pdf"
    try:
        if not curl_download(download_url, tmp_path):
            print(f"    Failed to download ID {entry['download_id']}")
            return None

        text = extract_pdf_text(tmp_path)
        if not text:
            print(f"    No text extracted from ID {entry['download_id']}")
            return None

        return text
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def normalize(entry: Dict, section_label: str, doc_type: str, text: str) -> Dict:
    """Normalize a NAIH entry to standard schema."""
    doc_id = entry.get('case_number') or f"NAIH-{entry['download_id']}"
    return {
        '_id': f"HU_NAIH_{doc_id.replace('/', '_').replace('.', '_')}",
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': entry['title'],
        'text': text,
        'date': entry.get('date'),
        'url': f"{BASE_URL}/hatarozatok-vegzesek?download={entry['download_id']}:{entry['slug']}",
        'case_number': entry.get('case_number'),
        'section': section_label,
        'document_type': doc_type,
        'tags': entry.get('tags', []),
        'language': 'hu',
    }


def fetch_all() -> Iterator[Dict]:
    """Yield all NAIH documents with full text."""
    for section_path, section_label, doc_type in SECTIONS:
        print(f"\n--- Section: {section_label} ({section_path}) ---")
        entries = fetch_section_entries(section_path)
        print(f"  Total entries in section: {len(entries)}")

        for i, entry in enumerate(entries):
            print(f"  [{i+1}/{len(entries)}] {entry['title'][:60]}...")
            text = download_and_extract(entry, section_path)
            if text:
                yield normalize(entry, section_label, doc_type, text)
            time.sleep(REQUEST_DELAY)


def fetch_updates(since: str) -> Iterator[Dict]:
    """Yield documents modified since a date."""
    from datetime import datetime as dt
    since_date = dt.fromisoformat(since).date() if since else None

    for section_path, section_label, doc_type in SECTIONS:
        entries = fetch_section_entries(section_path)
        for entry in entries:
            if since_date and entry.get('date'):
                try:
                    entry_date = dt.fromisoformat(entry['date']).date()
                    if entry_date < since_date:
                        continue
                except ValueError:
                    pass
            text = download_and_extract(entry, section_path)
            if text:
                yield normalize(entry, section_label, doc_type, text)
            time.sleep(REQUEST_DELAY)


def bootstrap_sample(max_per_section: int = 5) -> List[Dict]:
    """Fetch sample records from each section."""
    samples = []
    for section_path, section_label, doc_type in SECTIONS:
        print(f"\n--- Sampling: {section_label} ---")
        entries = fetch_section_entries(section_path)
        count = 0
        for entry in entries[:max_per_section * 2]:  # Try more in case some fail
            if count >= max_per_section:
                break
            print(f"  Downloading: {entry['title'][:60]}...")
            text = download_and_extract(entry, section_path)
            if text:
                record = normalize(entry, section_label, doc_type, text)
                samples.append(record)
                count += 1
                print(f"    OK: {len(text)} chars")
            else:
                print(f"    SKIP: no text")
            time.sleep(REQUEST_DELAY)
        print(f"  Sampled {count} from {section_label}")
    return samples


def main():
    parser = argparse.ArgumentParser(description='HU/NAIH Data Fetcher')
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

        # Save records
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
        for record in fetch_updates(args.since):
            filename = re.sub(r'[^\w-]', '_', record['_id'])[:100] + '.json'
            filepath = os.path.join(output_dir, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Fetched {count} updates since {args.since}")


if __name__ == '__main__':
    main()
