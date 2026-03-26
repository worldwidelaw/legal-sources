#!/usr/bin/env python3
"""
Council of Europe CPT Visit Reports Data Fetcher

Extracts CPT visit reports, general reports, and public statements
from the HUDOC-CPT database via JSON REST API.

Data source: https://hudoc.cpt.coe.int
License: Council of Europe - Open Access
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
from urllib.parse import quote

SOURCE_ID = "CoE/CPTReports"
BASE_URL = "https://hudoc.cpt.coe.int"
API_URL = f"{BASE_URL}/app/query/results"
HTML_URL = f"{BASE_URL}/app/conversion/docx/section/html/body"
REQUEST_DELAY = 1.5
PAGE_SIZE = 50

REFERER = "https://hudoc.cpt.coe.int/eng"


def curl_get(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL content using curl with required Referer header."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout),
             '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
             '-H', 'Accept: application/json, text/html, */*',
             '-H', f'Referer: {REFERER}',
             url],
            capture_output=True, text=True, timeout=timeout + 5
        )
        if result.returncode == 0:
            return result.stdout
        return None
    except Exception as e:
        print(f"  curl error: {e}")
        return None


def clean_html(html_text: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_text:
        return ""
    text = unescape(html_text)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</?p[^>]*>', '\n', text)
    text = re.sub(r'</?li[^>]*>', '\n- ', text)
    text = re.sub(r'</?(?:ul|ol)[^>]*>', '\n', text)
    text = re.sub(r'</?h[1-6][^>]*>', '\n\n', text)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def search_documents(start: int = 0, length: int = PAGE_SIZE,
                     doc_type: str = None) -> tuple[int, List[Dict]]:
    """Search HUDOC-CPT for documents. Returns (total_count, results)."""
    query_parts = ["contentsitename=CPT"]
    if doc_type:
        query_parts.append(f'cptdocumenttype="{doc_type}"')

    # Only get section 1 to get unique documents
    query_parts.append('cptsectionnumber="0001"')

    query = " AND ".join(query_parts)
    fields = ("cptdocumentid,cptsectiontitle,cptdocumentdate,"
              "cptdocumenttype,cptstate,cptvisittype,cptvisityear,"
              "cptlanguage,cptsectiontotal,cptdocumentreference,"
              "cptkeywords,cptpublicationdate")

    url = (f"{API_URL}?query={quote(query)}"
           f"&select={fields}"
           f"&sort={quote('cptdocumentdate Descending')}"
           f"&start={start}&length={length}")

    raw = curl_get(url, timeout=30)
    if not raw:
        return 0, []
    try:
        data = json.loads(raw)
        total = data.get('resultcount', 0)
        results = data.get('results', [])
        docs = []
        for r in results:
            cols = r.get('columns', {})
            docs.append(cols)
        return total, docs
    except json.JSONDecodeError:
        return 0, []


def fetch_full_text(doc_id: str, total_sections: int) -> str:
    """Fetch all sections of a document and combine into full text."""
    all_text = []
    for section_num in range(1, total_sections + 1):
        section_str = f"{section_num:04d}"
        url = (f"{HTML_URL}?library=CPT"
               f"&id={quote(doc_id)}"
               f"&sectionnumber={section_str}")
        raw = curl_get(url, timeout=45)
        if raw:
            text = clean_html(raw)
            if text:
                all_text.append(text)
        time.sleep(0.5)  # Be gentle between sections
    return "\n\n".join(all_text)


def normalize(doc: Dict, full_text: str) -> Dict:
    """Normalize a HUDOC-CPT document to standard schema."""
    doc_id = doc.get('cptdocumentid', '')
    title = doc.get('cptsectiontitle', '')
    date_str = doc.get('cptdocumentdate', '')
    pub_date = doc.get('cptpublicationdate', '')

    # Parse date
    date_iso = None
    for d in [date_str, pub_date]:
        if d:
            # Format may be "DD/MM/YYYY" or ISO
            match = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', d)
            if match:
                date_iso = f"{match.group(3)}-{match.group(2):>02}-{match.group(1):>02}"
                break
            match = re.match(r'(\d{4})-(\d{2})-(\d{2})', d)
            if match:
                date_iso = d[:10]
                break

    doc_type = doc.get('cptdocumenttype', '')
    state = doc.get('cptstate', '')
    visit_type = doc.get('cptvisittype', '')
    reference = doc.get('cptdocumentreference', '')
    keywords = doc.get('cptkeywords', '')
    language = doc.get('cptlanguage', 'en')

    return {
        '_id': f"CoE_CPT_{doc_id}",
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': full_text,
        'date': date_iso,
        'url': f"{BASE_URL}/?i={doc_id}-1",
        'document_type': doc_type,
        'state': state,
        'visit_type': visit_type,
        'reference': reference,
        'keywords': keywords,
        'language': language,
    }


def fetch_all() -> Iterator[Dict]:
    """Yield all CPT documents with full text."""
    start = 0
    total, docs = search_documents(start=0)
    print(f"  Total documents: {total}")

    while start < total:
        if start > 0:
            _, docs = search_documents(start=start)
        if not docs:
            break

        for doc in docs:
            doc_id = doc.get('cptdocumentid', '')
            total_sections = int(doc.get('cptsectiontotal', '1') or '1')
            print(f"  Fetching {doc_id} ({total_sections} sections)...")

            full_text = fetch_full_text(doc_id, total_sections)
            if full_text and len(full_text) > 100:
                record = normalize(doc, full_text)
                yield record
            time.sleep(REQUEST_DELAY)

        start += len(docs)


def fetch_updates(since: str) -> Iterator[Dict]:
    """Yield documents published after a date."""
    # The API sorts by date descending, so we can stop when we pass the since date
    start = 0
    while True:
        _, docs = search_documents(start=start)
        if not docs:
            break

        for doc in docs:
            pub_date = doc.get('cptpublicationdate', '') or doc.get('cptdocumentdate', '')
            # Simple string comparison works for ISO dates
            if pub_date and pub_date[:10] < since:
                return

            doc_id = doc.get('cptdocumentid', '')
            total_sections = int(doc.get('cptsectiontotal', '1') or '1')
            full_text = fetch_full_text(doc_id, total_sections)
            if full_text and len(full_text) > 100:
                record = normalize(doc, full_text)
                yield record
            time.sleep(REQUEST_DELAY)

        start += len(docs)


def bootstrap_sample(max_records: int = 12) -> List[Dict]:
    """Fetch sample records from recent reports."""
    samples = []
    _, docs = search_documents(start=0, length=max_records + 5)

    for doc in docs:
        if len(samples) >= max_records:
            break

        doc_id = doc.get('cptdocumentid', '')
        total_sections = int(doc.get('cptsectiontotal', '1') or '1')

        # For sample, limit to first 3 sections to save time
        sections_to_fetch = min(total_sections, 3)
        print(f"  Fetching {doc_id} ({sections_to_fetch}/{total_sections} sections)...")

        full_text = fetch_full_text(doc_id, sections_to_fetch)
        if full_text and len(full_text) > 100:
            record = normalize(doc, full_text)
            samples.append(record)
            print(f"    [{len(samples)}] {record['title'][:60]}... ({len(record['text'])} chars)")
        time.sleep(REQUEST_DELAY)

    return samples


def main():
    parser = argparse.ArgumentParser(description='CoE/CPTReports Data Fetcher')
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
        if not args.since:
            print("ERROR: --since required")
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
