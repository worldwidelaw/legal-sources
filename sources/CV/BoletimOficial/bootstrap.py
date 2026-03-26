#!/usr/bin/env python3
"""
Cabo Verde Official Gazette (Boletim Oficial) Data Fetcher

Extracts legislation from the INCV Boletim Oficial via JSON REST API.
Full text is available in HTML format without authentication.

Data source: https://boe.incv.cv
License: Public domain (official gazette)
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

SOURCE_ID = "CV/BoletimOficial"
BASE_URL = "https://boe.incv.cv"
API_URL = f"{BASE_URL}/api/v1"
REQUEST_DELAY = 1.5


def curl_get(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL content using curl."""
    try:
        result = subprocess.run(
            ['curl', '-sL', '--max-time', str(timeout), '-H',
             'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
             '-H', 'Accept: application/json', url],
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
    text = re.sub(r'</?h[1-6][^>]*>', '\n', text)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def get_days_with_publications(start: str, end: str) -> List[str]:
    """Get dates that have publications in a date range (YYYY-MM-DD format)."""
    url = f"{API_URL}/Home/DaysWithPublications?start={start}&end={end}"
    raw = curl_get(url)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        # Response: {"success": true, "content": {"2026-03-02T00:00:00": 2, ...}}
        if isinstance(data, dict) and data.get('success'):
            content = data.get('content', {})
            if isinstance(content, dict):
                return [k[:10] for k in content.keys()]
            if isinstance(content, list):
                return [str(d)[:10] for d in content]
        if isinstance(data, list):
            return [str(d)[:10] for d in data]
        return []
    except json.JSONDecodeError:
        return []


def fetch_bulletin(serie: int, date: str) -> List[Dict]:
    """Fetch bulletin entries for a given series and date."""
    url = f"{API_URL}/Home/BulletinFilter?serie={serie}&date={date}"
    raw = curl_get(url, timeout=60)
    if not raw:
        return []
    try:
        data = json.loads(raw)
        # Response: {"success": true, "content": [{"id":..., "index": [...]}]}
        if isinstance(data, dict) and data.get('success'):
            bulletins = data.get('content', [])
        elif isinstance(data, list):
            bulletins = data
        elif isinstance(data, dict):
            bulletins = [data]
        else:
            return []
        if not isinstance(bulletins, list):
            bulletins = [bulletins]

        entries = []
        for bulletin in bulletins:
            if not isinstance(bulletin, dict):
                continue
            index_entries = bulletin.get('index', [])
            bulletin_date = bulletin.get('date', date)
            if isinstance(bulletin_date, str):
                bulletin_date = bulletin_date[:10]
            for entry in (index_entries or []):
                if isinstance(entry, dict):
                    if not entry.get('titleDate') and not entry.get('titleDateShort'):
                        entry['_bulletin_date'] = bulletin_date
                    entries.append(entry)
        return entries
    except json.JSONDecodeError:
        return []


def normalize(entry: Dict, date: str = None) -> Dict:
    """Normalize a bulletin index entry to standard schema."""
    # Try various field name casings
    title = (entry.get('title') or entry.get('Title') or
             entry.get('refAct') or entry.get('RefAct') or '')
    content_html = (entry.get('content') or entry.get('Content') or
                    entry.get('summary') or entry.get('Summary') or '')
    text = clean_html(content_html)

    entry_date = (entry.get('titleDateShort') or entry.get('titleDate') or
                  entry.get('_bulletin_date') or date or '')
    if entry_date:
        entry_date = str(entry_date)[:10]

    entry_id = (entry.get('id') or entry.get('Id') or
                entry.get('indexEntryId') or entry.get('IndexEntryId') or '')

    ref_act = entry.get('refAct') or entry.get('RefAct') or ''
    part = entry.get('part') or entry.get('Part') or ''

    # Build a unique ID
    doc_id = f"CV_BO_{entry_id}" if entry_id else f"CV_BO_{entry_date}_{hash(title) % 100000}"

    # If title is empty, use refAct or first line of text
    if not title and ref_act:
        title = ref_act
    if not title and text:
        title = text[:100]

    # Extract entity info
    entities = entry.get('indexEntities') or entry.get('IndexEntities') or []
    entity_names = []
    for ent in entities:
        if isinstance(ent, dict):
            name = ent.get('name') or ent.get('Name') or ''
            if name:
                entity_names.append(name)

    return {
        '_id': doc_id,
        '_source': SOURCE_ID,
        '_type': 'legislation',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': text,
        'date': entry_date,
        'url': f"{BASE_URL}/Bulletins/View/{entry_id}" if entry_id else BASE_URL,
        'ref_act': ref_act,
        'part': part,
        'entities': entity_names,
        'language': 'pt',
    }


def fetch_all() -> Iterator[Dict]:
    """Yield all bulletin entries with full text."""
    # Iterate year by year from 1975 to present
    current_year = datetime.now().year
    for year in range(1975, current_year + 1):
        print(f"  Processing year {year}...")
        for month in range(1, 13):
            start = f"{year}-{month:02d}-01"
            if month == 12:
                end = f"{year}-12-31"
            else:
                end = f"{year}-{month + 1:02d}-01"

            days = get_days_with_publications(start, end)
            if not days:
                continue

            for day in days:
                for serie in [1, 2]:
                    entries = fetch_bulletin(serie, day)
                    for entry in entries:
                        record = normalize(entry, day)
                        if record['text']:
                            yield record
                    time.sleep(REQUEST_DELAY)


def fetch_updates(since: str) -> Iterator[Dict]:
    """Yield entries published after a date."""
    from datetime import timedelta

    since_date = datetime.strptime(since, '%Y-%m-%d')
    today = datetime.now()

    current = since_date
    while current <= today:
        month_start = current.strftime('%Y-%m-01')
        if current.month == 12:
            month_end = f"{current.year}-12-31"
        else:
            month_end = f"{current.year}-{current.month + 1:02d}-01"

        days = get_days_with_publications(month_start, month_end)
        for day in days:
            if day >= since:
                for serie in [1, 2]:
                    entries = fetch_bulletin(serie, day)
                    for entry in entries:
                        record = normalize(entry, day)
                        if record['text']:
                            yield record
                    time.sleep(REQUEST_DELAY)

        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)


def bootstrap_sample(max_records: int = 15) -> List[Dict]:
    """Fetch sample records from recent publications."""
    samples = []

    # Get recent dates with publications
    today = datetime.now()
    # Try last 6 months to find enough records
    for months_back in range(0, 6):
        if len(samples) >= max_records:
            break
        year = today.year
        month = today.month - months_back
        if month <= 0:
            month += 12
            year -= 1
        start = f"{year}-{month:02d}-01"
        if month == 12:
            end = f"{year}-12-31"
        else:
            end = f"{year}-{month + 1:02d}-01"

        print(f"  Checking {start} to {end}...")
        days = get_days_with_publications(start, end)
        if not days:
            continue

        print(f"    Found {len(days)} days with publications")
        for day in sorted(days, reverse=True):
            if len(samples) >= max_records:
                break
            for serie in [1, 2]:
                entries = fetch_bulletin(serie, day)
                for entry in entries:
                    if len(samples) >= max_records:
                        break
                    record = normalize(entry, day)
                    if record['text'] and len(record['text']) > 50:
                        samples.append(record)
                        print(f"    [{len(samples)}] {record['title'][:60]}... ({len(record['text'])} chars)")
                time.sleep(REQUEST_DELAY)

    return samples


def main():
    parser = argparse.ArgumentParser(description='CV/BoletimOficial Data Fetcher')
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
