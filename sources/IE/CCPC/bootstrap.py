#!/usr/bin/env python3
"""
Irish Competition and Consumer Protection Commission (CCPC) Data Fetcher

Extracts competition enforcement news, merger determinations, and consumer
protection announcements via WordPress REST API.

Data source: https://www.ccpc.ie/business
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
from html import unescape
from pathlib import Path
from typing import Iterator, Optional, Dict, List

SOURCE_ID = "IE/CCPC"
BASE_URL = "https://www.ccpc.ie/business"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
PER_PAGE = 100
REQUEST_DELAY = 1.0


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
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def normalize(post: Dict) -> Dict:
    """Normalize a WP post to standard schema."""
    title = unescape(post.get('title', {}).get('rendered', ''))
    content_html = post.get('content', {}).get('rendered', '')
    text = clean_html(content_html)
    date_str = post.get('date', '')
    date_iso = date_str[:10] if date_str else None
    wp_id = post.get('id', 0)

    return {
        '_id': f"IE_CCPC_{wp_id}",
        '_source': SOURCE_ID,
        '_type': 'doctrine',
        '_fetched_at': datetime.now(timezone.utc).isoformat(),
        'title': title,
        'text': text,
        'date': date_iso,
        'url': post.get('link', ''),
        'wp_id': wp_id,
        'slug': post.get('slug', ''),
        'language': 'en',
    }


def fetch_posts_page(page: int) -> List[Dict]:
    """Fetch a page of posts from WP REST API."""
    url = f"{API_URL}?per_page={PER_PAGE}&page={page}&_fields=id,date,slug,title,content,link"
    raw = curl_get(url)
    if not raw:
        return []
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return []


def get_total_pages() -> int:
    """Get total number of pages from WP API headers."""
    try:
        result = subprocess.run(
            ['curl', '-sI', '--max-time', '15', '-H',
             'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
             f"{API_URL}?per_page={PER_PAGE}"],
            capture_output=True, text=True, timeout=20
        )
        for line in result.stdout.split('\n'):
            if 'x-wp-totalpages' in line.lower():
                return int(line.split(':')[1].strip())
    except Exception:
        pass
    return 20  # fallback


def fetch_all() -> Iterator[Dict]:
    """Yield all CCPC posts with full text."""
    total_pages = get_total_pages()
    print(f"Total pages: {total_pages}")

    for page in range(1, total_pages + 1):
        print(f"  Fetching page {page}/{total_pages}...")
        posts = fetch_posts_page(page)
        if not posts:
            print(f"    No posts on page {page}, stopping")
            break

        for post in posts:
            record = normalize(post)
            if record['text']:
                yield record

        time.sleep(REQUEST_DELAY)


def fetch_updates(since: str) -> Iterator[Dict]:
    """Yield posts modified after a date."""
    page = 1
    while True:
        url = (f"{API_URL}?per_page={PER_PAGE}&page={page}"
               f"&after={since}T00:00:00"
               f"&_fields=id,date,slug,title,content,link")
        raw = curl_get(url)
        if not raw:
            break
        try:
            posts = json.loads(raw)
        except json.JSONDecodeError:
            break
        if not posts:
            break
        for post in posts:
            record = normalize(post)
            if record['text']:
                yield record
        page += 1
        time.sleep(REQUEST_DELAY)


def bootstrap_sample(max_records: int = 12) -> List[Dict]:
    """Fetch sample records."""
    samples = []
    posts = fetch_posts_page(1)
    for post in posts[:max_records]:
        record = normalize(post)
        if record['text']:
            samples.append(record)
            print(f"  {record['title'][:60]}... ({len(record['text'])} chars)")
    return samples


def main():
    parser = argparse.ArgumentParser(description='IE/CCPC Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'])
    parser.add_argument('--sample', action='store_true')
    parser.add_argument('--since', type=str)
    parser.add_argument('--output', type=str)
    parser.add_argument("--full", action="store_true", help="Fetch all records")
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
