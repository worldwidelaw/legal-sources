#!/usr/bin/env python3
"""
Algeria Supreme Court (Cour suprême) Data Fetcher

Fetches decisions from the Cour suprême via WordPress REST API custom post type.
1,261 decisions across criminal, civil, commercial, family, real estate,
and combined chambers. Full text in Arabic.

Data source: https://coursupreme.dz/
API: WordPress REST API (wp-json/wp/v2/decision)
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

# Constants
BASE_URL = "https://coursupreme.dz/wp-json/wp/v2"
DECISIONS_URL = f"{BASE_URL}/decision"
RATE_LIMIT_DELAY = 2  # seconds between requests
SESSION = requests.Session()
SESSION.headers.update({
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
})


def clean_html(html_text: str) -> str:
    """Remove HTML tags, WPBakery shortcodes, and clean up text."""
    if not html_text:
        return ""
    # Remove WPBakery/VC shortcodes
    text = re.sub(r'\[/?vc_[^\]]*\]', '', html_text)
    text = re.sub(r'\[/?[a-zA-Z_]+[^\]]*\]', '', text)
    # Parse remaining HTML
    soup = BeautifulSoup(text, 'html.parser')
    result = soup.get_text(separator='\n')
    # Clean up whitespace
    result = re.sub(r'\n\s*\n', '\n\n', result)
    return result.strip()


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """
    Fetch all decisions from the Supreme Court WordPress API.

    Yields raw decision data dicts.
    """
    page = 1
    per_page = 100
    doc_count = 0

    while True:
        params = {
            'per_page': per_page,
            'page': page,
            'orderby': 'date',
            'order': 'desc',
        }

        try:
            response = SESSION.get(DECISIONS_URL, params=params, timeout=30)
            if response.status_code == 400:
                break
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching page {page}: {e}", file=sys.stderr)
            break

        posts = response.json()
        if not posts:
            break

        total = response.headers.get('X-WP-Total', '?')
        total_pages = response.headers.get('X-WP-TotalPages', '?')
        print(f"Page {page}/{total_pages} (total: {total} decisions)", file=sys.stderr)

        for post in posts:
            content_html = post.get('content', {}).get('rendered', '')
            text = clean_html(content_html)

            if len(text) < 50:
                continue

            title_html = post.get('title', {}).get('rendered', '')
            title = clean_html(title_html)

            # Parse date
            date_str = post.get('date', '')
            date_iso = None
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str)
                    date_iso = dt.strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    pass

            raw = {
                '_id': f"DZ-CS-{post['id']}",
                'wp_id': post['id'],
                'title': title,
                'text': text,
                'date': date_iso,
                'url': post.get('link', ''),
                'slug': post.get('slug', ''),
            }

            yield raw
            doc_count += 1

            if max_docs and doc_count >= max_docs:
                return

        page += 1
        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions modified since a given date."""
    page = 1
    per_page = 100
    since_iso = since.strftime('%Y-%m-%dT%H:%M:%S')

    while True:
        params = {
            'per_page': per_page,
            'page': page,
            'modified_after': since_iso,
            'orderby': 'modified',
            'order': 'desc',
        }

        try:
            response = SESSION.get(DECISIONS_URL, params=params, timeout=30)
            if response.status_code == 400:
                break
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Error fetching updates page {page}: {e}", file=sys.stderr)
            break

        posts = response.json()
        if not posts:
            break

        for post in posts:
            content_html = post.get('content', {}).get('rendered', '')
            text = clean_html(content_html)

            if len(text) < 50:
                continue

            title_html = post.get('title', {}).get('rendered', '')
            title = clean_html(title_html)

            date_str = post.get('date', '')
            date_iso = None
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str)
                    date_iso = dt.strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    pass

            yield {
                '_id': f"DZ-CS-{post['id']}",
                'wp_id': post['id'],
                'title': title,
                'text': text,
                'date': date_iso,
                'url': post.get('link', ''),
                'slug': post.get('slug', ''),
            }

        page += 1
        time.sleep(RATE_LIMIT_DELAY)


def normalize(raw: dict) -> dict:
    """Transform raw decision data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    return {
        '_id': raw['_id'],
        '_source': 'DZ/CourSupreme',
        '_type': 'case_law',
        '_fetched_at': now,
        'title': raw['title'],
        'text': raw['text'],
        'date': raw.get('date'),
        'url': raw.get('url', ''),
        'wp_id': raw.get('wp_id'),
        'language': 'ar',
    }


def bootstrap_sample(sample_dir: Path, count: int = 100) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count):
        record = normalize(raw)
        samples.append(record)

        safe_filename = re.sub(r'[/\\:]', '_', f"{record['_id']}.json")
        with open(sample_dir / safe_filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {safe_filename} ({len(record['text'])} chars)", file=sys.stderr)

    if samples:
        with open(sample_dir / 'all_samples.json', 'w', encoding='utf-8') as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        text_lengths = [len(s['text']) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Algeria Supreme Court fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Generate sample data only')
    parser.add_argument('--count', type=int, default=100,
                       help='Number of samples to generate')
    parser.add_argument('--since', type=str,
                       help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / 'sample'

    if args.command == 'bootstrap':
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            for raw in fetch_all():
                record = normalize(raw)
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'fetch':
        for raw in fetch_all(max_docs=args.count if args.sample else None):
            record = normalize(raw)
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for raw in fetch_updates(since):
            record = normalize(raw)
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
