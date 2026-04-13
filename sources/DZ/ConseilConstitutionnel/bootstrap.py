#!/usr/bin/env python3
"""
Algeria Constitutional Court (Cour constitutionnelle) Data Fetcher

Fetches decisions from the Cour constitutionnelle via WordPress REST API.
The Constitutional Court replaced the former Conseil constitutionnel after
Algeria's 2020 Constitution.

Data source: https://cour-constitutionnelle.dz/
API: WordPress REST API (wp-json/wp/v2/posts)

Decisions are organized in year-grouped pages with Visual Composer accordion
sections. This fetcher extracts individual decisions from those pages.
"""

import json
import re
import sys
import time
import urllib3
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Generator, Optional

import requests
from bs4 import BeautifulSoup

# Suppress SSL warnings (site has certificate issues)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
BASE_URL = "https://cour-constitutionnelle.dz/wp-json/wp/v2"
POSTS_URL = f"{BASE_URL}/posts"
RATE_LIMIT_DELAY = 2  # seconds between requests

# Decision-related category IDs
DECISION_CATEGORIES = [
    579,   # Conformité/Cour
    577,   # L'exception d'inconstitutionnalité
    993,   # Constitutionnalité/Cour
    646,   # Contentieux électoral/Cour
    925,   # Avis/Cour
    841,   # Remplacement de Députés Cour
    1036,  # Levée de l'immunité parlementaire
    184,   # Constitutionnalité et conformité (old council)
    81,    # L'exception d'inconstitutionnalité (old council)
    583,   # Contentieux électoral (old council)
    585,   # Décisions de remplacement de députés (old council)
    4,     # Décisions
    352,   # Dernières Autres Décisions
    1274,  # Autres décisions
]

# Advisory opinion (Avis) year categories
AVIS_CATEGORIES = [
    192, 190, 188, 186, 133, 131, 129, 127, 125, 123, 121, 119,
    115, 113, 105, 103, 101, 99,  # Avis 1989-2019
]

# Proclamation categories
PROCLAMATION_CATEGORIES = [
    358,   # Proclamations par année
]


def clean_html(html_text: str) -> str:
    """Remove HTML tags, Visual Composer shortcodes, and clean up text."""
    if not html_text:
        return ""
    # Remove Visual Composer shortcodes
    text = re.sub(r'\[/?vc_[^\]]*\]', '', html_text)
    # Remove other WP shortcodes
    text = re.sub(r'\[/?[a-zA-Z_]+[^\]]*\]', '', text)
    # Parse remaining HTML
    soup = BeautifulSoup(text, 'html.parser')
    result = soup.get_text(separator='\n')
    # Clean up whitespace
    result = re.sub(r'\n\s*\n', '\n\n', result)
    return result.strip()


def extract_decision_id(text: str) -> Optional[str]:
    """Extract decision number from text like 'Décision n° 01/D.CC/CC/24'."""
    patterns = [
        r'[Dd]écision\s+n[°o]\s*([\d]+/[^\s,]+)',
        r'[Dd]écision\s+n[°o]\s*([\d]+-[^\s,]+)',
        r'[Aa]vis\s+n[°o]\s*([\d]+/[^\s,]+)',
        r'[Aa]vis\s+n[°o]\s*([\d]+-[^\s,]+)',
        r'[Pp]roclamation\s+n[°o]\s*([\d]+/[^\s,]+)',
    ]
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            return m.group(1).strip().rstrip('.')
    return None


def split_decisions(html_content: str, post_id: int, post_date: str, post_url: str) -> list[dict]:
    """Split a year-grouped page into individual decisions."""
    decisions = []

    # Split by vc_tta_section boundaries
    sections = re.split(r'\[vc_tta_section[^\]]*\]', html_content)

    for section in sections:
        text = clean_html(section)
        if len(text) < 200:
            continue

        # Try to extract individual decisions from this section
        # Look for decision boundaries: "Décision n°" at start of lines
        decision_blocks = re.split(
            r'(?=(?:Décision|DÉCISION|Avis|AVIS)\s+n[°o])',
            text
        )

        for block in decision_blocks:
            block = block.strip()
            if len(block) < 200:
                continue

            dec_id = extract_decision_id(block)
            if dec_id:
                # Extract title (first line or first sentence)
                lines = block.split('\n')
                title = lines[0].strip()[:200]
            else:
                # Use first meaningful line as title
                lines = [l.strip() for l in block.split('\n') if l.strip()]
                title = lines[0][:200] if lines else f"Decision from post {post_id}"
                dec_id = f"post{post_id}-{hash(block[:500]) % 100000}"

            decisions.append({
                '_id': f"DZ-CC-{dec_id}",
                'decision_number': dec_id,
                'title': title,
                'text': block,
                'date': post_date,
                'url': post_url,
                'wp_post_id': post_id,
            })

    # If no sections found, treat whole page as one record
    if not decisions:
        text = clean_html(html_content)
        if len(text) >= 200:
            dec_id = extract_decision_id(text)
            lines = [l.strip() for l in text.split('\n') if l.strip()]
            title = lines[0][:200] if lines else f"Decision collection (post {post_id})"

            decisions.append({
                '_id': f"DZ-CC-post{post_id}",
                'decision_number': dec_id,
                'title': title,
                'text': text,
                'date': post_date,
                'url': post_url,
                'wp_post_id': post_id,
            })

    return decisions


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """
    Fetch all decisions from the Constitutional Court WordPress API.

    Fetches posts from decision-related categories and splits year-grouped
    pages into individual decision records.
    """
    all_categories = DECISION_CATEGORIES + AVIS_CATEGORIES + PROCLAMATION_CATEGORIES
    seen_ids = set()
    seen_post_ids = set()
    doc_count = 0

    for cat_id in all_categories:
        page = 1
        while True:
            params = {
                'per_page': 100,
                'page': page,
                'categories': str(cat_id),
                'orderby': 'date',
                'order': 'desc',
            }

            try:
                response = requests.get(POSTS_URL, params=params, timeout=30, verify=False)
                if response.status_code == 400:
                    break
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"Error fetching cat {cat_id} page {page}: {e}", file=sys.stderr)
                break

            posts = response.json()
            if not posts:
                break

            for post in posts:
                if post['id'] in seen_post_ids:
                    continue
                seen_post_ids.add(post['id'])

                content_html = post.get('content', {}).get('rendered', '')
                if len(content_html) < 200:
                    continue

                # Parse date
                date_str = post.get('date', '')
                date_iso = None
                if date_str:
                    try:
                        dt = datetime.fromisoformat(date_str)
                        date_iso = dt.strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        pass

                post_url = post.get('link', '')

                # Split into individual decisions
                decisions = split_decisions(content_html, post['id'], date_iso, post_url)

                for dec in decisions:
                    if dec['_id'] in seen_ids:
                        continue
                    seen_ids.add(dec['_id'])

                    yield dec
                    doc_count += 1

                    if max_docs and doc_count >= max_docs:
                        return

            page += 1
            time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions modified since a given date."""
    all_categories = DECISION_CATEGORIES + AVIS_CATEGORIES + PROCLAMATION_CATEGORIES
    seen_ids = set()
    since_iso = since.strftime('%Y-%m-%dT%H:%M:%S')

    for cat_id in all_categories:
        params = {
            'per_page': 100,
            'page': 1,
            'categories': str(cat_id),
            'modified_after': since_iso,
            'orderby': 'modified',
            'order': 'desc',
        }

        try:
            response = requests.get(POSTS_URL, params=params, timeout=30, verify=False)
            if response.status_code == 400:
                continue
            response.raise_for_status()
        except requests.RequestException:
            continue

        posts = response.json()
        for post in posts:
            content_html = post.get('content', {}).get('rendered', '')
            if len(content_html) < 200:
                continue

            date_str = post.get('date', '')
            date_iso = None
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str)
                    date_iso = dt.strftime('%Y-%m-%d')
                except (ValueError, TypeError):
                    pass

            decisions = split_decisions(content_html, post['id'], date_iso, post.get('link', ''))
            for dec in decisions:
                if dec['_id'] not in seen_ids:
                    seen_ids.add(dec['_id'])
                    yield dec

        time.sleep(RATE_LIMIT_DELAY)


def normalize(raw: dict) -> dict:
    """Transform raw post data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    return {
        '_id': raw['_id'],
        '_source': 'DZ/ConseilConstitutionnel',
        '_type': 'case_law',
        '_fetched_at': now,
        'title': raw['title'],
        'text': raw['text'],
        'date': raw.get('date'),
        'url': raw.get('url', ''),
        'decision_number': raw.get('decision_number'),
        'wp_post_id': raw.get('wp_post_id'),
        'language': 'fr',
    }


def bootstrap_sample(sample_dir: Path, count: int = 100) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count):
        record = normalize(raw)
        samples.append(record)

        # Save individual sample
        safe_filename = re.sub(r'[/\\:]', '_', f"{record['_id']}.json")
        with open(sample_dir / safe_filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {safe_filename} ({len(record['text'])} chars)", file=sys.stderr)

    # Save combined samples
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

    parser = argparse.ArgumentParser(description='Algeria Constitutional Court fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch', 'updates'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Generate sample data only')
    parser.add_argument('--count', type=int, default=100,
                       help='Number of samples to generate')
    parser.add_argument('--since', type=str,
                       help='Fetch updates since date (YYYY-MM-DD)')

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
