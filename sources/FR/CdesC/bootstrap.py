#!/usr/bin/env python3
"""
Cour des comptes (French Court of Auditors) Data Fetcher

Fetches judicial decisions, audit reports, and observations from the Cour des comptes
website via sitemap discovery and HTML scraping.

Data source: https://www.ccomptes.fr
Total records: ~37,500 publications
License: Licence Ouverte (Open Licence 2.0)
"""

import argparse
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from hashlib import md5

import requests

# Constants
SITEMAP_URL = "https://www.ccomptes.fr/sitemaps/sitemap_publication/sitemap.xml"
RATE_LIMIT_DELAY = 1.5

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (academic research; legal data collection)",
    "Accept": "text/html,application/xhtml+xml",
})


def strip_html(html: str) -> str:
    """Strip HTML tags and clean text."""
    if not html:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'</(p|div|li|h[1-6])>', '\n', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def get_sitemap_urls(max_urls: int = None) -> list:
    """Parse sitemap XML to get publication URLs and dates."""
    print("  Fetching sitemap...", file=sys.stderr)
    try:
        resp = SESSION.get(SITEMAP_URL, timeout=60)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERROR] Failed to fetch sitemap: {e}", file=sys.stderr)
        return []

    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    root = ET.fromstring(resp.content)

    urls = []
    for url_elem in root.findall("sm:url", ns):
        loc = url_elem.find("sm:loc", ns)
        lastmod = url_elem.find("sm:lastmod", ns)
        if loc is not None:
            urls.append({
                "url": loc.text,
                "lastmod": lastmod.text if lastmod is not None else None,
            })
        if max_urls and len(urls) >= max_urls:
            break

    print(f"  Found {len(urls)} publication URLs in sitemap", file=sys.stderr)
    return urls


def extract_page_content(html: str, url: str) -> Optional[dict]:
    """Extract title, date, and body text from a publication page."""
    # Title
    title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
    title = ""
    if title_match:
        title = strip_html(title_match.group(1))

    if not title:
        return None

    # Date
    date_match = re.search(r'datetime="(\d{4}-\d{2}-\d{2})', html)
    date_val = date_match.group(1) if date_match else None

    # Body text (main content)
    text_parts = []

    # Chapo (lead/summary)
    chapo_match = re.search(
        r'field--name-field-chapo.*?field__item[^>]*>(.*?)</div>',
        html, re.DOTALL
    )
    if chapo_match:
        chapo_text = strip_html(chapo_match.group(1))
        if chapo_text:
            text_parts.append(chapo_text)

    # Main body
    body_match = re.search(
        r'field--name-body.*?field__item[^>]*>(.*?)</div>\s*</div>',
        html, re.DOTALL
    )
    if body_match:
        body_text = strip_html(body_match.group(1))
        if body_text:
            text_parts.append(body_text)

    text = "\n\n".join(text_parts)

    if not text or len(text) < 50:
        return None

    # Generate stable ID from URL
    slug = url.rstrip("/").split("/")[-1]
    doc_id = f"ccomptes-{slug[:80]}"

    return {
        "_id": doc_id,
        "_source": "FR/CdesC",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_val,
        "url": url,
    }


def fetch_all(max_items: int = None) -> Generator[dict, None, None]:
    """Fetch all publications from sitemap."""
    urls = get_sitemap_urls(max_urls=max_items * 2 if max_items else None)
    count = 0

    for entry in urls:
        if max_items and count >= max_items:
            return

        url = entry["url"]
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code != 200:
                continue
        except requests.RequestException as e:
            print(f"  [WARN] Failed to fetch {url}: {e}", file=sys.stderr)
            continue

        doc = extract_page_content(resp.text, url)
        if doc:
            if not doc["date"] and entry.get("lastmod"):
                doc["date"] = entry["lastmod"][:10]
            yield doc
            count += 1
            if count % 10 == 0:
                print(f"  Fetched {count} publications...", file=sys.stderr)

        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch publications modified since a date, using sitemap lastmod."""
    urls = get_sitemap_urls()
    count = 0

    for entry in urls:
        lastmod = entry.get("lastmod", "")
        if lastmod and lastmod[:10] >= since:
            url = entry["url"]
            try:
                resp = SESSION.get(url, timeout=30)
                if resp.status_code != 200:
                    continue
            except requests.RequestException:
                continue

            doc = extract_page_content(resp.text, url)
            if doc:
                if not doc["date"] and lastmod:
                    doc["date"] = lastmod[:10]
                yield doc
                count += 1

            time.sleep(RATE_LIMIT_DELAY)


def normalize(raw: dict) -> dict:
    """Already normalized during fetch."""
    return raw


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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Cour des comptes Data Fetcher')
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
