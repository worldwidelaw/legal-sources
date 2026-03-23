#!/usr/bin/env python3
"""
ARCOM (Autorité de régulation de la communication audiovisuelle et numérique) Data Fetcher

Fetches French audiovisual regulatory decisions and legal texts (deliberations,
recommendations) via the Drupal JSON:API endpoint on arcom.fr.

Data source: https://www.arcom.fr/jsonapi
Total records: ~2,200+ decisions + legal texts
License: Licence Ouverte (Open Licence 2.0)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Generator, Optional

import requests

# Constants
API_BASE = "https://www.arcom.fr/jsonapi"
DECISIONS_ENDPOINT = f"{API_BASE}/node/decision"
TEXTES_ENDPOINT = f"{API_BASE}/node/texte_juridique"
RATE_LIMIT_DELAY = 1.0

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "WorldWideLaw/1.0 (academic research; legal data collection)",
    "Accept": "application/vnd.api+json",
})


def strip_html(html: str) -> str:
    """Strip HTML tags and clean text."""
    if not html:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'</(p|div|li|h[1-6])>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def fetch_jsonapi_pages(endpoint: str, max_items: int = None) -> Generator[dict, None, None]:
    """Generic paginated fetch following JSON:API next links."""
    url = endpoint
    count = 0

    while url:
        if max_items and count >= max_items:
            return

        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            print(f"  [WARN] API request failed: {e}", file=sys.stderr)
            break

        items = data.get("data", [])
        if not items:
            break

        for item in items:
            if max_items and count >= max_items:
                return
            yield item
            count += 1

        # Follow next link
        links = data.get("links", {})
        next_link = links.get("next")
        if isinstance(next_link, dict):
            url = next_link.get("href")
        elif isinstance(next_link, str):
            url = next_link
        else:
            url = None

        if url:
            time.sleep(RATE_LIMIT_DELAY)


def normalize_decision(item: dict) -> Optional[dict]:
    """Normalize a decision item from JSON:API."""
    attrs = item.get("attributes", {})

    contenu = attrs.get("field_decision_contenu", {})
    text_html = ""
    if isinstance(contenu, dict):
        text_html = contenu.get("value", "") or contenu.get("processed", "")
    elif isinstance(contenu, str):
        text_html = contenu

    text = strip_html(text_html)
    if not text:
        return None

    date_val = attrs.get("field_date_de_decision", "")
    title = attrs.get("title", "")
    path_info = attrs.get("path", {})
    alias = path_info.get("alias", "") if isinstance(path_info, dict) else ""
    url = f"https://www.arcom.fr{alias}" if alias else "https://www.arcom.fr"

    return {
        "_id": f"arcom-decision-{item['id']}",
        "_source": "FR/CSA",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_val,
        "url": url,
        "decision_type": "decision",
    }


def normalize_texte(item: dict) -> Optional[dict]:
    """Normalize a legal text item from JSON:API."""
    attrs = item.get("attributes", {})

    contenu = attrs.get("field_presse_contenu", {})
    text_html = ""
    if isinstance(contenu, dict):
        text_html = contenu.get("value", "") or contenu.get("processed", "")
    elif isinstance(contenu, str):
        text_html = contenu

    text = strip_html(text_html)
    if not text:
        return None

    created = attrs.get("created", "")
    if created:
        try:
            created = created[:10]
        except Exception:
            pass

    title = attrs.get("title", "")
    path_info = attrs.get("path", {})
    alias = path_info.get("alias", "") if isinstance(path_info, dict) else ""
    url = f"https://www.arcom.fr{alias}" if alias else "https://www.arcom.fr"

    return {
        "_id": f"arcom-texte-{item['id']}",
        "_source": "FR/CSA",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": created,
        "url": url,
        "decision_type": "texte_juridique",
    }


def fetch_all(max_items: int = None) -> Generator[dict, None, None]:
    """Fetch all ARCOM records (decisions + legal texts)."""
    dec_limit = max_items // 2 if max_items else None
    txt_limit = (max_items - (dec_limit or 0)) if max_items else None

    count = 0
    for item in fetch_jsonapi_pages(DECISIONS_ENDPOINT, max_items=dec_limit):
        doc = normalize_decision(item)
        if doc:
            yield doc
            count += 1
            if count % 10 == 0:
                print(f"  Fetched {count} decisions...", file=sys.stderr)

    count = 0
    for item in fetch_jsonapi_pages(TEXTES_ENDPOINT, max_items=txt_limit):
        doc = normalize_texte(item)
        if doc:
            yield doc
            count += 1
            if count % 10 == 0:
                print(f"  Fetched {count} legal texts...", file=sys.stderr)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch records modified since a given date."""
    for doc in fetch_all():
        if doc.get("date") and doc["date"] >= since:
            yield doc


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
    parser = argparse.ArgumentParser(description='ARCOM Data Fetcher')
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
