#!/usr/bin/env python3
"""
INTL/UNIDROIT - UNIDROIT Instruments Full Text Fetcher

Fetches conventions, model laws, principles, and guides from UNIDROIT
(International Institute for the Unification of Private Law).

Data source: https://www.unidroit.org/instruments/
Method: WordPress REST API (/wp-json/wp/v2/pages)
License: Free access

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

API_BASE = "https://www.unidroit.org/wp-json/wp/v2/pages"
SOURCE_ID = "INTL/UNIDROIT"
SAMPLE_DIR = Path(__file__).parent / "sample"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "application/json",
}

RATE_LIMIT_DELAY = 1.5

# URL path segments that indicate non-instrument pages (skip these)
SKIP_SEGMENTS = {
    "depositary", "states-parties", "preparatory-work",
    "preparatory-history", "status", "ratifications", "declarations",
    "bibliography", "conferences", "events",
    "news", "related-instruments", "translations",
    "online-consultation", "consulta",
}

# Instrument categories to classify
INSTRUMENT_CATEGORIES = {
    "convention": ["convention", "protocol", "ccv", "cmr", "ulis", "ulfc"],
    "model_law": ["model-law"],
    "principles": ["principles", "upicc"],
    "guide": ["guide", "best-practices", "model-examples", "netting"],
}


def clean_html(html_str: str) -> str:
    """Strip HTML tags, WPBakery shortcodes, and decode entities."""
    if not html_str:
        return ""
    # Remove WPBakery shortcodes like [vc_row], [/vc_column_text], etc.
    text = re.sub(r'\[/?vc_[^\]]*\]', '', html_str)
    # Remove other shortcodes
    text = re.sub(r'\[/?[a-z_]+[^\]]*\]', '', text)
    soup = BeautifulSoup(text, "html.parser")
    return unescape(soup.get_text(separator="\n", strip=True))


def classify_instrument(url: str, title: str) -> str:
    """Classify instrument type based on URL and title."""
    url_lower = url.lower()
    title_lower = title.lower()
    for itype, keywords in INSTRUMENT_CATEGORIES.items():
        for kw in keywords:
            if kw in url_lower or kw in title_lower:
                return itype
    return "instrument"


def is_instrument_page(link: str) -> bool:
    """Filter to keep only actual instrument text pages."""
    if "/instruments/" not in link:
        return False
    if "/fr/" in link or link.endswith("-fr/") or "-fr/" in link:
        return False
    # Skip administrative/meta pages
    parts = link.rstrip("/").split("/")
    for segment in parts:
        if segment in SKIP_SEGMENTS:
            return False
    return True


def fetch_all_instrument_pages(session: requests.Session) -> list:
    """Fetch all instrument pages via WP REST API."""
    all_pages = []
    page_num = 1

    while True:
        url = f"{API_BASE}?per_page=100&page={page_num}"
        resp = session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 400:
            break
        resp.raise_for_status()

        data = resp.json()
        if not data:
            break

        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        print(f"  Fetching WP pages {page_num}/{total_pages}...")

        for p in data:
            link = p.get("link", "")
            if is_instrument_page(link):
                content_html = p["content"]["rendered"]
                text = clean_html(content_html)
                # Skip very short pages (navigation/index with no substance)
                if len(text) < 500:
                    continue
                all_pages.append({
                    "wp_id": p["id"],
                    "title": unescape(p["title"]["rendered"]),
                    "link": link,
                    "content_html": content_html,
                    "text": text,
                    "date": p["date"],
                    "modified": p["modified"],
                    "slug": p["slug"],
                    "parent": p["parent"],
                })

        page_num += 1
        if page_num > total_pages:
            break
        time.sleep(RATE_LIMIT_DELAY)

    return all_pages


def normalize(page: dict) -> dict:
    """Transform a WP page into the standard schema."""
    title = page["title"]
    link = page["link"]
    text = page["text"]

    # Build a meaningful ID from the URL path
    path = link.replace("https://www.unidroit.org/instruments/", "").strip("/")
    doc_id = f"UNIDROIT/{path}" if path else f"UNIDROIT/wp-{page['wp_id']}"

    instrument_type = classify_instrument(link, title)

    # Parse dates
    date_str = page["date"][:10] if page["date"] else None
    modified_str = page["modified"][:10] if page["modified"] else None

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "date_modified": modified_str,
        "url": link,
        "instrument_type": instrument_type,
        "language": "en",
    }


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Yield all normalized instrument records."""
    print("Fetching UNIDROIT instrument pages via WP REST API...")
    pages = fetch_all_instrument_pages(session)
    print(f"Found {len(pages)} instrument pages with substantial content")

    for page in pages:
        yield normalize(page)


def fetch_updates(session: requests.Session, since: str) -> Generator[dict, None, None]:
    """Yield instruments modified since a given date."""
    print(f"Fetching UNIDROIT instruments modified since {since}...")
    page_num = 1

    while True:
        url = (
            f"{API_BASE}?per_page=100&page={page_num}"
            f"&modified_after={since}T00:00:00&orderby=modified&order=asc"
        )
        resp = session.get(url, headers=HEADERS, timeout=30)
        if resp.status_code == 400:
            break
        resp.raise_for_status()

        data = resp.json()
        if not data:
            break

        for p in data:
            link = p.get("link", "")
            if not is_instrument_page(link):
                continue
            content_html = p["content"]["rendered"]
            text = clean_html(content_html)
            if len(text) < 500:
                continue
            page = {
                "wp_id": p["id"],
                "title": unescape(p["title"]["rendered"]),
                "link": link,
                "content_html": content_html,
                "text": text,
                "date": p["date"],
                "modified": p["modified"],
                "slug": p["slug"],
                "parent": p["parent"],
            }
            yield normalize(page)

        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        page_num += 1
        if page_num > total_pages:
            break
        time.sleep(RATE_LIMIT_DELAY)


def test_connection():
    """Test API connectivity."""
    print("Testing UNIDROIT WP REST API...")
    session = requests.Session()
    resp = session.get(f"{API_BASE}?per_page=1", headers=HEADERS, timeout=15)
    resp.raise_for_status()
    total = resp.headers.get("X-WP-Total", "?")
    print(f"OK — API accessible, {total} total pages")

    # Test an instrument page
    resp2 = session.get(
        f"{API_BASE}?slug=cape-town-convention&per_page=1",
        headers=HEADERS, timeout=15,
    )
    resp2.raise_for_status()
    data = resp2.json()
    if data:
        text = clean_html(data[0]["content"]["rendered"])
        print(f"Cape Town Convention: {len(text)} chars of text")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    session = requests.Session()
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = list(fetch_all(session))
    print(f"Total records: {len(records)}")

    if sample:
        # Save the 15 records with most text content
        to_save = sorted(records, key=lambda r: len(r.get("text", "")), reverse=True)[:15]
    else:
        to_save = records

    saved = 0
    for rec in to_save:
        safe_id = re.sub(r'[^\w\-]', '_', rec["_id"])
        path = SAMPLE_DIR / f"{safe_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1

    print(f"Saved {saved} records to {SAMPLE_DIR}")

    # Validation summary
    has_text = sum(1 for r in to_save if r.get("text") and len(r["text"]) > 100)
    print(f"Records with substantial text: {has_text}/{saved}")

    if to_save:
        avg_len = sum(len(r.get("text", "")) for r in to_save) // len(to_save)
        print(f"Average text length: {avg_len} chars")

    return saved


def main():
    parser = argparse.ArgumentParser(description="INTL/UNIDROIT bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        test_connection()
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            print("ERROR: No records fetched", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
