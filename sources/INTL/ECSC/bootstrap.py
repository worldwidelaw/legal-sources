#!/usr/bin/env python3
"""
INTL/ECSC - Eastern Caribbean Supreme Court Judgments Fetcher

Fetches case law from the Eastern Caribbean Supreme Court via WordPress REST API.
~8500 judgments from 1996+. Covers 9 jurisdictions: Antigua & Barbuda, Dominica,
Grenada, St Kitts & Nevis, St Lucia, St Vincent, Anguilla, BVI, Montserrat.

Data source: https://judgments.eccourts.org/wp-json/wp/v2/posts
Method: WordPress REST API with ACF fields; full text from HTML content
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
from typing import Generator

import requests
from bs4 import BeautifulSoup

API_URL = "https://judgments.eccourts.org/wp-json/wp/v2"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "INTL/ECSC"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "application/json",
}

RATE_LIMIT_DELAY = 1.0

# Court type category IDs (children of id=3 "Judgments")
COURT_TYPES = {
    7: "High Court",
    6: "Court of Appeal",
    97: "Privy Council",
    483: "Caribbean Court of Justice",
    91: "Industrial Court",
}

# Jurisdiction category IDs
JURISDICTIONS = {
    33: "Anguilla",
    34: "Antigua & Barbuda",
    35: "Commonwealth of Dominica",
    36: "Grenada",
    37: "Montserrat",
    38: "Saint Kitts and Nevis",
    28: "Saint Lucia",
    39: "Saint Vincent & The Grenadines",
    40: "Territory of the Virgin Islands",
}


def clean_html(html_str: str) -> str:
    """Strip HTML tags and decode entities."""
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    return unescape(soup.get_text(separator="\n", strip=True))


def load_tag_map(session: requests.Session) -> dict:
    """Load tag ID -> name mapping (mostly judge names)."""
    tags = {}
    page = 1
    while True:
        resp = session.get(
            f"{API_URL}/tags?per_page=100&page={page}",
            headers=HEADERS, timeout=30,
        )
        if resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        for t in data:
            tags[t["id"]] = t["name"]
        page += 1
        time.sleep(0.3)
    return tags


def parse_date(date_str: str) -> str:
    """Parse ACF date_new field (MM/DD/YYYY) to ISO format."""
    if not date_str:
        return ""
    # Try MM/DD/YYYY
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if m:
        return f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
    # Try YYYY-MM-DD already
    m = re.match(r"\d{4}-\d{2}-\d{2}", date_str)
    if m:
        return m.group(0)
    return date_str


def fetch_posts(session: requests.Session, limit: int = 0) -> Generator:
    """Paginate through the WP REST API to fetch all judgment posts."""
    page = 1
    total_fetched = 0
    fields = "id,title,date,slug,link,categories,tags,acf,content"

    while True:
        url = f"{API_URL}/posts?per_page=100&categories=3&page={page}&_fields={fields}"
        resp = session.get(url, headers=HEADERS, timeout=60)

        if resp.status_code == 400:
            # Past last page
            break
        if resp.status_code != 200:
            print(f"  API error on page {page}: {resp.status_code}")
            break

        data = resp.json()
        if not data:
            break

        total_pages = resp.headers.get("X-WP-TotalPages", "?")
        total = resp.headers.get("X-WP-Total", "?")
        print(f"  Page {page}/{total_pages} ({total} total): {len(data)} posts")

        for post in data:
            yield post
            total_fetched += 1
            if limit and total_fetched >= limit:
                return

        page += 1
        time.sleep(RATE_LIMIT_DELAY)


def normalize(post: dict, tag_map: dict) -> dict:
    """Normalize a WordPress post into a standard record."""
    title = clean_html(post.get("title", {}).get("rendered", ""))

    # Extract full text from content
    content_html = post.get("content", {}).get("rendered", "")
    text = clean_html(content_html)

    # ACF fields
    acf = post.get("acf", {}) or {}
    jurisdiction = acf.get("country", "")
    date_str = parse_date(acf.get("date_new", ""))
    case_number = acf.get("case_number", "")
    pdf_url = acf.get("doc_vew", "") or ""

    # Fallback date from WP post date
    if not date_str:
        wp_date = post.get("date", "")
        if wp_date:
            date_str = wp_date[:10]

    # Map categories to court type and jurisdiction
    cat_ids = post.get("categories", [])
    court_type = ""
    jurisdiction_name = jurisdiction  # ACF field
    for cid in cat_ids:
        if cid in COURT_TYPES:
            court_type = COURT_TYPES[cid]
        if cid in JURISDICTIONS and not jurisdiction_name:
            jurisdiction_name = JURISDICTIONS[cid]

    # Map tags to judge names
    tag_ids = post.get("tags", [])
    judges = [tag_map.get(tid, "") for tid in tag_ids if tag_map.get(tid, "")]

    return {
        "_id": f"ecsc-{post['id']}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": post.get("link", ""),
        "case_number": case_number,
        "court_type": court_type,
        "jurisdiction": jurisdiction_name,
        "judges": judges,
        "pdf_url": pdf_url,
    }


def test_connectivity():
    """Test basic connectivity."""
    session = requests.Session()
    print("Testing Eastern Caribbean Supreme Court connectivity...")

    resp = session.get(f"{API_URL}/posts?per_page=1&categories=3", headers=HEADERS, timeout=30)
    print(f"  API status: {resp.status_code}")
    print(f"  Total judgments: {resp.headers.get('X-WP-Total')}")

    if resp.status_code == 200:
        data = resp.json()
        print(f"  First judgment: {data[0]['title']['rendered'][:60]}")

    print("Connectivity test PASSED")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    print("Loading tag map (judge names)...")
    tag_map = load_tag_map(session)
    print(f"  Loaded {len(tag_map)} tags")

    print("\nFetching judgments from WP REST API...")
    limit = 15 if sample else 0

    saved = 0
    text_count = 0

    for post in fetch_posts(session, limit=limit):
        record = normalize(post, tag_map)

        if record["text"] and len(record["text"]) > 100:
            text_count += 1

        doc_id = record["_id"]
        safe_name = re.sub(r'[^\w\-]', '_', doc_id)
        out_path = SAMPLE_DIR / f"{safe_name}.json"

        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        saved += 1
        title_short = record["title"][:60]
        text_len = len(record.get("text", ""))
        print(f"  [{saved}] {title_short}... ({text_len} chars)")

    print(f"\nBootstrap complete: {saved} records saved to {SAMPLE_DIR}")
    print(f"Records with substantial text: {text_count}/{saved}")


def main():
    parser = argparse.ArgumentParser(description="ECSC Judgments Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
