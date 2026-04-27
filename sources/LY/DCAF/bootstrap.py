#!/usr/bin/env python3
"""
LY/DCAF - Libya DCAF Security Sector Legal Database

Fetches 2,175 Libyan legal texts from the DCAF (Geneva Centre for Security
Sector Governance) database via WordPress REST API.

Content types: Constitutional Law, Decrees, Laws, Resolutions, Judicial
Decisions, Bylaws, Declarations, International Agreements.

Full text in Arabic; partial English translations available.
License: Open access.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

SOURCE_ID = "LY/DCAF"
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"

# WordPress REST API endpoints
AR_BASE = "https://security-legislation.ly/ar/wp-json/wp/v2/latest-laws"
EN_BASE = "https://security-legislation.ly/wp-json/wp/v2/latest-laws"

# Taxonomy endpoints
TAXONOMY_ENDPOINTS = {
    "text_type": "https://security-legislation.ly/wp-json/wp/v2/text-type-categories?per_page=100",
    "status": "https://security-legislation.ly/wp-json/wp/v2/status-categories?per_page=100",
    "index": "https://security-legislation.ly/wp-json/wp/v2/database-index-categories?per_page=100",
    "institution": "https://security-legislation.ly/wp-json/wp/v2/institution-categories?per_page=100",
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "LegalDataHunter/1.0 (research)"})


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<p[^>]*>", "\n", text)
    text = re.sub(r"</p>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_taxonomies() -> dict:
    """Fetch taxonomy term mappings (id -> name)."""
    mappings = {}
    for tax_name, url in TAXONOMY_ENDPOINTS.items():
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 200:
                terms = resp.json()
                mappings[tax_name] = {t["id"]: strip_html(t["name"]) for t in terms}
        except Exception as e:
            print(f"  Warning: Could not fetch {tax_name} taxonomy: {e}")
    return mappings


def resolve_taxonomy(item: dict, tax_field: str, mappings: dict, tax_name: str) -> str:
    """Resolve taxonomy term IDs to names."""
    term_ids = item.get(tax_field, [])
    if not term_ids or tax_name not in mappings:
        return ""
    names = [mappings[tax_name].get(tid, "") for tid in term_ids]
    return "; ".join(n for n in names if n)


def normalize(item: dict, taxonomies: dict) -> dict:
    """Normalize a WP REST API item to standard schema."""
    wp_id = item.get("id", 0)
    title = strip_html(item.get("title", {}).get("rendered", ""))
    content_html = item.get("content", {}).get("rendered", "")
    full_text = strip_html(content_html)

    # Check for English-only placeholder
    if "ONLY AVAILABLE IN ARABIC" in full_text:
        full_text = ""

    date_str = item.get("date", "")[:10] if item.get("date") else None
    link = item.get("link", "")

    text_type = resolve_taxonomy(item, "text-type-categories", taxonomies, "text_type")
    status = resolve_taxonomy(item, "status-categories", taxonomies, "status")
    institution = resolve_taxonomy(item, "institution-categories", taxonomies, "institution")
    index_cat = resolve_taxonomy(item, "database-index-categories", taxonomies, "index")

    return {
        "_id": f"LY-DCAF-{wp_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date_str,
        "url": link,
        "language": "ar",
        "text_type": text_type,
        "status": status,
        "institution": institution,
        "index_category": index_cat,
    }


def iter_laws(limit: int = 0):
    """Iterate over all laws from the Arabic API endpoint."""
    print("Fetching taxonomy mappings...")
    taxonomies = fetch_taxonomies()
    for k, v in taxonomies.items():
        print(f"  {k}: {len(v)} terms")

    page = 1
    count = 0
    total = None

    while True:
        url = f"{AR_BASE}?per_page=100&page={page}"
        try:
            resp = SESSION.get(url, timeout=60)
            if resp.status_code == 400:
                break  # past last page
            resp.raise_for_status()
        except Exception as e:
            print(f"  Error fetching page {page}: {e}")
            break

        if total is None:
            total = int(resp.headers.get("X-WP-Total", 0))
            total_pages = int(resp.headers.get("X-WP-TotalPages", 0))
            print(f"  Total: {total} laws across {total_pages} pages")

        items = resp.json()
        if not items:
            break

        for item in items:
            record = normalize(item, taxonomies)
            if not record["text"] or len(record["text"]) < 50:
                continue
            yield record
            count += 1

            if limit and count >= limit:
                print(f"  Reached sample limit of {limit}")
                return

        page += 1
        time.sleep(0.5)

    print(f"  Processed {count} laws with full text")


def test_connectivity():
    """Test connectivity to the DCAF API."""
    print("Testing DCAF API connectivity...")

    # Test Arabic endpoint
    resp = SESSION.get(f"{AR_BASE}?per_page=1", timeout=30)
    total = resp.headers.get("X-WP-Total", "?")
    print(f"  Arabic endpoint: HTTP {resp.status_code}, {total} total items")

    # Test English endpoint
    resp2 = SESSION.get(f"{EN_BASE}?per_page=1", timeout=30)
    total2 = resp2.headers.get("X-WP-Total", "?")
    print(f"  English endpoint: HTTP {resp2.status_code}, {total2} total items")

    # Check if content has full text
    if resp.status_code == 200:
        items = resp.json()
        if items:
            content = items[0].get("content", {}).get("rendered", "")
            text = strip_html(content)
            print(f"  Sample content length: {len(text)} chars")
            print(f"  Sample title: {strip_html(items[0].get('title', {}).get('rendered', ''))[:80]}")

    print("Connectivity test complete")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    limit = 15 if sample else 0
    all_records = []
    saved = 0

    for record in iter_laws(limit=limit):
        out_path = SAMPLE_DIR / f"record_{saved:04d}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        all_records.append(record)
        saved += 1

    all_path = SAMPLE_DIR / "all_samples.json"
    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    print(f"\nBootstrap complete: {saved} records saved to {SAMPLE_DIR}")

    text_count = sum(1 for r in all_records if r.get("text") and len(r["text"]) > 100)
    print(f"  Records with substantial text: {text_count}/{saved}")

    if saved > 0 and text_count < saved * 0.5:
        print("WARNING: Less than 50% of records have substantial text")


def main():
    parser = argparse.ArgumentParser(description="LY/DCAF Libya Legal Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
