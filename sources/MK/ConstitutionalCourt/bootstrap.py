#!/usr/bin/env python3
"""
North Macedonia Constitutional Court (Уставен суд) Data Fetcher

Fetches case law from the Constitutional Court of North Macedonia
via the WordPress REST API.

Data source: https://ustavensud.mk
- 7,900+ documents (1991-present)
- Decisions (одлуки), resolutions (решенија), separate opinions
- Full text in content.rendered field
- Macedonian language, no authentication required
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests
from bs4 import BeautifulSoup

# Constants
API_URL = "https://ustavensud.mk/wp-json/wp/v2/posts"
BASE_URL = "https://ustavensud.mk"
RATE_LIMIT_DELAY = 1
PAGE_SIZE = 100

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal-data-collection)",
    "Accept": "application/json",
}

# Case law categories: decisions (80), resolutions (35), separate opinions (36)
CASE_LAW_CATEGORIES = "80,35,36"

# Category ID to label mapping
CATEGORY_LABELS = {
    80: "одлука",       # decision
    35: "решение",      # resolution
    36: "издвоено мислење",  # separate opinion
}


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n\s*\n", "\n\n", text)
    return text.strip()


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all case law posts via WP REST API."""
    page = 1
    total = None
    fetched = 0

    while True:
        params = {
            "categories": CASE_LAW_CATEGORIES,
            "per_page": PAGE_SIZE,
            "page": page,
            "orderby": "date",
            "order": "desc",
            "_fields": "id,date,title,content,link,categories",
        }

        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)

        if response.status_code == 400:
            # Past last page
            break

        response.raise_for_status()

        if total is None:
            total = int(response.headers.get("X-WP-Total", 0))
            total_pages = int(response.headers.get("X-WP-TotalPages", 0))
            print(f"Total posts: {total} ({total_pages} pages)", file=sys.stderr)

        posts = response.json()
        if not posts:
            break

        for post in posts:
            yield post
            fetched += 1
            if max_docs and fetched >= max_docs:
                return

        page += 1
        time.sleep(RATE_LIMIT_DELAY)

    print(f"Fetched {fetched}/{total} posts", file=sys.stderr)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch posts modified since a given date."""
    page = 1

    while True:
        params = {
            "categories": CASE_LAW_CATEGORIES,
            "per_page": PAGE_SIZE,
            "page": page,
            "after": since.isoformat(),
            "orderby": "date",
            "order": "desc",
            "_fields": "id,date,title,content,link,categories",
        }

        response = requests.get(API_URL, params=params, headers=HEADERS, timeout=30)
        if response.status_code == 400:
            break
        response.raise_for_status()

        posts = response.json()
        if not posts:
            break

        for post in posts:
            yield post

        page += 1
        time.sleep(RATE_LIMIT_DELAY)


def normalize(post: dict) -> dict:
    """Transform a WP post into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    wp_id = post["id"]
    title = clean_html(post["title"]["rendered"])
    text = clean_html(post["content"]["rendered"])
    date = post.get("date", "")[:10]  # YYYY-MM-DD
    link = post.get("link", f"{BASE_URL}/archives/{wp_id}")

    # Determine doc category from categories
    cats = post.get("categories", [])
    doc_category = "unknown"
    for cat_id, label in CATEGORY_LABELS.items():
        if cat_id in cats:
            doc_category = label
            break

    return {
        "_id": f"MK-CC-{wp_id}",
        "_source": "MK/ConstitutionalCourt",
        "_type": "case_law",
        "_fetched_at": now,
        "title": title,
        "text": text,
        "date": date,
        "url": link,
        "language": "mk",
        "wp_id": wp_id,
        "doc_category": doc_category,
        "category_ids": cats,
    }


def bootstrap_sample(sample_dir: Path, count: int = 100) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    empty_text = 0

    for post in fetch_all(max_docs=count):
        record = normalize(post)

        if not record["text"]:
            empty_text += 1
            continue

        samples.append(record)

        filename = re.sub(r"[^\w\-.]", "_", f"{record['_id']}.json")
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text'])} chars)", file=sys.stderr)

    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Skipped (no text): {empty_text}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)

        by_cat = {}
        for s in samples:
            c = s.get("doc_category", "unknown")
            by_cat[c] = by_cat.get(c, 0) + 1
        print(f"\nBy category:", file=sys.stderr)
        for c, n in sorted(by_cat.items(), key=lambda x: -x[1]):
            print(f"  {c}: {n}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="North Macedonia Constitutional Court fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Generate sample data only")
    parser.add_argument("--count", type=int, default=100,
                        help="Number of samples to generate")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            for post in fetch_all():
                record = normalize(post)
                if record["text"]:
                    print(json.dumps(record, ensure_ascii=False))

    elif args.command == "fetch":
        for post in fetch_all(max_docs=args.count if args.sample else None):
            record = normalize(post)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for post in fetch_updates(since):
            record = normalize(post)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
