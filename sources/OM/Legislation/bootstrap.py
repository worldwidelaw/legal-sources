#!/usr/bin/env python3
"""
OM/Legislation — Oman Legal Database (qanoon.om)

Fetches Arabic-language legislation from qanoon.om, the official Omani legal
database maintained by the Ministry of Justice and Legal Affairs.

Strategy:
  - WordPress REST API (wp-json/wp/v2/posts) paginated by category
  - ~11,800+ legal instruments: Royal Decrees (4,875), Ministerial Decisions (4,446),
    Official Gazette (875), Legal Opinions (687), International Agreements (680),
    Amended Laws (98), Royal Orders (84), Supreme Committee Decisions (83), etc.
  - Full Arabic text in post content.rendered field

Source: https://qanoon.om/
Rate limit: 1.5 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Connectivity test
"""

import sys
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

SOURCE_ID = "OM/Legislation"
BASE_URL = "https://qanoon.om/wp-json/wp/v2"
SAMPLE_DIR = Path(__file__).parent / "sample"
RATE_LIMIT = 1.5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("legal-data-hunter.OM.Legislation")

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "application/json",
}

# Category ID -> English name mapping
CATEGORIES = {
    2: "royal_decree",
    3: "ministerial_decision",
    131: "official_gazette",
    349: "legal_opinion",
    156: "international_agreement",
    152: "amended_law",
    133: "royal_order",
    313: "supreme_committee_decision",
    311: "traditional_law",
    367: "circular",
    344: "amended_regulation",
}

session = requests.Session()
session.headers.update(HEADERS)


def clean_html(html_str: str) -> str:
    """Strip HTML tags and decode entities, returning clean text."""
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    # Remove script/style elements
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    # Collapse excessive whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def api_get(endpoint: str, params: dict = None) -> requests.Response:
    """Make a GET request to the WordPress API with rate limiting."""
    url = f"{BASE_URL}{endpoint}"
    resp = session.get(url, params=params, timeout=30)
    resp.raise_for_status()
    time.sleep(RATE_LIMIT)
    return resp


def fetch_categories() -> dict:
    """Fetch all categories from the API."""
    resp = api_get("/categories", {"per_page": 100})
    cats = {}
    for cat in resp.json():
        cats[cat["id"]] = {
            "name": cat["name"],
            "count": cat["count"],
            "slug": cat["slug"],
        }
    return cats


def fetch_posts_page(category_id: int, page: int) -> tuple:
    """Fetch a page of posts for a category. Returns (posts, total_pages)."""
    resp = api_get("/posts", {
        "categories": category_id,
        "per_page": 100,
        "page": page,
        "_fields": "id,date,title,content,link,categories,tags",
    })
    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
    return resp.json(), total_pages


def normalize(post: dict, category_id: int) -> dict:
    """Transform a WordPress post into the standard schema."""
    post_id = post["id"]
    title = clean_html(post.get("title", {}).get("rendered", ""))
    raw_content = post.get("content", {}).get("rendered", "")
    text = clean_html(raw_content)
    date_str = post.get("date", "")[:10] if post.get("date") else None
    link = post.get("link", f"https://qanoon.om/?p={post_id}")
    leg_type = CATEGORIES.get(category_id, "legislation")

    # Extract year from date or title
    year = None
    if date_str and len(date_str) >= 4:
        year = date_str[:4]

    doc_id = f"OM-{leg_type}-{post_id}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": link,
        "legislation_type": leg_type,
        "legislation_year": year,
        "language": "ar",
        "wp_post_id": post_id,
        "wp_category_id": category_id,
    }


def fetch_all(sample: bool = False):
    """Yield all normalized legislation records."""
    cats = fetch_categories()
    log.info(f"Found {len(cats)} categories")

    for cat_id, cat_info in sorted(cats.items()):
        cat_name = CATEGORIES.get(cat_id, cat_info["name"])
        count = cat_info["count"]
        if count == 0:
            continue

        log.info(f"Category {cat_name} ({cat_id}): {count} posts")

        page = 1
        fetched = 0
        while True:
            try:
                posts, total_pages = fetch_posts_page(cat_id, page)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 400:
                    # Past last page
                    break
                raise

            if not posts:
                break

            for post in posts:
                rec = normalize(post, cat_id)
                if rec["text"] and len(rec["text"]) > 50:
                    yield rec
                    fetched += 1
                    if sample and fetched >= 3:
                        break

            if sample and fetched >= 3:
                break

            log.info(f"  Page {page}/{total_pages}: {len(posts)} posts (total fetched: {fetched})")

            if page >= total_pages:
                break
            page += 1

        log.info(f"  {cat_name}: {fetched} records with text")


def test_connection():
    """Test API connectivity."""
    log.info("Testing qanoon.om WordPress API...")

    cats = fetch_categories()
    total = sum(c["count"] for c in cats.values())
    log.info(f"Categories: {len(cats)}, total posts: {total}")
    for cid, info in sorted(cats.items()):
        log.info(f"  {cid}: {info['name']} ({info['count']} posts)")

    # Test fetching one post
    log.info("Fetching sample post (Royal Decrees)...")
    posts, pages = fetch_posts_page(2, 1)
    if posts:
        rec = normalize(posts[0], 2)
        log.info(f"  Title: {rec['title'][:100]}")
        log.info(f"  Text length: {len(rec['text'])} chars")
        log.info(f"  Date: {rec['date']}")
        log.info(f"  URL: {rec['url']}")
        log.info(f"  Text preview: {rec['text'][:200]}...")
    log.info("Test complete!")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = []
    for rec in fetch_all(sample=sample):
        records.append(rec)
        if len(records) % 100 == 0:
            log.info(f"Progress: {len(records)} records collected")

    log.info(f"Total records with text: {len(records)}")

    if sample:
        to_save = sorted(records, key=lambda r: len(r.get("text", "")), reverse=True)[:15]
    else:
        to_save = records

    saved = 0
    for rec in to_save:
        safe_id = re.sub(r"[^\w\-]", "_", rec["_id"])
        path = SAMPLE_DIR / f"{safe_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1

    log.info(f"Saved {saved} records to {SAMPLE_DIR}")

    has_text = sum(1 for r in to_save if r.get("text") and len(r["text"]) > 100)
    log.info(f"Records with substantial text: {has_text}/{saved}")

    if to_save:
        avg_len = sum(len(r.get("text", "")) for r in to_save) // len(to_save)
        log.info(f"Average text length: {avg_len} chars")

    return saved


def main():
    import argparse
    parser = argparse.ArgumentParser(description="OM/Legislation bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true")
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
