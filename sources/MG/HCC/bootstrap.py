#!/usr/bin/env python3
"""
MG/HCC — Madagascar Haute Cour Constitutionnelle (High Constitutional Court)

Fetches constitutional court decisions via the WordPress REST API.
Categories: Arrêts, Avis, D1 (conventions), D2 (unconstitutionality),
D3 (national legislation), Elections, Délibérations.

Source: https://www.hcc.gov.mg
Total: ~1,331 decisions across all categories.
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

SOURCE_ID = "MG/HCC"
BASE_URL = "https://www.hcc.gov.mg"
API_ROUTE = "/?rest_route=/wp/v2/posts"
PER_PAGE = 20
DELAY = 1.5

# Decision categories (exclude admin/news: communiqué, marchés publics, recrutements, etc.)
DECISION_CATEGORIES = {
    3: "Arrêts",
    20: "Arrêts",
    4: "Avis",
    7: "D1-Conventions Internationales",
    8: "D2-Exceptions d'inconstitutionnalité",
    9: "D3-Législation nationale",
    6: "Elections",
    24: "DB-Délibération",
}

CATEGORY_TO_TYPE = {
    3: "case_law",
    20: "case_law",
    4: "case_law",
    7: "case_law",
    8: "case_law",
    9: "case_law",
    6: "case_law",
    24: "case_law",
}


def get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; academic research)",
        "Accept": "application/json",
    })
    return s


def strip_html(text):
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def classify_decision(title, categories):
    """Determine decision sub-type from title pattern."""
    title_lower = title.lower()
    if "/ar" in title_lower or "arrêt" in title_lower:
        return "arrêt"
    if "/av" in title_lower or "avis" in title_lower:
        return "avis"
    if "/d1" in title_lower:
        return "décision_d1_convention"
    if "/d2" in title_lower:
        return "décision_d2_inconstitutionnalité"
    if "/d3" in title_lower:
        return "décision_d3_législation"
    if "/db" in title_lower or "délibération" in title_lower:
        return "délibération"
    if 6 in categories:
        return "élection"
    return "décision"


def extract_decision_number(title):
    """Extract decision reference number from title."""
    m = re.search(r'n[°º]?\s*(\d+[-/]HCC[-/][A-Z0-9]+)', title)
    if m:
        return m.group(1)
    m = re.search(r'n[°º]?\s*(\d+[-/][A-Z0-9/]+)', title)
    if m:
        return m.group(1)
    return None


def extract_decision_date(title, post_date):
    """Extract the decision date from title, falling back to post date."""
    m = re.search(r'du\s+(\d{1,2})\s+(\w+)\s+(\d{4})', title)
    if m:
        day, month_fr, year = m.group(1), m.group(2).lower(), m.group(3)
        months = {
            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
        }
        mm = months.get(month_fr)
        if mm:
            return f"{year}-{mm}-{int(day):02d}"
    return post_date[:10] if post_date else None


def fetch_posts(session, categories, page=1, per_page=PER_PAGE):
    """Fetch a page of posts filtered by category IDs."""
    cat_str = ",".join(str(c) for c in categories)
    url = f"{BASE_URL}{API_ROUTE}&per_page={per_page}&page={page}&categories={cat_str}"
    resp = session.get(url, timeout=30)
    if resp.status_code == 400:
        return [], 0
    resp.raise_for_status()
    total = int(resp.headers.get("X-WP-Total", 0))
    return resp.json(), total


def normalize(post):
    """Transform a WP post into standard schema."""
    title_raw = post["title"]["rendered"]
    title = html.unescape(title_raw)
    content_html = post["content"]["rendered"]
    text = strip_html(content_html)
    categories = post.get("categories", [])

    decision_number = extract_decision_number(title)
    decision_date = extract_decision_date(title, post.get("date"))
    decision_type = classify_decision(title, categories)

    doc_id = decision_number or str(post["id"])
    doc_id = doc_id.replace("/", "-")

    category_names = [DECISION_CATEGORIES.get(c, f"cat-{c}") for c in categories
                      if c in DECISION_CATEGORIES]

    return {
        "_id": f"MG-HCC-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": decision_date,
        "url": post.get("link", f"{BASE_URL}/?p={post['id']}"),
        "decision_number": decision_number,
        "decision_type": decision_type,
        "categories": category_names,
        "wp_id": post["id"],
    }


def fetch_all(session):
    """Yield all decision records from the WP REST API."""
    cat_ids = list(DECISION_CATEGORIES.keys())
    page = 1
    total = None

    while True:
        posts, total_count = fetch_posts(session, cat_ids, page=page)
        if total is None:
            total = total_count
            print(f"Total decisions to fetch: {total}")

        if not posts:
            break

        for post in posts:
            text = strip_html(post["content"]["rendered"])
            if len(text) < 100:
                continue
            yield normalize(post)

        print(f"  Page {page}: {len(posts)} posts")
        page += 1
        time.sleep(DELAY)


def fetch_updates(session, since):
    """Yield decisions modified since a given ISO date."""
    cat_ids = list(DECISION_CATEGORIES.keys())
    cat_str = ",".join(str(c) for c in cat_ids)
    page = 1

    while True:
        url = (f"{BASE_URL}{API_ROUTE}&per_page={PER_PAGE}&page={page}"
               f"&categories={cat_str}&modified_after={since}T00:00:00")
        resp = session.get(url, timeout=30)
        if resp.status_code == 400:
            break
        resp.raise_for_status()
        posts = resp.json()

        if not posts:
            break

        for post in posts:
            text = strip_html(post["content"]["rendered"])
            if len(text) < 100:
                continue
            yield normalize(post)

        page += 1
        time.sleep(DELAY)


def bootstrap_sample(n=15):
    """Download a sample of recent decisions for validation."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    session = get_session()
    cat_ids = list(DECISION_CATEGORIES.keys())
    count = 0
    page = 1

    while count < n:
        posts, total = fetch_posts(session, cat_ids, page=page, per_page=min(n * 2, 20))
        if not posts:
            break

        for post in posts:
            if count >= n:
                break

            text = strip_html(post["content"]["rendered"])
            if len(text) < 100:
                continue

            record = normalize(post)
            fname = f"{record['_id']}.json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"[{count + 1}/{n}] {record['title'][:80]}... ({len(record['text']):,} chars)")
            count += 1
            time.sleep(DELAY)

        page += 1

    print(f"\nDone: {count} samples saved to {sample_dir}")
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="MG/HCC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "fetch_all", "fetch_updates"])
    parser.add_argument("--sample", action="store_true", help="Run in sample mode")
    parser.add_argument("--since", help="ISO date for fetch_updates")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        else:
            session = get_session()
            count = 0
            for record in fetch_all(session):
                count += 1
                print(f"  [{count}] {record['title'][:60]}: {len(record['text']):,} chars")
            print(f"Total: {count} records")

    elif args.command == "fetch_updates":
        since = args.since or "2025-01-01"
        session = get_session()
        for record in fetch_updates(session, since):
            print(f"  {record['title'][:60]}: {len(record['text']):,} chars")

    elif args.command == "fetch_all":
        session = get_session()
        count = 0
        for record in fetch_all(session):
            count += 1
        print(f"Total: {count} records")
