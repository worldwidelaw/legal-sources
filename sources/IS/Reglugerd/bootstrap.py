#!/usr/bin/env python3
"""
IS/Reglugerd - Iceland Regulations Collection Fetcher

Fetches all Icelandic regulations from reglugerd.is, the official government
regulations database maintained by the Icelandic Ministry of Justice.

Data source: https://www.reglugerd.is/ (redirects to island.is/reglugerdir)
Listing API: island.is GraphQL (getRegulations)
Full text: HTML pages at reglugerd.is/reglugerdir/allar/nr/{number}-{year}
License: Public Domain (official government publications)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

GRAPHQL_URL = "https://island.is/api/graphql"
DETAIL_URL_TEMPLATE = "https://www.reglugerd.is/reglugerdir/allar/nr/{name}"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/Reglugerd"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

GRAPHQL_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": HEADERS["User-Agent"],
    "Accept": "application/json",
}

DELAY = 1.5  # seconds between requests


def list_regulations(page: int = 1) -> dict:
    """Fetch a page of regulation listings via GraphQL."""
    query = '{ getRegulations(input: { type: "newest", page: %d }) }' % page
    resp = requests.post(
        GRAPHQL_URL,
        headers=GRAPHQL_HEADERS,
        json={"query": query},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise RuntimeError(f"GraphQL error: {data['errors']}")
    return data["data"]["getRegulations"]


def fetch_all_listings(max_pages: int = 0) -> Generator[dict, None, None]:
    """Paginate through all regulation listings."""
    page = 1
    while True:
        print(f"  Fetching listing page {page}...", flush=True)
        result = list_regulations(page)
        items = result.get("data", [])
        if not items:
            break
        for item in items:
            yield item
        total_pages = result.get("totalPages", 0)
        if page >= total_pages:
            break
        if max_pages and page >= max_pages:
            break
        page += 1
        time.sleep(0.5)


def name_to_url_slug(name: str) -> str:
    """Convert regulation name like '0322/2026' to URL slug '0322-2026'."""
    return name.replace("/", "-")


def fetch_regulation_html(name: str) -> Optional[str]:
    """Fetch the full HTML page for a regulation."""
    slug = name_to_url_slug(name)
    url = DETAIL_URL_TEMPLATE.format(name=slug)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return resp.text
    except requests.exceptions.HTTPError as e:
        print(f"    HTTP {e.response.status_code} for {url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    Request error for {url}: {e}")
        return None


def extract_text_from_html(html: str) -> tuple[str, Optional[str]]:
    """Extract regulation text and publication date from HTML page.

    Returns (text, date_str) where date_str is ISO format or None.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Find the regulation article body
    article = soup.find("div", class_="reglugerd")
    if not article:
        return "", None

    body = article.find("div", class_="boxbody")
    if not body:
        return "", None

    # Extract date from the search index fields comment or from signature
    date_str = None
    # Try to find date in eplica-search-index-fields comment
    comments = body.find_all(string=lambda t: t and "ArticleDate=" in str(t))
    for comment in comments:
        m = re.search(r"ArticleDate=(\d{2})\.(\d{2})\.(\d{4})", str(comment))
        if m:
            day, month, year = m.groups()
            date_str = f"{year}-{month}-{day}"
            break

    # If no date from comment, try signature date
    if not date_str:
        sig_date = body.find("span", class_="signature__date")
        if sig_date:
            # Parse Icelandic date like "12. mars 2026"
            date_text = sig_date.get_text(strip=True).rstrip(".")
            date_str = _parse_icelandic_date(date_text)

    # Remove the search index comment, navigation, print button etc.
    for elem in body.find_all("div", class_="buttons"):
        elem.decompose()
    for elem in body.find_all("div", class_="rinfo"):
        elem.decompose()

    # Get the meta (regulation number) and title
    meta = body.find("p", class_="meta")
    if meta:
        meta.decompose()

    # Extract clean text: articles, paragraphs, signatures
    text_parts = []
    for elem in body.find_all(["h1", "h2", "h3", "p", "li", "table"]):
        text = elem.get_text(separator=" ", strip=True)
        if text:
            text_parts.append(text)

    full_text = "\n\n".join(text_parts)
    # Clean up multiple whitespace
    full_text = re.sub(r"[ \t]+", " ", full_text)
    full_text = re.sub(r"\n{3,}", "\n\n", full_text)

    return full_text.strip(), date_str


ICELANDIC_MONTHS = {
    "janúar": "01", "febrúar": "02", "mars": "03", "apríl": "04",
    "maí": "05", "júní": "06", "júlí": "07", "ágúst": "08",
    "september": "09", "október": "10", "nóvember": "11", "desember": "12",
}


def _parse_icelandic_date(text: str) -> Optional[str]:
    """Parse dates like '12. mars 2026' to '2026-03-12'."""
    m = re.match(r"(\d{1,2})\.\s*(\w+)\s+(\d{4})", text)
    if not m:
        return None
    day, month_name, year = m.groups()
    month = ICELANDIC_MONTHS.get(month_name.lower())
    if not month:
        return None
    return f"{year}-{month}-{int(day):02d}"


def normalize(listing: dict, text: str, date_str: Optional[str]) -> dict:
    """Normalize a regulation record into standard schema."""
    name = listing.get("name", "")
    # Parse number and year from name like "0322/2026"
    parts = name.split("/")
    reg_number = parts[0] if parts else name
    year = parts[1] if len(parts) > 1 else ""

    slug = name_to_url_slug(name)
    url = DETAIL_URL_TEMPLATE.format(name=slug)

    # Use date from HTML, or fall back to Jan 1 of the year
    if not date_str and year:
        date_str = f"{year}-01-01"

    ministry = listing.get("ministry", {})

    return {
        "_id": f"IS-reg-{slug}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": listing.get("title", ""),
        "text": text,
        "date": date_str,
        "url": url,
        "regulation_number": reg_number,
        "year": year,
        "regulation_type": listing.get("type", ""),
        "ministry": ministry.get("name", "") if isinstance(ministry, dict) else "",
        "ministry_slug": ministry.get("slug", "") if isinstance(ministry, dict) else "",
        "repealed": listing.get("repealed", False),
        "original_doc": listing.get("originalDoc", ""),
    }


def fetch_all(max_pages: int = 0) -> Generator[dict, None, None]:
    """Yield all regulations with full text."""
    count = 0
    errors = 0
    for listing in fetch_all_listings(max_pages=max_pages):
        name = listing.get("name", "")
        print(f"  [{count+1}] Fetching {name}: {listing.get('title', '')[:60]}...")

        html = fetch_regulation_html(name)
        if not html:
            errors += 1
            continue

        text, date_str = extract_text_from_html(html)
        if not text:
            print(f"    WARNING: Empty text for {name}")
            errors += 1
            continue

        record = normalize(listing, text, date_str)
        count += 1
        yield record
        time.sleep(DELAY)

    print(f"\nDone: {count} regulations fetched, {errors} errors")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch regulations updated since a date.

    The GraphQL listing returns newest first, so we paginate until we find
    regulations older than `since`.
    """
    since_dt = datetime.fromisoformat(since)
    for listing in fetch_all_listings():
        name = listing.get("name", "")
        parts = name.split("/")
        year = int(parts[1]) if len(parts) > 1 else 0
        if year < since_dt.year:
            break

        html = fetch_regulation_html(name)
        if not html:
            continue

        text, date_str = extract_text_from_html(html)
        if not text:
            continue

        record = normalize(listing, text, date_str)
        yield record
        time.sleep(DELAY)


def bootstrap_sample(count: int = 12) -> list[dict]:
    """Fetch a sample of regulations for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Get first page of listings
    result = list_regulations(page=1)
    items = result.get("data", [])

    # Pick a mix: some base regulations, some amending
    base_items = [i for i in items if i.get("type") == "base"]
    amending_items = [i for i in items if i.get("type") == "amending"]

    selected = base_items[:6] + amending_items[:6]
    if len(selected) < count:
        selected = items[:count]

    records = []
    for listing in selected[:count]:
        name = listing.get("name", "")
        print(f"  Fetching sample: {name} — {listing.get('title', '')[:60]}...")

        html = fetch_regulation_html(name)
        if not html:
            print(f"    SKIP: could not fetch HTML")
            continue

        text, date_str = extract_text_from_html(html)
        if not text:
            print(f"    SKIP: empty text")
            continue

        record = normalize(listing, text, date_str)
        records.append(record)

        # Save individual sample
        safe_name = name.replace("/", "-")
        sample_path = SAMPLE_DIR / f"{safe_name}.json"
        with open(sample_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    OK: {len(text)} chars, date={date_str}")
        time.sleep(DELAY)

    print(f"\nSample: {len(records)}/{count} regulations fetched")
    return records


def main():
    parser = argparse.ArgumentParser(description="IS/Reglugerd bootstrap")
    parser.add_argument("action", choices=["bootstrap", "fetch_all", "fetch_updates"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--since", type=str, help="ISO date for incremental fetch")
    parser.add_argument("--max-pages", type=int, default=0, help="Max listing pages")
    args = parser.parse_args()

    if args.action == "bootstrap":
        if args.sample or True:  # bootstrap always runs in sample mode
            records = bootstrap_sample()
            print(f"\nValidation:")
            print(f"  Records: {len(records)}")
            if records:
                text_lens = [len(r.get("text", "")) for r in records]
                print(f"  Text lengths: min={min(text_lens)}, max={max(text_lens)}, avg={sum(text_lens)//len(text_lens)}")
                has_text = sum(1 for r in records if r.get("text"))
                has_date = sum(1 for r in records if r.get("date"))
                print(f"  With text: {has_text}/{len(records)}")
                print(f"  With date: {has_date}/{len(records)}")

    elif args.action == "fetch_all":
        for record in fetch_all(max_pages=args.max_pages):
            pass

    elif args.action == "fetch_updates":
        if not args.since:
            print("Error: --since required for fetch_updates", file=sys.stderr)
            sys.exit(1)
        for record in fetch_updates(args.since):
            pass


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
