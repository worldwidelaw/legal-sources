#!/usr/bin/env python3
"""
FO/Logting - Faroe Islands Legislation Fetcher

Fetches all Faroese legislation from logir.fo (Logasavnið),
the official law collection of the Faroe Islands.

Data source: https://logir.fo/
License: Public Domain (official government publications)
Languages: Faroese, Danish
Total laws: ~7,300+
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://logir.fo"
GRID_URL = f"{BASE_URL}/Grid.aspx"
PAGE_SIZE = 200
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "FO/Logting"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": "https://logir.fo/",
}

# Faroese month names to numbers
FAROESE_MONTHS = {
    "januar": 1, "februar": 2, "mars": 3, "apríl": 4,
    "mai": 5, "juni": 6, "juli": 7, "august": 8,
    "september": 9, "oktober": 10, "november": 11, "desember": 12,
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def parse_faroese_date(date_str: str) -> Optional[str]:
    """Parse Faroese date like '13. apríl 2026' to ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip().rstrip(".")
    m = re.match(r"(\d{1,2})\.\s*(\w+)\s+(\d{4})", date_str)
    if not m:
        return None
    day, month_name, year = m.groups()
    month_num = FAROESE_MONTHS.get(month_name.lower())
    if not month_num:
        return None
    try:
        return f"{int(year):04d}-{month_num:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def fetch_grid_page(page: int) -> list[dict]:
    """Fetch a page of law entries from Grid.aspx."""
    data = {
        "Pager.CurrentPage": str(page),
        "Pager.PageSize": str(PAGE_SIZE),
        "Sorter.SortField": "lst.Date",
        "Sorter.SortDirection": "Desc",
    }
    resp = SESSION.post(GRID_URL, data=data, headers={
        "Content-Type": "application/x-www-form-urlencoded",
    }, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    entries = []

    for row in soup.select("tr"):
        tds = row.find_all("td")
        if len(tds) < 8:
            continue

        # Extract link from td[1]
        link_el = tds[1].find("a")
        if not link_el or not link_el.get("href"):
            continue

        href = link_el["href"]
        law_type_text = tds[2].get_text(strip=True)
        law_number = tds[3].get_text(strip=True).replace("nr.", "").strip()
        date_text = tds[5].get_text(strip=True)
        title_el = tds[7].find("a")
        title = title_el.get_text(strip=True) if title_el else tds[7].get_text(strip=True)
        status = tds[8].get_text(strip=True) if len(tds) > 8 else ""

        # Build full title with type prefix
        full_title = f"{law_type_text} nr. {law_number} - {title}" if law_number else title

        entries.append({
            "href": href,
            "law_type": law_type_text,
            "law_number": law_number,
            "date_text": date_text,
            "title": full_title,
            "status": status,
        })

    return entries


def fetch_full_text(href: str) -> Optional[str]:
    """Fetch full text of a law from its page."""
    url = f"{BASE_URL}/{href.lstrip('/')}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Try div#div_content first, then .content_body
    content = soup.select_one("div#div_content")
    if not content:
        content = soup.select_one(".content_body")
    if not content:
        # Fallback: find the largest text block
        best = None
        best_len = 0
        for div in soup.find_all("div"):
            text = div.get_text(strip=True)
            if len(text) > best_len:
                best_len = len(text)
                best = div
        content = best

    if not content:
        return None

    text = content.get_text(separator="\n", strip=True)
    return text if len(text) > 20 else None


def normalize(entry: dict, text: str) -> dict:
    """Transform raw entry into standard schema."""
    href = entry["href"]
    date_iso = parse_faroese_date(entry["date_text"])

    # Build unique ID from href slug
    slug = href.strip("/").replace("/", "_")
    doc_id = f"FO_{slug}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": entry["title"],
        "text": text,
        "date": date_iso,
        "url": f"{BASE_URL}/{href.lstrip('/')}",
        "law_type": entry["law_type"],
        "law_number": entry["law_number"],
        "status": entry["status"],
        "language": "fo",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all law records with full text."""
    page = 1
    total = 0

    while True:
        print(f"Fetching index page {page}...")
        entries = fetch_grid_page(page)
        if not entries:
            break

        for entry in entries:
            text = fetch_full_text(entry["href"])
            if text:
                total += 1
                yield normalize(entry, text)
            else:
                print(f"  Warning: no text for {entry['href']}")
            time.sleep(2)

        page += 1

    print(f"Total records fetched: {total}")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Yield documents modified since a date (ISO format)."""
    since_date = datetime.fromisoformat(since)
    page = 1

    while True:
        entries = fetch_grid_page(page)
        if not entries:
            break

        for entry in entries:
            date_iso = parse_faroese_date(entry["date_text"])
            if date_iso and datetime.fromisoformat(date_iso) < since_date:
                return
            text = fetch_full_text(entry["href"])
            if text:
                yield normalize(entry, text)
            time.sleep(2)

        page += 1


def bootstrap_sample(count: int = 15):
    """Fetch sample records for testing."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    saved = 0
    page = 1

    while saved < count:
        print(f"Fetching index page {page}...")
        entries = fetch_grid_page(page)
        if not entries:
            break

        for entry in entries:
            if saved >= count:
                break

            print(f"  [{saved + 1}/{count}] {entry['title'][:70]}...")
            text = fetch_full_text(entry["href"])
            if not text:
                print(f"    Skipped (no text)")
                continue

            record = normalize(entry, text)
            slug = re.sub(r"[^\w-]", "_", entry["href"].strip("/"))[:80]
            out_file = SAMPLE_DIR / f"{slug}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            saved += 1
            print(f"    Saved ({len(text)} chars)")
            time.sleep(2)

        page += 1

    print(f"\nSample complete: {saved} records saved to {SAMPLE_DIR}")
    return saved


def main():
    parser = argparse.ArgumentParser(description="FO/Logting legislation fetcher")
    parser.add_argument("command", choices=["bootstrap", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true",
                        help="Fetch all records (full bootstrap)")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            bootstrap_sample(args.count)
        else:
            count = 0
            for record in fetch_all():
                count += 1
                if count % 100 == 0:
                    print(f"  Progress: {count} records...")
            print(f"Full bootstrap complete: {count} records")
    elif args.command == "updates":
        if not args.since:
            print("Error: --since required for updates command")
            sys.exit(1)
        count = 0
        for record in fetch_updates(args.since):
            count += 1
        print(f"Updates complete: {count} records since {args.since}")


if __name__ == "__main__":
    main()
