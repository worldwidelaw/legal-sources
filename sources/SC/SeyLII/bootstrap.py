#!/usr/bin/env python3
"""
SC/SeyLII — Seychelles Court Decisions Fetcher

Fetches Seychelles court decisions from SeyLII (seylii.org), a Lexum/Norma
platform operated by AfricanLII.

Strategy:
  - Browse by date per court: /seylii/{court}/en/{YYYY}/nav_date.do
  - Three courts: sc (Supreme Court), ca (Court of Appeal), cc (Constitutional Court)
  - Paginate with ?page=N (25 items per page)
  - Fetch full text from /seylii/{court}/en/item/{id}/index.do?iframe=true
  - Extract text from <div id="document-content">

Source: https://seylii.org/
Rate limit: 2 sec between requests

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "SC/SeyLII"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SC.SeyLII")

BASE_URL = "https://www.seylii.org"

COURTS = [
    {"code": "sc", "name": "Supreme Court", "col_id": 1, "min_year": 1970, "max_pages_per_year": 30},
    {"code": "ca", "name": "Court of Appeal", "col_id": 2, "min_year": 1983, "max_pages_per_year": 10},
    {"code": "cc", "name": "Constitutional Court", "col_id": 3, "min_year": 2000, "max_pages_per_year": 5},
]

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h[1-6]>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</tr>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def list_decisions_for_year(court_code: str, year: int, max_pages: int = 30) -> list:
    """Fetch all decision links from a court's year listing."""
    docs = []
    seen_ids = set()

    for page in range(1, max_pages + 1):
        if year == datetime.now().year and page == 1:
            url = f"{BASE_URL}/seylii/{court_code}/en/nav_date.do?iframe=true"
        elif page == 1:
            url = f"{BASE_URL}/seylii/{court_code}/en/{year}/nav_date.do?iframe=true"
        else:
            if year == datetime.now().year:
                url = f"{BASE_URL}/seylii/{court_code}/en/nav_date.do?iframe=true&page={page}"
            else:
                url = f"{BASE_URL}/seylii/{court_code}/en/{year}/nav_date.do?iframe=true&page={page}"

        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 404:
                break
            resp.raise_for_status()
        except requests.exceptions.HTTPError:
            break
        except Exception as e:
            logger.warning(f"Failed to fetch {court_code}/{year} page {page}: {e}")
            break

        html = resp.text

        # Extract item links: /seylii/{court}/en/item/{id}/index.do
        items = re.findall(
            r'href="[^"]*?/seylii/' + court_code + r'/en/item/(\d+)/index\.do[^"]*"[^>]*>\s*(.*?)\s*</a>',
            html,
            re.DOTALL,
        )

        page_count = 0
        for item_id, title_html in items:
            if item_id in seen_ids:
                continue
            seen_ids.add(item_id)
            title = re.sub(r'<[^>]+>', '', title_html).strip()
            if not title:
                continue

            # Extract citation if present nearby
            citation = ""
            cit_pattern = rf'item/{item_id}/index\.do[^"]*"[^>]*>.*?</a>\s*(?:<[^>]+>\s*)*\[(\d{{4}})\]\s+(\w+)\s+(\d+)'
            cit_match = re.search(cit_pattern, html, re.DOTALL)
            if cit_match:
                citation = f"[{cit_match.group(1)}] {cit_match.group(2)} {cit_match.group(3)}"

            docs.append({
                "item_id": item_id,
                "title": title,
                "citation": citation,
                "court_code": court_code,
            })
            page_count += 1

        if page_count == 0:
            break

        time.sleep(2.0)

    return docs


def fetch_decision_text(court_code: str, item_id: str) -> tuple:
    """Fetch full text and metadata from a decision page. Returns (text, date, citation)."""
    url = f"{BASE_URL}/seylii/{court_code}/en/item/{item_id}/index.do?iframe=true"
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"Failed to fetch item {item_id}: {e}")
        return "", "", ""

    html = resp.text

    # Extract document content
    text = ""
    # Try <div id="document-content">
    m = re.search(r'<div\s+id="document-content"[^>]*>(.*?)</div>\s*(?:<div|<footer|<script|$)', html, re.DOTALL)
    if m:
        text = clean_html(m.group(1))

    # Fallback: try <div class="documentcontent">
    if not text:
        m = re.search(r'<div\s+class="documentcontent"[^>]*>(.*?)</div>\s*(?:<div\s+class="|<footer|<script|$)', html, re.DOTALL)
        if m:
            text = clean_html(m.group(1))

    # Fallback: try the main content area
    if not text:
        m = re.search(r'<div\s+id="content"[^>]*>(.*?)</div>\s*(?:<footer|<script)', html, re.DOTALL)
        if m:
            text = clean_html(m.group(1))

    # Extract date
    date = ""
    date_m = re.search(r'<span[^>]*class="[^"]*date[^"]*"[^>]*>\s*(\d{1,2}[/-]\d{1,2}[/-]\d{4})\s*</span>', html)
    if date_m:
        raw = date_m.group(1)
        try:
            for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y"):
                try:
                    dt = datetime.strptime(raw, fmt)
                    date = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    # Try ISO date in meta or elsewhere
    if not date:
        date_m = re.search(r'(\d{4}-\d{2}-\d{2})', html[:5000])
        if date_m:
            date = date_m.group(1)

    # Extract citation
    citation = ""
    cit_m = re.search(r'\[(\d{4})\]\s+(SCS[Cc]|SCCA|SCCC)\s+(\d+)', html)
    if cit_m:
        citation = f"[{cit_m.group(1)}] {cit_m.group(2).upper()} {cit_m.group(3)}"

    return text, date, citation


def normalize(doc_info: dict, full_text: str, date: str, citation: str) -> dict:
    """Normalize a document record to standard schema."""
    court_code = doc_info["court_code"]
    court_names = {"sc": "Supreme Court", "ca": "Court of Appeal", "cc": "Constitutional Court"}
    court_name = court_names.get(court_code, court_code.upper())

    item_id = doc_info["item_id"]
    doc_id = f"seylii-{court_code}-{item_id}"

    final_citation = citation or doc_info.get("citation", "")
    title = doc_info["title"]
    if final_citation and final_citation not in title:
        title = f"{title} {final_citation}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title.strip(),
        "text": full_text,
        "date": date,
        "url": f"{BASE_URL}/seylii/{court_code}/en/item/{item_id}/index.do",
        "court": court_name,
        "citation": final_citation,
        "language": "en",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all court decision records."""
    count = 0
    errors = 0
    empty = 0
    limit = 15 if sample else None
    current_year = datetime.now().year

    for court in COURTS:
        if limit and count >= limit:
            break

        court_code = court["code"]
        court_name = court["name"]
        min_year = court["min_year"]
        logger.info(f"Processing {court_name} ({court_code}) from {min_year}...")

        # Iterate years from most recent to oldest
        for year in range(current_year, min_year - 1, -1):
            if limit and count >= limit:
                break

            docs = list_decisions_for_year(court_code, year, court["max_pages_per_year"])
            if not docs:
                continue

            logger.info(f"  {court_code}/{year}: {len(docs)} decisions found")

            for doc_info in docs:
                if limit and count >= limit:
                    break

                try:
                    full_text, date, citation = fetch_decision_text(court_code, doc_info["item_id"])
                except Exception as e:
                    logger.error(f"Error fetching item {doc_info['item_id']}: {e}")
                    errors += 1
                    if errors > 50:
                        logger.error("Too many errors, stopping")
                        return
                    continue

                if not full_text or len(full_text) < 50:
                    empty += 1
                    continue

                record = normalize(doc_info, full_text, date, citation)
                count += 1
                logger.info(f"  [{count}] {record['title'][:60]}... ({len(full_text)} chars)")
                yield record
                time.sleep(2.0)

    logger.info(f"Done. Fetched {count} decisions, {empty} empty, {errors} errors.")


def save_sample(record: dict) -> None:
    """Save a record to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fname = f"{record['_id'][:80]}.json"
    path = SAMPLE_DIR / fname
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def cmd_test_api():
    """Test connectivity."""
    print("Testing SeyLII connectivity...")

    # Test Supreme Court listing
    url = f"{BASE_URL}/seylii/sc/en/nav_date.do?iframe=true"
    resp = SESSION.get(url, timeout=15)
    print(f"Status: {resp.status_code}")

    items = re.findall(r'/seylii/sc/en/item/(\d+)/index\.do', resp.text)
    unique = list(set(items))
    print(f"OK: Found {len(unique)} Supreme Court decisions on current year page")

    if unique:
        item_id = unique[0]
        text, date, citation = fetch_decision_text("sc", item_id)
        print(f"Item {item_id}: {len(text)} chars, date={date}, citation={citation}")
        if text:
            print(f"Preview: {text[:300]}...")
        else:
            print("No text found for first item, trying next...")
            for iid in unique[1:4]:
                time.sleep(2.0)
                text, date, citation = fetch_decision_text("sc", iid)
                if text:
                    print(f"Item {iid}: {len(text)} chars")
                    print(f"Preview: {text[:300]}...")
                    break


def cmd_bootstrap(sample: bool = False):
    """Run the bootstrap."""
    count = 0
    for record in fetch_all(sample=sample):
        if sample:
            save_sample(record)
        count += 1

    if sample:
        print(f"\nSaved {count} sample records to {SAMPLE_DIR}/")
    else:
        print(f"\nFetched {count} court decisions.")

    if count == 0:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SC/SeyLII Court Decisions Fetcher")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("test-api", help="Test connectivity")
    boot = sub.add_parser("bootstrap", help="Fetch court decisions")
    boot.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        cmd_test_api()
    elif args.command == "bootstrap":
        cmd_bootstrap(sample=args.sample)
    else:
        parser.print_help()
        sys.exit(1)
