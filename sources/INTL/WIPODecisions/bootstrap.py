#!/usr/bin/env python3
"""
INTL/WIPODecisions - WIPO UDRP Domain Name Dispute Decisions Fetcher

Fetches UDRP panel decisions from WIPO Arbitration and Mediation Center.
100K+ decisions. HTML full text for 1999-2021, PDF-only for 2022+.

Data source: https://www.wipo.int/amc/en/domains/decisionsx/
Method: JSP list pages for metadata, HTML decision pages for full text
License: Free access

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
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

BASE_URL = "https://www.wipo.int"
INDEX_URL = "https://www.wipo.int/amc/en/domains/decisionsx/index.html"
LIST_URL = "https://www.wipo.int/amc/en/domains/decisionsx/list.jsp"
TEXT_URL = "https://www.wipo.int/amc/en/domains/search/text.jsp"
SOURCE_ID = "INTL/WIPODecisions"
SAMPLE_DIR = Path(__file__).parent / "sample"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
}

RATE_LIMIT_DELAY = 1.5

# Maximum year for HTML full text (2022+ is PDF-only)
MAX_HTML_YEAR = 2021


def clean_html(html_str: str) -> str:
    """Strip HTML tags and decode entities."""
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    return unescape(soup.get_text(separator="\n", strip=True))


def get_list_page_urls(session: requests.Session) -> list:
    """Parse the master index to get all list.jsp URLs."""
    resp = session.get(INDEX_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    urls = []
    matches = re.findall(
        r'list\.jsp\?prefix=D&year=(\d+)&seq_min=(\d+)&seq_max=(\d+)',
        resp.text
    )
    for year, seq_min, seq_max in matches:
        year_int = int(year)
        if year_int > MAX_HTML_YEAR:
            continue  # Skip PDF-only years
        urls.append({
            "year": year_int,
            "seq_min": int(seq_min),
            "seq_max": int(seq_max),
            "url": f"{LIST_URL}?prefix=D&year={year}&seq_min={seq_min}&seq_max={seq_max}",
        })
    return urls


def parse_list_page(session: requests.Session, url: str) -> list:
    """Parse a list.jsp page to extract case metadata."""
    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    cases = []
    soup = BeautifulSoup(resp.text, "html.parser")

    # Find all rows in the results table
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        # First cell has case number link
        link = cells[0].find("a", href=True)
        if not link:
            continue

        case_match = re.search(r'case=(D\d{4}-\d{4,5})', link.get("href", ""))
        if not case_match:
            continue

        case_number = case_match.group(1)
        complainant = cells[1].get_text(strip=True)
        respondent = cells[2].get_text(strip=True)
        domain_names = cells[3].get_text(strip=True)
        outcome = cells[4].get_text(strip=True)

        cases.append({
            "case_number": case_number,
            "complainant": complainant,
            "respondent": respondent,
            "domain_names": domain_names,
            "outcome": outcome,
        })

    return cases


def fetch_decision_text(session: requests.Session, case_number: str) -> Optional[dict]:
    """Fetch full text of a decision via text.jsp redirect."""
    url = f"{TEXT_URL}?case={case_number}"
    try:
        resp = session.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Warning: failed to fetch {case_number}: {e}")
        return None

    final_url = resp.url
    # Skip PDFs
    if final_url.endswith(".pdf"):
        return None

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    # Extract meta tags
    meta = {}
    for tag in soup.find_all("meta", attrs={"name": True}):
        meta[tag["name"]] = tag.get("content", "")

    # Extract body text
    body = soup.find("body")
    text = unescape(body.get_text(separator="\n", strip=True)) if body else ""

    if len(text) < 200:
        return None

    return {
        "text": text,
        "html_url": final_url,
        "meta": meta,
    }


def normalize(case_meta: dict, decision: dict) -> dict:
    """Transform case metadata + decision text into standard schema."""
    meta = decision.get("meta", {})
    date_str = meta.get("date", "")
    # Validate date format
    if not re.match(r'\d{4}-\d{2}-\d{2}', date_str):
        date_str = None

    case_num = case_meta["case_number"]
    domains = case_meta.get("domain_names", meta.get("domains", ""))
    complainant = case_meta.get("complainant", meta.get("complainants", ""))

    title = f"{complainant} v. {case_meta.get('respondent', 'N/A')} ({case_num})"

    return {
        "_id": f"WIPO/{case_num}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": decision["text"],
        "date": date_str,
        "url": decision["html_url"],
        "case_number": case_num,
        "domain_names": domains,
        "complainant": complainant,
        "respondent": case_meta.get("respondent", ""),
        "outcome": case_meta.get("outcome", ""),
        "language": "en",
    }


def fetch_all(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Yield all normalized decision records."""
    print("Fetching WIPO UDRP decision index...")
    list_pages = get_list_page_urls(session)
    print(f"Found {len(list_pages)} list pages (1999-{MAX_HTML_YEAR})")

    if sample:
        # For sample, just fetch from a few recent years
        list_pages = [lp for lp in list_pages if lp["year"] == 2020 and lp["seq_min"] == 1]

    count = 0
    for lp in list_pages:
        print(f"  Fetching list: year={lp['year']}, seq={lp['seq_min']}-{lp['seq_max']}...")
        cases = parse_list_page(session, lp["url"])
        time.sleep(RATE_LIMIT_DELAY)

        for case_meta in cases:
            if sample and count >= 15:
                return

            decision = fetch_decision_text(session, case_meta["case_number"])
            if decision is None:
                continue

            yield normalize(case_meta, decision)
            count += 1
            time.sleep(RATE_LIMIT_DELAY)

            if count % 50 == 0:
                print(f"    Fetched {count} decisions...")


def test_connection():
    """Test connectivity."""
    print("Testing WIPO AMC decisions...")
    session = requests.Session()

    # Test index
    resp = session.get(INDEX_URL, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    matches = re.findall(r'list\.jsp\?prefix=D&year=(\d+)', resp.text)
    years = sorted(set(int(y) for y in matches))
    print(f"OK — Index accessible, years: {years[0]}-{years[-1]}")

    # Test a decision
    decision = fetch_decision_text(session, "D2020-0001")
    if decision:
        print(f"D2020-0001: {len(decision['text'])} chars, date={decision['meta'].get('date')}")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    session = requests.Session()
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    saved = 0
    for rec in fetch_all(session, sample=sample):
        safe_id = re.sub(r'[^\w\-]', '_', rec["_id"])
        path = SAMPLE_DIR / f"{safe_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1

        if sample and saved >= 15:
            break

    print(f"Saved {saved} records to {SAMPLE_DIR}")
    return saved


def main():
    parser = argparse.ArgumentParser(description="INTL/WIPODecisions bootstrap")
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
