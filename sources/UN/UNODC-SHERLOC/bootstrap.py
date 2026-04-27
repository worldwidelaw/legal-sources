#!/usr/bin/env python3
"""
UN/UNODC-SHERLOC - UNODC Sharing Electronic Resources and Laws on Crime

Fetches case law and legislation from the SHERLOC database via its JSON search
API and HTML detail pages.

Data source: https://sherloc.unodc.org/ (redirects to www.unodc.org/cld/)
Method: JSON search API for listing + HTML page scraping for legislation full text
License: United Nations / UNODC
Rate limit: ~2 seconds between requests

Databases covered: Case Law (3,373+), Legislation (13,926+)
Crime types: Trafficking, smuggling, organized crime, cybercrime, terrorism,
             firearms, money laundering, corruption, drug offences, piracy, etc.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import json
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

SOURCE_ID = "UN/UNODC-SHERLOC"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://www.unodc.org"

CASE_LAW_ENDPOINT = "/cld/v3/sherloc/cldb/data.json"
LEGISLATION_ENDPOINT = "/cld/v3/sherloc/legdb/data.json"

CASE_LAW_PAGE_SIZE = 10  # Fixed by API
LEGISLATION_PAGE_SIZE = 100  # Fixed by API

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
}

DELAY = 2  # seconds between requests


def strip_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html_text)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_json(url: str, session: requests.Session) -> Optional[dict]:
    """Fetch JSON with retries."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code >= 500:
                print(f"  Server error {resp.status_code}, retrying...")
                time.sleep(DELAY * 2)
                continue
            print(f"  HTTP {resp.status_code} for {url[:80]}")
            return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt + 1}): {e}")
            time.sleep(DELAY)
    return None


def fetch_html(url: str, session: requests.Session) -> Optional[str]:
    """Fetch HTML page with retries."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code >= 500:
                time.sleep(DELAY * 2)
                continue
            return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt + 1}): {e}")
            time.sleep(DELAY)
    return None


def extract_legislation_text(html: str) -> str:
    """Extract the 'Original Text' section from a legislation detail page."""
    match = re.search(
        r'class="originalText"[^>]*>(.*?)(?:</div>\s*</div>\s*</div>\s*<div class="clear"|$)',
        html, re.DOTALL
    )
    if match:
        return strip_html(match.group(1))
    # Fallback: try broader match
    match = re.search(r'Original Text</h3>(.*?)(?:Cross-Cutting Issues|$)', html, re.DOTALL)
    if match:
        return strip_html(match.group(1))
    return ""


def extract_case_sections(html: str) -> dict:
    """Extract structured sections from a case law detail page."""
    sections = {}
    for section_name, css_class in [
        ("fact_summary", "factSummary"),
        ("commentary", "commentaryAndSignificantFeatures"),
        ("procedural_history", "procedural-history"),
    ]:
        match = re.search(
            rf'class="{css_class}"[^>]*>(.*?)(?:</div>\s*</div>\s*</div>|$)',
            html, re.DOTALL
        )
        if match:
            sections[section_name] = strip_html(match.group(1))
    return sections


def build_search_url(endpoint: str, start_at: int = 0) -> str:
    """Build a search API URL with pagination."""
    criteria = json.dumps({"startAt": start_at}, separators=(",", ":"))
    return f"{BASE_URL}{endpoint}?lng=en&criteria={urllib.parse.quote(criteria)}"


def normalize_case_law(result: dict) -> dict:
    """Normalize a case law record from the search API."""
    values = result.get("values", {})
    uri = result.get("uri", values.get("uri", ""))

    fact_summary_html = values.get("//caseLaw/factSummary/html", "")
    text = strip_html(fact_summary_html)

    crime_types_raw = values.get("en#__el.caseLaw.crimeTypes_s", [])
    if isinstance(crime_types_raw, str):
        crime_types_raw = [crime_types_raw]

    date_str = values.get("caseLaw@decisionVerdictDate_s1", "")
    date_iso = None
    if date_str:
        # Parse dates like "Tue Oct 15 00:00:00 CEST 2024" or ISO formats
        for fmt in ["%Y-%m-%d", "%a %b %d %H:%M:%S %Z %Y"]:
            try:
                date_iso = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                break
            except ValueError:
                continue
        if not date_iso:
            # Try extracting year from string
            year_match = re.search(r"\b(19|20)\d{2}\b", date_str)
            if year_match:
                date_iso = year_match.group(0)

    unodc_no = values.get("caseLaw@unodcNo_s1", "")
    doc_id = unodc_no or uri.rstrip("/").split("/")[-1].replace(".html", "")

    return {
        "_id": f"SHERLOC-CL-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": values.get("page_title", ""),
        "text": text,
        "date": date_iso,
        "url": f"{BASE_URL}/cld{uri}?lng=en&tmpl=sherloc" if uri else "",
        "country": values.get("en#caseLaw@country_label_s1", ""),
        "country_code": values.get("caseLaw@country_s1", ""),
        "unodc_number": unodc_no,
        "crime_types": crime_types_raw,
    }


def normalize_legislation(result: dict, text: str = "") -> dict:
    """Normalize a legislation record."""
    values = result.get("values", {})
    uri = result.get("uri", values.get("uri", ""))

    crime_types_raw = values.get("en#__el.legislation.crimeTypes_s", [])
    if isinstance(crime_types_raw, str):
        crime_types_raw = [crime_types_raw]

    law_title = values.get("legislation.nationalLawArticle@title_s1", "")
    article = values.get("legislation.nationalLawArticle@article_s1", "")
    chapter = values.get("legislation.nationalLawArticle@chapterDescription_s1", "")

    title_parts = [law_title]
    if chapter:
        title_parts.append(chapter)
    if article:
        title_parts.append(article)
    title = " — ".join(p for p in title_parts if p)

    # Use the numeric ID from the search result for uniqueness
    result_id = result.get("id", "")
    # Extract numeric prefix if present (e.g., "2200,en,/legislation/...")
    id_num = result_id.split(",")[0] if result_id else ""
    # Also use path for readability
    id_parts = [p for p in uri.split("/") if p and p != "legislation" and not p.endswith(".html")]
    path_slug = "_".join(id_parts[-2:]) if len(id_parts) >= 2 else "_".join(id_parts)
    doc_id = f"{id_num}_{path_slug}" if id_num else path_slug

    return {
        "_id": f"SHERLOC-LEG-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": None,
        "url": f"{BASE_URL}/cld{uri}?lng=en&tmpl=sherloc" if uri else "",
        "country": values.get("en#legislation@country_label_s1", ""),
        "country_code": values.get("legislation@country_s1", ""),
        "article": article,
        "chapter": chapter,
        "law_title": law_title,
        "crime_types": crime_types_raw,
    }


def fetch_case_law(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all case law records from the search API."""
    start_at = 0
    total = None
    fetched = 0
    limit = 15 if sample else None

    while True:
        url = build_search_url(CASE_LAW_ENDPOINT, start_at)
        print(f"  Fetching case law page at offset {start_at}...")
        data = fetch_json(url, session)
        if not data:
            print("  Failed to fetch case law page, stopping.")
            break

        if total is None:
            total = data.get("found", 0)
            print(f"  Total case law records: {total}")

        results = data.get("results", [])
        if not results:
            break

        for result in results:
            record = normalize_case_law(result)
            if record["text"]:
                yield record
                fetched += 1
                if limit and fetched >= limit:
                    return
            else:
                print(f"  Skipping case {record.get('unodc_number', '?')} — no text")

        start_at += CASE_LAW_PAGE_SIZE
        if start_at >= total:
            break
        time.sleep(DELAY)

    print(f"  Case law: fetched {fetched} records with text")


def fetch_legislation(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Fetch legislation records. Requires fetching detail pages for full text."""
    start_at = 0
    total = None
    fetched = 0
    skipped = 0
    limit = 15 if sample else None

    while True:
        url = build_search_url(LEGISLATION_ENDPOINT, start_at)
        print(f"  Fetching legislation page at offset {start_at}...")
        data = fetch_json(url, session)
        if not data:
            print("  Failed to fetch legislation page, stopping.")
            break

        if total is None:
            total = data.get("found", 0)
            print(f"  Total legislation records: {total}")

        results = data.get("results", [])
        if not results:
            break

        for result in results:
            uri = result.get("uri", "")
            if not uri:
                continue

            # Fetch the detail page for full text
            detail_url = f"{BASE_URL}/cld{uri}?lng=en&tmpl=sherloc"
            time.sleep(DELAY)
            html = fetch_html(detail_url, session)

            text = ""
            if html:
                text = extract_legislation_text(html)

            record = normalize_legislation(result, text)
            if record["text"]:
                yield record
                fetched += 1
                if limit and fetched >= limit:
                    print(f"  Legislation sample: fetched {fetched}, skipped {skipped}")
                    return
            else:
                skipped += 1
                if skipped <= 5:
                    print(f"  No text for: {record['title'][:60]}")

        start_at += LEGISLATION_PAGE_SIZE
        if start_at >= total:
            break
        time.sleep(DELAY)

    print(f"  Legislation: fetched {fetched}, skipped {skipped} (no text)")


def save_record(record: dict, sample_dir: Path) -> None:
    """Save a record to the sample directory."""
    import hashlib
    sample_dir.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
    if len(safe_id) > 80:
        h = hashlib.md5(record["_id"].encode()).hexdigest()[:8]
        safe_id = safe_id[:70] + "_" + h
    filename = safe_id + ".json"
    filepath = sample_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def test_connectivity() -> bool:
    """Test that the SHERLOC API is reachable."""
    session = requests.Session()
    print("Testing SHERLOC API connectivity...")

    # Test case law endpoint
    url = build_search_url(CASE_LAW_ENDPOINT, 0)
    data = fetch_json(url, session)
    if not data:
        print("FAIL: Cannot reach case law API")
        return False
    print(f"  Case law API: OK ({data.get('found', 0)} records)")

    # Test legislation endpoint
    url = build_search_url(LEGISLATION_ENDPOINT, 0)
    data = fetch_json(url, session)
    if not data:
        print("FAIL: Cannot reach legislation API")
        return False
    print(f"  Legislation API: OK ({data.get('found', 0)} records)")

    # Test detail page
    test_uri = "/legislation/afg/amendments_to_the_anti-money_laundering_and_proceeds_of_crime_law/chapter_ii/article_11/article_11.html"
    detail_url = f"{BASE_URL}/cld{test_uri}?lng=en&tmpl=sherloc"
    html = fetch_html(detail_url, session)
    if html and "Original Text" in html:
        print("  Detail page: OK (Original Text found)")
    else:
        print("  Detail page: WARNING (no Original Text section)")

    print("All tests passed.")
    return True


def bootstrap(sample: bool = False) -> None:
    """Run the bootstrap process."""
    session = requests.Session()
    sample_dir = SAMPLE_DIR
    records_saved = 0

    if sample:
        # Clear sample dir
        if sample_dir.exists():
            for f in sample_dir.glob("*.json"):
                f.unlink()

    print(f"{'Sample' if sample else 'Full'} bootstrap starting...")

    # Fetch case law (text is in the search API response)
    print("\n--- Case Law ---")
    for record in fetch_case_law(session, sample=sample):
        save_record(record, sample_dir)
        records_saved += 1
        if records_saved % 50 == 0:
            print(f"  Saved {records_saved} records...")

    case_law_count = records_saved

    # Fetch legislation (requires detail page fetch for text)
    print("\n--- Legislation ---")
    leg_start = records_saved
    for record in fetch_legislation(session, sample=sample):
        save_record(record, sample_dir)
        records_saved += 1
        if records_saved % 50 == 0:
            print(f"  Saved {records_saved} records...")

    leg_count = records_saved - leg_start

    print(f"\nBootstrap complete: {records_saved} records saved")
    print(f"  Case law: {case_law_count}")
    print(f"  Legislation: {leg_count}")
    print(f"  Output: {sample_dir}")

    if records_saved == 0:
        print("ERROR: No records saved!")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="UN/UNODC-SHERLOC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only sample records (~15 per type)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
