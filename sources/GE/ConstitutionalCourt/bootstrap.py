#!/usr/bin/env python3
"""
Georgian Constitutional Court (საქართველოს საკონსტიტუციო სასამართლო) data fetcher.

Fetches constitutional court decisions from constcourt.ge.
The court publishes decisions in Georgian, with some available in English.
Full text is available in HTML format within the judicial-acts pages.

Coverage: Constitutional complaints, rulings, judgments, dissenting opinions
"""

import argparse
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

BASE_URL = "https://www.constcourt.ge"
LIST_URL = f"{BASE_URL}/ka/judicial-acts"
USER_AGENT = "WorldWideLaw/1.0 (+https://github.com/worldwidelaw/legal-sources)"
REQUEST_DELAY = 2  # seconds between requests


def get_session() -> requests.Session:
    """Create a configured requests session."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5,ka;q=0.3",
    })
    return session


def parse_georgian_date(date_str: str) -> Optional[str]:
    """Parse Georgian date string to ISO format.

    Georgian months:
    იანვარი (January), თებერვალი (February), მარტი (March), აპრილი (April),
    მაისი (May), ივნისი (June), ივლისი (July), აგვისტო (August),
    სექტემბერი (September), ოქტომბერი (October), ნოემბერი (November), დეკემბერი (December)
    """
    if not date_str:
        return None

    georgian_months = {
        'იანვარი': '01', 'თებერვალი': '02', 'მარტი': '03', 'აპრილი': '04',
        'მაისი': '05', 'ივნისი': '06', 'ივლისი': '07', 'აგვისტო': '08',
        'სექტემბერი': '09', 'ოქტომბერი': '10', 'ნოემბერი': '11', 'დეკემბერი': '12'
    }

    # Clean up the string
    date_str = date_str.strip()

    # Try pattern: "20 იანვარი 2026"
    for month_ka, month_num in georgian_months.items():
        if month_ka in date_str:
            # Extract day and year
            match = re.search(r'(\d{1,2})\s+' + month_ka + r'\s+(\d{4})', date_str)
            if match:
                day = match.group(1).zfill(2)
                year = match.group(2)
                return f"{year}-{month_num}-{day}"

    # Try pattern: "DD.MM.YYYY"
    match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
    if match:
        day = match.group(1).zfill(2)
        month = match.group(2).zfill(2)
        year = match.group(3)
        return f"{year}-{month}-{day}"

    return None


def fetch_decision_ids(session: requests.Session, page: int = 1, quantity: int = 100) -> list[dict]:
    """Fetch decision IDs and basic metadata from listing page."""
    url = f"{LIST_URL}?quantity={quantity}&page={page}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching listing page {page}: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    decisions = []

    # Find all decision links with legal= parameter
    for link in soup.find_all("a", href=re.compile(r'\?legal=\d+')):
        href = link.get("href", "")
        match = re.search(r'legal=(\d+)', href)
        if match:
            legal_id = match.group(1)
            title = link.get_text(strip=True)
            if title and legal_id:
                decisions.append({
                    "legal_id": legal_id,
                    "title_preview": title[:200]
                })

    # Remove duplicates
    seen = set()
    unique_decisions = []
    for d in decisions:
        if d["legal_id"] not in seen:
            seen.add(d["legal_id"])
            unique_decisions.append(d)

    return unique_decisions


def fetch_decision(session: requests.Session, legal_id: str) -> Optional[dict]:
    """Fetch a single decision with full text."""
    url = f"{LIST_URL}?legal={legal_id}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching decision {legal_id}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract metadata from table
    metadata = {}
    table = soup.find("table", class_="legal-inner-table")
    if table:
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                key = cells[0].get_text(strip=True)
                value = cells[1].get_text(strip=True)

                # Map Georgian field names
                if 'ტიპი' in key:  # Document type
                    metadata['document_type'] = value
                elif 'ნომერი' in key:  # Number
                    metadata['case_number'] = value
                elif 'კოლეგია' in key or 'პლენუმი' in key:  # Chamber/Plenum
                    metadata['chamber'] = value
                elif 'ავტორ' in key:  # Author
                    metadata['author'] = value
                elif 'თარიღი' in key and 'გამოქვეყნების' not in key:  # Date (not publication date)
                    metadata['date_raw'] = value
                    metadata['date'] = parse_georgian_date(value)
                elif 'გამოქვეყნების' in key:  # Publication date
                    metadata['publication_date_raw'] = value
                    metadata['publication_date'] = parse_georgian_date(value)

    # Extract title from meta tag
    og_desc = soup.find("meta", property="og:description")
    title = og_desc.get("content", "") if og_desc else ""
    if not title:
        title = metadata.get("case_number", f"Decision {legal_id}")

    # Extract full text from legal-inner div
    legal_inner = soup.find("div", class_="legal-inner")
    text_parts = []

    if legal_inner:
        # Get all paragraphs and headings within the content area
        for elem in legal_inner.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            text = elem.get_text(strip=True)
            if text:
                text_parts.append(text)

    full_text = "\n\n".join(text_parts)

    # Clean up HTML entities
    full_text = full_text.replace('\u00a0', ' ')  # nbsp
    full_text = re.sub(r'\s+', ' ', full_text)  # normalize whitespace
    full_text = full_text.strip()

    if not full_text:
        print(f"Warning: No text content found for decision {legal_id}", file=sys.stderr)
        return None

    return {
        "legal_id": legal_id,
        "title": title,
        "text": full_text,
        "url": url,
        **metadata
    }


def normalize(raw: dict) -> dict:
    """Transform raw decision data into standard schema."""
    legal_id = raw.get("legal_id", "")
    case_number = raw.get("case_number", "")

    # Create unique ID
    _id = f"GE-CC-{legal_id}"
    if case_number:
        _id = f"GE-CC-{case_number.replace('/', '-').replace(' ', '')}"

    return {
        "_id": _id,
        "_source": "GE/ConstitutionalCourt",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", ""),
        "legal_id": legal_id,
        "case_number": case_number,
        "document_type": raw.get("document_type"),
        "chamber": raw.get("chamber"),
        "author": raw.get("author"),
        "publication_date": raw.get("publication_date"),
    }


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all decisions."""
    session = get_session()
    page = 1
    total_fetched = 0

    while True:
        print(f"Fetching page {page}...", file=sys.stderr)
        decisions = fetch_decision_ids(session, page=page, quantity=100)

        if not decisions:
            print(f"No more decisions found on page {page}", file=sys.stderr)
            break

        for decision_info in decisions:
            if limit and total_fetched >= limit:
                return

            legal_id = decision_info["legal_id"]
            time.sleep(REQUEST_DELAY)

            raw = fetch_decision(session, legal_id)
            if raw:
                yield normalize(raw)
                total_fetched += 1
                print(f"Fetched decision {legal_id} ({total_fetched})", file=sys.stderr)

        page += 1
        time.sleep(REQUEST_DELAY)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date.

    Note: The website doesn't have a clear update mechanism,
    so this fetches recent pages until we hit decisions older than 'since'.
    """
    session = get_session()
    since_date = datetime.fromisoformat(since.replace('Z', '+00:00')).date()
    page = 1

    while True:
        print(f"Fetching page {page}...", file=sys.stderr)
        decisions = fetch_decision_ids(session, page=page, quantity=100)

        if not decisions:
            break

        found_older = False

        for decision_info in decisions:
            legal_id = decision_info["legal_id"]
            time.sleep(REQUEST_DELAY)

            raw = fetch_decision(session, legal_id)
            if raw:
                decision_date_str = raw.get("date")
                if decision_date_str:
                    try:
                        decision_date = datetime.fromisoformat(decision_date_str).date()
                        if decision_date < since_date:
                            found_older = True
                            continue
                    except ValueError:
                        pass

                yield normalize(raw)

        if found_older:
            # All subsequent pages will be older
            break

        page += 1
        time.sleep(REQUEST_DELAY)


def bootstrap_sample(sample_dir: Path, count: int = 12) -> None:
    """Fetch sample decisions for testing."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    session = get_session()
    decisions = fetch_decision_ids(session, page=1, quantity=100)

    fetched = 0
    for decision_info in decisions[:count + 5]:  # fetch a few extra in case some fail
        if fetched >= count:
            break

        legal_id = decision_info["legal_id"]
        time.sleep(REQUEST_DELAY)

        raw = fetch_decision(session, legal_id)
        if raw and raw.get("text"):
            normalized = normalize(raw)

            # Save to file
            filename = f"{normalized['_id']}.json"
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            print(f"Saved: {filename} ({len(normalized['text'])} chars)")
            fetched += 1

    print(f"\nFetched {fetched} sample decisions to {sample_dir}")


def main():
    parser = argparse.ArgumentParser(description="Georgian Constitutional Court data fetcher")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Fetch sample data")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    bootstrap_parser.add_argument("--count", type=int, default=12, help="Number of samples to fetch")
    bootstrap_parser.add_argument("--output", type=str, help="Output directory")

    # Fetch all command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch all decisions")
    fetch_parser.add_argument("--limit", type=int, help="Limit number of decisions")
    fetch_parser.add_argument("--output", type=str, help="Output directory")

    # Updates command
    updates_parser = subparsers.add_parser("updates", help="Fetch recent updates")
    updates_parser.add_argument("--since", type=str, required=True, help="ISO date to fetch updates since")
    updates_parser.add_argument("--output", type=str, help="Output directory")

    args = parser.parse_args()

    if args.command == "bootstrap":
        output_dir = Path(args.output) if args.output else Path(__file__).parent / "sample"
        bootstrap_sample(output_dir, args.count)

    elif args.command == "fetch":
        for decision in fetch_all(limit=args.limit):
            print(json.dumps(decision, ensure_ascii=False))

    elif args.command == "updates":
        for decision in fetch_updates(args.since):
            print(json.dumps(decision, ensure_ascii=False))


if __name__ == "__main__":
    main()
