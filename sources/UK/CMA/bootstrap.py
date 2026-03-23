#!/usr/bin/env python3
"""
UK/CMA - Competition and Markets Authority Fetcher

Fetches CMA cases from gov.uk including merger decisions, market investigations,
consumer enforcement, competition cases, and digital markets unit decisions.

Data source: https://www.gov.uk/cma-cases
API: GOV.UK Content API (no auth required)
License: Open Government Licence v3.0
Rate limit: 10 requests per second

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test API connectivity
"""

import argparse
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

SEARCH_API = "https://www.gov.uk/api/search.json"
CONTENT_API = "https://www.gov.uk/api/content"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UK/CMA"

HEADERS = {
    "User-Agent": "WorldWideLaw/1.0 (Open Data Research; github.com/worldwidelaw/legal-sources)",
    "Accept": "application/json",
}

# Case types available in CMA finder
CASE_TYPES = [
    "ca98-and-civil-cartels",
    "competition-disqualification",
    "consumer-enforcement",
    "criminal-cartels",
    "digital-markets-unit",
    "information-and-advice-to-government",
    "markets",
    "mergers",
    "oim-project",
    "regulatory-references-and-appeals",
    "review-of-orders-and-undertakings",
    "sau-referral",
]


def strip_html(html_content: str) -> str:
    """
    Strip HTML tags and clean text content.
    Preserves paragraph structure with newlines.
    """
    if not html_content:
        return ""

    # Replace block-level tags with newlines
    text = re.sub(r'</(p|div|li|tr|h[1-6]|blockquote)>', '\n', html_content)
    text = re.sub(r'<(br|hr)\s*/?>', '\n', text)

    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities
    text = html.unescape(text)

    # Clean up whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = '\n'.join(line.strip() for line in text.split('\n'))

    return text.strip()


def fetch_search_results(
    start: int = 0,
    count: int = 50,
    case_type: str = None,
    case_state: str = None,
) -> Optional[dict]:
    """
    Fetch CMA case listings from GOV.UK search API.

    Args:
        start: Offset for pagination
        count: Number of results per page
        case_type: Filter by case type
        case_state: Filter by state (open/closed)

    Returns:
        API response dict or None on error
    """
    params = {
        "filter_document_type": "cma_case",
        "start": start,
        "count": count,
        "fields": "title,link,description,public_timestamp",
    }

    if case_type:
        params["filter_case_type"] = case_type
    if case_state:
        params["filter_case_state"] = case_state

    try:
        resp = requests.get(SEARCH_API, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Error fetching search results: {e}")
        return None


def fetch_case_content(case_path: str) -> Optional[dict]:
    """
    Fetch full content for a CMA case from Content API.

    Args:
        case_path: Case path (e.g., "/cma-cases/cloud-services-market-investigation")

    Returns:
        API response dict or None on error
    """
    # Ensure path starts with /cma-cases/
    if not case_path.startswith("/cma-cases/"):
        case_path = f"/cma-cases/{case_path}"

    url = f"{CONTENT_API}{case_path}"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Error fetching case content: {e}")
        return None


def parse_case(case_data: dict) -> Optional[dict]:
    """
    Parse CMA case content into normalized record.

    Args:
        case_data: Raw case data from Content API

    Returns:
        Normalized record dict or None if invalid
    """
    details = case_data.get("details", {})

    # Extract HTML body and convert to text
    body_html = details.get("body", "")
    text = strip_html(body_html)

    if not text or len(text) < 100:
        print(f"    Warning: Case has insufficient text ({len(text)} chars)")
        return None

    # Extract case ID from path
    base_path = case_data.get("base_path", "")
    case_id = base_path.replace("/cma-cases/", "") if base_path else None

    if not case_id:
        return None

    # Extract metadata from details
    metadata = details.get("metadata", {})

    # Get case type
    case_type = None
    case_type_raw = metadata.get("case_type")
    if case_type_raw:
        if isinstance(case_type_raw, list):
            case_type = case_type_raw[0] if case_type_raw else None
        else:
            case_type = case_type_raw

    # Get case state
    case_state = metadata.get("case_state")

    # Get market sector
    market_sector = None
    market_sector_raw = metadata.get("market_sector")
    if market_sector_raw:
        if isinstance(market_sector_raw, list):
            market_sector = market_sector_raw
        else:
            market_sector = [market_sector_raw]

    # Get opened/closed dates
    opened_date = metadata.get("opened_date")
    closed_date = metadata.get("closed_date")

    # Use public_timestamp for main date
    date = case_data.get("public_updated_at")
    if not date:
        date = case_data.get("first_published_at")

    # Get attachments info
    attachments = details.get("attachments", [])
    attachment_count = len(attachments)

    return {
        "_id": case_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": case_data.get("title", ""),
        "text": text,
        "date": date,
        "url": f"https://www.gov.uk{base_path}",
        "description": case_data.get("description", ""),
        "case_type": case_type,
        "case_state": case_state,
        "market_sector": market_sector,
        "opened_date": opened_date,
        "closed_date": closed_date,
        "attachment_count": attachment_count,
        "language": "en",
    }


def fetch_all(
    max_records: int = None,
    case_type: str = None,
    case_state: str = None,
) -> Generator[dict, None, None]:
    """
    Fetch all CMA cases.

    Args:
        max_records: Maximum total records to yield
        case_type: Filter by case type
        case_state: Filter by state (open/closed)

    Yields:
        Normalized case records
    """
    total_yielded = 0
    start = 0
    per_page = 50

    print(f"Fetching CMA cases...")
    if case_type:
        print(f"  Filter: case_type={case_type}")
    if case_state:
        print(f"  Filter: case_state={case_state}")

    while True:
        if max_records and total_yielded >= max_records:
            return

        print(f"\nFetching page at offset {start}...")
        results = fetch_search_results(start=start, count=per_page, case_type=case_type, case_state=case_state)

        if not results or not results.get("results"):
            break

        cases = results["results"]
        total = results.get("total", 0)

        print(f"  Got {len(cases)} cases (total: {total})")

        for case_info in cases:
            if max_records and total_yielded >= max_records:
                return

            case_path = case_info.get("link", "")
            if not case_path:
                continue

            time.sleep(0.2)  # Rate limiting

            case_data = fetch_case_content(case_path)
            if not case_data:
                continue

            record = parse_case(case_data)
            if record:
                yield record
                total_yielded += 1

                if total_yielded % 50 == 0:
                    print(f"  Progress: {total_yielded} records fetched...")

        # Check if we've fetched all
        if start + per_page >= total:
            break

        start += per_page
        time.sleep(0.2)

    print(f"\nCompleted: {total_yielded} total records")


def normalize(raw: dict) -> dict:
    """Validate and normalize a record."""
    required = ["_id", "_source", "_type", "_fetched_at", "title", "text", "url"]
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    if not raw.get("text") or len(raw["text"]) < 50:
        raise ValueError(f"Document has insufficient text content ({len(raw.get('text', ''))} chars)")

    return raw


def bootstrap_sample(sample_count: int = 15):
    """
    Fetch sample records showing variety of CMA cases.
    """
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from UK/CMA...")
    print("=" * 60)

    records = []
    case_types_seen = set()

    # First, get a mix from different case types
    types_to_sample = ["mergers", "markets", "consumer-enforcement", "ca98-and-civil-cartels"]

    for case_type in types_to_sample:
        if len(records) >= sample_count:
            break

        print(f"\nFetching {case_type} cases...")

        results = fetch_search_results(start=0, count=5, case_type=case_type)
        if not results or not results.get("results"):
            continue

        for case_info in results["results"]:
            if len(records) >= sample_count:
                break

            case_path = case_info.get("link", "")
            if not case_path:
                continue

            time.sleep(0.2)

            case_data = fetch_case_content(case_path)
            if not case_data:
                continue

            record = parse_case(case_data)
            if not record:
                continue

            try:
                normalized = normalize(record)
                records.append(normalized)
                case_types_seen.add(normalized.get("case_type", "unknown"))

                # Save individual record
                idx = len(records)
                filename = SAMPLE_DIR / f"record_{idx:03d}.json"
                with open(filename, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

                text_len = len(normalized.get("text", ""))
                ct = normalized.get("case_type", "?")[:20]
                title = normalized["title"][:40]
                print(f"  [{idx:02d}] {ct}: {title}... ({text_len:,} chars)")

            except ValueError as e:
                print(f"    Skipping: {e}")

    print("\n" + "=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")
        print(f"Case types: {sorted(case_types_seen)}")

        # Show case state distribution
        states = {}
        for r in records:
            s = r.get("case_state", "unknown")
            states[s] = states.get(s, 0) + 1
        print(f"Case states: {states}")

    # Validation
    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get("text"))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print(f"\nVALIDATION PASSED: {len(records)} records with full text.")
    return True


def test_api():
    """Test API connectivity."""
    print("Testing UK CMA API...")

    # Test search endpoint
    print("\n1. Testing search endpoint...")
    results = fetch_search_results(start=0, count=5)
    if results and results.get("results"):
        total = results.get("total", 0)
        print(f"   OK: Got {len(results['results'])} results (total: {total})")
    else:
        print("   FAILED: Could not fetch search results")
        return False

    # Test content endpoint
    print("\n2. Testing content endpoint...")
    if results["results"]:
        test_path = results["results"][0].get("link", "")
        print(f"   Fetching: {test_path}")

        case_data = fetch_case_content(test_path)
        if case_data:
            body_len = len(case_data.get("details", {}).get("body", ""))
            print(f"   OK: Got case with {body_len:,} chars of HTML body")

            # Test parsing
            record = parse_case(case_data)
            if record:
                print(f"   OK: Parsed to {len(record['text']):,} chars of text")
                print(f"       Title: {record['title'][:60]}...")
                print(f"       Case type: {record.get('case_type', 'N/A')}")
                print(f"       Case state: {record.get('case_state', 'N/A')}")
            else:
                print("   FAILED: Could not parse case")
                return False
        else:
            print("   FAILED: Could not fetch case content")
            return False

    # Test case type filtering
    print("\n3. Testing case type filter (mergers)...")
    mergers = fetch_search_results(start=0, count=3, case_type="mergers")
    if mergers and mergers.get("results"):
        print(f"   OK: Got {len(mergers['results'])} merger cases")
    else:
        print("   FAILED: Could not filter by case type")
        return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="UK/CMA fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")

    args = parser.parse_args()

    if args.command == "test":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample(args.count)
            sys.exit(0 if success else 1)
        else:
            print("Full bootstrap not implemented yet. Use --sample flag.")
            sys.exit(1)

    elif args.command == "update":
        print("Update command not implemented yet.")
        sys.exit(1)


if __name__ == "__main__":
    main()
