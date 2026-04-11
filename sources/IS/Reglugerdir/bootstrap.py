#!/usr/bin/env python3
"""
IS/Reglugerdir - Icelandic Regulations (Reglugerðir) Fetcher

Fetches regulations from the island.is GraphQL API.

Data source: https://island.is/reglugerdir
API: https://island.is/api/graphql
License: Public Domain (official government regulations)
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

BASE_URL = "https://island.is/reglugerdir"
GRAPHQL_URL = "https://island.is/api/graphql"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/Reglugerdir"

# Request settings
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalSourcesBot/1.0; worldwidelaw/legal-sources)",
    "Content-Type": "application/json",
    "Accept": "application/json",
}
REQUEST_DELAY = 1.0  # Seconds between requests

# GraphQL queries
QUERY_YEARS = """
query GetRegulationsYears {
  getRegulationsYears
}
"""

QUERY_SEARCH = """
query GetRegulationsSearch($input: GetRegulationsSearchInput!) {
  getRegulationsSearch(input: $input) {
    data {
      name
      title
      publishedDate
      ministry {
        name
      }
    }
    paging {
      page
      pages
    }
  }
}
"""

QUERY_REGULATION = """
query GetRegulation($input: GetRegulationInput!) {
  getRegulation(input: $input) {
    name
    title
    text
    signatureDate
    publishedDate
    effectiveDate
    ministry {
      name
    }
    lawChapters {
      name
      slug
    }
    history {
      title
      name
      date
    }
  }
}
"""


def graphql_request(query: str, variables: dict = None) -> Optional[dict]:
    """Execute a GraphQL request against the island.is API."""
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    try:
        resp = requests.post(GRAPHQL_URL, json=payload, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if "errors" in data:
            print(f"  GraphQL errors: {data['errors']}")
            return None

        return data.get("data")

    except requests.RequestException as e:
        print(f"  Request error: {e}")
        return None


def strip_html(html_text: str) -> str:
    """Strip HTML tags and extract clean text from regulation body."""
    if not html_text:
        return ""

    # Try using BeautifulSoup if available, otherwise fall back to regex
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_text, 'html.parser')

        # Remove script and style elements
        for element in soup(['script', 'style']):
            element.decompose()

        text = soup.get_text(separator='\n', strip=True)
    except ImportError:
        # Fallback: regex-based HTML stripping
        text = re.sub(r'<br\s*/?>', '\n', html_text)
        text = re.sub(r'<p[^>]*>', '\n\n', text)
        text = re.sub(r'</p>', '', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def get_available_years() -> list[int]:
    """Get list of available regulation years."""
    data = graphql_request(QUERY_YEARS)
    if data and "getRegulationsYears" in data:
        return sorted(data["getRegulationsYears"], reverse=True)
    return []


def get_regulation_names(year: int = None, page: int = 1) -> tuple[list[str], int]:
    """
    Get regulation names (identifiers) for a given year.

    Returns:
        Tuple of (list of regulation names, total pages)
    """
    variables = {
        "input": {
            "page": page,
        }
    }
    if year:
        variables["input"]["year"] = year

    data = graphql_request(QUERY_SEARCH, variables)
    if not data or "getRegulationsSearch" not in data:
        return [], 0

    search_result = data["getRegulationsSearch"]
    names = [r["name"] for r in search_result.get("data", []) if r.get("name")]
    total_pages = search_result.get("paging", {}).get("pages", 0)

    return names, total_pages


def fetch_regulation(name: str) -> Optional[dict]:
    """Fetch a single regulation by its name/number."""
    variables = {
        "input": {
            "name": name,
        }
    }

    data = graphql_request(QUERY_REGULATION, variables)
    if not data or "getRegulation" not in data:
        return None

    reg = data["getRegulation"]
    if not reg:
        return None

    # Extract and clean text
    raw_text = reg.get("text", "")
    text = strip_html(raw_text)

    if not text or len(text) < 20:
        return None

    # Build regulation ID from name (e.g., "0006/2026" -> "0006_2026")
    reg_name = reg.get("name", name)
    reg_id = reg_name.replace("/", "_")

    # Determine date - prefer signature date, fall back to published
    date = reg.get("signatureDate") or reg.get("publishedDate") or ""
    if date and "T" in date:
        date = date.split("T")[0]

    record = {
        "_id": reg_id,
        "_source": SOURCE_ID,
        "_type": "regulation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": reg.get("title", f"Reglugerð nr. {reg_name}"),
        "text": text,
        "date": date,
        "url": f"{BASE_URL}/nr/{reg_name}",
        "language": "isl",
        "regulation_number": reg_name,
    }

    # Optional fields
    ministry = reg.get("ministry")
    if ministry and ministry.get("name"):
        record["ministry"] = ministry["name"]

    law_chapters = reg.get("lawChapters")
    if law_chapters:
        record["law_chapters"] = [
            {"name": ch.get("name", ""), "slug": ch.get("slug", "")}
            for ch in law_chapters
        ]

    effective_date = reg.get("effectiveDate")
    if effective_date:
        if "T" in effective_date:
            effective_date = effective_date.split("T")[0]
        record["effective_date"] = effective_date

    return record


def get_all_regulation_names(max_names: int = None) -> list[str]:
    """
    Get all regulation names across all years.

    Args:
        max_names: Maximum number of names to return (for sampling)

    Returns:
        List of regulation name identifiers
    """
    all_names = []

    print("Fetching available years...")
    years = get_available_years()

    if not years:
        # Fall back to paginated search without year filter
        print("  No years returned, trying paginated search...")
        page = 1
        while True:
            if max_names and len(all_names) >= max_names:
                break

            names, total_pages = get_regulation_names(page=page)
            if not names:
                break

            all_names.extend(names)
            print(f"    Page {page}/{total_pages}: {len(names)} regulations")

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.5)

        print(f"  Total regulation names discovered: {len(all_names)}")
        return all_names[:max_names] if max_names else all_names

    print(f"  Found {len(years)} years: {years[0]}..{years[-1]}")

    for year in years:
        if max_names and len(all_names) >= max_names:
            break

        page = 1
        while True:
            names, total_pages = get_regulation_names(year=year, page=page)
            if not names:
                break

            all_names.extend(names)

            if page >= total_pages:
                break
            page += 1
            time.sleep(0.3)

        print(f"    Year {year}: {len(all_names)} total so far")
        time.sleep(0.5)

        # Safety limit - ~2,500 regulations expected
        if len(all_names) > 3000:
            print(f"  Safety limit reached at {len(all_names)} names")
            break

    print(f"  Total regulation names discovered: {len(all_names)}")
    return all_names[:max_names] if max_names else all_names


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """
    Fetch all regulations with checkpoint/resume support.

    Args:
        max_records: Maximum number of records to yield (for sampling)

    Yields:
        Normalized document records
    """
    checkpoint_file = Path(__file__).parent / ".checkpoint"
    completed_names = set()

    # Load checkpoint if exists
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r') as f:
                completed_names = set(line.strip() for line in f if line.strip())
            print(f"Loaded checkpoint: {len(completed_names)} already processed")
        except Exception as e:
            print(f"Warning: Could not load checkpoint: {e}")

    # Get all regulation names
    if max_records:
        all_names = get_all_regulation_names(max_names=max_records + 20)
    else:
        all_names = get_all_regulation_names()

    # Filter out already completed
    pending_names = [n for n in all_names if n not in completed_names]
    if max_records:
        pending_names = pending_names[:max_records + 5]

    print(f"Processing {len(pending_names)} pending regulations (of {len(all_names)} total)...")

    count = 0
    for i, name in enumerate(pending_names):
        if max_records and count >= max_records:
            break

        print(f"  [{i+1}/{len(pending_names)}] Fetching {name}...")

        record = fetch_regulation(name)

        if record and len(record.get('text', '')) >= 20:
            yield record
            count += 1

            # Update checkpoint for full fetches
            if not max_records:
                try:
                    with open(checkpoint_file, 'a') as f:
                        f.write(f"{name}\n")
                except Exception:
                    pass

        time.sleep(REQUEST_DELAY)

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch regulations published since a given date."""
    for record in fetch_all():
        if record.get('date'):
            try:
                doc_date = datetime.fromisoformat(record['date'])
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def normalize(raw: dict) -> dict:
    """Validate and normalize the record."""
    required = ['_id', '_source', '_type', '_fetched_at', 'title', 'text', 'date', 'url']
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    if not raw.get('text') or len(raw['text']) < 20:
        raise ValueError("Document has insufficient text content")

    return raw


def bootstrap_sample(sample_count: int = 12):
    """Fetch sample records and save to sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from {SOURCE_ID}...")
    print("=" * 60)

    records = []
    for i, record in enumerate(fetch_all(max_records=sample_count)):
        try:
            normalized = normalize(record)
            records.append(normalized)

            filename = SAMPLE_DIR / f"record_{i+1:03d}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            text_len = len(normalized.get('text', ''))
            print(f"  [{i+1:02d}] {normalized['_id']}: {normalized['title'][:50]} ({text_len:,} chars)")

        except ValueError as e:
            print(f"  Skipping record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get('text', '')) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get('text'))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="IS/Reglugerdir regulations fetcher")
    parser.add_argument('command', choices=['bootstrap', 'update', 'fetch', 'info'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument('--count', type=int, default=12,
                       help="Number of sample records to fetch")
    parser.add_argument('--since', type=str, default=None,
                       help="Fetch updates since date (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.command == 'info':
        print(f"IS/Reglugerdir - Icelandic Regulations")
        print(f"Source URL: {BASE_URL}")
        print(f"API URL: {GRAPHQL_URL}")
        print(f"Expected records: ~2,487 in-force regulations")

        print("\nFetching available years...")
        years = get_available_years()
        if years:
            print(f"Available years: {years}")
        else:
            print("Could not fetch years from API")

    elif args.command == 'bootstrap':
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == 'update':
        since = datetime.fromisoformat(args.since) if args.since else datetime(2024, 1, 1)
        print(f"Fetching updates since {since.isoformat()}...")
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'fetch':
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
