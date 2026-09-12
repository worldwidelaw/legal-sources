#!/usr/bin/env python3
"""
IS/Personuvernd - Icelandic Data Protection Authority Decisions Fetcher

Fetches rulings, decisions, and opinions from Persónuvernd via the
island.is GraphQL API (Contentful CMS backend).

Data source: https://island.is/s/personuvernd/urskurdir-akvardanir-og-alit
License: Public Domain (official government decisions)
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional

import requests

GRAPHQL_URL = "https://island.is/api/graphql"
GENERIC_LIST_ID = "18Qfx6UBAJmLrmaNZZA6lM"
BASE_PAGE_URL = "https://island.is/s/personuvernd/urskurdir-akvardanir-og-alit"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "IS/Personuvernd"
PAGE_SIZE = 50
REQUEST_DELAY = 1.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Content-Type": "application/json",
}

LIST_QUERY = """
query GetGenericListItems($input: GetGenericListItemsInput!) {
  getGenericListItems(input: $input) {
    items {
      title
      slug
      date
    }
    total
  }
}
"""

DETAIL_QUERY = """
query GetGenericListItemBySlug($input: GetGenericListItemBySlugInput!) {
  getGenericListItemBySlug(input: $input) {
    title
    slug
    date
    content {
      ... on Html {
        id
        document
      }
    }
  }
}
"""


def extract_text_from_richtext(node: dict) -> str:
    """Recursively extract plain text from Contentful rich text document."""
    if not isinstance(node, dict):
        return ""

    node_type = node.get("nodeType", "")
    if node_type == "text":
        return node.get("value", "")

    parts = []
    for child in node.get("content", []):
        parts.append(extract_text_from_richtext(child))

    text = "".join(parts)

    # Add spacing for block-level elements
    if node_type in ("paragraph", "heading-1", "heading-2", "heading-3",
                     "heading-4", "heading-5", "heading-6", "blockquote",
                     "list-item", "table-row"):
        text = text.strip() + "\n\n"
    elif node_type in ("ordered-list", "unordered-list", "table"):
        text = text + "\n"

    return text


def extract_case_number(title: str) -> Optional[str]:
    """Extract case number from title like 'mál nr. 2022020414'."""
    match = re.search(r'(?:máli?\s+nr\.?\s*|mál\s+)(\d{7,})', title, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def fetch_list_page(session: requests.Session, page: int) -> tuple[list[dict], int]:
    """Fetch one page of decision listings."""
    payload = {
        "query": LIST_QUERY,
        "variables": {
            "input": {
                "genericListId": GENERIC_LIST_ID,
                "page": page,
                "size": PAGE_SIZE,
            }
        }
    }

    resp = session.post(GRAPHQL_URL, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    result = data.get("data", {}).get("getGenericListItems", {})
    items = result.get("items", [])
    total = result.get("total", 0)
    return items, total


def fetch_detail(session: requests.Session, slug: str) -> Optional[dict]:
    """Fetch the full content of a single decision."""
    payload = {
        "query": DETAIL_QUERY,
        "variables": {
            "input": {
                "slug": slug
            }
        }
    }

    try:
        resp = session.post(GRAPHQL_URL, json=payload, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        print(f"    Error fetching detail for {slug}: {e}")
        return None

    item = data.get("data", {}).get("getGenericListItemBySlug")
    if not item:
        return None

    # Extract text from all content blocks
    text_parts = []
    for block in item.get("content", []):
        doc = block.get("document")
        if doc:
            text_parts.append(extract_text_from_richtext(doc))

    full_text = "\n".join(text_parts).strip()
    # Clean up excessive whitespace
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r' {2,}', ' ', full_text)

    if len(full_text) < 50:
        return None

    title = item.get("title", "")
    date = item.get("date")
    case_number = extract_case_number(title)
    url = f"{BASE_PAGE_URL}/{slug}"

    return {
        "_id": slug,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": title,
        "text": full_text,
        "date": date,
        "url": url,
        "language": "isl",
        "case_number": case_number,
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all decisions with pagination."""
    session = requests.Session()
    session.headers.update(HEADERS)

    page = 1
    count = 0
    total = None

    while True:
        if max_records and count >= max_records:
            break

        print(f"  Fetching page {page}...")
        items, total_count = fetch_list_page(session, page)
        if total is None:
            total = total_count
            print(f"  Total decisions available: {total}")

        if not items:
            break

        for item in items:
            if max_records and count >= max_records:
                break

            slug = item.get("slug")
            if not slug:
                continue

            time.sleep(REQUEST_DELAY)
            record = fetch_detail(session, slug)

            if record and len(record.get("text", "")) >= 50:
                yield record
                count += 1
                if count % 10 == 0:
                    print(f"    Fetched {count} records...")

        page += 1
        time.sleep(REQUEST_DELAY)

    print(f"Total records yielded: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch decisions updated since a given date."""
    for record in fetch_all():
        if record.get("date"):
            try:
                doc_date = datetime.fromisoformat(record["date"])
                if doc_date >= since:
                    yield record
            except (ValueError, TypeError):
                yield record


def normalize(raw: dict) -> dict:
    """Validate and normalize the record."""
    required = ["_id", "_source", "_type", "_fetched_at", "title", "text", "date", "url"]
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    if not raw.get("text") or len(raw["text"]) < 50:
        raise ValueError("Document has insufficient text content")

    return raw


def bootstrap_sample(sample_count: int = 15):
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
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            text_len = len(normalized.get("text", ""))
            print(f"  [{i+1:02d}] {normalized['title'][:60]} ({text_len:,} chars)")

        except ValueError as e:
            print(f"  Skipping record: {e}")

    print("=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

    if len(records) < 10:
        print("WARNING: Fewer than 10 records fetched!")
        return False

    empty_text = sum(1 for r in records if not r.get("text"))
    if empty_text > 0:
        print(f"WARNING: {empty_text} records have empty text!")
        return False

    print("VALIDATION PASSED: All records have full text content.")
    return True


def main():
    parser = argparse.ArgumentParser(description="IS/Personuvernd data protection decisions fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "info"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "info":
        print(f"{SOURCE_ID} - Icelandic Data Protection Authority Decisions")
        print(f"Source URL: {BASE_PAGE_URL}")
        print(f"API: {GRAPHQL_URL}")
        print(f"GenericList ID: {GENERIC_LIST_ID}")

    elif args.command == "bootstrap":
        success = bootstrap_sample(args.count)
        sys.exit(0 if success else 1)

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
