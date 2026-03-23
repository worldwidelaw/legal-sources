#!/usr/bin/env python3
"""
UK/TRA - Trade Remedies Authority Fetcher

Fetches TRA publications from GOV.UK including anti-dumping decisions,
countervailing duty determinations, safeguard measures, investigation notices,
guidance, and corporate reports.

Data source: https://www.gov.uk/government/organisations/trade-remedies-authority
API: GOV.UK Search API + Content API (no auth required)
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
SOURCE_ID = "UK/TRA"

HEADERS = {
    "User-Agent": "WorldWideLaw/1.0 (Open Data Research; github.com/worldwidelaw/legal-sources)",
    "Accept": "application/json",
}

# Document types to include (skip org pages, about pages, etc.)
INCLUDE_DOC_TYPES = [
    "news_story",
    "press_release",
    "guidance",
    "corporate_report",
    "foi_release",
    "research",
    "policy_paper",
    "detailed_guide",
    "publication",
    "transparency",
    "speech",
]


def strip_html(html_content: str) -> str:
    """Strip HTML tags and clean text content."""
    if not html_content:
        return ""
    text = re.sub(r'</(p|div|li|tr|h[1-6]|blockquote)>', '\n', html_content)
    text = re.sub(r'<(br|hr)\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = '\n'.join(line.strip() for line in text.split('\n'))
    return text.strip()


def fetch_search_results(start: int = 0, count: int = 50) -> Optional[dict]:
    """Fetch TRA publications from GOV.UK search API."""
    params = {
        "filter_organisations": "trade-remedies-authority",
        "start": start,
        "count": count,
        "fields": "title,link,public_timestamp,description,content_store_document_type",
    }
    try:
        resp = requests.get(SEARCH_API, headers=HEADERS, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Error fetching search results: {e}")
        return None


def fetch_content(path: str) -> Optional[dict]:
    """Fetch full content for a document from Content API."""
    url = f"{CONTENT_API}{path}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Error fetching content for {path}: {e}")
        return None


def extract_text(content_data: dict) -> str:
    """Extract text from GOV.UK content API response."""
    details = content_data.get("details", {})

    parts = []

    # Main body
    body = details.get("body", "")
    if body:
        parts.append(strip_html(body))

    # Some documents have documents/attachments with content
    for doc in details.get("documents", []):
        if isinstance(doc, str):
            cleaned = strip_html(doc)
            if cleaned:
                parts.append(cleaned)

    # Collection documents may have group descriptions
    for group in details.get("collection_groups", []):
        body = group.get("body", "")
        if body:
            parts.append(strip_html(body))

    # Check for headers in details
    for header in details.get("headers", []):
        text = header.get("text", "")
        if text:
            parts.append(text)

    return "\n\n".join(parts)


def parse_document(content_data: dict) -> Optional[dict]:
    """Parse content API response into normalized record."""
    base_path = content_data.get("base_path", "")
    if not base_path:
        return None

    doc_type = content_data.get("document_type", "")

    # Skip org pages, about pages, etc.
    skip_types = {"organisation", "about", "our_governance", "recruitment",
                  "complaints_procedure", "media_enquiries", "publication_scheme",
                  "personal_information_charter", "about_our_services"}
    if doc_type in skip_types:
        return None

    text = extract_text(content_data)
    if not text or len(text) < 100:
        return None

    # Create a stable ID from the path
    doc_id = base_path.lstrip("/").replace("/", "_")

    date = content_data.get("public_updated_at")
    if not date:
        date = content_data.get("first_published_at")

    description = content_data.get("description", "")

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": content_data.get("title", ""),
        "text": text,
        "date": date,
        "url": f"https://www.gov.uk{base_path}",
        "description": description,
        "document_type": doc_type,
        "language": "en",
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """Fetch all TRA publications."""
    total_yielded = 0
    start = 0
    per_page = 50

    print("Fetching UK/TRA publications...")

    while True:
        if max_records and total_yielded >= max_records:
            return

        print(f"\nFetching page at offset {start}...")
        results = fetch_search_results(start=start, count=per_page)

        if not results or not results.get("results"):
            break

        items = results["results"]
        total = results.get("total", 0)
        print(f"  Got {len(items)} items (total: {total})")

        for item in items:
            if max_records and total_yielded >= max_records:
                return

            link = item.get("link", "")
            doc_type = item.get("content_store_document_type", "")

            # Skip non-content pages
            skip_types = {"organisation", "about", "our_governance", "recruitment",
                          "complaints_procedure", "media_enquiries", "publication_scheme",
                          "personal_information_charter", "about_our_services"}
            if doc_type in skip_types:
                continue

            if not link:
                continue

            time.sleep(0.2)

            content = fetch_content(link)
            if not content:
                continue

            record = parse_document(content)
            if record:
                yield record
                total_yielded += 1

                if total_yielded % 50 == 0:
                    print(f"  Progress: {total_yielded} records fetched...")

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
    """Fetch sample records showing variety of TRA publications."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from UK/TRA...")
    print("=" * 60)

    records = []
    doc_types_seen = set()

    for record in fetch_all(max_records=sample_count + 10):
        if len(records) >= sample_count:
            break

        try:
            normalized = normalize(record)
            records.append(normalized)
            doc_types_seen.add(normalized.get("document_type", "unknown"))

            idx = len(records)
            filename = SAMPLE_DIR / f"record_{idx:03d}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            text_len = len(normalized.get("text", ""))
            dt = normalized.get("document_type", "?")[:20]
            title = normalized["title"][:50]
            print(f"  [{idx:02d}] {dt}: {title}... ({text_len:,} chars)")

        except ValueError as e:
            print(f"    Skipping: {e}")

    print("\n" + "=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")
        print(f"Document types: {sorted(doc_types_seen)}")

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
    print("Testing UK/TRA API...")

    print("\n1. Testing search endpoint...")
    results = fetch_search_results(start=0, count=5)
    if results and results.get("results"):
        total = results.get("total", 0)
        print(f"   OK: Got {len(results['results'])} results (total: {total})")
    else:
        print("   FAILED: Could not fetch search results")
        return False

    print("\n2. Testing content endpoint...")
    for item in results["results"]:
        link = item.get("link", "")
        doc_type = item.get("content_store_document_type", "")
        if doc_type in ("organisation", "about"):
            continue
        if not link:
            continue

        print(f"   Fetching: {link}")
        content = fetch_content(link)
        if content:
            record = parse_document(content)
            if record:
                print(f"   OK: Parsed to {len(record['text']):,} chars of text")
                print(f"       Title: {record['title'][:60]}")
                print(f"       Type: {record.get('document_type', 'N/A')}")
                break
        print("   Trying next result...")
    else:
        print("   FAILED: Could not parse any content")
        return False

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="UK/TRA fetcher")
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
