#!/usr/bin/env python3
"""
UK/HMRC - HM Revenue & Customs Tax Manuals Fetcher

Fetches technical guidance manuals from HMRC covering all UK taxes.
Contains 247 manuals with 84,000+ sections covering Income Tax, VAT,
Capital Gains Tax, Inheritance Tax, Corporation Tax, and more.

Data source: https://www.gov.uk/government/collections/hmrc-manuals
API: GOV.UK Content API (no auth required)
License: Open Government Licence v3.0
Rate limit: 1 request per second

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test API connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, List, Optional

import requests

SEARCH_API = "https://www.gov.uk/api/search.json"
CONTENT_API = "https://www.gov.uk/api/content"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "UK/HMRC"

HEADERS = {
    "User-Agent": "WorldWideLaw/1.0 (Open Data Research; github.com/worldwidelaw/legal-sources)",
    "Accept": "application/json",
}


def strip_html(html: str) -> str:
    """
    Remove HTML tags and clean up text.

    Args:
        html: HTML content string

    Returns:
        Plain text with HTML tags removed
    """
    if not html:
        return ""

    # Remove script and style tags with content
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)

    # Convert common HTML entities and tags to meaningful text
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</?p[^>]*>", "\n\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</?div[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<li[^>]*>", "\n• ", text, flags=re.IGNORECASE)
    text = re.sub(r"</li>", "", text, flags=re.IGNORECASE)

    # Remove all remaining HTML tags
    text = re.sub(r"<[^>]+>", "", text)

    # Decode HTML entities
    text = unescape(text)

    # Clean up whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = "\n".join(line.strip() for line in text.split("\n"))
    text = text.strip()

    return text


def fetch_all_manuals() -> List[dict]:
    """
    Fetch list of all HMRC manuals from GOV.UK Search API.

    Returns:
        List of manual metadata dicts
    """
    params = {
        "filter_format": "hmrc_manual",
        "count": 300,  # Get all manuals (currently ~247)
        "fields": "title,link,public_timestamp,description",
    }

    resp = requests.get(SEARCH_API, params=params, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    data = resp.json()
    return data.get("results", [])


def fetch_manual_structure(manual_path: str) -> dict:
    """
    Fetch manual structure including all section links.

    Args:
        manual_path: Path like /hmrc-internal-manuals/capital-gains-manual

    Returns:
        Manual dict with section groups
    """
    url = f"{CONTENT_API}{manual_path}"
    resp = requests.get(url, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    return resp.json()


def fetch_section_content(section_path: str) -> Optional[dict]:
    """
    Fetch a manual section's full content.

    Args:
        section_path: Path like /hmrc-internal-manuals/capital-gains-manual/cg10110

    Returns:
        Section content dict or None on error
    """
    url = f"{CONTENT_API}{section_path}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Error fetching section: {e}")
        return None


def get_all_section_paths(manual: dict) -> List[str]:
    """
    Extract all section paths from a manual's structure.

    Args:
        manual: Manual dict from Content API

    Returns:
        List of section paths
    """
    sections = []
    details = manual.get("details", {})

    for group in details.get("child_section_groups", []):
        for section in group.get("child_sections", []):
            base_path = section.get("base_path")
            if base_path:
                sections.append(base_path)

    return sections


def parse_section(section_data: dict, manual_title: str) -> dict:
    """
    Parse a section into a normalized record.

    Args:
        section_data: Raw section data from Content API
        manual_title: Title of the parent manual

    Returns:
        Normalized record dict
    """
    details = section_data.get("details", {})
    body_html = details.get("body", "")
    text = strip_html(body_html)

    section_id = details.get("section_id", "")
    base_path = section_data.get("base_path", "")

    # Extract manual slug from path
    parts = base_path.split("/")
    manual_slug = parts[2] if len(parts) > 2 else ""

    # Generate unique ID
    doc_id = f"{manual_slug}_{section_id}".lower() if section_id else base_path.replace("/", "_").strip("_")

    # Get timestamps
    updated_at = section_data.get("public_updated_at") or section_data.get("updated_at")
    first_published = section_data.get("first_published_at")

    # Construct title
    section_title = section_data.get("title", "")
    if section_id:
        title = f"{section_id} - {section_title}"
    else:
        title = section_title

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": updated_at[:10] if updated_at else None,
        "url": f"https://www.gov.uk{base_path}",
        "manual_title": manual_title,
        "manual_slug": manual_slug,
        "section_id": section_id,
        "updated_at": updated_at,
        "first_published_at": first_published,
        "language": "en",
    }


def fetch_all(max_records: int = None) -> Generator[dict, None, None]:
    """
    Fetch all HMRC manual sections.

    Args:
        max_records: Maximum total records to yield

    Yields:
        Normalized section records
    """
    print("Fetching HMRC manuals list...")
    manuals = fetch_all_manuals()
    print(f"Found {len(manuals)} manuals")

    total_yielded = 0

    for manual_meta in manuals:
        if max_records and total_yielded >= max_records:
            return

        manual_path = manual_meta.get("link")
        manual_title = manual_meta.get("title", "")

        print(f"\nProcessing manual: {manual_title}")

        manual = fetch_manual_structure(manual_path)
        sections = get_all_section_paths(manual)
        print(f"  Found {len(sections)} sections")

        for section_path in sections:
            if max_records and total_yielded >= max_records:
                return

            section_data = fetch_section_content(section_path)
            if not section_data:
                continue

            record = parse_section(section_data, manual_title)

            # Skip sections without meaningful content
            if not record.get("text") or len(record["text"]) < 50:
                continue

            yield record
            total_yielded += 1

            if total_yielded % 100 == 0:
                print(f"  Progress: {total_yielded} records...")

            time.sleep(1.0)  # Rate limiting

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
    Fetch sample records from various HMRC manuals.
    """
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Fetching {sample_count} sample records from UK/HMRC...")
    print("=" * 60)

    print("\nFetching HMRC manuals list...")
    manuals = fetch_all_manuals()
    print(f"Found {len(manuals)} manuals")

    # Select a diverse sample of manuals
    sample_manuals = []
    # Get a variety of important tax manuals
    priority_slugs = [
        "capital-gains-manual",
        "income-tax-employment-manual",
        "vat-input-tax",
        "inheritance-tax-manual",
        "compliance-handbook",
        "company-taxation-manual",
        "pensions-tax-manual",
        "international-manual",
    ]

    # First add priority manuals
    for manual in manuals:
        for slug in priority_slugs:
            if slug in manual.get("link", ""):
                sample_manuals.append(manual)
                break
        if len(sample_manuals) >= 8:
            break

    # Add remaining manuals
    for manual in manuals:
        if manual not in sample_manuals:
            sample_manuals.append(manual)
        if len(sample_manuals) >= 15:
            break

    records = []
    manuals_seen = set()

    for manual_meta in sample_manuals:
        if len(records) >= sample_count:
            break

        manual_path = manual_meta.get("link")
        manual_title = manual_meta.get("title", "")
        manual_slug = manual_path.split("/")[-1]

        print(f"\n[Manual] {manual_title}")

        time.sleep(1.0)
        manual = fetch_manual_structure(manual_path)
        sections = get_all_section_paths(manual)

        if not sections:
            print("  No sections found, skipping...")
            continue

        # Fetch first content section (skip index/contents pages)
        for section_path in sections[:5]:
            section_id = section_path.split("/")[-1].lower()
            # Skip index and contents pages
            if section_id.endswith("c") or "index" in section_id or "contents" in section_id:
                continue

            time.sleep(1.0)
            section_data = fetch_section_content(section_path)
            if not section_data:
                continue

            record = parse_section(section_data, manual_title)

            if not record.get("text") or len(record["text"]) < 100:
                print(f"  Section {section_id}: insufficient text, trying next...")
                continue

            try:
                normalized = normalize(record)
                records.append(normalized)
                manuals_seen.add(manual_slug)

                # Save individual record
                idx = len(records)
                filename_safe = re.sub(r'[^\w\-]', '_', normalized["_id"])[:50]
                json_filename = SAMPLE_DIR / f"record_{idx:03d}_{filename_safe}.json"
                with open(json_filename, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

                text_len = len(normalized.get("text", ""))
                title_short = normalized["title"][:50]
                print(f"  [{idx:02d}] {title_short}... ({text_len:,} chars)")
                break

            except ValueError as e:
                print(f"  Skipping: {e}")

    print("\n" + "=" * 60)
    print(f"Saved {len(records)} sample records to {SAMPLE_DIR}")
    print(f"Manuals represented: {len(manuals_seen)}")

    if records:
        avg_text_len = sum(len(r.get("text", "")) for r in records) / len(records)
        print(f"Average text length: {avg_text_len:,.0f} chars/doc")

        # Show date range
        dates = [r.get("date") for r in records if r.get("date")]
        if dates:
            dates.sort()
            print(f"Date range: {dates[0]} to {dates[-1]}")

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
    """Test API connectivity and structure."""
    print("Testing UK HMRC data source...")

    # Test Search API
    print("\n1. Testing Search API...")
    try:
        manuals = fetch_all_manuals()
        print(f"   OK: Found {len(manuals)} manuals")
    except Exception as e:
        print(f"   FAILED: {e}")
        return False

    # Test Content API - fetch a manual
    print("\n2. Testing Content API (manual structure)...")
    if manuals:
        test_manual = manuals[0]
        manual_path = test_manual.get("link")
        print(f"   Testing: {test_manual.get('title')}")

        try:
            manual = fetch_manual_structure(manual_path)
            sections = get_all_section_paths(manual)
            print(f"   OK: Found {len(sections)} sections")
        except Exception as e:
            print(f"   FAILED: {e}")
            return False

        # Test section content fetch
        print("\n3. Testing Content API (section content)...")
        if sections:
            # Find a non-index section
            test_section_path = None
            for sp in sections[:10]:
                section_id = sp.split("/")[-1].lower()
                if not section_id.endswith("c") and "index" not in section_id:
                    test_section_path = sp
                    break

            if test_section_path:
                print(f"   Testing: {test_section_path}")
                section_data = fetch_section_content(test_section_path)
                if section_data:
                    record = parse_section(section_data, test_manual.get("title", ""))
                    text_len = len(record.get("text", ""))
                    print(f"   OK: Extracted {text_len:,} characters")
                    print(f"       Title: {record['title'][:60]}...")
                    print(f"       Section ID: {record.get('section_id', 'N/A')}")
                else:
                    print("   FAILED: Could not fetch section content")
                    return False
            else:
                print("   WARNING: No suitable test section found")

    print("\nAll tests passed!")
    return True


def main():
    parser = argparse.ArgumentParser(description="UK/HMRC fetcher")
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
