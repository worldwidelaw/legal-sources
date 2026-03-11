#!/usr/bin/env python3
"""
US/CongressGov - Congressional Bills from Congress.gov API

Fetches bills, resolutions, and amendments from the Congress.gov API
maintained by the Library of Congress.

Data coverage: All Congressional bills from 1789 to present.
Full text is retrieved from XML versions hosted on congress.gov.
"""

import argparse
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional
from urllib.parse import urljoin

import requests

# Rate limit: 5000/hour = ~1.4/sec. DEMO_KEY has stricter limits.
# Use 2s delay between requests and retry with exponential backoff on 429s.
DELAY_SECONDS = 2.0
MAX_RETRIES = 5
API_BASE = "https://api.congress.gov/v3"


def get_api_key() -> str:
    """Get API key from environment or use DEMO_KEY."""
    return os.environ.get("CONGRESS_API_KEY", "DEMO_KEY")


def make_request(url: str, retries: int = MAX_RETRIES) -> Optional[requests.Response]:
    """Make a request with retry logic for rate limiting."""
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 429:
                # Rate limited - wait with exponential backoff
                wait_time = (2 ** attempt) * 5  # 5, 10, 20, 40, 80 seconds
                print(f"  Rate limited. Waiting {wait_time}s before retry...", file=sys.stderr)
                time.sleep(wait_time)
                continue
            resp.raise_for_status()
            return resp
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait_time = (2 ** attempt) * 5
                print(f"  Rate limited. Waiting {wait_time}s before retry...", file=sys.stderr)
                time.sleep(wait_time)
                continue
            print(f"  HTTP error: {e}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"  Request error: {e}", file=sys.stderr)
            return None
    print(f"  Max retries exceeded for {url}", file=sys.stderr)
    return None


def strip_xml_tags(text: str) -> str:
    """Remove XML/HTML tags and clean up whitespace."""
    # Remove XML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Decode common entities
    text = text.replace("&amp;", "&")
    text = text.replace("&lt;", "<")
    text = text.replace("&gt;", ">")
    text = text.replace("&quot;", '"')
    text = text.replace("&apos;", "'")
    text = text.replace("&#8217;", "'")
    text = text.replace("&#8220;", '"')
    text = text.replace("&#8221;", '"')
    # Clean up whitespace
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def extract_text_from_xml(xml_content: str) -> str:
    """Extract readable text from bill XML."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        # Fallback: strip tags directly
        return strip_xml_tags(xml_content)

    # Collect text from relevant sections
    text_parts = []

    # Get title from metadata or form
    for title_elem in root.iter():
        if title_elem.tag in ["official-title", "short-title"]:
            if title_elem.text:
                text_parts.append(title_elem.text.strip())

    # Get main body content
    for body_tag in ["legis-body", "resolution-body", "engrossed-amendment-body"]:
        for body in root.iter(body_tag):
            # Extract all text content
            body_text = ET.tostring(body, encoding="unicode", method="text")
            if body_text:
                text_parts.append(body_text.strip())

    # If no structured content found, get all text
    if not text_parts:
        all_text = ET.tostring(root, encoding="unicode", method="text")
        text_parts.append(all_text)

    full_text = "\n\n".join(text_parts)
    # Clean up whitespace
    full_text = re.sub(r"\s+", " ", full_text)
    full_text = re.sub(r"\. ", ".\n", full_text)  # Basic sentence breaks

    return full_text.strip()


def fetch_bill_text(text_url: str, api_key: str) -> Optional[str]:
    """Fetch and extract full text from bill XML."""
    try:
        # Get text versions
        resp = make_request(f"{text_url}&api_key={api_key}")
        if not resp:
            return None
        data = resp.json()

        text_versions = data.get("textVersions", [])
        if not text_versions:
            return None

        # Get the most recent text version (first in list)
        latest = text_versions[0]
        formats = latest.get("formats", [])

        # Prefer XML format
        xml_url = None
        htm_url = None
        for fmt in formats:
            if fmt.get("type") == "Formatted XML":
                xml_url = fmt.get("url")
            elif fmt.get("type") == "Formatted Text":
                htm_url = fmt.get("url")

        if xml_url:
            time.sleep(DELAY_SECONDS)
            resp = make_request(xml_url)
            if resp:
                return extract_text_from_xml(resp.text)

        if htm_url:
            time.sleep(DELAY_SECONDS)
            resp = make_request(htm_url)
            if resp:
                return strip_xml_tags(resp.text)

        return None

    except Exception as e:
        print(f"  Error fetching bill text: {e}", file=sys.stderr)
        return None


def fetch_bill_details(bill_url: str, api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch detailed bill information."""
    try:
        resp = make_request(f"{bill_url}&api_key={api_key}")
        if not resp:
            return None
        data = resp.json()
        return data.get("bill", {})
    except Exception as e:
        print(f"  Error fetching bill details: {e}", file=sys.stderr)
        return None


def normalize(raw: Dict[str, Any], full_text: str) -> Dict[str, Any]:
    """Normalize a bill record to standard schema."""
    congress = raw.get("congress", "")
    bill_type = raw.get("type", "").lower()
    bill_number = raw.get("number", "")

    # Create unique ID
    doc_id = f"{congress}-{bill_type}-{bill_number}"

    # Get date
    intro_date = raw.get("introducedDate", "")
    latest_action = raw.get("latestAction", {})
    action_date = latest_action.get("actionDate", "")
    date_iso = intro_date or action_date

    # Build URL
    url = raw.get("legislationUrl", raw.get("url", ""))
    if not url and congress and bill_type and bill_number:
        url = f"https://www.congress.gov/bill/{congress}th-congress/{bill_type}/{bill_number}"

    # Get sponsor info
    sponsors = raw.get("sponsors", [])
    sponsor = None
    if sponsors:
        sponsor = sponsors[0].get("fullName", sponsors[0].get("name", ""))

    # Get policy area
    policy_area = raw.get("policyArea", {})
    policy_name = policy_area.get("name") if policy_area else None

    # Get cosponsors count
    cosponsors = raw.get("cosponsors", {})
    cosponsors_count = cosponsors.get("count", 0) if isinstance(cosponsors, dict) else 0

    return {
        "_id": doc_id,
        "_source": "US/CongressGov",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ""),
        "text": full_text,
        "date": date_iso,
        "url": url,
        "congress": congress,
        "bill_type": bill_type.upper(),
        "bill_number": bill_number,
        "sponsor": sponsor,
        "cosponsors_count": cosponsors_count,
        "policy_area": policy_name,
        "origin_chamber": raw.get("originChamber", ""),
        "latest_action": latest_action.get("text", ""),
        "update_date": raw.get("updateDate", ""),
    }


def fetch_all(
    congress: int = 118,
    bill_types: Optional[list] = None,
    max_records: Optional[int] = None,
) -> Generator[Dict[str, Any], None, None]:
    """Fetch all bills from specified Congress."""
    api_key = get_api_key()

    if bill_types is None:
        bill_types = ["hr", "s", "hjres", "sjres"]

    fetched = 0

    for bill_type in bill_types:
        if max_records and fetched >= max_records:
            return

        offset = 0
        limit = 250

        while True:
            if max_records and fetched >= max_records:
                return

            url = f"{API_BASE}/bill/{congress}/{bill_type}?format=json&limit={limit}&offset={offset}&api_key={api_key}"

            time.sleep(DELAY_SECONDS)
            resp = make_request(url)
            if not resp:
                print(f"Error fetching bills list, stopping", file=sys.stderr)
                break

            data = resp.json()
            bills = data.get("bills", [])
            if not bills:
                break

            for bill in bills:
                if max_records and fetched >= max_records:
                    return

                # Get detailed bill info
                bill_url = bill.get("url", "")
                if not bill_url:
                    continue

                time.sleep(DELAY_SECONDS)
                details = fetch_bill_details(bill_url, api_key)
                if not details:
                    continue

                # Get text versions URL
                text_versions = details.get("textVersions", {})
                text_url = text_versions.get("url") if isinstance(text_versions, dict) else None

                full_text = None
                if text_url:
                    time.sleep(DELAY_SECONDS)
                    full_text = fetch_bill_text(text_url, api_key)

                if not full_text or len(full_text) < 100:
                    # Skip bills without substantial text
                    continue

                record = normalize(details, full_text)
                yield record
                fetched += 1

                if fetched % 10 == 0:
                    print(f"  Fetched {fetched} records...")

            # Check for next page
            pagination = data.get("pagination", {})
            next_url = pagination.get("next")
            if not next_url:
                break

            offset += limit


def fetch_updates(since: datetime) -> Generator[Dict[str, Any], None, None]:
    """Fetch bills updated after a given date."""
    api_key = get_api_key()
    since_str = since.strftime("%Y-%m-%dT00:00:00Z")

    # Use the fromDateTime parameter
    offset = 0
    limit = 250

    while True:
        url = f"{API_BASE}/bill?format=json&limit={limit}&offset={offset}&fromDateTime={since_str}&api_key={api_key}"

        time.sleep(DELAY_SECONDS)
        resp = make_request(url)
        if not resp:
            print(f"Error fetching updates, stopping", file=sys.stderr)
            break

        data = resp.json()
        bills = data.get("bills", [])
        if not bills:
            break

        for bill in bills:
            bill_url = bill.get("url", "")
            if not bill_url:
                continue

            time.sleep(DELAY_SECONDS)
            details = fetch_bill_details(bill_url, api_key)
            if not details:
                continue

            text_versions = details.get("textVersions", {})
            text_url = text_versions.get("url") if isinstance(text_versions, dict) else None

            full_text = None
            if text_url:
                time.sleep(DELAY_SECONDS)
                full_text = fetch_bill_text(text_url, api_key)

            if not full_text or len(full_text) < 100:
                continue

            record = normalize(details, full_text)
            yield record

        pagination = data.get("pagination", {})
        next_url = pagination.get("next")
        if not next_url:
            break

        offset += limit


def bootstrap_sample(sample_dir: Path, count: int = 15) -> int:
    """Fetch sample records and save to sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    print(f"Fetching {count} sample bills from Congress.gov...")

    # Fetch from recent Congress with different bill types for variety
    for record in fetch_all(congress=118, bill_types=["hr", "s"], max_records=count):
        # Create safe filename from ID
        safe_id = record["_id"].replace("/", "_").replace("\\", "_")
        filename = f"{safe_id}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        title_preview = record.get("title", "")[:60]
        print(f"  Saved {filename} ({text_len:,} chars) - {title_preview}...")
        saved += 1

    return saved


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records meet requirements."""
    samples = list(sample_dir.glob("*.json"))

    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need at least 10")
        return False

    total_text_len = 0
    all_valid = True

    for sample_path in samples:
        with open(sample_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        text = record.get("text", "")
        if not text:
            print(f"FAIL: {sample_path.name} has no text")
            all_valid = False
        elif len(text) < 500:
            print(f"WARN: {sample_path.name} has short text ({len(text)} chars)")

        total_text_len += len(text)

        # Check required fields
        for field in ["_id", "_source", "_type", "title"]:
            if not record.get(field):
                print(f"WARN: {sample_path.name} missing {field}")

        # Check for raw HTML/XML tags
        if text and re.search(r"<[a-z]+[^>]*>", text, re.IGNORECASE):
            print(f"WARN: {sample_path.name} may contain HTML/XML tags")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="US/CongressGov data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "validate", "fetch"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of records to fetch",
    )
    parser.add_argument(
        "--congress",
        type=int,
        default=118,
        help="Congress number (default: 118)",
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Fetch records since date (YYYY-MM-DD)",
    )

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            saved = bootstrap_sample(sample_dir, args.count)
            print(f"\nSaved {saved} sample records to {sample_dir}")

            # Also run validation
            print("\nValidating samples...")
            valid = validate_samples(sample_dir)
            sys.exit(0 if saved >= 10 and valid else 1)
        else:
            print("Use --sample for bootstrap mode")
            sys.exit(1)

    elif args.command == "validate":
        valid = validate_samples(sample_dir)
        sys.exit(0 if valid else 1)

    elif args.command == "fetch":
        if args.since:
            since = datetime.fromisoformat(args.since)
            for record in fetch_updates(since):
                print(json.dumps(record, ensure_ascii=False))
        else:
            for record in fetch_all(congress=args.congress, max_records=args.count):
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
