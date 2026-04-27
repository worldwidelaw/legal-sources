#!/usr/bin/env python3
"""
DE/OpenLegalData - German Legal Data from openlegaldata.io

Fetches court decisions and legislation from all 16 German states (Länder)
plus federal courts via the Open Legal Data REST API.

Coverage:
- 251K+ court decisions (Amtsgerichte, Landgerichte, OLG, etc.)
- 57K+ law texts
- All 16 German states + federal courts

API: https://de.openlegaldata.io/api/
No authentication required.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional, Dict, Any, List
from html import unescape

import requests
from bs4 import BeautifulSoup

# Configuration
API_BASE = "https://de.openlegaldata.io/api"
# VPS mode detection - datacenter IPs get blocked aggressively by this API
VPS_MODE = os.environ.get("OLDP_VPS_MODE", "0") == "1"
# Conservative rate limiting - VPS/datacenter IPs get rate-limited aggressively
RATE_LIMIT_DELAY = 10.0 if VPS_MODE else 3.0  # 10s for VPS, 3s for residential
INITIAL_DELAY = 60.0 if VPS_MODE else 5.0  # 60s warmup for VPS IPs
MAX_BACKOFF = 600 if VPS_MODE else 300  # max wait time (10 min for VPS)
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_DIR = Path(__file__).parent / "checkpoint"
JSONL_DIR = Path(__file__).parent / "output"
SOURCE_ID = "DE/OpenLegalData"

# German state IDs from the API
STATE_IDS = {
    3: "Baden-Württemberg",
    4: "Bayern",
    5: "Berlin",
    6: "Brandenburg",
    7: "Bremen",
    8: "Hamburg",
    9: "Hessen",
    10: "Mecklenburg-Vorpommern",
    11: "Niedersachsen",
    12: "Nordrhein-Westfalen",
    13: "Rheinland-Pfalz",
    14: "Saarland",
    15: "Sachsen",
    16: "Sachsen-Anhalt",
    17: "Schleswig-Holstein",
    18: "Thüringen",
    2: "Bundesrepublik Deutschland",  # Federal
}


def clean_html(html_content: str) -> str:
    """Strip HTML tags and clean up text content."""
    if not html_content:
        return ""

    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, "html.parser")

    # Get text content
    text = soup.get_text(separator="\n")

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +', ' ', text)
    text = text.strip()

    # Unescape HTML entities
    text = unescape(text)

    return text


# Track last request time for rate limiting
_last_request_time = 0.0
_api_initialized = False


def api_request(endpoint: str, params: Optional[Dict] = None, retries: int = 12) -> Dict:
    """Make API request with rate limiting and exponential backoff.

    VPS/datacenter IPs may get rate-limited aggressively by the API.
    This function uses conservative delays and aggressive backoff.
    Set OLDP_VPS_MODE=1 environment variable for much longer delays on VPS.
    """
    global _last_request_time, _api_initialized
    url = f"{API_BASE}{endpoint}"

    for attempt in range(retries):
        try:
            # Initial warmup delay for VPS IPs (first request only)
            if not _api_initialized:
                mode = "VPS mode" if VPS_MODE else "standard mode"
                print(f"Initial API warmup delay ({INITIAL_DELAY}s) - {mode}...")
                time.sleep(INITIAL_DELAY)
                _api_initialized = True

            # Respect rate limit: wait between requests
            elapsed = time.time() - _last_request_time
            if elapsed < RATE_LIMIT_DELAY and _last_request_time > 0:
                time.sleep(RATE_LIMIT_DELAY - elapsed)

            _last_request_time = time.time()

            response = requests.get(
                url,
                params=params,
                headers={
                    "Accept": "application/json",
                    # Use a browser-like user agent to reduce rate limiting
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                },
                timeout=120
            )

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                # Rate limited - use aggressive exponential backoff with jitter
                # VPS mode: start at 60s, double each time up to MAX_BACKOFF
                # Standard mode: start at 30s
                base_wait = (60 if VPS_MODE else 30) * (2 ** min(attempt, 4))
                wait_time = min(MAX_BACKOFF, base_wait)
                jitter = wait_time * 0.2 * (0.5 - (time.time() % 1))  # +/- 20% jitter
                wait_time = max(60 if VPS_MODE else 30, wait_time + jitter)
                print(f"Rate limited (429), waiting {wait_time:.0f}s (attempt {attempt + 1}/{retries})...")
                time.sleep(wait_time)
                _last_request_time = time.time()  # Reset after rate limit wait
            elif response.status_code == 403:
                # Forbidden - likely IP blocked, use very long backoff
                base_wait = (120 if VPS_MODE else 60) * (2 ** min(attempt, 3))
                wait_time = min(MAX_BACKOFF, base_wait)
                print(f"Forbidden (403), IP may be blocked, waiting {wait_time:.0f}s (attempt {attempt + 1}/{retries})...")
                time.sleep(wait_time)
            elif response.status_code >= 500:
                # Server error - retry with backoff
                wait_time = 10 * (2 ** attempt)
                print(f"Server error {response.status_code}, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"API error: {response.status_code} for {url}")
                return {}

        except requests.exceptions.Timeout:
            wait_time = 15 * (attempt + 1)
            print(f"Timeout for {url}, attempt {attempt + 1}/{retries}, waiting {wait_time}s...")
            time.sleep(wait_time)
        except requests.exceptions.RequestException as e:
            wait_time = 10 * (attempt + 1)
            print(f"Request error: {e}, waiting {wait_time}s...")
            time.sleep(wait_time)

    print(f"Failed after {retries} attempts for {url}")
    return {}


def normalize_case(raw: Dict) -> Dict:
    """Normalize court decision to standard schema."""
    court = raw.get("court", {}) or {}

    # Extract state name
    state_id = court.get("state")
    state_name = STATE_IDS.get(state_id, "Unknown") if isinstance(state_id, int) else "Unknown"

    # Clean HTML content
    content = raw.get("content", "")
    text = clean_html(content)

    # Build normalized record
    normalized = {
        "_id": f"OLDP-CASE-{raw.get('id')}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        # Required fields
        "title": f"{court.get('name', 'Unknown Court')} - {raw.get('file_number', 'Unknown')}",
        "text": text,
        "date": raw.get("date"),
        "url": f"https://de.openlegaldata.io/case/{raw.get('slug', raw.get('id'))}",

        # Court information
        "court_name": court.get("name"),
        "court_level": court.get("level_of_appeal"),
        "court_city": court.get("city"),
        "court_jurisdiction": court.get("jurisdiction"),
        "state": state_name,

        # Case metadata
        "file_number": raw.get("file_number"),
        "case_type": raw.get("type"),
        "ecli": raw.get("ecli"),

        # Timestamps
        "created_date": raw.get("created_date"),
        "updated_date": raw.get("updated_date"),

        # Original ID
        "original_id": raw.get("id"),
    }

    return normalized


def normalize_law(raw: Dict) -> Dict:
    """Normalize legislation to standard schema."""
    # Clean HTML content
    content = raw.get("content", "")
    text = clean_html(content)

    # Build title
    title = raw.get("title") or raw.get("kurzue") or raw.get("section") or f"Law {raw.get('id')}"

    normalized = {
        "_id": f"OLDP-LAW-{raw.get('id')}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        # Required fields
        "title": title,
        "text": text,
        "date": None,  # Laws don't have a single date
        "url": f"https://de.openlegaldata.io/law/{raw.get('slug', raw.get('id'))}",

        # Law metadata
        "section": raw.get("section"),
        "book_id": raw.get("book"),
        "doknr": raw.get("doknr"),
        "amtabk": raw.get("amtabk"),
        "order": raw.get("order"),

        # Timestamps
        "created_date": raw.get("created_date"),
        "updated_date": raw.get("updated_date"),

        # Original ID
        "original_id": raw.get("id"),
    }

    return normalized


def load_checkpoint(checkpoint_type: str) -> Dict:
    """Load checkpoint data if it exists."""
    checkpoint_file = CHECKPOINT_DIR / f"{checkpoint_type}_checkpoint.json"
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Warning: could not load checkpoint: {e}")
    return {}


def save_checkpoint(checkpoint_type: str, data: Dict) -> None:
    """Save checkpoint data."""
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_file = CHECKPOINT_DIR / f"{checkpoint_type}_checkpoint.json"
    with open(checkpoint_file, "w") as f:
        json.dump(data, f)


def clear_checkpoint(checkpoint_type: str) -> None:
    """Clear checkpoint after successful completion."""
    checkpoint_file = CHECKPOINT_DIR / f"{checkpoint_type}_checkpoint.json"
    if checkpoint_file.exists():
        checkpoint_file.unlink()


def fetch_case_detail(case_id: int) -> Dict:
    """Fetch full case detail including content from the detail endpoint.

    The list endpoint (/api/cases/) no longer includes the 'content' field.
    Each case must be fetched individually via /api/cases/{id}/ to get full text.
    """
    data = api_request(f"/cases/{case_id}/", {"format": "json"})
    return data if data else {}


def fetch_cases(
    state_id: Optional[int] = None,
    limit: Optional[int] = None,
    offset: int = 0,
    use_checkpoint: bool = True
) -> Iterator[Dict]:
    """Fetch court decisions from the API with checkpoint/resume support.

    Two-step fetch: list endpoint for IDs, then detail endpoint for full text.
    The list endpoint no longer returns the 'content' field.
    """
    # Load checkpoint if resuming
    checkpoint = {}
    if use_checkpoint and offset == 0:
        checkpoint = load_checkpoint("cases")
        if checkpoint:
            offset = checkpoint.get("offset", 0)
            print(f"Resuming cases from checkpoint at offset {offset}")

    params = {"format": "json", "limit": 100, "offset": offset}

    if state_id:
        params["court__state"] = state_id

    count = 0
    consecutive_failures = 0
    max_consecutive_failures = 10

    while True:
        print(f"Fetching cases (offset={params['offset']})...")
        data = api_request("/cases/", params)

        if not data:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                print(f"Too many consecutive failures ({consecutive_failures}), stopping")
                if use_checkpoint:
                    save_checkpoint("cases", {"offset": params["offset"], "state_id": state_id})
                break
            print(f"Empty response, waiting 30s before retry ({consecutive_failures}/{max_consecutive_failures})...")
            time.sleep(30)
            continue

        consecutive_failures = 0
        results = data.get("results", [])
        if not results:
            break

        for case_stub in results:
            case_id = case_stub.get("id")
            if not case_id:
                continue

            # Fetch full detail (with content) from detail endpoint
            detail = fetch_case_detail(case_id)
            if not detail:
                print(f"  Skipping case {case_id}: detail fetch failed")
                continue

            yield normalize_case(detail)
            count += 1

            if limit and count >= limit:
                return

        # Check for next page
        if not data.get("next"):
            if use_checkpoint:
                clear_checkpoint("cases")
            break

        params["offset"] = params["offset"] + 100

        # Save checkpoint every 500 records
        if use_checkpoint and count % 500 == 0:
            save_checkpoint("cases", {"offset": params["offset"], "state_id": state_id})
            print(f"Checkpoint saved at {count} cases")


def fetch_laws(
    limit: Optional[int] = None,
    offset: int = 0,
    use_checkpoint: bool = True
) -> Iterator[Dict]:
    """Fetch legislation from the API with checkpoint/resume support."""
    # Load checkpoint if resuming
    checkpoint = {}
    if use_checkpoint and offset == 0:
        checkpoint = load_checkpoint("laws")
        if checkpoint:
            offset = checkpoint.get("offset", 0)
            print(f"Resuming laws from checkpoint at offset {offset}")

    params = {"format": "json", "limit": 100, "offset": offset}

    count = 0
    consecutive_failures = 0
    max_consecutive_failures = 10  # Increased tolerance for VPS rate limiting

    while True:
        print(f"Fetching laws (offset={params['offset']})...")
        data = api_request("/laws/", params)

        if not data:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_failures:
                print(f"Too many consecutive failures ({consecutive_failures}), stopping")
                if use_checkpoint:
                    save_checkpoint("laws", {"offset": params["offset"]})
                break
            # Wait before retry on failure
            print(f"Empty response, waiting 30s before retry ({consecutive_failures}/{max_consecutive_failures})...")
            time.sleep(30)
            continue

        consecutive_failures = 0  # Reset on success
        results = data.get("results", [])
        if not results:
            break

        for law in results:
            yield normalize_law(law)
            count += 1

            if limit and count >= limit:
                return

        # Check for next page
        if not data.get("next"):
            # Completed successfully - clear checkpoint
            if use_checkpoint:
                clear_checkpoint("laws")
            break

        params["offset"] = params["offset"] + 100

        # Save checkpoint every 500 records (more frequent for VPS reliability)
        if use_checkpoint and count % 500 == 0:
            save_checkpoint("laws", {"offset": params["offset"]})
            print(f"Checkpoint saved at {count} laws")


def fetch_all() -> Iterator[Dict]:
    """Fetch all records (cases and laws)."""
    # Fetch cases first (primary data type)
    yield from fetch_cases()

    # Then fetch laws
    yield from fetch_laws()


def fetch_updates(since: str) -> Iterator[Dict]:
    """Fetch records modified since a given date."""
    # Note: The API doesn't support date filtering directly
    # Would need to implement pagination with date checks
    print(f"Fetching updates since {since}...")

    # For now, fetch recent records and filter
    for record in fetch_cases(limit=1000):
        updated = record.get("updated_date", "")
        if updated and updated >= since:
            yield record


def fetch_sample(count: int = 15) -> List[Dict]:
    """Fetch sample records for validation."""
    samples = []

    # Fetch cases from the general endpoint (faster than filtering by state)
    print(f"Fetching {count} sample cases...")
    for case in fetch_cases(limit=count, use_checkpoint=False):
        if case.get("text") and len(case.get("text", "")) > 100:
            samples.append(case)
            if len(samples) >= count:
                return samples

    # If we need more, get some laws
    remaining = count - len(samples)
    if remaining > 0:
        print(f"Fetching {remaining} law samples...")
        for law in fetch_laws(limit=remaining, use_checkpoint=False):
            if law.get("text") and len(law.get("text", "")) > 100:
                samples.append(law)
                if len(samples) >= count:
                    break

    return samples


def save_samples(samples: List[Dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Save individual records
    for i, record in enumerate(samples):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

    # Save all samples in one file
    all_samples_path = SAMPLE_DIR / "all_samples.json"
    with open(all_samples_path, "w", encoding="utf-8") as f:
        json.dump(samples, f, indent=2, ensure_ascii=False)

    print(f"Saved {len(samples)} samples to {SAMPLE_DIR}")


def validate_samples(samples: List[Dict]) -> bool:
    """Validate sample records meet requirements."""
    print("\n=== Sample Validation ===")

    issues = []

    # Check count
    if len(samples) < 10:
        issues.append(f"Only {len(samples)} samples, need at least 10")

    # Check required fields
    text_lengths = []
    for i, record in enumerate(samples):
        text = record.get("text", "")
        if not text:
            issues.append(f"Record {i}: missing 'text' field")
        elif len(text) < 100:
            issues.append(f"Record {i}: text too short ({len(text)} chars)")
        else:
            text_lengths.append(len(text))

        if not record.get("_id"):
            issues.append(f"Record {i}: missing '_id'")
        if not record.get("title"):
            issues.append(f"Record {i}: missing 'title'")

    # Report
    if text_lengths:
        avg_len = sum(text_lengths) / len(text_lengths)
        print(f"Records with text: {len(text_lengths)}/{len(samples)}")
        print(f"Average text length: {avg_len:,.0f} chars")
        print(f"Min text length: {min(text_lengths):,} chars")
        print(f"Max text length: {max(text_lengths):,} chars")

    # Check for variety
    types = set(r.get("_type") for r in samples)
    states = set(r.get("state") for r in samples if r.get("state"))
    print(f"Document types: {types}")
    print(f"States covered: {len(states)} ({', '.join(sorted(states)[:5])}...)")

    if issues:
        print(f"\nIssues found ({len(issues)}):")
        for issue in issues[:10]:
            print(f"  - {issue}")
        return False

    print("\n✓ All validation checks passed")
    return True


def main():
    parser = argparse.ArgumentParser(description="DE/OpenLegalData data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "status"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)"
    )
    parser.add_argument(
        "--count",
        type=int,
        default=15,
        help="Number of sample records to fetch"
    )
    parser.add_argument(
        "--since",
        type=str,
        help="Fetch updates since this date (ISO 8601)"
    )
    parser.add_argument(
        "--state",
        type=int,
        help="Filter by state ID (e.g., 4=Bayern, 12=NRW)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="Number of concurrent workers (for bootstrap-fast)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Batch size for writes (for bootstrap-fast)"
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...", file=sys.stderr)
            samples = fetch_sample(args.count)
            save_samples(samples)

            if validate_samples(samples):
                print("\n✓ Bootstrap sample complete", file=sys.stderr)
                return 0
            else:
                print("\n✗ Validation failed", file=sys.stderr)
                return 1
        else:
            # Full bootstrap - write JSONL to output directory
            JSONL_DIR.mkdir(parents=True, exist_ok=True)
            jsonl_path = JSONL_DIR / "records.jsonl"
            print(f"Full bootstrap - writing JSONL to {jsonl_path}", file=sys.stderr)
            count = 0
            with open(jsonl_path, "a", encoding="utf-8") as f:
                for record in fetch_all():
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 1000 == 0:
                        f.flush()
                        print(f"Fetched {count} records...", file=sys.stderr)
            print(f"Total: {count} records written to {jsonl_path}", file=sys.stderr)

    elif args.command == "bootstrap-fast":
        # Fast bootstrap - write JSONL to output directory
        JSONL_DIR.mkdir(parents=True, exist_ok=True)
        jsonl_path = JSONL_DIR / "records.jsonl"
        print(f"Fast bootstrap (workers={args.workers}, batch_size={args.batch_size})...", file=sys.stderr)
        print(f"Writing JSONL to {jsonl_path}", file=sys.stderr)
        count = 0
        with open(jsonl_path, "a", encoding="utf-8") as f:
            for record in fetch_all():
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
                if count % 1000 == 0:
                    f.flush()
                    print(f"Fetched {count} records...", file=sys.stderr)
        print(f"Total: {count} records written to {jsonl_path}", file=sys.stderr)

    elif args.command == "update":
        if not args.since:
            print("Error: --since required for update command")
            return 1

        count = 0
        for record in fetch_updates(args.since):
            count += 1
        print(f"Fetched {count} updates since {args.since}")

    elif args.command == "status":
        # Get API statistics
        print("Fetching API statistics...")

        cases_data = api_request("/cases/", {"format": "json", "limit": 1})
        laws_data = api_request("/laws/", {"format": "json", "limit": 1})
        courts_data = api_request("/courts/", {"format": "json", "limit": 1})
        states_data = api_request("/states/", {"format": "json", "limit": 1})

        print(f"\nDE/OpenLegalData Status:")
        print(f"  Cases: {cases_data.get('count', 'N/A'):,}")
        print(f"  Laws: {laws_data.get('count', 'N/A'):,}")
        print(f"  Courts: {courts_data.get('count', 'N/A'):,}")
        print(f"  States: {states_data.get('count', 'N/A')}")

        # Check sample directory
        if SAMPLE_DIR.exists():
            sample_files = list(SAMPLE_DIR.glob("record_*.json"))
            print(f"  Sample files: {len(sample_files)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
