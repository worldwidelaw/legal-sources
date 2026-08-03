#!/usr/bin/env python3
"""
US/OpenStates -- Open States Legislative Data

Fetches state-level legislation from all 50 US states + DC + Puerto Rico
via the Open States API v3.

REQUIRES API KEY: Set OPENSTATES_API_KEY environment variable.
Register free at: https://open.pluralpolicy.com/accounts/profile/

The API provides bill metadata and links to bill versions (full text).
We fetch the version documents (HTML/PDF) and extract text content.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional
from html import unescape
import xml.etree.ElementTree as ET

import requests

# Configuration
SOURCE_ID = "US/OpenStates"
API_BASE = "https://v3.openstates.org"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research; contact@legaldatahunter.com)"
REQUEST_DELAY = 1.0  # seconds between requests
PER_PAGE = 20  # Open States API caps bill search at 20 per page

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
SAMPLE_DIR = SCRIPT_DIR / "sample"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

# Make the repo root importable so we can reuse common.storage for the
# full-corpus path (streams to data/records.jsonl for fleet ingest).
ROOT_DIR = SCRIPT_DIR.parent.parent.parent
sys.path.insert(0, str(ROOT_DIR))

from common.storage import StorageManager  # noqa: E402

# US State/Territory codes
JURISDICTIONS = [
    "al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga",
    "hi", "id", "il", "in", "ia", "ks", "ky", "la", "me", "md",
    "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj",
    "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc",
    "sd", "tn", "tx", "ut", "vt", "va", "wa", "wv", "wi", "wy",
    "dc", "pr"
]


class OpenStatesAPI:
    """Client for Open States API v3."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = API_BASE
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with auth headers."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "X-API-KEY": self.api_key,
        })
        return session

    def _request(self, endpoint: str, params: Dict = None, retries: int = 3) -> Dict:
        """Make an API request with retry logic."""
        url = f"{self.base_url}/{endpoint}"

        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=60)

                if response.status_code == 429:
                    # Rate limited
                    wait = 2 ** (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue

                if response.status_code == 401:
                    raise ValueError("Invalid API key. Register at https://open.pluralpolicy.com/accounts/profile/")

                if response.status_code == 404:
                    return {}

                response.raise_for_status()
                return response.json()

            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    print(f"  Timeout, retrying...")
                    time.sleep(2)
                    continue
                raise
            except requests.exceptions.RequestException as e:
                if attempt < retries - 1:
                    time.sleep(2)
                    continue
                raise

        return {}

    def get_jurisdictions(self) -> List[Dict]:
        """Get list of available jurisdictions."""
        result = self._request("jurisdictions", {"per_page": 52})
        return result.get("results", [])

    def search_bills(
        self,
        jurisdiction: str = None,
        session: str = None,
        updated_since: str = None,
        page: int = 1,
        per_page: int = 20,
        include: List[str] = None,
    ) -> Dict:
        """Search for bills with optional filters."""
        params = {
            "page": page,
            "per_page": per_page,
        }

        if jurisdiction:
            params["jurisdiction"] = jurisdiction
        if session:
            params["session"] = session
        if updated_since:
            params["updated_since"] = updated_since
        if include:
            params["include"] = include

        return self._request("bills", params)

    def get_bill(self, bill_id: str, include: List[str] = None) -> Dict:
        """Get a single bill by ID."""
        params = {}
        if include:
            params["include"] = include

        # Handle both ocd-bill/UUID and jurisdiction/session/identifier formats
        if bill_id.startswith("ocd-bill/"):
            endpoint = f"bills/{bill_id}"
        else:
            endpoint = f"bills/{bill_id}"

        return self._request(endpoint, params)

    def fetch_document(self, url: str) -> str:
        """Fetch a document from URL and extract text."""
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "").lower()

            if "html" in content_type:
                return self._extract_text_from_html(response.text)
            elif "xml" in content_type:
                return self._extract_text_from_xml(response.text)
            elif "pdf" in content_type:
                # PDFs require special handling, skip for now
                return ""
            else:
                # Try to extract as HTML
                return self._extract_text_from_html(response.text)

        except requests.exceptions.RequestException:
            return ""

    def _extract_text_from_html(self, html: str) -> str:
        """Extract text from HTML content."""
        # Remove script and style elements
        html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html)

        # Decode HTML entities
        text = unescape(text)

        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()

        return text

    def _extract_text_from_xml(self, xml: str) -> str:
        """Extract text from XML content."""
        try:
            root = ET.fromstring(xml)
            text_parts = []
            for elem in root.iter():
                if elem.text:
                    text_parts.append(elem.text.strip())
                if elem.tail:
                    text_parts.append(elem.tail.strip())
            return ' '.join(text_parts)
        except ET.ParseError:
            # Fall back to regex extraction
            return self._extract_text_from_html(xml)


def normalize(bill: Dict, full_text: str) -> Dict:
    """Transform Open States bill data into normalized schema."""

    # Get the primary identifier
    bill_id = bill.get("id", "")
    identifier = bill.get("identifier", "")
    jurisdiction = bill.get("jurisdiction", {}).get("name", "")
    jurisdiction_id = bill.get("jurisdiction", {}).get("id", "")

    # Extract state code from jurisdiction ID
    # Format: ocd-jurisdiction/country:us/state:ca/government
    state_code = ""
    if jurisdiction_id:
        match = re.search(r'/state:([a-z]{2})/', jurisdiction_id)
        if match:
            state_code = match.group(1).upper()

    # Get dates
    latest_action_date = None
    if bill.get("latest_action_date"):
        latest_action_date = bill["latest_action_date"]
    elif bill.get("actions"):
        actions = bill["actions"]
        if actions:
            latest_action_date = actions[-1].get("date")

    # Build abstract from abstracts list
    abstract = ""
    if bill.get("abstracts"):
        abstract = " ".join([a.get("abstract", "") for a in bill["abstracts"] if a.get("abstract")])

    # Get session info
    session_name = bill.get("session", "")

    # Get classifications
    classifications = bill.get("classification", [])

    # Construct a URL
    url = f"https://openstates.org/{state_code.lower()}/bills/{session_name}/{identifier}/"
    if bill.get("sources"):
        # Prefer official source
        for src in bill["sources"]:
            if src.get("url"):
                url = src["url"]
                break

    return {
        "_id": f"openstates-{bill_id.replace('ocd-bill/', '')}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": bill.get("title", ""),
        "text": full_text,
        "abstract": abstract,
        "date": latest_action_date,
        "url": url,
        "identifier": identifier,
        "session": session_name,
        "jurisdiction": jurisdiction,
        "state_code": state_code,
        "classification": classifications,
        "openstates_id": bill_id,
    }


def fetch_bill_full_text(client: OpenStatesAPI, bill: Dict) -> str:
    """Fetch full text for a bill from its versions."""
    versions = bill.get("versions", [])

    if not versions:
        return ""

    # Sort versions by date (newest first)
    sorted_versions = sorted(
        versions,
        key=lambda v: v.get("date") or "",
        reverse=True
    )

    # Try to get text from HTML links first (easier to parse than PDF)
    for version in sorted_versions:
        links = version.get("links", [])

        # Prefer HTML/XML over PDF
        html_links = [l for l in links if l.get("media_type") in ["text/html", "application/xml"]]

        for link in html_links:
            url = link.get("url", "")
            if not url:
                continue

            print(f"    Fetching version: {url[:80]}...")
            text = client.fetch_document(url)
            time.sleep(0.5)  # Small delay between document fetches

            if text and len(text) > 500:
                return text

    # If no HTML, try any link
    for version in sorted_versions:
        links = version.get("links", [])

        for link in links:
            url = link.get("url", "")
            media_type = link.get("media_type", "")

            # Skip PDFs (require specialized extraction)
            if media_type == "application/pdf":
                continue

            if not url:
                continue

            print(f"    Fetching version: {url[:80]}...")
            text = client.fetch_document(url)
            time.sleep(0.5)

            if text and len(text) > 500:
                return text

    return ""


BILL_INCLUDES = ["versions", "abstracts", "actions", "sources"]


def _load_checkpoint() -> Dict[str, Any]:
    """Load the resume checkpoint (completed jurisdictions + last page)."""
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {"done_jurisdictions": [], "progress": {}}


def _save_checkpoint(ckpt: Dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ckpt, f)
    tmp.replace(CHECKPOINT_PATH)


def fetch_all(
    client: OpenStatesAPI,
    jurisdictions: Optional[List[str]] = None,
    updated_since: Optional[str] = None,
    max_records: Optional[int] = None,
    checkpoint: Optional[Dict[str, Any]] = None,
) -> Generator[Dict, None, None]:
    """Yield normalized, full-text bill records across every jurisdiction.

    Paginates the /bills endpoint per jurisdiction (sorted updated_asc for a
    stable walk), pulling versions/abstracts/actions/sources inline, then
    fetches each bill's version document to obtain full text. Bills with no
    retrievable full text are skipped (metadata-only records are not useful).

    When a ``checkpoint`` dict is supplied, completed jurisdictions are skipped
    and per-jurisdiction page progress is recorded so a crashed run resumes
    where it stopped.
    """
    jurisdictions = jurisdictions or JURISDICTIONS
    yielded = 0

    done = set((checkpoint or {}).get("done_jurisdictions", []))
    progress = (checkpoint or {}).get("progress", {})

    for juris in jurisdictions:
        if juris in done:
            print(f"=== {juris.upper()} (already complete, skipping) ===")
            continue

        start_page = int(progress.get(juris, 0)) + 1
        print(f"=== {juris.upper()} (from page {start_page}) ===")

        page = start_page
        max_page = None
        while True:
            result = client.search_bills(
                jurisdiction=juris,
                updated_since=updated_since,
                page=page,
                per_page=PER_PAGE,
                include=BILL_INCLUDES,
            )
            time.sleep(REQUEST_DELAY)

            bills = result.get("results", [])
            if max_page is None:
                max_page = result.get("pagination", {}).get("max_page", 1)

            if not bills:
                break

            for bill in bills:
                full_text = fetch_bill_full_text(client, bill)
                if not full_text:
                    continue
                yield normalize(bill, full_text)
                yielded += 1
                if max_records and yielded >= max_records:
                    return
                time.sleep(REQUEST_DELAY)

            # Record page progress for resume, then advance.
            if checkpoint is not None:
                progress[juris] = page
                checkpoint["progress"] = progress
                _save_checkpoint(checkpoint)

            if max_page and page >= max_page:
                break
            page += 1

        # Mark the whole jurisdiction complete.
        if checkpoint is not None:
            done.add(juris)
            checkpoint["done_jurisdictions"] = sorted(done)
            progress.pop(juris, None)
            checkpoint["progress"] = progress
            _save_checkpoint(checkpoint)


def fetch_updates(
    client: OpenStatesAPI, since: str, max_records: Optional[int] = None
) -> Generator[Dict, None, None]:
    """Yield bills updated since ``since`` (YYYY-MM-DD) across all jurisdictions."""
    yield from fetch_all(client, updated_since=since, max_records=max_records)


def bootstrap_full(
    jurisdictions: Optional[List[str]] = None,
    updated_since: Optional[str] = None,
    max_records: Optional[int] = None,
) -> int:
    """Full corpus fetch that STREAMS every full-text bill to
    data/records.jsonl via StorageManager.

    This is the path the fleet ingest uses. It is idempotent: re-runs skip
    bills already written (dedup by _id) and resume from the jurisdiction/page
    checkpoint, so a crashed run continues where it stopped.
    """
    api_key = os.environ.get("OPENSTATES_API_KEY")
    if not api_key:
        print("ERROR: OPENSTATES_API_KEY environment variable not set.", file=sys.stderr)
        return 0

    client = OpenStatesAPI(api_key)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    storage = StorageManager(DATA_DIR)
    checkpoint = _load_checkpoint()

    written = 0
    skipped = 0
    for record in fetch_all(
        client,
        jurisdictions=jurisdictions,
        updated_since=updated_since,
        max_records=max_records,
        checkpoint=checkpoint,
    ):
        dedup_key = record["_id"]
        if storage.exists(dedup_key):
            skipped += 1
            continue
        storage.write(dedup_key, record)
        written += 1
        if written % 25 == 0:
            print(f"  Wrote {written} records (skipped {skipped} dupes)...")

    storage.close()
    print(f"\nFull bootstrap complete: {written} written, {skipped} skipped")
    print(f"Records at: {storage.records_path}")
    return written


def fetch_sample(client: OpenStatesAPI, count: int = 15) -> List[Dict]:
    """Fetch sample bills from multiple states."""
    print(f"Fetching {count} sample bills from Open States...")
    records = []

    # Sample from different states
    sample_states = ["ca", "ny", "tx", "fl", "il", "pa", "oh", "mi", "nc", "nj"]

    for state in sample_states:
        if len(records) >= count:
            break

        print(f"\n  Searching bills in {state.upper()}...")

        # Get recent bills with versions included
        result = client.search_bills(
            jurisdiction=state,
            per_page=5,
            include=["versions", "abstracts", "actions", "sources"]
        )
        time.sleep(REQUEST_DELAY)

        bills = result.get("results", [])

        for bill in bills:
            if len(records) >= count:
                break

            identifier = bill.get("identifier", "unknown")
            title = bill.get("title", "")[:60]
            print(f"  Processing {state.upper()} {identifier}: {title}...")

            # Fetch full text from versions
            full_text = fetch_bill_full_text(client, bill)

            if not full_text:
                print(f"    No full text available, skipping...")
                continue

            # Normalize the record
            record = normalize(bill, full_text)
            records.append(record)

            print(f"    [{len(records)}] {record['_id']}: {len(full_text):,} chars")

            time.sleep(REQUEST_DELAY)

    return records


def save_samples(records: List[Dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filename = f"record_{i:04d}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records meet requirements."""
    samples = list(sample_dir.glob("record_*.json"))

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

        # Check for raw HTML tags
        if text and re.search(r"<[a-z]+[^>]*>", text, re.IGNORECASE):
            print(f"WARN: {sample_path.name} may contain HTML tags")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="US/OpenStates data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    for cmd in ("bootstrap", "bootstrap-fast"):
        p = subparsers.add_parser(cmd, help="Initial data fetch")
        p.add_argument("--sample", action="store_true", help="Fetch sample only")
        p.add_argument("--full", action="store_true", help="Fetch full corpus to data/records.jsonl")
        p.add_argument("--max-records", type=int, default=None, help="Cap total records (testing)")
        p.add_argument("--jurisdictions", type=str, default=None,
                       help="Comma-separated state codes to restrict --full (e.g. ca,ny). "
                            "Enables per-state fleet parallelism; default = all 52.")

    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")
    updates_parser.add_argument("--max-records", type=int, default=None, help="Cap total records")

    subparsers.add_parser("validate", help="Validate sample records")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        valid = validate_samples(SAMPLE_DIR)
        sys.exit(0 if valid else 1)

    # Check for API key
    api_key = os.environ.get("OPENSTATES_API_KEY")
    if not api_key:
        print("ERROR: OPENSTATES_API_KEY environment variable not set.", file=sys.stderr)
        print("Register for a free key at: https://open.pluralpolicy.com/accounts/profile/", file=sys.stderr)
        sys.exit(1)

    client = OpenStatesAPI(api_key)

    if args.command in ("bootstrap", "bootstrap-fast"):
        if args.full:
            juris = None
            if args.jurisdictions:
                juris = [j.strip().lower() for j in args.jurisdictions.split(",") if j.strip()]
            written = bootstrap_full(jurisdictions=juris, max_records=args.max_records)
            sys.exit(0 if written > 0 else 1)
        if args.sample:
            print("Fetching samples from Open States API...")
            try:
                records = fetch_sample(client, count=15)
                if records:
                    save_samples(records)

                    # Validation summary
                    text_lengths = [len(r.get('text', '')) for r in records]
                    avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0
                    print(f"\nSummary:")
                    print(f"  Records: {len(records)}")
                    print(f"  Avg text length: {avg_len:,.0f} chars")
                    print(f"  Min text length: {min(text_lengths):,} chars")
                    print(f"  Max text length: {max(text_lengths):,} chars")

                    # Run validation
                    print("\nValidating samples...")
                    valid = validate_samples(SAMPLE_DIR)
                    sys.exit(0 if len(records) >= 10 and valid else 1)
                else:
                    print("No records fetched!", file=sys.stderr)
                    sys.exit(1)

            except ValueError as e:
                print(f"API Error: {e}", file=sys.stderr)
                sys.exit(1)
            except requests.HTTPError as e:
                print(f"HTTP error: {e}", file=sys.stderr)
                sys.exit(1)

        else:
            print("Use --sample for a sample fetch or --full for the full corpus")
            sys.exit(1)

    elif args.command == "updates":
        print(f"Fetching updates since {args.since}...")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        storage = StorageManager(DATA_DIR)
        written = skipped = 0
        for record in fetch_updates(client, args.since, max_records=args.max_records):
            dedup_key = record["_id"]
            if storage.exists(dedup_key):
                skipped += 1
                continue
            storage.write(dedup_key, record)
            written += 1
            if written % 25 == 0:
                print(f"  Wrote {written} records (skipped {skipped} dupes)...")
        storage.close()
        print(f"\nUpdates complete: {written} written, {skipped} skipped")
        sys.exit(0 if written > 0 else 1)


if __name__ == "__main__":
    main()
