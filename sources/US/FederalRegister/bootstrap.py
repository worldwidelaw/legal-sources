#!/usr/bin/env python3
"""
US/FederalRegister -- Federal Register API Data Source

Fetches regulatory documents from the Federal Register via their public REST API.
Documents include rules, proposed rules, notices, and presidential documents.

Data available from 1994 to present, with approximately 500-1000 new documents per week.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --recent   # Fetch last 30 days
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

# Configuration
SOURCE_ID = "US/FederalRegister"
BASE_URL = "https://www.federalregister.gov/api/v1"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research; contact@legaldatahunter.com)"
REQUEST_DELAY = 0.5  # seconds between requests

# Transient upstream failures worth retrying rather than aborting the crawl.
RETRY_STATUS = {500, 502, 503, 504}
RETRY_ATTEMPTS = 6
MAX_BACKOFF = 120  # seconds

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
SAMPLE_DIR = SCRIPT_DIR / "sample"


class FederalRegisterAPI:
    """Client for the Federal Register REST API."""

    def __init__(self):
        self.base_url = BASE_URL
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        return session

    def _backoff(self, response: requests.Response, attempt: int, label: str) -> None:
        """Sleep before retrying a throttled or failed request."""
        wait = min(2 ** (attempt + 1), MAX_BACKOFF)
        retry_after = response.headers.get("Retry-After") if response is not None else None
        if retry_after:
            try:
                wait = min(float(retry_after), MAX_BACKOFF)
            except ValueError:
                pass
        print(f"  {label}, waiting {wait:.0f}s...")
        time.sleep(wait)

    def _request(self, endpoint: str, params: Optional[Dict] = None,
                 retries: int = RETRY_ATTEMPTS) -> Dict:
        """Make a request to the API.

        Retries transient upstream failures (429 plus the 5xx gateway family).
        A 503 on the listing endpoint used to raise straight out of here and
        abort a whole multi-hour crawl over one blip — see issue #1476.
        """
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=60)
                if response.status_code == 429:
                    self._backoff(response, attempt, "Rate limited")
                    continue
                if response.status_code in RETRY_STATUS and attempt < retries - 1:
                    self._backoff(response, attempt,
                                  f"HTTP {response.status_code} from {endpoint}")
                    continue
                response.raise_for_status()
                return response.json()
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError) as e:
                if attempt < retries - 1:
                    print(f"  {type(e).__name__}, retrying...")
                    time.sleep(min(2 ** (attempt + 1), MAX_BACKOFF))
                    continue
                raise
        return {}

    def _fetch_url(self, url: str, retries: int = RETRY_ATTEMPTS) -> str:
        """Fetch content from a URL."""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=60)
                if response.status_code == 429:
                    self._backoff(response, attempt, "Rate limited on text fetch")
                    continue
                if response.status_code == 404:
                    return ""
                if response.status_code in RETRY_STATUS and attempt < retries - 1:
                    self._backoff(response, attempt,
                                  f"HTTP {response.status_code} on text fetch")
                    continue
                response.raise_for_status()
                return response.text
            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    print(f"  Timeout on text fetch, retrying...")
                    time.sleep(2)
                    continue
                raise
            except requests.exceptions.RequestException:
                return ""
        return ""

    def get_documents(self, page: int = 1, per_page: int = 20,
                      conditions: Optional[Dict] = None) -> Dict:
        """
        Fetch documents from the API.

        Args:
            page: Page number (1-indexed)
            per_page: Results per page (max 1000)
            conditions: Filter conditions (e.g., publication_date)
        """
        params = {
            "page": page,
            "per_page": min(per_page, 1000),
        }
        if conditions:
            for key, value in conditions.items():
                # Each operator is its own bracket group: a key like
                # "publication_date[gte]" must go out as
                # conditions[publication_date][gte]. Wrapping it whole gives
                # conditions[publication_date[gte]], which the API rejects with
                # 400 "publication_date[gte is not a valid field".
                field, _, operator = key.partition("[")
                suffix = f"[{operator}" if operator else ""
                params[f"conditions[{field}]{suffix}"] = value

        return self._request("/documents.json", params)

    def get_document(self, document_number: str) -> Dict:
        """Get detailed document data."""
        return self._request(f"/documents/{document_number}.json")

    def get_full_text(self, document: Dict) -> str:
        """
        Get full text for a document.

        Tries raw_text_url first, falls back to body_html_url.
        """
        # Try raw text URL first (preferred)
        raw_text_url = document.get("raw_text_url")
        if raw_text_url:
            text = self._fetch_url(raw_text_url)
            if text:
                return clean_text(text)

        # Fall back to HTML body
        body_html_url = document.get("body_html_url")
        if body_html_url:
            html_content = self._fetch_url(body_html_url)
            if html_content:
                return clean_html(html_content)

        return ""


def load_checkpoint() -> set:
    """Return the set of 'YYYY-MM' windows already fully fetched."""
    try:
        with open(CHECKPOINT_FILE, encoding="utf-8") as f:
            return set(json.load(f).get("completed_months", []))
    except (FileNotFoundError, ValueError, AttributeError):
        return set()


def save_checkpoint(done_months: set) -> None:
    """Persist completed month windows so a restart can skip them."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"completed_months": sorted(done_months)}, f)
    tmp.replace(CHECKPOINT_FILE)


def clean_html(text: str) -> str:
    """Clean HTML tags and entities from text."""
    if not text:
        return ""

    # Decode HTML entities
    text = html.unescape(text)

    # Convert common HTML to newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'<div[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)

    # Remove remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Normalize whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)

    return text.strip()


def clean_text(text: str) -> str:
    """Clean raw text from Federal Register format."""
    if not text:
        return ""

    # Decode HTML entities (sometimes present in text format)
    text = html.unescape(text)

    # Remove HTML wrapper if present
    text = re.sub(r'<html>.*?<body><pre>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'</pre></body></html>', '', text, flags=re.IGNORECASE)

    # Remove HTML tags that might be in the text
    text = re.sub(r'<a[^>]*href=[^>]*>([^<]*)</a>', r'\1', text)
    text = re.sub(r'<span[^>]*>([^<]*)</span>', r'\1', text)
    text = re.sub(r'<[^>]+>', '', text)

    # Clean up Federal Register formatting
    # Remove page markers
    text = re.sub(r'\[\[Page \d+\]\]', '', text)

    # Normalize whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r' *\n *', '\n', text)

    return text.strip()


def normalize(document: Dict, full_text: str) -> Dict:
    """Transform raw Federal Register data into normalized schema."""
    doc_number = document.get("document_number", "")

    # Get agencies
    agencies = document.get("agencies", [])
    agency_names = [a.get("name", "") for a in agencies if a.get("name")]

    # Get document type
    doc_type = document.get("type", "")
    # Map to our types
    type_map = {
        "Rule": "legislation",
        "Proposed Rule": "legislation",
        "Notice": "legislation",
        "Presidential Document": "legislation",
    }
    normalized_type = type_map.get(doc_type, "legislation")

    # Get dates
    pub_date = document.get("publication_date")
    effective_date = document.get("effective_on")

    return {
        "_id": f"fr-{doc_number}",
        "_source": SOURCE_ID,
        "_type": normalized_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": document.get("title", ""),
        "text": full_text,
        "date": pub_date,
        "url": document.get("html_url", ""),
        "document_number": doc_number,
        "type": doc_type,
        "subtype": document.get("subtype"),
        "abstract": document.get("abstract"),
        "agencies": agency_names,
        "cfr_references": document.get("cfr_references", []),
        "docket_ids": document.get("docket_ids", []),
        "citation": document.get("citation"),
        "start_page": document.get("start_page"),
        "end_page": document.get("end_page"),
        "page_length": document.get("page_length"),
        "pdf_url": document.get("pdf_url"),
        "publication_date": pub_date,
        "effective_date": effective_date,
        "volume": document.get("volume"),
    }


def fetch_sample(api: FederalRegisterAPI, count: int = 15) -> List[Dict]:
    """Fetch a sample of recent documents with full text."""
    print(f"Fetching {count} sample documents from Federal Register...")
    records = []

    # Get recent documents
    result = api.get_documents(page=1, per_page=50)
    documents = result.get("results", [])

    for doc in documents:
        if len(records) >= count:
            break

        doc_number = doc.get("document_number")
        if not doc_number:
            continue

        try:
            # Get detailed document data
            full_doc = api.get_document(doc_number)
            time.sleep(REQUEST_DELAY)

            # Get full text
            full_text = api.get_full_text(full_doc)
            time.sleep(REQUEST_DELAY)

            if len(full_text) < 500:
                print(f"  Skipping {doc_number}: only {len(full_text)} chars of text")
                continue

            record = normalize(full_doc, full_text)
            records.append(record)

            title_preview = record.get("title", "")[:50]
            print(f"  [{len(records)}/{count}] {record['_id']}: {len(full_text):,} chars - {title_preview}")

        except Exception as e:
            print(f"  Error fetching {doc_number}: {e}", file=sys.stderr)
            continue

    return records


def fetch_recent(api: FederalRegisterAPI, days: int = 30) -> Generator[Dict, None, None]:
    """Fetch documents from the last N days."""
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    print(f"Fetching documents from {start_date} to {end_date}...")
    page = 1
    count = 0

    while True:
        result = api.get_documents(
            page=page,
            per_page=100,
            conditions={
                "publication_date[gte]": start_date,
                "publication_date[lte]": end_date,
            }
        )

        documents = result.get("results", [])
        if not documents:
            break

        for doc in documents:
            doc_number = doc.get("document_number")
            if not doc_number:
                continue

            try:
                # Get detailed document data
                full_doc = api.get_document(doc_number)
                time.sleep(REQUEST_DELAY)

                # Get full text
                full_text = api.get_full_text(full_doc)
                time.sleep(REQUEST_DELAY)

                if len(full_text) < 100:
                    continue

                record = normalize(full_doc, full_text)
                count += 1
                yield record

                if count % 100 == 0:
                    print(f"  Fetched {count} documents...")

            except Exception as e:
                print(f"  Error fetching {doc_number}: {e}", file=sys.stderr)
                continue

        # Check for next page
        total_pages = result.get("total_pages", 1)
        if page >= total_pages:
            break

        page += 1
        time.sleep(REQUEST_DELAY)


def fetch_updates(api: FederalRegisterAPI, since: datetime) -> Generator[Dict, None, None]:
    """Fetch documents created/modified since a given date."""
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    days = max(1, (datetime.now(timezone.utc) - since).days + 1)
    for record in fetch_recent(api, days=days):
        yield record


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
    parser = argparse.ArgumentParser(description="US/FederalRegister data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--recent", action="store_true", help="Last 30 days only")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")
    bootstrap_parser.add_argument("--days", type=int, default=30, help="Days to fetch for --recent")

    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")
    updates_parser.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("validate", help="Validate sample records")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        valid = validate_samples(SAMPLE_DIR)
        sys.exit(0 if valid else 1)

    api = FederalRegisterAPI()

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample records from Federal Register...")
            try:
                records = fetch_sample(api, args.count)
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

            except requests.HTTPError as e:
                print(f"API error: {e}", file=sys.stderr)
                sys.exit(1)

        elif args.recent:
            print(f"Starting fetch (last {args.days} days)...")
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            count = 0
            with open(DATA_DIR / "records.jsonl", "w", encoding="utf-8") as f:
                for record in fetch_recent(api, args.days):
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                    count += 1
            print(f"Fetched {count} records")

        else:
            # Full bootstrap: fetch all documents by year, month by month
            print("Starting full bootstrap (all years, month by month)...")
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            count = 0
            current_year = datetime.now().year
            current_month = datetime.now().month

            # Resume from the last completed month. Without this a crawl that
            # dies late (503, timeout, teardown) restarts at the current month
            # and re-walks everything it already had (#1476).
            done_months = load_checkpoint()
            mode = "a" if done_months else "w"
            if done_months:
                print(f"Resuming: {len(done_months)} months already complete")

            with open(DATA_DIR / "records.jsonl", mode, encoding="utf-8") as f:
                for year in range(current_year, 1993, -1):
                    for month in range(12, 0, -1):
                        if year == current_year and month > current_month:
                            continue
                        month_key = f"{year}-{month:02d}"
                        if month_key in done_months:
                            continue
                        start = f"{year}-{month:02d}-01"
                        if month == 12:
                            end = f"{year}-{month:02d}-31"
                        else:
                            # Last day of month
                            import calendar
                            last_day = calendar.monthrange(year, month)[1]
                            end = f"{year}-{month:02d}-{last_day:02d}"

                        print(f"  Fetching {start} to {end}...")
                        page = 1
                        month_count = 0
                        while True:
                            result = api.get_documents(
                                page=page,
                                per_page=100,
                                conditions={
                                    "publication_date[gte]": start,
                                    "publication_date[lte]": end,
                                }
                            )
                            documents = result.get("results", [])
                            if not documents:
                                break

                            for doc in documents:
                                doc_number = doc.get("document_number")
                                if not doc_number:
                                    continue
                                try:
                                    full_doc = api.get_document(doc_number)
                                    time.sleep(REQUEST_DELAY)
                                    full_text = api.get_full_text(full_doc)
                                    time.sleep(REQUEST_DELAY)
                                    if len(full_text) < 100:
                                        continue
                                    record = normalize(full_doc, full_text)
                                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                                    count += 1
                                    month_count += 1
                                except Exception as e:
                                    print(f"    Error {doc_number}: {e}", file=sys.stderr)

                            total_pages = result.get("total_pages", 1)
                            if page >= total_pages:
                                break
                            page += 1
                            time.sleep(REQUEST_DELAY)

                        # Only mark the month done once every page of it landed.
                        f.flush()
                        done_months.add(month_key)
                        save_checkpoint(done_months)

                        if month_count > 0:
                            print(f"    {month_count} documents ({count} total)")

            print(f"Full bootstrap complete: {count} records")

    elif args.command == "updates":
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        print(f"Fetching updates since {since.date()}...")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        with open(DATA_DIR / "records.jsonl", "w", encoding="utf-8") as f:
            for record in fetch_updates(api, since):
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        print(f"Fetched {count} updated records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
