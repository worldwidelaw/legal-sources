#!/usr/bin/env python3
"""
FR/Judilibre -- French Judicial Case Law via PISTE API

Fetches case law from the Judilibre platform, which provides access to decisions from:
- Cour de cassation (cc) - Supreme Court for private/criminal law
- Cours d'appel (ca) - Appellate courts
- Tribunaux judiciaires (tj) - First-instance courts
- Tribunaux de commerce (tcom) - Commercial courts

Requires PISTE API key. See .env.template for setup instructions.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch (all jurisdictions)
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests
import yaml

# Load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# Configuration
SOURCE_ID = "FR/Judilibre"
SANDBOX_URL = "https://sandbox-api.piste.gouv.fr/cassation/judilibre/v1.0"
PRODUCTION_URL = "https://api.piste.gouv.fr/cassation/judilibre/v1.0"

USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"
REQUEST_DELAY = 0.5  # seconds between requests
JURISDICTIONS = ["cc", "ca", "tj", "tcom"]  # All jurisdictions: Cour de cassation, Cours d'appel, Tribunaux judiciaires, Tribunaux de commerce
WINDOW_DAYS = 7  # Date window size for /scan queries — weekly to avoid PISTE gateway caps (~10K/query)
TRUNCATION_THRESHOLD = 9500  # If a window returns >= this many results, it's likely truncated

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
SAMPLE_DIR = SCRIPT_DIR / "sample"
STATUS_FILE = SCRIPT_DIR / "status.yaml"
CHECKPOINT_FILE = SCRIPT_DIR / "checkpoint.json"

# Default date range
START_YEAR = 1860  # Judilibre now has historical CC decisions back to 1860
END_YEAR = datetime.now().year


class JudilibreAPI:
    """Client for the Judilibre PISTE API."""

    def __init__(self, api_key: str, environment: str = "production"):
        self.api_key = api_key
        self.environment = environment
        self.base_url = PRODUCTION_URL if environment == "production" else SANDBOX_URL
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with API key auth."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "KeyId": self.api_key,
        })
        return session

    def _request(self, endpoint: str, params: Optional[Dict] = None,
                 retries: int = 3) -> Dict:
        """Make an authenticated request to the API."""
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            response = self.session.get(url, params=params, timeout=60)
            if response.status_code == 429 and attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()

    def _request_raw_url(self, full_url: str, retries: int = 3) -> Dict:
        """Make an authenticated request to a full URL (for pagination)."""
        for attempt in range(retries):
            response = self.session.get(full_url, timeout=60)
            if response.status_code == 429 and attempt < retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()

    def search(self, query: str = "*", page: int = 0, page_size: int = 10,
               sort: str = "score", order: str = "desc",
               date_start: Optional[str] = None, date_end: Optional[str] = None,
               jurisdiction: Optional[str] = None) -> Dict:
        """Search for decisions."""
        params = {
            "query": query,
            "page": page,
            "page_size": min(page_size, 50),
            "sort": sort,
            "order": order,
        }
        if date_start:
            params["date_start"] = date_start
        if date_end:
            params["date_end"] = date_end
        if jurisdiction:
            params["jurisdiction"] = jurisdiction
        return self._request("/search", params)

    def get_decision(self, decision_id: str) -> Dict:
        """Get a specific decision by ID."""
        return self._request("/decision", params={"id": decision_id})

    def get_scan(self, date_start: str, date_end: str,
                 jurisdiction: Optional[List[str]] = None,
                 batch_size: int = 1000) -> Generator[Dict, None, None]:
        """Scan decisions in a date range using cursor-based pagination.

        The /scan endpoint returns a next_batch query string for pagination.
        We construct the full URL by appending it to the base scan URL.
        """
        params: Dict[str, Any] = {
            "date_start": date_start,
            "date_end": date_end,
            "batch_size": min(batch_size, 1000),
            "order": "asc",
            "date_type": "creation",
            "resolve_references": "true",
        }
        if jurisdiction:
            params["jurisdiction"] = jurisdiction

        result = self._request("/scan", params)
        scan_url = f"{self.base_url}/scan"

        while True:
            decisions = result.get("results", [])
            if not decisions:
                break

            for decision in decisions:
                yield decision

            next_batch = result.get("next_batch")
            if not next_batch:
                break

            time.sleep(REQUEST_DELAY)
            # next_batch is a query string, not a full URL
            result = self._request_raw_url(f"{scan_url}?{next_batch}")


def clean_text(text: str) -> str:
    """Clean HTML and normalize whitespace in decision text."""
    if not text:
        return ""
    import html
    text = html.unescape(text)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = text.strip()
    return text


def normalize(raw: Dict) -> Dict:
    """Transform raw Judilibre data into normalized schema."""
    doc_id = raw.get("id", "")
    ecli = raw.get("ecli", "")

    text = raw.get("text", "")
    if not text:
        files = raw.get("files", [])
        for f in files:
            if f.get("type") == "text":
                text = f.get("content", "")
                break
    text = clean_text(text)

    title_parts = []
    if raw.get("jurisdiction"):
        title_parts.append(raw["jurisdiction"])
    if raw.get("chamber"):
        title_parts.append(raw["chamber"])
    if raw.get("decision_date"):
        title_parts.append(raw["decision_date"])
    if raw.get("number"):
        title_parts.append(f"n\u00b0 {raw['number']}")
    title = " - ".join(title_parts) if title_parts else f"Decision {doc_id}"

    url = f"https://www.courdecassation.fr/decision/{doc_id}" if doc_id else ""

    return {
        "_id": doc_id or ecli,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": raw.get("decision_date"),
        "url": url,
        "ecli": ecli,
        "jurisdiction": raw.get("jurisdiction"),
        "chamber": raw.get("chamber"),
        "number": raw.get("number"),
        "solution": raw.get("solution"),
        "publication": raw.get("publication"),
        "themes": raw.get("themes", []),
        "visa": raw.get("visa", []),
        "rapprochements": raw.get("rapprochements", []),
    }


def fetch_sample(api: JudilibreAPI, count: int = 15) -> List[Dict]:
    """Fetch a sample of records with full text."""
    print(f"Searching for recent decisions...")
    records = []
    page = 0

    while len(records) < count:
        result = api.search(query="*", page=page, page_size=50, sort="date", order="desc")
        decisions = result.get("results", [])
        if not decisions:
            break

        for dec in decisions:
            if len(records) >= count:
                break
            decision_id = dec.get("id")
            if not decision_id:
                continue
            try:
                full = api.get_decision(decision_id)
                record = normalize(full)
                if len(record.get("text", "")) > 500:
                    records.append(record)
                    print(f"  [{len(records)}/{count}] {record['_id']}: {len(record['text'])} chars")
            except Exception as e:
                print(f"  Error fetching {decision_id}: {e}", file=sys.stderr)
                continue
            time.sleep(REQUEST_DELAY)

        page += 1
        if page > 10:
            break
    return records


def load_checkpoint() -> dict:
    """Load checkpoint from file if it exists."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, "r") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print("Warning: Invalid checkpoint file, starting fresh")
    return {"current_year": None, "current_month": None, "fetched_count": 0}


def save_checkpoint(checkpoint: dict):
    """Save checkpoint to file."""
    checkpoint["last_update"] = datetime.now(timezone.utc).isoformat()
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2)


def clear_checkpoint():
    """Clear checkpoint file."""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print("Checkpoint cleared")


def fetch_all(api: JudilibreAPI, days: int = 30) -> Generator[Dict, None, None]:
    """Fetch all decisions from the last N days, using weekly windows per jurisdiction."""
    now = datetime.now()
    start = now - timedelta(days=days)
    print(f"Scanning decisions from {start.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')} "
          f"(all jurisdictions, {WINDOW_DAYS}-day windows)...")

    for jurisdiction in JURISDICTIONS:
        window_start = start
        while window_start <= now:
            window_end = min(window_start + timedelta(days=WINDOW_DAYS - 1), now)
            ds = window_start.strftime("%Y-%m-%d")
            de = window_end.strftime("%Y-%m-%d")
            for record in _scan_window(api, ds, de, jurisdiction):
                yield record
            window_start = window_end + timedelta(days=1)


def _scan_window(api: JudilibreAPI, date_start: str, date_end: str,
                  jurisdiction: str) -> Generator[Dict, None, None]:
    """Scan a single date window. If the result count hits the truncation
    threshold, automatically subdivide into daily windows and re-scan."""
    count = 0
    for decision in api.get_scan(date_start, date_end,
                                 jurisdiction=[jurisdiction]):
        record = normalize(decision)
        if record.get("text"):
            count += 1
            yield record

    if count >= TRUNCATION_THRESHOLD:
        # Likely truncated by PISTE gateway — re-scan with daily windows
        print(f"    ⚠ {count} results in {date_start}→{date_end} (≥{TRUNCATION_THRESHOLD}), "
              f"re-scanning with daily windows...")
        seen_ids: set = set()
        day = datetime.strptime(date_start, "%Y-%m-%d")
        end = datetime.strptime(date_end, "%Y-%m-%d")
        while day <= end:
            ds = day.strftime("%Y-%m-%d")
            de = ds  # single day
            try:
                for decision in api.get_scan(ds, de, jurisdiction=[jurisdiction]):
                    record = normalize(decision)
                    doc_id = record.get("_id")
                    if record.get("text") and doc_id and doc_id not in seen_ids:
                        seen_ids.add(doc_id)
                        yield record
            except Exception as e:
                print(f"      Error on {ds}: {e}")
            day += timedelta(days=1)
        print(f"    ⚠ Daily re-scan recovered {len(seen_ids)} unique records "
              f"(vs {count} from wide window)")


def fetch_full_archive(api: JudilibreAPI, use_checkpoint: bool = True) -> Generator[Dict, None, None]:
    """
    Fetch all decisions from 2010 to present, per jurisdiction, using weekly
    date windows to avoid PISTE gateway result caps (~10K per query).
    Supports checkpoint/resume for this long-running operation.
    """
    if use_checkpoint:
        checkpoint = load_checkpoint()
        start_jurisdiction = checkpoint.get("current_jurisdiction") or JURISDICTIONS[0]
        resume_date = checkpoint.get("current_date")  # ISO date string
        fetched_count = checkpoint.get("fetched_count", 0)
        completed_jurisdictions = checkpoint.get("completed_jurisdictions", [])
        jurisdiction_counts = checkpoint.get("jurisdiction_counts", {})
        # Backwards-compat: old checkpoints used year/month
        if not resume_date and checkpoint.get("current_year"):
            y = checkpoint["current_year"]
            m = checkpoint.get("current_month", 1)
            resume_date = f"{y}-{m:02d}-01"
        if checkpoint.get("current_jurisdiction"):
            print(f"Resuming from checkpoint: {start_jurisdiction} {resume_date}, "
                  f"{fetched_count} fetched")
    else:
        start_jurisdiction = JURISDICTIONS[0]
        resume_date = None
        fetched_count = 0
        completed_jurisdictions = []
        jurisdiction_counts = {}

    now = datetime.now()
    archive_start = datetime(START_YEAR, 1, 1)

    for jurisdiction in JURISDICTIONS:
        if jurisdiction in completed_jurisdictions:
            continue

        if jurisdiction == start_jurisdiction and resume_date:
            window_start = datetime.strptime(resume_date, "%Y-%m-%d")
        else:
            window_start = archive_start

        jurisdiction_count = jurisdiction_counts.get(jurisdiction, 0)
        print(f"\n=== Jurisdiction: {jurisdiction} ===")

        while window_start <= now:
            window_end = min(window_start + timedelta(days=WINDOW_DAYS - 1), now)
            ds = window_start.strftime("%Y-%m-%d")
            de = window_end.strftime("%Y-%m-%d")

            window_count = 0
            try:
                for record in _scan_window(api, ds, de, jurisdiction):
                    fetched_count += 1
                    window_count += 1
                    jurisdiction_count += 1
                    yield record
            except Exception as e:
                print(f"    Error in {jurisdiction} {ds}→{de}: {e}")

            if window_count > 0:
                print(f"  [{jurisdiction}] {ds}→{de}: {window_count} records")

            window_start = window_end + timedelta(days=1)

            if use_checkpoint:
                jurisdiction_counts[jurisdiction] = jurisdiction_count
                save_checkpoint({
                    "current_jurisdiction": jurisdiction,
                    "current_date": window_start.strftime("%Y-%m-%d"),
                    "fetched_count": fetched_count,
                    "completed_jurisdictions": completed_jurisdictions,
                    "jurisdiction_counts": jurisdiction_counts,
                })

        completed_jurisdictions.append(jurisdiction)
        print(f"  === {jurisdiction} complete: {jurisdiction_count} records ===")

    if use_checkpoint:
        clear_checkpoint()
        print(f"Archive fetch complete - {fetched_count} total records - checkpoint cleared")
        for j, c in jurisdiction_counts.items():
            print(f"  {j}: {c} records")


def fetch_updates(api: JudilibreAPI, since: datetime) -> Generator[Dict, None, None]:
    """Fetch updates since a given date, using weekly windows per jurisdiction."""
    now = datetime.now()
    print(f"Scanning updates from {since.strftime('%Y-%m-%d')} to {now.strftime('%Y-%m-%d')} "
          f"(all jurisdictions, {WINDOW_DAYS}-day windows)...")

    for jurisdiction in JURISDICTIONS:
        window_start = since.replace(tzinfo=None) if since.tzinfo else since
        while window_start <= now:
            window_end = min(window_start + timedelta(days=WINDOW_DAYS - 1), now)
            ds = window_start.strftime("%Y-%m-%d")
            de = window_end.strftime("%Y-%m-%d")
            for record in _scan_window(api, ds, de, jurisdiction):
                yield record
            window_start = window_end + timedelta(days=1)


def save_samples(records: List[Dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def update_status(records_fetched: int, errors: int, sample_count: int = 0) -> None:
    """Update the status.yaml file."""
    now = datetime.now(timezone.utc).isoformat()
    status = {
        "last_run": now,
        "last_bootstrap": now if sample_count > 0 else None,
        "last_error": None,
        "total_records": 0,
        "run_history": [{
            "started_at": now,
            "finished_at": now,
            "records_fetched": records_fetched,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "sample_records_saved": sample_count,
            "errors": errors,
        }]
    }
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                existing = yaml.safe_load(f) or {}
            if "run_history" in existing:
                status["run_history"] = existing["run_history"][-9:] + status["run_history"]
        except Exception:
            pass
    with open(STATUS_FILE, 'w') as f:
        yaml.dump(status, f, default_flow_style=False)


def main():
    parser = argparse.ArgumentParser(description="FR/Judilibre case law fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full archive (1860-present)")
    bootstrap_parser.add_argument("--recent", action="store_true", help="Last 30 days only")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")
    bootstrap_parser.add_argument("--days", type=int, default=30, help="Days to fetch for --recent")
    bootstrap_parser.add_argument("--no-checkpoint", action="store_true", help="Disable checkpoint")
    bootstrap_parser.add_argument("--clear-checkpoint", action="store_true", help="Clear checkpoint first")

    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")

    subparsers.add_parser("status", help="Show checkpoint status")
    subparsers.add_parser("clear-checkpoint", help="Clear checkpoint file")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "status":
        checkpoint = load_checkpoint()
        print("Checkpoint status:")
        print(f"  Current jurisdiction: {checkpoint.get('current_jurisdiction', 'N/A')}")
        print(f"  Current date: {checkpoint.get('current_date', checkpoint.get('current_year', 'N/A'))}")
        print(f"  Fetched count: {checkpoint.get('fetched_count', 0)}")
        print(f"  Completed jurisdictions: {checkpoint.get('completed_jurisdictions', [])}")
        counts = checkpoint.get('jurisdiction_counts', {})
        for j, c in counts.items():
            print(f"    {j}: {c}")
        print(f"  Last update: {checkpoint.get('last_update', 'N/A')}")
        return

    if args.command == "clear-checkpoint":
        clear_checkpoint()
        return

    api_key = os.environ.get("JUDILIBRE_API_KEY")
    environment = os.environ.get("JUDILIBRE_ENVIRONMENT", "production")

    if not api_key:
        print("Error: Missing API key. Set JUDILIBRE_API_KEY environment variable.",
              file=sys.stderr)
        print("\nSee .env.template for instructions on obtaining an API key.",
              file=sys.stderr)
        sys.exit(1)

    api = JudilibreAPI(api_key, environment)

    if args.command == "bootstrap":
        if args.clear_checkpoint:
            clear_checkpoint()

        if args.sample:
            print(f"Fetching {args.count} sample records from Judilibre ({environment})...")
            try:
                records = fetch_sample(api, args.count)
                if records:
                    save_samples(records)
                    update_status(len(records), 0, len(records))
                    text_lengths = [len(r.get('text', '')) for r in records]
                    avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0
                    print(f"\nSummary:")
                    print(f"  Records: {len(records)}")
                    print(f"  Avg text length: {avg_len:.0f} chars")
                    print(f"  Min text length: {min(text_lengths)} chars")
                    print(f"  Max text length: {max(text_lengths)} chars")
                else:
                    print("No records fetched!", file=sys.stderr)
                    update_status(0, 1)
                    sys.exit(1)
            except requests.HTTPError as e:
                print(f"API error: {e}", file=sys.stderr)
                if e.response is not None:
                    print(f"Response: {e.response.text}", file=sys.stderr)
                update_status(0, 1)
                sys.exit(1)

        elif args.full:
            print(f"Starting full archive fetch ({START_YEAR}-present, all jurisdictions)...")
            use_checkpoint = not args.no_checkpoint
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            records_file = DATA_DIR / "records.jsonl"

            # Load existing IDs to avoid duplicates on resume
            existing_ids = set()
            if records_file.exists():
                with open(records_file, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            rec = json.loads(line.strip())
                            if rec.get("_id"):
                                existing_ids.add(rec["_id"])
                        except json.JSONDecodeError:
                            continue
                print(f"  Found {len(existing_ids)} existing records in JSONL")

            count = 0
            new_records = 0
            with open(records_file, "a", encoding="utf-8") as f:
                for record in fetch_full_archive(api, use_checkpoint=use_checkpoint):
                    count += 1
                    if record.get("_id") in existing_ids:
                        continue
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                    existing_ids.add(record["_id"])
                    new_records += 1
                    if new_records % 5000 == 0:
                        print(f"  Written {new_records} new records to JSONL...")
                        f.flush()
            print(f"Fetched {count} records, wrote {new_records} new to JSONL")
            update_status(count, 0)

        elif args.recent:
            print(f"Starting fetch (last {args.days} days)...")
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            count = 0
            with open(DATA_DIR / "records.jsonl", "w", encoding="utf-8") as f:
                for record in fetch_all(api, args.days):
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                    count += 1
                    if count % 100 == 0:
                        print(f"  {count} records...")
            print(f"Fetched {count} records")
            update_status(count, 0)

        else:
            print(f"Starting fetch (last 30 days, use --full for complete archive)...")
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            count = 0
            with open(DATA_DIR / "records.jsonl", "w", encoding="utf-8") as f:
                for record in fetch_all(api, 30):
                    f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                    count += 1
                    if count % 100 == 0:
                        print(f"  {count} records...")
            print(f"Fetched {count} records")
            update_status(count, 0)

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
        update_status(count, 0)


if __name__ == "__main__":
    main()
