#!/usr/bin/env python3
"""
US/CourtListener -- Federal and State Case Law via CourtListener API

Fetches case law from CourtListener (Free Law Project), which provides access to:
- Federal court opinions (Supreme Court, Circuit Courts, District Courts)
- State court opinions (Supreme Courts, Appellate Courts)
- PACER docket data
- Oral argument recordings

Requires API token. See .env.template for setup instructions.

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

# Load environment variables from .env file if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

# Configuration
SOURCE_ID = "US/CourtListener"
BASE_URL = "https://www.courtlistener.com/api/rest/v4"
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research; contact@legaldatahunter.com)"
REQUEST_DELAY = 0.75  # seconds between requests

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
SAMPLE_DIR = SCRIPT_DIR / "sample"


class CourtListenerAPI:
    """Client for the CourtListener REST API v4."""

    def __init__(self, api_token: str):
        self.api_token = api_token
        self.base_url = BASE_URL
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """Create a requests session with API token auth."""
        session = requests.Session()
        session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Authorization": f"Token {self.api_token}",
        })
        return session

    def _request(self, endpoint: str, params: Optional[Dict] = None,
                 retries: int = 3) -> Dict:
        """Make an authenticated request to the API."""
        url = f"{self.base_url}{endpoint}"
        for attempt in range(retries):
            try:
                response = self.session.get(url, params=params, timeout=60)
                if response.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                if response.status_code == 401:
                    print(f"  Authentication failed. Check your API token.", file=sys.stderr)
                    raise requests.HTTPError("401 Unauthorized", response=response)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    print(f"  Timeout, retrying...")
                    time.sleep(2)
                    continue
                raise
        return {}

    def _request_url(self, full_url: str, retries: int = 3) -> Dict:
        """Make an authenticated request to a full URL (for pagination)."""
        for attempt in range(retries):
            try:
                response = self.session.get(full_url, timeout=60)
                if response.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                response.raise_for_status()
                return response.json()
            except requests.exceptions.Timeout:
                if attempt < retries - 1:
                    print(f"  Timeout, retrying...")
                    time.sleep(2)
                    continue
                raise
        return {}

    def get_opinions(self, page_size: int = 20, ordering: str = "-date_created",
                     date_created__gte: Optional[str] = None,
                     date_created__lte: Optional[str] = None,
                     court: Optional[str] = None) -> Generator[Dict, None, None]:
        """
        Fetch opinions with pagination.

        Args:
            page_size: Number of results per page (max 100)
            ordering: Field to order by (prefix with - for descending)
            date_created__gte: Filter by date_created >= this date (YYYY-MM-DD)
            date_created__lte: Filter by date_created <= this date (YYYY-MM-DD)
            court: Filter by court (e.g., 'scotus', 'ca9')
        """
        params = {
            "page_size": min(page_size, 100),
            "order_by": ordering,
        }
        if date_created__gte:
            params["date_created__gte"] = date_created__gte
        if date_created__lte:
            params["date_created__lte"] = date_created__lte
        if court:
            params["cluster__docket__court"] = court

        result = self._request("/opinions/", params)

        while True:
            opinions = result.get("results", [])
            if not opinions:
                break

            for opinion in opinions:
                yield opinion

            next_url = result.get("next")
            if not next_url:
                break

            time.sleep(REQUEST_DELAY)
            result = self._request_url(next_url)

    def get_opinion_detail(self, opinion_id: int) -> Dict:
        """Get detailed opinion data including full text."""
        return self._request(f"/opinions/{opinion_id}/")

    def get_cluster_detail(self, cluster_id: int) -> Dict:
        """Get cluster (case) data including metadata."""
        return self._request(f"/clusters/{cluster_id}/")

    def get_courts(self) -> List[Dict]:
        """Get list of all courts."""
        result = self._request("/courts/", params={"page_size": 100})
        courts = result.get("results", [])

        # Paginate through all courts
        while result.get("next"):
            time.sleep(REQUEST_DELAY)
            result = self._request_url(result["next"])
            courts.extend(result.get("results", []))

        return courts


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


def extract_text(opinion: Dict) -> str:
    """
    Extract full text from opinion, preferring structured sources.

    Priority:
    1. html_with_citations (most complete, with linked citations)
    2. html_columbia (Columbia Law School formatted)
    3. html_lawbox (Lawbox formatted)
    4. xml_harvard (Harvard CAP XML)
    5. plain_text (plain text fallback)
    """
    # List of text fields in order of preference
    text_fields = [
        "html_with_citations",
        "html_columbia",
        "html_lawbox",
        "html",
        "plain_text",
        "xml_harvard",
    ]

    for field in text_fields:
        text = opinion.get(field)
        if text and len(text.strip()) > 100:
            # Clean HTML from the text
            if "html" in field.lower() or "xml" in field.lower():
                return clean_html(text)
            return text.strip()

    return ""


def normalize(opinion: Dict, cluster: Optional[Dict] = None) -> Dict:
    """Transform raw CourtListener data into normalized schema."""
    opinion_id = opinion.get("id")

    # Extract cluster info from opinion or separate cluster call
    cluster_url = opinion.get("cluster")
    cluster_id = None
    if cluster_url:
        # Extract cluster ID from URL like "/api/rest/v4/clusters/123/"
        match = re.search(r'/clusters/(\d+)/', cluster_url)
        if match:
            cluster_id = int(match.group(1))

    # Get text content
    text = extract_text(opinion)

    # Build title from available info
    case_name = ""
    if cluster:
        case_name = cluster.get("case_name", "") or cluster.get("case_name_short", "")

    # Get court info from opinion
    court = opinion.get("cluster__docket__court", "")

    # Get date from cluster or opinion
    date_filed = None
    if cluster:
        date_filed = cluster.get("date_filed")
    if not date_filed:
        date_filed = opinion.get("date_created", "")[:10] if opinion.get("date_created") else None

    # Build title
    if case_name:
        title = case_name
    else:
        title = f"Opinion {opinion_id}"

    # Get URL
    absolute_url = opinion.get("absolute_url", "")
    if absolute_url:
        url = f"https://www.courtlistener.com{absolute_url}"
    else:
        url = f"https://www.courtlistener.com/opinion/{opinion_id}/"

    # Get author
    author = opinion.get("author_str", "") or ""
    if not author and opinion.get("author"):
        # author is a URL to judge, extract name if available
        author = ""

    # Get type of opinion
    opinion_type = opinion.get("type", "")
    type_map = {
        "010combined": "combined",
        "015unamimous": "unanimous",
        "020lead": "lead",
        "025plurality": "plurality",
        "030concurrence": "concurrence",
        "035concurrenceinpart": "concurrence in part",
        "040dissent": "dissent",
        "050addendum": "addendum",
        "060remittitur": "remittitur",
        "070rehearing": "rehearing",
        "080onthemerits": "on the merits",
        "090onmotiontostrike": "on motion to strike",
    }
    opinion_type_str = type_map.get(opinion_type, opinion_type)

    return {
        "_id": f"cl-opinion-{opinion_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_filed,
        "url": url,
        "opinion_id": opinion_id,
        "cluster_id": cluster_id,
        "court": court,
        "author": author,
        "opinion_type": opinion_type_str,
        "citations": opinion.get("citation", []) if isinstance(opinion.get("citation"), list) else [],
        "date_created": opinion.get("date_created"),
        "date_modified": opinion.get("date_modified"),
        "sha1": opinion.get("sha1"),
        "download_url": opinion.get("download_url"),
    }


def fetch_sample(api: CourtListenerAPI, count: int = 15) -> List[Dict]:
    """Fetch a sample of recent opinions with full text."""
    print(f"Fetching {count} sample opinions from CourtListener...")
    records = []

    # Get recent opinions
    for opinion in api.get_opinions(page_size=50, ordering="-date_created"):
        if len(records) >= count:
            break

        opinion_id = opinion.get("id")
        if not opinion_id:
            continue

        try:
            # Get full opinion detail
            full_opinion = api.get_opinion_detail(opinion_id)

            # Get cluster for case name
            cluster = None
            cluster_url = full_opinion.get("cluster")
            if cluster_url:
                match = re.search(r'/clusters/(\d+)/', cluster_url)
                if match:
                    cluster_id = int(match.group(1))
                    try:
                        cluster = api.get_cluster_detail(cluster_id)
                    except Exception as e:
                        print(f"  Warning: Could not fetch cluster {cluster_id}: {e}")

            record = normalize(full_opinion, cluster)

            # Only include if we got substantial text
            text_len = len(record.get("text", ""))
            if text_len >= 500:
                records.append(record)
                title_preview = record.get("title", "")[:50]
                print(f"  [{len(records)}/{count}] {record['_id']}: {text_len:,} chars - {title_preview}")
            else:
                print(f"  Skipping {opinion_id}: only {text_len} chars of text")

        except Exception as e:
            print(f"  Error fetching opinion {opinion_id}: {e}", file=sys.stderr)
            continue

        time.sleep(REQUEST_DELAY)

    return records


def fetch_recent(api: CourtListenerAPI, days: int = 30) -> Generator[Dict, None, None]:
    """Fetch opinions from the last N days."""
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    print(f"Fetching opinions from {start_date} to {end_date}...")
    count = 0

    for opinion in api.get_opinions(
        page_size=100,
        ordering="-date_created",
        date_created__gte=start_date
    ):
        opinion_id = opinion.get("id")
        if not opinion_id:
            continue

        try:
            # Get full opinion detail
            full_opinion = api.get_opinion_detail(opinion_id)

            # Get cluster for case name
            cluster = None
            cluster_url = full_opinion.get("cluster")
            if cluster_url:
                match = re.search(r'/clusters/(\d+)/', cluster_url)
                if match:
                    cluster_id = int(match.group(1))
                    try:
                        cluster = api.get_cluster_detail(cluster_id)
                    except Exception:
                        pass

            record = normalize(full_opinion, cluster)

            if len(record.get("text", "")) >= 100:
                count += 1
                yield record

                if count % 100 == 0:
                    print(f"  Fetched {count} opinions...")

        except Exception as e:
            print(f"  Error fetching opinion {opinion_id}: {e}", file=sys.stderr)
            continue

        time.sleep(REQUEST_DELAY)


def fetch_updates(api: CourtListenerAPI, since: datetime) -> Generator[Dict, None, None]:
    """Fetch opinions created/modified since a given date."""
    since_str = since.strftime("%Y-%m-%d")
    print(f"Fetching opinions updated since {since_str}...")

    for record in fetch_recent(api, days=(datetime.now() - since).days + 1):
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
    parser = argparse.ArgumentParser(description="US/CourtListener case law fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--recent", action="store_true", help="Last 30 days only")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")
    bootstrap_parser.add_argument("--days", type=int, default=30, help="Days to fetch for --recent")

    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")

    subparsers.add_parser("validate", help="Validate sample records")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        valid = validate_samples(SAMPLE_DIR)
        sys.exit(0 if valid else 1)

    # Get API token
    api_token = os.environ.get("COURTLISTENER_API_TOKEN")

    if not api_token:
        print("Error: Missing API token. Set COURTLISTENER_API_TOKEN environment variable.",
              file=sys.stderr)
        print("\nTo get a token:", file=sys.stderr)
        print("1. Create an account at https://www.courtlistener.com/sign-in/", file=sys.stderr)
        print("2. Go to your profile and find your API token", file=sys.stderr)
        print("3. Set: export COURTLISTENER_API_TOKEN=your_token_here", file=sys.stderr)
        sys.exit(1)

    api = CourtListenerAPI(api_token)

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample records from CourtListener...")
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
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Response: {e.response.text}", file=sys.stderr)
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
            print("Use --sample for sample mode or --recent for recent data")
            sys.exit(1)

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
    main()
