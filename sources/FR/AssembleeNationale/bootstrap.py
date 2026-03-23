#!/usr/bin/env python3
"""
FR/AssembleeNationale -- French National Assembly (Assemblée Nationale)

Fetches parliamentary documents (projets de loi, propositions de loi) with full text
from the Open Data portal and document rendering system.

Data sources:
- Bulk document IDs: data.assemblee-nationale.fr/static/openData/repository/17/loi/dossiers_legislatifs/
- Full text HTML: www.assemblee-nationale.fr/dyn/docs/{doc_id}.raw

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Generator, Optional

import requests
import yaml

# Configuration
SOURCE_ID = "FR/AssembleeNationale"
BASE_URL = "https://data.assemblee-nationale.fr"
FULL_TEXT_BASE = "https://www.assemblee-nationale.fr/dyn/docs/"
METADATA_BASE = "https://www.assemblee-nationale.fr/dyn/opendata/"
DOCUMENTS_ZIP_URL = "https://data.assemblee-nationale.fr/static/openData/repository/17/loi/dossiers_legislatifs/Dossiers_Legislatifs.json.zip"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"
REQUEST_DELAY = 1.0  # seconds between requests

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
SAMPLE_DIR = SCRIPT_DIR / "sample"
STATUS_FILE = SCRIPT_DIR / "status.yaml"


class HTMLTextExtractor(HTMLParser):
    """Extract clean text from HTML content."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.in_style = False
        self.in_script = False

    def handle_starttag(self, tag, attrs):
        if tag == 'style':
            self.in_style = True
        elif tag == 'script':
            self.in_script = True
        elif tag in ('p', 'div', 'br', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li'):
            self.text_parts.append('\n')

    def handle_endtag(self, tag):
        if tag == 'style':
            self.in_style = False
        elif tag == 'script':
            self.in_script = False
        elif tag in ('p', 'div'):
            self.text_parts.append('\n')

    def handle_data(self, data):
        if not self.in_style and not self.in_script:
            text = data.strip()
            if text:
                self.text_parts.append(text)

    def get_text(self) -> str:
        text = ' '.join(self.text_parts)
        # Clean up whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n ', '\n', text)
        return text.strip()


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from HTML content."""
    if not html_content:
        return ""
    parser = HTMLTextExtractor()
    try:
        parser.feed(html_content)
        return parser.get_text()
    except Exception:
        # Fallback: simple tag stripping
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/json,*/*",
        "Accept-Language": "fr,en;q=0.5",
    })
    return session


def get_document_ids_from_zip(session: requests.Session, doc_types: list[str] = None) -> Generator[dict, None, None]:
    """
    Download the bulk ZIP and extract document IDs.

    doc_types: list of prefixes to filter, e.g., ['PRJL', 'PION'] for projets/propositions de loi
    """
    if doc_types is None:
        doc_types = ['PRJL', 'PION']  # Projets de loi, Propositions de loi

    print(f"Downloading document catalog from {DOCUMENTS_ZIP_URL}...")

    response = session.get(DOCUMENTS_ZIP_URL, timeout=120, stream=True)
    response.raise_for_status()

    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        for chunk in response.iter_content(chunk_size=8192):
            tmp.write(chunk)
        tmp_path = tmp.name

    try:
        with zipfile.ZipFile(tmp_path, 'r') as zf:
            # List all document files
            for name in zf.namelist():
                if not name.endswith('.json'):
                    continue

                # Check if it's a document file (not a dossier)
                if '/document/' not in name:
                    continue

                basename = os.path.basename(name)
                doc_id = basename.replace('.json', '')

                # Filter by document type prefix
                if not any(doc_id.startswith(prefix) for prefix in doc_types):
                    continue

                # Read and parse the document metadata
                try:
                    with zf.open(name) as f:
                        data = json.load(f)

                    doc_data = data.get('document', {})
                    if not doc_data:
                        continue

                    yield {
                        'uid': doc_data.get('uid', doc_id),
                        'legislature': doc_data.get('legislature'),
                        'title': doc_data.get('titres', {}).get('titrePrincipal', ''),
                        'title_short': doc_data.get('titres', {}).get('titrePrincipalCourt', ''),
                        'denomination': doc_data.get('denominationStructurelle', ''),
                        'date_depot': doc_data.get('cycleDeVie', {}).get('chrono', {}).get('dateDepot'),
                        'date_publication': doc_data.get('cycleDeVie', {}).get('chrono', {}).get('datePublication'),
                        'classification': doc_data.get('classification', {}),
                        'dossier_ref': doc_data.get('dossierRef'),
                    }
                except (json.JSONDecodeError, KeyError):
                    continue
    finally:
        os.unlink(tmp_path)


def fetch_full_text(session: requests.Session, doc_id: str) -> Optional[str]:
    """Fetch the full text HTML content for a document."""
    url = f"{FULL_TEXT_BASE}{doc_id}.raw"

    try:
        response = session.get(url, timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()

        html_content = response.text

        # Extract just the body content (skip the style definitions)
        body_match = re.search(r'<body[^>]*>(.*)</body>', html_content, re.DOTALL | re.IGNORECASE)
        if body_match:
            html_content = body_match.group(1)

        text = extract_text_from_html(html_content)
        return text if len(text) > 100 else None

    except requests.RequestException as e:
        print(f"  Error fetching {doc_id}: {e}", file=sys.stderr)
        return None


def normalize(raw: dict, full_text: str) -> dict:
    """Transform raw document data into normalized schema."""

    doc_id = raw.get('uid', '')

    # Determine document type
    doc_type = "legislation"
    denomination = raw.get('denomination', '')

    # Parse date
    date_str = raw.get('date_depot') or raw.get('date_publication')
    if date_str and 'T' in date_str:
        date_str = date_str.split('T')[0]

    # Build URL
    legislature = raw.get('legislature', '17')
    # Convert doc_id to URL-friendly format
    # e.g., PRJLANR5L17B0621 -> l17b0621_projet-loi
    url = f"https://www.assemblee-nationale.fr/dyn/{legislature}/textes/{doc_id.lower()}"

    # Get title
    title = raw.get('title') or raw.get('title_short') or denomination

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": doc_type,
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date_str,
        "url": url,
        "legislature": raw.get('legislature'),
        "denomination": denomination,
        "dossier_ref": raw.get('dossier_ref'),
        "classification": raw.get('classification', {}),
    }


def fetch_sample(session: requests.Session, count: int = 15) -> list[dict]:
    """Fetch a sample of records with full text."""

    records = []
    docs_checked = 0

    print(f"Fetching document catalog...")

    for doc_meta in get_document_ids_from_zip(session):
        if len(records) >= count:
            break

        docs_checked += 1
        doc_id = doc_meta.get('uid')

        if not doc_id:
            continue

        print(f"  [{docs_checked}] Fetching full text for {doc_id}...")

        time.sleep(REQUEST_DELAY)

        full_text = fetch_full_text(session, doc_id)

        if full_text:
            record = normalize(doc_meta, full_text)
            records.append(record)
            print(f"    -> {len(full_text)} chars")
        else:
            print(f"    -> No text found")

        # Don't check too many documents
        if docs_checked > count * 5:
            print(f"  Checked {docs_checked} documents, stopping...")
            break

    return records


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all available records with full text."""

    for doc_meta in get_document_ids_from_zip(session):
        doc_id = doc_meta.get('uid')
        if not doc_id:
            continue

        time.sleep(REQUEST_DELAY)

        full_text = fetch_full_text(session, doc_id)

        if full_text:
            yield normalize(doc_meta, full_text)


def fetch_updates(session: requests.Session, since: datetime) -> Generator[dict, None, None]:
    """Fetch updates since a given date."""
    since_str = since.strftime("%Y-%m-%d")

    for doc_meta in get_document_ids_from_zip(session):
        doc_id = doc_meta.get('uid')
        if not doc_id:
            continue

        # Check document date
        date_str = doc_meta.get('date_depot') or doc_meta.get('date_publication')
        if date_str:
            doc_date = date_str.split('T')[0]
            if doc_date < since_str:
                continue

        time.sleep(REQUEST_DELAY)

        full_text = fetch_full_text(session, doc_id)

        if full_text:
            yield normalize(doc_meta, full_text)


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


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

    # Load existing status if present
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
    parser = argparse.ArgumentParser(description="FR/AssembleeNationale data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")

    # Updates command
    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    session = get_session()

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample records...")
            records = fetch_sample(session, args.count)
            if records:
                save_samples(records)
                update_status(len(records), 0, len(records))

                # Print summary
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

        elif args.full:
            print("Starting full fetch...")
            count = 0
            for record in fetch_all(session):
                count += 1
                if count % 100 == 0:
                    print(f"  {count} records...")
            print(f"Fetched {count} records")
            update_status(count, 0)

    elif args.command == "updates":
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        print(f"Fetching updates since {since.date()}...")
        count = 0
        for record in fetch_updates(session, since):
            count += 1
        print(f"Fetched {count} updated records")
        update_status(count, 0)


if __name__ == "__main__":
    main()
