#!/usr/bin/env python3
"""
BOFiP - Bulletin Officiel des Finances Publiques Data Fetcher

Fetches French tax doctrine from the official open data API.
Covers all active BOFiP-Impôts publications: administrative comments on tax law,
rescrit decisions, ministerial responses, and jurisprudence comments.

Data source: https://data.economie.gouv.fr/explore/dataset/bofip-vigueur/api/
License: Licence Ouverte v2.0 (Etalab)
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests
from bs4 import BeautifulSoup

# Constants
API_BASE = "https://data.economie.gouv.fr/api/explore/v2.1"
DATASET_ID = "bofip-vigueur"
RECORDS_ENDPOINT = f"{API_BASE}/catalog/datasets/{DATASET_ID}/records"
RATE_LIMIT_DELAY = 0.5  # seconds between requests (API is generous)
PAGE_SIZE = 100  # max records per API call


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, 'html.parser')
    text = soup.get_text(separator='\n')
    # Clean up whitespace
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """
    Fetch all BOFiP publications from the open data API.

    Args:
        max_docs: Maximum number of documents to fetch (None for all)
    """
    offset = 0
    doc_count = 0

    while True:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "order_by": "debut_de_validite DESC"
        }

        try:
            response = requests.get(RECORDS_ENDPOINT, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error fetching records at offset {offset}: {e}", file=sys.stderr)
            break

        results = data.get("results", [])
        total_count = data.get("total_count", 0)

        if not results:
            break

        for record in results:
            yield record
            doc_count += 1

            if max_docs and doc_count >= max_docs:
                return

        offset += PAGE_SIZE

        if offset >= total_count:
            break

        # Progress report
        print(f"Fetched {doc_count}/{total_count} records...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """
    Fetch publications updated since a given date.

    Uses the debut_de_validite field for filtering.
    """
    since_str = since.strftime("%Y-%m-%d")
    offset = 0

    while True:
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "where": f"debut_de_validite >= '{since_str}'",
            "order_by": "debut_de_validite DESC"
        }

        try:
            response = requests.get(RECORDS_ENDPOINT, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error fetching updates at offset {offset}: {e}", file=sys.stderr)
            break

        results = data.get("results", [])
        total_count = data.get("total_count", 0)

        if not results:
            break

        for record in results:
            yield record

        offset += PAGE_SIZE

        if offset >= total_count:
            break

        time.sleep(RATE_LIMIT_DELAY)


def normalize(raw: dict) -> dict:
    """Transform raw API record into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    # Extract fields from the API response
    identifiant = raw.get("identifiant_juridique", "")
    titre = raw.get("titre", "")
    date_validite = raw.get("debut_de_validite")
    serie = raw.get("serie", "")
    division = raw.get("division", "")
    doc_type = raw.get("type", "")
    permalien = raw.get("permalien", "")

    # Get content - prefer HTML, fall back to plain text
    contenu_html = raw.get("contenu_html", "")
    contenu = raw.get("contenu", "")

    # Clean the content
    if contenu_html:
        text = clean_html(contenu_html)
    elif contenu:
        text = contenu.strip()
    else:
        text = ""

    # Build document ID
    doc_id = identifiant if identifiant else f"bofip-{hash(titre)}"

    # Build URL
    url = permalien if permalien else f"https://bofip.impots.gouv.fr/bofip/{identifiant}"

    return {
        "_id": doc_id,
        "_source": "FR/Bofip",
        "_type": "doctrine",
        "_fetched_at": now,
        "title": titre,
        "text": text,
        "date": date_validite,
        "url": url,
        "identifiant_juridique": identifiant,
        "serie": serie,
        "division": division,
        "type": doc_type,
        "language": "fr"
    }


def bootstrap_sample(sample_dir: Path, count: int = 15) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count):
        record = normalize(raw)

        # Skip records without text
        if not record["text"]:
            print(f"Skipping {record['_id']}: no text content", file=sys.stderr)
            continue

        samples.append(record)

        # Save individual sample
        safe_id = record["_id"].replace("/", "_").replace(":", "_")
        filename = f"{safe_id}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text'])} chars)", file=sys.stderr)

        if len(samples) >= count:
            break

    # Save combined samples
    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        # Calculate statistics
        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)

        # Count by serie
        by_serie = {}
        for s in samples:
            serie = s.get("serie") or "Unknown"
            by_serie[serie] = by_serie.get(serie, 0) + 1

        print(f"\nBy serie:", file=sys.stderr)
        for serie, count in sorted(by_serie.items(), key=lambda x: -x[1]):
            print(f"  {serie}: {count}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="BOFiP tax doctrine fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Generate sample data only")
    parser.add_argument("--count", type=int, default=15,
                       help="Number of samples to generate")
    parser.add_argument("--since", type=str,
                       help="Fetch updates since date (YYYY-MM-DD)")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            # Full bootstrap
            for raw in fetch_all():
                record = normalize(raw)
                if record["text"]:
                    print(json.dumps(record, ensure_ascii=False))

    elif args.command == "fetch":
        for raw in fetch_all(max_docs=args.count if args.sample else None):
            record = normalize(raw)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for raw in fetch_updates(since):
            record = normalize(raw)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
