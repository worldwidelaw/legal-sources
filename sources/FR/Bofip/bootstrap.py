#!/usr/bin/env python3
"""
BOFiP - Bulletin Officiel des Finances Publiques Data Fetcher

Fetches French tax doctrine from the official open data API.
Covers all active BOFiP-Impots publications: administrative comments on tax law,
rescrit decisions, ministerial responses, and jurisprudence comments.

Data source: https://data.economie.gouv.fr/explore/dataset/bofip-vigueur/
License: Licence Ouverte v2.0 (Etalab)
"""

import json
import re
import sys
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

# Constants
API_URL = "https://data.economie.gouv.fr/api/records/1.0/search/"
DATASET_ID = "bofip-vigueur"
RATE_LIMIT_DELAY = 1.5  # seconds between requests
PAGE_SIZE = 100  # max rows per API call


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    if HAS_BS4:
        soup = BeautifulSoup(html_text, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
    else:
        # Fallback: regex-based HTML stripping
        text = re.sub(r"<[^>]+>", " ", html_text)
        text = unescape(text)
    # Collapse excessive whitespace
    text = re.sub(r"\n[ \t]*\n[ \t]*\n+", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def api_get(params: dict) -> dict:
    """Make a GET request to the API and return parsed JSON."""
    params["dataset"] = DATASET_ID
    url = API_URL + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url)
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_records(max_docs: Optional[int] = None,
                  sort: str = "debut_de_validite") -> Generator[dict, None, None]:
    """
    Fetch BOFiP publications from the paginated API.

    Args:
        max_docs: Maximum number of documents to fetch (None for all)
        sort: Field to sort by
    """
    start = 0
    doc_count = 0

    while True:
        params = {
            "rows": PAGE_SIZE,
            "start": start,
            "sort": sort,
        }

        try:
            data = api_get(params)
        except Exception as e:
            print(f"Error fetching records at start={start}: {e}", file=sys.stderr)
            break

        records = data.get("records", [])
        nhits = data.get("nhits", 0)

        if not records:
            break

        for record in records:
            fields = record.get("fields", {})
            yield fields
            doc_count += 1

            if max_docs and doc_count >= max_docs:
                return

        start += PAGE_SIZE

        if start >= nhits:
            break

        print(f"Fetched {doc_count}/{nhits} records...", file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)

    print(f"Done: {doc_count} records fetched.", file=sys.stderr)


def normalize(fields: dict) -> dict:
    """Transform raw API fields into the normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    identifiant = fields.get("identifiant_juridique", "")
    titre = fields.get("titre", "")
    date_validite = fields.get("debut_de_validite", "")
    serie = fields.get("serie", "")
    division = fields.get("division", "")
    doc_type = fields.get("type", "")
    permalien = fields.get("permalien", "")

    # Get content: prefer plain-text "contenu", fall back to cleaning HTML
    contenu = fields.get("contenu", "")
    contenu_html = fields.get("contenu_html", "")

    if contenu and contenu.strip():
        text = contenu.strip()
    elif contenu_html:
        text = clean_html(contenu_html)
    else:
        text = ""

    # Build document ID
    doc_id = identifiant if identifiant else f"bofip-{hash(titre)}"

    # Build URL
    url = permalien if permalien else f"https://bofip.impots.gouv.fr/bofip/{identifiant}"

    # Normalize date to ISO format (already YYYY-MM-DD from API)
    date_str = date_validite[:10] if date_validite else ""

    return {
        "_id": doc_id,
        "_source": "FR/BOFiP",
        "_type": "doctrine",
        "_fetched_at": now,
        "title": titre,
        "text": text,
        "date": date_str,
        "url": url,
        "identifiant_juridique": identifiant,
        "serie": serie,
        "division": division,
        "type": doc_type,
        "language": "fr",
    }


def bootstrap_sample(sample_dir: Path, count: int = 15) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for fields in fetch_records(max_docs=count + 5):
        record = normalize(fields)

        # Skip records with no meaningful text
        if not record["text"] or len(record["text"]) < 50:
            print(f"Skipping {record['_id']}: insufficient text", file=sys.stderr)
            continue

        samples.append(record)

        # Save individual sample
        safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
        filename = f"{safe_id}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text']):,} chars)", file=sys.stderr)

        if len(samples) >= count:
            break

    # Save combined samples
    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)

        by_serie = {}
        for s in samples:
            serie = s.get("serie") or "Unknown"
            by_serie[serie] = by_serie.get(serie, 0) + 1

        print(f"\nBy serie:", file=sys.stderr)
        for serie, cnt in sorted(by_serie.items(), key=lambda x: -x[1]):
            print(f"  {serie}: {cnt}", file=sys.stderr)


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
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            # Full bootstrap: emit JSONL to stdout
            count = 0
            for fields in fetch_records():
                record = normalize(fields)
                if record["text"] and len(record["text"]) >= 50:
                    print(json.dumps(record, ensure_ascii=False))
                    count += 1
            print(f"Full bootstrap: {count} records emitted.", file=sys.stderr)

    elif args.command == "fetch":
        limit = args.count if args.sample else None
        for fields in fetch_records(max_docs=limit):
            record = normalize(fields)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since_str = args.since
        # Use API filter for date range
        start = 0
        count = 0
        while True:
            params = {
                "rows": PAGE_SIZE,
                "start": start,
                "sort": "debut_de_validite",
                "q": f"debut_de_validite >= {since_str}",
            }
            try:
                data = api_get(params)
            except Exception as e:
                print(f"Error: {e}", file=sys.stderr)
                break
            records = data.get("records", [])
            if not records:
                break
            for rec in records:
                fields = rec.get("fields", {})
                record = normalize(fields)
                if record["text"]:
                    print(json.dumps(record, ensure_ascii=False))
                    count += 1
            start += PAGE_SIZE
            if start >= data.get("nhits", 0):
                break
            time.sleep(RATE_LIMIT_DELAY)
        print(f"Updates: {count} records since {since_str}.", file=sys.stderr)


if __name__ == "__main__":
    main()
