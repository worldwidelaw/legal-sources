#!/usr/bin/env python3
"""
LegiMonaco Legal Database Fetcher

Fetches legislation and case law from Monaco's official legal database
via its open Elasticsearch API.

Data source: https://legimonaco.mc
- Legislation (lois, ordonnances, arrêtés ministériels): ~12,500 texts
- Case law (all Monaco courts): ~6,700 decisions
- Also: international treaties, codes, constitution

Full text available in enBody field. No authentication required.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests
from bs4 import BeautifulSoup

# Constants
ES_URL = "https://legimonaco.mc/~~search/depot/_search"
BASE_URL = "https://legimonaco.mc"
RATE_LIMIT_DELAY = 1  # seconds between requests
PAGE_SIZE = 100
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "LegalDataHunter/1.0 (legal-data-collection)"
}

# Document types to fetch and their _type mapping
DOC_TYPES = {
    "legislation": "legislation",       # lois
    "regulation": "legislation",        # arrêtés, ordonnances, etc.
    "tai": "legislation",               # international treaties
    "tc": "legislation",                # consolidated codes
    "constitution": "legislation",      # constitution
    "legislativeWork": "legislation",   # travaux législatifs
    "case": "case_law",                 # jurisprudence
}


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n\s*\n", "\n\n", text)
    return text.strip()


def es_search(query: dict, timeout: int = 30, retries: int = 3) -> dict:
    """Execute an Elasticsearch query with retry logic."""
    for attempt in range(retries):
        try:
            response = requests.post(ES_URL, json=query, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError) as e:
            print(f"  Connection error (attempt {attempt+1}/{retries}): {e}", file=sys.stderr)
            if attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise
        except requests.exceptions.HTTPError as e:
            print(f"  HTTP error (attempt {attempt+1}/{retries}): {e}", file=sys.stderr)
            if response.status_code >= 500 and attempt < retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                raise


def fetch_by_type(doc_type: str, max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """
    Fetch all documents of a given type using search_after pagination.
    Handles >10,000 results via search_after.
    """
    query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"scgroup": "res"}},
                    {"term": {"type": doc_type}}
                ]
            }
        },
        "_source": True,
        "size": PAGE_SIZE,
        "sort": [{"date": {"order": "desc"}}, {"_id": {"order": "asc"}}],
        "track_total_hits": True
    }

    total = None
    fetched = 0

    while True:
        result = es_search(query)

        if total is None:
            total = result["hits"]["total"]["value"]
            print(f"  Type '{doc_type}': {total} documents", file=sys.stderr)

        hits = result["hits"]["hits"]
        if not hits:
            break

        for hit in hits:
            yield hit
            fetched += 1
            if max_docs and fetched >= max_docs:
                return

        # Use search_after for next page
        last_sort = hits[-1]["sort"]
        query["search_after"] = last_sort

        time.sleep(RATE_LIMIT_DELAY)

    print(f"  Fetched {fetched}/{total} '{doc_type}' documents", file=sys.stderr)


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all documents across all types."""
    docs_per_type = None
    if max_docs:
        # Distribute max_docs across types proportionally
        # Give at least a few per type for sampling
        n_types = len(DOC_TYPES)
        docs_per_type = max(5, max_docs // n_types)

    for doc_type in DOC_TYPES:
        yield from fetch_by_type(doc_type, max_docs=docs_per_type)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Fetch documents updated since a given date."""
    since_str = since.strftime("%Y-%m-%d")

    for doc_type in DOC_TYPES:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"scgroup": "res"}},
                        {"term": {"type": doc_type}},
                        {"range": {"date": {"gte": since_str}}}
                    ]
                }
            },
            "_source": True,
            "size": PAGE_SIZE,
            "sort": [{"date": {"order": "desc"}}, {"_id": {"order": "asc"}}],
            "track_total_hits": True
        }

        total = None
        while True:
            result = es_search(query)

            if total is None:
                total = result["hits"]["total"]["value"]
                print(f"  Updates for '{doc_type}' since {since_str}: {total}", file=sys.stderr)

            hits = result["hits"]["hits"]
            if not hits:
                break

            for hit in hits:
                yield hit

            last_sort = hits[-1]["sort"]
            query["search_after"] = last_sort
            time.sleep(RATE_LIMIT_DELAY)


def normalize(hit: dict) -> dict:
    """Transform an ES hit into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()
    src = hit["_source"]
    es_id = hit["_id"]
    doc_type = src.get("type", "")

    # Get full text from enBody
    text = src.get("enBody", "")
    if not text:
        # Try caseAbstract as fallback for case law
        text = clean_html(src.get("caseAbstract", ""))

    # Build URL from path
    path = src.get("path", "")
    url = f"{BASE_URL}{path}" if path else ""

    # Determine _type
    mapped_type = DOC_TYPES.get(doc_type, "legislation")

    # Build title
    title = src.get("title", "")
    if not title:
        number = src.get("number", "")
        nature = src.get("tncNature", doc_type)
        title = f"{nature} n° {number}" if number else f"Document {es_id}"

    record = {
        "_id": es_id,
        "_source": "MC/LegiMonaco",
        "_type": mapped_type,
        "_fetched_at": now,
        "title": title,
        "text": text,
        "date": src.get("date"),
        "url": url,
        "language": "fr",
        "doc_type": doc_type,
    }

    # Add type-specific metadata
    if doc_type == "case":
        record["jurisdiction"] = src.get("jurisdiction", "")
        record["parties"] = src.get("parties", "")
        record["case_area"] = src.get("caseArea", "")
    else:
        record["number"] = src.get("number", "")
        record["nature"] = src.get("tncNature", "")

    return record


def bootstrap_sample(sample_dir: Path, count: int = 100) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    empty_text = 0

    for hit in fetch_all(max_docs=count):
        record = normalize(hit)

        if not record["text"]:
            empty_text += 1
            continue

        samples.append(record)

        # Save individual sample
        filename = f"{record['_id']}.json"
        # Sanitize filename
        filename = re.sub(r'[^\w\-.]', '_', filename)
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text'])} chars)", file=sys.stderr)

    # Save combined samples
    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Skipped (no text): {empty_text}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)

        # Count by type
        by_type = {}
        for s in samples:
            t = s.get("_type", "unknown")
            by_type[t] = by_type.get(t, 0) + 1
        print(f"\nBy type:", file=sys.stderr)
        for t, c in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"  {t}: {c}", file=sys.stderr)

        # Count by doc_type
        by_doc = {}
        for s in samples:
            d = s.get("doc_type", "unknown")
            by_doc[d] = by_doc.get(d, 0) + 1
        print(f"\nBy doc_type:", file=sys.stderr)
        for d, c in sorted(by_doc.items(), key=lambda x: -x[1]):
            print(f"  {d}: {c}", file=sys.stderr)


def bootstrap_fast(batch_size: int = 100):
    """Full bootstrap using StorageManager for batched writes to disk."""
    project_root = Path(__file__).resolve().parent.parent.parent.parent
    sys.path.insert(0, str(project_root))
    from common.storage import StorageManager

    source_dir = Path(__file__).parent
    storage = StorageManager(source_dir / "data")

    total_written = 0
    total_skipped = 0
    batch = []

    for hit in fetch_all():
        record = normalize(hit)
        if not record["text"]:
            total_skipped += 1
            continue

        dedup_key = f"MC/LegiMonaco:{record['_id']}"
        batch.append((dedup_key, record))

        if len(batch) >= batch_size:
            written = storage.write_batch(batch)
            total_written += written
            print(f"  Written {total_written} records so far...", file=sys.stderr)
            batch = []

    # Flush remaining
    if batch:
        written = storage.write_batch(batch)
        total_written += written

    storage.close()
    print(f"\nBootstrap-fast complete: {total_written} records written, {total_skipped} skipped (no text)", file=sys.stderr)
    print(f"Records written: {total_written}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py <command> [options]")
        print("Commands: bootstrap, bootstrap-fast, fetch, updates")
        print("Options: --sample, --count N, --since YYYY-MM-DD")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    # Parse --count and --since from argv
    count = 100
    since_str = None
    for i, arg in enumerate(sys.argv):
        if arg == "--count" and i + 1 < len(sys.argv):
            count = int(sys.argv[i + 1])
        if arg == "--since" and i + 1 < len(sys.argv):
            since_str = sys.argv[i + 1]

    if command == "bootstrap":
        if sample_mode:
            bootstrap_sample(sample_dir, count)
        else:
            written = 0
            empty = 0
            for hit in fetch_all():
                record = normalize(hit)
                if record["text"]:
                    print(json.dumps(record, ensure_ascii=False))
                    written += 1
                else:
                    empty += 1
            print(f"Bootstrap complete: {written} records written, {empty} skipped (no text)", file=sys.stderr)

    elif command == "bootstrap-fast":
        batch_size = 100
        for i, arg in enumerate(sys.argv):
            if arg == "--batch-size" and i + 1 < len(sys.argv):
                batch_size = int(sys.argv[i + 1])
        bootstrap_fast(batch_size=batch_size)

    elif command == "fetch":
        for hit in fetch_all(max_docs=count if sample_mode else None):
            record = normalize(hit)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))

    elif command == "updates":
        if not since_str:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since = datetime.fromisoformat(since_str)
        for hit in fetch_updates(since):
            record = normalize(hit)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
