#!/usr/bin/env python3
"""
CH/Entscheidsuche - Swiss Court Decisions from entscheidsuche.ch

Fetches court decisions from all Swiss federal and cantonal courts via
the Elasticsearch API at entscheidsuche.ch.

Coverage:
- Federal courts (CH): Bundesgericht, Bundesverwaltungsgericht, Bundesstrafgericht
- All 26 cantons: Zurich, Geneva, Bern, Vaud, Basel, etc.
- Total: 833K+ court decisions with full text

API: Elasticsearch at https://entscheidsuche.ch/_search.php
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# Configuration
API_URL = "https://entscheidsuche.ch/_search.php"
REQUEST_DELAY = 0.5  # 2 requests/second
PAGE_SIZE = 100
SAMPLE_SIZE = 15

# Canton codes
CANTONS = [
    "CH",  # Federal
    "AG", "AI", "AR", "BE", "BL", "BS", "FR", "GE", "GL", "GR",
    "JU", "LU", "NE", "NW", "OW", "SG", "SH", "SO", "SZ", "TG",
    "TI", "UR", "VD", "VS", "ZG", "ZH"
]


def search_documents(query: dict, timeout: int = 30) -> dict:
    """Execute Elasticsearch query against entscheidsuche.ch."""
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(
            API_URL,
            headers=headers,
            json=query,
            timeout=timeout
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error querying API: {e}", file=sys.stderr)
        return {}


def fetch_recent_decisions(days: int = 30, limit: int = 1000):
    """Fetch recent court decisions from the last N days."""
    since_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

    query = {
        "size": min(limit, PAGE_SIZE),
        "query": {
            "range": {
                "date": {
                    "gte": since_date
                }
            }
        },
        "sort": [{"date": "desc"}]
    }

    fetched = 0
    scroll_id = None

    while fetched < limit:
        if scroll_id:
            # Use search_after for pagination
            query["search_after"] = scroll_id

        result = search_documents(query)
        hits = result.get("hits", {}).get("hits", [])

        if not hits:
            break

        for hit in hits:
            yield normalize(hit)
            fetched += 1

            if fetched >= limit:
                break

        # Set search_after for next page
        if hits:
            last_hit = hits[-1]
            scroll_id = last_hit.get("sort")

        time.sleep(REQUEST_DELAY)


def fetch_by_canton(canton: str, limit: int = 100):
    """Fetch court decisions for a specific canton."""
    query = {
        "size": min(limit, PAGE_SIZE),
        "query": {
            "term": {"canton": canton}
        },
        "sort": [{"date": "desc"}]
    }

    fetched = 0

    while fetched < limit:
        result = search_documents(query)
        hits = result.get("hits", {}).get("hits", [])

        if not hits:
            break

        for hit in hits:
            yield normalize(hit)
            fetched += 1

            if fetched >= limit:
                break

        # Pagination using search_after
        if hits and fetched < limit:
            last_hit = hits[-1]
            query["search_after"] = last_hit.get("sort")
            time.sleep(REQUEST_DELAY)
        else:
            break


def fetch_all():
    """Fetch all court decisions (warning: 833K+ records)."""
    print("Warning: This will fetch 833K+ records. Use fetch_updates() for incremental fetching.")

    query = {
        "size": PAGE_SIZE,
        "query": {"match_all": {}},
        "sort": [{"date": "desc"}]
    }

    total_fetched = 0

    while True:
        result = search_documents(query)
        hits = result.get("hits", {}).get("hits", [])

        if not hits:
            break

        for hit in hits:
            yield normalize(hit)
            total_fetched += 1

            if total_fetched % 1000 == 0:
                print(f"Fetched {total_fetched} records...", file=sys.stderr)

        # Pagination
        last_hit = hits[-1]
        query["search_after"] = last_hit.get("sort")
        time.sleep(REQUEST_DELAY)


def fetch_updates(since: str):
    """Fetch documents modified since a given date (YYYY-MM-DD)."""
    query = {
        "size": PAGE_SIZE,
        "query": {
            "range": {
                "date": {
                    "gte": since
                }
            }
        },
        "sort": [{"date": "desc"}]
    }

    while True:
        result = search_documents(query)
        hits = result.get("hits", {}).get("hits", [])

        if not hits:
            break

        for hit in hits:
            yield normalize(hit)

        # Pagination
        last_hit = hits[-1]
        query["search_after"] = last_hit.get("sort")
        time.sleep(REQUEST_DELAY)


def normalize(raw: dict) -> dict:
    """Transform raw Elasticsearch hit into normalized schema."""
    source = raw.get("_source", {})
    doc_id = raw.get("_id", "")

    # Extract full text from attachment
    attachment = source.get("attachment", {})
    full_text = attachment.get("content", "")

    # Extract title (multilingual)
    title_obj = source.get("title", {})
    if isinstance(title_obj, dict):
        # Prefer German, then French, then Italian
        title = title_obj.get("de") or title_obj.get("fr") or title_obj.get("it") or ""
    else:
        title = str(title_obj)

    # Extract abstract (multilingual)
    abstract_obj = source.get("abstract", {})
    if isinstance(abstract_obj, dict):
        abstract = abstract_obj.get("de") or abstract_obj.get("fr") or abstract_obj.get("it") or ""
    else:
        abstract = str(abstract_obj) if abstract_obj else ""

    # Determine language from attachment or title
    language = attachment.get("language", "de")

    # Build content URL
    content_url = attachment.get("content_url", "")

    # Build web URL (use PDF URL if available)
    if content_url:
        web_url = content_url
    else:
        web_url = f"https://entscheidsuche.ch/docs/{doc_id.replace('_', '/')}"

    return {
        "_id": doc_id,
        "_source": "CH/Entscheidsuche",
        "_type": "case_law",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",

        # Core fields
        "title": title,
        "text": full_text,
        "abstract": abstract,
        "date": source.get("date", ""),
        "url": web_url,

        # Court metadata
        "canton": source.get("canton", ""),
        "hierarchy": source.get("hierarchy", []),
        "reference": source.get("reference", []),

        # Language
        "language": language,

        # Source metadata
        "scrape_date": source.get("scrapedate", ""),
        "content_type": attachment.get("content_type", "application/pdf"),
        "content_length": attachment.get("content_length", len(full_text)),

        # Index info
        "es_index": raw.get("_index", ""),
    }


def get_statistics():
    """Get statistics about available court decisions."""
    query = {
        "size": 0,
        "aggs": {
            "cantons": {
                "terms": {"field": "canton", "size": 50}
            },
            "years": {
                "date_histogram": {
                    "field": "date",
                    "calendar_interval": "year"
                }
            }
        }
    }

    result = search_documents(query)

    total = result.get("hits", {}).get("total", {})
    if isinstance(total, dict):
        total_count = total.get("value", 0)
    else:
        total_count = total

    cantons = result.get("aggregations", {}).get("cantons", {}).get("buckets", [])

    print(f"Total documents: {total_count}+")
    print("\nBy canton:")
    for bucket in cantons:
        print(f"  {bucket['key']}: {bucket['doc_count']:,}")


def bootstrap_sample():
    """Fetch sample records for testing."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    # Fetch a mix of federal and cantonal decisions
    samples = []

    # Get 5 federal decisions
    print("Fetching federal court decisions (CH)...")
    for doc in fetch_by_canton("CH", limit=5):
        samples.append(doc)
        time.sleep(REQUEST_DELAY)

    # Get 5 from Zurich
    print("Fetching Zurich court decisions (ZH)...")
    for doc in fetch_by_canton("ZH", limit=5):
        samples.append(doc)
        time.sleep(REQUEST_DELAY)

    # Get 5 from Geneva
    print("Fetching Geneva court decisions (GE)...")
    for doc in fetch_by_canton("GE", limit=5):
        samples.append(doc)
        time.sleep(REQUEST_DELAY)

    # Save samples
    total_text_length = 0
    records_with_text = 0

    for i, doc in enumerate(samples):
        filename = f"sample_{i+1:03d}.json"
        filepath = sample_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(doc, f, indent=2, ensure_ascii=False)

        text_len = len(doc.get("text", ""))
        total_text_length += text_len
        if text_len > 0:
            records_with_text += 1

        print(f"  Saved {filename}: {doc.get('canton', 'N/A')} - {doc.get('title', 'N/A')[:60]}... ({text_len} chars)")

    print(f"\n=== Sample Summary ===")
    print(f"Total records: {len(samples)}")
    print(f"Records with text: {records_with_text}")
    print(f"Average text length: {total_text_length // max(len(samples), 1)} chars")
    print(f"Samples saved to: {sample_dir}")

    return samples


def main():
    parser = argparse.ArgumentParser(description="CH/Entscheidsuche - Swiss Court Decisions")
    parser.add_argument("command", choices=["bootstrap", "stats", "fetch-canton", "fetch-recent"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--canton", type=str, help="Canton code (e.g., CH, ZH, GE)")
    parser.add_argument("--days", type=int, default=30, help="Number of days for recent fetch")
    parser.add_argument("--limit", type=int, default=100, help="Maximum records to fetch")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        else:
            print("Full bootstrap not recommended. Use --sample for testing.")

    elif args.command == "stats":
        get_statistics()

    elif args.command == "fetch-canton":
        if not args.canton:
            print("Error: --canton required", file=sys.stderr)
            sys.exit(1)

        for doc in fetch_by_canton(args.canton, limit=args.limit):
            print(json.dumps(doc, ensure_ascii=False))

    elif args.command == "fetch-recent":
        for doc in fetch_recent_decisions(days=args.days, limit=args.limit):
            print(json.dumps(doc, ensure_ascii=False))


if __name__ == "__main__":
    main()
