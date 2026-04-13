#!/usr/bin/env python3
"""
Canada Competition Bureau Data Fetcher

Fetches Competition Bureau doctrine from competition-bureau.canada.ca
via the Drupal JSON:API (node/page endpoint with full HTML body text).

Covers: enforcement guidelines, position statements, case outcomes,
consultation responses, market studies, publications, and more.

Data source: https://competition-bureau.canada.ca/
License: Crown Copyright (Government of Canada Open Data)
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

BASE_URL = "https://competition-bureau.canada.ca"
JSONAPI_PAGES = f"{BASE_URL}/en/jsonapi/node/page"
PAGE_LIMIT = 50
RATE_LIMIT_DELAY = 1.5
CURL_TIMEOUT = 60

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


def fetch_json(url: str, retries: int = 2) -> Optional[dict]:
    """Fetch a JSON:API URL using curl."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-H", "Accept: application/vnd.api+json",
                 "-w", "\n%{http_code}", url],
                capture_output=True, text=True, timeout=CURL_TIMEOUT + 10
            )
            parts = result.stdout.rsplit("\n", 1)
            if len(parts) == 2:
                body, status = parts[0], parts[1].strip()
            else:
                body, status = result.stdout, "000"

            if not status.startswith("2"):
                if attempt == retries:
                    print(f"HTTP {status} for {url}", file=sys.stderr)
                    return None
                time.sleep(3)
                continue
            if body:
                return json.loads(body)
            if attempt == retries:
                return None
            time.sleep(3)
        except Exception as e:
            if attempt == retries:
                print(f"Failed to fetch {url}: {e}", file=sys.stderr)
                return None
            time.sleep(3)
    return None


def strip_html(html: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if not html:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode common entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    # Collapse whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    return text.strip()


def categorize_text(title: str) -> str:
    """Categorize a document by its title."""
    if not title:
        return "other"
    t = title.lower()
    if "position statement" in t:
        return "position_statement"
    if "enforcement guideline" in t or "guideline" in t:
        return "enforcement_guideline"
    if "consultation" in t or "submission" in t:
        return "consultation"
    if "publication" in t or "market stud" in t or "report" in t:
        return "publication"
    if "case" in t or "outcome" in t or "consent agreement" in t:
        return "case_outcome"
    if "bid-rigging" in t or "bid rigging" in t or "price-fixing" in t:
        return "bid_rigging"
    if "deceptive" in t or "misleading" in t:
        return "deceptive_marketing"
    if "merger" in t:
        return "merger"
    if "fraud" in t or "scam" in t:
        return "fraud_awareness"
    if "label" in t:
        return "labelling"
    return "other"


def normalize(node: dict) -> Optional[dict]:
    """Transform a Drupal JSON:API node into a standard record."""
    attrs = node.get("attributes", {})
    title = attrs.get("title", "")
    body = attrs.get("body", {})
    if body is None:
        body = {}
    html_text = body.get("value", "") or ""
    text = strip_html(html_text)

    if not text or len(text) < 50:
        return None

    nid = attrs.get("drupal_internal__nid")
    url = f"{BASE_URL}/en/node/{nid}" if nid else ""

    changed = attrs.get("changed", "")
    created = attrs.get("created", "")
    date_str = changed or created or ""
    if date_str:
        date_str = date_str[:10]  # YYYY-MM-DD

    node_id = node.get("id", "")
    doc_category = categorize_text(title)

    return {
        "_id": f"CA/CompetitionBureau/{node_id}",
        "_source": "CA/CompetitionBureau",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": url,
        "doc_category": doc_category,
        "drupal_id": node_id,
        "language": "en",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all page nodes from the Drupal JSON:API."""
    offset = 0
    total = 0
    fields = "fields%5Bnode--page%5D=title,body,drupal_internal__nid,changed,created"
    while True:
        url = f"{JSONAPI_PAGES}?page%5Blimit%5D={PAGE_LIMIT}&page%5Boffset%5D={offset}&{fields}"
        print(f"Fetching offset={offset}...", file=sys.stderr)
        data = fetch_json(url)
        if not data or "data" not in data:
            print(f"No data at offset={offset}, stopping.", file=sys.stderr)
            break

        nodes = data["data"]
        if not nodes:
            break

        for node in nodes:
            record = normalize(node)
            if record:
                yield record
                total += 1
                if sample and total >= 15:
                    print(f"Sample mode: collected {total} records.", file=sys.stderr)
                    return

        offset += PAGE_LIMIT
        time.sleep(RATE_LIMIT_DELAY)

    print(f"Total records: {total}", file=sys.stderr)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents modified since a given date (ISO 8601)."""
    fields = "fields%5Bnode--page%5D=title,body,drupal_internal__nid,changed,created"
    date_filter = f"filter%5Bchanged%5D%5Bcondition%5D%5Bpath%5D=changed&filter%5Bchanged%5D%5Bcondition%5D%5Boperator%5D=%3E%3D&filter%5Bchanged%5D%5Bcondition%5D%5Bvalue%5D={since}"
    offset = 0
    total = 0
    while True:
        url = f"{JSONAPI_PAGES}?page%5Blimit%5D={PAGE_LIMIT}&page%5Boffset%5D={offset}&{fields}&{date_filter}"
        data = fetch_json(url)
        if not data or "data" not in data:
            break
        nodes = data["data"]
        if not nodes:
            break
        for node in nodes:
            record = normalize(node)
            if record:
                yield record
                total += 1
        offset += PAGE_LIMIT
        time.sleep(RATE_LIMIT_DELAY)
    print(f"Updated records since {since}: {total}", file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Canada Competition Bureau data fetcher")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch documents")
    boot.add_argument("--sample", action="store_true", help="Fetch only ~15 sample records")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    upd = sub.add_parser("update", help="Fetch updates since date")
    upd.add_argument("--since", required=True, help="ISO date (YYYY-MM-DD)")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            out_path = SAMPLE_DIR / f"{count:04d}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
            if count % 50 == 0:
                print(f"Saved {count} records...", file=sys.stderr)
        print(f"Done. Saved {count} records to {SAMPLE_DIR}/", file=sys.stderr)

    elif args.command == "update":
        count = 0
        for record in fetch_updates(args.since):
            out_path = SAMPLE_DIR / f"upd_{count:04d}.json"
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
        print(f"Done. Saved {count} updated records.", file=sys.stderr)


if __name__ == "__main__":
    main()
