#!/usr/bin/env python3
"""
Moldova Constitutional Court (Curtea Constituțională) Data Fetcher

Fetches case law from the Moldova Constitutional Court via its DataTables
JSON API for listing and HTML pages for full text extraction.

Data source: https://www.constcourt.md
- 5,700+ documents (1995-present)
- Judgments (hotărîri), decisions (decizii), referrals (sesizări),
  opinions (avize), addresses (adrese)
- Full text available in HTML pages
- Romanian language, no authentication required
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
SEARCH_URL = "https://www.constcourt.md/ccdoc_search_request_test.php"
DOC_URL = "https://www.constcourt.md/ccdocview.php"
BASE_URL = "https://www.constcourt.md"
RATE_LIMIT_DELAY = 2  # seconds between requests
PAGE_SIZE = 200

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal-data-collection)",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.constcourt.md/ccdocview.php?l=ro",
}

# Document type registers (with full text)
# Sesizări (reg 2) are referral notices without legal reasoning - excluded
DOC_REGISTERS = {
    "1": "hotariri",    # Judgments - full text available
    "3": "decizii",     # Decisions - full text available
    "4": "avize",       # Opinions - full text available
    "5": "adrese",      # Addresses - full text available
}

# Types without meaningful full text (filing notices)
SKIP_TYPES = {"sesizari"}


def list_documents(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """List all documents via the DataTables JSON API."""
    offset = 0
    total = None
    fetched = 0

    while True:
        data = {
            "sEcho": str(offset // PAGE_SIZE + 1),
            "iDisplayStart": str(offset),
            "iDisplayLength": str(PAGE_SIZE),
            "reg[]": list(DOC_REGISTERS.keys()),
            "langinit": "1",
            "l": "ro",
            "iSortCol_0": "0",
            "sSortDir_0": "desc",
            "sSearch": "",
        }

        response = requests.post(SEARCH_URL, data=data, headers=HEADERS, timeout=30)
        response.raise_for_status()
        result = response.json()

        if total is None:
            total = result["iTotalDisplayRecords"]
            print(f"Total documents: {total}", file=sys.stderr)

        rows = result.get("aaData", [])
        if not rows:
            break

        for row in rows:
            row_id = row.get("DT_RowId", "")
            doc_id = row_id.replace("row_", "") if row_id.startswith("row_") else row_id
            date = row.get("0", "")
            doc_number = row.get("1", "")
            html_cell = row.get("2", "")
            doc_type_label = row.get("3", "")

            # Parse the link from HTML cell
            soup = BeautifulSoup(html_cell, "html.parser")
            link = soup.find("a")
            title = link.get_text(strip=True) if link else ""
            href = link.get("href", "") if link else ""

            # Extract tip and docid from href
            tip = ""
            if "tip=" in href:
                tip = href.split("tip=")[1].split("&")[0]

            yield {
                "doc_id": doc_id,
                "date": date,
                "doc_number": doc_number,
                "title": title,
                "doc_type_label": doc_type_label,
                "tip": tip,
                "href": href,
            }
            fetched += 1

            if max_docs and fetched >= max_docs:
                return

        offset += PAGE_SIZE
        if offset >= total:
            break

        time.sleep(RATE_LIMIT_DELAY)

    print(f"Listed {fetched}/{total} documents", file=sys.stderr)


def fetch_full_text(tip: str, doc_id: str) -> str:
    """Fetch full text from an individual document page."""
    url = f"{DOC_URL}?tip={tip}&docid={doc_id}&l=ro"
    response = requests.get(url, headers={
        "User-Agent": "LegalDataHunter/1.0 (legal-data-collection)"
    }, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Primary: div#tabs-1 > div.newscontent
    tabs = soup.find("div", id="tabs-1")
    if tabs:
        news = tabs.find("div", class_="newscontent")
        if news:
            text = news.get_text(separator="\n").strip()
            if text:
                # Clean up whitespace
                text = re.sub(r"\n\s*\n", "\n\n", text)
                return text

    # Fallback: any div.newscontent
    for div in soup.find_all("div", class_="newscontent"):
        text = div.get_text(separator="\n").strip()
        if text and len(text) > 100:
            text = re.sub(r"\n\s*\n", "\n\n", text)
            return text

    return ""


def fetch_all(max_docs: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all documents with full text."""
    for doc in list_documents(max_docs=max_docs):
        if not doc["tip"] or not doc["doc_id"]:
            continue

        text = fetch_full_text(doc["tip"], doc["doc_id"])
        time.sleep(RATE_LIMIT_DELAY)

        doc["text"] = text
        yield doc


def normalize(raw: dict) -> dict:
    """Transform raw document data into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    doc_id = raw.get("doc_id", "")
    tip = raw.get("tip", "")
    href = raw.get("href", "")
    url = f"{BASE_URL}/{href}" if href else ""

    # Map doc_type_label to a simpler category
    type_label = raw.get("doc_type_label", "").lower()

    return {
        "_id": f"MD-CC-{tip}-{doc_id}",
        "_source": "MD/CurteaConstitutionala",
        "_type": "case_law",
        "_fetched_at": now,
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": url,
        "language": "ro",
        "doc_number": raw.get("doc_number", ""),
        "doc_category": tip,
        "doc_type_label": raw.get("doc_type_label", ""),
    }


def bootstrap_sample(sample_dir: Path, count: int = 100) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    empty_text = 0

    for raw in fetch_all(max_docs=count):
        record = normalize(raw)

        if not record["text"]:
            empty_text += 1
            print(f"  SKIP (no text): {record['_id']}", file=sys.stderr)
            continue

        samples.append(record)

        filename = re.sub(r"[^\w\-.]", "_", f"{record['_id']}.json")
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text'])} chars)", file=sys.stderr)

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

        by_cat = {}
        for s in samples:
            c = s.get("doc_category", "unknown")
            by_cat[c] = by_cat.get(c, 0) + 1
        print(f"\nBy category:", file=sys.stderr)
        for c, n in sorted(by_cat.items(), key=lambda x: -x[1]):
            print(f"  {c}: {n}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Moldova Constitutional Court fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Generate sample data only")
    parser.add_argument("--count", type=int, default=100,
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
        since_date = args.since
        for raw in fetch_all():
            if raw.get("date", "") >= since_date:
                record = normalize(raw)
                if record["text"]:
                    print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
