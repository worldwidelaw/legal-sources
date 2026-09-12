#!/usr/bin/env python3
"""
HT/LeMoniteur-dLOC — Haiti Official Gazette via UFDC Patron API

Fetches Le Moniteur (journal officiel de la République d'Haïti) from the
University of Florida Digital Collections API. Collection BibID: AA00098215.
3,421 issues from 1867+ with OCR full text.
"""

import json
import os
import sys
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERROR: requests library required. Install with: pip install requests")
    sys.exit(1)

BASE_URL = "https://api.patron.uflib.ufl.edu"
BIBID = "AA00098215"
SOURCE_ID = "HT/LeMoniteur-dLOC"
DELAY = 0.5  # seconds between requests


def get_all_vids():
    """Extract all VIDs from the serial hierarchy."""
    url = f"{BASE_URL}/serialhierarchy?bibid={BIBID}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    vids = []

    def extract(node):
        if isinstance(node, list):
            for item in node:
                if isinstance(item, dict):
                    if "vid" in item:
                        vids.append({
                            "vid": item["vid"],
                            "text": item.get("text", ""),
                            "item_title": item.get("item_title", "")
                        })
                    if "values" in item:
                        extract(item["values"])

    extract(data)
    return vids


def fetch_citation(vid):
    """Fetch citation metadata for a specific issue."""
    url = f"{BASE_URL}/{BIBID}/{vid}/citation"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_pagetext(vid):
    """Fetch OCR text for all pages of an issue."""
    url = f"{BASE_URL}/pagetext?bibid={BIBID}&vid={vid}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    hits = data.get("hits", [])
    pages = []
    for hit in sorted(hits, key=lambda h: h.get("pageorder", 0)):
        text = hit.get("pagetext", "")
        if text:
            pages.append(text)
    return "\n\n".join(pages)


def normalize(vid_info, citation, full_text):
    """Normalize a record into the standard schema."""
    vid = vid_info["vid"]
    issue_id = f"{BIBID}_{vid}"

    # Extract date from citation
    date = citation.get("conv_date")
    if not date:
        pub_dates = citation.get("PublicationDate", [])
        if pub_dates:
            date = pub_dates[0] if isinstance(pub_dates, list) else pub_dates

    title = citation.get("Title", vid_info.get("item_title", "Le Moniteur"))
    date_label = vid_info.get("text", "")
    if date_label:
        title = f"{title} — {date_label}"

    publishers = citation.get("Publisher", [])
    publisher = "; ".join(publishers) if isinstance(publishers, list) else str(publishers)

    page_count = citation.get("pageCount", 0)
    language = citation.get("Language", "French")

    return {
        "_id": issue_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": f"https://ufdc.ufl.edu/{BIBID}/{vid}",
        "publisher": publisher,
        "page_count": page_count,
        "language": language,
    }


def fetch_all():
    """Yield all normalized records."""
    vids = get_all_vids()
    print(f"Found {len(vids)} issues in collection {BIBID}")

    for i, vid_info in enumerate(vids):
        vid = vid_info["vid"]
        try:
            time.sleep(DELAY)
            citation = fetch_citation(vid)
            time.sleep(DELAY)
            full_text = fetch_pagetext(vid)

            if not full_text or len(full_text.strip()) < 100:
                print(f"  [{i+1}/{len(vids)}] VID {vid}: insufficient text, skipping")
                continue

            record = normalize(vid_info, citation, full_text)
            print(f"  [{i+1}/{len(vids)}] VID {vid}: {len(full_text)} chars")
            yield record

        except Exception as e:
            print(f"  [{i+1}/{len(vids)}] VID {vid}: ERROR {e}")
            continue


def fetch_updates(since):
    """Fetch documents added/modified since a date. Not supported — full refetch only."""
    return fetch_all()


def bootstrap_sample(n=15):
    """Fetch a sample of records for testing."""
    vids = get_all_vids()
    # Sample from different time periods
    step = max(1, len(vids) // n)
    sample_vids = vids[1::step][:n]  # skip first (often empty), take evenly spaced

    records = []
    for i, vid_info in enumerate(sample_vids):
        vid = vid_info["vid"]
        try:
            time.sleep(DELAY)
            citation = fetch_citation(vid)
            time.sleep(DELAY)
            full_text = fetch_pagetext(vid)

            if not full_text or len(full_text.strip()) < 100:
                print(f"  [{i+1}/{n}] VID {vid}: insufficient text, skipping")
                continue

            record = normalize(vid_info, citation, full_text)
            records.append(record)
            print(f"  [{i+1}/{n}] VID {vid}: {len(full_text)} chars, date={record.get('date')}")

        except Exception as e:
            print(f"  [{i+1}/{n}] VID {vid}: ERROR {e}")
            continue

    return records


def main():
    parser = argparse.ArgumentParser(description="HT/LeMoniteur-dLOC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "sample", "count"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Run in sample mode (15 records)")
    parser.add_argument("--limit", type=int, default=None,
                        help="Limit number of records")
    args = parser.parse_args()

    if args.command == "count":
        vids = get_all_vids()
        print(f"Total issues: {len(vids)}")
        return

    if args.command == "sample" or args.sample:
        records = bootstrap_sample()
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        for record in records:
            fname = f"{record['_id'].replace('/', '_')}.json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"\nSaved {len(records)} sample records to {sample_dir}/")
        # Validation
        texts = [r.get("text", "") for r in records]
        print(f"Text field lengths: min={min(len(t) for t in texts)}, max={max(len(t) for t in texts)}, avg={sum(len(t) for t in texts)//len(texts)}")
        return

    # Full bootstrap
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in fetch_all():
        fname = f"{record['_id'].replace('/', '_')}.json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        if args.limit and count >= args.limit:
            break

    print(f"\nBootstrap complete: {count} records saved")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    # Handle "bootstrap --sample" shorthand
    if len(sys.argv) >= 2 and sys.argv[1] == "bootstrap" and "--sample" in sys.argv:
        sys.argv.remove("--sample")
        sys.argv[1] = "sample"
    main()
