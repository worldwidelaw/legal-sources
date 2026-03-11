#!/usr/bin/env python3
"""
Montenegro Supreme Court (Vrhovni sud) - Case Law Fetcher

API: https://sudovi.me/api
- POST /search/decisions - Search for decisions
- GET /decision/{id} - Get single decision with full text

Total available: ~48,700+ Supreme Court decisions
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Configuration
API_BASE = "https://sudovi.me/api"
COURT_CODE = "vrhs"  # Supreme Court code
SOURCE_ID = "ME/SupremeCourt"
SAMPLE_DIR = Path(__file__).parent / "sample"
DELAY = 0.5  # seconds between requests


def strip_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    soup = BeautifulSoup(html_text, "html.parser")
    # Get text, preserving some structure with newlines
    text = soup.get_text(separator="\n", strip=True)
    # Clean up multiple newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def search_decisions(court_code: str, start: int = 0, rows: int = 100) -> dict:
    """Search for decisions from the Supreme Court."""
    url = f"{API_BASE}/search/decisions"
    payload = {
        "courtCode": court_code,
        "start": start,
        "rows": rows
    }
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    
    response = requests.post(url, json=payload, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def get_decision(dbid: int) -> dict:
    """Get a single decision with full text."""
    url = f"{API_BASE}/decision/{dbid}"
    headers = {
        "Accept": "application/json",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def normalize(raw: dict, detail: dict = None) -> dict:
    """Normalize raw decision data into standard schema."""
    now = datetime.now(timezone.utc).isoformat()
    
    # Parse date
    date_str = raw.get("datumVijecanja") or (detail.get("datum_vijecanja") if detail else None)
    if date_str:
        try:
            # Handle ISO format with Z suffix
            date_str = date_str.replace("Z", "+00:00")
            dt = datetime.fromisoformat(date_str)
            date_iso = dt.strftime("%Y-%m-%d")
        except:
            date_iso = None
    else:
        date_iso = None
    
    # Build case number from components
    sifra = raw.get("sifraPredmeta", "")
    broj = raw.get("upisnikBroj", "")
    godina = raw.get("upisnikGodina", "")
    case_number = f"{sifra} {broj}/{godina}".strip()
    
    # Get full text from detail if available
    full_text = ""
    if detail and detail.get("tekst"):
        full_text = strip_html(detail["tekst"])
    
    # Build title
    court_name = raw.get("courtName", "VRHOVNI SUD CG")
    decision_type = raw.get("vrstaOdluke", "Odluka")
    title = f"{court_name} - {decision_type} {case_number}"
    
    return {
        "_id": f"ME-VRHS-{raw.get('dbid')}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": now,
        "title": title,
        "text": full_text,
        "date": date_iso,
        "url": f"https://sudovi.me/vrhs/odluka/{raw.get('dbid')}",
        "case_number": case_number,
        "court": court_name,
        "court_code": COURT_CODE,
        "department": raw.get("odjeljenjeName", ""),
        "decision_type": decision_type,
        "case_type": raw.get("vrstaPredmeta", ""),
        "dbid": raw.get("dbid"),
        "year": godina,
    }


def fetch_all(limit: int = None):
    """Fetch all decisions with full text."""
    print(f"Fetching decisions from Montenegro Supreme Court...")
    
    # First, get total count
    result = search_decisions(COURT_CODE, start=0, rows=1)
    total = result.get("numFound", 0)
    print(f"Total decisions available: {total}")
    
    if limit:
        total = min(total, limit)
        print(f"Limiting to {total} records")
    
    fetched = 0
    batch_size = 100
    
    while fetched < total:
        result = search_decisions(COURT_CODE, start=fetched, rows=batch_size)
        docs = result.get("docs", [])
        
        if not docs:
            break
        
        for doc in docs:
            if limit and fetched >= limit:
                break
            
            dbid = doc.get("dbid")
            if not dbid:
                continue
            
            # Fetch full text
            time.sleep(DELAY)
            try:
                detail = get_decision(dbid)
            except Exception as e:
                print(f"Error fetching decision {dbid}: {e}")
                detail = None
            
            record = normalize(doc, detail)
            
            if record["text"]:
                yield record
                fetched += 1
                
                if fetched % 10 == 0:
                    print(f"Fetched {fetched}/{total} decisions...")
            else:
                print(f"Warning: No text for decision {dbid}")
        
        time.sleep(DELAY)
    
    print(f"Total fetched: {fetched}")


def fetch_updates(since: str):
    """Fetch decisions modified since a given date."""
    # The API doesn't support date filtering directly,
    # so we fetch recent decisions and filter client-side
    print(f"Fetching decisions since {since}...")
    
    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    
    for record in fetch_all(limit=1000):  # Check last 1000
        record_date = record.get("date")
        if record_date:
            try:
                rec_dt = datetime.fromisoformat(record_date)
                if rec_dt >= since_dt.date() if hasattr(since_dt, 'date') else since_dt:
                    yield record
            except:
                yield record
        else:
            yield record


def bootstrap_sample(count: int = 12):
    """Fetch sample records for testing."""
    print(f"Fetching {count} sample decisions...")
    
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    
    saved = 0
    total_chars = 0
    
    for record in fetch_all(limit=count):
        if not record["text"]:
            continue
        
        # Save to sample directory
        filename = SAMPLE_DIR / f"{record['_id']}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        
        text_len = len(record["text"])
        total_chars += text_len
        saved += 1
        
        print(f"  Saved: {record['_id']} ({text_len} chars)")
        
        if saved >= count:
            break
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"Sample Bootstrap Complete")
    print(f"{'='*50}")
    print(f"Records saved: {saved}")
    print(f"Total characters: {total_chars:,}")
    print(f"Average chars/record: {total_chars // saved if saved else 0:,}")
    print(f"Sample directory: {SAMPLE_DIR}")
    
    return saved


def main():
    parser = argparse.ArgumentParser(
        description="Montenegro Supreme Court Case Law Fetcher"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Bootstrap command
    bootstrap_parser = subparsers.add_parser(
        "bootstrap", help="Fetch initial sample data"
    )
    bootstrap_parser.add_argument(
        "--sample", action="store_true",
        help="Fetch sample records only (12 records)"
    )
    bootstrap_parser.add_argument(
        "--count", type=int, default=12,
        help="Number of sample records to fetch"
    )
    
    # Fetch command
    fetch_parser = subparsers.add_parser(
        "fetch", help="Fetch all decisions"
    )
    fetch_parser.add_argument(
        "--limit", type=int,
        help="Limit number of records to fetch"
    )
    
    # Updates command
    updates_parser = subparsers.add_parser(
        "updates", help="Fetch recent updates"
    )
    updates_parser.add_argument(
        "--since", required=True,
        help="Fetch records since this date (ISO format)"
    )
    
    args = parser.parse_args()
    
    if args.command == "bootstrap":
        if args.sample:
            return bootstrap_sample(args.count)
        else:
            count = 0
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))
                count += 1
            return count
    
    elif args.command == "fetch":
        count = 0
        for record in fetch_all(limit=args.limit):
            print(json.dumps(record, ensure_ascii=False))
            count += 1
        return count
    
    elif args.command == "updates":
        count = 0
        for record in fetch_updates(args.since):
            print(json.dumps(record, ensure_ascii=False))
            count += 1
        return count


if __name__ == "__main__":
    sys.exit(main() or 0)
