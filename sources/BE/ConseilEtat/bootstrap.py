#!/usr/bin/env python3
"""
Belgian Council of State (Conseil d'État / Raad van State) Data Fetcher

Extracts administrative court decisions from raadvst-consetat.be.
- PDF downloads via arr.php endpoint
- Full text extraction using pdfplumber
- Coverage from 1994 to present
- ~5,000 decisions per year (excluding aliens cases)

Data source: https://www.raadvst-consetat.be
License: Open Government Data
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional, List
from html import unescape

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

SOURCE_ID = "BE/ConseilEtat"
BASE_URL = "https://www.raadvst-consetat.be"
PDF_ENDPOINT = f"{BASE_URL}/arr.php"
RECENT_DECISIONS_URL = f"{BASE_URL}/?lang=fr&page=lastmonth_{{mm}}"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research)",
    "Accept": "application/pdf,text/html,*/*",
    "Accept-Language": "fr-BE,fr;q=0.9,nl-BE;q=0.8,en;q=0.7",
}


def fetch_recent_decision_numbers(session: requests.Session, months: int = 3) -> List[int]:
    """Fetch decision numbers from recent months pages."""
    decision_numbers = []
    current_month = datetime.now().month
    
    for i in range(months):
        month = current_month - i
        if month <= 0:
            month += 12
        month_str = f"{month:02d}"
        
        url = RECENT_DECISIONS_URL.format(mm=month_str)
        try:
            response = session.get(url, headers=HEADERS, timeout=30)
            if response.status_code != 200:
                continue
            
            # Parse HTML to find decision links (arr.php?nr=NNNNNN)
            pattern = r'arr\.php\?nr=(\d+)'
            matches = re.findall(pattern, response.text)
            for m in matches:
                nr = int(m)
                if nr not in decision_numbers:
                    decision_numbers.append(nr)
        except Exception as e:
            print(f"Error fetching month {month_str}: {e}")
            continue
    
    return sorted(decision_numbers, reverse=True)


def fetch_decision(session: requests.Session, nr: int, lang: str = "fr") -> Optional[dict]:
    """Fetch a single decision by number."""
    url = f"{PDF_ENDPOINT}?nr={nr}&l={lang}"
    
    try:
        response = session.get(url, headers=HEADERS, timeout=60)
        if response.status_code != 200:
            return None
        
        # Check if it's actually a PDF
        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not response.content[:4] == b"%PDF":
            return None
        
        # Extract text via centralized PDF extractor
        source_id = f"BE_CONSETAT_{nr}"
        full_text = extract_pdf_markdown(
            source=SOURCE_ID, source_id=source_id,
            pdf_bytes=response.content, table="case_law",
        )
        if not full_text or len(full_text.strip()) < 100:
            return None

        # Try to extract date and case info from PDF text
        date_match = re.search(r'(\d{1,2})\s+(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s+(\d{4})', full_text.lower())
        date_iso = None
        year = None
        if date_match:
            day = int(date_match.group(1))
            months_fr = {'janvier':1, 'février':2, 'mars':3, 'avril':4, 'mai':5, 'juin':6,
                        'juillet':7, 'août':8, 'septembre':9, 'octobre':10, 'novembre':11, 'décembre':12}
            month = months_fr.get(date_match.group(2), 1)
            year = int(date_match.group(3))
            date_iso = f"{year:04d}-{month:02d}-{day:02d}"
        
        if not year:
            # Estimate year from decision number range
            if nr > 260000:
                year = 2026
            elif nr > 255000:
                year = 2025
            elif nr > 250000:
                year = 2024
            elif nr > 245000:
                year = 2023
            else:
                year = 2022
        
        # Extract subject from first lines
        lines = full_text.split('\n')[:10]
        subject = None
        for line in lines:
            if 'affaire' in line.lower() or 'requête' in line.lower():
                subject = line.strip()[:200]
                break
        
        return {
            "_id": source_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "arret_nr": str(nr),
            "year": year,
            "title": f"Arrêt n° {nr} - Conseil d'État",
            "date": date_iso,
            "court": "Belgian Council of State",
            "court_type": "administrative",
            "language": lang,
            "subject": subject,
            "url": url,
            "text": full_text,
            "text_length": len(full_text),
        }
    
    except Exception as e:
        print(f"Error fetching decision {nr}: {e}")
        return None


def fetch_all(
    start_nr: int = None,
    end_nr: int = None,
    sample: bool = False,
    limit: int = None
) -> Iterator[dict]:
    """
    Fetch Council of State decisions.
    
    Args:
        start_nr: Starting decision number (highest)
        end_nr: Ending decision number (lowest)
        sample: If True, fetch only a sample
        limit: Maximum number of records to fetch
    """
    session = requests.Session()
    
    # First, discover recent decision numbers
    print("Discovering recent decisions...")
    recent_nrs = fetch_recent_decision_numbers(session, months=3)
    
    if recent_nrs:
        print(f"Found {len(recent_nrs)} recent decisions")
        max_nr = max(recent_nrs)
        print(f"Highest decision number: {max_nr}")
    else:
        # Fallback: estimate current range
        max_nr = 265600 if datetime.now().year >= 2026 else 260000
        print(f"No recent decisions found, estimating max: {max_nr}")
    
    if start_nr is None:
        start_nr = max_nr
    if end_nr is None:
        end_nr = start_nr - 500 if sample else 200000  # Go back far enough
    
    count = 0
    target_limit = limit if limit else (100 if sample else 100000)
    consecutive_failures = 0
    
    for nr in range(start_nr, end_nr, -1):
        if count >= target_limit:
            break
        
        if consecutive_failures > 20:
            print(f"Too many consecutive failures at {nr}, stopping")
            break
        
        record = fetch_decision(session, nr)
        if record:
            count += 1
            consecutive_failures = 0
            print(f"  [{count}] {record['arret_nr']} - {record['text_length']} chars")
            yield record
            time.sleep(1.0)  # Rate limiting
        else:
            consecutive_failures += 1
            time.sleep(0.3)


def fetch_updates(since: datetime) -> Iterator[dict]:
    """Fetch decisions added since a specific date."""
    # Use recent months discovery
    yield from fetch_all(sample=False)


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema. Data is already normalized in fetch."""
    return raw


def bootstrap_sample(sample_dir: Path, limit: int = 100):
    """Create sample data for testing."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    records = []
    for record in fetch_all(sample=True, limit=limit):
        records.append(record)
        
        # Save individual file
        filename = f"{record['_id']}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
    
    # Save combined file
    if records:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        
        # Print statistics
        total_chars = sum(r.get("text_length", 0) for r in records)
        avg_chars = total_chars // len(records) if records else 0
        print(f"\nSaved {len(records)} samples to {sample_dir}")
        print(f"Average text length: {avg_chars} chars")


def main():
    parser = argparse.ArgumentParser(description="Belgian Council of State Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "sample", "test"], 
                       help="Command to execute")
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--limit", type=int, default=100, help="Maximum records to fetch")
    parser.add_argument("--start-nr", type=int, help="Start decision number")
    parser.add_argument("--end-nr", type=int, help="End decision number")
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    
    if args.command == "test":
        print("Testing PDF access...")
        session = requests.Session()
        
        # Discover recent decisions
        recent = fetch_recent_decision_numbers(session, months=1)
        if recent:
            print(f"Found {len(recent)} recent decisions")
            nr = recent[0]
            print(f"Testing decision {nr}...")
            record = fetch_decision(session, nr)
            if record:
                print(f"  Full text: {record['text_length']} chars")
                print(f"  Date: {record.get('date', 'Unknown')}")
                print(f"  First 200 chars: {record['text'][:200]}...")
    
    elif args.command == "sample" or args.sample:
        bootstrap_sample(sample_dir, limit=args.limit)
    
    elif args.command == "bootstrap":
        bootstrap_sample(sample_dir, limit=args.limit)


if __name__ == "__main__":
    main()
