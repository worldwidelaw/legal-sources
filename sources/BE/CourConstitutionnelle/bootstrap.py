#!/usr/bin/env python3
"""
Belgian Constitutional Court (Cour constitutionnelle / Grondwettelijk Hof) Data Fetcher

Extracts Constitutional Court decisions from const-court.be.
- PDF downloads via predictable URL pattern
- Full text extraction using pdfplumber
- Coverage from 1985 to present

Data source: https://www.const-court.be
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
from typing import Iterator, Optional

import requests

# Try to import pdfplumber for PDF text extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    print("Warning: pdfplumber not installed. Install with: pip install pdfplumber")

SOURCE_ID = "BE/CourConstitutionnelle"
BASE_URL = "https://www.const-court.be"
PDF_BASE_FR = "https://fr.const-court.be/public/f"
PDF_BASE_NL = "https://nl.const-court.be/public/n"

HEADERS = {
    "User-Agent": "World Wide Law/1.0 (EU Legal Research)",
    "Accept": "application/pdf,*/*",
    "Accept-Language": "fr-BE,fr;q=0.9,nl-BE;q=0.8,en;q=0.7",
}

# Court decisions start from 1985
START_YEAR = 1985


def extract_pdf_text(pdf_content: bytes) -> str:
    """Extract text from PDF content using pdfplumber."""
    if not HAS_PDFPLUMBER:
        return ""
    
    import io
    text_parts = []
    
    try:
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
    except Exception as e:
        print(f"Error extracting PDF text: {e}")
        return ""
    
    return "\n\n".join(text_parts)


def build_pdf_url(year: int, nr: int, lang: str = "f") -> str:
    """Build PDF URL for a specific decision."""
    if lang == "f":
        return f"{PDF_BASE_FR}/{year}/{year}-{nr:03d}f.pdf"
    else:
        return f"{PDF_BASE_NL}/{year}/{year}-{nr:03d}n.pdf"


def check_decision_exists(session: requests.Session, year: int, nr: int) -> bool:
    """Check if a decision exists using HEAD request."""
    url = build_pdf_url(year, nr, "f")
    try:
        response = session.head(url, headers=HEADERS, timeout=10, allow_redirects=True)
        return response.status_code == 200
    except:
        return False


def fetch_decision(session: requests.Session, year: int, nr: int) -> Optional[dict]:
    """Fetch a single decision."""
    url_fr = build_pdf_url(year, nr, "f")
    
    try:
        response = session.get(url_fr, headers=HEADERS, timeout=60)
        if response.status_code != 200:
            return None
        
        # Check if it's actually a PDF
        content_type = response.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not response.content[:4] == b"%PDF":
            return None
        
        # Extract text
        full_text = extract_pdf_text(response.content)
        if not full_text or len(full_text.strip()) < 100:
            return None
        
        # Build record
        ecli = f"ECLI:BE:GHCC:{year}:{nr}"
        doc_id = f"BE_CONSTCOURT_{year}_{nr:03d}"
        
        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "ecli": ecli,
            "arret_nr": f"{year}/{nr}",
            "year": year,
            "number": nr,
            "title": f"Arrêt {nr}/{year} - Constitutional Court of Belgium",
            "date": f"{year}-01-01",  # Exact date not in URL, use year
            "court": "Belgian Constitutional Court",
            "court_type": "constitutional",
            "language": "fr",
            "url": url_fr,
            "text": full_text,
            "text_length": len(full_text),
        }
    
    except Exception as e:
        print(f"Error fetching decision {year}/{nr}: {e}")
        return None


def find_max_decision_for_year(session: requests.Session, year: int) -> int:
    """Find the maximum decision number for a given year using binary search."""
    low, high = 1, 300
    max_found = 0
    
    while low <= high:
        mid = (low + high) // 2
        if check_decision_exists(session, year, mid):
            max_found = mid
            low = mid + 1
        else:
            high = mid - 1
        time.sleep(0.3)
    
    return max_found


def fetch_all(
    start_year: int = None,
    end_year: int = None,
    sample: bool = False,
    limit: int = None
) -> Iterator[dict]:
    """
    Fetch all Constitutional Court decisions.
    
    Args:
        start_year: Starting year (default: current year)
        end_year: Ending year (default: START_YEAR)
        sample: If True, fetch only a sample
        limit: Maximum number of records to fetch
    """
    session = requests.Session()
    current_year = datetime.now().year
    
    if start_year is None:
        start_year = current_year
    if end_year is None:
        end_year = START_YEAR if not sample else current_year - 3
    
    count = 0
    target_limit = limit if limit else (100 if sample else 100000)
    
    for year in range(start_year, end_year - 1, -1):
        if count >= target_limit:
            break
        
        print(f"Processing year {year}...")
        
        # Find max decision for this year
        max_nr = find_max_decision_for_year(session, year)
        if max_nr == 0:
            print(f"  No decisions found for {year}")
            continue
        
        print(f"  Found {max_nr} decisions for {year}")
        
        # Fetch decisions from max down to 1
        for nr in range(max_nr, 0, -1):
            if count >= target_limit:
                break
            
            record = fetch_decision(session, year, nr)
            if record:
                count += 1
                print(f"  [{count}] {record['arret_nr']} - {record['text_length']} chars")
                yield record
                time.sleep(1.0)  # Rate limiting
            else:
                time.sleep(0.5)


def fetch_updates(since: datetime) -> Iterator[dict]:
    """Fetch decisions added since a specific date."""
    current_year = datetime.now().year
    since_year = since.year
    
    yield from fetch_all(start_year=current_year, end_year=since_year)


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema. Data is already normalized in fetch."""
    return raw


def bootstrap_sample(sample_dir: Path, limit: int = None, start_year: int = None, end_year: int = None):
    """Create sample data for testing."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    # Determine if this is a sample run based on limit
    is_sample = limit is not None and limit <= 100

    records = []
    for record in fetch_all(sample=is_sample, limit=limit, start_year=start_year, end_year=end_year):
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
    parser = argparse.ArgumentParser(description="Belgian Constitutional Court Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "sample", "test"],
                       help="Command to execute")
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only (100 records)")
    parser.add_argument("--limit", type=int, default=None, help="Maximum records to fetch")
    parser.add_argument("--start-year", type=int, help="Start year")
    parser.add_argument("--end-year", type=int, help="End year")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "test":
        print("Testing PDF access...")
        session = requests.Session()

        # Test with recent decisions
        year = datetime.now().year
        for nr in range(1, 30):
            exists = check_decision_exists(session, year, nr)
            print(f"  {year}/{nr}: {'OK' if exists else 'NOT FOUND'}")
            if exists:
                record = fetch_decision(session, year, nr)
                if record:
                    print(f"    Full text: {record['text_length']} chars")
                    print(f"    First 200 chars: {record['text'][:200]}...")
                break

    elif args.command == "sample" or args.sample:
        # Sample mode: fetch limited records (default 100)
        limit = args.limit if args.limit else 100
        bootstrap_sample(sample_dir, limit=limit, start_year=args.start_year, end_year=args.end_year)

    elif args.command == "bootstrap":
        # Full bootstrap: fetch all records (no limit by default)
        # When running full bootstrap, we still save to sample dir but with no limit
        limit = args.limit if args.limit else None
        bootstrap_sample(sample_dir, limit=limit, start_year=args.start_year, end_year=args.end_year)


if __name__ == "__main__":
    main()
