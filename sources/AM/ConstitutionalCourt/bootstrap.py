#!/usr/bin/env python3
"""
Armenian Constitutional Court (concourt.am) data fetcher.

Fetches decisions from the Constitutional Court of Armenia.
Full text is extracted from PDF documents.

Usage:
    python bootstrap.py bootstrap --sample  # Fetch ~12 sample records
    python bootstrap.py bootstrap           # Fetch all decisions
    python bootstrap.py updates --since 2024-01-01  # Fetch updates since date
"""

import argparse
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.concourt.am"
DECISIONS_PATH = "/decisions/cc-decision/"
RATE_LIMIT = 2  # seconds between requests
USER_AGENT = "LegalDataHunter/1.0 (research project; +https://github.com/ZachLaik/LegalDataHunter)"

# Armenian month names for date parsing
ARMENIAN_MONTHS = {
    "հdelays": 1, "փdelays": 2, "մdelays": 3, "delays": 4,
    "մdelays": 5, "հdelays": 6, "հdelays": 7, "delays": 8,
    "սdelays": 9, "հdelays": 10, "նdelays": 11, "delays": 12,
    # Common patterns
    "delays": 1, "delays": 2,
}

# Parse Armenian date like "17 դdelays delays 2024delays."
ARMENIAN_DATE_PATTERN = re.compile(r"(\d{1,2})\s+(\S+)\s+(\d{4})")


def get_session() -> requests.Session:
    """Create a requests session with proper headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    return session


def parse_armenian_date(date_str: str) -> Optional[str]:
    """Parse Armenian date string to ISO format."""
    if not date_str:
        return None
    
    # Try ISO format first
    iso_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", date_str)
    if iso_match:
        return f"{iso_match.group(1)}-{iso_match.group(2)}-{iso_match.group(3)}"
    
    # Try European format (DD.MM.YYYY or DD/MM/YYYY)
    eu_match = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", date_str)
    if eu_match:
        day, month, year = eu_match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    
    # Try Armenian format (day month year)
    arm_match = ARMENIAN_DATE_PATTERN.search(date_str)
    if arm_match:
        day = int(arm_match.group(1))
        month_name = arm_match.group(2).lower()
        year = int(arm_match.group(3))
        
        # Map Armenian month names to numbers
        month_map = {
            "հdelays": 1, "փetrul": 2, "մարdelays": 3, "delays": 4,
            "մdelays": 5, "հուdelays": 6, "հdelays": 7, "delays": 8,
            "սdelays": 9, "հdelays": 10, "նdelays": 11, "delays": 12,
        }
        
        # Try to match month by first few characters
        for pattern, num in month_map.items():
            if month_name.startswith(pattern[:3]):
                return f"{year}-{num:02d}-{day:02d}"
        
        # Fallback: try to extract month from the year context
        return f"{year}-01-{day:02d}"
    
    return None


def extract_decision_number(text: str) -> Optional[str]:
    """Extract decision number like DCC-1765, SDAO-1765, or SDO-1765."""
    # Try DCC format first (English)
    match = re.search(r"(DCC|SDV|SDAO)\s*[-–—]?\s*(\d+)", text, re.IGNORECASE)
    if match:
        prefix = match.group(1).upper()
        num = match.group(2)
        return f"{prefix}-{num}"

    # Try Armenian format: ՍԴՈ - 1765 (SDO in Armenian)
    # Armenian: Ս=S, Դ=D, Ո=O
    match = re.search(r"ՍԴՈ\s*[-–—]?\s*(\d+)", text)
    if match:
        num = match.group(1)
        return f"SDO-{num}"

    # Try generic number extraction if we see decision-like patterns
    # Look for format: just a number if text looks like a case reference
    match = re.search(r"[-–—]\s*(\d{4})", text)
    if match:
        num = match.group(1)
        return f"SDO-{num}"

    return None


def download_pdf_text(session: requests.Session, pdf_url: str) -> Optional[str]:
    """Download PDF and extract text content."""
    try:
        full_url = urljoin(BASE_URL, pdf_url)
        response = session.get(full_url, timeout=60)
        response.raise_for_status()
        
        # Save to temp file and extract text
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(response.content)
            temp_path = f.name
        
        try:
            doc = fitz.open(temp_path)
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()
            return text.strip()
        finally:
            os.unlink(temp_path)
            
    except Exception as e:
        print(f"Error downloading PDF {pdf_url}: {e}", file=sys.stderr)
        return None


def fetch_decisions_page(session: requests.Session, year: int = None, page: int = 1) -> tuple[list[dict], bool]:
    """
    Fetch a page of decisions and return parsed data.
    Returns (decisions, has_more_pages).
    """
    url = f"{BASE_URL}{DECISIONS_PATH}"
    params = {}
    if year:
        params["year"] = year
    if page > 1:
        params["page"] = page
    
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    decisions = []
    
    # Find all decision blocks
    for block in soup.find_all(class_="decisionsListBody"):
        divs = block.find_all("div", recursive=False)
        if len(divs) < 3:
            continue
        
        # Extract dates and case number from divs
        pub_date_text = divs[0].get_text(strip=True) if len(divs) > 0 else ""
        case_info_text = divs[1].get_text(strip=True) if len(divs) > 1 else ""
        title_text = divs[2].get_text(strip=True) if len(divs) > 2 else ""
        
        # Skip if this is a dissenting opinion or special opinion (no case number)
        if not case_info_text or "կdelays" in case_info_text.lower() or "special" in title_text.lower():
            # Check if it has a case number
            decision_num = extract_decision_number(case_info_text)
            if not decision_num:
                continue
        
        # Parse dates
        pub_date = parse_armenian_date(pub_date_text)
        
        # Extract decision number - usually in second div
        decision_num = extract_decision_number(case_info_text)
        
        # Also try to extract decision date from case_info_text
        decision_date = parse_armenian_date(case_info_text)
        if not decision_date:
            decision_date = pub_date
        
        # Find PDF link
        pdf_url = None
        for a in block.find_all("a"):
            href = a.get("href", "")
            if "/decision/" in href and href.endswith(".pdf"):
                pdf_url = href
                break
        
        if decision_num and pdf_url:
            decisions.append({
                "decision_number": decision_num,
                "title": title_text,
                "publication_date": pub_date,
                "decision_date": decision_date or pub_date,
                "pdf_url": pdf_url,
                "year": year,
            })
    
    # Check for pagination
    has_more = bool(soup.find("a", href=re.compile(rf"\?.*page={page + 1}")))
    
    return decisions, has_more


def normalize(raw: dict, text: str) -> dict:
    """Normalize raw decision data to standard schema."""
    now = datetime.now(timezone.utc).isoformat()
    
    return {
        "_id": f"AM/ConstitutionalCourt/{raw['decision_number']}",
        "_source": "AM/ConstitutionalCourt",
        "_type": "case_law",
        "_fetched_at": now,
        "decision_number": raw["decision_number"],
        "title": raw.get("title", ""),
        "date": raw.get("decision_date") or raw.get("publication_date"),
        "publication_date": raw.get("publication_date"),
        "text": text,
        "url": urljoin(BASE_URL, raw["pdf_url"]),
        "court": "Constitutional Court of Armenia",
        "jurisdiction": "AM",
        "language": "hy",  # Armenian
        "year": raw.get("year"),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """
    Fetch all Constitutional Court decisions.
    If sample=True, fetches only ~12 recent decisions.
    """
    session = get_session()
    
    # Get available years
    current_year = datetime.now().year
    years = range(current_year, 1999, -1)  # 2000-present in reverse
    
    if sample:
        # Only fetch from current and previous year for sample
        years = [current_year, current_year - 1]
    
    seen_ids = set()
    count = 0
    max_sample = 12
    
    for year in years:
        if sample and count >= max_sample:
            break
            
        page = 1
        while True:
            print(f"Fetching year {year}, page {page}...", file=sys.stderr)
            
            try:
                decisions, has_more = fetch_decisions_page(session, year=year, page=page)
            except Exception as e:
                print(f"Error fetching page: {e}", file=sys.stderr)
                break
            
            for decision in decisions:
                if sample and count >= max_sample:
                    break
                    
                dec_id = decision["decision_number"]
                if dec_id in seen_ids:
                    continue
                seen_ids.add(dec_id)
                
                # Download and extract PDF text
                time.sleep(RATE_LIMIT)
                text = download_pdf_text(session, decision["pdf_url"])
                
                if text and len(text) > 100:  # Ensure we have meaningful content
                    record = normalize(decision, text)
                    count += 1
                    print(f"  [{count}] {dec_id}: {len(text)} chars", file=sys.stderr)
                    yield record
                else:
                    print(f"  Skipping {dec_id}: no text extracted", file=sys.stderr)
            
            if sample and count >= max_sample:
                break
            
            if not has_more:
                break
            
            page += 1
            time.sleep(RATE_LIMIT)
        
        time.sleep(RATE_LIMIT)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch decisions modified since the given date."""
    since_date = datetime.fromisoformat(since.replace("Z", "+00:00"))
    
    for record in fetch_all(sample=False):
        rec_date_str = record.get("date") or record.get("publication_date")
        if rec_date_str:
            try:
                rec_date = datetime.fromisoformat(rec_date_str.replace("Z", "+00:00"))
                if rec_date >= since_date:
                    yield record
            except ValueError:
                # If date parsing fails, include the record to be safe
                yield record


def save_sample(records: list[dict], sample_dir: Path):
    """Save sample records to JSON files."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    
    for record in records:
        filename = record["_id"].replace("/", "_") + ".json"
        filepath = sample_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"Saved: {filepath}")


def main():
    parser = argparse.ArgumentParser(description="Fetch Armenian Constitutional Court decisions")
    subparsers = parser.add_subparsers(dest="command", required=True)
    
    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Fetch decisions")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    
    # Updates command
    updates_parser = subparsers.add_parser("updates", help="Fetch updates since date")
    updates_parser.add_argument("--since", required=True, help="ISO date (e.g., 2024-01-01)")
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    
    if args.command == "bootstrap":
        records = list(fetch_all(sample=args.sample))
        print(f"\nFetched {len(records)} records", file=sys.stderr)
        
        if args.sample:
            save_sample(records, sample_dir)
        else:
            # Output to stdout for piping
            for record in records:
                print(json.dumps(record, ensure_ascii=False))
                
    elif args.command == "updates":
        for record in fetch_updates(args.since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
