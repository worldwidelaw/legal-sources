#!/usr/bin/env python3
"""
SI/SupremeCourt Bootstrap
Slovenian Case Law Database (sodnapraksa.si)

Fetches court decisions from Slovenia's public case law database.

Databases:
- SOVS: Supreme Court (Vrhovno sodišče)
- IESP: Higher Courts (Višja sodišča)
- VDSS: Higher Labor and Social Court
- UPRS: Administrative Court
- SEU: Court of Justice of the EU

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 10-15 sample records
    python bootstrap.py bootstrap --full     # Fetch all records (NOT recommended)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from html import unescape
from urllib.parse import urljoin, urlencode, quote

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.sodnapraksa.si/"
RATE_LIMIT = 2  # seconds between requests

# Available databases
DATABASES = {
    "SOVS": "Supreme Court",
    "IESP": "Higher Courts",
    "VDSS": "Higher Labor and Social Court",
    "UPRS": "Administrative Court",
    "SEU": "Court of Justice of the EU",
}

# Source directory
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

# HTTP session with user agent
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/legal-data-hunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
})


def search_documents(database="SOVS", query="*", page=0, rows_per_page=10):
    """
    Search for documents in the specified database.

    Returns list of document IDs and metadata.
    """
    # Database parameter uses array notation: database[SOVS]=SOVS
    params = {
        "q": query,
        f"database[{database}]": database,
        "_submit": "išči",
        "rowsPerPage": rows_per_page,
        "page": page,
        "order": "date",
        "direction": "desc",
    }

    url = f"{BASE_URL}?{urlencode(params)}"
    print(f"Searching: {url}")

    response = session.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")

    # Extract total count
    num_hits = soup.find("span", id="num-hits")
    total = 0
    if num_hits:
        match = re.search(r"(\d+)", num_hits.text.replace(".", ""))
        if match:
            total = int(match.group(1))

    # Extract document links from results table
    results = []
    table = soup.find("table", id="results-table")
    if table:
        for row in table.find_all("tr"):
            link = row.find("a", href=re.compile(r"id=\d+"))
            if link:
                href = link.get("href", "")
                match = re.search(r"id=(\d+)", href)
                if match:
                    doc_id = match.group(1)
                    results.append({
                        "id": doc_id,
                        "title": link.text.strip(),
                    })

    return results, total


def fetch_document(doc_id, database="SOVS"):
    """
    Fetch a single document by ID.

    Returns dict with all extracted fields including full text.
    """
    params = {
        "q": "*",
        "database": database,
        "_submit": "išči",
        "rowsPerPage": "10",
        "page": "0",
        "id": doc_id,
    }

    url = f"{BASE_URL}?{urlencode(params)}"

    response = session.get(url, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")

    # Find document content container
    doc_content = soup.find("div", id="doc-content")
    if not doc_content:
        return None

    # Extract header info
    doc_head = doc_content.find("p", id="doc-head-right")
    decision_number = ""
    if doc_head:
        decision_number = doc_head.get_text(separator=" ", strip=True)

    # Extract metadata table
    meta_table = doc_content.find("table", id="doc-meta")
    metadata = {}
    if meta_table:
        for row in meta_table.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if th and td:
                key = th.text.strip().rstrip(":")
                value = td.text.strip()
                metadata[key] = value

    # Extract main content sections
    def get_section(header_text):
        """Find content following a specific h2 header."""
        for h2 in doc_content.find_all("h2"):
            if h2.text.strip() == header_text:
                # Get all siblings until next h2 or end
                content_parts = []
                for sibling in h2.find_next_siblings():
                    if sibling.name == "h2":
                        break
                    if sibling.name == "p":
                        text = sibling.get_text(separator=" ", strip=True)
                        if text:
                            content_parts.append(text)
                    elif sibling.name == "br":
                        continue
                    elif hasattr(sibling, "get_text"):
                        text = sibling.get_text(separator=" ", strip=True)
                        if text and sibling.name not in ["strong", "dl"]:
                            content_parts.append(text)
                return "\n\n".join(content_parts)
        return ""

    jedro = get_section("Jedro")
    izrek = get_section("Izrek")
    obrazlozitev = get_section("Obrazložitev")

    # Get legal references (Zveza)
    references = ""
    zveza_div = doc_content.find("div", id="doc-connection")
    if zveza_div:
        references = zveza_div.get_text(separator=" ", strip=True)

    # Get last modified date
    last_modified = ""
    mod_dl = doc_content.find("dl", id="doc-date-mod")
    if mod_dl:
        dd = mod_dl.find("dd")
        if dd:
            last_modified = dd.text.strip()

    # Parse date from metadata
    date = metadata.get("Datum odločbe", "")
    if date:
        # Convert from DD.MM.YYYY to YYYY-MM-DD
        try:
            parts = date.split(".")
            if len(parts) == 3:
                date = f"{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}"
        except Exception:
            pass

    return {
        "doc_id": doc_id,
        "decision_number": decision_number,
        "ecli": metadata.get("ECLI", ""),
        "evidence_number": metadata.get("Evidenčna številka", ""),
        "court": metadata.get("Sodišče", ""),
        "department": metadata.get("Oddelek", ""),
        "date": date,
        "legal_area": metadata.get("Področje", ""),
        "keywords": metadata.get("Institut", ""),
        "summary": jedro,
        "disposition": izrek,
        "reasoning": obrazlozitev,
        "references": references,
        "last_modified": last_modified,
        "database": database,
    }


def normalize(raw_data):
    """Transform raw document data into standard schema."""
    # Combine all text sections for full text
    text_parts = []
    if raw_data.get("summary"):
        text_parts.append(f"JEDRO (Summary):\n{raw_data['summary']}")
    if raw_data.get("disposition"):
        text_parts.append(f"IZREK (Disposition):\n{raw_data['disposition']}")
    if raw_data.get("reasoning"):
        text_parts.append(f"OBRAZLOŽITEV (Reasoning):\n{raw_data['reasoning']}")

    full_text = "\n\n".join(text_parts)

    # Clean HTML entities
    full_text = unescape(full_text)
    full_text = re.sub(r'\s+', ' ', full_text)
    full_text = full_text.replace(" \n", "\n").replace("\n ", "\n")

    return {
        "_id": f"SI/SupremeCourt/{raw_data['ecli'] or raw_data['doc_id']}",
        "_source": "SI/SupremeCourt",
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),

        # Core fields
        "title": raw_data["decision_number"],
        "text": full_text,
        "date": raw_data["date"] if raw_data["date"] else None,
        "url": f"{BASE_URL}?id={raw_data['doc_id']}",

        # Case law specific
        "ecli": raw_data.get("ecli", ""),
        "decision_number": raw_data.get("decision_number", ""),
        "evidence_number": raw_data.get("evidence_number", ""),
        "court": raw_data.get("court", ""),
        "department": raw_data.get("department", ""),
        "legal_area": raw_data.get("legal_area", ""),
        "keywords": raw_data.get("keywords", ""),

        # Content sections
        "summary": raw_data.get("summary", ""),
        "disposition": raw_data.get("disposition", ""),
        "reasoning": raw_data.get("reasoning", ""),
        "references": raw_data.get("references", ""),

        # Metadata
        "database": raw_data.get("database", ""),
        "last_modified": raw_data.get("last_modified", ""),
    }


def fetch_all(sample_mode=False, sample_size=100, databases=None):
    """
    Fetch all records or a sample.

    Args:
        sample_mode: If True, only fetch sample_size records
        sample_size: Number of records to fetch in sample mode (default: 100)
        databases: List of database codes to query (default: ["SOVS"])
    """
    if databases is None:
        databases = ["SOVS"]  # Focus on Supreme Court for main fetch

    # Checkpoint file for resumable fetching
    checkpoint_file = SOURCE_DIR / "checkpoint.json"

    rows_per_page = 50  # Max per page
    total_fetched = 0

    for database in databases:
        print(f"\n{'='*60}")
        print(f"Fetching from {database} ({DATABASES.get(database, 'Unknown')})")
        print("=" * 60)

        # Get first page to find total count
        results, total = search_documents(database=database, page=0, rows_per_page=rows_per_page)
        print(f"Total documents in {database}: {total:,}")

        if total == 0:
            print(f"No documents found in {database}, skipping...")
            continue

        if sample_mode:
            # For sample mode: fetch from multiple pages to show pagination works
            # Get records spread across archive (first page, middle, recent)
            pages_to_fetch = [0]  # Start with first page
            total_pages = (total // rows_per_page) + 1

            # Add middle page if there are enough pages
            if total_pages > 10:
                pages_to_fetch.append(total_pages // 2)

            target_per_db = sample_size // len(databases)
            records_per_page = (target_per_db // len(pages_to_fetch)) + 1

            for page_num in pages_to_fetch:
                print(f"\nFetching page {page_num + 1} of {total_pages}...")
                results, _ = search_documents(
                    database=database,
                    page=page_num,
                    rows_per_page=rows_per_page
                )

                for i, result in enumerate(results[:records_per_page]):
                    if total_fetched >= sample_size:
                        print(f"Reached sample limit of {sample_size}")
                        return

                    doc_id = result["id"]
                    print(f"[{total_fetched+1}/{sample_size}] Fetching {result['title']}...")

                    try:
                        raw_data = fetch_document(doc_id, database=database)
                        if raw_data:
                            record = normalize(raw_data)
                            text_len = len(record.get("text", ""))
                            print(f"  Full text: {text_len:,} chars")
                            total_fetched += 1
                            yield record
                        else:
                            print(f"  ERROR: Could not parse document")
                    except Exception as e:
                        print(f"  ERROR: {e}")

                    # Rate limiting
                    time.sleep(RATE_LIMIT)
        else:
            # Full mode: paginate through entire database
            # Load checkpoint if exists
            start_page = 0
            if checkpoint_file.exists():
                try:
                    with open(checkpoint_file, "r") as f:
                        checkpoint = json.load(f)
                        if checkpoint.get("database") == database:
                            start_page = checkpoint.get("page", 0)
                            print(f"Resuming from page {start_page}")
                except Exception:
                    pass

            total_pages = (total // rows_per_page) + 1
            print(f"Total pages to fetch: {total_pages}")

            for page_num in range(start_page, total_pages):
                print(f"\nFetching page {page_num + 1} of {total_pages}...")
                results, _ = search_documents(
                    database=database,
                    page=page_num,
                    rows_per_page=rows_per_page
                )

                if not results:
                    print(f"No results on page {page_num}, stopping pagination")
                    break

                for i, result in enumerate(results):
                    doc_id = result["id"]
                    print(f"[{total_fetched+1}] Fetching {result['title']}...")

                    try:
                        raw_data = fetch_document(doc_id, database=database)
                        if raw_data:
                            record = normalize(raw_data)
                            text_len = len(record.get("text", ""))
                            print(f"  Full text: {text_len:,} chars")
                            total_fetched += 1
                            yield record
                        else:
                            print(f"  ERROR: Could not parse document")
                    except Exception as e:
                        print(f"  ERROR: {e}")

                    # Rate limiting
                    time.sleep(RATE_LIMIT)

                # Save checkpoint after each page
                with open(checkpoint_file, "w") as f:
                    json.dump({
                        "database": database,
                        "page": page_num + 1,
                        "fetched": total_fetched,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }, f)

            # Clear checkpoint when done with database
            if checkpoint_file.exists():
                checkpoint_file.unlink()

    print(f"\nTotal records fetched: {total_fetched}")


def fetch_updates(since):
    """Fetch records modified since a given date."""
    # The search can be filtered by date, but for now just fetch recent
    since_date = datetime.fromisoformat(since.replace("Z", "+00:00"))

    for database in ["SOVS", "IESP", "UPRS"]:
        results, total = search_documents(
            database=database,
            page=0,
            rows_per_page=50,
        )

        for result in results:
            raw_data = fetch_document(result["id"], database=database)
            if raw_data:
                record = normalize(raw_data)

                # Check if modified after since date
                if raw_data.get("last_modified"):
                    try:
                        parts = raw_data["last_modified"].split(".")
                        if len(parts) == 3:
                            mod_date = datetime(
                                int(parts[2]), int(parts[1]), int(parts[0]),
                                tzinfo=timezone.utc
                            )
                            if mod_date >= since_date:
                                yield record
                    except Exception:
                        yield record  # Include if can't parse date
                else:
                    yield record

            time.sleep(RATE_LIMIT)


def bootstrap(sample=False):
    """Main bootstrap function."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = []
    text_lengths = []

    for record in fetch_all(sample_mode=sample, sample_size=100):
        records.append(record)

        # Track text lengths
        if record.get("text"):
            text_lengths.append(len(record["text"]))

        # Save individual record
        # Use ECLI or doc_id for filename
        safe_id = record.get("ecli", "").replace(":", "_").replace("/", "-")
        if not safe_id:
            safe_id = record["_id"].split("/")[-1]
        filename = f"{safe_id}.json"
        filepath = SAMPLE_DIR / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Print summary
    print("\n" + "=" * 60)
    print("BOOTSTRAP SUMMARY")
    print("=" * 60)
    print(f"Total records: {len(records)}")
    print(f"Records with full text: {len(text_lengths)}")
    if text_lengths:
        print(f"Average text length: {sum(text_lengths) // len(text_lengths):,} chars")
        print(f"Min text length: {min(text_lengths):,} chars")
        print(f"Max text length: {max(text_lengths):,} chars")

    # Validate minimum requirements
    if len(records) < 10:
        print("\nWARNING: Less than 10 records fetched!")
        return False

    if len(text_lengths) < 10:
        print("\nWARNING: Less than 10 records have full text!")
        return False

    print("\nValidation PASSED")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SI/SupremeCourt Bootstrap")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        success = bootstrap(sample=args.sample or not args.full)
        sys.exit(0 if success else 1)
