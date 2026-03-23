#!/usr/bin/env python3
"""
BE/VlaamseCodex - Flemish Legislation Database
===============================================
Fetches Flemish laws and regulations from the Vlaamse Codex Open Data API.

API Documentation: https://codex.opendata.api.vlaanderen.be/docs/
Total documents: ~39,000 legislation documents
Full text available: Yes (via VolledigDocument endpoint)
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests
import yaml

# Configuration
API_BASE = "https://codex.opendata.api.vlaanderen.be"
DOCUMENTS_ENDPOINT = f"{API_BASE}/api/WetgevingDocument"
FULL_DOC_ENDPOINT = f"{API_BASE}/api/v2/WetgevingDocument/{{doc_id}}/VolledigDocument"
PAGE_SIZE = 100
DELAY_SECONDS = 1.0
TIMEOUT = 30

# Directory setup
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"


def get_session() -> requests.Session:
    """Create a session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "User-Agent": "LegalDataHunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)"
    })
    return session


def fetch_document_list(
    session: requests.Session,
    skip: int = 0,
    take: int = PAGE_SIZE
) -> Dict[str, Any]:
    """Fetch a page of documents from the API."""
    params = {
        "skip": skip,
        "take": take
    }
    resp = session.get(DOCUMENTS_ENDPOINT, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def fetch_full_document(
    session: requests.Session,
    doc_id: int
) -> Optional[Dict[str, Any]]:
    """Fetch full document content including article text."""
    url = FULL_DOC_ENDPOINT.format(doc_id=doc_id)
    try:
        resp = session.get(url, timeout=TIMEOUT)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        print(f"  Warning: Failed to fetch document {doc_id}: {e}", file=sys.stderr)
        return None


def extract_text_from_full_document(full_doc: Dict[str, Any]) -> str:
    """Extract all article text from a full document response."""
    text_parts = []

    inhoud = full_doc.get("Inhoud", {})

    # Extract article versions with text
    artikel_versies = inhoud.get("ArtikelVersies", []) or []
    for av in artikel_versies:
        artikel_versie = av.get("ArtikelVersie", {})
        if artikel_versie:
            tekst = artikel_versie.get("Tekst", "")
            if tekst:
                artikel_nummer = artikel_versie.get("ArtikelNummer", "")
                if artikel_nummer:
                    text_parts.append(f"Artikel {artikel_nummer}\n{tekst}")
                else:
                    text_parts.append(tekst)

    # Also check for future article versions
    toekomstige = inhoud.get("ToekomstigeArtikelVersies", []) or []
    for av in toekomstige:
        artikel_versie = av.get("ArtikelVersie", {})
        if artikel_versie:
            tekst = artikel_versie.get("Tekst", "")
            if tekst:
                text_parts.append(tekst)

    return "\n\n".join(text_parts)


def clean_html(text: str) -> str:
    """Remove HTML tags from text."""
    if not text:
        return ""
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode common entities
    text = text.replace('&nbsp;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&quot;', '"')
    text = text.replace('&#39;', "'")
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def normalize(raw: Dict[str, Any], full_doc: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Transform raw API response to standard schema."""
    # Extract full text from the document
    text = ""
    if full_doc:
        text = extract_text_from_full_document(full_doc)
        text = clean_html(text)

    # Build the normalized record
    doc_info = full_doc.get("Document", raw) if full_doc else raw

    # Parse date
    datum = doc_info.get("Datum", "")
    date_str = None
    if datum:
        try:
            # Handle ISO format with timezone
            dt = datetime.fromisoformat(datum.replace('Z', '+00:00'))
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            date_str = datum[:10] if len(datum) >= 10 else datum

    # Build URL to the document
    doc_id = raw.get("Id") or doc_info.get("Id")
    url = f"https://codex.vlaanderen.be/Zoeken/Document.aspx?DID={doc_id}"

    numac = doc_info.get("Numac")

    return {
        "_id": f"BE/VlaamseCodex/{doc_id}",
        "_source": "BE/VlaamseCodex",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "id": doc_id,
        "numac": numac,
        "title": doc_info.get("Opschrift", ""),
        "text": text,
        "date": date_str,
        "document_type": doc_info.get("WetgevingDocumentType", ""),
        "bs_datum": doc_info.get("BSDatum", "")[:10] if doc_info.get("BSDatum") else None,
        "bs_pagina": doc_info.get("BSPagina"),
        "commentaar": clean_html(doc_info.get("Commentaar", "") or ""),
        "is_federal": doc_info.get("IsFederaal", False),
        "url": url,
        "heeft_inhoud": raw.get("HeeftInhoud", False)
    }


def fetch_all() -> Generator[Dict[str, Any], None, None]:
    """Yield all documents with full text."""
    session = get_session()

    # Get total count
    first_page = fetch_document_list(session, skip=0, take=1)
    total = first_page.get("TotaalAantal", 0)
    print(f"Total documents in API: {total}")

    skip = 0
    fetched = 0

    while skip < total:
        print(f"Fetching documents {skip} to {skip + PAGE_SIZE}...")
        page = fetch_document_list(session, skip=skip, take=PAGE_SIZE)
        results = page.get("ResultatenLijst", [])

        if not results:
            break

        for doc in results:
            doc_id = doc.get("Id")
            has_content = doc.get("HeeftInhoud", False)

            # Only fetch full document if it has content
            full_doc = None
            if has_content:
                time.sleep(DELAY_SECONDS)
                full_doc = fetch_full_document(session, doc_id)

            record = normalize(doc, full_doc)
            yield record
            fetched += 1

            if fetched % 100 == 0:
                print(f"  Processed {fetched} documents...")

        skip += PAGE_SIZE
        time.sleep(DELAY_SECONDS)

    print(f"Total documents fetched: {fetched}")


def fetch_updates(since: str) -> Generator[Dict[str, Any], None, None]:
    """Fetch documents updated since a date.

    Note: The API doesn't have a direct 'updated since' filter,
    so this fetches by year and filters. For production use,
    consider using the BijgewerktTot endpoint to track updates.
    """
    session = get_session()

    # Parse the since date
    since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
    since_year = since_dt.year

    # Fetch documents by year
    for year in range(since_year, datetime.now().year + 1):
        url = f"{API_BASE}/api/WetgevingDocument/Datum/{year}"
        resp = session.get(url, timeout=TIMEOUT)
        if resp.status_code == 404:
            continue
        resp.raise_for_status()

        results = resp.json().get("ResultatenLijst", [])
        for doc in results:
            doc_id = doc.get("Id")
            has_content = doc.get("HeeftInhoud", False)

            full_doc = None
            if has_content:
                time.sleep(DELAY_SECONDS)
                full_doc = fetch_full_document(session, doc_id)

            record = normalize(doc, full_doc)
            yield record

        time.sleep(DELAY_SECONDS)


def bootstrap_sample(sample_size: int = 15) -> List[Dict[str, Any]]:
    """Fetch a sample of documents for testing."""
    session = get_session()
    samples = []

    print(f"Fetching {sample_size} sample documents...")

    # Fetch first page of documents
    page = fetch_document_list(session, skip=0, take=50)
    results = page.get("ResultatenLijst", [])
    total = page.get("TotaalAantal", 0)

    print(f"Total documents available: {total}")

    # Filter to documents with content and sample them
    docs_with_content = [d for d in results if d.get("HeeftInhoud", False)]

    for doc in docs_with_content[:sample_size]:
        doc_id = doc.get("Id")
        print(f"  Fetching document {doc_id}: {doc.get('Opschrift', '')[:60]}...")

        time.sleep(DELAY_SECONDS)
        full_doc = fetch_full_document(session, doc_id)

        record = normalize(doc, full_doc)
        samples.append(record)

        # Print text length for verification
        text_len = len(record.get("text", ""))
        print(f"    Text length: {text_len} chars")

    return samples


def save_samples(samples: List[Dict[str, Any]]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for record in samples:
        doc_id = record.get("id")
        filename = f"{doc_id}.json"
        filepath = SAMPLE_DIR / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(samples)} samples to {SAMPLE_DIR}")


def validate_samples(samples: List[Dict[str, Any]]) -> bool:
    """Validate that samples meet requirements."""
    print("\n=== Sample Validation ===")

    # Check minimum count
    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples (need at least 10)")
        return False
    print(f"OK: {len(samples)} samples")

    # Check text field presence
    has_text = sum(1 for s in samples if s.get("text"))
    if has_text < len(samples) * 0.5:
        print(f"FAIL: Only {has_text}/{len(samples)} samples have text")
        return False
    print(f"OK: {has_text}/{len(samples)} samples have text")

    # Check text is not just title
    real_text = sum(1 for s in samples if len(s.get("text", "")) > 50)
    print(f"OK: {real_text}/{len(samples)} samples have >50 chars of text")

    # Calculate average text length
    text_lengths = [len(s.get("text", "")) for s in samples if s.get("text")]
    if text_lengths:
        avg_len = sum(text_lengths) / len(text_lengths)
        print(f"Average text length: {avg_len:.0f} chars")

    # Check required fields
    required = ["_id", "_source", "_type", "title", "date"]
    for field in required:
        missing = sum(1 for s in samples if not s.get(field))
        if missing > 0:
            print(f"WARNING: {missing} samples missing '{field}'")

    print("=== Validation Complete ===\n")
    return True


def main():
    parser = argparse.ArgumentParser(description="BE/VlaamseCodex data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Bootstrap sample data")
    bootstrap_parser.add_argument(
        "--sample", action="store_true",
        help="Only fetch sample data (default: full fetch)"
    )
    bootstrap_parser.add_argument(
        "--sample-size", type=int, default=15,
        help="Number of samples to fetch (default: 15)"
    )

    # Status command
    subparsers.add_parser("status", help="Show API status")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            samples = bootstrap_sample(args.sample_size)
            save_samples(samples)
            validate_samples(samples)
        else:
            print("Full fetch mode - this will take a long time!")
            print("Use --sample for testing")
            count = 0
            for record in fetch_all():
                count += 1
                if count % 100 == 0:
                    print(f"Processed {count} records...")
            print(f"Total: {count} records")

    elif args.command == "status":
        session = get_session()
        page = fetch_document_list(session, skip=0, take=1)
        total = page.get("TotaalAantal", 0)
        print(f"API Status: OK")
        print(f"Total documents: {total}")

        # Check last update date
        update_url = f"{API_BASE}/api/WetgevingDocument/BijgewerktTot"
        resp = session.get(update_url, timeout=TIMEOUT)
        if resp.ok:
            print(f"Last update: {resp.text}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
