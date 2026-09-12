#!/usr/bin/env python3
"""
Montenegro Central Bank (CBCG) regulations and laws fetcher.

Fetches banking regulations and laws from cbcg.me.
Documents are PDF files with English translations; full text is extracted.
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

BASE_URL = "https://www.cbcg.me"
REGULATIONS_URL = f"{BASE_URL}/en/about-us/legislation/regulations"
LAWS_URL = f"{BASE_URL}/en/about-us/legislation/laws"
REQUEST_DELAY = 2.0


def get_session() -> requests.Session:
    """Create a session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal research project)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en,sr;q=0.5",
    })
    return session


def parse_pdf_links(session: requests.Session, page_url: str,
                    data_type: str) -> list[dict]:
    """
    Parse a CBCG page and extract all PDF links with titles.

    Args:
        session: requests session
        page_url: URL of the page to parse
        data_type: 'regulation' or 'law'

    Returns:
        List of dicts with title, pdf_url, category, data_type
    """
    try:
        response = session.get(page_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {page_url}: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    seen_urls = set()

    # Track current category from section headers
    current_category = "General"

    # Walk through all elements to track categories and links
    for element in soup.find_all(["h2", "h3", "h4", "a", "strong"]):
        # Update category from headers
        if element.name in ("h2", "h3", "h4"):
            header_text = element.get_text(strip=True)
            if header_text and len(header_text) > 3:
                current_category = header_text
            continue

        # Also pick up bold section headers
        if element.name == "strong":
            strong_text = element.get_text(strip=True)
            if strong_text and len(strong_text) > 5 and not strong_text.endswith(".pdf"):
                # Check if this looks like a category header (not a link title)
                parent = element.parent
                if parent and parent.name != "a":
                    current_category = strong_text
            continue

        # Process PDF links
        if element.name == "a":
            href = element.get("href", "")
            if not href.endswith(".pdf"):
                continue

            pdf_url = urljoin(BASE_URL, href)
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            title = element.get_text(strip=True)
            if not title:
                # Try parent element text
                parent = element.parent
                if parent:
                    title = parent.get_text(strip=True)

            # Clean title
            title = re.sub(r'\s+', ' ', title).strip() if title else ""

            # Extract OGM reference for date hints
            ogm_match = re.search(r'\((?:OGM|OGRM)\s*([\d/,\s]+)\)', title)
            ogm_ref = ogm_match.group(1).strip() if ogm_match else None

            results.append({
                "title": title,
                "pdf_url": pdf_url,
                "pdf_path": href,
                "category": current_category,
                "data_type": data_type,
                "ogm_ref": ogm_ref,
            })

    return results


def extract_year_from_ogm(ogm_ref: str) -> Optional[str]:
    """Extract the most recent year from an OGM reference like '19/22, 78/24'."""
    if not ogm_ref:
        return None
    years = re.findall(r'/(\d{2,4})', ogm_ref)
    if not years:
        return None
    # Convert 2-digit years to 4-digit
    full_years = []
    for y in years:
        if len(y) == 2:
            yi = int(y)
            full_years.append(f"20{y}" if yi < 50 else f"19{y}")
        else:
            full_years.append(y)
    return max(full_years) if full_years else None


def extract_year_from_filename(pdf_path: str) -> Optional[str]:
    """Try to extract a year from the PDF filename."""
    # Match patterns like _78-24.pdf or _2024.pdf
    match = re.search(r'[_-](\d{2})\.pdf$', pdf_path)
    if match:
        y = int(match.group(1))
        return f"20{match.group(1)}" if y < 50 else f"19{match.group(1)}"
    match = re.search(r'[_-](20\d{2})\.pdf$', pdf_path)
    if match:
        return match.group(1)
    return None


def download_and_extract(session: requests.Session, pdf_url: str) -> Optional[str]:
    """Download a PDF and extract text."""
    try:
        response = session.get(pdf_url, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  Failed to download {pdf_url}: {e}", file=sys.stderr)
        return None

    content = response.content
    if not content or len(content) < 100:
        return None

    # Verify it looks like a PDF
    if not (content[:4] == b'%PDF' or
            response.headers.get("Content-Type", "").startswith("application/pdf")):
        print(f"  Not a PDF: {pdf_url}", file=sys.stderr)
        return None

    # Generate a stable source_id from the URL path
    url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:12]

    text = extract_pdf_markdown(
        source="ME/CBCG",
        source_id=f"cbcg-{url_hash}",
        pdf_bytes=content,
        table="legislation",
    )

    return text if text and len(text) >= 50 else None


def normalize(raw: dict) -> dict:
    """Normalize raw document data to standard schema."""
    pdf_path = raw.get("pdf_path", "")
    url_hash = hashlib.md5(raw.get("pdf_url", "").encode()).hexdigest()[:10]
    _id = f"ME-CBCG-{url_hash}"

    # Determine date from OGM reference or filename
    year = extract_year_from_ogm(raw.get("ogm_ref"))
    if not year:
        year = extract_year_from_filename(pdf_path)
    date = f"{year}-01-01" if year else None

    return {
        "_id": _id,
        "_source": "ME/CBCG",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": raw.get("title") or f"CBCG Document {url_hash}",
        "text": raw.get("text", ""),
        "date": date,
        "url": raw.get("pdf_url", ""),
        "category": raw.get("category"),
        "data_type": raw.get("data_type"),
        "ogm_ref": raw.get("ogm_ref"),
        "language": "en",
    }


def fetch_all(session: requests.Session, max_docs: int = 500) -> Iterator[dict]:
    """Fetch all regulations and laws with full text."""
    count = 0

    for page_url, dtype in [(REGULATIONS_URL, "regulation"), (LAWS_URL, "law")]:
        print(f"\nFetching {dtype}s from {page_url}...", file=sys.stderr)
        entries = parse_pdf_links(session, page_url, dtype)
        print(f"Found {len(entries)} PDF links", file=sys.stderr)
        time.sleep(REQUEST_DELAY)

        for entry in entries:
            if count >= max_docs:
                return

            title_short = (entry["title"] or "")[:60]
            print(f"\n[{count + 1}] {title_short}...", file=sys.stderr)

            text = download_and_extract(session, entry["pdf_url"])
            if not text:
                print(f"  Skipped (no text extracted)", file=sys.stderr)
                time.sleep(REQUEST_DELAY)
                continue

            entry["text"] = text
            record = normalize(entry)

            if record.get("text"):
                yield record
                count += 1
                print(f"  OK ({len(text):,} chars)", file=sys.stderr)

            time.sleep(REQUEST_DELAY)

    print(f"\nTotal: {count} documents fetched", file=sys.stderr)


def bootstrap_sample(output_dir: Path, sample_size: int = 15):
    """Fetch sample documents and save to output directory."""
    output_dir.mkdir(parents=True, exist_ok=True)
    session = get_session()

    print(f"Fetching {sample_size} sample documents...", file=sys.stderr)

    count = 0
    total_chars = 0

    for record in fetch_all(session, max_docs=sample_size):
        filename = f"{record['_id']}.json"
        filepath = output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        print(f"  Saved: {filename} ({text_len:,} chars)", file=sys.stderr)
        total_chars += text_len
        count += 1

    print(f"\n{'=' * 50}", file=sys.stderr)
    print(f"Sample complete:", file=sys.stderr)
    print(f"  Documents: {count}", file=sys.stderr)
    print(f"  Total chars: {total_chars:,}", file=sys.stderr)
    print(f"  Avg chars/doc: {total_chars // count if count else 0:,}", file=sys.stderr)
    print(f"  Output: {output_dir}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Montenegro Central Bank (CBCG) regulations fetcher"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bootstrap_parser = subparsers.add_parser(
        "bootstrap", help="Fetch sample documents"
    )
    bootstrap_parser.add_argument(
        "--sample", action="store_true", help="Fetch sample data only"
    )
    bootstrap_parser.add_argument(
        "--output", type=Path,
        default=Path(__file__).parent / "sample",
        help="Output directory for samples"
    )
    bootstrap_parser.add_argument(
        "--count", type=int, default=15,
        help="Number of samples to fetch"
    )
    bootstrap_parser.add_argument("--full", action="store_true",
                                  help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.full:
            session = get_session()
            count = 0
            for record in fetch_all(session, max_docs=500):
                print(json.dumps(record, ensure_ascii=False))
                count += 1
            print(f"Total: {count} records", file=sys.stderr)
        elif args.sample:
            bootstrap_sample(args.output, args.count)
        else:
            bootstrap_sample(args.output, args.count)
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
