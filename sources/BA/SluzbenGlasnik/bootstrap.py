#!/usr/bin/env python3
"""
Bosnia and Herzegovina Official Gazette (Službeni glasnik BiH) fetcher.

Fetches state-level legislation from sluzbenilist.ba.
Covers:
  - Službeni glasnik BiH (state-level gazette)
  - Službeni glasnik BiH – Međunarodni ugovori (international treaties)

Note: Federation BiH and Canton-level documents require subscription.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "http://www.sluzbenilist.ba"
SEARCH_ENDPOINT = "/search/searchresult"
DOCUMENT_ENDPOINT = "/page/akt/{doc_id}"
REQUEST_DELAY = 2.0  # seconds between requests


def get_session() -> requests.Session:
    """Create a session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal research project)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "bs,hr,sr,en;q=0.5",
    })
    session.verify = False  # SSL cert issues on this site
    return session


def search_documents(session: requests.Session, query: str = "", page: int = 1,
                     max_results: int = 100) -> list[dict]:
    """
    Search for documents via the search endpoint.

    Args:
        session: requests session
        query: search query (empty for all)
        page: page number
        max_results: maximum results to return

    Returns:
        List of document metadata dicts
    """
    url = f"{BASE_URL}{SEARCH_ENDPOINT}"
    params = {"naziv": query}

    try:
        response = session.get(url, params=params, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Search request failed: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    results = []

    # Find all search result items
    for item in soup.select(".search-result-item"):
        # Check if document is accessible (has direct link, not login popup)
        link_elem = item.select_one('a[href^="/page/akt/"]')
        if not link_elem:
            continue  # Skip subscription-only items

        doc_url = link_elem.get("href", "")
        doc_id = doc_url.replace("/page/akt/", "") if doc_url else None

        if not doc_id:
            continue

        # Extract metadata
        h4 = item.select_one("h4")
        h6 = item.select_one("h6")
        p = item.select_one("p")

        # Parse gazette info from h4
        gazette_type = None
        gazette_number = None
        gazette_date = None

        if h4:
            h4_text = h4.get_text(strip=True)

            # Determine gazette type
            if "Službeni glasnik BiH – Međunarodni ugovori" in h4_text or "međunarodni ugovori" in h4_text.lower():
                gazette_type = "MU"  # International treaties
            elif "Službeni glasnik BiH" in h4_text:
                gazette_type = "BiH"  # State level
            elif "Službene novine Federacije BiH" in h4_text:
                gazette_type = "FBiH"  # Federation (subscription)
                continue  # Skip these
            elif "Službene novine Kantona Sarajevo" in h4_text:
                gazette_type = "KS"  # Canton (subscription)
                continue  # Skip these

            # Extract gazette number (e.g., "broj 28/21")
            broj_match = re.search(r'broj\s+(\d+/\d+)', h4_text)
            if broj_match:
                gazette_number = broj_match.group(1)

            # Extract date
            date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', h4_text)
            if date_match:
                gazette_date = date_match.group(1)

        # Get issuer and title
        issuer = h6.get_text(strip=True) if h6 else None
        title = p.get_text(strip=True) if p else None

        if gazette_type and gazette_type in ("BiH", "MU"):
            results.append({
                "doc_id": doc_id,
                "gazette_type": gazette_type,
                "gazette_number": gazette_number,
                "gazette_date": gazette_date,
                "issuer": issuer,
                "title": title,
                "url": f"{BASE_URL}{doc_url}",
            })

        if len(results) >= max_results:
            break

    return results


def fetch_document(session: requests.Session, doc_id: str) -> Optional[dict]:
    """
    Fetch a single document by ID.

    Args:
        session: requests session
        doc_id: document identifier (base64 encoded)

    Returns:
        Document dict with full text, or None on error
    """
    url = f"{BASE_URL}/page/akt/{doc_id}"

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch document {doc_id}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Check if this is a valid document page
    if "nepostojeća Stranica" in soup.title.text if soup.title else "":
        print(f"Document not found: {doc_id}", file=sys.stderr)
        return None

    # Extract gazette info from breadcrumbs and header
    h1 = soup.select_one("h1")
    gazette_info = h1.get_text(strip=True) if h1 else ""

    # Parse gazette number from h1 (e.g., "Službeni glasnik BiH, broj 28/21")
    gazette_type = None
    gazette_number = None
    gazette_year = None

    if "Međunarodni ugovori" in gazette_info or "međunarodni ugovori" in gazette_info.lower():
        gazette_type = "MU"
    elif "Službeni glasnik BiH" in gazette_info:
        gazette_type = "BiH"

    broj_match = re.search(r'broj\s+(\d+)/(\d+)', gazette_info)
    if broj_match:
        gazette_number = f"{broj_match.group(1)}/{broj_match.group(2)}"
        gazette_year = 2000 + int(broj_match.group(2)) if int(broj_match.group(2)) < 100 else int(broj_match.group(2))

    # Extract document content
    content_div = soup.select_one(".col-md-12.margin-bottom-10")
    if not content_div:
        print(f"No content found for document {doc_id}", file=sys.stderr)
        return None

    # Remove navigation and header elements
    for nav in content_div.select(".breadcrumb, .breadcrumbs, .header, .navbar"):
        nav.decompose()

    # Get title from the first strong/centered element
    title = None
    title_elem = content_div.select_one("p.text-center strong")
    if title_elem:
        title = title_elem.get_text(strip=True)

    # Extract text content
    # Remove script and style tags
    for tag in content_div.select("script, style, noscript"):
        tag.decompose()

    # Get text with preserved structure
    text = content_div.get_text(separator="\n", strip=True)

    # Clean up text
    text = re.sub(r'\n{3,}', '\n\n', text)  # Reduce multiple newlines
    text = re.sub(r'[ \t]+', ' ', text)  # Normalize spaces

    # Extract date from the page if available
    date_str = None
    date_div = soup.select_one(".col-md-3.margin-bottom-10 p.text-center")
    if date_div:
        date_text = date_div.get_text()
        date_match = re.search(r'(\d{1,2})\.\s*(\w+)\s*(\d{4})', date_text)
        if date_match:
            day = date_match.group(1)
            month_text = date_match.group(2).lower()
            year = date_match.group(3)

            # Map Bosnian month names to numbers
            month_map = {
                'januar': '01', 'januara': '01',
                'februar': '02', 'februara': '02',
                'mart': '03', 'marta': '03',
                'april': '04', 'aprila': '04',
                'maj': '05', 'maja': '05',
                'juni': '06', 'juna': '06',
                'juli': '07', 'jula': '07',
                'august': '08', 'augusta': '08',
                'septembar': '09', 'septembra': '09',
                'oktobar': '10', 'oktobra': '10',
                'novembar': '11', 'novembra': '11',
                'decembar': '12', 'decembra': '12',
            }
            month = month_map.get(month_text, '01')
            date_str = f"{year}-{month}-{day.zfill(2)}"

    # Extract issuer
    issuer = None
    # Look for issuer in standard locations
    issuer_patterns = [
        r'(VIJEĆE MINISTARA[^<\n]+)',
        r'(MINISTARSTVO[^<\n]+)',
        r'(AGENCIJA[^<\n]+)',
        r'(PARLAMENT[^<\n]+)',
        r'(PREDSJEDNIŠTVO[^<\n]+)',
    ]
    for pattern in issuer_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            issuer = match.group(1).strip()
            break

    return {
        "doc_id": doc_id,
        "gazette_type": gazette_type,
        "gazette_number": gazette_number,
        "gazette_year": gazette_year,
        "title": title,
        "issuer": issuer,
        "text": text,
        "date": date_str,
        "url": url,
    }


def normalize(raw: dict) -> dict:
    """
    Normalize raw document data to standard schema.

    Args:
        raw: raw document dict

    Returns:
        Normalized document dict
    """
    doc_id = raw.get("doc_id", "")
    gazette_number = raw.get("gazette_number", "")
    gazette_year = raw.get("gazette_year")

    # Generate unique ID
    _id = f"BA-SG-{gazette_number.replace('/', '-')}-{doc_id[:8]}" if gazette_number else f"BA-SG-{doc_id[:16]}"

    return {
        "_id": _id,
        "_source": "BA/SluzbenGlasnik",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": raw.get("title") or f"Document {gazette_number}",
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", f"http://www.sluzbenilist.ba/page/akt/{doc_id}"),
        "gazette_type": raw.get("gazette_type"),
        "gazette_number": gazette_number,
        "gazette_year": gazette_year,
        "issuer": raw.get("issuer"),
        "language": "bs",  # Bosnian (also hr, sr)
    }


def fetch_all(session: requests.Session, max_docs: int = 1000) -> Iterator[dict]:
    """
    Fetch all documents.

    Args:
        session: requests session
        max_docs: maximum documents to fetch

    Yields:
        Normalized document dicts
    """
    # Search for documents (empty query returns recent documents)
    print("Searching for documents...", file=sys.stderr)
    doc_list = search_documents(session, query="", max_results=max_docs)
    print(f"Found {len(doc_list)} accessible documents", file=sys.stderr)

    for i, doc_meta in enumerate(doc_list):
        doc_id = doc_meta["doc_id"]
        print(f"Fetching {i+1}/{len(doc_list)}: {doc_id[:20]}...", file=sys.stderr)

        raw = fetch_document(session, doc_id)
        if raw and raw.get("text"):
            yield normalize(raw)

        time.sleep(REQUEST_DELAY)


def fetch_updates(session: requests.Session, since: str) -> Iterator[dict]:
    """
    Fetch documents modified since a date.

    Args:
        session: requests session
        since: ISO date string (YYYY-MM-DD)

    Yields:
        Normalized document dicts
    """
    # For now, just fetch recent documents
    # The search returns most recent first
    yield from fetch_all(session, max_docs=50)


def bootstrap_sample(output_dir: Path, sample_size: int = 12):
    """
    Fetch sample documents and save to output directory.

    Args:
        output_dir: directory to save samples
        sample_size: number of samples to fetch
    """
    import warnings
    warnings.filterwarnings("ignore", message="Unverified HTTPS request")

    output_dir.mkdir(parents=True, exist_ok=True)
    session = get_session()

    print(f"Fetching {sample_size} sample documents...", file=sys.stderr)

    # Search for various types of documents
    all_docs = []

    # Search for laws (zakon)
    print("Searching for laws...", file=sys.stderr)
    laws = search_documents(session, query="zakon", max_results=20)
    all_docs.extend(laws)
    time.sleep(REQUEST_DELAY)

    # Search for decisions (odluka)
    print("Searching for decisions...", file=sys.stderr)
    decisions = search_documents(session, query="odluka", max_results=20)
    all_docs.extend(decisions)
    time.sleep(REQUEST_DELAY)

    # Search for regulations (uredba)
    print("Searching for regulations...", file=sys.stderr)
    regulations = search_documents(session, query="uredba", max_results=20)
    all_docs.extend(regulations)
    time.sleep(REQUEST_DELAY)

    # Deduplicate by doc_id
    seen = set()
    unique_docs = []
    for doc in all_docs:
        if doc["doc_id"] not in seen:
            seen.add(doc["doc_id"])
            unique_docs.append(doc)

    print(f"Found {len(unique_docs)} unique accessible documents", file=sys.stderr)

    # Fetch and save samples
    count = 0
    total_chars = 0

    for doc_meta in unique_docs:
        if count >= sample_size:
            break

        doc_id = doc_meta["doc_id"]
        print(f"Fetching {count+1}/{sample_size}: {doc_meta.get('title', doc_id)[:50]}...", file=sys.stderr)

        raw = fetch_document(session, doc_id)
        if raw and raw.get("text"):
            # Merge metadata from search with fetched content
            raw["issuer"] = raw.get("issuer") or doc_meta.get("issuer")
            raw["title"] = raw.get("title") or doc_meta.get("title")

            normalized = normalize(raw)

            # Check text length
            text_len = len(normalized.get("text", ""))
            if text_len < 100:
                print(f"  Skipping (text too short: {text_len} chars)", file=sys.stderr)
                continue

            # Save to file
            filename = f"{normalized['_id']}.json"
            filepath = output_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            print(f"  Saved: {filename} ({text_len} chars)", file=sys.stderr)
            total_chars += text_len
            count += 1

        time.sleep(REQUEST_DELAY)

    print(f"\nSample complete:", file=sys.stderr)
    print(f"  Documents: {count}", file=sys.stderr)
    print(f"  Total chars: {total_chars:,}", file=sys.stderr)
    print(f"  Avg chars/doc: {total_chars // count if count else 0:,}", file=sys.stderr)
    print(f"  Output: {output_dir}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Bosnia and Herzegovina Official Gazette fetcher"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser(
        "bootstrap",
        help="Fetch sample documents"
    )
    bootstrap_parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample data only"
    )
    bootstrap_parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent / "sample",
        help="Output directory for samples"
    )
    bootstrap_parser.add_argument(
        "--count",
        type=int,
        default=12,
        help="Number of samples to fetch"
    )

    # Search command
    search_parser = subparsers.add_parser(
        "search",
        help="Search for documents"
    )
    search_parser.add_argument(
        "query",
        nargs="?",
        default="",
        help="Search query"
    )
    search_parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "bootstrap" and args.sample:
        bootstrap_sample(args.output, args.count)
    elif args.command == "search":
        import warnings
        warnings.filterwarnings("ignore", message="Unverified HTTPS request")
        session = get_session()
        results = search_documents(session, args.query, max_results=20)
        for r in results:
            print(f"{r['gazette_type']} {r['gazette_number']}: {r['title'][:60]}...")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
