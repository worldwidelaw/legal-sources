#!/usr/bin/env python3
"""
Republika Srpska Legislation Fetcher.

Fetches legislation from Paragraf BA (paragraf.ba), which provides free access
to key Republika Srpska legislation with full text in HTML format.

Source: https://www.paragraf.ba/besplatni-propisi-republike-srpske.html
Coverage: Major RS laws, regulations, and decisions (approximately 160+ documents).

This source provides consolidated (precisceni) versions of legislation published in
Službeni glasnik Republike Srpske (Official Gazette of Republika Srpska).

Note: The official gazette (slglasnik.org) requires subscription for full access.
Paragraf BA provides free public access to the most important legislation with
full text, which is sufficient for legal research purposes.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://www.paragraf.ba"
INDEX_URL = f"{BASE_URL}/besplatni-propisi-republike-srpske.html"
REQUEST_DELAY = 1.5  # seconds between requests


def get_session() -> requests.Session:
    """Create a session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "WorldWideLaw/1.0 (legal research project)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "sr,bs,hr,en;q=0.5",
    })
    return session


def list_legislation(session: requests.Session) -> list[dict]:
    """
    Get list of all available legislation from index page.

    Args:
        session: requests session

    Returns:
        List of dicts with legislation metadata
    """
    try:
        response = session.get(INDEX_URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch index: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    legislation = []

    # Find all legislation links with pattern: propisi/republika-srpska/*.html
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")

        if "propisi/republika-srpska/" in href and href.endswith(".html"):
            # Get the title from link text
            title = link.get_text(strip=True)
            if not title:
                # Extract from URL
                path_part = href.split("/")[-1].replace(".html", "")
                title = path_part.replace("-", " ").title()

            # Build full URL
            if href.startswith("http"):
                full_url = href
            else:
                full_url = urljoin(BASE_URL, href)

            # Extract slug for ID generation
            slug = href.split("/")[-1].replace(".html", "")

            # Avoid duplicates
            if not any(l["url"] == full_url for l in legislation):
                legislation.append({
                    "url": full_url,
                    "title": title,
                    "slug": slug,
                })

    return legislation


def extract_text_content(html: str) -> str:
    """
    Extract main text content from HTML page.

    Args:
        html: HTML content

    Returns:
        Cleaned text content
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove unwanted elements
    for tag in soup.find_all(["script", "style", "nav", "header", "footer", "noscript"]):
        tag.decompose()

    # Find main content in article tag (where legislation text is)
    article = soup.find("article")

    if article:
        # Remove table header with metadata (keep the actual legislation text)
        for table in article.find_all("table", class_="tabelanaslov"):
            table.decompose()

        text = article.get_text(separator="\n", strip=True)
    else:
        # Fallback: get all text from body
        body = soup.find("body")
        text = body.get_text(separator="\n", strip=True) if body else ""

    # Clean up
    text = unescape(text)

    # Remove nav/site elements
    nav_phrases = [
        "POČETNA", "Vesti", "O NAMA", "BESPLATNO", "IZDANJA I PRETPLATA",
        "SAVJETOVANJA", "KORISNIČKI KUTAK", "KONTAKT", "LOG IN",
        "Paragraf Lex", "Generalne informacije", "Naš tim",
        "Postanite dio Paragraf tima", "Referentna lista",
        "Protokoli sa fakultetima", "Besplatni propisi",
        "Registar službenih glasila", "Sudska praksa",
        "Neradni dani", "Pravna baza", "Zakažite besplatnu prezentaciju",
        "Priručnici", "Video uputstvo", "Screen Saveri", "FAQ",
        "Slažem se", "Saznaj više o kolačićima",
        "Korištenje kolačića", "Web stranicu koristi kolačiće",
    ]
    for phrase in nav_phrases:
        text = text.replace(phrase, "")

    # Clean multiple newlines and spaces
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r' +', ' ', text)

    return text.strip()


def extract_gazette_info(text: str, soup: BeautifulSoup) -> tuple[Optional[str], Optional[str]]:
    """
    Extract gazette number and date from document.

    Args:
        text: document text
        soup: BeautifulSoup object

    Returns:
        Tuple of (gazette_number, gazette_date)
    """
    gazette_number = None
    gazette_date = None

    # Find subtitle with gazette references
    subtitle = soup.find("span", class_="podnaslovpropisa")
    if subtitle:
        subtitle_text = subtitle.get_text()
        # Pattern like: 64/2017, 104/2018 - odluka US, 15/2021...
        numbers = re.findall(r'(\d+/\d{4})', subtitle_text)
        if numbers:
            gazette_number = numbers[0]  # First (original) publication

    # Try to extract date from gazette number
    if gazette_number:
        match = re.search(r'/(\d{4})$', gazette_number)
        if match:
            year = match.group(1)
            gazette_date = f"{year}-01-01"  # Default to Jan 1 if no exact date

    # Look for specific date in text
    date_match = re.search(r'(\d{1,2})\.\s*(\w+)\s*(\d{4})\.\s*godine', text)
    if date_match:
        day = date_match.group(1)
        month_name = date_match.group(2).lower()
        year = date_match.group(3)

        month_map = {
            'januar': '01', 'januara': '01',
            'februar': '02', 'februara': '02',
            'mart': '03', 'marta': '03',
            'april': '04', 'aprila': '04',
            'maj': '05', 'maja': '05',
            'juni': '06', 'juna': '06',
            'juli': '07', 'jula': '07',
            'august': '08', 'augusta': '08', 'avgusta': '08',
            'septembar': '09', 'septembra': '09',
            'oktobar': '10', 'oktobra': '10',
            'novembar': '11', 'novembra': '11',
            'decembar': '12', 'decembra': '12',
        }
        month = month_map.get(month_name, '01')
        gazette_date = f"{year}-{month}-{day.zfill(2)}"

    return gazette_number, gazette_date


def extract_document_type(title: str, text: str) -> str:
    """
    Determine document type from title and content.

    Args:
        title: document title
        text: document text

    Returns:
        Document type string
    """
    title_lower = title.lower()

    if "zakon" in title_lower or "zakonik" in title_lower:
        return "zakon"  # Law
    elif "uredba" in title_lower:
        return "uredba"  # Decree/Regulation
    elif "pravilnik" in title_lower:
        return "pravilnik"  # Rulebook
    elif "odluka" in title_lower:
        return "odluka"  # Decision
    elif "ugovor" in title_lower or "kolektivni" in title_lower:
        return "ugovor"  # Agreement
    elif "tarif" in title_lower:
        return "tarifa"  # Tariff
    elif "pravila" in title_lower:
        return "pravila"  # Rules
    else:
        return "propis"  # General regulation


def fetch_document(session: requests.Session, doc_info: dict) -> Optional[dict]:
    """
    Fetch a single document with full text.

    Args:
        session: requests session
        doc_info: dict with url, title, slug

    Returns:
        Document dict or None
    """
    url = doc_info["url"]

    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}", file=sys.stderr)
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract text content
    text = extract_text_content(response.text)

    if len(text) < 500:
        print(f"  Text too short for {doc_info.get('title', url)[:50]} ({len(text)} chars)", file=sys.stderr)
        return None

    # Extract title from page
    title_elem = soup.find("span", class_="naslovpropisa1")
    if title_elem:
        title = title_elem.get_text(strip=True)
        # Check for second line
        title_elem2 = soup.find("span", class_="naslovpropisa1a")
        if title_elem2:
            title += " " + title_elem2.get_text(strip=True)
    else:
        title = doc_info.get("title", "")

    # Get gazette info
    gazette_number, gazette_date = extract_gazette_info(text, soup)

    # Get document type
    doc_type = extract_document_type(title, text)

    return {
        "url": url,
        "title": title,
        "slug": doc_info.get("slug", ""),
        "text": text,
        "gazette_number": gazette_number,
        "gazette_date": gazette_date,
        "doc_type": doc_type,
    }


def normalize(raw: dict) -> dict:
    """
    Normalize raw document data to standard schema.

    Args:
        raw: raw document dict

    Returns:
        Normalized document dict
    """
    slug = raw.get("slug", "unknown")
    gazette_number = raw.get("gazette_number", "")

    # Generate unique ID
    if gazette_number:
        _id = f"BA-RS-{gazette_number.replace('/', '-')}-{slug[:30]}"
    else:
        _id = f"BA-RS-{slug[:50]}"

    # Clean ID
    _id = re.sub(r'[^\w\-]', '-', _id)
    _id = re.sub(r'-+', '-', _id)

    return {
        "_id": _id,
        "_source": "BA/RS",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("gazette_date"),
        "url": raw.get("url", ""),
        "gazette_number": gazette_number,
        "doc_type": raw.get("doc_type", "propis"),
        "language": "sr",  # Serbian (also bs/hr)
    }


def fetch_all(session: requests.Session, max_docs: int = 1000) -> Iterator[dict]:
    """
    Fetch all legislation documents.

    Args:
        session: requests session
        max_docs: maximum documents to fetch

    Yields:
        Normalized document dicts
    """
    print(f"Fetching legislation index from {INDEX_URL}...", file=sys.stderr)
    legislation = list_legislation(session)
    print(f"Found {len(legislation)} documents", file=sys.stderr)

    count = 0

    for doc_info in legislation:
        if count >= max_docs:
            break

        print(f"  Fetching: {doc_info.get('title', doc_info['url'])[:50]}...", file=sys.stderr)

        raw = fetch_document(session, doc_info)
        if raw and raw.get("text"):
            yield normalize(raw)
            count += 1

        time.sleep(REQUEST_DELAY)


def fetch_updates(session: requests.Session, since: str) -> Iterator[dict]:
    """
    Fetch documents modified since a date.

    Since this source doesn't provide modification dates,
    this fetches all documents.

    Args:
        session: requests session
        since: ISO date string (YYYY-MM-DD) - not used

    Yields:
        Normalized document dicts
    """
    yield from fetch_all(session)


def bootstrap_sample(output_dir: Path, sample_size: int = 12):
    """
    Fetch sample documents and save to output directory.

    Args:
        output_dir: directory to save samples
        sample_size: number of samples to fetch
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    session = get_session()

    print(f"Fetching {sample_size} sample documents...", file=sys.stderr)
    print(f"Source: {INDEX_URL}", file=sys.stderr)

    # Get list of all legislation
    print("\nFetching legislation index...", file=sys.stderr)
    legislation = list_legislation(session)
    print(f"Found {len(legislation)} documents available", file=sys.stderr)

    time.sleep(REQUEST_DELAY)

    count = 0
    total_chars = 0

    for doc_info in legislation:
        if count >= sample_size:
            break

        print(f"\n  Processing: {doc_info.get('title', doc_info['url'])[:60]}...", file=sys.stderr)

        raw = fetch_document(session, doc_info)
        if not raw:
            print("    Skipping (fetch failed)", file=sys.stderr)
            time.sleep(REQUEST_DELAY)
            continue

        normalized = normalize(raw)

        # Validate
        text_len = len(normalized.get("text", ""))
        if text_len < 500:
            print(f"    Skipping (text too short: {text_len} chars)", file=sys.stderr)
            time.sleep(REQUEST_DELAY)
            continue

        # Save to file
        filename = f"{normalized['_id']}.json"
        filepath = output_dir / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        print(f"    Saved: {filename}", file=sys.stderr)
        print(f"    Title: {normalized['title'][:60]}", file=sys.stderr)
        print(f"    Text: {text_len:,} chars", file=sys.stderr)
        print(f"    Type: {normalized.get('doc_type', 'unknown')}", file=sys.stderr)

        total_chars += text_len
        count += 1

        time.sleep(REQUEST_DELAY)

    print(f"\n{'='*60}", file=sys.stderr)
    print(f"Sample complete:", file=sys.stderr)
    print(f"  Documents: {count}", file=sys.stderr)
    print(f"  Total chars: {total_chars:,}", file=sys.stderr)
    print(f"  Avg chars/doc: {total_chars // count if count else 0:,}", file=sys.stderr)
    print(f"  Output: {output_dir}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Republika Srpska Legislation Fetcher"
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

    # List command
    list_parser = subparsers.add_parser(
        "list",
        help="List available legislation"
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum items to list"
    )

    args = parser.parse_args()

    if args.command == "bootstrap" and args.sample:
        bootstrap_sample(args.output, args.count)
    elif args.command == "list":
        session = get_session()
        legislation = list_legislation(session)
        for i, doc in enumerate(legislation[:args.limit]):
            print(f"  {i+1}. {doc['title'][:70]}")
            print(f"     URL: {doc['url']}")
        print(f"\nTotal: {len(legislation)} documents available")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
