#!/usr/bin/env python3
"""
Federation of Bosnia and Herzegovina (FBiH) Legislation Fetcher.

Fetches legislation from the FBiH Government website (fbihvlada.gov.ba).
The website provides chronological registers of laws by year, with each law
having its own page containing the full text in HTML format.

Source: https://fbihvlada.gov.ba/bs/zakoni
Coverage: FBiH federal laws from 2019 onwards (approximately 30-50 laws per year).

Note: This source complements BA/SluzbenGlasnik (state-level) and BA/Brcko (district).
The official gazette (sluzbenilist.ba) requires subscription for FBiH content.
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://fbihvlada.gov.ba"
LAWS_INDEX_URL = f"{BASE_URL}/bs/zakoni"
REQUEST_DELAY = 1.5  # seconds between requests

# Years with chronological registers available
YEARS = list(range(2019, datetime.now().year + 1))


def get_session() -> requests.Session:
    """Create a session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "WorldWideLaw/1.0 (legal research project)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "bs,hr,sr,en;q=0.5",
    })
    return session


def get_year_register_url(year: int) -> str:
    """
    Get URL for chronological register of a specific year.

    Args:
        year: The year to get register for

    Returns:
        URL string for the year's register
    """
    if year == 2026:
        return f"{BASE_URL}/bs/hronoloski-registar-zakona-objavljenih-u-sluzbenim-novinama-fbih-u-2026-godini"
    elif year == 2025:
        return f"{BASE_URL}/bs/hronoloski-registar-zakona-objavljenih-u-sluzbenim-novinama-fbih-u-2025-godini"
    elif year == 2023:
        # Note: URL has typo "hronocoski" instead of "hronoloski"
        return f"{BASE_URL}/bs/hronocoski-registar-zakona-objavljenih-u-sluzbenim-novinama-federacije-bih-u-2023-godini"
    else:
        return f"{BASE_URL}/bs/hronoloski-registar-zakona-objavljenih-u-sluzbenim-novinama-federacije-bih-u-{year}-godini"


def list_laws_for_year(session: requests.Session, year: int) -> list[dict]:
    """
    Get list of laws from a year's chronological register.

    Args:
        session: requests session
        year: year to fetch laws for

    Returns:
        List of dicts with law metadata
    """
    url = get_year_register_url(year)

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {year} register: {e}", file=sys.stderr)
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    laws = []

    # Find all law links - they are in the content area
    # Pattern: /bs/{number}-zakon-...
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")

        # Match law URLs: /bs/{number}-zakon-...  or ../../bs/{number}-zakon-...
        if "-zakon-" in href.lower() or "-zakon-" in href.lower():
            # Clean the URL
            if href.startswith("../../"):
                href = href.replace("../../", "/")

            # Extract law number from URL
            match = re.search(r'/(\d+)-zakon-', href)
            if not match:
                match = re.search(r'/(\d+)-zakon-', href)

            law_number = match.group(1) if match else None

            # Get the title from link text or href
            title = link.get_text(strip=True)
            if not title:
                # Extract from URL
                path_part = href.split("/")[-1]
                title = path_part.replace("-", " ").title()

            # Build full URL
            if href.startswith("http"):
                full_url = href
            else:
                full_url = urljoin(BASE_URL, href)

            # Avoid duplicates
            if not any(l["url"] == full_url for l in laws):
                laws.append({
                    "url": full_url,
                    "title": title,
                    "law_number": law_number,
                    "year": year,
                })

    return laws


class ContentExtractor(HTMLParser):
    """Extract text content from HTML, focusing on article content."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.in_content = False
        self.skip_depth = 0
        self.skip_tags = {"script", "style", "nav", "header", "footer", "noscript"}

    def handle_starttag(self, tag, attrs):
        if tag in self.skip_tags:
            self.skip_depth += 1

        attrs_dict = dict(attrs)
        classes = attrs_dict.get("class", "")

        # Start capturing when we hit content sections
        if "content-article" in classes or "content-section" in classes:
            self.in_content = True

    def handle_endtag(self, tag):
        if tag in self.skip_tags and self.skip_depth > 0:
            self.skip_depth -= 1

    def handle_data(self, data):
        if self.skip_depth == 0 and self.in_content:
            text = data.strip()
            if text:
                self.text_parts.append(text)

    def get_text(self) -> str:
        """Get concatenated text content."""
        text = " ".join(self.text_parts)
        # Clean up
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r' ([.,;:!?])', r'\1', text)
        return text.strip()


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

    # Find main content areas
    content_divs = soup.find_all(class_=["content-article", "content-section"])

    if content_divs:
        text_parts = []
        for div in content_divs:
            text = div.get_text(separator=" ", strip=True)
            if text:
                text_parts.append(text)
        text = " ".join(text_parts)
    else:
        # Fallback: get all text from body
        body = soup.find("body")
        text = body.get_text(separator=" ", strip=True) if body else ""

    # Clean up
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r' ([.,;:!?])', r'\1', text)

    # Remove navigation elements text
    nav_phrases = [
        "Bosna i Hercegovina", "Federacija Bosne i Hercegovine",
        "Vlada Federacije Bosne i Hercegovine", "Početna", "Vlada",
        "Premijer", "Ministri", "Korisni linkovi", "Pretraži",
        "BOSANSKI", "HRVATSKI", "SRPSKI", "ENGLISH",
        "Slažem se", "Saznaj više o kolačićima",
    ]
    for phrase in nav_phrases:
        text = text.replace(phrase, " ")

    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def fetch_law(session: requests.Session, law_info: dict) -> Optional[dict]:
    """
    Fetch a single law with full text.

    Args:
        session: requests session
        law_info: dict with url, title, law_number, year

    Returns:
        Document dict or None
    """
    url = law_info["url"]

    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Failed to fetch {url}: {e}", file=sys.stderr)
        return None

    # Extract text content
    text = extract_text_content(response.text)

    if len(text) < 500:
        print(f"  Text too short for {law_info.get('title', url)[:50]} ({len(text)} chars)", file=sys.stderr)
        return None

    # Extract title from page if not set
    soup = BeautifulSoup(response.text, "html.parser")
    page_title = soup.find("title")
    if page_title:
        title_text = page_title.get_text(strip=True)
        # Remove site name prefix
        if "|" in title_text:
            title_text = title_text.split("|", 1)[1].strip()
        law_info["title"] = title_text or law_info.get("title", "")

    # Try to extract gazette info from text
    gazette_number = None
    gazette_date = None

    # Look for gazette reference (e.g., "Službene novine FBiH", broj 5/25)
    gazette_match = re.search(r'broj[:\s]+(\d+/\d+)', text, re.IGNORECASE)
    if gazette_match:
        gazette_number = gazette_match.group(1)

    # Look for date (e.g., "30. decembra 2024. godine")
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

    return {
        "url": url,
        "title": law_info.get("title", ""),
        "law_number": law_info.get("law_number"),
        "year": law_info.get("year"),
        "gazette_number": gazette_number,
        "gazette_date": gazette_date,
        "text": text,
    }


def normalize(raw: dict) -> dict:
    """
    Normalize raw document data to standard schema.

    Args:
        raw: raw document dict

    Returns:
        Normalized document dict
    """
    year = raw.get("year", "")
    law_number = raw.get("law_number", "")
    gazette_number = raw.get("gazette_number", "")

    # Generate unique ID
    if gazette_number:
        _id = f"BA-FBiH-{year}-{gazette_number.replace('/', '-')}"
    elif law_number:
        _id = f"BA-FBiH-{year}-{law_number}"
    else:
        # Fallback: use URL hash
        url_part = raw.get("url", "").split("/")[-1][:30]
        _id = f"BA-FBiH-{year}-{url_part}"

    return {
        "_id": _id,
        "_source": "BA/FBiH",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("gazette_date"),
        "url": raw.get("url", ""),
        "gazette_number": gazette_number,
        "year": year,
        "law_number": law_number,
        "language": "bs",  # Bosnian (also hr, sr)
    }


def fetch_all(session: requests.Session, max_docs: int = 1000) -> Iterator[dict]:
    """
    Fetch all laws.

    Args:
        session: requests session
        max_docs: maximum documents to fetch

    Yields:
        Normalized document dicts
    """
    count = 0

    for year in sorted(YEARS, reverse=True):
        if count >= max_docs:
            break

        print(f"Fetching laws for {year}...", file=sys.stderr)
        laws = list_laws_for_year(session, year)
        print(f"  Found {len(laws)} laws", file=sys.stderr)

        for law_info in laws:
            if count >= max_docs:
                break

            print(f"  Fetching: {law_info.get('title', law_info['url'])[:50]}...", file=sys.stderr)

            raw = fetch_law(session, law_info)
            if raw and raw.get("text"):
                yield normalize(raw)
                count += 1

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
    # Get laws from the current year and previous year
    current_year = datetime.now().year

    for year in [current_year, current_year - 1]:
        laws = list_laws_for_year(session, year)

        for law_info in laws:
            raw = fetch_law(session, law_info)
            if raw and raw.get("text"):
                yield normalize(raw)
            time.sleep(REQUEST_DELAY)


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
    print(f"Source: {LAWS_INDEX_URL}", file=sys.stderr)

    # Fetch from recent years
    count = 0
    total_chars = 0

    for year in sorted(YEARS, reverse=True):
        if count >= sample_size:
            break

        print(f"\nFetching laws for {year}...", file=sys.stderr)
        laws = list_laws_for_year(session, year)
        print(f"  Found {len(laws)} laws", file=sys.stderr)

        time.sleep(REQUEST_DELAY)

        for law_info in laws:
            if count >= sample_size:
                break

            print(f"\n  Processing: {law_info.get('title', law_info['url'])[:60]}...", file=sys.stderr)

            raw = fetch_law(session, law_info)
            if not raw:
                print("    Skipping (fetch failed)", file=sys.stderr)
                continue

            normalized = normalize(raw)

            # Validate
            text_len = len(normalized.get("text", ""))
            if text_len < 500:
                print(f"    Skipping (text too short: {text_len} chars)", file=sys.stderr)
                continue

            # Save to file
            filename = f"{normalized['_id']}.json"
            filepath = output_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            print(f"    Saved: {filename}", file=sys.stderr)
            print(f"    Title: {normalized['title'][:60]}", file=sys.stderr)
            print(f"    Text: {text_len:,} chars", file=sys.stderr)

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
        description="Federation of BiH Legislation Fetcher"
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
        help="List available laws"
    )
    list_parser.add_argument(
        "--year",
        type=int,
        default=datetime.now().year,
        help="Year to list laws for"
    )
    list_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum laws to list"
    )

    args = parser.parse_args()

    if args.command == "bootstrap" and args.sample:
        bootstrap_sample(args.output, args.count)
    elif args.command == "list":
        session = get_session()
        laws = list_laws_for_year(session, args.year)
        for i, law in enumerate(laws[:args.limit]):
            print(f"  {i+1}. {law['title'][:70]}")
            print(f"     URL: {law['url']}")
        print(f"\nTotal: {len(laws)} laws in {args.year}")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
