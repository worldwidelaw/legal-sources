#!/usr/bin/env python3
"""
VA/CodexIurisCanonici - 1983 Code of Canon Law Fetcher

Fetches the 1983 Codex Iuris Canonici from vatican.va.
Parses individual canons (1-1752) from static HTML pages.
Latin (official) and English translation included.

Data source: https://www.vatican.va/archive/cod-iuris-canonici/
Method: Static HTML scraping (7 Latin pages + ~40 English pages)
License: Public Domain (Holy See)
Rate limit: ~2 seconds between requests
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, Generator, List, Optional, Tuple

import requests

SOURCE_ID = "VA/CodexIurisCanonici"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://www.vatican.va"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,*/*",
}

DELAY = 2  # seconds between requests
PROMULGATION_DATE = "1983-01-25"  # Sacrae disciplinae leges

# Latin book pages (one per book)
LATIN_BOOKS = [
    ("I", "/archive/cod-iuris-canonici/latin/documents/cic_liberI_la.html", "De Normis Generalibus"),
    ("II", "/archive/cod-iuris-canonici/latin/documents/cic_liberII_la.html", "De Populo Dei"),
    ("III", "/archive/cod-iuris-canonici/latin/documents/cic_liberIII_la.html", "De Ecclesiae Munere Docendi"),
    ("IV", "/archive/cod-iuris-canonici/latin/documents/cic_liberIV_la.html", "De Ecclesiae Munere Sanctificandi"),
    ("V", "/archive/cod-iuris-canonici/latin/documents/cic_liberV_la.html", "De Bonis Ecclesiae Temporalibus"),
    ("VI", "/archive/cod-iuris-canonici/latin/documents/cic_liberVI_la.html", "De Sanctionibus in Ecclesia"),
    ("VII", "/archive/cod-iuris-canonici/latin/documents/cic_liberVII_la.html", "De Processibus"),
]

ENGLISH_BOOK_NAMES = {
    "I": "General Norms",
    "II": "The People of God",
    "III": "The Teaching Function of the Church",
    "IV": "The Sanctifying Function of the Church",
    "V": "The Temporal Goods of the Church",
    "VI": "Sanctions in the Church",
    "VII": "Processes",
}


def strip_html(html_text: str) -> str:
    """Remove HTML tags and clean text."""
    if not html_text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html_text)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"</?blockquote[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_page(url: str, session: requests.Session) -> Optional[str]:
    """Fetch an HTML page with retries."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 404:
                print(f"  404: {url}")
                return None
            print(f"  HTTP {resp.status_code}, retrying...")
            time.sleep(DELAY * 2)
        except requests.RequestException as e:
            print(f"  Error (attempt {attempt + 1}): {e}")
            time.sleep(DELAY)
    return None


def parse_canons_from_html(html: str) -> List[Tuple[int, str]]:
    """Parse individual canons from an HTML page.

    Returns list of (canon_number, canon_text) tuples.
    """
    canons = []
    # Pattern: Can. N followed by text until next Can. or end
    # Latin pages: <b>Can. N</b> &#x2014; text
    # English pages: Can. N text (no bold, no em-dash)
    # Split on canon boundaries
    pattern = re.compile(
        r'<(?:b|strong)>?\s*Can\.\s*(\d+)\s*</(?:b|strong)>?\s*(?:&#x2014;|&#8212;|\u2014|[-–—])?\s*'
        r'|'
        r'(?<=>)\s*Can\.\s*(\d+)\s+',
        re.IGNORECASE
    )

    matches = list(pattern.finditer(html))
    if not matches:
        return canons

    for i, match in enumerate(matches):
        canon_num = int(match.group(1) or match.group(2))
        start = match.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(html)

        raw_text = html[start:end]
        text = strip_html(raw_text)

        # Clean up leading/trailing dashes or whitespace
        text = re.sub(r'^[\s\u2014\-–—]+', '', text)
        text = text.strip()

        if text:
            canons.append((canon_num, text))

    return canons


def discover_english_pages(session: requests.Session) -> List[str]:
    """Parse the English index page to find all content page URLs."""
    index_url = f"{BASE_URL}/archive/cod-iuris-canonici/cic_index_en.html"
    html = fetch_page(index_url, session)
    if not html:
        print("  Warning: Could not fetch English index page")
        return []

    # Find all links to English canon pages
    pattern = re.compile(
        r'href="(/archive/cod-iuris-canonici/eng/documents/cic_lib\d+-cann[\d-]+_en\.html)"'
    )
    paths = pattern.findall(html)
    return [f"{BASE_URL}{p}" for p in paths]


def fetch_all_english_canons(session: requests.Session) -> Dict[int, str]:
    """Fetch and parse all English translation pages, returning {canon_num: text}."""
    english_canons = {}
    pages = discover_english_pages(session)
    if not pages:
        return english_canons

    print(f"  Fetching {len(pages)} English pages...")
    for url in pages:
        html = fetch_page(url, session)
        if html:
            canons = parse_canons_from_html(html)
            for num, text in canons:
                english_canons[num] = text
        time.sleep(DELAY)

    print(f"  Parsed {len(english_canons)} English canons")
    return english_canons


def normalize(canon_num: int, latin_text: str, english_text: str,
              book_num: str, book_title_la: str) -> dict:
    """Normalize a canon into a standard record."""
    book_title_en = ENGLISH_BOOK_NAMES.get(book_num, "")
    return {
        "_id": f"VA-CIC-Can-{canon_num}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"Canon {canon_num}",
        "text": latin_text,
        "text_en": english_text,
        "canon_number": canon_num,
        "book": f"Liber {book_num}",
        "book_title_la": book_title_la,
        "book_title_en": book_title_en,
        "date": PROMULGATION_DATE,
        "url": f"{BASE_URL}/archive/cod-iuris-canonici/cic_index_la.html",
        "language": "la",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all canons from the Code of Canon Law."""
    session = requests.Session()
    total = 0

    # Fetch English translations (all at once for matching)
    if sample:
        # For sample, only fetch English for Book I
        print("  Fetching English Book I for sample...")
        en_url = f"{BASE_URL}/archive/cod-iuris-canonici/eng/documents/cic_lib1-cann1-6_en.html"
        english_canons = {}
        # Fetch the first few English pages for Book I
        book1_en_pages = [
            f"{BASE_URL}/archive/cod-iuris-canonici/eng/documents/cic_lib1-cann1-6_en.html",
            f"{BASE_URL}/archive/cod-iuris-canonici/eng/documents/cic_lib1-cann7-22_en.html",
            f"{BASE_URL}/archive/cod-iuris-canonici/eng/documents/cic_lib1-cann23-28_en.html",
        ]
        for url in book1_en_pages:
            html = fetch_page(url, session)
            if html:
                for num, text in parse_canons_from_html(html):
                    english_canons[num] = text
            time.sleep(DELAY)
    else:
        english_canons = fetch_all_english_canons(session)

    books_to_fetch = LATIN_BOOKS[:1] if sample else LATIN_BOOKS

    for book_num, path, book_title in books_to_fetch:
        url = f"{BASE_URL}{path}"
        print(f"  Fetching Liber {book_num}: {book_title}...")
        html = fetch_page(url, session)
        if not html:
            print(f"  ERROR: Could not fetch {url}")
            continue

        canons = parse_canons_from_html(html)
        print(f"  Parsed {len(canons)} canons from Liber {book_num}")

        for canon_num, latin_text in canons:
            english_text = english_canons.get(canon_num, "")
            record = normalize(canon_num, latin_text, english_text,
                               book_num, book_title)
            yield record
            total += 1

            if sample and total >= 15:
                print(f"  Sample limit reached ({total} records)")
                return

        time.sleep(DELAY)

    print(f"  Total canons fetched: {total}")


def cmd_test(session: requests.Session) -> bool:
    """Test connectivity to vatican.va."""
    url = f"{BASE_URL}/archive/cod-iuris-canonici/latin/documents/cic_liberI_la.html"
    html = fetch_page(url, session)
    if not html:
        print("FAIL: Could not fetch Book I")
        return False
    canons = parse_canons_from_html(html)
    if len(canons) < 50:
        print(f"FAIL: Expected 200+ canons in Book I, got {len(canons)}")
        return False
    print(f"OK: Book I has {len(canons)} canons")
    # Verify first canon
    if canons[0][0] == 1:
        print(f"OK: First canon text starts with: {canons[0][1][:80]}...")
    return True


def main():
    parser = argparse.ArgumentParser(description="VA/CodexIurisCanonici fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only sample data (~15 records)")
    parser.add_argument("--full", action="store_true",
                        help="Full bootstrap (all canons)")
    args = parser.parse_args()

    if args.command == "test":
        session = requests.Session()
        ok = cmd_test(session)
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap":
        is_sample = args.sample or not args.full
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        count = 0
        for record in fetch_all(sample=is_sample):
            if is_sample:
                fname = SAMPLE_DIR / f"{record['_id']}.json"
                fname.write_text(json.dumps(record, indent=2, ensure_ascii=False))
            else:
                print(json.dumps(record, ensure_ascii=False))
            count += 1

        print(f"\n{'Sample' if is_sample else 'Full'} bootstrap complete: {count} records")
        if is_sample:
            print(f"Samples saved to {SAMPLE_DIR}/")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
