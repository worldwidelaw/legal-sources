#!/usr/bin/env python3
"""
VE/TSJGaceta - Venezuela Legislation Fetcher (via Justia)

Fetches Venezuelan laws, decrees, regulations, resolutions, and codes from
Justia Venezuela, which mirrors official gazette content with full text in HTML.

Data source: https://venezuela.justia.com/federales/
Method: HTML scraping of index pages + /gdoc/ detail pages (text in <noframes>)
License: Public Venezuelan legislation / Justia
Rate limit: ~2 seconds between requests

Categories: leyes (314), decretos (155), reglamentos (60), resoluciones (13),
            codigos (11) — ~553 total documents

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

SOURCE_ID = "VE/TSJGaceta"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://venezuela.justia.com"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html, */*",
}

DELAY = 2  # seconds between requests

CATEGORIES = {
    "leyes": "Ley",
    "decretos": "Decreto",
    "reglamentos": "Reglamento",
    "resoluciones": "Resolución",
    "codigos": "Código",
}


def strip_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html_text)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"</?div[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def fetch_page(url: str, session: requests.Session) -> Optional[str]:
    """Fetch a page with retries."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                resp.encoding = "utf-8"
                return resp.text
            if resp.status_code == 404:
                return None
            if resp.status_code >= 500:
                time.sleep(DELAY * 2)
                continue
            return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt + 1}): {e}")
            time.sleep(DELAY)
    return None


def list_documents(category: str, session: requests.Session) -> list:
    """List all document URLs in a category."""
    url = f"{BASE_URL}/federales/{category}/"
    html = fetch_page(url, session)
    if not html:
        return []

    # Find links to gdoc pages
    pattern = rf'href="(/federales/{category}/[^"]+/gdoc/)"'
    links = re.findall(pattern, html)

    # Also find links without /gdoc/ and append it
    pattern2 = rf'href="(/federales/{category}/[^"]+/)"'
    all_links = re.findall(pattern2, html)
    gdoc_links = set(links)
    for link in all_links:
        if not link.endswith("/gdoc/") and link != f"/federales/{category}/":
            gdoc_link = link.rstrip("/") + "/gdoc/"
            gdoc_links.add(gdoc_link)

    return sorted(gdoc_links)


def extract_title_from_url(url_path: str, category: str) -> str:
    """Extract a readable title from the URL slug."""
    slug = url_path.split(f"/federales/{category}/")[1].rstrip("/").replace("/gdoc", "")
    title = slug.replace("-", " ").title()
    return title


def extract_text_from_gdoc(html: str) -> str:
    """Extract full text from a gdoc page's <noframes> tag."""
    match = re.search(r"<noframes>(.*?)</noframes>", html, re.DOTALL)
    if match:
        return strip_html(match.group(1))
    return ""


def extract_title_from_page(html: str) -> Optional[str]:
    """Extract the page title from the HTML."""
    # Prefer h1 tag for clean title
    match = re.search(r"<h1[^>]*>([^<]+)</h1>", html)
    if match:
        title = unescape(match.group(1)).strip()
        if title:
            return title
    # Fallback to <title> tag
    match = re.search(r"<title>([^<]+)</title>", html)
    if match:
        title = unescape(match.group(1)).strip()
        # Extract the document name from breadcrumb-style title
        # "Justia Venezuela :: Federales > Leyes > NAME :: Ley de Venezuela"
        parts = title.split("::")
        if len(parts) >= 2:
            mid = parts[1].strip()
            segments = mid.split(">")
            if segments:
                return segments[-1].strip()
        return title
    return None


def normalize(url_path: str, category: str, text: str,
              page_title: Optional[str] = None) -> dict:
    """Normalize a document record."""
    slug = url_path.split(f"/federales/{category}/")[1].rstrip("/").replace("/gdoc", "")

    title = page_title or extract_title_from_url(url_path, category)

    # Try to extract date from text header (GACETA OFICIAL... Caracas, DD de MES de YYYY)
    date_iso = None
    date_match = re.search(
        r"Caracas,?\s+(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", text[:500]
    )
    if date_match:
        months_es = {
            "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
            "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
            "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12",
        }
        day = date_match.group(1).zfill(2)
        month = months_es.get(date_match.group(2).lower(), "01")
        year = date_match.group(3)
        date_iso = f"{year}-{month}-{day}"

    doc_id = f"VE-{category}-{slug}"
    if len(doc_id) > 100:
        h = hashlib.md5(doc_id.encode()).hexdigest()[:8]
        doc_id = doc_id[:90] + "_" + h

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": f"{BASE_URL}{url_path.replace('/gdoc/', '/')}",
        "category": CATEGORIES.get(category, category),
    }


def fetch_all(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all documents across all categories."""
    limit = 15 if sample else None
    fetched = 0
    skipped = 0

    for category in CATEGORIES:
        doc_paths = list_documents(category, session)
        if not doc_paths:
            print(f"  {category}: no documents found")
            continue

        print(f"  {category}: {len(doc_paths)} documents")
        time.sleep(DELAY)

        for path in doc_paths:
            time.sleep(DELAY)
            url = f"{BASE_URL}{path}"
            html = fetch_page(url, session)

            if not html:
                skipped += 1
                continue

            text = extract_text_from_gdoc(html)
            page_title = extract_title_from_page(html)

            if not text or len(text) < 50:
                skipped += 1
                if skipped <= 10:
                    print(f"    No text: {path[:60]}")
                continue

            record = normalize(path, category, text, page_title)
            yield record
            fetched += 1

            if limit and fetched >= limit:
                print(f"  Sample complete: {fetched} fetched, {skipped} skipped")
                return

    print(f"  Total: {fetched} fetched, {skipped} skipped")


def save_record(record: dict, sample_dir: Path) -> None:
    """Save a record to the sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
    if len(safe_id) > 80:
        h = hashlib.md5(record["_id"].encode()).hexdigest()[:8]
        safe_id = safe_id[:70] + "_" + h
    filename = safe_id + ".json"
    filepath = sample_dir / filename
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def test_connectivity() -> bool:
    """Test that Justia Venezuela is reachable."""
    session = requests.Session()
    print("Testing Justia Venezuela connectivity...")

    # Test index page
    url = f"{BASE_URL}/federales/leyes/"
    html = fetch_page(url, session)
    if not html:
        print("FAIL: Cannot reach Justia Venezuela")
        return False

    links = re.findall(r'href="(/federales/leyes/[^"]+/gdoc/)"', html)
    print(f"  Index page: OK ({len(links)} law links)")

    # Test a document page
    if links:
        time.sleep(DELAY)
        doc_url = f"{BASE_URL}{links[0]}"
        doc_html = fetch_page(doc_url, session)
        if doc_html:
            text = extract_text_from_gdoc(doc_html)
            print(f"  Document page: OK ({len(text)} chars of text)")
        else:
            print("  Document page: FAIL")
            return False

    print("All tests passed.")
    return True


def bootstrap(sample: bool = False) -> None:
    """Run the bootstrap process."""
    session = requests.Session()
    sample_dir = SAMPLE_DIR
    records_saved = 0

    if sample:
        if sample_dir.exists():
            for f in sample_dir.glob("*.json"):
                f.unlink()

    print(f"{'Sample' if sample else 'Full'} bootstrap starting...")

    for record in fetch_all(session, sample=sample):
        save_record(record, sample_dir)
        records_saved += 1
        if records_saved % 50 == 0:
            print(f"  Saved {records_saved} records...")

    print(f"\nBootstrap complete: {records_saved} records saved to {sample_dir}")

    if records_saved == 0:
        print("ERROR: No records saved!")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="VE/TSJGaceta bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only ~15 sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)


if __name__ == "__main__":
    main()
