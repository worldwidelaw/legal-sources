#!/usr/bin/env python3
"""
VE/AsambaleaNacional - Venezuela Asamblea Nacional Legislation Fetcher

Fetches Venezuelan laws from the official Asamblea Nacional website.
Each law has a detail page with metadata and a downloadable PDF containing
the full text as published in the Gaceta Oficial.

Data source: https://www.asambleanacional.gob.ve/leyes/vigentes
Method: HTML scraping of listing pages + PDF download & text extraction
License: Public Venezuelan legislation
Rate limit: ~2 seconds between requests

Categories scraped:
  - vigentes  (current/active laws)
  - sancionadas (enacted laws)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import hashlib
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

SOURCE_ID = "VE/AsambaleaNacional"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://www.asambleanacional.gob.ve"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-VE,es;q=0.9,en;q=0.5",
}

DELAY = 2  # seconds between requests

# Listing categories to scrape (page 1 of each)
CATEGORIES = ["vigentes", "sancionadas"]

# Maximum pagination pages to attempt per category
MAX_PAGES = 60


def fetch_page(url: str, session: requests.Session) -> Optional[str]:
    """Fetch a page with retries."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200:
                resp.encoding = "utf-8"
                return resp.text
            if resp.status_code == 403:
                # WAF block on paginated requests
                return None
            if resp.status_code == 404:
                return None
            if resp.status_code >= 500:
                time.sleep(DELAY * 2)
                continue
            return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt + 1}): {e}", file=sys.stderr)
            time.sleep(DELAY)
    return None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    if pdfplumber is None:
        # Try common/pdf_extract as fallback
        try:
            from common.pdf_extract import extract_pdf_markdown
            return ""  # Can't use extract_pdf_markdown with raw bytes easily
        except ImportError:
            pass
        return ""

    try:
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
        pages_text = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
        pdf.close()
        return "\n\n".join(pages_text)
    except Exception as e:
        print(f"  PDF extraction error: {e}", file=sys.stderr)
        return ""


def download_pdf(url: str, session: requests.Session) -> Optional[bytes]:
    """Download a PDF file."""
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=60)
            if resp.status_code == 200 and len(resp.content) > 100:
                return resp.content
            if resp.status_code >= 500:
                time.sleep(DELAY * 2)
                continue
            return None
        except requests.RequestException as e:
            print(f"  PDF download error (attempt {attempt + 1}): {e}", file=sys.stderr)
            time.sleep(DELAY)
    return None


def list_laws_from_category(
    category: str, session: requests.Session, max_pages: int = MAX_PAGES
) -> list[dict]:
    """List all law entries from a category's listing pages."""
    laws = []
    seen_slugs = set()

    for page_num in range(1, max_pages + 1):
        if page_num == 1:
            url = f"{BASE_URL}/leyes/{category}"
        else:
            url = f"{BASE_URL}/leyes/{category}?page={page_num}"

        html = fetch_page(url, session)
        if not html:
            if page_num > 1:
                break  # WAF block on pagination, stop
            continue

        # Extract law cards
        # Pattern: date, gazette number, link with title
        card_pattern = re.compile(
            r'Fecha:\s*(\d{2}/\d{2}/\d{4}).*?'
            r'Gaceta\s+N[ºo°]\s*<b>([^<]*)</b>.*?'
            r'href="(https://www\.asambleanacional\.gob\.ve/leyes/sancionadas/([^"]+))"[^>]*>'
            r'\s*<b>([^<]+)</b>',
            re.DOTALL,
        )

        page_laws = 0
        for m in card_pattern.finditer(html):
            date_str, gazette, detail_url, slug, title = (
                m.group(1),
                m.group(2).strip(),
                m.group(3),
                m.group(4),
                unescape(m.group(5).strip()),
            )
            if slug in seen_slugs:
                continue
            seen_slugs.add(slug)

            # Parse date DD/MM/YYYY -> YYYY-MM-DD
            try:
                parts = date_str.split("/")
                date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except (IndexError, ValueError):
                date_iso = None

            laws.append(
                {
                    "slug": slug,
                    "title": title,
                    "date": date_iso,
                    "gazette_number": gazette,
                    "detail_url": detail_url,
                    "category": category,
                }
            )
            page_laws += 1

        print(f"  {category} page {page_num}: {page_laws} laws")
        if page_laws == 0:
            break
        time.sleep(DELAY)

    return laws


def fetch_detail_and_pdf(law: dict, session: requests.Session) -> Optional[dict]:
    """Fetch a law's detail page to find PDF URL, then download and extract text."""
    detail_html = fetch_page(law["detail_url"], session)
    if not detail_html:
        return None

    # Find PDF link on detail page
    pdf_match = re.search(
        r'href="(https://www\.asambleanacional\.gob\.ve/storage/documentos/leyes/[^"]+\.pdf)"',
        detail_html,
    )
    if not pdf_match:
        # Try relative PDF link
        pdf_match = re.search(
            r'href="(/storage/documentos/leyes/[^"]+\.pdf)"', detail_html
        )
        if pdf_match:
            pdf_url = BASE_URL + pdf_match.group(1)
        else:
            print(f"  No PDF found for {law['slug']}", file=sys.stderr)
            return None
    else:
        pdf_url = pdf_match.group(1)

    time.sleep(DELAY)

    # Download PDF
    pdf_bytes = download_pdf(pdf_url, session)
    if not pdf_bytes:
        print(f"  Failed to download PDF for {law['slug']}", file=sys.stderr)
        return None

    # Extract text
    text = extract_pdf_text(pdf_bytes)
    if not text or len(text) < 50:
        print(f"  Empty/short text for {law['slug']}", file=sys.stderr)
        return None

    return {**law, "text": text, "pdf_url": pdf_url}


def normalize(raw: dict) -> dict:
    """Normalize a raw law record into the standard schema."""
    slug = raw["slug"]
    doc_id = f"VE-AN-{slug}"
    if len(doc_id) > 120:
        h = hashlib.md5(doc_id.encode()).hexdigest()[:8]
        doc_id = doc_id[:110] + "_" + h

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw["title"],
        "text": raw["text"],
        "date": raw.get("date"),
        "gazette_number": raw.get("gazette_number"),
        "url": raw["detail_url"],
        "pdf_url": raw.get("pdf_url", ""),
    }


def fetch_all(
    session: requests.Session, sample: bool = False
) -> Generator[dict, None, None]:
    """Fetch all laws across categories."""
    all_laws = []
    for cat in CATEGORIES:
        print(f"Listing {cat}...")
        laws = list_laws_from_category(cat, session, max_pages=1 if sample else MAX_PAGES)
        all_laws.extend(laws)
        print(f"  Found {len(laws)} laws in {cat}")

    # Deduplicate by slug
    seen = set()
    unique = []
    for law in all_laws:
        if law["slug"] not in seen:
            seen.add(law["slug"])
            unique.append(law)
    all_laws = unique

    print(f"\nTotal unique laws to fetch: {len(all_laws)}")
    limit = 15 if sample else None

    fetched = 0
    for law in all_laws:
        if limit and fetched >= limit:
            break

        print(f"  [{fetched + 1}/{len(all_laws)}] {law['title'][:60]}...")
        result = fetch_detail_and_pdf(law, session)
        if result:
            record = normalize(result)
            fetched += 1
            yield record
        time.sleep(DELAY)

    print(f"\nFetched {fetched} laws with full text")


def save_sample(record: dict) -> None:
    """Save a record to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    filename = re.sub(r"[^\w\-]", "_", record["_id"])[:80] + ".json"
    path = SAMPLE_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def test_connectivity() -> bool:
    """Test that the data source is accessible."""
    session = requests.Session()
    html = fetch_page(f"{BASE_URL}/leyes/vigentes", session)
    if not html:
        print("FAIL: Cannot reach Asamblea Nacional website")
        return False

    links = re.findall(
        r'href="https://www\.asambleanacional\.gob\.ve/leyes/sancionadas/[^"]+"', html
    )
    if not links:
        print("FAIL: No law links found on vigentes page")
        return False

    print(f"OK: Found {len(links)} law links on vigentes page 1")
    return True


def main():
    parser = argparse.ArgumentParser(description="VE/AsambaleaNacional bootstrapper")
    parser.add_argument(
        "command", choices=["bootstrap", "test"], help="Command to run"
    )
    parser.add_argument(
        "--sample", action="store_true", help="Fetch only sample records"
    )
    parser.add_argument(
        "--full", action="store_true", help="Full bootstrap (all pages)"
    )
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)

    if args.command == "bootstrap":
        session = requests.Session()
        sample = args.sample or not args.full
        count = 0
        for record in fetch_all(session, sample=sample):
            if sample:
                save_sample(record)
            text_len = len(record.get("text", ""))
            print(f"    Saved: {record['title'][:50]}... ({text_len} chars)")
            count += 1

        print(f"\nDone. {count} records saved.")
        if sample:
            print(f"Sample files in: {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
