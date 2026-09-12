#!/usr/bin/env python3
"""
KG/ConstitutionalCourt - Kyrgyzstan Constitutional Court Decisions

Data source: https://constsot.kg
Format: WordPress REST API (JSON) + PDF attachments for full text
License: Public Domain (government constitutional court decisions)
Records: ~958 constitutional court decisions (1995–present)

The Constitutional Chamber of the Supreme Court of the Kyrgyz Republic
publishes decisions on a WordPress site. The WP REST API provides
structured access; full text is in PDF documents linked from post titles.
"""

import argparse
import io
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Dict, Generator, List, Optional

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    PDF_EXTRACTOR = "pdfplumber"
except ImportError:
    try:
        import PyPDF2
        PDF_EXTRACTOR = "PyPDF2"
    except ImportError:
        PDF_EXTRACTOR = None

# Configuration
SOURCE_ID = "KG/ConstitutionalCourt"
BASE_URL = "https://constsot.kg"
API_URL = f"{BASE_URL}/kg/wp-json/wp/v2/posts"
CATEGORY_AKTY = 4  # Parent category for all court acts
REQUEST_DELAY = 1.5  # seconds between requests
PER_PAGE = 100

# Sub-category mapping (ID -> type name)
CATEGORY_TYPES = {
    5: "decision",           # Решения
    7: "resolution",         # Постановления
    6: "determination",      # Определения
    8: "conclusion",         # Заключения
    9: "collegial_determination",  # Определения коллегии
    43: "accepted",          # О принятии к производству
    44: "refused",           # Об отказе в принятии
}


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "application/json",
        "Accept-Language": "ru-RU,ru;q=0.9,ky;q=0.8,en;q=0.7",
    })
    return session


def extract_pdf_url(title_rendered: str) -> Optional[str]:
    """Extract PDF URL from the title.rendered field which contains an <a> tag."""
    if not title_rendered:
        return None
    match = re.search(r'href="([^"]+\.pdf)"', title_rendered, re.IGNORECASE)
    if match:
        return match.group(1)
    # Also check for non-.pdf links that might be document downloads
    match = re.search(r'href="([^"]+/uploads/[^"]+)"', title_rendered)
    if match:
        return match.group(1)
    return None


def extract_title_text(title_rendered: str) -> str:
    """Extract clean title text from the title.rendered field."""
    if not title_rendered:
        return ""
    soup = BeautifulSoup(title_rendered, "html.parser")
    text = soup.get_text(separator=" ")
    text = unescape(text)
    # Remove language markers like (Русский)
    text = re.sub(r"\(Русский\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using available library."""
    if PDF_EXTRACTOR == "pdfplumber":
        return _extract_with_pdfplumber(pdf_bytes)
    elif PDF_EXTRACTOR == "PyPDF2":
        return _extract_with_pypdf2(pdf_bytes)
    else:
        return ""


def _extract_with_pdfplumber(pdf_bytes: bytes) -> str:
    """Extract text using pdfplumber."""
    text_parts = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
    except Exception as e:
        print(f"  pdfplumber error: {e}", file=sys.stderr)
    return "\n\n".join(text_parts)


def _extract_with_pypdf2(pdf_bytes: bytes) -> str:
    """Extract text using PyPDF2."""
    text_parts = []
    try:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    except Exception as e:
        print(f"  PyPDF2 error: {e}", file=sys.stderr)
    return "\n\n".join(text_parts)


def download_pdf(session: requests.Session, url: str) -> Optional[bytes]:
    """Download a PDF file, return bytes or None on failure."""
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "")
        if "pdf" in content_type or "octet-stream" in content_type or url.endswith(".pdf"):
            return resp.content
        return None
    except Exception as e:
        print(f"  PDF download error for {url}: {e}", file=sys.stderr)
        return None


def classify_decision(categories: List[int]) -> str:
    """Classify decision type based on category IDs."""
    for cat_id in categories:
        if cat_id in CATEGORY_TYPES:
            return CATEGORY_TYPES[cat_id]
    return "unknown"


def fetch_posts_page(session: requests.Session, page: int = 1,
                     category: int = CATEGORY_AKTY) -> tuple:
    """Fetch a page of posts from the WP REST API. Returns (posts, total_posts)."""
    params = {
        "categories": category,
        "per_page": PER_PAGE,
        "page": page,
        "orderby": "date",
        "order": "asc",
    }
    try:
        resp = session.get(API_URL, params=params, timeout=30)
        resp.raise_for_status()
        total = int(resp.headers.get("X-WP-Total", 0))
        return resp.json(), total
    except Exception as e:
        print(f"  Error fetching page {page}: {e}", file=sys.stderr)
        return [], 0


def normalize(raw: Dict, full_text: str) -> Dict:
    """Transform raw WP post + extracted text into standard schema."""
    post_id = raw.get("id", "")

    # Title
    title = extract_title_text(raw.get("title", {}).get("rendered", ""))
    if not title:
        title = f"Constitutional Court Decision {post_id}"

    # Date — use the post date from WP
    date_str = raw.get("date_gmt") or raw.get("date") or ""
    parsed_date = None
    if date_str:
        match = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        if match:
            parsed_date = match.group(1)
            # Sanity check for obviously wrong dates
            try:
                year = int(parsed_date[:4])
                if year < 1993 or year > 2030:
                    parsed_date = None
            except ValueError:
                parsed_date = None

    # Decision type from categories
    categories = raw.get("categories", [])
    decision_type = classify_decision(categories)

    # URL
    url = raw.get("link") or f"{BASE_URL}/?p={post_id}"

    # Clean text
    clean_text = re.sub(r"[ \t]+", " ", full_text)
    clean_text = re.sub(r"\n\s*\n+", "\n\n", clean_text)
    clean_text = clean_text.strip()

    return {
        "_id": f"KG-CC-{post_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": clean_text,
        "date": parsed_date,
        "url": url,
        "decision_type": decision_type,
        "pdf_url": extract_pdf_url(raw.get("title", {}).get("rendered", "")),
    }


def fetch_all() -> Generator[Dict, None, None]:
    """Fetch all constitutional court decisions with full text."""
    session = get_session()
    page = 1
    total_yielded = 0
    total_skipped = 0

    # First request to get total
    posts, total_posts = fetch_posts_page(session, page=1)
    if not posts:
        print("ERROR: Could not fetch posts from WP REST API", file=sys.stderr)
        return

    print(f"  Total posts in akty category: {total_posts}")
    total_pages = (total_posts + PER_PAGE - 1) // PER_PAGE

    while True:
        if page > 1:
            posts, _ = fetch_posts_page(session, page=page)

        if not posts:
            break

        for post in posts:
            pdf_url = extract_pdf_url(post.get("title", {}).get("rendered", ""))
            if not pdf_url:
                total_skipped += 1
                continue

            time.sleep(0.5)  # Be polite with PDF downloads
            pdf_bytes = download_pdf(session, pdf_url)
            if not pdf_bytes:
                total_skipped += 1
                continue

            full_text = extract_text_from_pdf(pdf_bytes)
            if len(full_text) < 100:
                total_skipped += 1
                continue

            record = normalize(post, full_text)
            yield record
            total_yielded += 1

        print(f"  Page {page}/{total_pages} done — yielded: {total_yielded}, skipped: {total_skipped}")

        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_DELAY)

    print(f"  Total records yielded: {total_yielded}")
    print(f"  Total skipped (no PDF / no text): {total_skipped}")


def fetch_updates(since: datetime) -> Generator[Dict, None, None]:
    """Fetch decisions modified since a given date."""
    session = get_session()
    since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
    page = 1

    while True:
        params = {
            "categories": CATEGORY_AKTY,
            "per_page": PER_PAGE,
            "page": page,
            "after": since_iso,
            "orderby": "date",
            "order": "asc",
        }
        try:
            resp = session.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
            posts = resp.json()
            total = int(resp.headers.get("X-WP-Total", 0))
        except Exception as e:
            print(f"  Error fetching updates page {page}: {e}", file=sys.stderr)
            break

        if not posts:
            break

        for post in posts:
            pdf_url = extract_pdf_url(post.get("title", {}).get("rendered", ""))
            if not pdf_url:
                continue

            time.sleep(0.5)
            pdf_bytes = download_pdf(session, pdf_url)
            if not pdf_bytes:
                continue

            full_text = extract_text_from_pdf(pdf_bytes)
            if len(full_text) < 100:
                continue

            yield normalize(post, full_text)

        total_pages = (total + PER_PAGE - 1) // PER_PAGE
        if page >= total_pages:
            break
        page += 1
        time.sleep(REQUEST_DELAY)


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    if not PDF_EXTRACTOR:
        print("ERROR: No PDF extraction library available (need pdfplumber or PyPDF2)")
        sys.exit(1)

    session = get_session()

    print(f"Using PDF extractor: {PDF_EXTRACTOR}")
    print("Fetching constitutional court decisions...")

    posts, total_posts = fetch_posts_page(session, page=1)
    if not posts:
        print("ERROR: Could not fetch posts from WP REST API")
        return

    print(f"Total available: {total_posts} posts in akty category")
    print(f"First page returned: {len(posts)} posts")

    sample_dir.mkdir(parents=True, exist_ok=True)

    records_saved = 0
    records_attempted = 0
    total_text_chars = 0
    page = 1

    while records_saved < count:
        if page > 1:
            print(f"\n  Fetching page {page} for more samples...")
            time.sleep(REQUEST_DELAY)
            posts, _ = fetch_posts_page(session, page=page)
            if not posts:
                break

        for post in posts:
            if records_saved >= count:
                break

            records_attempted += 1
            title_rendered = post.get("title", {}).get("rendered", "")
            pdf_url = extract_pdf_url(title_rendered)

            if not pdf_url:
                title_text = extract_title_text(title_rendered)
                print(f"  Skipping post {post.get('id')} — no PDF URL found: {title_text[:60]}")
                continue

            print(f"  Downloading PDF: {pdf_url.split('/')[-1]}")
            time.sleep(0.5)
            pdf_bytes = download_pdf(session, pdf_url)
            if not pdf_bytes:
                print(f"  Skipping — PDF download failed")
                continue

            full_text = extract_text_from_pdf(pdf_bytes)
            text_len = len(full_text)

            if text_len < 100:
                print(f"  Skipping — text too short ({text_len} chars, may be scanned PDF)")
                continue

            record = normalize(post, full_text)
            total_text_chars += text_len
            records_saved += 1

            safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
            filename = f"{safe_name}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  [{records_saved}/{count}] {filename}")
            print(f"    Title: {record.get('title', '?')[:80]}")
            print(f"    Text: {text_len:,} chars | Date: {record.get('date', '?')} | Type: {record.get('decision_type', '?')}")

        page += 1

    # Summary
    print("\n" + "=" * 60)
    print("SAMPLE SUMMARY")
    print("=" * 60)
    print(f"Records attempted: {records_attempted}")
    print(f"Records saved: {records_saved}")
    if records_saved > 0:
        avg_chars = total_text_chars // records_saved
        print(f"Total text chars: {total_text_chars:,}")
        print(f"Average text length: {avg_chars:,} chars/doc")
    print(f"Sample directory: {sample_dir}")

    if records_saved >= 10:
        print("\nSUCCESS: 10+ sample records with full text")
    else:
        print(f"\nWARNING: Only {records_saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(description="Kyrgyzstan Constitutional Court Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true",
                        help="Run full bootstrap (all records)")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Running full bootstrap...")
            sample_dir.mkdir(parents=True, exist_ok=True)
            records_saved = 0
            for record in fetch_all():
                safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])[:100]
                filename = f"{safe_name}.json"
                filepath = sample_dir / filename

                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                records_saved += 1
                if records_saved % 50 == 0:
                    print(f"  Saved {records_saved} records...")

            print(f"\nFull bootstrap complete: {records_saved} records saved")

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
