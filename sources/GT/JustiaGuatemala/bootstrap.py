#!/usr/bin/env python3
"""
GT/JustiaGuatemala -- Guatemala National Legislation via Justia

Fetches Guatemalan decrees (Decretos del Congreso) from Justia Guatemala.
~338 national laws spanning 1952-2009.

Strategy:
  - Scrape index page at guatemala.justia.com/nacionales/leyes/
  - Extract decree links, titles, and dates from HTML
  - Download PDFs from docs.costa-rica.justia.com and extract full text
  - PDFs are text-based (not scanned images)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Quick connectivity test
"""

import hashlib
import json
import re
import sys
import time
import logging
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GT.JustiaGuatemala")

SOURCE_ID = "GT/JustiaGuatemala"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://guatemala.justia.com"
INDEX_URL = f"{BASE_URL}/nacionales/leyes/"
DELAY = 2  # seconds between requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html, */*",
}

MONTHS_ES = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


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
            logger.warning(f"Request error (attempt {attempt + 1}): {e}")
            time.sleep(DELAY)
    return None


def parse_index_page(html: str) -> list[dict]:
    """Parse the index page to extract decree entries with title, date, and URL slug."""
    entries = []
    # Pattern: <li><span class="law_name"><a href="/nacionales/leyes/SLUG/gdoc/">
    #   <span class="law_name">TITLE - <span class="law_details">DETAILS</span></span>
    #   </a></span> - <span class="law_published">DATE</span></li>
    pattern = re.compile(
        r'<a\s+href="(/nacionales/leyes/([^"]+)/gdoc/)"[^>]*>'
        r'\s*<span class="law_name">([^<]*?)(?:\s*-\s*<span class="law_details">([^<]*)</span>)?'
        r'</span></a></span>\s*-\s*<span class="law_published">([^<]*)</span>',
        re.DOTALL,
    )

    for m in pattern.finditer(html):
        path = m.group(1)
        slug = m.group(2)
        decree_part = unescape(m.group(3).strip())
        details = unescape(m.group(4).strip()) if m.group(4) else ""
        date_str = m.group(5).strip()

        title = decree_part
        if details:
            title = f"{decree_part} - {details}"

        # Parse date like "Aug 10 2009"
        date_iso = None
        date_match = re.match(r"(\w{3})\s+(\d{1,2})\s+(\d{4})", date_str)
        if date_match:
            month = MONTHS_ES.get(date_match.group(1).lower(), "01")
            day = date_match.group(2).zfill(2)
            year = date_match.group(3)
            date_iso = f"{year}-{month}-{day}"

        entries.append({
            "path": path,
            "slug": slug,
            "title": title,
            "date": date_iso,
            "date_raw": date_str,
        })

    return entries


def get_pdf_url(slug: str, html: Optional[str] = None) -> str:
    """Get the PDF URL for a decree.

    Try to extract from gdoc page first, fall back to constructed URL.
    """
    if html:
        match = re.search(r'href="(https?://[^"]*\.pdf)"', html)
        if match:
            return match.group(1)

    # Construct URL based on observed pattern
    return f"http://docs.costa-rica.justia.com/nacionales/leyes/{slug}.pdf"


def normalize(entry: dict, text: str) -> dict:
    """Normalize a decree record."""
    slug = entry["slug"]
    doc_id = f"GT-ley-{slug}"
    if len(doc_id) > 100:
        h = hashlib.md5(doc_id.encode()).hexdigest()[:8]
        doc_id = doc_id[:90] + "_" + h

    # Extract decree number from title
    decree_match = re.match(r"(Decreto No \d+[-/]\d+)", entry["title"])
    decree_number = decree_match.group(1) if decree_match else ""

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": entry["title"],
        "text": text,
        "date": entry["date"],
        "url": f"{BASE_URL}/nacionales/leyes/{slug}/",
        "decree_number": decree_number,
        "pdf_url": entry.get("pdf_url", ""),
    }


def fetch_all(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all Guatemala decrees."""
    limit = 15 if sample else None
    fetched = 0
    skipped_no_text = 0

    logger.info(f"Fetching index page: {INDEX_URL}")
    html = fetch_page(INDEX_URL, session)
    if not html:
        logger.error("Failed to fetch index page")
        return

    entries = parse_index_page(html)
    logger.info(f"Found {len(entries)} decrees on index page")

    for i, entry in enumerate(entries):
        time.sleep(DELAY)

        # Fetch gdoc page to get exact PDF URL
        gdoc_url = f"{BASE_URL}{entry['path']}"
        gdoc_html = fetch_page(gdoc_url, session)
        pdf_url = get_pdf_url(entry["slug"], gdoc_html)
        entry["pdf_url"] = pdf_url

        if (i + 1) % 20 == 0:
            logger.info(f"[{i+1}/{len(entries)}] Processing: {entry['title'][:60]}")

        # Extract text from PDF
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=f"GT-ley-{entry['slug']}",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

        if not text or len(text) < 50:
            skipped_no_text += 1
            if skipped_no_text <= 10:
                logger.warning(f"No text extracted: {entry['title'][:60]}")
            continue

        # Cap text to prevent OOM
        if len(text) > 500_000:
            text = text[:500_000]

        record = normalize(entry, text)
        yield record
        fetched += 1

        if limit and fetched >= limit:
            logger.info(f"Sample complete: {fetched} fetched, {skipped_no_text} skipped")
            return

    logger.info(f"Total: {fetched} fetched, {skipped_no_text} skipped (no text)")


def save_record(record: dict, sample_dir: Path) -> None:
    """Save a record to the sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
    if len(safe_id) > 80:
        h = hashlib.md5(record["_id"].encode()).hexdigest()[:8]
        safe_id = safe_id[:70] + "_" + h
    filepath = sample_dir / f"{safe_id}.json"
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def test_connectivity() -> bool:
    """Test that Justia Guatemala is reachable and PDFs are extractable."""
    session = requests.Session()
    logger.info("Testing Justia Guatemala connectivity...")

    html = fetch_page(INDEX_URL, session)
    if not html:
        logger.error("FAIL: Cannot reach Justia Guatemala")
        return False

    entries = parse_index_page(html)
    logger.info(f"Index page: OK ({len(entries)} decree links)")

    if entries:
        entry = entries[0]
        gdoc_url = f"{BASE_URL}{entry['path']}"
        gdoc_html = fetch_page(gdoc_url, session)
        pdf_url = get_pdf_url(entry["slug"], gdoc_html)
        logger.info(f"Sample decree: {entry['title'][:60]}")
        logger.info(f"PDF URL: {pdf_url}")

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=f"test-{entry['slug']}",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""
        logger.info(f"PDF text: {len(text)} chars")
        if text:
            logger.info(f"Preview: {text[:200]}")

    logger.info("Connectivity test passed!")
    return True


def main():
    import argparse
    parser = argparse.ArgumentParser(description="GT/JustiaGuatemala data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    session = requests.Session()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_mode = args.sample
        count = 0
        for record in fetch_all(session, sample=sample_mode):
            save_record(record, SAMPLE_DIR)
            count += 1
        logger.info(f"Saved {count} records to {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
