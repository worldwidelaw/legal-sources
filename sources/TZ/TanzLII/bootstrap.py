#!/usr/bin/env python3
"""
TZ/TanzLII -- Tanzania Legal Information Institute

Fetches Tanzanian legislation from tanzlii.org (Laws.Africa / AfricanLII).
6,054 legislative documents: principal acts, subsidiary legislation, government notices.

Strategy:
  - Iterate /en/legislation/all?year=YYYY for each year (1920-present)
  - Parse listing tables to discover document URLs
  - Fetch individual document pages for full text from Akoma Ntoso HTML
  - Respect 5-second crawl delay per robots.txt

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full extraction
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "TZ/TanzLII"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TZ.TanzLII")

BASE_URL = "https://tanzlii.org"
LEGISLATION_URL = f"{BASE_URL}/en/legislation/all"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 5  # seconds, per robots.txt


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</h\d>', '\n\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def fetch_page(url: str) -> Optional[str]:
    """Fetch an HTML page with retry."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            if resp.status_code == 403:
                logger.warning(f"403 Forbidden: {url}")
                return None
            resp.raise_for_status()
            return resp.text
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None


def parse_listing_page(html: str) -> list:
    """Parse a legislation listing page to extract document links."""
    entries = []

    # Find document rows - look for links to /en/akn/tz/act/ patterns
    for match in re.finditer(
        r'<a\s+[^>]*href="(/en/akn/tz/(?:act|legislation)[^"]*)"[^>]*>(.*?)</a>',
        html, re.DOTALL | re.IGNORECASE
    ):
        path = match.group(1)
        title = clean_html(match.group(2)).strip()
        if not title or len(title) < 3:
            continue
        # Skip fragment/section links
        if '#' in path:
            continue
        full_url = urljoin(BASE_URL, path)
        entries.append({"url": full_url, "title": title})

    # Deduplicate by URL
    seen = set()
    unique = []
    for e in entries:
        if e["url"] not in seen:
            seen.add(e["url"])
            unique.append(e)

    return unique


def get_next_page_url(html: str, current_url: str) -> Optional[str]:
    """Find the next page URL from pagination."""
    # Look for "next" pagination link
    next_match = re.search(
        r'<a[^>]*href="([^"]*)"[^>]*>\s*(?:Next|›|»)\s*</a>',
        html, re.IGNORECASE
    )
    if next_match:
        return urljoin(current_url, next_match.group(1))
    return None


def extract_document_text(html: str) -> str:
    """Extract full text from a TanzLII document page."""
    # Primary: Akoma Ntoso content div
    akn_match = re.search(
        r'<div[^>]*class="[^"]*la-akoma-ntoso[^"]*"[^>]*>(.*?)</div>\s*(?:</div>|<div[^>]*class="[^"]*enrichment)',
        html, re.DOTALL | re.IGNORECASE
    )
    if akn_match:
        return clean_html(akn_match.group(1))

    # Broader match: content-and-enrichments
    content_match = re.search(
        r'<div[^>]*class="[^"]*content-and-enrichments__inner[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        html, re.DOTALL | re.IGNORECASE
    )
    if content_match:
        return clean_html(content_match.group(1))

    # Fallback: look for any large block of text in article/main content
    article = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL | re.IGNORECASE)
    if article:
        text = clean_html(article.group(1))
        if len(text) > 200:
            return text

    return ""


def extract_metadata(html: str) -> dict:
    """Extract metadata from meta tags."""
    meta = {}
    # og:title
    m = re.search(r'<meta[^>]*property="og:title"[^>]*content="([^"]*)"', html, re.IGNORECASE)
    if m:
        meta["title"] = unescape(m.group(1))

    # article:published_time
    m = re.search(r'<meta[^>]*property="article:published_time"[^>]*content="([^"]*)"', html, re.IGNORECASE)
    if m:
        meta["date"] = m.group(1)[:10]  # YYYY-MM-DD

    # Try date from breadcrumb or content
    m = re.search(r'(\d{4}-\d{2}-\d{2})', html[:5000])
    if m and "date" not in meta:
        meta["date"] = m.group(1)

    return meta


def extract_akn_id(url: str) -> str:
    """Extract an AKN-based ID from the URL."""
    # /en/akn/tz/act/2023/15/eng@2023-10-20 -> tz-act-2023-15
    m = re.search(r'/akn/tz/([^/]+(?:/[^/]+)*)/eng@', url)
    if m:
        return "tz-" + m.group(1).replace("/", "-")
    # Fallback
    m = re.search(r'/akn/tz/(.+?)(?:/eng|$)', url)
    if m:
        return "tz-" + m.group(1).replace("/", "-")
    return url.split("/")[-1]


def fetch_legislation_by_year(year: int, limit: int = 0) -> list:
    """Fetch all legislation for a given year."""
    records = []
    url = f"{LEGISLATION_URL}?year={year}"
    page_num = 0

    while url and page_num < 10:  # Max 10 pages per year
        html = fetch_page(url)
        if not html:
            break

        entries = parse_listing_page(html)
        if not entries:
            break

        for entry in entries:
            if limit and len(records) >= limit:
                return records

            time.sleep(CRAWL_DELAY)
            doc_html = fetch_page(entry["url"])
            if not doc_html:
                continue

            text = extract_document_text(doc_html)
            if not text or len(text) < 100:
                logger.debug(f"  Skipped (no text): {entry['title'][:50]}")
                continue

            meta = extract_metadata(doc_html)
            akn_id = extract_akn_id(entry["url"])

            record = {
                "_id": f"TZ/TanzLII/{akn_id}",
                "_source": SOURCE_ID,
                "_type": "legislation",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": meta.get("title", entry["title"]),
                "text": text,
                "date": meta.get("date", f"{year}-01-01"),
                "url": entry["url"],
                "year": year,
                "language": "en",
            }
            records.append(record)
            logger.info(f"  [{len(records)}] {record['title'][:60]}... ({len(text)} chars)")

        # Check for next page
        url = get_next_page_url(html, url)
        page_num += 1
        if url:
            time.sleep(CRAWL_DELAY)

    return records


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from recent years."""
    records = []
    current_year = datetime.now().year

    for year in range(current_year, current_year - 10, -1):
        if len(records) >= count:
            break
        logger.info(f"Checking year {year}...")
        remaining = count - len(records)
        year_records = fetch_legislation_by_year(year, limit=remaining)
        records.extend(year_records)
        time.sleep(CRAWL_DELAY)

    return records[:count]


def fetch_all() -> Generator[dict, None, None]:
    """Yield all legislation documents."""
    current_year = datetime.now().year
    for year in range(current_year, 1919, -1):
        logger.info(f"Processing year {year}...")
        records = fetch_legislation_by_year(year)
        for r in records:
            yield r
        if records:
            logger.info(f"  Year {year}: {len(records)} documents")
        time.sleep(CRAWL_DELAY)


def test_api():
    """Test connectivity to TanzLII."""
    logger.info("Testing TanzLII connectivity...")

    # Test homepage
    html = fetch_page(BASE_URL)
    if html:
        logger.info(f"Homepage OK - {len(html)} bytes")
    else:
        logger.error("Homepage unreachable")
        return False

    # Test legislation listing
    html = fetch_page(f"{LEGISLATION_URL}?year=2024")
    if html:
        entries = parse_listing_page(html)
        logger.info(f"Legislation listing OK - {len(entries)} entries for 2024")
    else:
        logger.error("Legislation listing failed")
        return False

    # Test a document page
    if entries:
        time.sleep(CRAWL_DELAY)
        doc_html = fetch_page(entries[0]["url"])
        if doc_html:
            text = extract_document_text(doc_html)
            meta = extract_metadata(doc_html)
            title = meta.get("title", entries[0]["title"])
            logger.info(f"Document OK - '{title[:60]}' - {len(text)} chars")
            if text:
                logger.info(f"Preview: {text[:200]}...")
                return True
            else:
                logger.error("No text extracted from document")
                return False

    return False


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
        filename = f"sample_{i:02d}_{safe_id}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")

    years = set(r.get("year", 0) for r in records)
    logger.info(f"  - Years covered: {min(years)}-{max(years)}")

    return len(records) >= 10 and avg_text > 200


def main():
    parser = argparse.ArgumentParser(description="TZ/TanzLII Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            logger.info("Full bootstrap mode")
            count = 0
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            for record in fetch_all():
                count += 1
                safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
