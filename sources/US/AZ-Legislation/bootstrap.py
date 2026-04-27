#!/usr/bin/env python3
"""
US/AZ-Legislation -- Arizona Revised Statutes

Fetches Arizona Revised Statutes from azleg.gov static HTML pages.
49 titles (2 repealed), sections served as individual .htm files.

Strategy:
  - GET /arsDetail?title=N to list chapters and section links
  - Parse section links from the detail page (pattern: /ars/T/SSSSS.htm)
  - GET each section .htm for full text
  - Clean HTML to extract statute text

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample sections
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

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/AZ-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AZ-Legislation")

BASE_URL = "https://www.azleg.gov"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 1.5  # seconds between requests

# All valid title numbers (2 and 24 are repealed)
TITLES = [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
          21, 22, 23, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
          39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49]


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
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


def fetch_url(url: str, params: dict = None) -> Optional[str]:
    """Fetch a URL with retry logic."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, params=params, timeout=30)
            if resp.status_code == 404:
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


def get_sections_for_title(title_num: int) -> list:
    """Get list of section URLs and names for a title."""
    html = fetch_url(f"{BASE_URL}/arsDetail", params={"title": title_num})
    if not html:
        return []

    sections = []
    # Links use /viewdocument/?docName=https://www.azleg.gov/ars/T/SSSSS.htm
    # Anchor tags have class/style attributes before href
    for match in re.finditer(
        r'href="/viewdocument/\?docName=https?://www\.azleg\.gov(/ars/\d+/[\w\-\.]+\.htm)"[^>]*>\s*([^<]+)',
        html, re.IGNORECASE
    ):
        url_path = match.group(1)
        sec_label = match.group(2).strip()
        sections.append((url_path, sec_label))

    return sections


def extract_section_title(html: str) -> str:
    """Extract the section title/heading from HTML."""
    # Look for heading elements
    m = re.search(r'<h\d[^>]*>([^<]+)</h\d>', html, re.IGNORECASE)
    if m:
        return unescape(m.group(1)).strip()
    # Look for bold text at start
    m = re.search(r'<b>([^<]+)</b>', html, re.IGNORECASE)
    if m:
        return unescape(m.group(1)).strip()
    return ""


def normalize(section_id: str, title_num: int, url_path: str,
              title_text: str, text: str) -> dict:
    """Normalize a section record."""
    return {
        "_id": f"US/AZ-Legislation/ARS-{section_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title_text or f"ARS § {section_id}",
        "text": text,
        "date": "2025-01-01",
        "url": f"{BASE_URL}{url_path}",
        "section_number": section_id,
        "title_number": title_num,
        "jurisdiction": "US-AZ",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all section records with full text."""
    total = 0
    for title_num in TITLES:
        logger.info(f"Processing Title {title_num}...")
        sections = get_sections_for_title(title_num)
        time.sleep(CRAWL_DELAY)

        if not sections:
            logger.warning(f"  No sections found for Title {title_num}")
            continue

        logger.info(f"  Found {len(sections)} sections")

        for url_path, sec_label in sections:
            html = fetch_url(f"{BASE_URL}{url_path}")
            time.sleep(CRAWL_DELAY)

            if not html:
                continue

            title_text = extract_section_title(html)
            text = clean_html(html)

            if not text or len(text) < 20:
                continue

            # Extract section number from label (e.g., "1-101" from "1-101")
            section_id = sec_label.replace("§", "").strip()

            record = normalize(section_id, title_num, url_path, title_text, text)
            total += 1
            if total % 100 == 0:
                logger.info(f"  Progress: {total} sections fetched")
            yield record

    logger.info(f"Total sections with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from a few titles."""
    records = []
    sample_titles = [1, 13, 28, 41, 49]

    for title_num in sample_titles:
        if len(records) >= count:
            break

        logger.info(f"Sampling Title {title_num}...")
        sections = get_sections_for_title(title_num)
        time.sleep(CRAWL_DELAY)

        if not sections:
            continue

        for url_path, sec_label in sections[:4]:
            if len(records) >= count:
                break

            html = fetch_url(f"{BASE_URL}{url_path}")
            time.sleep(CRAWL_DELAY)

            if not html:
                continue

            title_text = extract_section_title(html)
            text = clean_html(html)

            if not text or len(text) < 20:
                continue

            section_id = sec_label.replace("§", "").strip()
            record = normalize(section_id, title_num, url_path, title_text, text)
            records.append(record)
            logger.info(f"  [{len(records)}] ARS § {section_id} - {len(text)} chars")

    return records


def test_api():
    """Test connectivity to azleg.gov."""
    logger.info("Testing Arizona Legislature connectivity...")

    html = fetch_url(f"{BASE_URL}/arstitle/")
    if not html:
        logger.error("Title listing page unreachable")
        return False
    logger.info(f"Title listing OK - {len(html)} bytes")

    time.sleep(CRAWL_DELAY)
    sections = get_sections_for_title(1)
    if not sections:
        logger.error("No sections found for Title 1")
        return False
    logger.info(f"Title 1 detail OK - {len(sections)} sections")

    time.sleep(CRAWL_DELAY)
    url_path, sec_label = sections[0]
    html = fetch_url(f"{BASE_URL}{url_path}")
    if not html:
        logger.error("Section page failed")
        return False

    text = clean_html(html)
    logger.info(f"Section OK - {sec_label}: {len(text)} chars")
    if text:
        logger.info(f"Preview: {text[:200]}...")
        return True

    logger.error("No text extracted")
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

    return len(records) >= 10 and avg_text > 200


def main():
    parser = argparse.ArgumentParser(description="US/AZ-Legislation Data Fetcher")
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
