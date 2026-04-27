#!/usr/bin/env python3
"""
US/AK-Legislation -- Alaska Statutes

Fetches Alaska Statutes from akleg.gov via their internal AJAX endpoints.
47 titles, ~700+ chapters of consolidated statutory law.

Strategy:
  - GET /basis/statutes.asp (main page) to discover all 47 title numbers
  - For each title, GET ?media=js&type=TOC&title=N to list chapters
  - For each chapter, GET ?media=print&secStart=CH&secEnd=CH for full text
  - Parse HTML to extract section text and metadata

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample chapters
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

SOURCE_ID = "US/AK-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AK-Legislation")

BASE_URL = "https://www.akleg.gov/basis/statutes.asp"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 2  # seconds between requests

# All 47 Alaska Statute titles
TITLES = list(range(1, 48))


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


def get_chapters_for_title(title_num: int) -> list:
    """Get list of chapter IDs for a title via the TOC AJAX endpoint."""
    padded = f"{title_num:02d}"
    html = fetch_url(BASE_URL, params={"media": "js", "type": "TOC", "title": title_num})
    if not html:
        return []

    chapters = []
    for match in re.finditer(r'loadTOC\("([^"]+)"\)', html):
        ch_id = match.group(1)
        if "." in ch_id:
            chapters.append(ch_id)

    # Extract title name
    title_name_match = re.search(r'Title\s+\d+\.\s*([^<]+)', html)
    title_name = title_name_match.group(1).strip().rstrip('.') if title_name_match else f"Title {title_num}"

    # Extract chapter names from the <b> tags inside each <li>
    chapter_names = {}
    for match in re.finditer(
        r'loadTOC\("(\d+\.\d+)"\)[^>]*>[^<]*<b>([^<]+)',
        html
    ):
        ch_id = match.group(1)
        ch_name = match.group(2).strip().rstrip('.')
        # Remove "Chapter XX. " prefix since we add it in the title
        ch_name = re.sub(r'^Chapter\s+\d+\.\s*', '', ch_name)
        chapter_names[ch_id] = ch_name

    return [(ch, chapter_names.get(ch, ""), title_name) for ch in chapters]


def fetch_chapter_text(chapter_id: str) -> Optional[str]:
    """Fetch the full text of a chapter via the print AJAX endpoint."""
    html = fetch_url(BASE_URL, params={"media": "print", "secStart": chapter_id, "secEnd": chapter_id})
    if not html:
        return None
    return html


def parse_chapter_content(html: str, chapter_id: str) -> dict:
    """Parse chapter HTML into structured content."""
    text = clean_html(html)
    if not text or len(text) < 50:
        return {"text": "", "sections": []}

    # Extract individual sections
    sections = []
    for match in re.finditer(r'<a\s+name="([^"]+)"', html):
        sec_id = match.group(1)
        if sec_id and "." in sec_id:
            sections.append(sec_id)

    return {"text": text, "sections": sections}


def normalize_chapter(chapter_id: str, chapter_name: str, title_name: str,
                      content: dict) -> dict:
    """Normalize a chapter record."""
    title_num = chapter_id.split(".")[0]
    chapter_num = chapter_id.split(".")[1] if "." in chapter_id else ""

    title = f"AS Title {int(title_num)}, Chapter {int(chapter_num)}"
    if chapter_name:
        title += f": {chapter_name}"

    return {
        "_id": f"US/AK-Legislation/AS-{chapter_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": content["text"],
        "date": "2024-01-01",  # Current compilation year
        "url": f"{BASE_URL}#{chapter_id}",
        "title_number": int(title_num),
        "title_name": title_name,
        "chapter_number": chapter_num,
        "chapter_name": chapter_name,
        "section_count": len(content.get("sections", [])),
        "jurisdiction": "US-AK",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all chapter records."""
    for title_num in TITLES:
        logger.info(f"Processing Title {title_num}...")
        chapters = get_chapters_for_title(title_num)
        time.sleep(CRAWL_DELAY)

        for chapter_id, chapter_name, title_name in chapters:
            html = fetch_chapter_text(chapter_id)
            time.sleep(CRAWL_DELAY)

            if not html:
                logger.warning(f"  Skipped {chapter_id}: no content")
                continue

            content = parse_chapter_content(html, chapter_id)
            if not content["text"] or len(content["text"]) < 100:
                logger.debug(f"  Skipped {chapter_id}: too short")
                continue

            record = normalize_chapter(chapter_id, chapter_name, title_name, content)
            logger.info(f"  [{chapter_id}] {chapter_name[:50]} - {len(content['text'])} chars, {len(content['sections'])} sections")
            yield record


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from a few titles."""
    records = []
    # Sample from diverse titles
    sample_titles = [1, 9, 11, 14, 21, 29, 37, 43, 45]

    for title_num in sample_titles:
        if len(records) >= count:
            break

        logger.info(f"Sampling Title {title_num}...")
        chapters = get_chapters_for_title(title_num)
        time.sleep(CRAWL_DELAY)

        for chapter_id, chapter_name, title_name in chapters[:2]:
            if len(records) >= count:
                break

            html = fetch_chapter_text(chapter_id)
            time.sleep(CRAWL_DELAY)

            if not html:
                continue

            content = parse_chapter_content(html, chapter_id)
            if not content["text"] or len(content["text"]) < 100:
                continue

            record = normalize_chapter(chapter_id, chapter_name, title_name, content)
            records.append(record)
            logger.info(f"  [{len(records)}] {record['title'][:60]} - {len(content['text'])} chars")

    return records


def test_api():
    """Test connectivity to akleg.gov."""
    logger.info("Testing Alaska Legislature connectivity...")

    # Test main page
    html = fetch_url(BASE_URL)
    if not html:
        logger.error("Main page unreachable")
        return False
    logger.info(f"Main page OK - {len(html)} bytes")

    # Test TOC endpoint
    time.sleep(CRAWL_DELAY)
    chapters = get_chapters_for_title(1)
    if not chapters:
        logger.error("TOC endpoint failed")
        return False
    logger.info(f"TOC OK - Title 1 has {len(chapters)} chapters")

    # Test content endpoint
    time.sleep(CRAWL_DELAY)
    ch_id = chapters[0][0]
    html = fetch_chapter_text(ch_id)
    if not html:
        logger.error("Content endpoint failed")
        return False

    content = parse_chapter_content(html, ch_id)
    logger.info(f"Content OK - Chapter {ch_id}: {len(content['text'])} chars, {len(content['sections'])} sections")
    if content["text"]:
        logger.info(f"Preview: {content['text'][:200]}...")
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
    parser = argparse.ArgumentParser(description="US/AK-Legislation Data Fetcher")
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
