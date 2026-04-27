#!/usr/bin/env python3
"""
US/MN-Legislation -- Minnesota Statutes

Fetches Minnesota statutes from the official Revisor of Statutes website.
The statutes are available as structured HTML at:
  https://www.revisor.mn.gov/statutes/cite/{chapter}/full

Strategy:
  1. Scrape main page to enumerate all subject parts
  2. Scrape each part page to get chapter numbers
  3. Download chapter-level full HTML pages
  4. Parse individual sections from div.section elements

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
from html.parser import HTMLParser
from pathlib import Path
from typing import Generator, List, Optional, Tuple

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/MN-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MN-Legislation")

BASE_URL = "https://www.revisor.mn.gov"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 1  # seconds between requests


def strip_html(html_text: str) -> str:
    """Remove HTML tags and decode entities, returning clean text."""
    # Remove script/style content
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    # Replace <br>, <p>, <div> closings with newlines
    text = re.sub(r'<br\s*/?>|</p>|</div>', '\n', text, flags=re.IGNORECASE)
    # Remove all remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode common entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    # Clean whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    return text.strip()


def get_all_parts() -> List[str]:
    """Get all subject part URL paths from the main statutes page."""
    url = f"{BASE_URL}/statutes/"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        parts = re.findall(r'/statutes/part/([^"\'<> ]+)', resp.text)
        return sorted(set(parts))
    except Exception as e:
        logger.warning(f"Failed to get parts list: {e}")
        return []


def get_chapters_for_part(part_path: str) -> List[str]:
    """Get chapter numbers from a subject part page."""
    url = f"{BASE_URL}/statutes/part/{part_path}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        chapters = re.findall(r'/statutes/cite/([0-9A-Za-z.]+)', resp.text)
        # Filter to chapter-level only (no section dots like 1.01)
        chapter_only = [c for c in chapters if '.' not in c]
        return sorted(set(chapter_only), key=lambda x: (len(x), x))
    except Exception as e:
        logger.warning(f"Failed to get chapters for part {part_path}: {e}")
        return []


def get_all_chapters() -> List[str]:
    """Get all chapter numbers across all subject parts."""
    logger.info("Enumerating all subject parts...")
    parts = get_all_parts()
    logger.info(f"Found {len(parts)} parts")

    all_chapters = set()
    for i, part in enumerate(parts):
        chapters = get_chapters_for_part(part)
        all_chapters.update(chapters)
        if (i + 1) % 20 == 0:
            logger.info(f"  Scanned {i + 1}/{len(parts)} parts, {len(all_chapters)} chapters so far")
        time.sleep(0.5)

    result = sorted(all_chapters, key=lambda x: (len(x), x))
    logger.info(f"Total unique chapters: {len(result)}")
    return result


def parse_sections_from_html(html: str, chapter: str) -> List[dict]:
    """Parse individual statute sections from a chapter's full HTML page."""
    sections = []

    # Find all <div class="section" id="stat.X.XX"> blocks
    # Each section is followed by an optional <div class="history"> block
    section_pattern = re.compile(
        r'<div\s+class="section"\s+id="(stat\.[^"]+)">(.*?)(?=<div\s+class="(?:section|history)"|$)',
        re.DOTALL
    )

    for match in section_pattern.finditer(html):
        anchor_id = match.group(1)  # e.g., "stat.1.01"
        section_html = match.group(2)

        # Extract section number from anchor id
        sec_id = anchor_id.replace("stat.", "")

        # Extract heading from <h1 class="shn">
        heading_match = re.search(r'<h1[^>]*class="shn"[^>]*>(.*?)</h1>', section_html, re.DOTALL)
        if heading_match:
            raw_heading = strip_html(heading_match.group(1))
        else:
            raw_heading = sec_id

        # Separate section number from title in heading
        # Pattern: "1.01 EXTENT." or "1.041 OFFICIAL FLOWER."
        title_match = re.match(r'^[\d.A-Za-z]+\s+(.+?)\.?\s*$', raw_heading)
        sec_title = title_match.group(1).strip().rstrip('.') if title_match else raw_heading

        # Get body text (everything after the h1)
        body_start = heading_match.end() if heading_match else 0
        body_html = section_html[body_start:]
        body_text = strip_html(body_html).strip()

        if len(body_text) < 10:
            continue

        sections.append({
            "section_id": sec_id,
            "section_title": sec_title,
            "text": body_text,
            "chapter": chapter,
        })

    return sections


def download_chapter(chapter: str) -> Optional[str]:
    """Download the full text HTML for a chapter."""
    url = f"{BASE_URL}/statutes/cite/{chapter}/full"
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning(f"Failed to download chapter {chapter}: {e}")
        return None


def normalize(section: dict) -> dict:
    """Normalize a parsed section into standard schema."""
    sec_id = section["section_id"]
    sec_title = section["section_title"]
    chapter = section["chapter"]

    url = f"{BASE_URL}/statutes/cite/{sec_id}"
    full_title = f"Minnesota Statutes § {sec_id} - {sec_title}"

    return {
        "_id": f"US/MN-Legislation/{sec_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": section["text"],
        "date": "2025-01-01",
        "url": url,
        "section_id": sec_id,
        "chapter": chapter,
        "jurisdiction": "US-MN",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all Minnesota Statutes sections with full text."""
    logger.info("Enumerating all chapters...")
    chapters = get_all_chapters()
    logger.info(f"Found {len(chapters)} chapters")

    total = 0
    for i, chapter in enumerate(chapters):
        logger.info(f"Downloading chapter {chapter} ({i + 1}/{len(chapters)})...")
        html = download_chapter(chapter)
        time.sleep(CRAWL_DELAY)

        if not html:
            continue

        sections = parse_sections_from_html(html, chapter)
        for section in sections:
            record = normalize(section)
            total += 1
            yield record

        if (i + 1) % 50 == 0:
            logger.info(f"  Progress: {i + 1}/{len(chapters)} chapters, {total} sections")

    logger.info(f"Total sections with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from chapters 1 and 2."""
    records = []

    for ch in ["1", "2"]:
        logger.info(f"Downloading chapter {ch} for sample...")
        html = download_chapter(ch)
        if not html:
            logger.error(f"Failed to download chapter {ch}")
            continue

        sections = parse_sections_from_html(html, ch)
        logger.info(f"Parsed {len(sections)} sections from chapter {ch}")

        for section in sections:
            record = normalize(section)
            records.append(record)
            if len(records) >= count:
                break

        time.sleep(CRAWL_DELAY)
        if len(records) >= count:
            break

    return records[:count]


def test_api():
    """Test connectivity to the Minnesota Revisor site."""
    logger.info("Testing Minnesota Revisor connectivity...")

    # Test main page
    parts = get_all_parts()
    if not parts:
        logger.error("Failed to get parts list")
        return False
    logger.info(f"Found {len(parts)} subject parts")

    time.sleep(CRAWL_DELAY)

    # Test chapter listing
    chapters = get_chapters_for_part(parts[0])
    if not chapters:
        logger.error("Failed to get chapter listing")
        return False
    logger.info(f"First part has {len(chapters)} chapters: {chapters[:5]}...")

    time.sleep(CRAWL_DELAY)

    # Test full chapter download
    test_ch = chapters[0] if chapters else "1"
    logger.info(f"Downloading chapter {test_ch}...")
    html = download_chapter(test_ch)
    if not html:
        logger.error("Failed to download chapter")
        return False

    sections = parse_sections_from_html(html, test_ch)
    if not sections:
        logger.error("No sections parsed from chapter")
        return False

    logger.info(f"Parsed {len(sections)} sections from chapter {test_ch}")
    sample = sections[0]
    logger.info(f"Sample: § {sample['section_id']} - {sample['section_title']}")
    logger.info(f"Text preview ({len(sample['text'])} chars): {sample['text'][:200]}...")

    return True


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r"[^\w\-]", "_", record["_id"])[:80]
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

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="US/MN-Legislation Data Fetcher")
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
                safe_id = re.sub(r"[^\w\-]", "_", record["_id"])[:80]
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
