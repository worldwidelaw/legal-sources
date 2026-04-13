#!/usr/bin/env python3
"""
US/MO-Legislation -- Missouri Revised Statutes

Fetches Missouri statutes from the official Office of Revisor of Statutes:
  https://revisor.mo.gov

Strategy:
  1. Scrape chapter TOC pages to enumerate section numbers
  2. Download individual section pages for full text
  3. Parse statute text from OneSection.aspx HTML

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
from pathlib import Path
from typing import Generator, List, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/MO-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MO-Legislation")

BASE_URL = "https://revisor.mo.gov"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 1  # seconds between requests


def strip_html(html_text: str) -> str:
    """Remove HTML tags and decode entities, returning clean text."""
    text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>|</p>|</div>|</li>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
    return text.strip()


def get_chapter_list() -> List[int]:
    """Get list of chapter numbers from the main page."""
    url = f"{BASE_URL}/main/Home.aspx"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        # Find chapter links: OneChapter.aspx?chapter=NNN
        chapters = re.findall(r'OneChapter\.aspx\?chapter=(\d+)', resp.text)
        result = sorted(set(int(c) for c in chapters))
        logger.info(f"Found {len(result)} chapters from main page")
        return result
    except Exception as e:
        logger.warning(f"Failed to get chapter list from main page: {e}")
        return []


def get_sections_for_chapter(chapter: int) -> List[dict]:
    """Get section numbers and titles from a chapter TOC page."""
    url = f"{BASE_URL}/main/OneChapter.aspx?chapter={chapter}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()

        sections = []
        # Links use PageSelect.aspx?section=X.XXX&bid=NNNNN&hl=
        pattern = re.compile(
            r'PageSelect\.aspx\?section=([\d.]+)[^"]*"[^>]*>(.*?)</a>',
            re.DOTALL
        )
        for match in pattern.finditer(resp.text):
            sec_num = match.group(1)
            raw_title = strip_html(match.group(2)).strip()
            # Skip if this is just a chapter-level link, not a section
            if '.' not in sec_num:
                continue
            sections.append({
                "section_id": sec_num,
                "raw_title": raw_title,
            })

        # Deduplicate by section_id
        seen = set()
        unique = []
        for s in sections:
            if s["section_id"] not in seen:
                seen.add(s["section_id"])
                unique.append(s)

        return unique
    except Exception as e:
        logger.warning(f"Failed to get sections for chapter {chapter}: {e}")
        return []


def download_section(section_id: str) -> Optional[str]:
    """Download the full text HTML for a single section."""
    url = f"{BASE_URL}/main/OneSection.aspx?section={section_id}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        logger.warning(f"Failed to download section {section_id}: {e}")
        return None


def parse_section_text(html: str) -> str:
    """Extract the statute body text from a OneSection page."""
    # Missouri uses <p class="norm"> elements for statute text.
    # These may contain nested <span class="bold">, <span class="rsmo"> etc.
    # Match <p class="norm">...</p> blocks (content may span multiple lines).
    norm_blocks = re.findall(
        r'<p\s+class="norm"[^>]*>(.*?)</p>',
        html, re.DOTALL
    )
    if norm_blocks:
        combined = '\n'.join(norm_blocks)
        text = strip_html(combined)
        if len(text) > 20:
            return text

    # Fallback: strip entire page and extract between section number and footer markers
    cleaned = re.sub(r'<(script|style|head)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
    full_text = strip_html(cleaned)

    lines = full_text.split('\n')
    capture = False
    captured = []
    for line in lines:
        line = line.strip()
        if not line:
            if capture:
                captured.append('')
            continue
        if re.match(r'^\d+\.\d+\.', line):
            capture = True
        if capture:
            if re.match(r'^(Previous|Next|Top|Home|©|Copyright)', line, re.IGNORECASE):
                break
            captured.append(line)

    if captured and len('\n'.join(captured)) > 20:
        return '\n'.join(captured).strip()

    return full_text


def extract_title_from_text(sec_id: str, text: str) -> str:
    """Extract the section title from the beginning of the statute text."""
    # Text starts like: "1.010.  Common law in force — effect. — 1. The..."
    # Extract the descriptive title between section number and the em-dash + body
    match = re.match(
        r'[\d.]+\.\s+(.+?)\s*[-\u2014]\s*(?:\d+\.|[A-Z])',
        text
    )
    if match:
        title = match.group(1).strip().rstrip('.')
        if len(title) > 5:
            return title
    return ""


def normalize(section: dict, text: str) -> dict:
    """Normalize a section into standard schema."""
    sec_id = section["section_id"]
    chapter = sec_id.split('.')[0] if '.' in sec_id else sec_id

    # Extract descriptive title from the text body
    desc_title = extract_title_from_text(sec_id, text)

    if desc_title:
        full_title = f"Missouri Revised Statutes § {sec_id} - {desc_title}"
    else:
        full_title = f"Missouri Revised Statutes § {sec_id}"

    url = f"{BASE_URL}/main/OneSection.aspx?section={sec_id}"

    return {
        "_id": f"US/MO-Legislation/{sec_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": text,
        "date": "2025-01-01",
        "url": url,
        "section_id": sec_id,
        "chapter": chapter,
        "jurisdiction": "US-MO",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all Missouri Statutes sections with full text."""
    chapters = get_chapter_list()
    if not chapters:
        logger.error("No chapters found")
        return

    total = 0
    for i, chapter in enumerate(chapters):
        logger.info(f"Processing chapter {chapter} ({i + 1}/{len(chapters)})...")
        sections = get_sections_for_chapter(chapter)
        time.sleep(CRAWL_DELAY)

        for sec in sections:
            html = download_section(sec["section_id"])
            if not html:
                continue

            text = parse_section_text(html)
            if len(text) < 20:
                logger.warning(f"Section {sec['section_id']}: text too short ({len(text)} chars), skipping")
                continue

            record = normalize(sec, text)
            total += 1
            yield record
            time.sleep(CRAWL_DELAY)

        if (i + 1) % 50 == 0:
            logger.info(f"  Progress: {i + 1}/{len(chapters)} chapters, {total} sections")

    logger.info(f"Total sections with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from early chapters."""
    records = []

    # Get a few chapters to sample from
    chapters = get_chapter_list()
    if not chapters:
        logger.error("No chapters found")
        return records

    sample_chapters = chapters[:5]  # First 5 chapters

    for chapter in sample_chapters:
        logger.info(f"Getting sections for chapter {chapter}...")
        sections = get_sections_for_chapter(chapter)
        time.sleep(CRAWL_DELAY)

        if not sections:
            continue

        # Take up to 5 sections per chapter
        for sec in sections[:5]:
            html = download_section(sec["section_id"])
            if not html:
                continue

            text = parse_section_text(html)
            if len(text) < 20:
                logger.warning(f"Section {sec['section_id']}: text too short, skipping")
                time.sleep(CRAWL_DELAY)
                continue

            record = normalize(sec, text)
            records.append(record)
            logger.info(f"  Fetched § {sec['section_id']} ({len(text)} chars)")
            time.sleep(CRAWL_DELAY)

            if len(records) >= count:
                break

        if len(records) >= count:
            break

    return records[:count]


def test_api():
    """Test connectivity to revisor.mo.gov."""
    logger.info("Testing Missouri Revisor connectivity...")

    # Test main page and chapter listing
    chapters = get_chapter_list()
    if not chapters:
        logger.error("Failed to get chapter list")
        return False
    logger.info(f"Found {len(chapters)} chapters")

    time.sleep(CRAWL_DELAY)

    # Test chapter TOC
    test_ch = chapters[0]
    sections = get_sections_for_chapter(test_ch)
    if not sections:
        logger.error(f"Failed to get sections for chapter {test_ch}")
        return False
    logger.info(f"Chapter {test_ch} has {len(sections)} sections")

    time.sleep(CRAWL_DELAY)

    # Test section download
    test_sec = sections[0]
    logger.info(f"Downloading section {test_sec['section_id']}...")
    html = download_section(test_sec["section_id"])
    if not html:
        logger.error("Failed to download section")
        return False

    text = parse_section_text(html)
    logger.info(f"Parsed text ({len(text)} chars): {text[:200]}...")

    return len(text) > 20


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
    parser = argparse.ArgumentParser(description="US/MO-Legislation Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")

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
