#!/usr/bin/env python3
"""
US/NH-Legislation -- New Hampshire Revised Statutes Annotated

Fetches NH RSA from the General Court website (gencourt.state.nh.us).
67 titles, hundreds of chapters. Full text in <codesect> HTML tags.

Strategy:
  - Parse nhtoc.htm for all 67 title links
  - For each title, parse NHTOC-{TITLE}.htm for chapter links
  - For each chapter, fetch the merged file ({CHAPTER}-mrg.htm) for all sections
  - Extract text from <codesect> elements, metadata from HTML comments

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

SOURCE_ID = "US/NH-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NH-Legislation")

BASE_URL = "https://www.gencourt.state.nh.us/rsa/html"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# robots.txt specifies Crawl-Delay: 11
CRAWL_DELAY = 11


def fetch_page(url: str) -> Optional[str]:
    """Fetch a page with retries."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 404:
                logger.debug(f"404: {url}")
                return None
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning(f"HTTP error for {url}: {e}")
                return None
    return None


def get_titles() -> list:
    """Parse main TOC to get title numbers and names."""
    html = fetch_page(f"{BASE_URL}/nhtoc.htm")
    if not html:
        return []

    titles = []
    # Links like: NHTOC/NHTOC-I.htm, NHTOC/NHTOC-XIX-A.htm
    for m in re.finditer(r'href="NHTOC/NHTOC-([^"]+)\.htm"', html, re.IGNORECASE):
        title_id = m.group(1)
        titles.append(title_id)

    logger.info(f"Found {len(titles)} titles")
    return titles


def get_chapters(title_id: str) -> list:
    """Parse a title TOC to get chapter info."""
    url = f"{BASE_URL}/NHTOC/NHTOC-{title_id}.htm"
    html = fetch_page(url)
    if not html:
        return []

    chapters = []
    # Chapter links: ../{TITLE}/{CHAPTER}/{CHAPTER}-mrg.htm or individual section links
    # The chapter heading links to the merged file
    # Pattern: href="../{TITLE}/{CHAPTER}/{CHAPTER}-mrg.htm"
    for m in re.finditer(
        r'href="\.\./([^/]+)/([^/]+)/([^"]+)-mrg\.htm"', html, re.IGNORECASE
    ):
        title_dir = m.group(1)
        chapter_dir = m.group(2)
        chapter_id = m.group(3)
        chapters.append({
            "title_dir": title_dir,
            "chapter_dir": chapter_dir,
            "chapter_id": chapter_id,
        })

    if not chapters:
        # Try alternate pattern: NHTOC-{TITLE}-{CHAPTER}.htm links to get chapter names
        # Then construct merged URLs
        for m in re.finditer(
            r'href="NHTOC-[^"]*-([^"]+)\.htm"[^>]*>.*?CHAPTER\s+(\S+)', html, re.IGNORECASE | re.DOTALL
        ):
            ch_id = m.group(1)
            chapters.append({
                "title_dir": title_id,
                "chapter_dir": ch_id,
                "chapter_id": ch_id,
            })

    return chapters


def parse_merged_chapter(html: str, chapter_id: str) -> list:
    """Parse a merged chapter file to extract individual sections."""
    sections = []

    # Extract title and chapter metadata from HTML comments
    title_name = ""
    chapter_name = ""
    tm = re.search(r'<titlename>(.*?)</titlename>', html, re.IGNORECASE | re.DOTALL)
    if tm:
        title_name = clean_text(tm.group(1))
    cm = re.search(r'<chapter>(.*?)</chapter>', html, re.IGNORECASE | re.DOTALL)
    if cm:
        chapter_name = clean_text(cm.group(1))

    # Split by <codesect> blocks - each represents one section
    # Pattern: section heading followed by <codesect>...</codesect> and <sourcenote>
    parts = re.split(r'(?=<center><h3>Section\s)', html, flags=re.IGNORECASE)

    for part in parts:
        if '<codesect>' not in part.lower():
            continue

        # Extract section number from <h3>
        sec_match = re.search(r'<h3>Section\s+([^<]+)</h3>', part, re.IGNORECASE)
        if not sec_match:
            continue
        section_number = sec_match.group(1).strip()

        # Extract section heading (bold text before codesect)
        heading = ""
        head_match = re.search(
            r'<b>\s*[\d\w:.-]+\s+(.*?)(?:</b>|&#150;|&ndash;|-\s*</b>)',
            part, re.IGNORECASE | re.DOTALL
        )
        if head_match:
            heading = clean_text(head_match.group(1)).rstrip(' -\u2013\u2014')

        # Try alternate heading: <sectiontitle> in comments
        if not heading:
            st_match = re.search(r'<sectiontitle>Section\s+\S+\s+(.*?)</sectiontitle>', part, re.IGNORECASE)
            if st_match:
                heading = clean_text(st_match.group(1))

        # Extract full text from <codesect>
        code_match = re.search(r'<codesect>(.*?)</codesect>', part, re.IGNORECASE | re.DOTALL)
        if not code_match:
            continue
        body_html = code_match.group(1)
        body_text = clean_html(body_html)

        if not body_text or len(body_text) < 10:
            continue

        # Extract source note
        source_text = ""
        src_match = re.search(r'<sourcenote>(.*?)</sourcenote>', part, re.IGNORECASE | re.DOTALL)
        if src_match:
            source_text = clean_text(re.sub(r'<[^>]+>', ' ', src_match.group(1)))

        sections.append({
            "section_number": section_number,
            "heading": heading,
            "title_name": title_name,
            "chapter_name": chapter_name,
            "chapter_id": chapter_id,
            "text": body_text,
            "source": source_text,
        })

    return sections


def clean_html(html: str) -> str:
    """Strip HTML tags and clean up text."""
    # Replace <br> with newlines
    text = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    # Replace <p> with double newlines
    text = re.sub(r'</?p[^>]*>', '\n\n', text, flags=re.IGNORECASE)
    # Strip all other tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = unescape(text)
    # Clean up whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def clean_text(text: str) -> str:
    """Clean a simple text string."""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    return re.sub(r'\s+', ' ', text).strip()


def normalize(section: dict) -> dict:
    """Normalize a parsed section into standard schema."""
    sec_num = section["section_number"]
    heading = section.get("heading", "")

    full_text = section["text"]
    if section.get("source"):
        full_text += f"\n\nSource: {section['source']}"

    title = f"RSA {sec_num}"
    if heading:
        title += f". {heading}"

    return {
        "_id": f"US/NH-Legislation/{sec_num}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "url": f"https://www.gencourt.state.nh.us/rsa/html/",
        "section_number": sec_num,
        "chapter": section.get("chapter_id", ""),
        "chapter_name": section.get("chapter_name", ""),
        "title_name": section.get("title_name", ""),
        "jurisdiction": "US-NH",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all statute sections with full text."""
    titles = get_titles()
    if not titles:
        logger.error("No titles found")
        return

    total = 0
    for title_id in titles:
        logger.info(f"Processing title {title_id}...")
        time.sleep(CRAWL_DELAY)

        chapters = get_chapters(title_id)
        if not chapters:
            continue

        for ch in chapters:
            mrg_url = f"{BASE_URL}/{ch['title_dir']}/{ch['chapter_dir']}/{ch['chapter_id']}-mrg.htm"
            time.sleep(CRAWL_DELAY)

            html = fetch_page(mrg_url)
            if not html:
                continue

            sections = parse_merged_chapter(html, ch["chapter_id"])
            for sec in sections:
                record = normalize(sec)
                total += 1
                yield record

    logger.info(f"Total sections: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from the first title/chapter."""
    records = []

    titles = get_titles()
    if not titles:
        return []

    # Try first few titles to get enough sections
    for title_id in titles[:3]:
        if len(records) >= count:
            break

        time.sleep(CRAWL_DELAY)
        chapters = get_chapters(title_id)
        if not chapters:
            continue

        for ch in chapters[:3]:
            if len(records) >= count:
                break

            mrg_url = f"{BASE_URL}/{ch['title_dir']}/{ch['chapter_dir']}/{ch['chapter_id']}-mrg.htm"
            logger.info(f"Fetching: {mrg_url}")
            time.sleep(CRAWL_DELAY)

            html = fetch_page(mrg_url)
            if not html:
                continue

            sections = parse_merged_chapter(html, ch["chapter_id"])
            for sec in sections:
                if len(records) >= count:
                    break
                record = normalize(sec)
                records.append(record)

    return records


def test_api():
    """Test connectivity."""
    logger.info("Testing NH RSA connectivity...")

    titles = get_titles()
    if not titles:
        logger.error("Failed to fetch title list")
        return False
    logger.info(f"Found {len(titles)} titles")

    time.sleep(CRAWL_DELAY)
    chapters = get_chapters(titles[0])
    if not chapters:
        logger.error("Failed to get chapters for first title")
        return False
    logger.info(f"Title {titles[0]}: {len(chapters)} chapters")

    if chapters:
        ch = chapters[0]
        mrg_url = f"{BASE_URL}/{ch['title_dir']}/{ch['chapter_dir']}/{ch['chapter_id']}-mrg.htm"
        time.sleep(CRAWL_DELAY)
        html = fetch_page(mrg_url)
        if html:
            sections = parse_merged_chapter(html, ch["chapter_id"])
            logger.info(f"Chapter {ch['chapter_id']}: {len(sections)} sections")
            if sections:
                r = normalize(sections[0])
                logger.info(f"Sample: {r['title'][:80]}")
                logger.info(f"Text ({len(r['text'])} chars): {r['text'][:200]}...")
        else:
            logger.error("Failed to fetch merged chapter")
            return False

    return True


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

    return len(records) >= 10 and avg_text > 50


def main():
    parser = argparse.ArgumentParser(description="US/NH-Legislation Data Fetcher")
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
