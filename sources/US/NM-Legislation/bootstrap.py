#!/usr/bin/env python3
"""
US/NM-Legislation -- New Mexico Statutes Annotated (NMSA 1978)

Fetches NM statutes from NMOneSource.com (NM Compilation Commission).
Uses unannotated PDF downloads per chapter, extracts text via pypdf.

Strategy:
  - Scrape chapter listing from nav_date.do pages to get item IDs
  - Download chapter PDFs from /nmos/nmsa-unanno/en/{itemId}/1/document.do
  - Extract text with pypdf, split into individual sections by section number pattern
  - Normalize into standard schema with full text

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample sections
  python bootstrap.py bootstrap             # Full extraction (all 81 chapters)
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional, Union

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/NM-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NM-Legislation")

BASE_URL = "https://nmonesource.com/nmos/nmsa-unanno/en"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 2


def fetch_url(url: str, binary: bool = False) -> Optional[Union[bytes, str]]:
    """Fetch a URL with retries."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=60)
            resp.raise_for_status()
            return resp.content if binary else resp.text
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


def get_chapters() -> list:
    """Scrape chapter listing pages to get item IDs and titles."""
    chapters = []
    for page in range(1, 6):
        url = f"{BASE_URL}/nav_date.do?page={page}&iframe=true"
        html = fetch_url(url)
        if not html:
            break

        # Extract item links: /nmos/nmsa-unanno/en/item/{ID}/index.do
        for m in re.finditer(
            r'href="[^"]*?/item/(\d+)/index\.do"[^>]*>\s*(.*?)\s*</a>',
            html, re.IGNORECASE | re.DOTALL
        ):
            item_id = m.group(1)
            raw_title = re.sub(r'<[^>]+>', '', m.group(2)).strip()
            # Extract chapter number from title like "Chapter 1 - Elections"
            ch_match = re.match(r'Chapter\s+(\S+)\s*[-–—]\s*(.*)', raw_title, re.IGNORECASE)
            if ch_match:
                ch_num = ch_match.group(1)
                ch_name = ch_match.group(2).strip()
            else:
                ch_num = item_id
                ch_name = raw_title

            chapters.append({
                "item_id": item_id,
                "chapter_number": ch_num,
                "chapter_name": ch_name,
            })

        time.sleep(CRAWL_DELAY)

        # Stop if this page had no results
        if not re.search(r'/item/\d+/index\.do', html):
            break

    logger.info(f"Found {len(chapters)} chapters")
    return chapters


def download_chapter_pdf(item_id: str) -> Optional[bytes]:
    """Download a chapter PDF."""
    url = f"{BASE_URL}/{item_id}/1/document.do"
    data = fetch_url(url, binary=True)
    if data and isinstance(data, bytes) and data[:5] == b'%PDF-':
        return data
    logger.warning(f"Failed to download PDF for item {item_id}")
    return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/NM-Legislation",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""

def split_into_sections(full_text: str, chapter_number: str, chapter_name: str) -> list:
    """Split chapter text into individual statute sections."""
    sections = []

    # Pattern: section number like "1-1-1." or "1-1-1.1." at start of line
    # NM section numbers follow pattern: Chapter-Article-Section[.Subsection]
    section_pattern = re.compile(
        r'^(\d+[A-Za-z]?-\d+[A-Za-z]?-\d+(?:\.\d+)?)\.\s+(.*)',
        re.MULTILINE
    )

    matches = list(section_pattern.finditer(full_text))
    if not matches:
        return sections

    for i, match in enumerate(matches):
        sec_num = match.group(1)
        heading_start = match.group(2)

        # Get text from this match to the next match (or end)
        start_pos = match.start()
        end_pos = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
        raw_text = full_text[start_pos:end_pos].strip()

        # Extract heading (first line after section number, before the body)
        heading = ""
        lines = raw_text.split('\n', 2)
        if lines:
            first_line = lines[0]
            # The heading is the text after "X-Y-Z. " on the first line
            h_match = re.match(r'\d+[A-Za-z]?-\d+[A-Za-z]?-\d+(?:\.\d+)?\.\s+(.*)', first_line)
            if h_match:
                heading = h_match.group(1).strip().rstrip('.')

        # Clean up the body text
        body = raw_text
        # Remove "History:" lines and everything after (source notes)
        history = ""
        hist_match = re.search(r'\nHistory:.*', body, re.DOTALL)
        if hist_match:
            history = hist_match.group(0).strip()
            body = body[:hist_match.start()].strip()

        if not body or len(body) < 10:
            continue

        sections.append({
            "section_number": sec_num,
            "heading": heading,
            "chapter_number": chapter_number,
            "chapter_name": chapter_name,
            "text": body,
            "history": history,
        })

    return sections


def normalize(section: dict) -> dict:
    """Normalize a parsed section into standard schema."""
    sec_num = section["section_number"]
    heading = section.get("heading", "")

    full_text = section["text"]
    if section.get("history"):
        full_text += f"\n\n{section['history']}"

    title = f"NMSA {sec_num}"
    if heading:
        title += f". {heading}"

    return {
        "_id": f"US/NM-Legislation/{sec_num}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "url": f"https://nmonesource.com/nmos/nmsa/en/nav.do",
        "section_number": sec_num,
        "chapter": section.get("chapter_number", ""),
        "chapter_name": section.get("chapter_name", ""),
        "jurisdiction": "US-NM",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all statute sections with full text."""
    chapters = get_chapters()
    if not chapters:
        logger.error("No chapters found")
        return

    total = 0
    for ch in chapters:
        logger.info(f"Processing Chapter {ch['chapter_number']} - {ch['chapter_name']}...")
        time.sleep(CRAWL_DELAY)

        pdf_data = download_chapter_pdf(ch["item_id"])
        if not pdf_data:
            continue

        full_text = extract_text_from_pdf(pdf_data)
        if not full_text:
            logger.warning(f"No text extracted from Chapter {ch['chapter_number']}")
            continue

        sections = split_into_sections(full_text, ch["chapter_number"], ch["chapter_name"])
        logger.info(f"  Chapter {ch['chapter_number']}: {len(sections)} sections")

        for sec in sections:
            record = normalize(sec)
            total += 1
            yield record

    logger.info(f"Total sections: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from the first chapter."""
    records = []

    chapters = get_chapters()
    if not chapters:
        return []

    for ch in chapters[:3]:
        if len(records) >= count:
            break

        logger.info(f"Fetching Chapter {ch['chapter_number']} - {ch['chapter_name']}...")
        time.sleep(CRAWL_DELAY)

        pdf_data = download_chapter_pdf(ch["item_id"])
        if not pdf_data:
            continue

        full_text = extract_text_from_pdf(pdf_data)
        if not full_text:
            continue

        sections = split_into_sections(full_text, ch["chapter_number"], ch["chapter_name"])
        for sec in sections:
            if len(records) >= count:
                break
            record = normalize(sec)
            records.append(record)

    return records


def test_api():
    """Test connectivity and PDF extraction."""
    logger.info("Testing NM OneSource connectivity...")

    chapters = get_chapters()
    if not chapters:
        logger.error("Failed to fetch chapter list")
        return False
    logger.info(f"Found {len(chapters)} chapters")

    ch = chapters[0]
    logger.info(f"Testing Chapter {ch['chapter_number']} - {ch['chapter_name']}")
    time.sleep(CRAWL_DELAY)

    pdf_data = download_chapter_pdf(ch["item_id"])
    if not pdf_data:
        logger.error("Failed to download PDF")
        return False
    logger.info(f"PDF size: {len(pdf_data)} bytes")

    full_text = extract_text_from_pdf(pdf_data)
    if not full_text:
        logger.error("Failed to extract text from PDF")
        return False
    logger.info(f"Extracted {len(full_text)} chars of text")

    sections = split_into_sections(full_text, ch["chapter_number"], ch["chapter_name"])
    logger.info(f"Found {len(sections)} sections")

    if sections:
        r = normalize(sections[0])
        logger.info(f"Sample: {r['title'][:80]}")
        logger.info(f"Text ({len(r['text'])} chars): {r['text'][:200]}...")

    return len(sections) > 0


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
    parser = argparse.ArgumentParser(description="US/NM-Legislation Data Fetcher")
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
