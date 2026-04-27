#!/usr/bin/env python3
"""
US/IA-Legislation -- Iowa Code

Fetches Iowa statutes from the official Iowa Legislature site.
The Iowa Code is available as RTF files per chapter at:
  https://www.legis.iowa.gov/docs/code/2026/{chapter}.rtf

Strategy:
  1. Enumerate all chapters across 16 titles
  2. Download chapter-level RTF files
  3. Convert RTF to text using striprtf
  4. Parse individual sections from the text

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

try:
    from striprtf.striprtf import rtf_to_text
except ImportError:
    print("ERROR: striprtf not installed. Run: pip3 install striprtf")
    sys.exit(1)

SOURCE_ID = "US/IA-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IA-Legislation")

BASE_URL = "https://www.legis.iowa.gov"
CODE_YEAR = "2026"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 1  # seconds between requests

TITLES = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII",
          "IX", "X", "XI", "XII", "XIII", "XIV", "XV", "XVI"]

# Section header pattern: e.g., "1.1  State boundaries." or "10A.104  Definitions."
SECTION_RE = re.compile(
    r"^(\d+[A-Z]?\.\d+[A-Z]?(?:\.\d+)?)\s{2,}(.+?)\.?\s*$",
    re.MULTILINE,
)


def get_chapters_for_title(title: str) -> List[str]:
    """Get chapter numbers for a given title numeral."""
    url = f"{BASE_URL}/law/iowaCode/chapters?title={title}&year={CODE_YEAR}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        chapters = list(set(re.findall(r"codeChapter=([^&\"]+)", resp.text)))
        return sorted(chapters, key=lambda x: (len(x), x))
    except Exception as e:
        logger.warning(f"Failed to get chapters for title {title}: {e}")
        return []


def get_all_chapters() -> List[str]:
    """Get all chapter numbers across all titles."""
    all_chapters = []
    for title in TITLES:
        chapters = get_chapters_for_title(title)
        all_chapters.extend(chapters)
        time.sleep(CRAWL_DELAY)
    return all_chapters


def download_chapter_rtf(chapter: str) -> Optional[str]:
    """Download an RTF file for a chapter and convert to text."""
    url = f"{BASE_URL}/docs/code/{CODE_YEAR}/{chapter}.rtf"
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
        rtf_content = resp.content.decode("utf-8", errors="replace")
        text = rtf_to_text(rtf_content)
        return text
    except Exception as e:
        logger.warning(f"Failed to download chapter {chapter}: {e}")
        return None


def parse_sections(chapter_text: str, chapter: str) -> List[dict]:
    """Parse individual sections from chapter text."""
    sections = []
    matches = list(SECTION_RE.finditer(chapter_text))

    if not matches:
        return []

    for i, m in enumerate(matches):
        sec_id = m.group(1)
        sec_title = m.group(2).strip().rstrip(".")

        # Get text from after this header to next section header
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(chapter_text)
        sec_text = chapter_text[start:end].strip()

        # Clean up whitespace
        sec_text = re.sub(r"\n\s*\n\s*\n+", "\n\n", sec_text)
        sec_text = re.sub(r"[ \t]+", " ", sec_text)
        sec_text = sec_text.strip()

        if len(sec_text) < 20:
            continue

        sections.append({
            "section_id": sec_id,
            "section_title": sec_title,
            "text": sec_text,
            "chapter": chapter,
        })

    return sections


def normalize(section: dict) -> dict:
    """Normalize a parsed section into standard schema."""
    sec_id = section["section_id"]
    sec_title = section["section_title"]
    chapter = section["chapter"]

    url = f"{BASE_URL}/docs/code/{CODE_YEAR}/{sec_id}.pdf"
    full_title = f"Iowa Code § {sec_id} - {sec_title}"

    return {
        "_id": f"US/IA-Legislation/{sec_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": section["text"],
        "date": f"{CODE_YEAR}-01-01",
        "url": url,
        "section_id": sec_id,
        "chapter": chapter,
        "jurisdiction": "US-IA",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all Iowa Code sections with full text."""
    logger.info("Enumerating all chapters...")
    chapters = get_all_chapters()
    logger.info(f"Found {len(chapters)} chapters")

    total = 0
    for i, chapter in enumerate(chapters):
        logger.info(f"Downloading chapter {chapter} ({i + 1}/{len(chapters)})...")
        chapter_text = download_chapter_rtf(chapter)
        time.sleep(CRAWL_DELAY)

        if not chapter_text:
            continue

        sections = parse_sections(chapter_text, chapter)
        for section in sections:
            record = normalize(section)
            total += 1
            yield record

        if (i + 1) % 50 == 0:
            logger.info(f"  Progress: {i + 1}/{len(chapters)} chapters, {total} sections")

    logger.info(f"Total sections with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from chapter 1."""
    records = []

    logger.info("Downloading chapter 1 for sample...")
    chapter_text = download_chapter_rtf("1")
    if not chapter_text:
        logger.error("Failed to download chapter 1")
        return []

    sections = parse_sections(chapter_text, "1")
    logger.info(f"Parsed {len(sections)} sections from chapter 1")

    for section in sections[:count]:
        record = normalize(section)
        records.append(record)

    return records


def test_api():
    """Test connectivity to Iowa Legislature site."""
    logger.info("Testing Iowa Legislature connectivity...")

    # Test chapter listing
    chapters = get_chapters_for_title("I")
    if not chapters:
        logger.error("Failed to get chapter listing")
        return False
    logger.info(f"Title I: {len(chapters)} chapters")

    time.sleep(CRAWL_DELAY)

    # Test RTF download
    logger.info("Downloading chapter 1 RTF...")
    text = download_chapter_rtf("1")
    if not text:
        logger.error("Failed to download chapter 1")
        return False

    sections = parse_sections(text, "1")
    if not sections:
        logger.error("No sections parsed")
        return False

    logger.info(f"Parsed {len(sections)} sections from chapter 1")
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
    parser = argparse.ArgumentParser(description="US/IA-Legislation Data Fetcher")
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
