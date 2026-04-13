#!/usr/bin/env python3
"""
US/NE-Legislation -- Nebraska Revised Statutes

Fetches NE statutes from the official Nebraska Legislature GitHub repo:
https://github.com/nelegislature/LegalDocs

55,000+ statute sections in XML format, organized by chapter.
Each XML file contains one section with full text in <para> elements.

Strategy:
  - Use GitHub API to get the full repo tree (all file paths)
  - Download raw XML files from raw.githubusercontent.com
  - Parse XML: extract statute number, catchline, paragraph text, and source history

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
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/NE-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NE-Legislation")

GITHUB_REPO = "nelegislature/LegalDocs"
GITHUB_API = f"https://api.github.com/repos/{GITHUB_REPO}"
RAW_BASE = f"https://raw.githubusercontent.com/{GITHUB_REPO}/master"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 0.3  # seconds between raw file downloads (GitHub is generous)


def get_statute_file_paths() -> list:
    """Get all statute XML file paths from the repo tree."""
    url = f"{GITHUB_API}/git/trees/master?recursive=1"
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch repo tree: {e}")
        return []

    paths = [
        f["path"] for f in data.get("tree", [])
        if f["path"].startswith("Statutes/") and f["path"].endswith(".xml")
    ]
    paths.sort()
    logger.info(f"Found {len(paths)} statute XML files")
    return paths


def download_xml(path: str) -> Optional[str]:
    """Download a raw XML file from GitHub."""
    url = f"{RAW_BASE}/{path}"
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
            else:
                logger.warning(f"Failed to download {path}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Download error for {path}: {e}")
            return None
    return None


def parse_statute_xml(xml_text: str) -> Optional[dict]:
    """Parse a statute XML file and extract section data."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.warning(f"XML parse error: {e}")
        return None

    # Find the section/amendatorysection element
    section = root.find(".//amendatorysection")
    if section is None:
        section = root.find(".//section")
    if section is None:
        return None

    # Extract statute number
    statute_no_el = section.find(".//statuteno") if section is not None else None
    statute_no = ""
    if statute_no_el is not None and statute_no_el.text:
        statute_no = statute_no_el.text.strip()

    if not statute_no:
        # Try from attribute
        statute_no = section.get("statutenumber", "") if section is not None else ""

    if not statute_no:
        return None

    # Extract catchline (title)
    catchline_el = section.find(".//catchline")
    catchline = ""
    if catchline_el is not None:
        catchline = "".join(catchline_el.itertext()).strip()

    # Extract chapter name
    chapter_name = ""
    if section is not None:
        chapter_name = section.get("chaptername", "")

    # Extract all paragraph text
    paragraphs = []
    for para in section.findall(".//para"):
        text = "".join(para.itertext()).strip()
        if text:
            paragraphs.append(text)

    # Extract source/history
    source_el = root.find(".//source")
    history_parts = []
    if source_el is not None:
        for para in source_el.findall(".//para"):
            text = "".join(para.itertext()).strip()
            if text:
                history_parts.append(text)

    body_text = "\n\n".join(paragraphs)
    if not body_text:
        return None

    history_text = " ".join(history_parts)

    # Clean up HTML entities
    body_text = unescape(body_text)
    history_text = unescape(history_text)

    # Parse chapter number from statute number (e.g., "1-116" -> chapter "1")
    chapter = statute_no.split("-")[0] if "-" in statute_no else ""

    return {
        "statute_number": statute_no,
        "catchline": catchline,
        "chapter_name": chapter_name,
        "chapter": chapter,
        "text": body_text,
        "history": history_text,
    }


def normalize(section: dict) -> dict:
    """Normalize a parsed section into standard schema."""
    statute_no = section["statute_number"]
    catchline = section["catchline"]
    chapter = section["chapter"]

    full_text = section["text"]
    if section.get("history"):
        full_text += f"\n\nHistory: {section['history']}"

    title = f"§ {statute_no}"
    if catchline:
        title += f". {catchline}"

    return {
        "_id": f"US/NE-Legislation/{statute_no}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "url": f"https://nebraskalegislature.gov/laws/statutes.php?statute={statute_no}",
        "statute_number": statute_no,
        "chapter": chapter,
        "chapter_name": section.get("chapter_name", ""),
        "jurisdiction": "US-NE",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all statute sections with full text."""
    paths = get_statute_file_paths()
    if not paths:
        logger.error("No statute files found")
        return

    total = 0
    errors = 0

    for i, path in enumerate(paths):
        if i % 500 == 0:
            logger.info(f"Processing {i+1}/{len(paths)}...")

        xml_text = download_xml(path)
        if not xml_text:
            errors += 1
            continue

        section = parse_statute_xml(xml_text)
        if not section or len(section.get("text", "")) < 10:
            continue

        record = normalize(section)
        total += 1
        yield record

        if i < len(paths) - 1:
            time.sleep(CRAWL_DELAY)

    logger.info(f"Total sections with full text: {total} (errors: {errors})")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from the first chapter."""
    records = []

    # Get file paths for Chapter 1
    url = f"{GITHUB_API}/contents/Statutes/CHAP01"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        files = resp.json()
    except Exception as e:
        logger.error(f"Failed to list CHAP01: {e}")
        return []

    xml_files = [f for f in files if f["name"].endswith(".xml")]
    xml_files.sort(key=lambda f: f["name"])

    for f in xml_files:
        if len(records) >= count:
            break

        path = f"Statutes/CHAP01/{f['name']}"
        logger.info(f"Downloading: {path}")
        xml_text = download_xml(path)
        if not xml_text:
            continue

        section = parse_statute_xml(xml_text)
        if not section or len(section.get("text", "")) < 10:
            continue

        record = normalize(section)
        records.append(record)
        time.sleep(CRAWL_DELAY)

    return records


def test_api():
    """Test connectivity to GitHub repo."""
    logger.info("Testing GitHub repo connectivity...")

    # Test repo tree
    paths = get_statute_file_paths()
    if not paths:
        logger.error("Repo tree fetch failed")
        return False
    logger.info(f"Repo tree OK - {len(paths)} statute files")

    # Test single file download
    test_path = paths[0]
    logger.info(f"Downloading test file: {test_path}")
    xml_text = download_xml(test_path)
    if not xml_text:
        logger.error("Test download failed")
        return False

    section = parse_statute_xml(xml_text)
    if not section:
        logger.error("XML parsing failed")
        return False

    record = normalize(section)
    logger.info(f"Sample: {record['title'][:80]}")
    logger.info(f"Text preview ({len(record['text'])} chars): {record['text'][:200]}...")
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
    parser = argparse.ArgumentParser(description="US/NE-Legislation Data Fetcher")
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
                safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
                filepath = SAMPLE_DIR / f"record_{safe_id}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(f"Processed {count} records")


if __name__ == "__main__":
    main()
