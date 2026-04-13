#!/usr/bin/env python3
"""
US/NC-Legislation -- North Carolina General Statutes

Fetches NC General Statutes from Internet Archive ODT bulk downloads.
ncleg.gov is behind Cloudflare and blocks programmatic access, so we use
the official bulk exports archived at archive.org/details/gov.nc.code.

Each ODT file contains one title/chapter of the NC General Statutes in
OpenDocument XML format. Sections are identified by style patterns:
  P4 = section heading (e.g., "§ 1-1. Remedies.")
  P5 = field label ("Text", "History", "Annotations")
  P6 = statute body text
  P7 = annotation/case notes text

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
import tempfile
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/NC-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NC-Legislation")

# Internet Archive base URL for NC statutes
IA_BASE = "https://archive.org/download/gov.nc.code"
IA_RELEASE = "release85.2023.06"
IA_METADATA = "https://archive.org/metadata/gov.nc.code"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 0.5  # seconds between downloads (archive.org is generous)

# ODF XML namespaces
NS = {
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
}
STYLE_ATTR = "{urn:oasis:names:tc:opendocument:xmlns:text:1.0}style-name"

# Section heading pattern: § chapter-section. Title.
SECTION_RE = re.compile(r"^§\s*([\d]+[A-Za-z]?-[\d]+[A-Za-z.\-]*)\.\s*(.+)$")


def list_title_files() -> list:
    """Get list of statute title ODT files from Internet Archive metadata."""
    try:
        resp = SESSION.get(IA_METADATA, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"Failed to fetch IA metadata: {e}")
        return []

    files = []
    for f in data.get("files", []):
        name = f["name"]
        if (name.startswith(f"{IA_RELEASE}/gov.nc.stat.title.") and
                name.endswith(".odt")):
            files.append(name)

    files.sort()
    logger.info(f"Found {len(files)} title ODT files")
    return files


def download_odt(file_path: str) -> Optional[bytes]:
    """Download an ODT file from Internet Archive."""
    url = f"{IA_BASE}/{file_path}"
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=120)
            resp.raise_for_status()
            return resp.content
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
            else:
                logger.warning(f"Failed to download {file_path}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Download error for {file_path}: {e}")
            return None
    return None


def parse_odt_sections(odt_bytes: bytes) -> list:
    """Parse an ODT file and extract individual statute sections."""
    try:
        with tempfile.NamedTemporaryFile(suffix=".odt") as tmp:
            tmp.write(odt_bytes)
            tmp.flush()

            with zipfile.ZipFile(tmp.name) as z:
                with z.open("content.xml") as f:
                    tree = ET.parse(f)
                    root = tree.getroot()
    except Exception as e:
        logger.warning(f"Failed to parse ODT: {e}")
        return []

    paragraphs = root.findall(".//text:p", NS)

    sections = []
    current_section = None

    for p in paragraphs:
        style = p.get(STYLE_ATTR, "")
        text = "".join(p.itertext()).strip()
        if not text:
            continue

        if style == "P4":
            # Section heading — start a new section
            m = SECTION_RE.match(text)
            if m:
                if current_section and current_section["text_parts"]:
                    sections.append(current_section)

                section_num = m.group(1)
                section_title = m.group(2).strip()
                # Extract chapter from section number (e.g., "1-1" -> "1")
                chapter = section_num.split("-")[0] if "-" in section_num else ""

                current_section = {
                    "section_number": section_num,
                    "title": f"§ {section_num}. {section_title}",
                    "chapter": chapter,
                    "text_parts": [],
                    "history": [],
                    "current_field": None,
                }

        elif current_section is not None:
            if style == "P5":
                # Field label
                field = text.strip().lower()
                if field == "text":
                    current_section["current_field"] = "text"
                elif field == "history":
                    current_section["current_field"] = "history"
                elif field == "annotations":
                    current_section["current_field"] = "annotations"
                else:
                    current_section["current_field"] = None

            elif style == "P6":
                if current_section.get("current_field") == "text":
                    current_section["text_parts"].append(text)
                elif current_section.get("current_field") == "history":
                    current_section["history"].append(text)

            elif style == "P7":
                pass  # Skip annotations/case notes

    # Don't forget the last section
    if current_section and current_section["text_parts"]:
        sections.append(current_section)

    return sections


def normalize(section: dict) -> dict:
    """Normalize a parsed section into standard schema."""
    section_num = section["section_number"]
    chapter = section["chapter"]
    text = "\n".join(section["text_parts"])
    history = "\n".join(section.get("history", []))

    full_text = text
    if history:
        full_text += f"\n\nHistory: {history}"

    return {
        "_id": f"US/NC-Legislation/GS-{section_num}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": section["title"],
        "text": full_text,
        "date": "2023-06-01",  # Release date of archive data
        "url": f"https://www.ncleg.gov/EnactedLegislation/Statutes/HTML/BySection/Chapter_{chapter}/GS_{section_num}.html",
        "section_number": section_num,
        "chapter": chapter,
        "jurisdiction": "US-NC",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all statute sections with full text."""
    title_files = list_title_files()
    if not title_files:
        logger.error("No title files found")
        return

    total = 0
    for i, file_path in enumerate(title_files):
        logger.info(f"Processing {i+1}/{len(title_files)}: {file_path}")

        odt_bytes = download_odt(file_path)
        if not odt_bytes:
            continue

        sections = parse_odt_sections(odt_bytes)
        for section in sections:
            record = normalize(section)
            if len(record["text"]) >= 20:
                total += 1
                yield record

        if i < len(title_files) - 1:
            time.sleep(CRAWL_DELAY)

    logger.info(f"Total sections with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from a few title files."""
    records = []

    # Use a few small-to-medium title files for sampling
    sample_files = [
        f"{IA_RELEASE}/gov.nc.stat.title.001.odt",
        f"{IA_RELEASE}/gov.nc.stat.title.007A.odt",
    ]

    for file_path in sample_files:
        if len(records) >= count:
            break

        logger.info(f"Downloading sample: {file_path}")
        odt_bytes = download_odt(file_path)
        if not odt_bytes:
            continue

        sections = parse_odt_sections(odt_bytes)
        for section in sections:
            if len(records) >= count:
                break
            record = normalize(section)
            if len(record["text"]) >= 20:
                records.append(record)

        time.sleep(CRAWL_DELAY)

    return records


def test_api():
    """Test connectivity to Internet Archive."""
    logger.info("Testing Internet Archive connectivity...")

    try:
        resp = SESSION.get(IA_METADATA, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        files = [f for f in data.get("files", [])
                 if f["name"].startswith(f"{IA_RELEASE}/gov.nc.stat.title.")
                 and f["name"].endswith(".odt")]
        logger.info(f"Metadata OK - {len(files)} title files in {IA_RELEASE}")
    except Exception as e:
        logger.error(f"Metadata fetch failed: {e}")
        return False

    # Download and parse a small test file
    test_file = f"{IA_RELEASE}/gov.nc.stat.title.001B.odt"
    logger.info(f"Downloading test file: {test_file}")
    odt_bytes = download_odt(test_file)
    if not odt_bytes:
        logger.error("Test download failed")
        return False

    sections = parse_odt_sections(odt_bytes)
    logger.info(f"Parsed {len(sections)} sections from test file")

    if sections:
        sample = normalize(sections[0])
        logger.info(f"Sample: {sample['title'][:80]}")
        logger.info(f"Text preview ({len(sample['text'])} chars): {sample['text'][:200]}...")
        return True

    logger.error("No sections parsed from test file")
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

    return len(records) >= 10 and avg_text > 50


def main():
    parser = argparse.ArgumentParser(description="US/NC-Legislation Data Fetcher")
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
