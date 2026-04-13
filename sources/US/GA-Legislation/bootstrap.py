#!/usr/bin/env python3
"""
US/GA-Legislation -- Official Code of Georgia Annotated (O.C.G.A.)

Fetches Georgia statutes from the Internet Archive's structured ODT collection.
The O.C.G.A. was ruled non-copyrightable by the US Supreme Court in
Georgia v. Public.Resource.Org (2020).

Source: https://archive.org/details/gov.ga.ocga.2018 (release86, Nov 2022)

Each ODT file is a ZIP containing content.xml with structured paragraphs:
  - P6 style = section header (e.g., "1-1-1. Enactment of Code.")
  - P4 "Text" = start of statutory text
  - P7 style = statutory text paragraphs
  - P4 "Annotations" / "History" = annotations (included separately)

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample sections
  python bootstrap.py bootstrap             # Full extraction (all 53 titles)
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import json
import logging
import re
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Generator, List, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/GA-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.GA-Legislation")

# Internet Archive base URL for the OCGA collection
IA_BASE = "https://archive.org/download/gov.ga.ocga.2018/release86.2022.11"
IA_METADATA = "https://archive.org/metadata/gov.ga.ocga.2018"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 1  # seconds between requests

# ODF XML namespaces
NS = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
}

# Section ID pattern: e.g., "1-1-1" or "10-2-3.1"
SECTION_RE = re.compile(r"^(\d+(?:-\d+)+(?:\.\d+)?)\.\s+(.+)$")


def get_title_files() -> List[str]:
    """Get list of OCGA title ODT filenames from the collection."""
    titles = []
    for num in range(1, 54):
        titles.append(f"gov.ga.ocga.title.{num:02d}.odt")
    return titles


def download_odt(filename: str) -> Optional[bytes]:
    """Download an ODT file from Internet Archive."""
    url = f"{IA_BASE}/{filename}"
    try:
        resp = SESSION.get(url, timeout=120)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        logger.warning(f"Failed to download {filename}: {e}")
        return None


def parse_odt_sections(odt_bytes: bytes) -> List[dict]:
    """Parse an ODT file and extract individual code sections with full text."""
    sections = []

    try:
        zf = zipfile.ZipFile(BytesIO(odt_bytes))
        content_xml = zf.read("content.xml").decode("utf-8")
        root = ET.fromstring(content_xml)
    except Exception as e:
        logger.warning(f"Failed to parse ODT: {e}")
        return []

    body = root.find(".//office:body/office:text", NS)
    if body is None:
        return []

    style_attr = f"{{{NS['text']}}}style-name"
    paragraphs = body.findall(".//text:p", NS)

    current_section_id = None
    current_section_title = None
    current_text_parts = []
    current_annotations = []
    in_text = False
    in_annotations = False

    def flush_section():
        nonlocal current_section_id, current_section_title
        nonlocal current_text_parts, current_annotations
        nonlocal in_text, in_annotations

        if current_section_id and current_text_parts:
            text = "\n".join(current_text_parts).strip()
            annotations = "\n".join(current_annotations).strip()
            if len(text) >= 20:
                sections.append({
                    "section_id": current_section_id,
                    "section_title": current_section_title,
                    "text": text,
                    "annotations": annotations if annotations else None,
                })

        current_section_id = None
        current_section_title = None
        current_text_parts = []
        current_annotations = []
        in_text = False
        in_annotations = False

    for p in paragraphs:
        style = p.get(style_attr, "")
        text = "".join(p.itertext()).strip()
        if not text:
            continue

        # Section header (P6 style)
        if style == "P6":
            flush_section()
            m = SECTION_RE.match(text)
            if m:
                current_section_id = m.group(1)
                current_section_title = m.group(2)
            else:
                # Try to extract section ID from the beginning
                parts = text.split(".", 1)
                if parts and re.match(r"^\d+(-\d+)+", parts[0]):
                    current_section_id = parts[0]
                    current_section_title = parts[1].strip() if len(parts) > 1 else text
            continue

        # Sub-section markers (P4 style)
        if style == "P4":
            if text == "Text":
                in_text = True
                in_annotations = False
            elif text in ("Annotations", "History"):
                in_text = False
                in_annotations = True
            continue

        # Collect text content
        if current_section_id:
            if in_text and style == "P7":
                current_text_parts.append(text)
            elif in_annotations and style in ("P5", "P7"):
                current_annotations.append(text)

    # Flush last section
    flush_section()

    return sections


def normalize(section: dict, title_num: int) -> dict:
    """Normalize a parsed section into standard schema."""
    section_id = section["section_id"]
    section_title = section["section_title"]

    # Build full title string
    full_title = f"O.C.G.A. § {section_id} - {section_title}"

    url = f"https://law.justia.com/codes/georgia/title-{title_num}/chapter-{section_id.split('-')[1] if '-' in section_id else '1'}/section-{section_id}/"

    record = {
        "_id": f"US/GA-Legislation/{section_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": section["text"],
        "date": "2022-11-01",  # Release 86, November 2022
        "url": url,
        "section_id": section_id,
        "title_number": str(title_num),
        "jurisdiction": "US-GA",
        "language": "en",
    }

    if section.get("annotations"):
        record["annotations"] = section["annotations"]

    return record


def fetch_all() -> Generator[dict, None, None]:
    """Yield all OCGA sections with full text."""
    title_files = get_title_files()
    total = 0

    for filename in title_files:
        title_match = re.search(r"title\.(\d+)\.odt$", filename)
        if not title_match:
            continue
        title_num = int(title_match.group(1))

        logger.info(f"Downloading Title {title_num} ({filename})...")
        odt_bytes = download_odt(filename)
        if not odt_bytes:
            continue

        time.sleep(CRAWL_DELAY)

        sections = parse_odt_sections(odt_bytes)
        logger.info(f"  Title {title_num}: {len(sections)} sections parsed")

        for section in sections:
            record = normalize(section, title_num)
            total += 1
            yield record

    logger.info(f"Total sections with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from Title 1."""
    records = []

    logger.info("Downloading Title 1 for sample...")
    odt_bytes = download_odt("gov.ga.ocga.title.01.odt")
    if not odt_bytes:
        logger.error("Failed to download Title 1")
        return []

    sections = parse_odt_sections(odt_bytes)
    logger.info(f"Parsed {len(sections)} sections from Title 1")

    for section in sections[:count]:
        record = normalize(section, 1)
        records.append(record)

    return records


def test_api():
    """Test connectivity to Internet Archive."""
    logger.info("Testing Internet Archive connectivity...")

    # Test metadata endpoint
    try:
        resp = SESSION.get(IA_METADATA, timeout=30)
        resp.raise_for_status()
        meta = resp.json()
        files = [f for f in meta.get("files", [])
                 if "release86" in f.get("name", "") and f["name"].endswith(".odt")]
        logger.info(f"Metadata OK - {len(files)} ODT files in release86")
    except Exception as e:
        logger.error(f"Metadata request failed: {e}")
        return False

    time.sleep(CRAWL_DELAY)

    # Test downloading Title 1
    logger.info("Downloading Title 1 for test...")
    odt_bytes = download_odt("gov.ga.ocga.title.01.odt")
    if not odt_bytes:
        logger.error("Failed to download Title 1")
        return False

    sections = parse_odt_sections(odt_bytes)
    if not sections:
        logger.error("No sections parsed from Title 1")
        return False

    logger.info(f"Parsed {len(sections)} sections from Title 1")
    sample = sections[0]
    logger.info(f"Sample section: § {sample['section_id']} - {sample['section_title']}")
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
    parser = argparse.ArgumentParser(description="US/GA-Legislation Data Fetcher")
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
