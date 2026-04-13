#!/usr/bin/env python3
"""
US/HI-Legislation -- Hawaii Revised Statutes (HRS)

Fetches Hawaii statutes from the OpenHRS open data project on GitHub.
Source: https://github.com/OpenHRS/openhrs-data

The repo contains structured JSON files for every HRS section with full text,
organized as: hrscurrent/division/{N}/title/{N}/chapter/{N}/section/{id}.json

Each JSON file has:
  - name: section title
  - number: section number within chapter
  - text: full statutory text with annotations

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

SOURCE_ID = "US/HI-Legislation"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.HI-Legislation")

GITHUB_RAW_BASE = "https://raw.githubusercontent.com/OpenHRS/openhrs-data/master/hrscurrent"
GITHUB_API_BASE = "https://api.github.com/repos/OpenHRS/openhrs-data"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 0.5  # seconds between requests


def get_section_tree() -> List[dict]:
    """Get the full tree of section JSON files from the GitHub API."""
    url = f"{GITHUB_API_BASE}/git/trees/master:hrscurrent?recursive=1"
    try:
        resp = SESSION.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        tree = data.get("tree", [])
        sections = []
        for item in tree:
            if item["path"].endswith(".json") and "/section/" in item["path"]:
                sections.append(item)
        return sections
    except Exception as e:
        logger.error(f"Failed to get tree: {e}")
        return []


def download_section(path: str) -> Optional[dict]:
    """Download a section JSON file from GitHub."""
    url = f"{GITHUB_RAW_BASE}/{path}"
    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Failed to download {path}: {e}")
        return None


def parse_section_path(path: str) -> dict:
    """Extract division, title, chapter info from path."""
    info = {"division": "", "title_num": "", "chapter": ""}
    parts = path.split("/")
    for i, part in enumerate(parts):
        if part == "division" and i + 1 < len(parts):
            info["division"] = parts[i + 1]
        elif part == "title" and i + 1 < len(parts):
            info["title_num"] = parts[i + 1]
        elif part == "chapter" and i + 1 < len(parts):
            info["chapter"] = parts[i + 1]
    return info


def clean_text(text: str) -> str:
    """Clean up text: normalize whitespace, remove non-breaking spaces."""
    if not text:
        return ""
    text = text.replace("\u00a0", " ")
    # Collapse multiple spaces but preserve newlines
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n ", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def normalize(section_data: dict, path: str) -> Optional[dict]:
    """Normalize a section JSON into standard schema."""
    name = section_data.get("name", "")
    number = section_data.get("number", "")
    text = section_data.get("text", "")

    if not text or len(text) < 20:
        return None

    text = clean_text(text)
    path_info = parse_section_path(path)
    chapter = path_info["chapter"]

    # Section ID is chapter-number (e.g., "1-1" for chapter 1, section 1)
    section_id = f"{chapter}-{number}" if number else chapter

    # Build URL to capitol.hawaii.gov
    chapter_padded = chapter.zfill(4) if chapter.isdigit() else chapter
    url = f"https://www.capitol.hawaii.gov/hrscurrent/"

    full_title = f"HRS § {section_id} - {name}" if name else f"HRS § {section_id}"

    return {
        "_id": f"US/HI-Legislation/{section_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": full_title,
        "text": text,
        "date": "2017-01-01",  # Latest data in OpenHRS
        "url": url,
        "section_id": section_id,
        "chapter": chapter,
        "division": path_info["division"],
        "title_number": path_info["title_num"],
        "jurisdiction": "US-HI",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all HRS sections with full text."""
    logger.info("Getting section tree from GitHub API...")
    sections = get_section_tree()
    logger.info(f"Found {len(sections)} section files")

    total = 0
    skipped = 0

    for i, item in enumerate(sections):
        path = item["path"]
        data = download_section(path)

        if not data:
            skipped += 1
            continue

        record = normalize(data, path)
        if record:
            total += 1
            yield record
        else:
            skipped += 1

        if (i + 1) % 100 == 0:
            logger.info(f"  Processed {i + 1}/{len(sections)} files, {total} records, {skipped} skipped")
            time.sleep(CRAWL_DELAY)

    logger.info(f"Total sections with full text: {total}, skipped: {skipped}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records from first few sections."""
    records = []

    # Fetch from division 1, title 1, chapter 1
    logger.info("Getting section tree for sample...")
    tree = get_section_tree()

    if not tree:
        logger.error("Failed to get section tree")
        return []

    # Take first N sections
    sample_paths = tree[:count * 2]  # fetch extra in case some fail
    logger.info(f"Downloading {len(sample_paths)} section files...")

    for item in sample_paths:
        if len(records) >= count:
            break
        path = item["path"]
        data = download_section(path)
        time.sleep(CRAWL_DELAY)

        if not data:
            continue

        record = normalize(data, path)
        if record:
            records.append(record)

    return records


def test_api():
    """Test connectivity to GitHub and OpenHRS data."""
    logger.info("Testing GitHub API connectivity...")

    # Test tree endpoint
    tree = get_section_tree()
    if not tree:
        logger.error("Failed to get section tree")
        return False
    logger.info(f"Tree OK - {len(tree)} section files found")

    time.sleep(CRAWL_DELAY)

    # Test downloading a section
    path = tree[0]["path"]
    logger.info(f"Downloading test section: {path}")
    data = download_section(path)
    if not data:
        logger.error("Failed to download section")
        return False

    logger.info(f"Section: {data.get('name', 'N/A')}")
    text = data.get("text", "")
    logger.info(f"Text preview ({len(text)} chars): {text[:200]}...")

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
    parser = argparse.ArgumentParser(description="US/HI-Legislation Data Fetcher")
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
