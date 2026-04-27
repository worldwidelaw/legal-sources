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
  python bootstrap.py bootstrap             # Full extraction (all titles)
  python bootstrap.py test-api              # Test connectivity
"""

import json
import logging
import re
import sys
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

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

CRAWL_DELAY = 1  # seconds between requests

# ODF XML namespaces
NS = {
    "office": "urn:oasis:names:tc:opendocument:xmlns:office:1.0",
    "text": "urn:oasis:names:tc:opendocument:xmlns:text:1.0",
}

# Section ID pattern: e.g., "1-1-1" or "10-2-3.1"
SECTION_RE = re.compile(r"^(\d+(?:-\d+)+(?:\.\d+)?)\.\s+(.+)$")


class GALegislationScraper(BaseScraper):
    """Scraper for the Official Code of Georgia Annotated (O.C.G.A.)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get_title_files(self) -> List[str]:
        """Get list of OCGA title ODT filenames from Internet Archive metadata."""
        try:
            resp = self.session.get(IA_METADATA, timeout=30)
            resp.raise_for_status()
            meta = resp.json()
            files = [
                f["name"]
                for f in meta.get("files", [])
                if "release86" in f.get("name", "") and f["name"].endswith(".odt")
            ]
            files.sort()
            logger.info(f"Found {len(files)} ODT files in archive")
            return files
        except Exception as e:
            logger.warning(f"Metadata fetch failed, falling back to range: {e}")
            return [f"gov.ga.ocga.title.{num:02d}.odt" for num in range(1, 54)]

    def _download_odt(self, filename: str) -> Optional[bytes]:
        """Download an ODT file from Internet Archive."""
        # Metadata returns paths relative to item root (e.g. release86.2022.11/gov.ga.ocga.title.01.odt)
        url = f"https://archive.org/download/gov.ga.ocga.2018/{filename}"
        try:
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download {filename}: {e}")
            return None

    def _parse_odt_sections(self, odt_bytes: bytes) -> List[dict]:
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

    def _extract_title_num(self, filename: str) -> Optional[int]:
        """Extract title number from ODT filename."""
        m = re.search(r"title\.(\d+)\.odt$", filename)
        return int(m.group(1)) if m else None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all OCGA sections with full text."""
        title_files = self._get_title_files()
        total = 0

        for filename in title_files:
            title_num = self._extract_title_num(filename)
            if title_num is None:
                continue

            logger.info(f"Downloading Title {title_num} ({filename})...")
            odt_bytes = self._download_odt(filename)
            if not odt_bytes:
                continue

            time.sleep(CRAWL_DELAY)

            sections = self._parse_odt_sections(odt_bytes)
            logger.info(f"  Title {title_num}: {len(sections)} sections parsed")

            for section in sections:
                section["title_num"] = title_num
                total += 1
                yield section

        logger.info(f"Total sections with full text: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """OCGA is a static archive — fetch_updates yields same as fetch_all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize a parsed section into standard schema."""
        section_id = raw["section_id"]
        section_title = raw["section_title"]
        title_num = raw.get("title_num", 0)

        full_title = f"O.C.G.A. § {section_id} - {section_title}"

        chapter = section_id.split("-")[1] if "-" in section_id else "1"
        url = f"https://law.justia.com/codes/georgia/title-{title_num}/chapter-{chapter}/section-{section_id}/"

        record = {
            "_id": f"US/GA-Legislation/{section_id}",
            "_source": "US/GA-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": raw["text"],
            "date": "2022-11-01",  # Release 86, November 2022
            "url": url,
            "section_id": section_id,
            "title_number": str(title_num),
            "jurisdiction": "US-GA",
            "language": "en",
        }

        if raw.get("annotations"):
            record["annotations"] = raw["annotations"]

        return record


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/GA-Legislation Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    scraper = GALegislationScraper()

    if args.command == "test-api":
        logger.info("Testing Internet Archive connectivity...")
        try:
            resp = scraper.session.get(IA_METADATA, timeout=30)
            resp.raise_for_status()
            meta = resp.json()
            files = [
                f for f in meta.get("files", [])
                if "release86" in f.get("name", "") and f["name"].endswith(".odt")
            ]
            logger.info(f"Metadata OK - {len(files)} ODT files in release86")
        except Exception as e:
            logger.error(f"Metadata request failed: {e}")
            sys.exit(1)

        time.sleep(CRAWL_DELAY)

        logger.info("Downloading Title 1 for test...")
        odt_bytes = scraper._download_odt("gov.ga.ocga.title.01.odt")
        if not odt_bytes:
            logger.error("Failed to download Title 1")
            sys.exit(1)

        sections = scraper._parse_odt_sections(odt_bytes)
        if not sections:
            logger.error("No sections parsed from Title 1")
            sys.exit(1)

        logger.info(f"Parsed {len(sections)} sections from Title 1")
        sample = sections[0]
        logger.info(f"Sample section: § {sample['section_id']} - {sample['section_title']}")
        logger.info(f"Text preview ({len(sample['text'])} chars): {sample['text'][:200]}...")

    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.run_sample(n=15)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
