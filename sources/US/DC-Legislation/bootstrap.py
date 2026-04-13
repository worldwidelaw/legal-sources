#!/usr/bin/env python3
"""
US/DC-Legislation -- District of Columbia Code (code.dccouncil.gov)

Fetches all DC Code sections with full text from the DC Council's open-source
XML repository on GitHub (DCCouncil/law-xml-codified).

Strategy:
  1. Use GitHub API to get full file tree of the repo
  2. Filter for section XML files under us/dc/council/code/titles/*/sections/
  3. Fetch each section XML from raw.githubusercontent.com
  4. Parse XML to extract section number, heading, and full text
  5. Normalize into standard schema

Data: Public domain (DC government works). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all sections)
  python bootstrap.py bootstrap --sample   # Fetch ~30 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DC-Legislation")

RAW_BASE = "https://raw.githubusercontent.com/DCCouncil/law-xml-codified/publication/2021-10-18"
API_TREE = "https://api.github.com/repos/DCCouncil/law-xml-codified/git/trees/publication/2021-10-18?recursive=1"
CODE_BASE = "us/dc/council/code"

NS = {"dc": "https://code.dccouncil.us/schemas/dc-library"}

# Sample: titles 1 (Government), 22 (Criminal), 47 (Taxation)
SAMPLE_TITLES = {"1", "22", "47"}
SAMPLE_LIMIT_PER_TITLE = 10


def extract_text_from_element(elem) -> str:
    """Recursively extract text from an XML element and its children."""
    parts = []
    if elem.text:
        parts.append(elem.text.strip())
    for child in elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if tag == "para":
            para_text = extract_para(child)
            if para_text:
                parts.append(para_text)
        elif tag == "text":
            t = extract_inline_text(child)
            if t:
                parts.append(t)
        elif tag in ("include", "container"):
            sub = extract_text_from_element(child)
            if sub:
                parts.append(sub)
        elif tag == "aftertext":
            t = extract_inline_text(child)
            if t:
                parts.append(t)
        # Skip annotations, codify, etc.
    return "\n".join(parts)


def extract_para(para_elem) -> str:
    """Extract text from a <para> element with its number prefix."""
    parts = []
    num = para_elem.find("dc:num", NS)
    num_text = ""
    if num is not None and num.text:
        num_text = num.text.strip()

    # Get direct <text> children
    for child in para_elem:
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if tag == "text":
            t = extract_inline_text(child)
            if t:
                if num_text and not parts:
                    parts.append(f"({num_text}) {t}" if not num_text.startswith("(") else f"{num_text} {t}")
                else:
                    parts.append(t)
        elif tag == "para":
            sub = extract_para(child)
            if sub:
                parts.append(sub)
        elif tag == "aftertext":
            t = extract_inline_text(child)
            if t:
                parts.append(t)
        elif tag == "include":
            sub = extract_text_from_element(child)
            if sub:
                parts.append(sub)

    # If we have a num but no text children added it yet
    if num_text and not parts:
        parts.append(f"({num_text})" if not num_text.startswith("(") else num_text)

    return "\n".join(parts)


def extract_inline_text(elem) -> str:
    """Extract text from a <text> element, handling inline children like <cite>."""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        # Inline elements like <cite>, <code-cite> — get their text
        if child.text:
            parts.append(child.text)
        if child.tail:
            parts.append(child.tail)
    if elem.tail:
        pass  # tail belongs to parent
    return "".join(parts).strip()


class DCLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "application/xml, text/xml, */*",
            },
            timeout=60,
        )
        self.delay = 0.5  # GitHub raw is generous with rate limits

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def test_api(self):
        """Test connectivity to DC Code XML on GitHub."""
        logger.info("Testing DC Code XML on GitHub...")
        try:
            xml_text = self._get(f"{RAW_BASE}/{CODE_BASE}/titles/1/sections/1-101.xml")
            root = ET.fromstring(xml_text)
            num = root.find("dc:num", NS)
            heading = root.find("dc:heading", NS)
            if num is not None and num.text == "1-101" and heading is not None:
                logger.info(f"  Section 1-101: OK ({heading.text})")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Section 1-101: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_sections(self) -> list:
        """Use GitHub API to discover all section XML file paths.

        Returns list of tuples: (title_num, section_filename, full_path)
        """
        logger.info("Discovering sections via GitHub tree API...")
        resp = self.http.get(API_TREE)
        data = resp.json()

        sections = []
        pattern = re.compile(
            rf'^{re.escape(CODE_BASE)}/titles/([^/]+)/sections/(.+\.xml)$'
        )
        for item in data.get("tree", []):
            m = pattern.match(item["path"])
            if m:
                title_num = m.group(1)
                sec_file = m.group(2)
                sections.append((title_num, sec_file, item["path"]))

        logger.info(f"Discovered {len(sections)} section files across all titles")
        return sections

    def parse_section_xml(self, xml_text: str, title_num: str) -> dict:
        """Parse a section XML file and extract structured data."""
        root = ET.fromstring(xml_text)

        num_elem = root.find("dc:num", NS)
        heading_elem = root.find("dc:heading", NS)

        if num_elem is None or not num_elem.text:
            return None

        sec_num = num_elem.text.strip()
        heading = heading_elem.text.strip() if heading_elem is not None and heading_elem.text else ""

        # Extract full text from all <para> and <text> children (skip <annotations>)
        text_parts = []
        for child in root:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if tag == "text":
                t = extract_inline_text(child)
                if t:
                    text_parts.append(t)
            elif tag == "para":
                t = extract_para(child)
                if t:
                    text_parts.append(t)
            elif tag == "include":
                t = extract_text_from_element(child)
                if t:
                    text_parts.append(t)
            # Skip: num, heading, annotations, codify, etc.

        text = "\n".join(text_parts).strip()

        if not text:
            return None

        title = f"D.C. Code § {sec_num}"
        if heading:
            title = f"D.C. Code § {sec_num} — {heading}"

        return {
            "section_num": sec_num,
            "heading": heading,
            "title": title,
            "text": text,
            "title_num": title_num,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return {
            "_id": raw["section_num"],
            "_source": "US/DC-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw["title"],
            "text": raw["text"],
            "date": today,
            "url": f"https://code.dccouncil.gov/us/dc/council/code/sections/{raw['section_num']}",
            "title_num": raw["title_num"],
            "section_num": raw["section_num"],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all DC Code sections."""
        sections = self.discover_sections()
        logger.info(f"Fetching {len(sections)} sections...")
        total = 0
        errors = 0
        for title_num, sec_file, full_path in sections:
            try:
                xml_text = self._get(f"{RAW_BASE}/{full_path}")
                raw = self.parse_section_xml(xml_text, title_num)
                if raw:
                    yield self.normalize(raw)
                    total += 1
                    if total % 500 == 0:
                        logger.info(f"  Progress: {total} sections fetched")
            except Exception as e:
                errors += 1
                if errors <= 10:
                    logger.warning(f"  {full_path}: failed ({e})")
        logger.info(f"Total sections fetched: {total} (errors: {errors})")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample from a few titles."""
        sections = self.discover_sections()
        logger.info(f"Fetching sample from titles {SAMPLE_TITLES}...")
        count = 0
        title_counts = {}
        for title_num, sec_file, full_path in sections:
            if title_num not in SAMPLE_TITLES:
                continue
            tc = title_counts.get(title_num, 0)
            if tc >= SAMPLE_LIMIT_PER_TITLE:
                continue
            try:
                xml_text = self._get(f"{RAW_BASE}/{full_path}")
                raw = self.parse_section_xml(xml_text, title_num)
                if raw:
                    yield self.normalize(raw)
                    count += 1
                    title_counts[title_num] = tc + 1
            except Exception as e:
                logger.warning(f"  {full_path}: failed ({e})")

        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DC-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = DCLegislationScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            gen = scraper.fetch_sample()
        else:
            gen = scraper.fetch_all()

        count = 0
        for record in gen:
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            if count <= 20 or count % 100 == 0:
                logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
