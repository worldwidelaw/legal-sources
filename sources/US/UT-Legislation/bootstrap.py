#!/usr/bin/env python3
"""
US/UT-Legislation -- Utah Code

Fetches Utah statutes from the Utah State Legislature website as XML.
Each title is available as a single XML file with structured section data.

Strategy:
  1. Fetch the code index page to discover all titles and their versions
  2. Download each title's XML file from le.utah.gov
  3. Parse <section> elements for section numbers, catchlines, and full text
  4. Normalize into standard schema

Data: Public domain. No auth required.
Rate limit: 1 req / 2 sec (SSL cert may require --insecure).

Usage:
  python bootstrap.py bootstrap            # Full pull (all ~95 titles)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
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
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.UT-Legislation")

BASE_URL = "https://le.utah.gov/xcode"
INDEX_URL = f"{BASE_URL}/C_1800010118000101.html"

# Sample titles: Criminal, Taxation, Education, Property, Courts
SAMPLE_TITLES = ["76", "59", "53E", "57", "78B"]


def extract_section_text(section_el) -> str:
    """Recursively extract all text from a <section> element,
    removing XML tags but preserving structure with newlines."""
    parts = []

    def _walk(el, depth=0):
        tag = el.tag if isinstance(el.tag, str) else ""
        # Skip history/metadata elements
        if tag in ("histories", "history", "modyear", "modchap"):
            return

        text = (el.text or "").strip()
        if text:
            parts.append(text)

        for child in el:
            child_tag = child.tag if isinstance(child.tag, str) else ""
            if child_tag == "subsection":
                _walk(child, depth + 1)
            elif child_tag == "xref":
                # Include cross-reference text inline
                ref_text = (child.text or "").strip()
                if ref_text:
                    parts.append(ref_text)
                tail = (child.tail or "").strip()
                if tail:
                    parts.append(tail)
            elif child_tag in ("tab",):
                pass  # skip formatting
            elif child_tag not in ("histories", "history", "modyear", "modchap"):
                _walk(child, depth + 1)

            # Handle tail text (text after a child element)
            if child_tag not in ("xref",):  # already handled above
                tail = (child.tail or "").strip()
                if tail:
                    parts.append(tail)

    _walk(section_el)
    return "\n".join(parts)


class UTLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
            },
            timeout=120,
            verify=False,
        )
        self.delay = 2.0

    def _get(self, url: str) -> Optional[str]:
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        try:
            resp = self.http.get(url)
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _discover_titles(self) -> list[dict]:
        """Parse the index page to discover all titles and their versions."""
        html = self._get(INDEX_URL)
        if not html:
            logger.error("Failed to fetch index page")
            return []

        titles = []
        # Pattern: href="Title{ID}/{ID}.html?v=C{ID}_{VERSION}"
        pattern = re.compile(
            r'href="Title([^/]+)/[^"]*\?v=(C[^"]+)"[^>]*>.*?</a>\s*</td>\s*<td>([^<]+)',
            re.DOTALL,
        )
        for m in pattern.finditer(html):
            title_id = m.group(1)
            version = m.group(2)
            catchline = m.group(3).strip()
            titles.append({
                "title_id": title_id,
                "version": version,
                "catchline": catchline,
            })
        logger.info(f"Discovered {len(titles)} titles")
        return titles

    def _xml_url(self, title_id: str, version: str) -> str:
        return f"{BASE_URL}/Title{title_id}/{version}.xml"

    def _parse_xml_title(self, xml_text: str, title_id: str, title_catchline: str) -> Generator[dict, None, None]:
        """Parse all sections from a title's XML."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as e:
            logger.warning(f"XML parse error for Title {title_id}: {e}")
            return

        # Build section→chapter mapping first (avoids O(n²))
        sec_to_chapter = {}
        for chapter in root.iter("chapter"):
            ch_cl = chapter.find("catchline")
            ch_name = ch_cl.text.strip() if ch_cl is not None and ch_cl.text else ""
            for s in chapter.iter("section"):
                sn = s.get("number", "")
                if sn:
                    sec_to_chapter[sn] = ch_name

        # Walk all <section> elements
        for section in root.iter("section"):
            sec_num = section.get("number", "")
            if not sec_num:
                continue

            # Get catchline
            catchline_el = section.find("catchline")
            catchline = ""
            if catchline_el is not None and catchline_el.text:
                catchline = catchline_el.text.strip()

            # Get amendment year from histories
            year = ""
            modyear_el = section.find(".//modyear")
            if modyear_el is not None and modyear_el.text:
                year = modyear_el.text.strip()

            # Extract full text
            text = extract_section_text(section)
            if not text or len(text) < 5:
                continue

            yield {
                "section_number": sec_num,
                "catchline": catchline,
                "text": text,
                "title_id": title_id,
                "title_catchline": title_catchline,
                "chapter_catchline": sec_to_chapter.get(sec_num, ""),
                "year": year,
            }

    def normalize(self, raw: dict) -> dict:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sec = raw["section_number"]
        date = f"{raw['year']}-01-01" if raw.get("year") else None

        return {
            "_id": f"utcode-{sec}",
            "_source": "US/UT-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": f"Utah Code § {sec} — {raw['catchline']}",
            "text": raw["text"],
            "date": date,
            "url": f"https://le.utah.gov/xcode/Title{raw['title_id']}/{raw['title_id']}.html",
            "section_number": sec,
            "title_number": raw["title_id"],
            "title_name": raw["title_catchline"],
            "chapter": raw["chapter_catchline"],
        }

    def test_api(self):
        logger.info("Testing le.utah.gov XML access...")
        try:
            # Test index
            html = self._get(INDEX_URL)
            if not html or "Utah Code" not in html:
                logger.error("Index page test FAILED")
                return False
            logger.info("  Index page OK")

            # Test XML download for Title 7
            xml = self._get(f"{BASE_URL}/Title7/C7_1800010118000101.xml")
            if not xml or "<title" not in xml:
                logger.error("XML download test FAILED")
                return False
            root = ET.fromstring(xml)
            sections = list(root.iter("section"))
            logger.info(f"  Title 7 XML: {len(xml)} bytes, {len(sections)} sections")
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        titles = self._discover_titles()
        total = 0
        for t in titles:
            xml = self._get(self._xml_url(t["title_id"], t["version"]))
            if not xml or len(xml) < 100:
                logger.warning(f"  Skipping Title {t['title_id']}: empty XML")
                continue

            title_count = 0
            for raw in self._parse_xml_title(xml, t["title_id"], t["catchline"]):
                yield raw
                total += 1
                title_count += 1

            if title_count > 0:
                logger.info(f"  Title {t['title_id']} ({t['catchline']}): {title_count} sections")

        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        logger.info("Fetching sample sections...")
        titles = self._discover_titles()
        count = 0

        # Filter to sample titles
        sample_set = set(SAMPLE_TITLES)
        sample_titles = [t for t in titles if t["title_id"] in sample_set]

        # If sample titles not found, use first 3
        if len(sample_titles) < 3:
            sample_titles = titles[:3]

        for t in sample_titles:
            if count >= 15:
                break
            xml = self._get(self._xml_url(t["title_id"], t["version"]))
            if not xml or len(xml) < 100:
                continue

            for raw in self._parse_xml_title(xml, t["title_id"], t["catchline"]):
                if count >= 15:
                    break
                yield raw
                count += 1

        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/UT-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = UTLegislationScraper()

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
            safe_id = record["_id"].replace("/", "_")
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} - {record['title'][:60]} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
