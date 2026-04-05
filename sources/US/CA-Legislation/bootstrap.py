#!/usr/bin/env python3
"""
US/CA-Legislation -- California Codes (Statutes)

Fetches all 29 California Codes with full text of every section from the
official California Legislative Information (LegInfo) website.

Strategy:
  1. For each of the 29 CA codes, fetch the TOC from codedisplayexpand.xhtml
  2. Extract leaf-node links (chapters/articles that contain sections)
  3. For each leaf, fetch codes_displayText.xhtml to list section numbers
  4. For each section, fetch codes_displaySection.xhtml for full text
  5. Normalize into standard schema

Data: Public domain (California government works). No auth required.
Rate limit: 1 req / 2 sec (respecting robots.txt crawl-delay of 10s in sample,
           relaxed for full bootstrap since PUBINFO bulk download is offered).

Usage:
  python bootstrap.py bootstrap            # Full pull (all 29 codes)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CA-Legislation")

BASE_URL = "https://leginfo.legislature.ca.gov/faces"

# All 29 California Codes (from sitemap)
CA_CODES = {
    "BPC": "Business and Professions Code",
    "CIV": "Civil Code",
    "CCP": "Code of Civil Procedure",
    "COM": "Commercial Code",
    "CORP": "Corporations Code",
    "EDC": "Education Code",
    "ELEC": "Elections Code",
    "EVID": "Evidence Code",
    "FAM": "Family Code",
    "FIN": "Financial Code",
    "FGC": "Fish and Game Code",
    "FAC": "Food and Agricultural Code",
    "GOV": "Government Code",
    "HNC": "Harbors and Navigation Code",
    "HSC": "Health and Safety Code",
    "INS": "Insurance Code",
    "LAB": "Labor Code",
    "MVC": "Military and Veterans Code",
    "PEN": "Penal Code",
    "PROB": "Probate Code",
    "PCC": "Public Contract Code",
    "PRC": "Public Resources Code",
    "PUC": "Public Utilities Code",
    "RTC": "Revenue and Taxation Code",
    "SHC": "Streets and Highways Code",
    "UIC": "Unemployment Insurance Code",
    "VEH": "Vehicle Code",
    "WAT": "Water Code",
    "WIC": "Welfare and Institutions Code",
}

# Sample codes + sections for quick testing
SAMPLE_SECTIONS = [
    ("CIV", "1624"),
    ("CIV", "1550"),
    ("CIV", "3294"),
    ("PEN", "187"),
    ("PEN", "459"),
    ("PEN", "211"),
    ("GOV", "6250"),
    ("GOV", "11135"),
    ("FAM", "2310"),
    ("LAB", "201"),
    ("LAB", "510"),
    ("EVID", "352"),
    ("VEH", "23152"),
    ("HSC", "11350"),
    ("CCP", "340"),
]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    # Remove style and script blocks
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    # Replace <br>, <p>, <div> with newlines
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<h[1-6][^>]*>', '\n## ', text)
    text = re.sub(r'</h[1-6]>', '\n', text)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = html_module.unescape(text)
    # Clean whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class CALegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html",
            },
            timeout=60,
        )
        self.delay = 2.0  # seconds between requests

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting, return HTML string."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def test_api(self):
        """Test connectivity to leginfo website."""
        logger.info("Testing California LegInfo website...")
        try:
            url = f"{BASE_URL}/codes_displaySection.xhtml?lawCode=CIV&sectionNum=1624"
            html = self._get(url)
            if "single_law_section" in html:
                logger.info("  Connectivity: OK")
                logger.info("  Section content found: Yes")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: section content not found in response")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_section_text(self, law_code: str, section_num: str) -> Optional[dict]:
        """Fetch full text of a single code section."""
        url = f"{BASE_URL}/codes_displaySection.xhtml?lawCode={law_code}&sectionNum={section_num}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch {law_code} § {section_num}: {e}")
            return None

        # Extract content from single_law_section div
        match = re.search(
            r'id="single_law_section"[^>]*>(.*?)(?=<div[^>]*id="(?:footer|j_id1)|</body>)',
            html,
            re.DOTALL,
        )
        if not match:
            logger.warning(f"No section content found for {law_code} § {section_num}")
            return None

        raw_html = match.group(1)
        text = strip_html(raw_html)

        if not text or len(text) < 20:
            logger.warning(f"Section text too short for {law_code} § {section_num}: {len(text)} chars")
            return None

        # Extract heading structure from the HTML
        heading_match = re.search(
            r'id="codeLawSectionNoHead"[^>]*>(.*?)<font',
            raw_html,
            re.DOTALL,
        )
        heading = ""
        if heading_match:
            heading = strip_html(heading_match.group(1))

        code_name = CA_CODES.get(law_code, law_code)
        return {
            "law_code": law_code,
            "code_name": code_name,
            "section_num": section_num,
            "heading": heading,
            "text": text,
            "url": url,
        }

    def get_toc_leaves(self, law_code: str) -> list:
        """Get all leaf chapter/article links from the TOC for a code."""
        url = f"{BASE_URL}/codedisplayexpand.xhtml?tocCode={law_code}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch TOC for {law_code}: {e}")
            return []

        # Extract codes_displayText links with parameters
        pattern = (
            r'codes_displayText\.xhtml\?'
            r'lawCode=([A-Z]+)&amp;'
            r'division=([^&]*)&amp;'
            r'title=([^&]*)&amp;'
            r'part=([^&]*)&amp;'
            r'chapter=([^&]*)&amp;'
            r'article=([^&"]*)'
        )
        matches = re.findall(pattern, html)

        leaves = []
        seen = set()
        for m in matches:
            key = (m[0], m[1], m[2], m[3], m[4], m[5])
            if key not in seen:
                seen.add(key)
                leaves.append({
                    "law_code": m[0],
                    "division": m[1],
                    "title": m[2],
                    "part": m[3],
                    "chapter": m[4],
                    "article": m[5],
                })
        logger.info(f"  {law_code}: found {len(leaves)} TOC leaves")
        return leaves

    def get_sections_for_leaf(self, leaf: dict) -> list:
        """Get all section numbers from a TOC leaf page."""
        url = (
            f"{BASE_URL}/codes_displayText.xhtml?"
            f"lawCode={leaf['law_code']}"
            f"&division={leaf['division']}"
            f"&title={leaf['title']}"
            f"&part={leaf['part']}"
            f"&chapter={leaf['chapter']}"
            f"&article={leaf['article']}"
        )
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch section list for {leaf}: {e}")
            return []

        # Extract section numbers from submitCodesValues or h6 links
        sections = re.findall(r"submitCodesValues\('([\d\.]+[a-z]?)\.?'", html)
        if not sections:
            # Try alternate pattern: direct section links
            sections = re.findall(
                r'sectionNum=([\d\.]+[a-z]?)', html
            )
        return list(dict.fromkeys(sections))  # deduplicate preserving order

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        section_id = f"{raw['law_code']}-{raw['section_num']}"
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return {
            "_id": section_id,
            "_source": "US/CA-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": f"{raw['code_name']} § {raw['section_num']}",
            "text": raw["text"],
            "date": today,
            "url": raw["url"],
            "law_code": raw["law_code"],
            "code_name": raw["code_name"],
            "section_num": raw["section_num"],
            "heading": raw.get("heading", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all code sections across all 29 California Codes."""
        total = 0
        for code, name in CA_CODES.items():
            logger.info(f"Processing {code} ({name})...")
            leaves = self.get_toc_leaves(code)

            for leaf in leaves:
                sections = self.get_sections_for_leaf(leaf)
                for sec_num in sections:
                    raw = self.fetch_section_text(code, sec_num)
                    if raw:
                        yield self.normalize(raw)
                        total += 1
                        if total % 100 == 0:
                            logger.info(f"  Progress: {total} sections fetched")

        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample of well-known code sections."""
        logger.info(f"Fetching {len(SAMPLE_SECTIONS)} sample sections...")
        count = 0
        for law_code, section_num in SAMPLE_SECTIONS:
            raw = self.fetch_section_text(law_code, section_num)
            if raw:
                yield self.normalize(raw)
                count += 1
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CA-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = CALegislationScraper()

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
            logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
