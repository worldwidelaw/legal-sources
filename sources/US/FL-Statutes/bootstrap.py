#!/usr/bin/env python3
"""
US/FL-Statutes -- Florida Statutes (Online Sunshine)

Fetches all Florida Statutes with full text from the official Online Sunshine
website (leg.state.fl.us).

Strategy:
  1. Scrape title index pages to discover all chapter URLs (with range dirs)
  2. For each chapter, fetch the ContentsIndex page to get section file names
  3. Fetch each section's HTML page and extract full text from structured spans
  4. Normalize into standard schema

Data: Public domain (Florida government works). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all chapters)
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
logger = logging.getLogger("legal-data-hunter.US.FL-Statutes")

BASE_URL = "https://www.leg.state.fl.us/statutes"
STATUTE_YEAR = "2025"

# Roman numeral titles (I through XLIX)
TITLE_NUMERALS = [
    "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X",
    "XI", "XII", "XIII", "XIV", "XV", "XVI", "XVII", "XVIII", "XIX", "XX",
    "XXI", "XXII", "XXIII", "XXIV", "XXV", "XXVI", "XXVII", "XXVIII",
    "XXIX", "XXX", "XXXI", "XXXII", "XXXIII", "XXXIV", "XXXV", "XXXVI",
    "XXXVII", "XXXVIII", "XXXIX", "XL", "XLI", "XLII", "XLIII", "XLIV",
    "XLV", "XLVI", "XLVII", "XLVIII", "XLIX",
]

# Sample: chapters 1 (Construction of Statutes), 316 (Motor Vehicles), 775 (Crimes)
SAMPLE_CHAPTERS = [
    {"range": "0000-0099", "padded": "0001"},
    {"range": "0300-0399", "padded": "0316"},
    {"range": "0700-0799", "padded": "0775"},
]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n\n', text)
    # Remove complete tags
    text = re.sub(r'<[^>]+>', '', text)
    # Remove any trailing incomplete tags (e.g., "<div" at end of truncated HTML)
    text = re.sub(r'<[^>]*$', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class FLStatutesScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=60,
        )
        self.delay = 2.0

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting and retry."""
        time.sleep(self.delay)
        for attempt in range(3):
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code in (404, 410):
                    return ""
                logger.warning("HTTP %d for %s (attempt %d)", resp.status_code, url[:100], attempt + 1)
            except Exception as e:
                logger.warning("Request failed for %s (attempt %d): %s", url[:100], attempt + 1, e)
            if attempt < 2:
                time.sleep(3 * (attempt + 1))
        return ""

    def test_api(self):
        """Test connectivity to Florida statutes site."""
        logger.info("Testing Florida Online Sunshine...")
        try:
            # Test title index
            url = f"{BASE_URL}/index.cfm?App_mode=Display_Index&Title_Request=I"
            html = self._get(url)
            if "0001" in html and "ContentsIndex" in html:
                logger.info("  Title index: OK")
            else:
                logger.error("  Title index: unexpected content")
                return False

            # Test chapter TOC
            url = f"{BASE_URL}/index.cfm?App_mode=Display_Statute&URL=0000-0099/0001/0001ContentsIndex.html&StatuteYear={STATUTE_YEAR}"
            html = self._get(url)
            if "Sections/0001.01.html" in html:
                logger.info("  Chapter TOC: OK (Chapter 1)")
            else:
                logger.error("  Chapter TOC: unexpected content")
                return False

            # Test section page
            url = f"{BASE_URL}/index.cfm?App_mode=Display_Statute&Search_String=&URL=0000-0099/0001/Sections/0001.01.html"
            html = self._get(url)
            if "Definitions" in html and "SectionBody" in html:
                logger.info("  Section page: OK (§ 1.01)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Section page: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_chapters(self) -> list:
        """Discover all chapters by scraping title index pages.

        Returns list of dicts with 'range' and 'padded' keys.
        """
        chapters = {}  # padded -> range_dir

        for title in TITLE_NUMERALS:
            url = f"{BASE_URL}/index.cfm?App_mode=Display_Index&Title_Request={title}"
            try:
                html = self._get(url)
                # Pattern: URL=0700-0799/0775/0775ContentsIndex.html
                for m in re.finditer(r'(\d{4}-\d{4})/(\d{4})/\d{4}ContentsIndex\.html', html):
                    range_dir = m.group(1)
                    chap_padded = m.group(2)
                    chapters[chap_padded] = range_dir
                logger.info(f"  Title {title}: total chapters so far: {len(chapters)}")
            except Exception as e:
                logger.warning(f"  Title {title}: failed ({e})")

        result = [{"range": chapters[k], "padded": k} for k in sorted(chapters.keys())]
        logger.info(f"Discovered {len(result)} chapters total")
        return result

    def get_section_files(self, range_dir: str, chap_padded: str) -> list:
        """Get list of section file names from a chapter's TOC page."""
        url = f"{BASE_URL}/index.cfm?App_mode=Display_Statute&URL={range_dir}/{chap_padded}/{chap_padded}ContentsIndex.html&StatuteYear={STATUTE_YEAR}"
        html = self._get(url)
        if not html:
            return []

        section_files = []
        for m in re.finditer(r'Sections/(\d{4}\.\d+[a-z]?\d*\.html)', html):
            fname = m.group(1)
            if fname not in section_files:
                section_files.append(fname)

        return section_files

    def fetch_section(self, range_dir: str, chap_padded: str, section_file: str) -> dict:
        """Fetch a single section page and extract text."""
        url = f"{BASE_URL}/index.cfm?App_mode=Display_Statute&Search_String=&URL={range_dir}/{chap_padded}/Sections/{section_file}"
        html = self._get(url)

        if "cannot be found" in html.lower():
            return None

        # Extract section number from filename (e.g., "0775.082.html" -> "775.082")
        sec_match = re.match(r'0*(\d+\.\d+[a-z]?\d*)\.html', section_file)
        if not sec_match:
            return None
        sec_num = sec_match.group(1)

        # Extract the statute content area
        # The content is in structured spans: SectionNumber, Catchline, SectionBody
        # Find the main content div
        content_start = html.find('class="Section"')
        if content_start == -1:
            content_start = html.find('class="SectionNumber"')
        if content_start == -1:
            content_start = html.find('SectionBody')

        if content_start == -1:
            # Fallback: extract everything between content markers
            return None

        # Go back to find the opening tag
        tag_start = html.rfind('<', 0, content_start)
        if tag_start == -1:
            tag_start = content_start

        # Find the end: look for History section or end of content area
        content_html = html[tag_start:]

        # Extract catchline (title)
        catchline_match = re.search(r'class="Catchline"[^>]*>(.*?)</span>', content_html, re.DOTALL)
        catchline = strip_html(catchline_match.group(1)) if catchline_match else ""

        # Extract the section body text
        # The body is in spans/divs with class="SectionBody" and sub-elements
        # Best approach: grab everything from section start to History section
        history_pos = content_html.find('class="History"')
        if history_pos == -1:
            history_pos = content_html.find('History.&#x2014;')
        if history_pos == -1:
            history_pos = content_html.find('History.')

        if history_pos > 0:
            body_html = content_html[:history_pos]
        else:
            body_html = content_html

        text = strip_html(body_html)

        if not text or len(text) < 10:
            return None

        title = f"Florida Statutes § {sec_num}"
        if catchline:
            title = f"Florida Statutes § {sec_num} — {catchline}"

        ch_num = chap_padded.lstrip("0") or "0"

        return {
            "section_num": sec_num,
            "chapter": ch_num,
            "title": title,
            "text": text,
            "section_file": section_file,
            "range_dir": range_dir,
            "chap_padded": chap_padded,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return {
            "_id": raw["section_num"],
            "_source": "US/FL-Statutes",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw["title"],
            "text": raw["text"],
            "date": today,
            "url": f"https://www.leg.state.fl.us/statutes/index.cfm?App_mode=Display_Statute&Search_String=&URL={raw['range_dir']}/{raw['chap_padded']}/Sections/{raw['section_file']}",
            "chapter": raw["chapter"],
            "section_num": raw["section_num"],
        }

    def process_chapter(self, chapter_info: dict) -> Generator[dict, None, None]:
        """Fetch all sections from one chapter."""
        range_dir = chapter_info["range"]
        chap_padded = chapter_info["padded"]
        try:
            section_files = self.get_section_files(range_dir, chap_padded)
            if not section_files:
                logger.debug(f"  Chapter {chap_padded}: no sections found in TOC")
                return

            count = 0
            for sf in section_files:
                raw = self.fetch_section(range_dir, chap_padded, sf)
                if raw:
                    yield raw
                    count += 1

            logger.info(f"  Chapter {chap_padded}: {count}/{len(section_files)} sections")
        except Exception as e:
            logger.warning(f"  Chapter {chap_padded}: failed ({e})")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all sections across all Florida Statute chapters."""
        chapters = self.discover_chapters()
        logger.info(f"Processing {len(chapters)} chapters...")
        total = 0
        for ch in chapters:
            for record in self.process_chapter(ch):
                yield record
                total += 1
                if total % 500 == 0:
                    logger.info(f"  Progress: {total} sections fetched")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample from a few chapters."""
        logger.info(f"Fetching sample from {len(SAMPLE_CHAPTERS)} chapters...")
        count = 0
        for ch in SAMPLE_CHAPTERS:
            for record in self.process_chapter(ch):
                yield record
                count += 1
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FL-Statutes bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FLStatutesScraper()

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
        try:
            for raw in gen:
                record = scraper.normalize(raw)
                out_path = sample_dir / f"{record['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count <= 20 or count % 100 == 0:
                    logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")
        except (KeyboardInterrupt, SystemExit):
            logger.warning(f"Interrupted after {count} records")
        except Exception as e:
            logger.error(f"Crashed after {count} records: {e}")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        sys.exit(0)  # partial data is still valid


if __name__ == "__main__":
    main()
