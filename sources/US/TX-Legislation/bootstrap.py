#!/usr/bin/env python3
"""
US/TX-Legislation -- Texas Statutes (All Codes)

Fetches all 31 Texas Codes with full text from the official Texas Capitol
Statute Server (tcss.legis.texas.gov) via HTML ZIP bulk downloads.

Strategy:
  1. Fetch code listing from StatuteCodeDownloads.json
  2. For each code, download the HTML ZIP archive
  3. Extract chapter HTML files from the ZIP
  4. Parse each chapter into individual sections using <a name="X.XX"> anchors
  5. Normalize into standard schema

Data: Public domain (Texas government works). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all 31 codes)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import zipfile
import io
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-Legislation")

CODES_JSON_URL = "https://statutes.capitol.texas.gov/assets/StatuteCodeDownloads.json"
TCSS_BASE = "https://tcss.legis.texas.gov/resources"

# Sample: specific codes + chapters for quick testing
SAMPLE_CODES = ["PE", "GV", "FA"]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class TXLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "*/*",
            },
            timeout=120,
        )
        self.delay = 2.0

    def _get(self, url: str, binary: bool = False):
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        if binary:
            return resp.content
        return resp.text

    def test_api(self):
        """Test connectivity to Texas statute server."""
        logger.info("Testing Texas Capitol Statute Server...")
        try:
            text = self._get(CODES_JSON_URL)
            data = json.loads(text)
            codes = data.get("StatuteCode", [])
            logger.info(f"  Code listing: {len(codes)} codes found")

            # Test a single chapter download
            url = f"{TCSS_BASE}/PE/htm/PE.1.htm"
            html = self._get(url)
            if "Sec." in html and "PENAL CODE" in html:
                logger.info("  Chapter HTML: OK (Penal Code Ch 1)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: unexpected chapter content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def get_code_list(self) -> list:
        """Fetch the list of all Texas codes."""
        text = self._get(CODES_JSON_URL)
        data = json.loads(text)
        return data.get("StatuteCode", [])

    def download_code_zip(self, code: str) -> Optional[zipfile.ZipFile]:
        """Download and return the HTML ZIP for a code."""
        url = f"{TCSS_BASE}/Zips/{code}.htm.zip"
        try:
            data = self._get(url, binary=True)
            return zipfile.ZipFile(io.BytesIO(data))
        except Exception as e:
            logger.warning(f"Failed to download ZIP for {code}: {e}")
            return None

    def parse_chapter_sections(self, html: str, code: str, code_name: str,
                                chapter: str) -> list:
        """Parse individual sections from a chapter HTML file.

        Structure: each section starts with a bold link containing "Sec. X.XX."
        and runs until the next such link or end of body.
        """
        sections = []

        title_match = re.search(r'<title>(.*?)</title>', html, re.IGNORECASE)
        chapter_title = strip_html(title_match.group(1)) if title_match else f"{code_name} Chapter {chapter}"

        # Find all section starts via the bold "Sec. X.XX." links
        # Pattern: <a ...font-weight:bold;">Sec. 1.01.  SHORT TITLE.</a>
        sec_starts = list(re.finditer(
            r'font-weight:\s*bold[^>]*>Sec\.\s+(\d+[A-Za-z]?\.\d+[a-z]?)\.',
            html
        ))

        if not sec_starts:
            return sections

        for i, m in enumerate(sec_starts):
            sec_num = m.group(1)
            # Find the beginning of this section's block — go back to find
            # the <p class="left"><a name= that precedes this Sec.
            block_start = html.rfind('<p class="left"><a name=', 0, m.start())
            if block_start == -1:
                block_start = m.start()

            # End is start of next section's block, or end of body
            if i + 1 < len(sec_starts):
                next_block = html.rfind('<p class="left"><a name=', 0, sec_starts[i + 1].start())
                block_end = next_block if next_block > block_start else sec_starts[i + 1].start()
            else:
                block_end = html.find('</body>', m.start())
                if block_end == -1:
                    block_end = len(html)

            raw_html = html[block_start:block_end]
            text = strip_html(raw_html)

            if text and len(text) > 20:
                sections.append({
                    "code": code,
                    "code_name": code_name,
                    "chapter": chapter,
                    "chapter_title": chapter_title,
                    "section_num": sec_num,
                    "text": text,
                })

        return sections

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        section_id = f"{raw['code']}-{raw['section_num']}"
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        # Build a clean title
        sec_title = f"{raw['code_name']} § {raw['section_num']}"

        return {
            "_id": section_id,
            "_source": "US/TX-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": sec_title,
            "text": raw["text"],
            "date": today,
            "url": f"https://statutes.capitol.texas.gov/Docs/{raw['code']}/htm/{raw['code']}.{raw['chapter']}.htm#{raw['section_num']}",
            "code": raw["code"],
            "code_name": raw["code_name"],
            "chapter": raw["chapter"],
            "chapter_title": raw.get("chapter_title", ""),
            "section_num": raw["section_num"],
        }

    def process_code(self, code_info: dict, max_chapters: int = 0) -> Generator[dict, None, None]:
        """Process a single code: download ZIP, parse all chapters."""
        code = code_info["code"]
        code_name = code_info["CodeName"]
        logger.info(f"Processing {code} ({code_name})...")

        zf = self.download_code_zip(code)
        if not zf:
            return

        chapter_files = sorted(zf.namelist())
        if max_chapters > 0:
            chapter_files = chapter_files[:max_chapters]

        total_sections = 0
        for filename in chapter_files:
            # Extract chapter number from filename like "pe.1.htm"
            ch_match = re.match(r'[a-z]+\.(.+)\.htm$', filename, re.IGNORECASE)
            if not ch_match:
                continue

            chapter = ch_match.group(1)
            # Skip "_old" suffix files (superseded versions)
            if chapter.endswith("_old"):
                continue

            try:
                html = zf.read(filename).decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"  Failed to read {filename}: {e}")
                continue

            sections = self.parse_chapter_sections(html, code, code_name, chapter)
            for sec in sections:
                yield self.normalize(sec)
                total_sections += 1

        logger.info(f"  {code}: {total_sections} sections extracted")
        zf.close()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all sections across all Texas Codes."""
        codes = self.get_code_list()
        logger.info(f"Found {len(codes)} Texas codes")
        total = 0
        for code_info in codes:
            for record in self.process_code(code_info):
                yield record
                total += 1
                if total % 500 == 0:
                    logger.info(f"  Progress: {total} sections fetched")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample: 3 codes, first 2 chapters each."""
        codes = self.get_code_list()
        sample_codes = [c for c in codes if c["code"] in SAMPLE_CODES]
        logger.info(f"Fetching sample from {len(sample_codes)} codes...")
        count = 0
        for code_info in sample_codes:
            for record in self.process_code(code_info, max_chapters=2):
                yield record
                count += 1
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TX-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = TXLegislationScraper()

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
