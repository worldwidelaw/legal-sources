#!/usr/bin/env python3
"""
US/WI-Legislation -- Wisconsin Statutes (docs.legis.wisconsin.gov)

Fetches all Wisconsin Statutes with full text from the official
Legislature documentation portal.

Strategy:
  1. Scrape the TOC page to discover all chapter numbers
  2. For each chapter, fetch the HTML page
  3. Parse individual sections from the chapter HTML
  4. Normalize into standard schema

Data: Public domain (Wisconsin government works). No auth required.

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
logger = logging.getLogger("legal-data-hunter.US.WI-Legislation")

BASE_URL = "https://docs.legis.wisconsin.gov"
TOC_URL = f"{BASE_URL}/statutes/prefaces/toc"
CHAPTER_URL = f"{BASE_URL}/statutes/statutes"

# Sample chapters for --sample mode (common chapters likely to have content)
SAMPLE_CHAPTERS = ["1", "19", "48", "100", "346", "939", "990"]


def strip_html(text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'<[^>]*$', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class WILegislationScraper(BaseScraper):

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
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def test_api(self):
        """Test connectivity to Wisconsin statutes site."""
        logger.info("Testing Wisconsin Legislature statutes site...")
        try:
            html = self._get(TOC_URL)
            if "chapter" in html.lower() or "statutes" in html.lower():
                logger.info("  TOC page: OK")
            else:
                logger.error("  TOC page: unexpected content")
                return False

            html = self._get(f"{CHAPTER_URL}/1")
            if html and len(html) > 500:
                logger.info("  Chapter 1 page: OK")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Chapter 1 page: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_chapters(self) -> list:
        """Discover all chapter numbers from the TOC page."""
        logger.info("Discovering chapters from TOC page...")
        html = self._get(TOC_URL)

        # Look for chapter links in the TOC page
        # Typical patterns: /statutes/statutes/1, /statutes/statutes/19, etc.
        chapter_nums = []
        seen = set()

        # Pattern 1: links to /statutes/statutes/{number}
        for m in re.finditer(r'href="[^"]*?/statutes/statutes/(\d+(?:\.\d+)?)"', html):
            ch = m.group(1)
            if ch not in seen:
                seen.add(ch)
                chapter_nums.append(ch)

        # Pattern 2: relative links like statutes/{number}
        if not chapter_nums:
            for m in re.finditer(r'href="(?:statutes/)?(\d+(?:\.\d+)?)"', html):
                ch = m.group(1)
                if ch not in seen:
                    seen.add(ch)
                    chapter_nums.append(ch)

        # Pattern 3: look for chapter numbers in text like "Chapter 1" or "Ch. 1"
        if not chapter_nums:
            for m in re.finditer(r'(?:Chapter|Ch\.?)\s+(\d+(?:\.\d+)?)', html):
                ch = m.group(1)
                if ch not in seen:
                    seen.add(ch)
                    chapter_nums.append(ch)

        # Sort numerically
        chapter_nums.sort(key=lambda x: float(x) if '.' in x else int(x))

        logger.info(f"Discovered {len(chapter_nums)} chapters")
        return chapter_nums

    def parse_chapter(self, chapter_num: str) -> list:
        """Fetch a chapter page and parse individual sections.

        Returns list of raw section dicts.
        """
        url = f"{CHAPTER_URL}/{chapter_num}"
        html = self._get(url)

        # Extract chapter title from page
        chapter_title = ""
        # Try various heading patterns
        for pattern in [
            r'<h[12][^>]*>\s*Chapter\s+\d+[^<]*?[-—:]\s*(.*?)\s*</h[12]>',
            r'<h[12][^>]*>(.*?)</h[12]>',
            r'<title>(.*?)</title>',
        ]:
            m = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if m:
                chapter_title = strip_html(m.group(1)).strip()
                if chapter_title:
                    break

        sections = []

        # Strategy 1: Look for section anchors/headings with common patterns
        # Wisconsin statutes use section numbers like "1.01", "19.31", etc.
        # Common HTML patterns for statute sections:

        # Pattern A: Section headings with id attributes
        sec_pattern = re.compile(
            r'<(?:h[2-6]|p|div|span)[^>]*(?:id|name)="(?:sec[_-]?)?'
            + re.escape(chapter_num)
            + r'[._](\d+(?:[a-z])?)"[^>]*>(.*?)</(?:h[2-6]|p|div|span)>',
            re.DOTALL | re.IGNORECASE,
        )

        # Pattern B: Bold section numbers like "1.01" at start of content
        sec_pattern_b = re.compile(
            r'<(?:b|strong|h[3-6])[^>]*>\s*'
            + re.escape(chapter_num)
            + r'\.(\d+(?:[a-z])?)\s*[^<]*</(?:b|strong|h[3-6])>',
            re.DOTALL | re.IGNORECASE,
        )

        # Pattern C: Generic section pattern with section number in text
        sec_pattern_c = re.compile(
            r'(?:<[^>]+>)*\s*(?:§\s*)?'
            + re.escape(chapter_num)
            + r'\.(\d+(?:[a-z])?)\b\s*(.*?)(?=(?:§\s*)?'
            + re.escape(chapter_num)
            + r'\.\d|$)',
            re.DOTALL,
        )

        # Try to find section boundaries
        # First, look for distinct section blocks
        # Wisconsin docs often use <div> or heading-based section structures

        # Try finding sections by looking for the section number pattern in the HTML
        sec_num_pattern = re.compile(
            r'(?:<[^>]*>)*\s*(?:<b>|<strong>|<h\d[^>]*>)?\s*'
            r'(?:§\s*)?(\d+\.\d+(?:[a-z])?)\s*'
            r'(.*?)'
            r'(?=(?:<[^>]*>)*\s*(?:<b>|<strong>|<h\d[^>]*>)?\s*(?:§\s*)?\d+\.\d+[a-z]?\s|</?(?:footer|nav)\b|$)',
            re.DOTALL,
        )

        matches = list(sec_num_pattern.finditer(html))

        if not matches:
            # Fallback: try to extract the whole chapter as a single record
            body_text = strip_html(html)
            if body_text and len(body_text) > 100:
                sections.append({
                    "section_num": chapter_num,
                    "section_title": chapter_title,
                    "title": f"Wisconsin Statutes Chapter {chapter_num}",
                    "text": body_text,
                    "chapter_num": chapter_num,
                    "chapter_title": chapter_title,
                })
            return sections

        for i, match in enumerate(matches):
            sec_num = match.group(1)  # e.g., "1.01"

            # Get the content between this section and the next
            start = match.start()
            if i + 1 < len(matches):
                end = matches[i + 1].start()
            else:
                end = len(html)

            section_html = html[start:end]
            text = strip_html(section_html)

            if not text or len(text) < 10:
                continue

            # Extract section title from the first line
            lines = text.split('\n', 2)
            first_line = lines[0].strip()

            # Try to extract title from "1.01 Title here" pattern
            title_match = re.match(
                r'(?:§\s*)?' + re.escape(sec_num) + r'\s+(.+?)(?:\.\s|$)',
                first_line,
            )
            sec_title = title_match.group(1).strip() if title_match else first_line

            full_title = f"Wisconsin Statutes § {sec_num}"
            if sec_title and sec_title != sec_num:
                full_title = f"Wisconsin Statutes § {sec_num} — {sec_title}"

            sections.append({
                "section_num": sec_num,
                "section_title": sec_title,
                "title": full_title,
                "text": text,
                "chapter_num": chapter_num,
                "chapter_title": chapter_title,
            })

        return sections

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return {
            "_id": raw["section_num"],
            "_source": "US/WI-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw["title"],
            "text": raw["text"],
            "date": today,
            "url": f"{CHAPTER_URL}/{raw['chapter_num']}",
            "chapter": raw["chapter_num"],
            "chapter_name": raw.get("chapter_title", ""),
            "section_num": raw["section_num"],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all sections across all WI statute chapters."""
        chapter_nums = self.discover_chapters()
        logger.info(f"Processing {len(chapter_nums)} chapters...")
        total = 0
        for ch in chapter_nums:
            try:
                sections = self.parse_chapter(ch)
                for raw in sections:
                    yield raw
                    total += 1
                if sections:
                    logger.info(f"  Chapter {ch}: {len(sections)} sections (total: {total})")
            except Exception as e:
                logger.warning(f"  Chapter {ch}: failed ({e})")
            if total % 500 == 0 and total > 0:
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
            try:
                sections = self.parse_chapter(ch)
                for raw in sections:
                    yield raw
                    count += 1
                logger.info(f"  Chapter {ch}: {len(sections)} sections")
            except Exception as e:
                logger.warning(f"  Chapter {ch}: failed ({e})")
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WI-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WILegislationScraper()

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
        for raw in gen:
            record = scraper.normalize(raw)
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            if count <= 20 or count % 100 == 0:
                logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
