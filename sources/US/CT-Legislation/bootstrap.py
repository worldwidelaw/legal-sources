#!/usr/bin/env python3
"""
US/CT-Legislation -- Connecticut General Statutes (cga.ct.gov)

Fetches all Connecticut General Statutes with full text from the official
General Assembly website.

Strategy:
  1. Scrape titles.htm to discover all title page URLs
  2. For each title page, extract chapter/article URLs (chap_*.htm, art_*.htm)
  3. For each chapter page, parse individual sections using span.catchln anchors
  4. Normalize into standard schema

Data: Public domain (Connecticut government works). No auth required.
Note: SSL verification disabled due to server certificate chain issue.

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
logger = logging.getLogger("legal-data-hunter.US.CT-Legislation")

BASE_URL = "https://www.cga.ct.gov/current/pub"

# Sample chapters for --sample mode
SAMPLE_CHAPTERS = ["chap_001.htm", "chap_248.htm", "chap_952.htm"]

# Annotation CSS classes that mark end of section body text
ANNOTATION_CLASSES = {
    "source-first", "source", "history-first", "history",
    "annotation-first", "annotation", "cross-ref-first", "cross-ref",
    "front-note-first", "front-note",
}


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


def is_annotation_paragraph(p_html: str) -> bool:
    """Check if a <p> tag has an annotation class."""
    class_match = re.search(r'class="([^"]*)"', p_html[:200])
    if not class_match:
        return False
    classes = set(class_match.group(1).split())
    return bool(classes & ANNOTATION_CLASSES)


class CTLegislationScraper(BaseScraper):

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
            verify=False,
        )
        self.delay = 2.0

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def test_api(self):
        """Test connectivity to CT statutes site."""
        logger.info("Testing CT General Assembly statutes site...")
        try:
            html = self._get(f"{BASE_URL}/titles.htm")
            if "title_01.htm" in html:
                logger.info("  Titles page: OK")
            else:
                logger.error("  Titles page: unexpected content")
                return False

            html = self._get(f"{BASE_URL}/title_01.htm")
            if "chap_001.htm" in html:
                logger.info("  Title 1 page: OK")
            else:
                logger.error("  Title 1 page: unexpected content")
                return False

            html = self._get(f"{BASE_URL}/chap_001.htm")
            if 'id="sec_1-1"' in html:
                logger.info("  Chapter 1 page: OK (found sec_1-1)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Chapter 1 page: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_chapters(self) -> list:
        """Discover all chapter/article page URLs by walking titles."""
        logger.info("Discovering chapters from titles page...")
        html = self._get(f"{BASE_URL}/titles.htm")

        # Extract unique title page filenames
        title_files = []
        for m in re.finditer(r'href="(title_[^"]+\.htm)"', html):
            fname = m.group(1)
            if fname not in title_files:
                title_files.append(fname)

        logger.info(f"Found {len(title_files)} title pages")

        chapter_files = []
        seen = set()
        for tf in title_files:
            try:
                title_html = self._get(f"{BASE_URL}/{tf}")
                # Extract chap_*.htm and art_*.htm links
                for m in re.finditer(r'href="((chap|art)_[^"]+\.htm)"', title_html):
                    cf = m.group(1)
                    if cf not in seen:
                        seen.add(cf)
                        chapter_files.append(cf)
                logger.info(f"  {tf}: total chapters so far: {len(chapter_files)}")
            except Exception as e:
                logger.warning(f"  {tf}: failed ({e})")

        logger.info(f"Discovered {len(chapter_files)} chapter/article pages total")
        return chapter_files

    def parse_chapter(self, chapter_file: str) -> list:
        """Fetch a chapter page and parse individual sections.

        Returns list of raw section dicts.
        """
        url = f"{BASE_URL}/{chapter_file}"
        html = self._get(url)

        # Extract chapter name from heading
        chap_name = ""
        name_match = re.search(r'<h2[^>]*class="chap-name"[^>]*>(.*?)</h2>', html, re.DOTALL)
        if name_match:
            chap_name = strip_html(name_match.group(1))

        chap_no = ""
        no_match = re.search(r'<h2[^>]*class="chap-no"[^>]*>(.*?)</h2>', html, re.DOTALL)
        if no_match:
            chap_no = strip_html(no_match.group(1))

        # Split HTML into paragraphs for processing
        # Find all section anchors: <span class="catchln" id="sec_X-Y">
        section_pattern = re.compile(
            r'<span\s+class="catchln"\s+id="(sec_[^"]+)">(.*?)</span>',
            re.DOTALL,
        )

        sections = []
        matches = list(section_pattern.finditer(html))

        for i, match in enumerate(matches):
            sec_id_attr = match.group(1)  # e.g., "sec_1-1"
            catchln_text = strip_html(match.group(2))  # e.g., "Sec. 1-1. Words and phrases."

            # Extract section number from the catchline text
            sec_num_match = re.match(r'Sec\.\s+([\d\w.-]+)\.?\s*(.*)', catchln_text)
            if sec_num_match:
                sec_num = sec_num_match.group(1).rstrip('.')
                sec_title = sec_num_match.group(2).rstrip('.')
            else:
                # Fallback: use the id attribute
                sec_num = sec_id_attr.replace("sec_", "").replace("secs_", "")
                sec_title = catchln_text

            # Skip "reserved" sections
            if "reserved" in catchln_text.lower() and "future use" in catchln_text.lower():
                continue

            # Determine the text boundary: from after the catchln span
            # to the start of the next section's catchln, or end of content
            start_pos = match.end()
            if i + 1 < len(matches):
                # Find the <p> tag that contains the next section's catchln
                next_match_pos = matches[i + 1].start()
                # Go back to find the opening <p> tag of the next section
                p_start = html.rfind('<p', 0, next_match_pos)
                if p_start > start_pos:
                    end_pos = p_start
                else:
                    end_pos = next_match_pos
            else:
                end_pos = len(html)

            body_html = html[start_pos:end_pos]

            # Remove annotation paragraphs (source, history, annotation, cross-ref)
            # Split into <p>...</p> blocks and filter
            clean_parts = []
            # Process the body: first part before any <p> tag (continuation of catchln paragraph)
            first_p_pos = body_html.find('<p')
            if first_p_pos == -1:
                # No paragraph tags, use the whole body
                clean_parts.append(body_html)
            else:
                # Text before first <p> is continuation of the catchln's <p>
                clean_parts.append(body_html[:first_p_pos])

                # Process subsequent paragraphs
                p_blocks = re.finditer(r'<p[^>]*>.*?</p>', body_html[first_p_pos:], re.DOTALL)
                for pb in p_blocks:
                    p_text = pb.group(0)
                    if not is_annotation_paragraph(p_text):
                        clean_parts.append(p_text)

            text = strip_html('\n'.join(clean_parts))

            if not text or len(text) < 5:
                continue

            full_title = f"Connecticut General Statutes § {sec_num}"
            if sec_title:
                full_title = f"Connecticut General Statutes § {sec_num} — {sec_title}"

            sections.append({
                "section_num": sec_num,
                "section_title": sec_title,
                "title": full_title,
                "text": text,
                "chapter_file": chapter_file,
                "chapter_no": chap_no,
                "chapter_name": chap_name,
            })

        return sections

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return {
            "_id": raw["section_num"],
            "_source": "US/CT-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw["title"],
            "text": raw["text"],
            "date": today,
            "url": f"{BASE_URL}/{raw['chapter_file']}#sec_{raw['section_num']}",
            "chapter": raw["chapter_no"],
            "chapter_name": raw["chapter_name"],
            "section_num": raw["section_num"],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all sections across all CT statute chapters."""
        chapter_files = self.discover_chapters()
        logger.info(f"Processing {len(chapter_files)} chapters...")
        total = 0
        for cf in chapter_files:
            try:
                sections = self.parse_chapter(cf)
                for raw in sections:
                    yield raw
                    total += 1
                if sections:
                    logger.info(f"  {cf}: {len(sections)} sections (total: {total})")
            except Exception as e:
                logger.warning(f"  {cf}: failed ({e})")
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
        for cf in SAMPLE_CHAPTERS:
            try:
                sections = self.parse_chapter(cf)
                for raw in sections:
                    yield raw
                    count += 1
                logger.info(f"  {cf}: {len(sections)} sections")
            except Exception as e:
                logger.warning(f"  {cf}: failed ({e})")
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CT-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CTLegislationScraper()

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
