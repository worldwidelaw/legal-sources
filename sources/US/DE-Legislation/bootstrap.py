#!/usr/bin/env python3
"""
US/DE-Legislation -- Delaware Code (delcode.delaware.gov)

Fetches all Delaware Code sections with full text from the official
Delaware Legislature website.

Strategy:
  1. Scrape homepage to discover all title URLs (title1 through title31)
  2. For each title, discover chapter URLs (c001, c002, etc.)
  3. For each chapter, check if it has subchapter links (sc01, sc02, etc.)
     - If yes, fetch each subchapter page for sections
     - If no, parse sections directly from the chapter page
  4. Parse sections using div.SectionHead anchors and p.subsection content
  5. Normalize into standard schema

Data: Public domain (Delaware government works). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all titles)
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
logger = logging.getLogger("legal-data-hunter.US.DE-Legislation")

BASE_URL = "https://delcode.delaware.gov"

# Sample: titles 1 (General), 11 (Crimes), 30 (Banking)
SAMPLE_TITLES = ["title1", "title11", "title30"]
SAMPLE_MAX_CHAPTERS = 2


def strip_html(text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'<[^>]*$', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class DELegislationScraper(BaseScraper):

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
        """Fetch URL with rate limiting. Force UTF-8 encoding."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        resp.encoding = "utf-8"
        return resp.text

    def test_api(self):
        """Test connectivity to Delaware Code site."""
        logger.info("Testing Delaware Code Online...")
        try:
            html = self._get(f"{BASE_URL}/")
            if "title1/index.html" in html:
                logger.info("  Homepage: OK")
            else:
                logger.error("  Homepage: unexpected content")
                return False

            html = self._get(f"{BASE_URL}/title1/c001/index.html")
            if 'id="101"' in html:
                logger.info("  Title 1 Chapter 1: OK (found § 101)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Title 1 Chapter 1: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_titles(self) -> list:
        """Discover all title directory names from homepage."""
        html = self._get(f"{BASE_URL}/")
        titles = []
        seen = set()
        for m in re.finditer(r'(title\d+)/index\.html', html):
            t = m.group(1)
            if t not in seen:
                seen.add(t)
                titles.append(t)
        titles.sort(key=lambda x: int(re.search(r'\d+', x).group()))
        logger.info(f"Discovered {len(titles)} titles")
        return titles

    def discover_chapters(self, title_dir: str) -> list:
        """Discover chapter paths for a title."""
        html = self._get(f"{BASE_URL}/{title_dir}/index.html")
        chapters = []
        seen = set()
        for m in re.finditer(rf'{title_dir}/(c\d+)/index\.html', html):
            ch = m.group(1)
            if ch not in seen:
                seen.add(ch)
                chapters.append(ch)
        return chapters

    def discover_subchapters(self, title_dir: str, chap_dir: str, html: str) -> list:
        """Check if a chapter page has subchapter links. Return list or empty."""
        subs = []
        seen = set()
        for m in re.finditer(rf'{title_dir}/{chap_dir}/(sc\d+)/index\.html', html):
            sc = m.group(1)
            if sc not in seen:
                seen.add(sc)
                subs.append(sc)
        return subs

    def parse_sections_from_html(self, html: str, title_num: str, page_url: str) -> list:
        """Parse sections from a page containing SectionHead divs.

        Returns list of raw section dicts.
        """
        sections = []

        # Find all SectionHead divs
        head_pattern = re.compile(
            r'<div\s+class="SectionHead"\s+id="([^"]+)">(.*?)</div>',
            re.DOTALL,
        )
        matches = list(head_pattern.finditer(html))

        for i, match in enumerate(matches):
            sec_id_attr = match.group(1)  # e.g., "101", "501", "504-510"
            head_text = strip_html(match.group(2))  # e.g., "§ 101. Designation and citation of Code."

            # Skip reserved/repealed range sections like "504-510"
            if re.match(r'^\d+-\d+$', sec_id_attr) and "reserved" in head_text.lower():
                continue

            # Clean up head text (remove stray whitespace/newlines)
            head_text = re.sub(r'\s+', ' ', head_text).strip()

            # Parse section number and title from the head text
            sec_match = re.match(r'§\s*(\S+)\.\s*(.*)', head_text)
            if sec_match:
                sec_num = sec_match.group(1).rstrip('.')
                sec_title = sec_match.group(2).rstrip('.').strip()
            else:
                sec_num = sec_id_attr
                sec_title = head_text

            # Get body text: from end of this SectionHead div to start of next Section div
            start_pos = match.end()
            # Find the next Section div or SectionHead
            if i + 1 < len(matches):
                # Go back to find the <div class="Section"> before the next SectionHead
                next_pos = matches[i + 1].start()
                section_div = html.rfind('<div class="Section">', start_pos, next_pos)
                if section_div > start_pos:
                    end_pos = section_div
                else:
                    end_pos = next_pos
            else:
                # Last section: go until footer area
                footer_pos = html.find('<footer', start_pos)
                if footer_pos > 0:
                    end_pos = footer_pos
                else:
                    end_pos = len(html)

            body_html = html[start_pos:end_pos]

            # Extract subsection paragraphs
            body_parts = []
            for p_match in re.finditer(r'<p\s+class="subsection">(.*?)</p>', body_html, re.DOTALL):
                p_text = strip_html(p_match.group(1))
                if p_text:
                    body_parts.append(p_text)

            # If no subsection paragraphs, try getting all text from the body
            if not body_parts:
                cleaned = strip_html(body_html)
                # Remove citation/history lines at the end
                lines = cleaned.split('\n')
                text_lines = []
                for line in lines:
                    # Stop at citation lines (e.g., "1 Del. C. 1953, § 101;")
                    if re.match(r'^\d+\s+Del\.\s+[CL]', line.strip()):
                        break
                    if line.strip():
                        text_lines.append(line)
                if text_lines:
                    body_parts = text_lines

            text = "\n".join(body_parts).strip()

            if not text or len(text) < 5:
                continue

            full_title = f"Delaware Code Title {title_num}, § {sec_num}"
            if sec_title:
                full_title = f"Delaware Code Title {title_num}, § {sec_num} — {sec_title}"

            sections.append({
                "section_num": sec_num,
                "section_title": sec_title,
                "title": full_title,
                "text": text,
                "title_num": title_num,
                "url": f"{page_url}#{sec_id_attr}",
            })

        return sections

    def fetch_chapter_sections(self, title_dir: str, chap_dir: str) -> list:
        """Fetch all sections from a chapter (handling subchapters)."""
        title_num = re.search(r'\d+', title_dir).group()
        url = f"{BASE_URL}/{title_dir}/{chap_dir}/index.html"
        html = self._get(url)

        # Check for subchapters
        subs = self.discover_subchapters(title_dir, chap_dir, html)
        if subs:
            all_sections = []
            for sc in subs:
                sc_url = f"{BASE_URL}/{title_dir}/{chap_dir}/{sc}/index.html"
                try:
                    sc_html = self._get(sc_url)
                    sections = self.parse_sections_from_html(sc_html, title_num, sc_url)
                    all_sections.extend(sections)
                except Exception as e:
                    logger.warning(f"  {title_dir}/{chap_dir}/{sc}: failed ({e})")
            return all_sections
        else:
            return self.parse_sections_from_html(html, title_num, url)

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return {
            "_id": f"T{raw['title_num']}-{raw['section_num']}",
            "_source": "US/DE-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw["title"],
            "text": raw["text"],
            "date": today,
            "url": raw["url"],
            "title_num": raw["title_num"],
            "section_num": raw["section_num"],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Delaware Code sections."""
        titles = self.discover_titles()
        total = 0
        for title_dir in titles:
            chapters = self.discover_chapters(title_dir)
            logger.info(f"  {title_dir}: {len(chapters)} chapters")
            for chap_dir in chapters:
                try:
                    sections = self.fetch_chapter_sections(title_dir, chap_dir)
                    for raw in sections:
                        yield self.normalize(raw)
                        total += 1
                    if sections:
                        logger.info(f"    {title_dir}/{chap_dir}: {len(sections)} sections (total: {total})")
                except Exception as e:
                    logger.warning(f"    {title_dir}/{chap_dir}: failed ({e})")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample from a few titles."""
        logger.info(f"Fetching sample from titles {SAMPLE_TITLES}...")
        count = 0
        for title_dir in SAMPLE_TITLES:
            chapters = self.discover_chapters(title_dir)
            for chap_dir in chapters[:SAMPLE_MAX_CHAPTERS]:
                try:
                    sections = self.fetch_chapter_sections(title_dir, chap_dir)
                    for raw in sections:
                        yield self.normalize(raw)
                        count += 1
                    logger.info(f"  {title_dir}/{chap_dir}: {len(sections)} sections")
                except Exception as e:
                    logger.warning(f"  {title_dir}/{chap_dir}: failed ({e})")
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DE-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = DELegislationScraper()

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
