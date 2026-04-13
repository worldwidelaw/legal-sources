#!/usr/bin/env python3
"""
US/MT-Legislation -- Montana Code Annotated (MCA)

Fetches all sections of the Montana Code Annotated with full text from the
official Montana Legislature website (mca.legmt.gov).

Strategy:
  1. Fetch the TOC at /index.html to get all title links
  2. For each title, fetch chapters_index.html
  3. For each chapter, fetch parts_index.html
  4. For each part, fetch sections_index.html to list individual sections
  5. For each section, fetch the HTML page and extract full text

Data: Public domain (Montana government works). No auth required.
Rate limit: 1 req / 2 sec.

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
logger = logging.getLogger("legal-data-hunter.US.MT-Legislation")

BASE_URL = "https://mca.legmt.gov/bills/mca"

# Sample sections: well-known MT statutes for quick testing
SAMPLE_SECTIONS = [
    # Title 1 - General Laws
    ("title_0010/chapter_0010/part_0010/section_0010/0010-0010-0010-0010.html", "1-1-101"),
    ("title_0010/chapter_0010/part_0010/section_0020/0010-0010-0010-0020.html", "1-1-102"),
    ("title_0010/chapter_0010/part_0010/section_0030/0010-0010-0010-0030.html", "1-1-103"),
    # Title 45 - Crimes
    ("title_0450/chapter_0050/part_0010/section_0020/0450-0050-0010-0020.html", "45-5-102"),
    ("title_0450/chapter_0050/part_0010/section_0030/0450-0050-0010-0030.html", "45-5-103"),
    ("title_0450/chapter_0020/part_0010/section_0010/0450-0020-0010-0010.html", "45-2-101"),
    # Title 70 - Property
    ("title_0700/chapter_0010/part_0010/section_0010/0700-0010-0010-0010.html", "70-1-101"),
    # Title 27 - Civil Liability
    ("title_0270/chapter_0010/part_0010/section_0010/0270-0010-0010-0010.html", "27-1-101"),
    # Title 30 - Trade and Commerce (UCC)
    ("title_0300/chapter_0010/part_0010/section_0010/0300-0010-0010-0010.html", "30-1-101"),
    # Title 15 - Taxation
    ("title_0150/chapter_0010/part_0010/section_0010/0150-0010-0010-0010.html", "15-1-101"),
    # Title 39 - Labor
    ("title_0390/chapter_0010/part_0010/section_0010/0390-0010-0010-0010.html", "39-1-101"),
    # Title 40 - Family Law
    ("title_0400/chapter_0010/part_0010/section_0010/0400-0010-0010-0010.html", "40-1-101"),
    # Title 46 - Criminal Procedure
    ("title_0460/chapter_0010/part_0010/section_0010/0460-0010-0010-0010.html", "46-1-101"),
    # Title 50 - Health and Safety
    ("title_0500/chapter_0010/part_0010/section_0010/0500-0010-0010-0010.html", "50-1-101"),
    # Title 61 - Motor Vehicles
    ("title_0610/chapter_0010/part_0010/section_0010/0610-0010-0010-0010.html", "61-1-101"),
]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<h[1-6][^>]*>', '\n## ', text)
    text = re.sub(r'</h[1-6]>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class MTLegislationScraper(BaseScraper):

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
        self.delay = 2.0

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting, return HTML string."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def test_api(self):
        """Test connectivity to mca.legmt.gov."""
        logger.info("Testing Montana MCA website...")
        try:
            url = f"{BASE_URL}/title_0010/chapter_0010/part_0010/section_0010/0010-0010-0010-0010.html"
            html = self._get(url)
            if "section-content" in html:
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

    def parse_section_page(self, html: str, url: str) -> Optional[dict]:
        """Parse a section HTML page and extract structured data."""
        # Extract section citation from <span class="citation">
        citation_match = re.search(
            r'<span class="catchline"><span class="citation">([^<]+)</span>',
            html,
        )
        if not citation_match:
            return None
        section_id = citation_match.group(1).strip()

        # Extract catchline (section title)
        catchline_match = re.search(
            r'<span class="catchline">.*?</span>\s*\.?\s*&#8195;([^<]+?)\.?</span>',
            html,
        )
        catchline = ""
        if catchline_match:
            catchline = html_module.unescape(catchline_match.group(1)).strip().rstrip(".")

        # Extract section content (full text)
        content_match = re.search(
            r'<div class="section-content">(.*?)</div>\s*</div>',
            html,
            re.DOTALL,
        )
        if not content_match:
            return None
        content_html = content_match.group(1)
        text = strip_html(content_html)

        if not text or len(text) < 10:
            return None

        # Extract history
        history_match = re.search(
            r'<div class="history-content">(.*?)</div>',
            html,
            re.DOTALL,
        )
        history = ""
        if history_match:
            history = strip_html(history_match.group(1))

        # Extract title/chapter/part from breadcrumb
        title_match = re.search(
            r'class="section-title-title">\s*(.*?)\s*</h4>',
            html,
            re.DOTALL,
        )
        mca_title = strip_html(title_match.group(1)) if title_match else ""

        chapter_match = re.search(
            r'class="section-chapter-title">\s*(.*?)\s*</h3>',
            html,
            re.DOTALL,
        )
        chapter = strip_html(chapter_match.group(1)) if chapter_match else ""

        part_match = re.search(
            r'class="section-part-title">\s*(.*?)\s*</h2>',
            html,
            re.DOTALL,
        )
        part = strip_html(part_match.group(1)) if part_match else ""

        return {
            "section_id": section_id,
            "catchline": catchline,
            "text": text,
            "history": history,
            "mca_title": mca_title,
            "chapter": chapter,
            "part": part,
            "url": url,
        }

    def get_title_links(self) -> list:
        """Get all title links from the MCA TOC page."""
        url = f"{BASE_URL}/index.html"
        html = self._get(url)
        # Pattern: ./title_XXXX/chapters_index.html
        matches = re.findall(
            r'href="\./(title_\d{4})/chapters_index\.html"[^>]*>([^<]+)',
            html,
        )
        titles = []
        seen = set()
        for path, name in matches:
            if path not in seen:
                seen.add(path)
                titles.append({"path": path, "name": name.strip()})
        logger.info(f"Found {len(titles)} titles")
        return titles

    def get_chapter_links(self, title_path: str) -> list:
        """Get chapter links for a title."""
        url = f"{BASE_URL}/{title_path}/chapters_index.html"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch chapters for {title_path}: {e}")
            return []
        matches = re.findall(
            r'href="\./?(chapter_[^/]+)/parts_index\.html"',
            html,
        )
        return list(dict.fromkeys(matches))

    def get_part_links(self, title_path: str, chapter_path: str) -> list:
        """Get part links for a chapter."""
        url = f"{BASE_URL}/{title_path}/{chapter_path}/parts_index.html"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch parts for {title_path}/{chapter_path}: {e}")
            return []

        # Some chapters have parts, some have sections directly
        part_matches = re.findall(
            r'href="\./?(part_[^/]+)/sections_index\.html"',
            html,
        )
        if part_matches:
            return list(dict.fromkeys(part_matches))

        # If no parts, check for direct section links on the parts_index page
        section_matches = re.findall(
            r'href="\./?([^"]+\.html)"',
            html,
        )
        # Filter for section files (4-digit pattern)
        section_files = [
            m for m in section_matches
            if re.match(r'(section_\d+/)?[\d]+-[\d]+-[\d]+-[\d]+\.html', m)
        ]
        if section_files:
            # Return a special marker indicating these are direct section links
            return [("__direct__", section_files)]

        return []

    def get_section_links(self, title_path: str, chapter_path: str, part_path: str) -> list:
        """Get section links for a part."""
        url = f"{BASE_URL}/{title_path}/{chapter_path}/{part_path}/sections_index.html"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch sections: {e}")
            return []
        # Match section links like ./section_0010/0010-0010-0010-0010.html
        matches = re.findall(
            r'href="\./?([^"]*\d+-\d+-\d+-\d+\.html)"',
            html,
        )
        return list(dict.fromkeys(matches))

    def fetch_section(self, relative_path: str) -> Optional[dict]:
        """Fetch and parse a single section by its relative path from BASE_URL."""
        url = f"{BASE_URL}/{relative_path}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch section {relative_path}: {e}")
            return None
        return self.parse_section_page(html, url)

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        full_text = raw["text"]
        if raw.get("history"):
            full_text += f"\n\nHistory: {raw['history']}"

        title_str = f"MCA § {raw['section_id']}"
        if raw.get("catchline"):
            title_str += f" - {raw['catchline']}"

        return {
            "_id": raw["section_id"],
            "_source": "US/MT-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title_str,
            "text": full_text,
            "date": today,
            "url": raw["url"],
            "section_id": raw["section_id"],
            "catchline": raw.get("catchline", ""),
            "mca_title": raw.get("mca_title", ""),
            "chapter": raw.get("chapter", ""),
            "part": raw.get("part", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all MCA sections across all titles."""
        total = 0
        titles = self.get_title_links()

        for title_info in titles:
            title_path = title_info["path"]
            logger.info(f"Processing {title_info['name']}...")

            chapters = self.get_chapter_links(title_path)
            for chapter in chapters:
                parts = self.get_part_links(title_path, chapter)

                for part_item in parts:
                    # Handle direct section links (chapters without parts)
                    if isinstance(part_item, tuple) and part_item[0] == "__direct__":
                        section_files = part_item[1]
                        for sec_file in section_files:
                            full_path = f"{title_path}/{chapter}/{sec_file}"
                            raw = self.fetch_section(full_path)
                            if raw:
                                yield self.normalize(raw)
                                total += 1
                                if total % 100 == 0:
                                    logger.info(f"  Progress: {total} sections fetched")
                        continue

                    sections = self.get_section_links(title_path, chapter, part_item)
                    for sec_path in sections:
                        full_path = f"{title_path}/{chapter}/{part_item}/{sec_path}"
                        raw = self.fetch_section(full_path)
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
        """Fetch a small sample of well-known MCA sections."""
        logger.info(f"Fetching {len(SAMPLE_SECTIONS)} sample sections...")
        count = 0
        for rel_path, section_id in SAMPLE_SECTIONS:
            raw = self.fetch_section(rel_path)
            if raw:
                yield self.normalize(raw)
                count += 1
            else:
                logger.warning(f"Could not fetch sample section {section_id}")
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MT-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = MTLegislationScraper()

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
