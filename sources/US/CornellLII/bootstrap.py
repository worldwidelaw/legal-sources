#!/usr/bin/env python3
"""
US/CornellLII -- Cornell Legal Information Institute (US Code)

Fetches full text of all 54 titles of the United States Code from Cornell LII.
LII provides clean HTML rendering of the official US Code (public domain).

Strategy:
  - List all 54 USC titles from the index page
  - For each title, discover chapters via the title page
  - For each chapter, discover sections
  - Fetch each section's full text from its HTML page
  - Clean HTML to plain text preserving structure

Data: Public domain (US government works).
Rate limit: 1 req/2 sec (self-imposed, robots.txt allows /uscode/).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample sections
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
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CornellLII")

BASE_URL = "https://www.law.cornell.edu"


def html_to_text(html_content: str) -> str:
    """Extract clean text from Cornell LII HTML page content."""
    if not html_content:
        return ""

    # Try to extract just the main content area
    # LII uses div#content or div.field-name-body for statute text
    content = html_content

    # Extract the tab-content or main-content area
    patterns = [
        r'<div[^>]*id="block-system-main"[^>]*>(.*?)</div>\s*</div>\s*</div>\s*</section',
        r'<div[^>]*class="[^"]*field-name-body[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        r'<div[^>]*id="content"[^>]*>(.*?)</div>\s*<!--\s*/#content',
    ]

    for pattern in patterns:
        match = re.search(pattern, content, re.DOTALL | re.IGNORECASE)
        if match:
            content = match.group(1)
            break

    # Preserve headings
    content = re.sub(r'<h[1-6][^>]*>(.*?)</h[1-6]>', r'\n\n## \1\n', content, flags=re.DOTALL | re.IGNORECASE)

    # Preserve paragraph breaks
    content = re.sub(r'<p[^>]*>', '\n\n', content, flags=re.IGNORECASE)
    content = re.sub(r'</p>', '', content, flags=re.IGNORECASE)

    # Preserve list items
    content = re.sub(r'<li[^>]*>', '\n  - ', content, flags=re.IGNORECASE)

    # Preserve line breaks
    content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)

    # Remove script and style blocks
    content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)

    # Remove navigation, menus, sidebars
    content = re.sub(r'<nav[^>]*>.*?</nav>', '', content, flags=re.DOTALL | re.IGNORECASE)

    # Remove all remaining HTML tags
    content = re.sub(r'<[^>]+>', '', content)

    # Decode HTML entities
    content = html_module.unescape(content)

    # Clean whitespace
    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r'\n[ \t]+', '\n', content)
    content = re.sub(r'\n{3,}', '\n\n', content)

    return content.strip()


def extract_section_text(html_content: str) -> str:
    """Extract the statutory text from a USC section page."""
    if not html_content:
        return ""

    # The main statute text on Cornell LII is in div with class containing
    # "field-name-body" or within the main content block
    # Look for the statute text specifically

    # Try to find the tab content with the actual statute
    tab_match = re.search(
        r'<div[^>]*class="[^"]*tab-pane[^"]*active[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
        html_content, re.DOTALL | re.IGNORECASE
    )
    if tab_match:
        return html_to_text(tab_match.group(1))

    # Try field-name-body
    body_match = re.search(
        r'<div[^>]*class="[^"]*field-name-body[^"]*"[^>]*>(.*?)</div>\s*</div>',
        html_content, re.DOTALL | re.IGNORECASE
    )
    if body_match:
        return html_to_text(body_match.group(1))

    # Fallback: extract from the whole page
    return html_to_text(html_content)


class CornellLIIScraper(BaseScraper):

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

    def test_api(self):
        """Test connectivity to Cornell LII."""
        logger.info("Testing Cornell LII connectivity...")
        try:
            resp = self.http.get(f"{BASE_URL}/uscode/text/1/1")
            logger.info(f"  Status: {resp.status_code}")
            if resp.status_code == 200 and "General Provisions" in resp.text or "Words denoting" in resp.text:
                logger.info("Connectivity test PASSED")
                return True
            else:
                logger.error(f"Connectivity test FAILED: unexpected response")
                return False
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def get_title_chapters(self, title_number: int) -> list:
        """Get list of chapters for a given USC title."""
        url = f"{BASE_URL}/uscode/text/{title_number}"
        try:
            resp = self.http.get(url)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for title {title_number}")
                return []
            # Find chapter links: /uscode/text/{title}/chapter-{N}
            chapters = re.findall(
                rf'/uscode/text/{title_number}/chapter-(\d+[A-Za-z]*)',
                resp.text
            )
            # Deduplicate while preserving order
            seen = set()
            unique = []
            for ch in chapters:
                if ch not in seen:
                    seen.add(ch)
                    unique.append(ch)
            return unique
        except Exception as e:
            logger.warning(f"Failed to get chapters for title {title_number}: {e}")
            return []

    def get_chapter_sections(self, title_number: int, chapter: str) -> list:
        """Get list of section numbers for a chapter."""
        url = f"{BASE_URL}/uscode/text/{title_number}/chapter-{chapter}"
        try:
            time.sleep(2)
            resp = self.http.get(url)
            if resp.status_code != 200:
                return []
            # Find section links: /uscode/text/{title}/{section}
            # Sections can have letters and dashes, e.g., 101, 101a, 2000e-2
            sections = re.findall(
                rf'href="/uscode/text/{title_number}/(\d+[a-zA-Z0-9\-]*)"',
                resp.text
            )
            # Filter out chapter/subtitle links
            sections = [s for s in sections if not s.startswith("chapter") and not s.startswith("subtitle")]
            seen = set()
            unique = []
            for s in sections:
                if s not in seen:
                    seen.add(s)
                    unique.append(s)
            return unique
        except Exception as e:
            logger.warning(f"Failed to get sections for title {title_number} ch {chapter}: {e}")
            return []

    def fetch_section(self, title_number: int, section: str) -> Optional[dict]:
        """Fetch a single USC section with full text."""
        url = f"{BASE_URL}/uscode/text/{title_number}/{section}"
        try:
            time.sleep(2)
            resp = self.http.get(url)
            if resp.status_code != 200:
                logger.warning(f"HTTP {resp.status_code} for {title_number} USC §{section}")
                return None

            text = extract_section_text(resp.text)
            if not text or len(text) < 20:
                logger.warning(f"No text extracted for {title_number} USC §{section}")
                return None

            # Try to extract the section title from the page
            title_match = re.search(
                r'<h1[^>]*>(.*?)</h1>', resp.text, re.DOTALL | re.IGNORECASE
            )
            section_title = ""
            if title_match:
                section_title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()

            if not section_title:
                title_match = re.search(
                    r'<title>(.*?)</title>', resp.text, re.DOTALL | re.IGNORECASE
                )
                if title_match:
                    section_title = title_match.group(1).strip()

            return {
                "title_number": title_number,
                "section": section,
                "section_title": section_title,
                "text": text,
                "url": url,
            }
        except Exception as e:
            logger.warning(f"Failed to fetch {title_number} USC §{section}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw section data into standard schema."""
        if not raw or not raw.get("text") or len(raw["text"]) < 20:
            return None

        title_num = raw["title_number"]
        section = raw["section"]
        citation = f"{title_num} USC §{section}"

        title = raw.get("section_title") or citation

        return {
            "_id": f"USC-{title_num}-{section}".replace(" ", "_"),
            "_source": "US/CornellLII",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "url": raw["url"],
            "usc_citation": citation,
            "title_number": title_num,
            "section_number": section,
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all USC sections with full text."""
        sample_limit = 15 if sample else None
        count = 0

        # For sample mode, fetch from a few diverse titles
        if sample:
            sample_titles = [1, 5, 18, 26, 42]  # General, Gov Org, Crimes, Tax, Public Health
        else:
            sample_titles = list(range(1, 55))  # All 54 titles

        for title_num in sample_titles:
            logger.info(f"Processing Title {title_num}...")
            time.sleep(2)
            chapters = self.get_title_chapters(title_num)
            if not chapters:
                logger.info(f"  No chapters found for title {title_num}, trying direct sections")
                # Some titles list sections directly
                chapters = ["1"]

            logger.info(f"  Found {len(chapters)} chapters")

            for chapter in chapters:
                sections = self.get_chapter_sections(title_num, chapter)
                if not sections:
                    continue

                logger.info(f"  Chapter {chapter}: {len(sections)} sections")

                for section in sections:
                    raw = self.fetch_section(title_num, section)
                    if not raw:
                        continue

                    record = self.normalize(raw)
                    if record:
                        count += 1
                        logger.info(f"  [{count}] {record['usc_citation']} — {len(record['text'])} chars")
                        yield record

                        if sample_limit and count >= sample_limit:
                            logger.info(f"Sample limit ({sample_limit}) reached")
                            return

        logger.info(f"Total sections fetched: {count}")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Incremental updates — re-fetch all (USC is updated infrequently)."""
        yield from self.fetch_all(sample=False)

    def bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = Path(self.source_dir) / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in self.fetch_all(sample=sample):
            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
            count += 1
            logger.info(f"Saved: {out_file.name}")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/CornellLII bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 sections)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CornellLIIScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = scraper.bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()
