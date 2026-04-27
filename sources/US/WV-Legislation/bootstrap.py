#!/usr/bin/env python3
"""
US/WV-Legislation -- West Virginia Code (code.wvlegislature.gov)

Fetches all West Virginia Code sections with full text from the official
Legislature WordPress site.

Strategy:
  1. Fetch sitemap.xml to discover all page URLs
  2. Filter for section-level URLs (pattern: /{chapter}-{article}-{section}/)
  3. For each section page, extract text from div.sectiontext
  4. Normalize into standard schema

Data: Public domain (West Virginia government works). No auth required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all sections)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
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
logger = logging.getLogger("legal-data-hunter.US.WV-Legislation")

BASE_URL = "https://code.wvlegislature.gov"

# Section URL pattern: /{chapter}-{article}-{section}/ (at least two hyphens)
SECTION_URL_RE = re.compile(r'^https://code\.wvlegislature\.gov/(\d+[A-Za-z]*-\d+[A-Za-z]*-\d+[A-Za-z]*)/?$')

# Sample section URLs for --sample mode
SAMPLE_SECTIONS = [
    "/1-1-1/", "/1-1-2/", "/1-1-3/", "/1-1-4/", "/1-1-5/",
    "/5-1-1/", "/5-1-2/", "/5-1-3/", "/5-1-4/", "/5-1-5/",
    "/61-2-1/", "/61-2-2/", "/61-2-3/", "/61-2-4/", "/61-2-5/",
]


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
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class WVLegislationScraper(BaseScraper):

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
        """Test connectivity to WV Code site."""
        logger.info("Testing WV Legislature Code site...")
        try:
            html = self._get(f"{BASE_URL}/1-1-1/")
            if 'sectiontext' in html:
                logger.info("  Section 1-1-1: OK (found sectiontext)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Section 1-1-1: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_sections_from_sitemap(self) -> list:
        """Fetch sitemap.xml and extract section-level URLs."""
        logger.info("Fetching sitemap.xml...")
        xml_text = self._get(f"{BASE_URL}/sitemap.xml")

        # Parse XML — handle namespace
        root = ET.fromstring(xml_text)
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        urls = []
        for url_elem in root.findall('.//sm:url/sm:loc', ns):
            loc = url_elem.text.strip() if url_elem.text else ""
            m = SECTION_URL_RE.match(loc)
            if m:
                urls.append(m.group(1))  # e.g., "1-1-1"

        # If namespace didn't match, try without namespace
        if not urls:
            for url_elem in root.iter():
                if url_elem.tag.endswith('}loc') or url_elem.tag == 'loc':
                    loc = url_elem.text.strip() if url_elem.text else ""
                    m = SECTION_URL_RE.match(loc)
                    if m:
                        urls.append(m.group(1))

        logger.info(f"Discovered {len(urls)} section URLs from sitemap")
        return urls

    def parse_section(self, section_id: str) -> dict:
        """Fetch a section page and extract text.

        Returns a raw section dict or None if no content found.
        """
        url = f"{BASE_URL}/{section_id}/"
        html = self._get(url)

        # Extract text from <div class='sectiontext ...'>
        # The heading is in <h4> and body in <p> tags within that div
        section_match = re.search(
            r"<div\s+class=['\"]sectiontext[^'\"]*['\"][^>]*>(.*?)</div>",
            html, re.DOTALL
        )
        if not section_match:
            return None

        section_html = section_match.group(1)

        # Extract heading from h4
        heading = ""
        h4_match = re.search(r'<h4[^>]*>(.*?)</h4>', section_html, re.DOTALL)
        if h4_match:
            heading = strip_html(h4_match.group(1))

        # Extract body text from all <p> tags
        body_parts = []
        for p_match in re.finditer(r'<p[^>]*>(.*?)</p>', section_html, re.DOTALL):
            p_text = strip_html(p_match.group(1))
            if p_text:
                body_parts.append(p_text)

        text = '\n\n'.join(body_parts)
        if not text or len(text) < 5:
            return None

        # Parse section number from heading: §1-1-1. Title here.
        sec_title = ""
        sec_match = re.match(r'§\s*([\d\w.-]+)\.\s*(.*)', heading)
        if sec_match:
            sec_title = sec_match.group(2).rstrip('.')
        else:
            sec_title = heading

        # Extract chapter from section_id (first part before first hyphen)
        parts = section_id.split('-')
        chapter = parts[0] if parts else ""

        full_title = f"West Virginia Code § {section_id}"
        if sec_title:
            full_title = f"West Virginia Code § {section_id} — {sec_title}"

        return {
            "section_id": section_id,
            "section_title": sec_title,
            "title": full_title,
            "text": text,
            "heading": heading,
            "chapter": chapter,
            "url": url,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        return {
            "_id": raw["section_id"],
            "_source": "US/WV-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw["title"],
            "text": raw["text"],
            "date": today,
            "url": raw["url"],
            "chapter": raw["chapter"],
            "section_id": raw["section_id"],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all WV Code sections."""
        section_ids = self.discover_sections_from_sitemap()
        logger.info(f"Processing {len(section_ids)} sections...")
        total = 0
        for sid in section_ids:
            try:
                raw = self.parse_section(sid)
                if raw:
                    yield raw
                    total += 1
                    if total <= 20 or total % 200 == 0:
                        logger.info(f"  {sid}: OK ({len(raw['text'])} chars, total: {total})")
            except Exception as e:
                logger.warning(f"  {sid}: failed ({e})")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample of sections."""
        logger.info(f"Fetching sample from {len(SAMPLE_SECTIONS)} sections...")
        count = 0
        for path in SAMPLE_SECTIONS:
            sid = path.strip('/')
            try:
                raw = self.parse_section(sid)
                if raw:
                    yield raw
                    count += 1
                    logger.info(f"  {sid}: OK ({len(raw['text'])} chars)")
            except Exception as e:
                logger.warning(f"  {sid}: failed ({e})")
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WV-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WVLegislationScraper()

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
