#!/usr/bin/env python3
"""
US/TX-AdminCode -- Texas Administrative Code (via Cornell LII)

Fetches all Texas Administrative Code rules with full text from
Cornell Law's Legal Information Institute (law.cornell.edu).

Strategy:
  1. Fetch title list from /regulations/texas
  2. Recursively discover parts → chapters → subchapters → sections
  3. For each section, extract full rule text from statereg-text div

Data: Public domain (Texas government works). No auth required.

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
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-AdminCode")

BASE_URL = "https://www.law.cornell.edu"

# Sample subchapters for --sample mode
SAMPLE_PATHS = [
    "/regulations/texas/title-1/part-1/chapter-3/subchapter-A",
    "/regulations/texas/title-22/part-1/chapter-1",
    "/regulations/texas/title-30/part-1/chapter-101/subchapter-A",
]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
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


class TXAdminCodeScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research; https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = 2.0

    def _get(self, url: str, retries: int = 3) -> str:
        """Fetch URL with rate limiting and retries."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 404:
                    return ""
                if resp.status_code >= 500 and attempt < retries:
                    logger.warning(f"HTTP {resp.status_code} for {url}, retry {attempt+1}/{retries}")
                    time.sleep(5 * (attempt + 1))
                    continue
                return resp.text
            except Exception as e:
                if attempt < retries:
                    logger.warning(f"Error fetching {url}: {e}, retry {attempt+1}/{retries}")
                    time.sleep(5 * (attempt + 1))
                else:
                    raise
        return ""

    def test_api(self):
        """Test connectivity to Cornell LII."""
        logger.info("Testing law.cornell.edu/regulations/texas...")
        try:
            html = self._get(f"{BASE_URL}/regulations/texas")
            if "title-1" in html:
                logger.info("  Title list: OK")
            else:
                logger.error("  Title list: unexpected content")
                return False

            html = self._get(f"{BASE_URL}/regulations/texas/1-Tex-Admin-Code-SS-3-1")
            if "Applicability" in html and "statereg-text" in html:
                logger.info("  Section page: OK (§3.1)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Section page: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def _find_links(self, html: str, pattern: str) -> list:
        """Extract unique href links matching a pattern."""
        matches = re.findall(pattern, html)
        seen = set()
        result = []
        for m in matches:
            if m not in seen:
                seen.add(m)
                result.append(m)
        return result

    def discover_section_urls(self, page_url: str, depth: int = 0) -> list:
        """Recursively discover section URLs from a hierarchy page.

        Section URLs look like: /regulations/texas/1-Tex-Admin-Code-SS-3-1
        Navigation URLs look like: /regulations/texas/title-N/part-M/...
        """
        if depth > 6:
            return []

        html = self._get(f"{BASE_URL}{page_url}")
        if not html:
            return []

        # Find section links (actual regulation section pages)
        section_pat = r'href="(/regulations/texas/\d+-Tex-Admin-Code-SS-[^"]+)"'
        sections = self._find_links(html, section_pat)

        if sections:
            return sections

        # No section links found — look for deeper navigation links
        nav_pat = r'href="(/regulations/texas/title-\d+/[^"]+)"'
        nav_links = self._find_links(html, nav_pat)

        # Filter: only follow links deeper than current page
        deeper = [l for l in nav_links if len(l) > len(page_url)]

        all_sections = []
        for link in deeper:
            child_sections = self.discover_section_urls(link, depth + 1)
            all_sections.extend(child_sections)

        return all_sections

    def fetch_section(self, section_path: str):
        """Fetch a single section page and extract text."""
        url = f"{BASE_URL}{section_path}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch {section_path}: {e}")
            return None

        if not html:
            return None

        # Extract title from h1
        h1_match = re.search(r'<h1[^>]*id="page_title"[^>]*>(.*?)</h1>', html, re.DOTALL)
        heading = strip_html(h1_match.group(1)).strip() if h1_match else ""

        # Extract full text from statereg-text div
        text = self._extract_rule_text(html)

        # Extract source note from statereg-notes
        source_note = ""
        notes_match = re.search(
            r'<div class="statereg-notes">(.*?)</div>\s*</div>',
            html, re.DOTALL
        )
        if notes_match:
            source_note = strip_html(notes_match.group(1)).strip()
            # Clean up the "Notes" heading
            source_note = re.sub(r'^Notes\s*', '', source_note).strip()

        if not text:
            return None

        # Parse section id from path: /regulations/texas/1-Tex-Admin-Code-SS-3-1
        # Format: {title}-Tex-Admin-Code-SS-{section}
        path_match = re.search(r'/(\d+)-Tex-Admin-Code-SS-(.+)$', section_path)
        if path_match:
            title_num = path_match.group(1)
            sec_num = path_match.group(2)
        else:
            title_num = ""
            sec_num = section_path.split("/")[-1]

        # Parse chapter from section number (e.g., "3-1" → chapter 3)
        chap_match = re.match(r'(\d+)', sec_num)
        chap_num = chap_match.group(1) if chap_match else ""

        # Extract TAC title info from breadcrumb or page context
        tac_title = ""
        bc_match = re.search(r'class="breadcrumb">(.*?)</[ou]l>', html, re.DOTALL)
        if bc_match:
            bc_text = strip_html(bc_match.group(1))
            # Look for "Title N" in breadcrumb
            t_match = re.search(r'(Title \d+)', bc_text)
            if t_match:
                tac_title = t_match.group(1)

        return {
            "section_id": f"TAC-{title_num}-{chap_num}-{sec_num}",
            "section_number": sec_num,
            "title": heading,
            "text": text,
            "tac_title": tac_title,
            "tac_title_num": title_num,
            "chapter": f"Chapter {chap_num}" if chap_num else "",
            "chapter_num": chap_num,
            "source_note": source_note,
            "url": url,
        }

    def _extract_rule_text(self, html: str) -> str:
        """Extract the main rule text from a Cornell LII section page."""
        # Find the statereg-text div
        start_marker = 'class="statereg-text">'
        start_idx = html.find(start_marker)
        if start_idx == -1:
            return ""
        content = html[start_idx + len(start_marker):]

        # Cut at the closing </div> for this div
        end_idx = content.find('</div>')
        if end_idx > 0:
            content = content[:end_idx]

        text = strip_html(content)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()
        return text

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["section_id"],
            "_source": "US/TX-AdminCode",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "section_id": raw["section_id"],
            "section_number": raw.get("section_number", ""),
            "title": raw["title"],
            "text": raw["text"],
            "tac_title": raw.get("tac_title", ""),
            "tac_title_num": raw.get("tac_title_num", ""),
            "chapter": raw.get("chapter", ""),
            "chapter_num": raw.get("chapter_num", ""),
            "source_note": raw.get("source_note", ""),
            "url": raw.get("url", ""),
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all TAC sections with full text."""
        html = self._get(f"{BASE_URL}/regulations/texas")
        title_paths = self._find_links(html, r'href="(/regulations/texas/title-\d+)"')
        logger.info(f"Discovered {len(title_paths)} titles")

        total_sections = 0
        for title_path in title_paths:
            title_num = title_path.split("-")[-1]
            logger.info(f"Processing Title {title_num}...")
            section_urls = self.discover_section_urls(title_path)
            logger.info(f"  Found {len(section_urls)} sections")
            for sec_path in section_urls:
                raw = self.fetch_section(sec_path)
                if raw and raw.get("text"):
                    yield raw
                    total_sections += 1
        logger.info(f"Total sections fetched: {total_sections}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updates since a date. TAC doesn't have date filtering, so full pull."""
        yield from self.fetch_all()

    def bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = Path(self.source_dir) / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info("Running in SAMPLE mode — fetching from sample paths")
            count = 0
            for page_path in SAMPLE_PATHS:
                section_urls = self.discover_section_urls(page_path)
                logger.info(f"  {page_path}: {len(section_urls)} sections")
                for sec_path in section_urls[:5]:
                    raw = self.fetch_section(sec_path)
                    if raw and raw.get("text"):
                        record = self.normalize(raw)
                        out_file = sample_dir / f"{record['_id'].replace('/', '_')}.json"
                        out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                        count += 1
                        logger.info(f"  Saved: {record['_id']} ({len(record['text'])} chars)")
            logger.info(f"Sample complete: {count} records saved to {sample_dir}")
        else:
            logger.info("Running FULL bootstrap")
            count = 0
            for record in self.fetch_all():
                out_file = sample_dir / f"{record['_id'].replace('/', '_')}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                if count % 100 == 0:
                    logger.info(f"  Progress: {count} records saved")
            logger.info(f"Full bootstrap complete: {count} records")


def main():
    scraper = TXAdminCodeScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [test-api|bootstrap] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)
    elif cmd == "bootstrap":
        sample = "--sample" in sys.argv
        scraper.bootstrap(sample=sample)
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
