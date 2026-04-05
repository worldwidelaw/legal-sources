#!/usr/bin/env python3
"""
US/TX-AdminCode -- Texas Administrative Code (eLaws mirror)

Fetches all Texas Administrative Code rules with full text from the
eLaws mirror at txrules.elaws.us.

Strategy:
  1. Fetch title list page to discover all 16 title URLs
  2. For each title, fetch its page to discover all chapter URLs
  3. For each chapter, fetch its page to discover all section URLs
  4. For each section, fetch the page and extract full rule text

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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-AdminCode")

BASE_URL = "https://txrules.elaws.us"

# Sample: a few sections from Title 1 Ch 3, Title 22 Ch 1, Title 30 Ch 101
SAMPLE_CHAPTERS = [
    "title1_chapter3",
    "title22_chapter1",
    "title30_chapter101",
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
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = 3.0

    def _get(self, url: str, retries: int = 2) -> str:
        """Fetch URL with rate limiting and retry on generic page."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            resp = self.http.get(url)
            html = resp.text
            # Check if we got a real page with content
            # Section pages should have CiteTitle or rulehome_rightdetail
            # Chapter pages should have section links (_sec.)
            # Title pages should have chapter links (_chapter)
            if self._is_real_page(url, html):
                return html
            if attempt < retries:
                logger.warning(f"Got generic page for {url}, retry {attempt+1}/{retries}")
                time.sleep(5)
        return html

    def _is_real_page(self, url: str, html: str) -> bool:
        """Check if we got real content vs a rate-limited generic page."""
        # The main title list page - check for title links
        if url.endswith('/rule'):
            return 'title1' in html
        # Section pages should have rule content
        if '_sec.' in url:
            return 'CiteTitle' in html or 'rulehome_rightdetail' in html
        # Chapter pages should have section links
        if '_chapter' in url:
            return '_sec.' in html or 'Sec.' in html
        # Title pages should have chapter links
        return '_chapter' in html or 'CHAPTER' in html

    def test_api(self):
        """Test connectivity to txrules.elaws.us."""
        logger.info("Testing txrules.elaws.us...")
        try:
            html = self._get(f"{BASE_URL}/rule")
            if "title1" in html.lower() or "TITLE 1" in html:
                logger.info("  Title list: OK")
            else:
                logger.error("  Title list: unexpected content")
                return False

            html = self._get(f"{BASE_URL}/rule/title1_chapter3")
            if "sec.3." in html or "SECTION 3." in html:
                logger.info("  Chapter page: OK (Title 1, Ch 3)")
            else:
                logger.error("  Chapter page: unexpected content")
                return False

            html = self._get(f"{BASE_URL}/rule/title1_chapter3_sec.3.1")
            if "Applicability" in html or "3.1" in html:
                logger.info("  Section page: OK (§3.1)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Section page: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def discover_titles(self) -> list:
        """Discover all title URLs from the main page."""
        html = self._get(f"{BASE_URL}/rule")
        titles = []
        for m in re.finditer(r'href="(/rule/title(\d+))"', html):
            path = m.group(1)
            num = m.group(2)
            titles.append({"path": path, "num": num})
        # Deduplicate
        seen = set()
        unique = []
        for t in titles:
            if t["num"] not in seen:
                seen.add(t["num"])
                unique.append(t)
        logger.info(f"Discovered {len(unique)} titles")
        return unique

    def discover_chapters(self, title_path: str) -> list:
        """Discover all chapter URLs from a title page."""
        html = self._get(f"{BASE_URL}{title_path}")
        chapters = []
        # Links can be absolute or relative
        pat = re.compile(r'href="(?:https://txrules\.elaws\.us)?(/rule/title\d+_chapter\d+)"')
        seen = set()
        for m in pat.finditer(html):
            path = m.group(1)
            if path not in seen:
                seen.add(path)
                chapters.append(path)
        return chapters

    def discover_sections(self, chapter_path: str) -> list:
        """Discover all section URLs from a chapter page."""
        html = self._get(f"{BASE_URL}{chapter_path}")
        sections = []
        # Links are absolute URLs on this site
        pat = re.compile(r'href="(?:https://txrules\.elaws\.us)?(/rule/title\d+_chapter\d+_sec\.[^"]+)"')
        seen = set()
        for m in pat.finditer(html):
            path = m.group(1)
            if path not in seen:
                seen.add(path)
                sections.append(path)
        return sections

    def fetch_section(self, section_path: str):
        """Fetch a single section page and extract text."""
        url = f"{BASE_URL}{section_path}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch {section_path}: {e}")
            return None

        if not html or "cannot be found" in html.lower() or "404" in html[:500]:
            return None

        # Extract section heading from CiteTitle span
        cite_match = re.search(r'<span id="CiteTitle">(.*?)</span>', html, re.DOTALL)
        heading = strip_html(cite_match.group(1)) if cite_match else ""

        # Extract breadcrumb from <title> tag
        # Format: "SECTION 3.1. ..., SUBCHAPTER A. ..., CHAPTER 3. ..., PART 1. ..., TITLE 1. ..."
        tac_title = ""
        chapter = ""
        title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
        if title_match:
            title_text = strip_html(title_match.group(1))
            parts = [p.strip() for p in title_text.split(",")]
            for p in parts:
                if p.startswith("TITLE "):
                    tac_title = p
                elif p.startswith("CHAPTER "):
                    chapter = p

        # Extract the main rule text content
        # The rule text is typically in a div after the h1, before source note
        # Try to find the content between the heading and source note
        text = self._extract_rule_text(html)

        # Extract source note
        source_note = ""
        sn_match = re.search(r'Source Note:?\s*(.*?)(?:</p>|</div>|$)', html, re.DOTALL)
        if sn_match:
            source_note = strip_html(sn_match.group(1)).strip()

        if not text:
            return None

        # Parse section number from path
        sec_match = re.search(r'_sec\.(.+)$', section_path)
        sec_num = sec_match.group(1) if sec_match else ""

        # Parse title and chapter numbers from path
        path_match = re.search(r'title(\d+)_chapter(\d+)', section_path)
        title_num = path_match.group(1) if path_match else ""
        chap_num = path_match.group(2) if path_match else ""

        return {
            "section_id": f"TAC-{title_num}-{chap_num}-{sec_num}",
            "section_number": sec_num,
            "title": heading,
            "text": text,
            "tac_title": tac_title,
            "tac_title_num": title_num,
            "chapter": chapter,
            "chapter_num": chap_num,
            "source_note": source_note,
            "url": url,
        }

    def _extract_rule_text(self, html: str) -> str:
        """Extract the main rule text from a section page.

        The rule text lives inside <div class="rulehome_rightdetail">,
        before the <div class="rule_historical"> (source note).
        """
        # Find the rulehome_rightdetail div
        start_marker = 'class="rulehome_rightdetail">'
        start_idx = html.find(start_marker)
        if start_idx == -1:
            return ""
        content = html[start_idx + len(start_marker):]

        # Cut before rule_historical (source note) or end-content marker
        for marker in ['class="rule_historical"', '<!-- End content -->',
                       'class="sharepart"']:
            idx = content.find(marker)
            if idx > 0:
                content = content[:idx]
                break

        text = strip_html(content)

        # Remove XML processing instructions that leak through
        text = re.sub(r'<\?[^>]*\?>', '', text)

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
        titles = self.discover_titles()
        total_sections = 0
        for title in titles:
            logger.info(f"Processing Title {title['num']}...")
            chapters = self.discover_chapters(title["path"])
            logger.info(f"  Found {len(chapters)} chapters")
            for chap_path in chapters:
                sections = self.discover_sections(chap_path)
                logger.info(f"  {chap_path}: {len(sections)} sections")
                for sec_path in sections:
                    raw = self.fetch_section(sec_path)
                    if raw and raw.get("text"):
                        yield self.normalize(raw)
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
            logger.info("Running in SAMPLE mode — fetching from 3 chapters")
            count = 0
            for chap_path_suffix in SAMPLE_CHAPTERS:
                chap_path = f"/rule/{chap_path_suffix}"
                sections = self.discover_sections(chap_path)
                logger.info(f"  {chap_path}: {len(sections)} sections")
                # Take up to 5 sections per chapter
                for sec_path in sections[:5]:
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
