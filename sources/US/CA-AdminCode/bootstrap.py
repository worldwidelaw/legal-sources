#!/usr/bin/env python3
"""
US/CA-AdminCode -- California Code of Regulations (CCR)

Fetches all CCR sections with full text from Cornell LII
(law.cornell.edu/regulations/california).

Strategy:
  1. Fetch title list to discover all 27 title URLs
  2. For each title, crawl division -> chapter -> article hierarchy
  3. At article (or chapter) level, extract individual section links
  4. For each section, fetch the page and extract full regulation text

Data: Public domain (California government regulations). No auth required.
Crawl-delay: 10 seconds per robots.txt.

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
logger = logging.getLogger("legal-data-hunter.US.CA-AdminCode")

BASE_URL = "https://www.law.cornell.edu"

# Sample sections from different titles for --sample mode
SAMPLE_SECTIONS = [
    "/regulations/california/1-CCR-16",
    "/regulations/california/1-CCR-10",
    "/regulations/california/1-CCR-12",
    "/regulations/california/2-CCR-11008",
    "/regulations/california/2-CCR-11017",
    "/regulations/california/2-CCR-11035",
    "/regulations/california/3-CCR-3591",
    "/regulations/california/4-CCR-1",
    "/regulations/california/5-CCR-80001",
    "/regulations/california/8-CCR-3203",
    "/regulations/california/8-CCR-5155",
    "/regulations/california/11-CCR-1005",
    "/regulations/california/14-CCR-670",
    "/regulations/california/18-CCR-17014",
    "/regulations/california/22-CCR-50000",
]


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'</div>', '\n\n', text)
    text = re.sub(r'</li>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class CAAdminCodeScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )
        self.delay = 10.0  # robots.txt crawl-delay

    def _get(self, url: str, retries: int = 2) -> str:
        """Fetch URL with rate limiting and retries."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 429:
                    wait = min(60, self.delay * (attempt + 2))
                    logger.warning(f"Rate limited on {url}, waiting {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    logger.debug(f"404: {url}")
                    return ""
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url}: {e}")
                if attempt < retries:
                    time.sleep(5)
        return ""

    def test_api(self):
        """Test connectivity to Cornell LII."""
        logger.info("Testing Cornell LII California regulations...")
        try:
            html = self._get(f"{BASE_URL}/regulations/california")
            if "title-1" in html:
                logger.info("  Title list: OK")
            else:
                logger.error("  Title list: unexpected content")
                return False

            html = self._get(f"{BASE_URL}/regulations/california/1-CCR-16")
            if "Clarity" in html or "clarity" in html.lower():
                logger.info("  Section page: OK (1 CCR § 16)")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("  Section page: unexpected content")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def _extract_links(self, html: str, pattern: str) -> list:
        """Extract unique links matching a regex pattern from HTML."""
        links = []
        seen = set()
        for m in re.finditer(pattern, html):
            path = m.group(1)
            if path not in seen:
                seen.add(path)
                links.append(path)
        return links

    def discover_titles(self) -> list:
        """Discover all title URLs from the main page."""
        html = self._get(f"{BASE_URL}/regulations/california")
        titles = self._extract_links(html, r'href="(/regulations/california/title-(\d+))"')
        # _extract_links returns the full match group(1), we need to re-parse
        title_paths = []
        seen = set()
        for m in re.finditer(r'href="(/regulations/california/title-(\d+))"', html):
            path = m.group(1)
            num = m.group(2)
            if num not in seen:
                seen.add(num)
                title_paths.append({"path": path, "num": num})
        logger.info(f"Discovered {len(title_paths)} titles")
        return title_paths

    def discover_hierarchy(self, page_path: str) -> list:
        """Discover child links from a hierarchy page (division/chapter/article).

        Returns a list of paths. Detects whether children are:
        - Divisions (/title-N/division-N)
        - Chapters (/title-N/.../chapter-N)
        - Articles (/title-N/.../article-N)
        - Sections (/regulations/california/N-CCR-...)
        """
        html = self._get(f"{BASE_URL}{page_path}")
        if not html:
            return []

        # Look for section links first (leaf level)
        sections = []
        for m in re.finditer(r'href="(/regulations/california/(\d+)-CCR-[^"]+)"', html):
            path = m.group(1)
            # Skip appendix links and anchors
            if '#' not in path:
                sections.append(path)

        if sections:
            # Deduplicate
            seen = set()
            unique = []
            for s in sections:
                if s not in seen:
                    seen.add(s)
                    unique.append(s)
            return unique

        # Look for hierarchy links (divisions, chapters, articles)
        children = []
        for pattern in [
            r'href="(' + re.escape(page_path) + r'/division-[^"]+)"',
            r'href="(' + re.escape(page_path) + r'/chapter-[^"]+)"',
            r'href="(' + re.escape(page_path) + r'/article-[^"]+)"',
            r'href="(' + re.escape(page_path) + r'/subchapter-[^"]+)"',
        ]:
            found = self._extract_links(html, pattern)
            if found:
                children.extend(found)

        # Fallback: look for any deeper regulation path links
        if not children:
            base_escaped = re.escape(page_path)
            found = self._extract_links(html, r'href="(' + base_escaped + r'/[^"]+)"')
            children.extend(found)

        return children

    def crawl_to_sections(self, page_path: str, depth: int = 0) -> list:
        """Recursively crawl hierarchy until we find section links."""
        if depth > 6:
            logger.warning(f"Max depth reached at {page_path}")
            return []

        children = self.discover_hierarchy(page_path)
        if not children:
            return []

        # Check if children are section links (N-CCR-...)
        if children and re.match(r'/regulations/california/\d+-CCR-', children[0]):
            return children

        # Otherwise, recurse into each child
        all_sections = []
        for child in children:
            sections = self.crawl_to_sections(child, depth + 1)
            all_sections.extend(sections)
        return all_sections

    def fetch_section(self, section_path: str) -> dict:
        """Fetch a single section page and extract full text."""
        url = f"{BASE_URL}{section_path}"
        html = self._get(url)
        if not html:
            return None

        # Extract section number from path (e.g., /regulations/california/1-CCR-16)
        path_match = re.match(r'/regulations/california/(\d+)-CCR-(.+)', section_path)
        if not path_match:
            return None
        title_num = path_match.group(1)
        section_num = path_match.group(2)

        # Extract the page title
        heading = ""
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
        if title_match:
            heading = strip_html(title_match.group(1))
        if not heading:
            title_match = re.search(r'<title>(.*?)</title>', html, re.DOTALL)
            if title_match:
                heading = strip_html(title_match.group(1)).split("|")[0].strip()

        # Extract the main regulation text
        text = self._extract_regulation_text(html)
        if not text:
            return None

        # Extract authority and reference
        authority = ""
        reference = ""
        auth_match = re.search(r'(?:Note:\s*)?Authority\s+cited?:?\s*(.*?)(?:(?:Reference|$))', html, re.DOTALL | re.IGNORECASE)
        if auth_match:
            authority = strip_html(auth_match.group(1)).strip().rstrip('.')
        ref_match = re.search(r'Reference:?\s*(.*?)(?:</p>|</div>|<br|$)', html, re.DOTALL | re.IGNORECASE)
        if ref_match:
            reference = strip_html(ref_match.group(1)).strip().rstrip('.')

        return {
            "section_id": f"CCR-{title_num}-{section_num}",
            "title_num": title_num,
            "section_number": section_num,
            "title": heading,
            "text": text,
            "authority": authority,
            "reference": reference,
            "url": url,
        }

    def _extract_regulation_text(self, html: str) -> str:
        """Extract the main regulation text from a section page."""
        # Try to find the main content area
        # Cornell LII uses various content containers
        text = ""

        # Strategy 1: Look for the main content div
        for marker in [
            'id="block-system-main"',
            'class="field-name-body"',
            'class="field--name-body"',
            'property="content:encoded"',
            'class="pane-node-body"',
            'id="content"',
        ]:
            idx = html.find(marker)
            if idx > 0:
                content = html[idx:]
                # Find the end of the content area
                # Cut before notes/history section or footer
                for end_marker in ['Note: Authority', 'NOTE: Authority',
                                   'AUTHORITY:', 'HISTORY:', 'History:',
                                   'class="field-name-field-notes"',
                                   'id="footer"', '</article>']:
                    end_idx = content.find(end_marker)
                    if end_idx > 0:
                        content = content[:end_idx]
                        break
                text = strip_html(content)
                if len(text) > 50:
                    break

        # Strategy 2: Extract everything between first heading and Note/Authority
        if len(text) < 50:
            # Find content after h1
            h1_end = html.find('</h1>')
            if h1_end > 0:
                content = html[h1_end + 5:]
                for end_marker in ['Note: Authority', 'NOTE: Authority',
                                   'AUTHORITY:', 'class="footnote"']:
                    end_idx = content.find(end_marker)
                    if end_idx > 0:
                        content = content[:end_idx]
                        break
                text = strip_html(content)

        # Clean up common artifacts
        if text:
            # Remove leaked HTML attributes
            text = re.sub(r'^id="[^"]*"[^>]*>\s*', '', text)
            # Remove the repeated section heading at top (already in title field)
            text = re.sub(r'^Cal\.\s*Code\s*Regs\.\s*Tit\.\s*\d+,\s*§\s*\S+\s*-?\s*[^\n]*\n*', '', text)
            # Remove navigation text
            text = re.sub(r'^\s*State Regulations\s*\n*', '', text)
            text = re.sub(r'^\s*Compare\s*\n*', '', text)
            text = re.sub(r'(?:Previous|Next)\s*(?:§|Section)\s*', '', text)
            text = re.sub(r'(?:Table of Contents|Browse)', '', text)
            # Remove LII-specific boilerplate
            text = re.sub(r'Cornell Law School.*?Legal Information Institute', '', text, flags=re.DOTALL)
            text = re.sub(r'About LII.*$', '', text, flags=re.DOTALL)
            # Remove Notes/History section at the end
            text = re.sub(r'\n\s*Notes\s*\n\s*Cal\.\s*Code.*$', '', text, flags=re.DOTALL)
            text = re.sub(r'\n{3,}', '\n\n', text).strip()

        return text

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["section_id"],
            "_source": "US/CA-AdminCode",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "section_id": raw["section_id"],
            "title_num": raw.get("title_num", ""),
            "section_number": raw.get("section_number", ""),
            "title": raw["title"],
            "text": raw["text"],
            "authority": raw.get("authority", ""),
            "reference": raw.get("reference", ""),
            "url": raw.get("url", ""),
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all CCR sections with full text."""
        titles = self.discover_titles()
        total = 0
        for title in titles:
            logger.info(f"Processing Title {title['num']}...")
            sections = self.crawl_to_sections(title["path"])
            logger.info(f"  Found {len(sections)} sections in Title {title['num']}")
            for sec_path in sections:
                raw = self.fetch_section(sec_path)
                if raw and raw.get("text"):
                    yield self.normalize(raw)
                    total += 1
                    if total % 50 == 0:
                        logger.info(f"  Progress: {total} sections fetched")
        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updates since a date. CCR doesn't support date filtering."""
        yield from self.fetch_all()

    def bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = Path(self.source_dir) / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info(f"Running in SAMPLE mode — fetching {len(SAMPLE_SECTIONS)} sections")
            count = 0
            for sec_path in SAMPLE_SECTIONS:
                raw = self.fetch_section(sec_path)
                if raw and raw.get("text"):
                    record = self.normalize(raw)
                    out_file = sample_dir / f"{record['_id'].replace('/', '_')}.json"
                    out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                    count += 1
                    logger.info(f"  Saved: {record['_id']} ({len(record['text'])} chars)")
                else:
                    logger.warning(f"  No text for {sec_path}")
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
    scraper = CAAdminCodeScraper()

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
