#!/usr/bin/env python3
"""
US/DE-Code -- Delaware Code Fetcher

Fetches the Delaware Code from the official delcode.delaware.gov website.
Crawls all 31 titles, discovers chapters and subchapters, then parses
individual sections from HTML pages.

Strategy:
  - Discover all title pages from the main index
  - For each title, discover chapter and subchapter pages
  - Parse section content from HTML using div.Section / div.SectionHead
  - Extract plain text by stripping HTML tags

Data:
  - 31 titles covering all Delaware law
  - Notable: Title 8 (General Corporation Law)
  - Format: HTML with section-level granularity
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DE-Code")

BASE_URL = "https://delcode.delaware.gov"
TITLES = list(range(1, 32))  # Titles 1-31


class DECodeScraper(BaseScraper):
    """
    Scraper for US/DE-Code -- Delaware Code.
    Country: US
    URL: https://delcode.delaware.gov

    Data types: legislation
    Auth: none (official government website)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html",
            },
            timeout=30,
        )

    def _fetch_page(self, path: str) -> Optional[str]:
        """Fetch an HTML page and return its content."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(path)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            # Page declares UTF-8 in meta but server sends ISO-8859-1 header
            resp.encoding = "utf-8"
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {path}: {e}")
            return None

    def _discover_pages(self, title_num: int) -> List[str]:
        """
        Discover all section-containing pages for a title.
        Returns list of URL paths that contain section content.
        """
        pages = []
        title_path = f"/title{title_num}/index.html"
        title_html = self._fetch_page(title_path)
        if not title_html:
            return pages

        # Find chapter links (format: ../titleN/cNNN/index.html or cNNN/index.html)
        chapter_links = re.findall(
            rf'href="[^"]*?(c\d+/index\.html)"', title_html
        )
        # Deduplicate
        chapter_links = list(dict.fromkeys(chapter_links))

        if not chapter_links:
            # Title page itself might have sections
            if '<div class="SectionHead"' in title_html:
                pages.append(title_path)
            return pages

        for ch_link in chapter_links:
            ch_path = f"/title{title_num}/{ch_link}"
            ch_html = self._fetch_page(ch_path)
            if not ch_html:
                continue

            # Check if chapter has subchapters (format: ../titleN/cNNN/scNN/index.html or scNN/index.html)
            sc_links = re.findall(
                r'href="[^"]*?(sc\d+/index\.html)"', ch_html
            )
            sc_links = list(dict.fromkeys(sc_links))

            if sc_links:
                # Has subchapters — fetch each
                ch_dir = ch_link.replace("/index.html", "")
                for sc_link in sc_links:
                    sc_path = f"/title{title_num}/{ch_dir}/{sc_link}"
                    pages.append(sc_path)
            elif '<div class="SectionHead"' in ch_html:
                # Chapter has sections directly
                pages.append(ch_path)

        return pages

    def _parse_sections(self, html: str, page_path: str) -> List[Dict[str, Any]]:
        """
        Parse individual sections from a page HTML.
        Returns list of section dicts with id, heading, text, history.
        """
        sections = []

        # Extract title/chapter from breadcrumb
        title_match = re.search(r'Title\s*(\d+)', html)
        title_num = title_match.group(1) if title_match else ""

        # Split on Section divs
        parts = re.split(r'<div class="Section">', html)

        for part in parts[1:]:  # Skip before first Section
            # Extract section ID
            head_match = re.search(r'<div class="SectionHead" id="([^"]+)">', part)
            if not head_match:
                continue
            section_id = head_match.group(1)

            # Get content up to end of this section
            end_marker = part.find("</div><br>")
            if end_marker > 0:
                section_html = part[:end_marker]
            else:
                section_html = part

            # Extract heading from SectionHead
            head_end = section_html.find("</div>")
            head_html = section_html[:head_end] if head_end > 0 else ""
            # Get text content of heading
            heading_text = re.sub(r"<[^>]+>", " ", head_html)
            heading_text = re.sub(r"\s+", " ", heading_text).strip()
            # Clean up section number prefix (§ NNN. or just NNN.)
            heading_text = re.sub(r"^[§\s]*" + re.escape(section_id) + r"\.?\s*", "", heading_text)
            heading_text = heading_text.strip(". ")

            # Extract full text (strip HTML tags)
            text = re.sub(r"<[^>]+>", " ", section_html)
            text = unescape(text)
            text = re.sub(r"\s+", " ", text).strip()

            # Clean up leading section marker
            text = re.sub(r"^[§\s]*" + re.escape(section_id) + r"\.?\s*", "", text)

            # Extract amendment history (typically at end after section text)
            history = ""
            hist_patterns = [
                r"(\d+\s+Del\.\s+Laws.*?)$",
                r"(\d+\s+Del\.\s+C\.\s+\d+.*?)$",
            ]
            for pat in hist_patterns:
                hist_match = re.search(pat, text)
                if hist_match:
                    history = hist_match.group(1).strip()
                    break

            if not text or len(text) < 20:
                continue

            sections.append({
                "section_id": section_id,
                "title_number": title_num,
                "heading": heading_text,
                "text": text,
                "history": history,
                "page_path": page_path,
            })

        return sections

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Delaware Code sections."""
        total_pages = 0
        total_sections = 0

        for title_num in TITLES:
            logger.info(f"Discovering pages for Title {title_num}...")
            pages = self._discover_pages(title_num)
            logger.info(f"Title {title_num}: {len(pages)} section pages found")
            total_pages += len(pages)

            for page_path in pages:
                html = self._fetch_page(page_path)
                if not html:
                    continue

                sections = self._parse_sections(html, page_path)
                for section in sections:
                    total_sections += 1
                    yield section

            if total_sections > 0 and total_sections % 500 == 0:
                logger.info(f"Progress: {total_sections} sections yielded so far")

        logger.info(f"Total: {total_pages} pages, {total_sections} sections")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Delaware Code doesn't support incremental updates."""
        logger.info("No incremental update support — running full fetch")
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw section data into normalized record."""
        section_id = raw["section_id"]
        title_num = raw["title_number"]
        text = raw["text"]
        heading = raw["heading"]

        if not text or len(text) < 30:
            return None

        # Build title string
        title = f"Del. C. tit. {title_num}, § {section_id}"
        if heading:
            title = f"{title} — {heading}"

        # Build URL
        url = f"{BASE_URL}{raw['page_path']}#{section_id}"

        # Try to extract a date from history
        date = None
        history = raw.get("history", "")
        if history:
            # Look for year patterns like "83 Del. Laws, c. 42, § 1;"
            year_matches = re.findall(r"(\d{2,3})\s+Del\.\s+Laws", history)
            # Delaware Laws volumes roughly correspond to years
            # Volume 49 ≈ 1953, so year ≈ 1904 + volume
            # But this is approximate, so skip date extraction from volumes

        return {
            "_id": f"US/DE-Code/{title_num}-{section_id}",
            "_source": "US/DE-Code",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "section_number": section_id,
            "title_number": title_num,
            "heading": heading,
            "jurisdiction": "US-DE",
        }


# ── CLI ──────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DE-Code scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (10 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = DECodeScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            resp = scraper.client.get("/")
            resp.raise_for_status()
            logger.info(f"Status: {resp.status_code}, Length: {len(resp.text)}")
            logger.info("Connectivity test PASSED")
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
