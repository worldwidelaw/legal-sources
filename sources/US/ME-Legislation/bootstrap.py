#!/usr/bin/env python3
"""
US/ME-Legislation -- Maine Revised Statutes

Fetches codified statutes from legislature.maine.gov by crawling the static HTML
hierarchy: title index → chapter index → section pages.

URL patterns:
  - Title list:   /statutes/
  - Title index:  /statutes/{T}/title{T}ch0sec0.html  (lists chapters)
  - Chapter index: /statutes/{T}/title{T}ch{C}sec0.html (lists sections)
  - Section page: /statutes/{T}/title{T}sec{S}.html   (full text)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py update --since YYYY-MM-DD  # Re-fetch all
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.ME-Legislation")

BASE_URL = "https://legislature.maine.gov/statutes"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Sample sections across diverse titles
SAMPLE_SECTIONS = [
    ("1", "1"),      # General Provisions - Sovereignty
    ("5", "1"),      # Administrative Procedures
    ("9-A", "1-101"),  # Consumer Credit Code
    ("12", "1"),     # Conservation
    ("14", "1"),     # Court Procedure
    ("15", "1"),     # Court Procedure -- Criminal
    ("17-A", "1"),   # Maine Criminal Code
    ("18-C", "1-101"),  # Probate Code
    ("20-A", "1"),   # Education
    ("22", "1"),     # Health and Welfare
    ("24-A", "1"),   # Insurance
    ("26", "1"),     # Labor and Industry
    ("29-A", "101"), # Motor Vehicles
    ("33", "1"),     # Property
    ("36", "1"),     # Taxation
]


class MELegislationScraper(BaseScraper):
    """
    Scraper for US/ME-Legislation — Maine Revised Statutes.
    Crawls legislature.maine.gov HTML hierarchy.
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
        })

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page with retries."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                resp.encoding = "utf-8"
                return resp.text
            except requests.exceptions.Timeout:
                if attempt < 2:
                    time.sleep(2)
                    continue
                return None
            except Exception as e:
                logger.warning(f"Fetch {url} attempt {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(2)
                    continue
                return None
        return None

    def _get_title_ids(self) -> List[str]:
        """Get list of all title IDs from the main statutes page."""
        html = self._fetch_page(f"{BASE_URL}/")
        if not html:
            return []
        # Pattern: href="1/title1ch0sec0.html" or "17-A/title17-Ach0sec0.html"
        matches = re.findall(r'href="([^/]+)/title[^"]*ch0sec0\.html"', html)
        # Deduplicate while preserving order
        seen = set()
        titles = []
        for t in matches:
            if t not in seen:
                seen.add(t)
                titles.append(t)
        return titles

    def _get_title_name(self, html: str) -> str:
        """Extract title name from a title index page."""
        match = re.search(r'Title\s+\d+[-A-Z]*:\s*([^<\n]+)', html, re.IGNORECASE)
        if match:
            return match.group(0).strip()
        return ""

    def _get_chapters(self, title_id: str) -> List[Tuple[str, str]]:
        """Get list of (chapter_id, chapter_name) from a title index page."""
        url = f"{BASE_URL}/{title_id}/title{title_id}ch0sec0.html"
        html = self._fetch_page(url)
        if not html:
            return []

        # Find chapter links: ./title1ch1sec0.html
        pattern = rf'href="\./title{re.escape(title_id)}ch([^s"]+)sec0\.html"'
        chapter_ids = re.findall(pattern, html)
        # Deduplicate
        seen = set()
        chapters = []
        for ch in chapter_ids:
            if ch != "0" and ch not in seen:
                seen.add(ch)
                chapters.append(ch)
        return chapters

    def _get_sections(self, title_id: str, chapter_id: str) -> List[str]:
        """Get list of section numbers from a chapter index page."""
        url = f"{BASE_URL}/{title_id}/title{title_id}ch{chapter_id}sec0.html"
        html = self._fetch_page(url)
        if not html:
            return []

        # Find section links: ./title1sec1.html
        pattern = rf'href="\./title{re.escape(title_id)}sec([^."]+)\.html"'
        section_ids = re.findall(pattern, html)
        # Deduplicate
        seen = set()
        sections = []
        for s in section_ids:
            if s not in seen:
                seen.add(s)
                sections.append(s)
        return sections

    def _get_chapter_name(self, html: str) -> str:
        """Extract chapter name from a chapter index page."""
        match = re.search(r'Chapter\s+\d+[-A-Z]*:\s*([^<\n]+)', html, re.IGNORECASE)
        if match:
            return match.group(0).strip()
        return ""

    def _fetch_section(self, title_id: str, section_id: str) -> Optional[Dict[str, Any]]:
        """Fetch full text of a statute section."""
        url = f"{BASE_URL}/{title_id}/title{title_id}sec{section_id}.html"
        html = self._fetch_page(url)
        if not html:
            return None

        # Extract title name
        title_match = re.search(r'Title\s+[\d][-\dA-Z]*:\s*([^<\n]+)', html)
        title_name = title_match.group(0).strip() if title_match else f"Title {title_id}"

        # Extract chapter info
        chapter_match = re.search(r'Chapter\s+[\d][-\dA-Z]*:\s*([^<\n]+)', html)
        chapter_name = chapter_match.group(0).strip() if chapter_match else ""

        # Extract section heading from heading_section class
        sec_heading_match = re.search(
            r'class="heading_section"[^>]*>\s*§' + re.escape(section_id) + r'[\.\s]+([^<]+)',
            html
        )
        if not sec_heading_match:
            # Fallback: try anywhere in content
            sec_heading_match = re.search(
                r'§' + re.escape(section_id) + r'\.\s+([^<"\n]+)',
                html
            )
        section_heading = sec_heading_match.group(1).strip() if sec_heading_match else ""

        # Extract body text
        text = self._extract_text(html, section_id)
        if not text or len(text) < 10:
            logger.warning(f"No text for title {title_id} sec {section_id}")
            return None

        return {
            "title_id": title_id,
            "title_name": title_name,
            "chapter_name": chapter_name,
            "section_id": section_id,
            "section_heading": section_heading,
            "text": text,
            "url": url,
        }

    def _extract_text(self, html: str, section_id: str) -> str:
        """Extract statute text from section HTML page."""
        # Extract the section-content div which contains the statute text
        match = re.search(r'row\s+section-content">(.*?)(?:The Revisor|Office of the Revisor|<footer)', html, re.DOTALL)
        if not match:
            # Fallback: try body
            match = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL)
            if not match:
                return ""

        chunk = match.group(1)

        # Strip HTML tags
        text = re.sub(r'<[^>]+>', '\n', chunk)
        # Decode entities
        text = re.sub(r'&sect;', '\u00a7', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&#39;', "'", text)
        text = re.sub(r'&quot;', '"', text)
        text = re.sub(r'&mdash;', '\u2014', text)
        text = re.sub(r'&ndash;', '\u2013', text)

        lines = [l.strip() for l in text.split('\n') if l.strip()]

        # Filter out footer/disclaimer lines
        content_lines = []
        for line in lines:
            if any(marker in line for marker in [
                "Revisor's Office", "Office of the Revisor",
                "cannot provide legal advice",
                "State House Station", "Augusta, Maine",
                "Data for this page extracted on",
            ]):
                break
            content_lines.append(line)

        result = '\n'.join(content_lines)
        result = re.sub(r'\n{3,}', '\n\n', result)
        return result.strip()

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Maine Revised Statutes sections."""
        titles = self._get_title_ids()
        if not titles:
            logger.error("No titles found")
            return

        logger.info(f"Found {len(titles)} titles")
        total = 0

        for t_idx, title_id in enumerate(titles):
            logger.info(f"Title {t_idx+1}/{len(titles)}: {title_id}")
            delay = self.config.get("fetch", {}).get("delay", 1.5)
            time.sleep(delay)

            chapters = self._get_chapters(title_id)
            if not chapters:
                logger.warning(f"No chapters for title {title_id}")
                continue

            for ch in chapters:
                time.sleep(delay)
                sections = self._get_sections(title_id, ch)

                for sec in sections:
                    time.sleep(delay)
                    raw = self._fetch_section(title_id, sec)
                    if raw:
                        total += 1
                        yield raw

            logger.info(f"Progress: {total} records after title {title_id}")

        logger.info(f"Total fetched: {total}")

    def fetch_sample(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch a representative sample of sections."""
        for title_id, sec_id in SAMPLE_SECTIONS:
            delay = self.config.get("fetch", {}).get("delay", 1.5)
            time.sleep(delay)
            raw = self._fetch_section(title_id, sec_id)
            if raw:
                yield raw

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all sections (static HTML, no incremental)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw section record into the standard schema."""
        title_id = raw.get("title_id", "")
        section_id = raw.get("section_id", "")
        title_name = raw.get("title_name", f"Title {title_id}")
        section_heading = raw.get("section_heading", "")

        doc_id = f"US-ME-T{title_id}-S{section_id}"
        title = f"{title_name} \u00a7{section_id}"
        if section_heading:
            title += f" \u2014 {section_heading}"

        return {
            "_id": doc_id,
            "_source": "US/ME-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("url", ""),
            "title_number": title_id,
            "title_name": title_name,
            "chapter": raw.get("chapter_name", ""),
            "section_number": section_id,
            "section_heading": section_heading,
            "jurisdiction": "US-ME",
        }

    def test_connection(self) -> bool:
        """Test connectivity to legislature.maine.gov."""
        try:
            titles = self._get_title_ids()
            if not titles:
                logger.error("No titles found")
                return False
            logger.info(f"Got {len(titles)} titles")

            raw = self._fetch_section("1", "1")
            if raw and raw.get("text"):
                logger.info(f"Text fetch OK: {len(raw['text'])} chars")
                return True

            logger.error("Text fetch returned no content")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/ME-Legislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--since", help="ISO date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = MELegislationScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999

        gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()
        for raw in gen:
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(f"[{count + 1}] {record['_id']}: {record['title'][:60]} ({text_len} chars)")
            count += 1
            if count >= target:
                break

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates(since=args.since):
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
