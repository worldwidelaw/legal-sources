#!/usr/bin/env python3
"""
US/TN-Legislation -- Tennessee Code Annotated

Fetches Tennessee statutes from Internet Archive's public domain HTML
collection published by Public.Resource.Org (Release 76, May 2021).

Strategy:
  1. Download each title's HTML file from archive.org
  2. Parse sections using BeautifulSoup (h3 with section IDs)
  3. Extract statutory text from ol/li elements and p tags
  4. Normalize into standard schema

Data: Public domain. No auth required.
Rate limit: 1 req / 2 sec.

Usage:
  python bootstrap.py bootstrap            # Full pull (all 67 titles)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TN-Legislation")

IA_BASE = "https://archive.org/download/gov.tn.tca/release76.2021.05.21"
TITLE_RANGE = range(1, 72)  # titles 1-71 (some may not exist)

# Pattern matching section numbers like 1-1-101, 39-13-202, etc.
SECTION_RE = re.compile(r"^(\d+-\d+-\d+(?:\.\d+)?)\.\s+(.+?)\.?\s*$")


try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)


class TNLegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
            },
            timeout=120,
        )
        self.delay = 2.0

    def _get_html(self, url: str) -> Optional[str]:
        """Fetch URL with rate limiting, return HTML text."""
        time.sleep(self.delay)
        try:
            resp = self.http.get(url)
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _title_url(self, num: int) -> str:
        return f"{IA_BASE}/gov.tn.tca.title.{num:02d}.html"

    def _extract_section_text(self, section_div) -> str:
        """Extract statutory text from a section's content elements.

        Collects text from ol/li elements and direct p tags that contain
        statutory text. Stops at annotation markers (history, compiler's
        notes, cross-references, law reviews, etc.).
        """
        parts = []
        annotation_markers = {
            "acts ", "compiler's notes", "cross-references", "cross references",
            "law reviews", "effective dates", "amendments.", "notes to decisions",
            "textbooks.", "opinions of the attorney", "research references",
            "collateral references", "cited:", "transfer of functions",
        }

        # Get the h3 element's next siblings
        for elem in section_div.find_next_siblings():
            # Stop at next section header
            if elem.name == "h3":
                break
            # Stop at next div (new section container)
            if elem.name == "div":
                break

            if elem.name == "ol":
                for li in elem.find_all("li", recursive=False):
                    text = li.get_text(separator=" ", strip=True)
                    if text:
                        parts.append(text)
            elif elem.name == "p":
                text = elem.get_text(separator=" ", strip=True)
                if not text:
                    continue
                # Check if this is an annotation paragraph
                text_lower = text.lower()
                if any(text_lower.startswith(m) for m in annotation_markers):
                    break
                # History line (e.g., "Acts 1953, ch. 80, § 1;")
                if re.match(r"^Acts\s+\d{4}", text):
                    break
                # Only include substantive text
                if len(text) > 10:
                    parts.append(text)

        return "\n\n".join(parts)

    def _parse_title(self, html: str, title_num: int) -> Generator[dict, None, None]:
        """Parse all sections from a title's HTML."""
        soup = BeautifulSoup(html, "html.parser")

        # Get title name from h1
        h1 = soup.find("h1")
        title_name = ""
        if h1:
            title_name = h1.get_text(separator=" ", strip=True)
            # Clean "Title N <heading>"
            title_name = re.sub(r"^Title\s+\d+\s*", "", title_name).strip()

        # Current chapter tracking
        current_chapter = ""

        for elem in soup.find_all(["h2", "h3"]):
            if elem.name == "h2":
                current_chapter = elem.get_text(separator=" ", strip=True)
                # Clean "Chapter N <heading>"
                current_chapter = re.sub(r"^Chapter\s+\d+\s*", "", current_chapter).strip()
                continue

            if elem.name == "h3":
                heading_text = elem.get_text(separator=" ", strip=True)
                match = SECTION_RE.match(heading_text)
                if not match:
                    continue

                section_num = match.group(1)
                section_heading = match.group(2)

                # Extract the statutory text
                text = self._extract_section_text(elem)
                if not text or len(text) < 10:
                    continue

                yield {
                    "section_number": section_num,
                    "heading": section_heading,
                    "text": text,
                    "title_number": title_num,
                    "title_name": title_name,
                    "chapter": current_chapter,
                }

    def normalize(self, raw: dict) -> dict:
        """Transform parsed section into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        sec = raw["section_number"]

        return {
            "_id": f"tca-{sec}",
            "_source": "US/TN-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": f"TCA § {sec} — {raw['heading']}",
            "text": raw["text"],
            "date": "2021-05-21",  # Release date
            "url": f"{IA_BASE}/gov.tn.tca.title.{raw['title_number']:02d}.html#t{raw['title_number']:02d}c{sec.split('-')[1]}s{sec}",
            "section_number": sec,
            "title_number": raw["title_number"],
            "title_name": raw["title_name"],
            "chapter": raw["chapter"],
        }

    def test_api(self):
        """Test connectivity to archive.org."""
        logger.info("Testing archive.org access for TN Code...")
        try:
            html = self._get_html(self._title_url(1))
            if html and len(html) > 1000:
                soup = BeautifulSoup(html, "html.parser")
                sections = soup.find_all("h3")
                logger.info(f"  Title 1 downloaded: {len(html)} bytes, {len(sections)} sections")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: empty or short response")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all TCA sections from all titles."""
        total = 0
        for num in TITLE_RANGE:
            url = self._title_url(num)
            html = self._get_html(url)
            if not html or len(html) < 500:
                continue

            title_count = 0
            for raw in self._parse_title(html, num):
                yield raw
                total += 1
                title_count += 1

            if title_count > 0:
                logger.info(f"  Title {num}: {title_count} sections")

        logger.info(f"Total sections fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Static archive — fetch_all is the only option."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch sample sections from a few titles."""
        logger.info("Fetching sample sections...")
        count = 0
        sample_titles = [1, 4, 29, 39, 55]  # General, Admin, Taxation, Criminal, Property

        for num in sample_titles:
            if count >= 15:
                break
            url = self._title_url(num)
            html = self._get_html(url)
            if not html or len(html) < 500:
                continue

            for raw in self._parse_title(html, num):
                if count >= 15:
                    break
                yield raw
                count += 1

        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TN-Legislation bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TNLegislationScraper()

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
            safe_id = record["_id"].replace("/", "_")
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} - {record['title'][:60]} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
