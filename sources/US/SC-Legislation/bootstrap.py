#!/usr/bin/env python3
"""
US/SC-Legislation -- South Carolina Code of Laws

Fetches full text of SC statutes from scstatehouse.gov.

Strategy:
  - Scrape title index (/code/statmast.php) for all 63 titles
  - Scrape each title page (/code/titleN.php) for chapter links
  - Fetch each chapter page (/code/tNNcNNN.php) for full text HTML
  - Parse sections from chapter HTML using SECTION heading pattern

Data Coverage:
  - All 63 titles of the South Carolina Code of Laws
  - Full text of every section
  - Language: English
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Same as bootstrap (statutes are current)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.SC-Legislation")

BASE_URL = "https://www.scstatehouse.gov"

# Regex to find chapter links on title pages
CHAPTER_LINK_PATTERN = re.compile(
    r'href="(/code/t\d+c\d+\.php)"', re.IGNORECASE
)

# Regex to find title links on the index page
TITLE_LINK_PATTERN = re.compile(
    r'href="(/code/title(\d+)\.php)"', re.IGNORECASE
)

# Regex to split sections from chapter HTML
# Sections start with "SECTION X-Y-Z." pattern in bold (span or strong)
SECTION_HEADING_PATTERN = re.compile(
    r'<(?:strong|span\b[^>]*)>\s*(SECTION\s+([\d]+-[\d]+-[\d]+))\.\s*</(?:strong|span)>\s*([^<\n]*)',
    re.IGNORECASE,
)


class HTMLTextExtractor(HTMLParser):
    """Simple HTML tag stripper."""

    def __init__(self):
        super().__init__()
        self.result = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self._skip = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self._skip = False
        if tag in ("p", "br", "div", "li"):
            self.result.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.result.append(data)

    def get_text(self):
        return "".join(self.result)


def strip_html(html: str) -> str:
    """Strip HTML tags and return clean text."""
    parser = HTMLTextExtractor()
    parser.feed(html)
    text = parser.get_text()
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_title_name(html: str) -> str:
    """Extract the title name from a chapter page."""
    m = re.search(r'<title>([^<]+)</title>', html, re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""


def parse_sections_from_chapter(html: str) -> List[Dict[str, Any]]:
    """Parse individual sections from a chapter HTML page."""
    sections = []

    # Find all section headings
    headings = list(SECTION_HEADING_PATTERN.finditer(html))
    if not headings:
        return sections

    for i, heading in enumerate(headings):
        section_number = heading.group(2)
        section_title = heading.group(3).strip().rstrip('.')

        # Extract body from this heading to the next heading
        start = heading.end()
        end = headings[i + 1].start() if i + 1 < len(headings) else len(html)
        body_html = html[start:end]

        # Strip HTML tags
        body_text = strip_html(body_html)

        # Clean up HISTORY lines (keep them as metadata)
        body_text = body_text.strip()

        if not body_text:
            continue

        sections.append({
            "section_number": section_number,
            "section_title": section_title,
            "text": body_text,
        })

    return sections


class SCLegislationScraper(BaseScraper):
    """Scraper for US/SC-Legislation -- South Carolina Code of Laws."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)

    def _curl_get(self, url: str, timeout: int = 30) -> str:
        """Fetch a URL using curl (needed because Python 3.9 + LibreSSL 2.8
        doesn't support TLS 1.3 required by scstatehouse.gov/Cloudflare)."""
        result = subprocess.run(
            ["curl", "-sS", "-L", "--max-time", str(timeout),
             "-A", "LegalDataHunter/1.0 (legal research; open data collection)",
             url],
            capture_output=True, text=True, timeout=timeout + 10,
        )
        if result.returncode != 0:
            raise RuntimeError(f"curl failed for {url}: {result.stderr.strip()}")
        return result.stdout

    def _get_titles(self) -> List[Dict[str, str]]:
        """Fetch list of all titles from the index page."""
        url = f"{BASE_URL}/code/statmast.php"
        html = self._curl_get(url)

        titles = []
        for m in TITLE_LINK_PATTERN.finditer(html):
            path = m.group(1)
            title_num = m.group(2)
            titles.append({
                "number": title_num,
                "url": f"{BASE_URL}{path}",
            })

        # Deduplicate
        seen = set()
        unique = []
        for t in titles:
            if t["number"] not in seen:
                seen.add(t["number"])
                unique.append(t)

        return unique

    def _get_chapters(self, title_url: str) -> List[str]:
        """Fetch list of chapter URLs from a title page."""
        try:
            html = self._curl_get(title_url)
        except Exception as e:
            logger.warning(f"Failed to fetch title page {title_url}: {e}")
            return []

        chapters = []
        seen = set()
        for m in CHAPTER_LINK_PATTERN.finditer(html):
            path = m.group(1)
            if path not in seen:
                seen.add(path)
                chapters.append(f"{BASE_URL}{path}")

        return chapters

    def _fetch_chapter(self, chapter_url: str) -> str:
        """Fetch a chapter page HTML."""
        try:
            return self._curl_get(chapter_url)
        except Exception as e:
            logger.warning(f"Failed to fetch chapter {chapter_url}: {e}")
            return ""

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all sections from all titles and chapters."""
        titles = self._get_titles()
        logger.info(f"Found {len(titles)} titles")

        for title in titles:
            title_num = title["number"]
            logger.info(f"Processing Title {title_num}...")
            time.sleep(self.config.get("fetch", {}).get("delay", 1.5))

            chapters = self._get_chapters(title["url"])
            logger.info(f"  Title {title_num}: {len(chapters)} chapters")

            for chapter_url in chapters:
                time.sleep(self.config.get("fetch", {}).get("delay", 1.5))

                html = self._fetch_chapter(chapter_url)
                if not html:
                    continue

                # Extract chapter info from URL
                chapter_match = re.search(r't(\d+)c(\d+)\.php', chapter_url)
                chapter_id = ""
                if chapter_match:
                    chapter_id = f"T{chapter_match.group(1)}-C{chapter_match.group(2)}"

                page_title = extract_title_name(html)
                sections = parse_sections_from_chapter(html)

                if not sections:
                    logger.debug(f"No sections found in {chapter_url}")
                    continue

                logger.info(f"  {chapter_id}: {len(sections)} sections")

                for section in sections:
                    section["chapter_url"] = chapter_url
                    section["chapter_id"] = chapter_id
                    section["page_title"] = page_title
                    section["title_number"] = title_num
                    yield section

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Statutes are current — same as full fetch."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw section into the standard schema."""
        section_num = raw.get("section_number", "")
        section_title = raw.get("section_title", "")
        doc_id = f"US-SC-{section_num}"

        display_title = f"S.C. Code {section_num}"
        if section_title:
            display_title += f" - {section_title}"

        return {
            "_id": doc_id,
            "_source": "US/SC-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": display_title,
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("chapter_url", ""),
            "section_number": section_num,
            "chapter": raw.get("chapter_id", ""),
            "jurisdiction": "US-SC",
        }

    def test_connection(self) -> bool:
        """Test that the code index is accessible."""
        try:
            titles = self._get_titles()
            logger.info(f"Connection test: found {len(titles)} titles")
            if not titles:
                return False
            chapters = self._get_chapters(titles[0]["url"])
            logger.info(f"Connection test: Title {titles[0]['number']} has {len(chapters)} chapters")
            return len(chapters) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/SC-Legislation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch small sample")
    parser.add_argument("--since", help="ISO date for incremental updates")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SCLegislationScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        count = 0
        target = 15 if args.sample else 999999

        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record['_id']}: {record['title'][:60]} "
                f"({text_len} chars)"
            )
            count += 1
            if count >= target:
                break

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
