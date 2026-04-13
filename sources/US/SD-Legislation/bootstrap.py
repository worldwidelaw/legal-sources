#!/usr/bin/env python3
"""
US/SD-Legislation -- South Dakota Codified Laws

Fetches full text of SD statutes from sdlegislature.gov JSON API.

Strategy:
  - GET /api/Statutes/Title → list of all 71 titles
  - For each title, GET /api/Statutes/Statute/{id} → navigate via Next field
  - For each section, extract full text from Html field
  - Navigate through entire statute tree using Next links

Data Coverage:
  - All 71 titles of the South Dakota Codified Laws
  - Full text of every section
  - Language: English
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Same as bootstrap
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.SD-Legislation")

BASE_URL = "https://sdlegislature.gov"
API_BASE = f"{BASE_URL}/api/Statutes"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"


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
        if tag in ("p", "br", "div", "li", "tr"):
            self.result.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self.result.append(data)

    def get_text(self):
        return "".join(self.result)


def strip_html(html: str) -> str:
    """Strip HTML tags and return clean text."""
    if not html:
        return ""
    parser = HTMLTextExtractor()
    parser.feed(html)
    text = parser.get_text()
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Clean up excessive whitespace
    text = re.sub(r'[ \t]+\n', '\n', text)
    text = re.sub(r'\xa0+', ' ', text)
    return text.strip()


class SDLegislationScraper(BaseScraper):
    """Scraper for US/SD-Legislation -- South Dakota Codified Laws."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)

    def _api_get_json(self, url: str, timeout: int = 30) -> Any:
        """Fetch a JSON API endpoint."""
        req = Request(url, headers={"User-Agent": USER_AGENT})
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = data.decode("utf-16")
        return json.loads(text)

    def _api_get_html(self, url: str, timeout: int = 60) -> str:
        """Fetch an HTML endpoint."""
        req = Request(url, headers={"User-Agent": USER_AGENT})
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        try:
            return data.decode("utf-8")
        except UnicodeDecodeError:
            return data.decode("utf-16")

    def _get_titles(self) -> List[Dict[str, Any]]:
        """Fetch list of all titles."""
        url = f"{API_BASE}/Title"
        titles = self._api_get_json(url)
        logger.info(f"Found {len(titles)} titles")
        return titles

    def _get_statute(self, identifier: str) -> Dict[str, Any]:
        """Fetch a statute by identifier."""
        url = f"{API_BASE}/Statute/{identifier}"
        return self._api_get_json(url)

    def _extract_chapter_ids(self, title_html: str, title_id: str) -> List[str]:
        """Extract chapter identifiers from title HTML."""
        pattern = re.compile(
            r'Statutes?\??\s*(?:=|/)(' + re.escape(title_id) + r'-\d+[A-Z]?)\b',
            re.IGNORECASE,
        )
        seen = set()
        result = []
        for m in pattern.finditer(title_html):
            cid = m.group(1)
            if cid not in seen:
                seen.add(cid)
                result.append(cid)
        return result

    def _extract_section_ids(self, chapter_html: str, chapter_id: str) -> List[str]:
        """Extract section identifiers from chapter HTML."""
        pattern = re.compile(
            r'Statutes?\??\s*(?:=|/)(' + re.escape(chapter_id) + r'-[\d.]+)\b',
            re.IGNORECASE,
        )
        seen = set()
        result = []
        for m in pattern.finditer(chapter_html):
            sid = m.group(1)
            if sid not in seen:
                seen.add(sid)
                result.append(sid)
        return result

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all sections from all titles."""
        titles = self._get_titles()
        delay = self.config.get("fetch", {}).get("delay", 1.0)

        for title in titles:
            title_id = title.get("Statute", "")
            title_catchline = title.get("CatchLine", "")
            if not title_id:
                continue

            logger.info(f"Processing Title {title_id} ({title_catchline})...")
            time.sleep(delay)

            # Get title detail to find chapters
            try:
                title_data = self._get_statute(title_id)
            except Exception as e:
                logger.warning(f"Failed to fetch title {title_id}: {e}")
                continue

            title_html = title_data.get("Html", "")
            chapter_ids = self._extract_chapter_ids(title_html, title_id)

            if not chapter_ids:
                logger.warning(f"  No chapters found for title {title_id}")
                continue

            logger.info(f"  Title {title_id}: {len(chapter_ids)} chapters")

            for chapter_id in chapter_ids:
                time.sleep(delay)

                try:
                    chapter_data = self._get_statute(chapter_id)
                except Exception as e:
                    logger.warning(f"  Failed to fetch chapter {chapter_id}: {e}")
                    continue

                chapter_catchline = chapter_data.get("CatchLine", "")
                chapter_html = chapter_data.get("Html", "")
                section_ids = self._extract_section_ids(chapter_html, chapter_id)

                if not section_ids:
                    logger.debug(f"  No sections in chapter {chapter_id}")
                    continue

                logger.info(f"  Chapter {chapter_id} ({chapter_catchline}): {len(section_ids)} sections")

                for section_id in section_ids:
                    time.sleep(delay)

                    try:
                        sec_data = self._get_statute(section_id)
                    except Exception as e:
                        logger.warning(f"    Failed to fetch section {section_id}: {e}")
                        continue

                    sec_html = sec_data.get("Html", "")
                    sec_catchline = sec_data.get("CatchLine", "")
                    sec_repealed = sec_data.get("Repealed", False)

                    text = strip_html(sec_html)
                    if not text and not sec_repealed:
                        continue

                    yield {
                        "section_id": section_id,
                        "section_catchline": sec_catchline,
                        "title_id": title_id,
                        "title_catchline": title_catchline,
                        "chapter_id": chapter_id,
                        "chapter_catchline": chapter_catchline,
                        "text": text,
                        "repealed": sec_repealed,
                        "url": f"{BASE_URL}/Statutes/{section_id}",
                    }

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Statutes are current — same as full fetch."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw section into the standard schema."""
        section_id = raw.get("section_id", "")
        section_catchline = raw.get("section_catchline", "")
        doc_id = f"US-SD-{section_id}"

        display_title = f"S.D. Codified Laws {section_id}"
        if section_catchline:
            display_title += f" - {section_catchline}"

        return {
            "_id": doc_id,
            "_source": "US/SD-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": display_title,
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("url", ""),
            "section_number": section_id,
            "chapter": raw.get("chapter_id", ""),
            "jurisdiction": "US-SD",
            "repealed": raw.get("repealed", False),
        }

    def test_connection(self) -> bool:
        """Test that the API is accessible."""
        try:
            titles = self._get_titles()
            if not titles:
                return False
            first = titles[0].get("Statute", "1")
            data = self._get_statute(first)
            logger.info(f"Connection test: Title {first} = {data.get('CatchLine', '?')}")
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/SD-Legislation data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch small sample")
    parser.add_argument("--since", help="ISO date for incremental updates")
    args = parser.parse_args()

    scraper = SDLegislationScraper()

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
