#!/usr/bin/env python3
"""
US/MD-Legislation -- Maryland Code and Statutes

Fetches codified statutes from mgaleg.maryland.gov using the official JSON APIs
for article/section discovery and HTML statute text pages for full text.

APIs used:
  - /api/Laws/GetArticles — list all article codes
  - /api/Laws/GetSections?articleCode=X — list all sections in an article
  - /Laws/StatuteText?article=X&section=Y — HTML page with statute text

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all articles)
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
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.MD-Legislation")

BASE_URL = "https://mgaleg.maryland.gov/mgawebsite"
ARTICLES_API = f"{BASE_URL}/api/Laws/GetArticles"
SECTIONS_API = f"{BASE_URL}/api/Laws/GetSections"
STATUTE_URL = f"{BASE_URL}/Laws/StatuteText"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# Sample articles covering diverse areas of Maryland law
SAMPLE_ARTICLES = [
    ("gcr", "1-101"),   # Criminal Law - Definitions
    ("gcr", "2-201"),   # Criminal Law - Murder
    ("gfl", "1-101"),   # Family Law - Definitions
    ("gtg", "1-101"),   # Tax General - Definitions
    ("gen", "1-101"),   # Environment - Definitions
    ("ged", "1-101"),   # Education - Definitions
    ("gtr", "1-101"),   # Transportation - Definitions
    ("gbr", "1-101"),   # Business Regulation - Definitions
    ("gcj", "1-101"),   # Courts and Judicial Proceedings
    ("gps", "1-101"),   # Public Safety
    ("gin", "1-101"),   # Insurance
    ("ghg", "1-101"),   # Health General
    ("grp", "1-101"),   # Real Property
    ("gcs", "1-101"),   # Correctional Services
    ("gel", "1-101"),   # Election Law
]


class _HTMLTextExtractor(HTMLParser):
    """Extract text from HTML, stripping all tags."""

    def __init__(self):
        super().__init__()
        self._pieces = []

    def handle_data(self, data):
        self._pieces.append(data)

    def handle_entityref(self, name):
        entities = {"sect": "\u00a7", "nbsp": " ", "ndash": "\u2013",
                     "mdash": "\u2014", "amp": "&", "lt": "<", "gt": ">"}
        self._pieces.append(entities.get(name, f"&{name};"))

    def get_text(self):
        return "".join(self._pieces)


def strip_html(html: str) -> str:
    extractor = _HTMLTextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class MDLegislationScraper(BaseScraper):
    """
    Scraper for US/MD-Legislation — Maryland Code and Statutes.
    Uses mgaleg.maryland.gov JSON APIs for discovery and HTML pages for text.
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def _get_articles(self) -> List[Dict[str, str]]:
        """Get list of all article codes via API."""
        for attempt in range(3):
            try:
                resp = self.session.get(ARTICLES_API, params={"enactments": "false"}, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                # Deduplicate by Value
                seen = set()
                articles = []
                for item in data:
                    val = item.get("Value", "")
                    if val and val not in seen:
                        seen.add(val)
                        articles.append({
                            "code": val,
                            "name": item.get("DisplayText", ""),
                        })
                logger.info(f"Found {len(articles)} articles")
                return articles
            except Exception as e:
                logger.warning(f"GetArticles attempt {attempt+1} failed: {e}")
                time.sleep(2)
        return []

    def _get_sections(self, article_code: str) -> List[Dict[str, str]]:
        """Get list of all sections for an article."""
        for attempt in range(3):
            try:
                resp = self.session.get(
                    SECTIONS_API,
                    params={"articleCode": article_code, "enactments": "false"},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                sections = []
                for item in data:
                    sections.append({
                        "display": item.get("DisplayText", ""),
                        "value": item.get("Value", ""),
                    })
                return sections
            except Exception as e:
                logger.warning(f"GetSections({article_code}) attempt {attempt+1} failed: {e}")
                time.sleep(2)
        return []

    def _fetch_statute_text(self, article_code: str, section: str) -> Optional[str]:
        """Fetch full text of a statute section from the HTML page."""
        url = STATUTE_URL
        params = {
            "article": article_code,
            "section": section,
            "enactments": "false",
            "archived": "false",
        }
        for attempt in range(3):
            try:
                self.session.headers["Accept"] = "text/html"
                resp = self.session.get(url, params=params, timeout=30)
                self.session.headers["Accept"] = "application/json"
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp.text
            except requests.exceptions.Timeout:
                if attempt < 2:
                    time.sleep(2)
                    continue
                return None
            except Exception as e:
                logger.warning(f"Fetch {article_code}/{section} attempt {attempt+1}: {e}")
                if attempt < 2:
                    time.sleep(2)
                    continue
                return None
        return None

    def _extract_text_from_html(self, html: str) -> str:
        """Extract statute text from the mainBody div."""
        # Find mainBody content
        idx = html.find('id="mainBody"')
        if idx < 0:
            return ""

        # Get content between mainBody and the footer/nav sections
        chunk = html[idx:]
        # Find the statute text section - content between the header and footer
        # The pattern: after "Statutes Text" and article name, the actual text is
        # between navigation buttons and the footer links

        # Extract content from mainBody, strip navigation and footer
        text = re.sub(r'<[^>]+>', '\n', chunk)
        text = re.sub(r'&sect;', '\u00a7', text)
        text = re.sub(r'&ndash;', '\u2013', text)
        text = re.sub(r'&mdash;', '\u2014', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&#39;', "'", text)
        text = re.sub(r'&quot;', '"', text)

        lines = [l.strip() for l in text.split('\n') if l.strip()]

        # Find the actual statute content - skip header lines and stop at footer
        content_lines = []
        started = False
        for line in lines:
            # Skip navigation and header items
            if line in ('Statutes Text', 'Previous', 'Next', 'Validation',
                        'Please fix the following:', 'OK', 'Success', 'Okay'):
                if started:
                    break  # Hit the bottom nav
                continue
            if line.startswith('Article -') or line.startswith('Article\u2013'):
                continue
            if 'Helpful Links' in line or 'Executive Branch' in line:
                break
            if line.startswith('\u00a7') or (not started and re.match(r'\d+-\d+', line)):
                started = True
            if started:
                content_lines.append(line)

        result = '\n'.join(content_lines)
        result = re.sub(r'\n{3,}', '\n\n', result)
        return result.strip()

    def _process_section(self, article_code: str, article_name: str,
                         section_display: str) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single statute section."""
        html = self._fetch_statute_text(article_code, section_display)
        if not html:
            return None

        text = self._extract_text_from_html(html)
        if not text or len(text) < 10:
            logger.warning(f"No text for {article_code}/{section_display}")
            return None

        return {
            "article_code": article_code,
            "article_name": article_name,
            "section_display": section_display,
            "text": text,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Maryland Code sections."""
        articles = self._get_articles()
        if not articles:
            logger.error("No articles found")
            return

        total = 0
        for art_idx, article in enumerate(articles):
            code = article["code"]
            name = article["name"]
            logger.info(f"Article {art_idx+1}/{len(articles)}: {name} ({code})")

            sections = self._get_sections(code)
            if not sections:
                logger.warning(f"No sections for {code}")
                continue

            logger.info(f"  {len(sections)} sections in {code}")

            for sec in sections:
                delay = self.config.get("fetch", {}).get("delay", 1.5)
                time.sleep(delay)

                raw = self._process_section(code, name, sec["display"])
                if raw:
                    total += 1
                    yield raw

            logger.info(f"Progress: {total} records total after {code}")

        logger.info(f"Total fetched: {total}")

    def fetch_sample(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch a representative sample of sections."""
        articles = self._get_articles()
        article_map = {a["code"]: a["name"] for a in articles}

        for art_code, sec_display in SAMPLE_ARTICLES:
            delay = self.config.get("fetch", {}).get("delay", 1.5)
            time.sleep(delay)

            art_name = article_map.get(art_code, art_code)
            raw = self._process_section(art_code, art_name, sec_display)
            if raw:
                yield raw

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all sections (statutes page has no incremental API)."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw section record into the standard schema."""
        article_code = raw.get("article_code", "")
        section_display = raw.get("section_display", "")
        article_name = raw.get("article_name", "")

        # Clean article name: remove " - (code)" suffix
        clean_name = re.sub(r'\s*-\s*\(\w+\)\s*$', '', article_name).strip()

        doc_id = f"US-MD-{article_code}-{section_display}"

        title = f"{clean_name} \u00a7{section_display}"

        url = (f"https://mgaleg.maryland.gov/mgawebsite/Laws/StatuteText"
               f"?article={article_code}&section={section_display}&enactments=false")

        return {
            "_id": doc_id,
            "_source": "US/MD-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": None,  # Codified statutes, no per-section date
            "url": url,
            "article_code": article_code,
            "article_name": clean_name,
            "section_number": section_display,
            "jurisdiction": "US-MD",
        }

    def test_connection(self) -> bool:
        """Test connectivity to mgaleg.maryland.gov APIs."""
        try:
            articles = self._get_articles()
            if not articles:
                logger.error("No articles returned")
                return False
            logger.info(f"Got {len(articles)} articles")

            # Test section listing
            sections = self._get_sections(articles[0]["code"])
            if not sections:
                logger.error("No sections returned")
                return False
            logger.info(f"Got {len(sections)} sections for {articles[0]['code']}")

            # Test text fetch
            raw = self._process_section(
                articles[0]["code"],
                articles[0]["name"],
                sections[0]["display"],
            )
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

    parser = argparse.ArgumentParser(description="US/MD-Legislation data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--since", help="ISO date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = MDLegislationScraper()

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
