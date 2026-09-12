#!/usr/bin/env python3
"""
EU/GDPRhub -- GDPR DPA Decisions Wiki Fetcher

Fetches Data Protection Authority decisions from GDPRhub.eu, a noyb.eu
initiative collecting GDPR decisions from 30+ countries in English.

Strategy:
  - Use MediaWiki API embeddedin query to enumerate all pages using
    the DPAdecisionBOX template (~3,200 pages)
  - Batch-fetch wikitext content (50 pages per request)
  - Parse structured metadata from DPAdecisionBOX template parameters
  - Extract full text from English Summary and Machine Translation sections
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
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

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.GDPRhub")

API_URL = "https://gdprhub.eu/api.php"
TEMPLATE_NAME = "Template:DPAdecisionBOX"
BATCH_SIZE = 50  # pages per content request


class GDPRhubScraper(BaseScraper):
    """Scraper for EU/GDPRhub -- GDPR DPA decisions wiki."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter; open-data collection)",
            "Accept": "application/json",
        })

    def _api_get(self, params: dict, timeout: int = 60) -> Optional[dict]:
        """Call MediaWiki API with retry."""
        params["format"] = "json"
        for attempt in range(3):
            try:
                time.sleep(1)
                resp = self.session.get(API_URL, params=params, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _enumerate_pages(self) -> List[Dict[str, Any]]:
        """List all pages that transclude DPAdecisionBOX template."""
        pages = []
        params = {
            "action": "query",
            "list": "embeddedin",
            "eititle": TEMPLATE_NAME,
            "eilimit": "500",
        }
        while True:
            data = self._api_get(params)
            if not data:
                break
            for p in data.get("query", {}).get("embeddedin", []):
                pages.append({"pageid": p["pageid"], "title": p["title"]})
            cont = data.get("continue")
            if cont:
                params.update(cont)
            else:
                break
        logger.info(f"Enumerated {len(pages)} decision pages")
        return pages

    def _fetch_page_content(self, titles: List[str]) -> Dict[int, Dict[str, str]]:
        """Batch-fetch wikitext content for multiple pages."""
        result = {}
        params = {
            "action": "query",
            "titles": "|".join(titles),
            "prop": "revisions|info",
            "rvprop": "content",
            "rvslots": "main",
        }
        data = self._api_get(params)
        if not data:
            return result
        for pid_str, page in data.get("query", {}).get("pages", {}).items():
            pid = int(pid_str)
            if pid < 0:
                continue
            revs = page.get("revisions")
            if not revs:
                continue
            content = revs[0].get("slots", {}).get("main", {}).get("*", "")
            result[pid] = {
                "title": page.get("title", ""),
                "wikitext": content,
                "touched": page.get("touched", ""),
            }
        return result

    def _parse_template_params(self, wikitext: str) -> Dict[str, str]:
        """Extract DPAdecisionBOX template parameters from wikitext."""
        params = {}
        # Find the template block using brace-counting for nested templates
        idx = wikitext.find("{{DPAdecisionBOX")
        if idx < 0:
            return params
        level = 0
        end_idx = idx
        while end_idx < len(wikitext):
            if wikitext[end_idx:end_idx + 2] == "{{":
                level += 1
                end_idx += 2
            elif wikitext[end_idx:end_idx + 2] == "}}":
                level -= 1
                if level == 0:
                    end_idx += 2
                    break
                end_idx += 2
            else:
                end_idx += 1
        block = wikitext[idx:end_idx]
        # Parse key=value pairs line by line
        for line in block.split("\n"):
            line = line.strip()
            if not line.startswith("|"):
                continue
            line = line[1:]  # remove leading |
            eq = line.find("=")
            if eq < 0:
                continue
            key = line[:eq].strip()
            val = line[eq + 1:].strip()
            if key and val:
                params[key] = val
        return params

    def _extract_sections(self, wikitext: str) -> Dict[str, str]:
        """Extract named sections from wikitext."""
        sections = {}
        # Split on == Header == patterns
        parts = re.split(r'^(={2,})\s*(.+?)\s*\1\s*$', wikitext, flags=re.MULTILINE)
        # parts: [before, level, title, after, level, title, after, ...]
        i = 0
        while i < len(parts):
            if i + 3 < len(parts) and parts[i + 1] in ('==', '===', '===='):
                title = parts[i + 2].strip()
                body = parts[i + 3] if i + 3 < len(parts) else ""
                sections[title] = body.strip()
                i += 3
            else:
                i += 1
        return sections

    def _clean_wikitext(self, text: str) -> str:
        """Remove wikitext markup from text."""
        # Remove templates
        text = re.sub(r'\{\{[^}]*\}\}', '', text)
        # Remove wiki links, keep display text
        text = re.sub(r'\[\[(?:[^|\]]*\|)?([^\]]+)\]\]', r'\1', text)
        # Remove external links, keep display text
        text = re.sub(r'\[https?://[^\s\]]+ ([^\]]+)\]', r'\1', text)
        text = re.sub(r'\[https?://[^\]]+\]', '', text)
        # Remove bold/italic markup
        text = re.sub(r"'{2,5}", '', text)
        # Remove HTML tags
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL)
        text = re.sub(r'<ref[^/]*/>', '', text)
        text = re.sub(r'</?(?:pre|nowiki|br|div|span|small|big|blockquote)[^>]*>', '', text, flags=re.IGNORECASE)
        # Remove category links
        text = re.sub(r'\[\[Category:[^\]]+\]\]', '', text)
        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _parse_date(self, date_str: str) -> str:
        """Parse DD.MM.YYYY or similar to ISO format."""
        if not date_str:
            return ""
        date_str = date_str.strip()
        for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %B %Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return ""

    def _extract_gdpr_articles(self, params: Dict[str, str]) -> List[str]:
        """Extract all GDPR article references from template params."""
        articles = []
        for i in range(1, 21):
            key = f"GDPR_Article_{i}"
            val = params.get(key, "")
            if val:
                articles.append(val)
        return articles

    def _process_page(self, pageid: int, title: str, wikitext: str) -> Optional[Dict[str, Any]]:
        """Process a single page's wikitext into a raw record."""
        tpl = self._parse_template_params(wikitext)
        sections = self._extract_sections(wikitext)

        # Build text from multiple sections
        text_parts = []

        # English Summary (Facts + Holding)
        summary = sections.get("English Summary", "")
        if summary:
            text_parts.append(self._clean_wikitext(summary))

        # Facts and Holding subsections
        facts = sections.get("Facts", "")
        if facts:
            text_parts.append("Facts:\n" + self._clean_wikitext(facts))
        holding = sections.get("Holding", "")
        if holding:
            text_parts.append("Holding:\n" + self._clean_wikitext(holding))

        # Comment section
        comment = sections.get("Comment", "")
        if comment:
            text_parts.append("Comment:\n" + self._clean_wikitext(comment))

        # English Machine Translation (full decision text)
        translation = sections.get("English Machine Translation of the Decision", "")
        if translation:
            text_parts.append("Full Decision (Machine Translation):\n" + self._clean_wikitext(translation))

        # Also check for non-standard translation section names
        for sec_name, sec_text in sections.items():
            if "translation" in sec_name.lower() and sec_name != "English Machine Translation of the Decision":
                text_parts.append(f"{sec_name}:\n" + self._clean_wikitext(sec_text))

        full_text = "\n\n".join(t for t in text_parts if t and len(t) > 20)

        if not full_text or len(full_text) < 50:
            logger.debug(f"Skipping {title}: insufficient text ({len(full_text)} chars)")
            return None

        # Parse fine amount
        fine_str = tpl.get("Fine", "")
        fine = None
        if fine_str:
            try:
                fine = float(re.sub(r'[^\d.]', '', fine_str))
            except (ValueError, TypeError):
                fine = None

        # Original source links
        original_url = tpl.get("Original_Source_Link_1", "")
        original_lang = tpl.get("Original_Source_Language_1", "")

        return {
            "page_id": pageid,
            "title": title,
            "text": full_text,
            "date": self._parse_date(tpl.get("Date_Decided", "")),
            "date_published": self._parse_date(tpl.get("Date_Published", "")),
            "url": f"https://gdprhub.eu/index.php?title={title.replace(' ', '_')}",
            "jurisdiction": tpl.get("Jurisdiction", ""),
            "dpa": tpl.get("DPA_Abbrevation", ""),
            "dpa_full": tpl.get("DPA_With_Country", ""),
            "case_number": tpl.get("Case_Number_Name", ""),
            "outcome": tpl.get("Outcome", ""),
            "decision_type": tpl.get("Type", ""),
            "fine": fine,
            "currency": tpl.get("Currency", ""),
            "gdpr_articles": self._extract_gdpr_articles(tpl),
            "original_url": original_url,
            "original_language": original_lang,
            "year": tpl.get("Year", ""),
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": f"gdprhub-{raw['page_id']}",
            "_source": "EU/GDPRhub",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "page_id": raw.get("page_id"),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "date_published": raw.get("date_published", ""),
            "url": raw.get("url", ""),
            "jurisdiction": raw.get("jurisdiction", ""),
            "dpa": raw.get("dpa", ""),
            "dpa_full": raw.get("dpa_full", ""),
            "case_number": raw.get("case_number", ""),
            "outcome": raw.get("outcome", ""),
            "decision_type": raw.get("decision_type", ""),
            "fine": raw.get("fine"),
            "currency": raw.get("currency", ""),
            "gdpr_articles": raw.get("gdpr_articles", []),
            "original_url": raw.get("original_url", ""),
            "original_language": raw.get("original_language", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all GDPRhub decision pages."""
        pages = self._enumerate_pages()
        count = 0
        skipped = 0

        # Process in batches
        for i in range(0, len(pages), BATCH_SIZE):
            batch = pages[i:i + BATCH_SIZE]
            titles = [p["title"] for p in batch]
            id_map = {p["title"]: p["pageid"] for p in batch}

            contents = self._fetch_page_content(titles)
            if not contents:
                logger.warning(f"Failed to fetch batch {i//BATCH_SIZE + 1}")
                continue

            for pid, data in contents.items():
                record = self._process_page(pid, data["title"], data["wikitext"])
                if record:
                    count += 1
                    yield record
                else:
                    skipped += 1

            logger.info(f"Batch {i//BATCH_SIZE + 1}: {len(contents)} pages processed, {count} total records")

        logger.info(f"Completed: {count} records fetched, {skipped} skipped (no text)")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent changes to decision pages."""
        params = {
            "action": "query",
            "list": "recentchanges",
            "rcnamespace": "0",
            "rclimit": "100",
            "rcprop": "title|ids|timestamp",
            "rctype": "edit|new",
        }
        data = self._api_get(params)
        if not data:
            return

        titles = []
        for rc in data.get("query", {}).get("recentchanges", []):
            titles.append(rc["title"])

        if not titles:
            return

        # Deduplicate
        titles = list(dict.fromkeys(titles))

        for i in range(0, len(titles), BATCH_SIZE):
            batch = titles[i:i + BATCH_SIZE]
            contents = self._fetch_page_content(batch)
            if not contents:
                continue
            for pid, page_data in contents.items():
                record = self._process_page(pid, page_data["title"], page_data["wikitext"])
                if record:
                    yield record

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test API
        data = self._api_get({
            "action": "query",
            "list": "embeddedin",
            "eititle": TEMPLATE_NAME,
            "eilimit": "5",
        })
        if not data:
            logger.error("Cannot reach GDPRhub API")
            return False

        pages = data.get("query", {}).get("embeddedin", [])
        if not pages:
            logger.error("No decision pages found")
            return False

        logger.info(f"API OK: found {len(pages)} pages in test query")

        # Test content fetch
        title = pages[0]["title"]
        content = self._fetch_page_content([title])
        if not content:
            logger.error("Cannot fetch page content")
            return False

        pid = list(content.keys())[0]
        record = self._process_page(pid, content[pid]["title"], content[pid]["wikitext"])
        if record:
            logger.info(f"Content OK: {record['title'][:60]} ({len(record['text'])} chars)")
            return True

        logger.error("Failed to parse page content")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="EU/GDPRhub data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GDPRhubScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
