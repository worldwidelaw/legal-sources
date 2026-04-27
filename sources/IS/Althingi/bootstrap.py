#!/usr/bin/env python3
"""
IS/Althingi — Althingi Lagasafn (Icelandic Law Collection)

Fetches the official consolidated Icelandic law collection from Althingi.

Strategy:
  - Parse the Lagasafn index page to discover all law URLs
  - Fetch each law's HTML page and extract full text
  - ~900 laws covering Iceland's entire statutory framework

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update --since 2024-01-01
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as htmlmod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IS.Althingi")

BASE_URL = "https://www.althingi.is"
INDEX_URL = f"{BASE_URL}/lagasafn/"
EDITION = "157a"  # Current edition


class AlthingiScraper(BaseScraper):
    """
    Scraper for IS/Althingi — Icelandic Law Collection.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "is,en;q=0.5",
        })

    def _discover_laws(self) -> list[dict]:
        """Discover all law URLs from the Lagasafn index page."""
        logger.info(f"Fetching index: {INDEX_URL}")
        resp = self.session.get(INDEX_URL, timeout=30)
        resp.raise_for_status()

        laws = []
        seen = set()

        # Extract law links: href="/lagas/157a/YYYYNNN.html"
        pattern = re.compile(
            r'href="/lagas/' + re.escape(EDITION) + r'/(\d+)\.html"[^>]*>\s*(.*?)\s*</a>',
            re.DOTALL,
        )
        for match in pattern.finditer(resp.text):
            law_id = match.group(1)
            if law_id in seen:
                continue
            seen.add(law_id)

            # Clean link text (law title)
            title = re.sub(r'<[^>]+>', '', match.group(2)).strip()
            title = htmlmod.unescape(title)

            # Parse year and number from the ID (e.g., 1940019 -> year=1940, nr=19)
            if len(law_id) >= 7:
                year = law_id[:4]
                nr = str(int(law_id[4:]))  # Remove leading zeros
            else:
                year = ""
                nr = law_id

            laws.append({
                "law_id": law_id,
                "title": title,
                "year": year,
                "number": nr,
                "url": f"{BASE_URL}/lagas/{EDITION}/{law_id}.html",
            })

        logger.info(f"Discovered {len(laws)} laws")
        return laws

    def _extract_law_text(self, html_content: str) -> tuple[str, str, str]:
        """
        Extract the law text from an Althingi law page.
        Returns (text, title, date_str).
        """
        # The law content is in <div class="article box login"> -> <div class="boxbody">
        # Find the law content section
        match = re.search(
            r'<div class="article\s+box[^"]*">\s*<div class="boxbody">(.*?)(?:</div>\s*</div>)',
            html_content,
            re.DOTALL,
        )
        if not match:
            # Try alternative: find content between Lagasafn header and pgfoot
            match = re.search(
                r'Lagasafn\..*?Útgáfa\s+\w+\.\s*</small>\s*</p>(.*?)(?:<div class="pgfoot"|$)',
                html_content,
                re.DOTALL,
            )

        if not match:
            return "", "", ""

        content_html = match.group(1)

        # Extract title from <h2>
        title_match = re.search(r'<h2>\s*(.*?)\s*</h2>', content_html, re.DOTALL)
        title = ""
        if title_match:
            title = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
            title = htmlmod.unescape(title)

        # Extract date from the line after h2 (e.g., "1940 19 12. febrúar")
        date_str = ""
        date_match = re.search(
            r'<p[^>]*>\s*<strong>\s*(\d{4})\s+\d+\s+(\d+\.\s+\w+)\s*</strong>',
            content_html,
        )
        if date_match:
            date_str = f"{date_match.group(1)}"  # Just the year for now

        # Clean HTML to text
        text = content_html

        # Convert <br> to newlines
        text = re.sub(r'<br\s*/?>', '\n', text)

        # Convert block elements to double newlines
        text = re.sub(r'</(?:p|div|h[1-6]|li|tr)>', '\n\n', text)

        # Strip all remaining HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Decode HTML entities
        text = htmlmod.unescape(text)

        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n+', '\n\n', text)
        text = text.strip()

        return text, title, date_str

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Icelandic laws."""
        laws = self._discover_laws()

        for i, law_info in enumerate(laws):
            url = law_info["url"]
            logger.info(f"[{i+1}/{len(laws)}] Fetching: {law_info['title'][:60]} ({url})")

            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=60)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                    continue

                text, extracted_title, date_str = self._extract_law_text(resp.text)

                if not text or len(text) < 50:
                    logger.warning(f"No/insufficient text for {law_info['law_id']}")
                    continue

                law_info["text"] = text
                if extracted_title:
                    law_info["title"] = extracted_title
                law_info["date_str"] = date_str

                yield law_info

            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch updates — re-fetches all since content is periodically consolidated."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw law data into standardized schema."""
        law_id = raw["law_id"]
        year = raw.get("year", "")
        number = raw.get("number", "")

        # Build law number string
        law_number = f"{number}/{year}" if year and number else law_id

        # Try to parse a date
        date = None
        if year:
            try:
                date = f"{year}-01-01"
            except (ValueError, TypeError):
                pass

        return {
            "_id": f"IS/Althingi/{law_id}",
            "_source": "IS/Althingi",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": date,
            "url": raw["url"],
            "law_number": law_number,
            "law_year": year,
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="IS/Althingi bootstrap")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    boot_parser = subparsers.add_parser("bootstrap", help="Full bootstrap or sample")
    boot_parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot_parser.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    update_parser = subparsers.add_parser("update", help="Incremental update")
    update_parser.add_argument("--since", required=True, help="ISO date (e.g. 2024-01-01)")
    update_parser.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = AlthingiScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        resp = scraper.session.get(INDEX_URL, timeout=15)
        logger.info(f"Index page: HTTP {resp.status_code}, {len(resp.content)} bytes")
        # Test one law
        resp2 = scraper.session.get(f"{BASE_URL}/lagas/{EDITION}/1940019.html", timeout=30)
        logger.info(f"Sample law: HTTP {resp2.status_code}, {len(resp2.content)} bytes")
        logger.info("Connectivity test passed!")

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
