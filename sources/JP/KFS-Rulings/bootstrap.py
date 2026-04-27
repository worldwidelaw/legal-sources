#!/usr/bin/env python3
"""
JP/KFS-Rulings -- Japan National Tax Tribunal Published Rulings

Fetches published rulings (公表裁決事例) from the National Tax Tribunal
(国税不服審判所 / KFS) at kfs.go.jp.

Strategy:
  - Enumerate collection numbers (43-140+) from main index
  - For each collection, find case numbers from the collection index
  - Fetch each case HTML page and extract full text
  - Pages are Shift_JIS encoded

Data:
  - ~700-1000 published tax tribunal rulings
  - Full text in Japanese
  - Covers income tax, corporate tax, consumption tax, etc.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch latest collections
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import html
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JP.KFS-Rulings")

BASE_URL = "https://www.kfs.go.jp"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

# Japanese era to Gregorian year mapping
ERA_MAP = {
    "令和": 2018,   # Reiwa starts 2019 (year 1 = 2019, so base = 2018)
    "平成": 1988,   # Heisei starts 1989
    "昭和": 1925,   # Showa starts 1926
}


def _fetch_url(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch URL and decode from Shift_JIS."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        raw = resp.read()
        # Try Shift_JIS first, then UTF-8
        for enc in ["shift_jis", "cp932", "utf-8"]:
            try:
                return raw.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return raw.decode("utf-8", errors="replace")
    except HTTPError as e:
        if e.code in (404, 410, 403):
            return None
        raise
    except URLError:
        return None


def _strip_html(html_text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<script[^>]*>.*?</script>", "", html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def _parse_japanese_date(title: str) -> Optional[str]:
    """Extract and convert Japanese date from title to ISO 8601."""
    # Match patterns like 令和７年９月26日 or 平成30年3月20日
    m = re.search(r"(令和|平成|昭和)([\d０-９]+)年([\d０-９]+)月([\d０-９]+)日", title)
    if not m:
        return None

    era = m.group(1)
    # Convert full-width to half-width digits
    def fw_to_hw(s):
        return s.translate(str.maketrans("０１２３４５６７８９", "0123456789"))

    era_year = int(fw_to_hw(m.group(2)))
    month = int(fw_to_hw(m.group(3)))
    day = int(fw_to_hw(m.group(4)))

    base = ERA_MAP.get(era, 0)
    if not base:
        return None

    year = base + era_year
    try:
        return f"{year:04d}-{month:02d}-{day:02d}"
    except ValueError:
        return None


class JapanKFSRulingsScraper(BaseScraper):
    """
    Scraper for JP/KFS-Rulings.
    Country: JP
    URL: https://www.kfs.go.jp/service/JP/index.html

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _get_collection_numbers(self) -> List[int]:
        """Get all collection numbers from the main index."""
        url = f"{BASE_URL}/service/JP/index.html"
        page = _fetch_url(url)
        if not page:
            raise RuntimeError("Cannot access KFS main index")

        numbers = re.findall(r"idx/(\d+)\.html", page)
        return sorted(set(int(n) for n in numbers))

    def _get_case_numbers(self, collection: int) -> List[str]:
        """Get case numbers for a given collection."""
        url = f"{BASE_URL}/service/JP/idx/{collection}.html"
        self.rate_limiter.wait()
        page = _fetch_url(url)
        if not page:
            return []

        # Find links like ../140/01/index.html or /{collection}/02/index.html
        pattern = rf"/{collection}/(\d+)/index\.html"
        cases = re.findall(pattern, page)
        return sorted(set(cases))

    def _fetch_ruling(self, collection: int, case_num: str) -> Optional[dict]:
        """Fetch a single ruling."""
        url = f"{BASE_URL}/service/JP/{collection}/{case_num}/index.html"
        self.rate_limiter.wait()

        page = _fetch_url(url)
        if not page or len(page) < 500:
            return None

        # Extract title from <title> tag
        title_match = re.search(r"<title>([^<]+)</title>", page)
        title = html.unescape(title_match.group(1)).strip() if title_match else ""
        # Clean title - remove site suffix
        title = re.sub(r"\s*\|.*$", "", title).strip()

        # Extract main content from saiketsu div or contents div
        content = ""
        for pattern in [
            r'<div id="saiketsu">(.*?)(?:</div>\s*<!--|\Z)',
            r'<div id="contents">(.*?)(?:</div>\s*<div id="footer"|\Z)',
        ]:
            m = re.search(pattern, page, re.DOTALL)
            if m:
                content = m.group(1)
                break

        if not content:
            # Fallback: extract body
            m = re.search(r"<body[^>]*>(.*?)</body>", page, re.DOTALL)
            if m:
                content = m.group(1)

        if not content:
            return None

        # Remove navigation elements
        content = re.sub(r'<div id="(?:header|footer|navi|menu)[^"]*"[^>]*>.*?</div>', '',
                         content, flags=re.DOTALL | re.IGNORECASE)

        text = _strip_html(content)
        if len(text) < 100:
            return None

        # Parse date from title
        date = _parse_japanese_date(title)
        if not date:
            # Try from page content
            date = _parse_japanese_date(text[:200])

        ruling_id = f"KFS-{collection}-{case_num}"

        return {
            "ruling_id": ruling_id,
            "title": title,
            "text": text,
            "date": date or "",
            "url": url,
            "collection_number": collection,
            "case_number": case_num,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all KFS rulings across all collections."""
        collections = self._get_collection_numbers()
        logger.info(f"Found {len(collections)} collections ({collections[0]}-{collections[-1]})")

        for col in collections:
            cases = self._get_case_numbers(col)
            if not cases:
                continue

            logger.info(f"Collection {col}: {len(cases)} cases")

            for case_num in cases:
                doc = self._fetch_ruling(col, case_num)
                if doc:
                    yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch rulings from the most recent collections."""
        collections = self._get_collection_numbers()
        # Only check the last 3 collections for updates
        recent = collections[-3:] if len(collections) >= 3 else collections

        for col in recent:
            cases = self._get_case_numbers(col)
            for case_num in cases:
                doc = self._fetch_ruling(col, case_num)
                if doc:
                    yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw KFS ruling into standard schema."""
        return {
            "_id": raw["ruling_id"],
            "_source": "JP/KFS-Rulings",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "ruling_id": raw["ruling_id"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw["date"],
            "url": raw["url"],
            "collection_number": raw["collection_number"],
            "case_number": raw.get("case_number", ""),
        }


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="JP/KFS-Rulings bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--sample-size", type=int, default=15)
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = JapanKFSRulingsScraper()

    if args.command == "test":
        logger.info("Testing connectivity to KFS...")
        page = _fetch_url(f"{BASE_URL}/service/JP/index.html")
        if page and "国税不服審判所" in page:
            logger.info("SUCCESS: KFS accessible")
        else:
            logger.error("FAILED: Could not access KFS")
            sys.exit(1)

    elif args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap result: {json.dumps(result, indent=2, default=str)}")

    elif args.command == "update":
        result = scraper.update()
        logger.info(f"Update result: {json.dumps(result, indent=2, default=str)}")


if __name__ == "__main__":
    main()
