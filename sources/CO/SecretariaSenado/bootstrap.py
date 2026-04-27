#!/usr/bin/env python3
"""
CO/SecretariaSenado -- Colombia Secretaria del Senado Laws Fetcher

Fetches full text of Colombian laws from the Senate basedoc HTML pages.

Strategy:
  - Enumerate law numbers from recent to old
  - Try likely years for each law number
  - Download and clean HTML to extract full text
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Full initial pull (recent laws)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.SecretariaSenado")

BASE_URL = "http://www.secretariasenado.gov.co/senado/basedoc"

# Approximate law number ranges per year (Colombian laws are globally numbered)
# This helps us guess the right year for a given law number
LAW_YEAR_RANGES = [
    (2400, 2500, 2024), (2300, 2399, 2023), (2200, 2299, 2022),
    (2100, 2199, 2021), (2000, 2099, 2020), (1950, 1999, 2019),
    (1900, 1949, 2018), (1850, 1899, 2017), (1800, 1849, 2016),
    (1750, 1799, 2015), (1700, 1749, 2014), (1650, 1699, 2013),
    (1600, 1649, 2012), (1450, 1599, 2011), (1400, 1449, 2010),
    (1350, 1399, 2009), (1200, 1349, 2008), (1150, 1199, 2007),
    (1100, 1149, 2006), (950, 1099, 2005), (900, 949, 2004),
    (800, 899, 2003), (750, 799, 2002), (700, 749, 2001),
    (600, 699, 2000), (500, 599, 1999), (450, 499, 1998),
    (370, 449, 1997), (270, 369, 1996), (200, 269, 1995),
    (130, 199, 1994), (30, 129, 1993), (1, 29, 1992),
]


class SecretariaSenadoScraper(BaseScraper):
    """Scraper for CO/SecretariaSenado -- Colombian laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=30)
        except ImportError:
            self.client = None

    def _http_get(self, url: str) -> Optional[str]:
        """HTTP GET returning response text."""
        for attempt in range(2):
            try:
                if self.client:
                    resp = self.client.get(url, headers={"User-Agent": "Mozilla/5.0"})
                    if resp.status_code == 200:
                        return resp.text
                    if resp.status_code == 404:
                        return None
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                else:
                    import urllib.request
                    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                err_str = str(e)
                if "404" in err_str:
                    return None
                if attempt == 0:
                    time.sleep(2)
                else:
                    logger.warning(f"Failed for {url}: {e}")
        return None

    def _guess_years(self, law_num: int) -> list:
        """Return list of likely years to try for a given law number."""
        years = []
        for low, high, year in LAW_YEAR_RANGES:
            if low <= law_num <= high:
                years.append(year)
                # Also try adjacent years
                years.extend([year - 1, year + 1])
                break
        if not years:
            # Fallback: try recent years
            years = list(range(2024, 1990, -1))
        return years

    def _fetch_law(self, law_num: int) -> Optional[Tuple[str, int, str]]:
        """Try to fetch a law by number, returning (html, year, url) or None."""
        years = self._guess_years(law_num)
        for year in years:
            url = f"{BASE_URL}/ley_{law_num:04d}_{year}.html"
            content = self._http_get(url)
            if content and len(content) > 5000:
                return content, year, url
        return None

    def _clean_html(self, raw_html: str) -> Optional[str]:
        """Extract clean text from basedoc HTML."""
        # Remove script/style
        text = re.sub(r'<script[^>]*>.*?</script>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<head[^>]*>.*?</head>', '', text, flags=re.DOTALL | re.IGNORECASE)
        # Remove navigation elements
        text = re.sub(r'<nav[^>]*>.*?</nav>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<header[^>]*>.*?</header>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<footer[^>]*>.*?</footer>', '', text, flags=re.DOTALL | re.IGNORECASE)
        # Replace block tags with newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</h[1-6]>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</tr>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
        # Strip remaining tags
        text = re.sub(r'<[^>]+>', '', text)
        # Decode entities
        text = html.unescape(text)
        # Clean whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()
        return text if len(text) > 200 else None

    def _extract_title(self, raw_html: str) -> str:
        """Extract law title from HTML."""
        # Try <title> tag
        m = re.search(r'<title[^>]*>(.*?)</title>', raw_html, re.DOTALL | re.IGNORECASE)
        if m:
            title = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            title = html.unescape(title)
            if title and len(title) > 5:
                return title
        # Try first heading
        m = re.search(r'<h[12][^>]*>(.*?)</h[12]>', raw_html, re.DOTALL | re.IGNORECASE)
        if m:
            title = re.sub(r'<[^>]+>', '', m.group(1)).strip()
            return html.unescape(title)
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        law_num = raw.get("law_number", 0)
        year = raw.get("year", "")
        title = raw.get("title", f"Ley {law_num} de {year}")
        text = raw.get("text", "")
        url = raw.get("url", "")

        return {
            "_id": f"CO-LEY-{law_num:04d}-{year}",
            "_source": "CO/SecretariaSenado",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": f"{year}-01-01" if year else "",
            "url": url,
            "law_number": str(law_num),
            "year": str(year),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch laws from most recent to oldest."""
        count = 0
        consecutive_misses = 0

        for law_num in range(2430, 0, -1):
            time.sleep(2)
            result = self._fetch_law(law_num)
            if not result:
                consecutive_misses += 1
                if consecutive_misses > 20:
                    logger.info(f"20 consecutive misses at law {law_num}, stopping")
                    break
                continue

            consecutive_misses = 0
            raw_html, year, url = result
            title = self._extract_title(raw_html)
            text = self._clean_html(raw_html)

            if not text:
                continue

            raw = {
                "law_number": law_num,
                "year": year,
                "title": title,
                "text": text,
                "url": url,
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} laws fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent laws."""
        # Just fetch the most recent 100 law numbers
        count = 0
        for law_num in range(2430, 2330, -1):
            time.sleep(2)
            result = self._fetch_law(law_num)
            if not result:
                continue

            raw_html, year, url = result
            title = self._extract_title(raw_html)
            text = self._clean_html(raw_html)
            if not text:
                continue

            raw = {
                "law_number": law_num,
                "year": year,
                "title": title,
                "text": text,
                "url": url,
            }
            count += 1
            yield raw

        logger.info(f"Updates: {count} laws")

    def test(self) -> bool:
        """Quick connectivity test."""
        result = self._fetch_law(2300)
        if not result:
            logger.error("Could not fetch Ley 2300")
            return False

        raw_html, year, url = result
        title = self._extract_title(raw_html)
        text = self._clean_html(raw_html)
        logger.info(f"OK: {title[:80]} ({year}) - {len(text)} chars")
        return bool(text and len(text) > 200)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CO/SecretariaSenado data fetcher")
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

    scraper = SecretariaSenadoScraper()

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
    main()
