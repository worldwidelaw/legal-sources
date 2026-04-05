#!/usr/bin/env python3
"""
CA/CRA_TaxFolios -- Canada Revenue Agency Income Tax Folios

Fetches CRA's technical tax guidance (Income Tax Folios) from canada.ca.

Strategy:
  1. Scrape the folio index page for all series/folio links
  2. For each folio page, find chapter links
  3. For each chapter, extract full HTML text content

Endpoints:
  - Index: https://www.canada.ca/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index.html
  - Folio pages: /series-N-.../folio-N-....html
  - Chapter pages: /income-tax-folio-sN-fN-cN-....html

Data:
  - 7 series, ~35 folios, ~150+ chapters
  - Each chapter 5,000-20,000+ words of technical tax guidance
  - Full HTML text, no PDF extraction needed
  - No auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.CRA_TaxFolios")

BASE_URL = "https://www.canada.ca"
INDEX_URL = "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index.html"

# Series pages with their folio sub-pages
SERIES_PATHS = [
    "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index/series-1-individuals.html",
    "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index/series-2-employers-employees.html",
    "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index/series-3-property-investments-savings-plans.html",
    "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index/series-4-businesses.html",
    "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index/series-5-international-residency.html",
    "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index/series-6-trusts.html",
    "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index/series-7-charities-non-profit-organizations.html",
]


try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    print("WARNING: beautifulsoup4 not installed. Install with: pip install beautifulsoup4")


class CRATaxFoliosScraper(BaseScraper):
    """
    Scraper for CA/CRA_TaxFolios -- Canada Revenue Agency Income Tax Folios.
    Country: CA
    URL: https://www.canada.ca/.../income-tax-folios-index.html

    Data types: doctrine
    Auth: none (Public Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-CA,en;q=0.9",
            },
            timeout=60,
        )

    def _get_page(self, path: str) -> str:
        """Fetch an HTML page from canada.ca."""
        resp = self.client.get(path)
        resp.raise_for_status()
        return resp.text

    def _discover_chapter_urls(self) -> List[Dict[str, str]]:
        """
        Discover all chapter URLs by crawling the index and folio pages.
        Returns list of dicts with 'url', 'series', 'folio_name'.
        """
        chapters = []

        # First get the main index to find folio pages
        logger.info("Fetching folio index page...")
        index_html = self._get_page(INDEX_URL)
        soup = BeautifulSoup(index_html, "html.parser")

        # Find all links that point to folio pages
        folio_links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "income-tax-folios-index" in href and href != INDEX_URL:
                if href.startswith("/"):
                    folio_links.add(href)
                elif href.startswith("http"):
                    # Extract path
                    from urllib.parse import urlparse
                    parsed = urlparse(href)
                    if "canada.ca" in parsed.netloc:
                        folio_links.add(parsed.path)

        logger.info(f"Found {len(folio_links)} folio/series pages")

        # Visit each folio page to find chapter links
        for folio_url in sorted(folio_links):
            time.sleep(1)
            try:
                logger.info(f"Scanning folio page: {folio_url}")
                folio_html = self._get_page(folio_url)
                folio_soup = BeautifulSoup(folio_html, "html.parser")

                # Find chapter links - they contain "income-tax-folio-s" in URL
                for a in folio_soup.find_all("a", href=True):
                    href = a["href"]
                    if "income-tax-folio-s" in href.lower():
                        chapter_path = href if href.startswith("/") else urljoin(folio_url, href)
                        # Extract folio ID from URL (e.g., s1-f1-c1)
                        folio_id_match = re.search(r'income-tax-folio-(s\d+-f\d+-c\d+)', chapter_path, re.IGNORECASE)
                        if folio_id_match:
                            folio_id = folio_id_match.group(1).upper()
                            chapter_title = a.get_text(strip=True)
                            chapters.append({
                                "url": chapter_path,
                                "folio_id": folio_id,
                                "title": chapter_title,
                                "folio_page": folio_url,
                            })

            except Exception as e:
                logger.warning(f"Error scanning {folio_url}: {e}")

        # Deduplicate by folio_id
        seen = set()
        unique_chapters = []
        for ch in chapters:
            if ch["folio_id"] not in seen:
                seen.add(ch["folio_id"])
                unique_chapters.append(ch)

        logger.info(f"Discovered {len(unique_chapters)} unique chapters")
        return unique_chapters

    def _extract_text(self, html_content: str) -> str:
        """Extract clean text from a chapter page HTML."""
        soup = BeautifulSoup(html_content, "html.parser")

        # Remove script/style/nav elements
        for tag in soup.find_all(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()

        # Find main content area
        main = soup.find("main") or soup.find("article") or soup.find("div", class_="mwsbodytext")
        if not main:
            # Fallback: look for the content div
            main = soup.find("div", {"id": "wb-cont"}) or soup.find("div", class_="col-md-9")
            if not main:
                main = soup

        # Get text and clean it
        text = main.get_text(separator="\n", strip=False)

        # Clean up excessive whitespace
        lines = []
        for line in text.split("\n"):
            stripped = line.strip()
            if stripped:
                lines.append(stripped)

        text = "\n\n".join(lines)

        # Remove common boilerplate
        boilerplate = [
            "Skip to main content",
            "Skip to \"About government\"",
            "Language selection",
            "Search Canada.ca",
            "Government of Canada",
            "Report a problem",
            "Date modified:",
        ]
        for bp in boilerplate:
            text = text.replace(bp, "")

        return text.strip()

    def _extract_metadata(self, html_content: str, url: str) -> Dict[str, Any]:
        """Extract metadata from a chapter page."""
        soup = BeautifulSoup(html_content, "html.parser")

        meta = {}

        # Title
        title_tag = soup.find("h1")
        if title_tag:
            meta["title"] = title_tag.get_text(strip=True)

        # Date modified
        date_tag = soup.find("time")
        if date_tag:
            meta["date_modified"] = date_tag.get("datetime", date_tag.get_text(strip=True))

        # Extract folio ID from title or URL
        folio_match = re.search(r'(S\d+-F\d+-C\d+)', meta.get("title", "") + url, re.IGNORECASE)
        if folio_match:
            meta["folio_id"] = folio_match.group(1).upper()

        return meta

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all folio chapters with full text."""
        if not HAS_BS4:
            raise ImportError("beautifulsoup4 is required. Install with: pip install beautifulsoup4")

        chapters = self._discover_chapter_urls()
        logger.info(f"Starting fetch of {len(chapters)} chapters")

        for i, chapter in enumerate(chapters):
            time.sleep(1.5)  # Respect rate limits
            try:
                logger.info(f"[{i+1}/{len(chapters)}] Fetching {chapter['folio_id']}: {chapter['title'][:60]}")
                chapter_html = self._get_page(chapter["url"])

                text = self._extract_text(chapter_html)
                metadata = self._extract_metadata(chapter_html, chapter["url"])

                if len(text) < 100:
                    logger.warning(f"Very short text for {chapter['folio_id']} ({len(text)} chars), skipping")
                    continue

                yield {
                    "folio_id": chapter.get("folio_id", metadata.get("folio_id", "")),
                    "title": metadata.get("title", chapter.get("title", "")),
                    "text": text,
                    "url": BASE_URL + chapter["url"],
                    "date_modified": metadata.get("date_modified", ""),
                    "folio_page": chapter.get("folio_page", ""),
                }

            except Exception as e:
                logger.error(f"Error fetching {chapter['folio_id']}: {e}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch updated chapters (re-fetches all since CRA doesn't have an update feed)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw chapter data into standardized schema."""
        folio_id = raw.get("folio_id", "")
        if not folio_id:
            return None

        title = raw.get("title", "")
        text = raw.get("text", "")

        if not text or len(text) < 100:
            return None

        # Parse date
        date_str = raw.get("date_modified", "")
        if date_str:
            try:
                # Try ISO format
                date_str = date_str[:10]  # Take just YYYY-MM-DD
            except Exception:
                date_str = ""

        return {
            "_id": f"CA/CRA_TaxFolios/{folio_id}",
            "_source": "CA/CRA_TaxFolios",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str or None,
            "url": raw.get("url", ""),
            "folio_id": folio_id,
            "document_type": "income_tax_folio",
            "jurisdiction": "CA",
            "language": "en",
        }

    def test_api(self):
        """Quick connectivity test."""
        print("Testing CRA Income Tax Folios access...")

        # Test index page
        print("\n1. Testing index page...")
        try:
            index_html = self._get_page(INDEX_URL)
            print(f"   Index page fetched: {len(index_html)} bytes")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test a specific chapter
        print("\n2. Testing chapter page (S1-F1-C1 Medical Expense)...")
        chapter_url = "/en/revenue-agency/services/tax/technical-information/income-tax/income-tax-folios-index/series-1-individuals/folio-1-health-medical/income-tax-folio-s1-f1-c1-medical-expense-tax-credit.html"
        try:
            chapter_html = self._get_page(chapter_url)
            text = self._extract_text(chapter_html)
            meta = self._extract_metadata(chapter_html, chapter_url)
            print(f"   Title: {meta.get('title', 'Unknown')[:80]}")
            print(f"   Text length: {len(text)} characters")
            print(f"   First 300 chars: {text[:300]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nConnectivity test complete!")


def main():
    scraper = CRATaxFoliosScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
