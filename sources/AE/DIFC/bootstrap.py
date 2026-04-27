#!/usr/bin/env python3
"""
AE/DIFC -- DIFC Courts Judgments & Orders Fetcher

Fetches judgments and orders from DIFC Courts by scraping paginated listings:
  https://www.difccourts.ae/rules-decisions/judgments-orders

Full text is embedded as HTML on each judgment page (no PDFs).
~5,000 documents from 2007-present. Concrete5 CMS, 12 per page.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator
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
logger = logging.getLogger("legal-data-hunter.AE.DIFC")

BASE_URL = "https://www.difccourts.ae/rules-decisions/judgments-orders"
LISTING_URL = (
    BASE_URL
    + "?ccm_paging_p={page}&ccm_order_by=ak_date&ccm_order_by_direction=desc"
)

# Regex for listing entries
ENTRY_RE = re.compile(
    r'<div\s+class="each_result\s+content_set">\s*'
    r'<h4><a\s+href="([^"]+)">([^<]+)</a></h4>\s*'
    r'<p\s+class="label_small">\s*'
    r'([^<]+?)\s*</p>',
    re.DOTALL,
)

# Regex for pagination total
TOTAL_RE = re.compile(r'of\s+(\d[\d,]*)\s+results', re.IGNORECASE)

# HTML tag stripper
TAG_RE = re.compile(r'<[^>]+>')
MULTI_SPACE_RE = re.compile(r'[ \t]+')
MULTI_NL_RE = re.compile(r'\n{3,}')


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = unescape(html)
    text = TAG_RE.sub(' ', text)
    text = MULTI_SPACE_RE.sub(' ', text)
    # Normalize line breaks
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)
    text = MULTI_NL_RE.sub('\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> str:
    """Parse date like 'April 20, 2026' to ISO format."""
    date_str = date_str.strip()
    for fmt in ('%B %d, %Y', '%b %d, %Y', '%B %d %Y', '%d %B %Y'):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    # Try to extract year at minimum
    year_match = re.search(r'\b(20\d{2})\b', date_str)
    if year_match:
        return f"{year_match.group(1)}-01-01"
    return ""


def id_from_url(url: str) -> str:
    """Extract a stable ID from the judgment URL slug."""
    # URL like /rules-decisions/judgments-orders/court-first-instance/slug
    slug = url.rstrip('/').split('/')[-1]
    # Truncate very long slugs
    if len(slug) > 120:
        slug = slug[:120]
    return f"DIFC-{slug}"


def parse_meta(meta_text: str) -> tuple[str, str, str]:
    """Parse 'April 20, 2026  court of first instance - Judgments' into (date, court, doc_type)."""
    meta_text = meta_text.strip()
    # Split on the last dash to separate doc_type
    parts = meta_text.rsplit('-', 1)
    doc_type = parts[-1].strip() if len(parts) > 1 else ""
    rest = parts[0].strip() if len(parts) > 1 else meta_text

    # The date is at the beginning, court type after
    # Try to find date pattern (month name, day, year)
    date_match = re.match(
        r'(\w+\s+\d{1,2},?\s+\d{4})\s*(.*)',
        rest
    )
    if date_match:
        date_str = date_match.group(1).strip()
        court = date_match.group(2).strip()
    else:
        date_str = ""
        court = rest

    return date_str, court, doc_type


class DIFCCourtsScraper(BaseScraper):
    """
    Scraper for AE/DIFC -- DIFC Courts Judgments & Orders.
    Country: AE
    URL: https://www.difccourts.ae/rules-decisions/judgments-orders

    Data types: case_law
    Auth: none (public HTML)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=90,
        )

    # -- Helpers ------------------------------------------------------------

    def _fetch_listing_page(self, page: int) -> str:
        """Fetch a listing page and return HTML."""
        url = LISTING_URL.format(page=page)
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.text

    def _parse_listing(self, html: str) -> list[dict]:
        """Parse a listing page into a list of entry dicts."""
        entries = []
        for match in ENTRY_RE.finditer(html):
            href = match.group(1).strip()
            title = unescape(match.group(2).strip())
            meta = match.group(3).strip()

            date_str, court, doc_type = parse_meta(meta)

            # Build absolute URL
            if href.startswith('/'):
                url = f"https://www.difccourts.ae{href}"
            elif not href.startswith('http'):
                url = urljoin(BASE_URL, href)
            else:
                url = href

            entries.append({
                'url': url,
                'title': title,
                'date_raw': date_str,
                'court': court,
                'doc_type': doc_type,
            })
        return entries

    def _get_total_results(self, html: str) -> int:
        """Extract total result count from listing page."""
        match = TOTAL_RE.search(html)
        if match:
            return int(match.group(1).replace(',', ''))
        return 0

    def _fetch_full_text(self, url: str) -> str:
        """Fetch an individual judgment page and extract full text."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

        html = resp.text

        # Extract text from content_set_detail > content_desc
        # Try multiple patterns
        patterns = [
            re.compile(
                r'<div\s+class="content_set_detail[^"]*"[^>]*>.*?'
                r'<div\s+class="content_desc"[^>]*>(.*?)</div>\s*</div>',
                re.DOTALL
            ),
            re.compile(
                r'<div\s+class="content_desc"[^>]*>(.*?)</div>',
                re.DOTALL
            ),
        ]

        for pattern in patterns:
            match = pattern.search(html)
            if match:
                raw_html = match.group(1)
                text = strip_html(raw_html)
                if len(text) > 100:
                    return text

        # Fallback: try to get any large text block
        logger.warning(f"Could not extract content_desc from {url}")
        return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments from DIFC Courts."""
        logger.info("Fetching DIFC Courts judgments listing...")

        # Get first page to determine total
        html = self._fetch_listing_page(1)
        total = self._get_total_results(html)
        if total == 0:
            # Count entries on page as fallback
            entries = self._parse_listing(html)
            if not entries:
                logger.error("No entries found on first page")
                return
            total = len(entries)
            total_pages = 1
        else:
            total_pages = (total + 11) // 12  # 12 per page
            entries = self._parse_listing(html)

        logger.info(f"Total results: {total}, pages: {total_pages}")

        # Yield entries from first page
        for entry in entries:
            yield entry

        # Paginate through remaining pages
        for page in range(2, total_pages + 1):
            if page % 50 == 0:
                logger.info(f"Page {page}/{total_pages}...")
            try:
                html = self._fetch_listing_page(page)
                entries = self._parse_listing(html)
                if not entries:
                    logger.warning(f"No entries on page {page}, stopping")
                    break
                for entry in entries:
                    yield entry
            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents newer than since date."""
        for entry in self.fetch_all():
            date_str = parse_date(entry.get('date_raw', ''))
            if date_str:
                try:
                    entry_date = datetime.strptime(date_str, '%Y-%m-%d')
                    if entry_date < since.replace(tzinfo=None):
                        continue
                except ValueError:
                    pass
            yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw entry into standard schema."""
        url = raw.get('url', '')
        doc_id = id_from_url(url)

        # Fetch full text from individual page
        full_text = self._fetch_full_text(url)

        # Parse date
        date_iso = parse_date(raw.get('date_raw', ''))

        return {
            '_id': doc_id,
            '_source': 'AE/DIFC',
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw.get('title', ''),
            'text': full_text,
            'date': date_iso,
            'url': url,
            'court': raw.get('court', ''),
            'doc_type': raw.get('doc_type', ''),
            'country': 'AE',
            'jurisdiction': 'DIFC',
            'language': 'en',
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing DIFC Courts Judgments...")

        html = self._fetch_listing_page(1)
        total = self._get_total_results(html)
        entries = self._parse_listing(html)

        print(f"  Total results: {total}")
        print(f"  Entries on page 1: {len(entries)}")

        if entries:
            entry = entries[0]
            print(f"\n  First entry:")
            print(f"    Title: {entry['title'][:80]}")
            print(f"    Date: {entry['date_raw']}")
            print(f"    Court: {entry['court']}")
            print(f"    Type: {entry['doc_type']}")
            print(f"    URL: {entry['url']}")

            # Test full text extraction
            print(f"\n  Fetching full text...")
            text = self._fetch_full_text(entry['url'])
            if text:
                print(f"  Full text: SUCCESS ({len(text)} chars)")
                print(f"  Sample: {text[:300]}...")
            else:
                print("  Full text: FAILED")

        print("\nTest completed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = DIFCCourtsScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
