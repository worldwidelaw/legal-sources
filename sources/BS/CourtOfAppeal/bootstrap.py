#!/usr/bin/env python3
"""
BS/CourtOfAppeal -- Bahamas Court of Appeal Judgments Fetcher

Fetches judgments from courtofappeal.org.bs. PHP site with paginated
browse listings. Full text in PDFs.

Strategy:
  - Browse listings via ?skip=0,15,30,... (15 per page)
  - Parse HTML table for file_number, title, date, judgment_id
  - Fetch detail pages for summary, judges, classification, PDF link
  - Download PDFs and extract full text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
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
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BS.CourtOfAppeal")

BASE_URL = "https://www.courtofappeal.org.bs"
LISTING_URL = f"{BASE_URL}/judgments.php"
PER_PAGE = 15

# Parse table rows from listing
# Each row: <td>file_number</td><td><a href="...">title</a></td><td>date</td>
ROW_RE = re.compile(
    r'<tr[^>]*>\s*'
    r'<td[^>]*>(.*?)</td>\s*'
    r'<td[^>]*>\s*<a\s+href="([^"]+)"[^>]*>(.*?)</a>\s*</td>\s*'
    r'<td[^>]*>(.*?)</td>\s*'
    r'</tr>',
    re.DOTALL | re.IGNORECASE,
)

# Extract judgment ID from URL
JID_RE = re.compile(r'judgment=(\d+)')

# Extract PDF link from detail page
PDF_RE = re.compile(r'href="(download/[^"]+\.pdf)"', re.IGNORECASE)
PDF_RE2 = re.compile(r'href="([^"]*\.pdf)"', re.IGNORECASE)

# Extract metadata from detail page
CASE_NUM_RE = re.compile(r'Case\s*#:\s*(.+?)(?:<|$)', re.IGNORECASE)
JUDGE_RE = re.compile(r'(?:Judge|Justice|Judges?):\s*(.+?)(?:<|$)', re.IGNORECASE | re.DOTALL)
CLASSIFICATION_RE = re.compile(r'(?:Classification|Category):\s*(.+?)(?:<|$)', re.IGNORECASE)
SUMMARY_RE = re.compile(
    r'<p\s+class="[^"]*"[^>]*>((?:(?!<p\s).)+?)</p>',
    re.DOTALL,
)

# HTML cleaner
TAG_RE = re.compile(r'<[^>]+>')
MULTI_SPACE = re.compile(r'[ \t]+')


def strip_tags(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = unescape(html)
    text = TAG_RE.sub(' ', text)
    text = MULTI_SPACE.sub(' ', text)
    return text.strip()


def parse_date(date_str: str) -> str:
    """Parse date like 'DD/MM/YYYY' or 'Month DD, YYYY' to ISO format."""
    date_str = strip_tags(date_str).strip()
    # Try DD/MM/YYYY
    for fmt in ('%d/%m/%Y', '%m/%d/%Y', '%B %d, %Y', '%b %d, %Y', '%d %B %Y'):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    # Extract year
    year_match = re.search(r'\b(20\d{2})\b', date_str)
    if year_match:
        return f"{year_match.group(1)}-01-01"
    return ""


class BahamasCOAScraper(BaseScraper):
    """
    Scraper for BS/CourtOfAppeal -- Bahamas Court of Appeal.
    Country: BS
    URL: https://www.courtofappeal.org.bs/judgments.php

    Data types: case_law
    Auth: none (public PDFs)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=90,
        )

    # -- Helpers ------------------------------------------------------------

    def _fetch_listing_page(self, skip: int = 0) -> str:
        """Fetch a browse listing page."""
        url = f"{LISTING_URL}?skip={skip}" if skip > 0 else LISTING_URL
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.text

    def _parse_listing(self, html: str) -> list[dict]:
        """Parse listing page into entries."""
        entries = []
        for match in ROW_RE.finditer(html):
            file_number = strip_tags(match.group(1))
            href = match.group(2).strip()
            title = strip_tags(match.group(3))
            date_raw = strip_tags(match.group(4))

            # Skip header rows
            if 'File Number' in file_number or 'Judgment' in title.lower() and len(title) < 15:
                continue

            # Extract judgment ID
            jid_match = JID_RE.search(href)
            judgment_id = jid_match.group(1) if jid_match else ""
            if not judgment_id:
                continue

            # Build detail URL
            detail_url = f"{LISTING_URL}?action=view&judgment={judgment_id}"

            entries.append({
                'judgment_id': judgment_id,
                'file_number': file_number,
                'title': title,
                'date_raw': date_raw,
                'detail_url': detail_url,
            })
        return entries

    def _get_total_from_listing(self, html: str) -> int:
        """Try to extract total count from listing page."""
        # Look for "Showing X - Y of Z"
        match = re.search(r'of\s+(\d[\d,]*)\s', html)
        if match:
            return int(match.group(1).replace(',', ''))
        # Count "Next" links to determine if more pages exist
        return 0

    def _has_next_page(self, html: str) -> bool:
        """Check if there's a next page."""
        return 'skip=' in html and 'Next' in html

    def _fetch_detail(self, detail_url: str) -> dict:
        """Fetch judgment detail page and extract metadata + PDF URL."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(detail_url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch detail {detail_url}: {e}")
            return {}

        html = resp.text
        result = {}

        # Extract PDF link
        pdf_match = PDF_RE.search(html)
        if pdf_match:
            result['pdf_url'] = f"{BASE_URL}/{pdf_match.group(1)}"
        else:
            pdf_match2 = PDF_RE2.search(html)
            if pdf_match2:
                pdf_href = pdf_match2.group(1)
                if pdf_href.startswith('http'):
                    result['pdf_url'] = pdf_href
                elif pdf_href.startswith('/'):
                    result['pdf_url'] = f"{BASE_URL}{pdf_href}"
                else:
                    result['pdf_url'] = f"{BASE_URL}/{pdf_href}"

        # Extract classification
        class_match = CLASSIFICATION_RE.search(html)
        if class_match:
            result['classification'] = strip_tags(class_match.group(1))

        # Extract judges
        # Look for judge names in the detail section
        judge_section = re.search(
            r'(?:Judges?|Justice|Justices?).*?</(?:p|div|td)>',
            html, re.DOTALL | re.IGNORECASE
        )
        if judge_section:
            judges_text = strip_tags(judge_section.group(0))
            # Clean up
            judges_text = re.sub(r'^.*?(?:Judges?|Justice|Justices?)\s*:?\s*', '', judges_text, flags=re.IGNORECASE)
            if judges_text and len(judges_text) < 500:
                result['judges'] = judges_text.strip()

        # Extract summary text (the description paragraph)
        # Look for text blocks that aren't navigation/headers
        desc_blocks = re.findall(
            r'<p[^>]*>((?:(?!</p>).)+)</p>',
            html, re.DOTALL
        )
        for block in desc_blocks:
            text = strip_tags(block)
            if len(text) > 100 and 'javascript' not in text.lower():
                result['summary'] = text[:2000]
                break

        return result

    def _extract_pdf_text(self, pdf_url: str, source_id: str) -> str:
        """Extract text from judgment PDF."""
        if not pdf_url:
            return ""
        return extract_pdf_markdown(
            source="BS/CourtOfAppeal",
            source_id=source_id,
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments from browse listing."""
        logger.info("Fetching Bahamas Court of Appeal judgments...")

        skip = 0
        total_yielded = 0
        consecutive_empty = 0

        while True:
            if skip > 0 and skip % 300 == 0:
                logger.info(f"Progress: skip={skip}, yielded={total_yielded}...")

            try:
                html = self._fetch_listing_page(skip)
                entries = self._parse_listing(html)
            except Exception as e:
                logger.error(f"Error fetching skip={skip}: {e}")
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                skip += PER_PAGE
                continue

            if not entries:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info(f"3 consecutive empty pages at skip={skip}, stopping")
                    break
                skip += PER_PAGE
                continue

            consecutive_empty = 0
            for entry in entries:
                yield entry
                total_yielded += 1

            # Check for more pages
            if not self._has_next_page(html) and len(entries) < PER_PAGE:
                break

            skip += PER_PAGE

        logger.info(f"Finished listing: {total_yielded} entries total")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents newer than since date."""
        for entry in self.fetch_all():
            date_str = parse_date(entry.get('date_raw', ''))
            if date_str:
                try:
                    entry_date = datetime.strptime(date_str, '%Y-%m-%d')
                    if entry_date < since.replace(tzinfo=None):
                        return  # Listing is date-sorted, stop early
                except ValueError:
                    pass
            yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw entry into standard schema."""
        judgment_id = raw.get('judgment_id', '')
        doc_id = f"BS-COA-{judgment_id}"
        detail_url = raw.get('detail_url', '')

        # Fetch detail page for PDF link and metadata
        detail = self._fetch_detail(detail_url) if detail_url else {}

        # Extract full text from PDF
        pdf_url = detail.get('pdf_url', '')
        full_text = self._extract_pdf_text(pdf_url, doc_id) if pdf_url else ""

        # Parse date
        date_iso = parse_date(raw.get('date_raw', ''))

        return {
            '_id': doc_id,
            '_source': 'BS/CourtOfAppeal',
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': raw.get('title', ''),
            'text': full_text,
            'date': date_iso,
            'url': detail_url,
            'file_number': raw.get('file_number', ''),
            'pdf_url': pdf_url,
            'classification': detail.get('classification', ''),
            'judges': detail.get('judges', ''),
            'summary': detail.get('summary', ''),
            'country': 'BS',
            'language': 'en',
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Bahamas Court of Appeal...")

        html = self._fetch_listing_page(0)
        entries = self._parse_listing(html)
        has_next = self._has_next_page(html)

        print(f"  Entries on page 1: {len(entries)}")
        print(f"  Has next page: {has_next}")

        if entries:
            entry = entries[0]
            print(f"\n  First entry:")
            print(f"    Title: {entry['title'][:80]}")
            print(f"    File #: {entry['file_number']}")
            print(f"    Date: {entry['date_raw']}")
            print(f"    ID: {entry['judgment_id']}")

            # Fetch detail
            print(f"\n  Fetching detail page...")
            detail = self._fetch_detail(entry['detail_url'])
            if detail:
                print(f"    PDF URL: {detail.get('pdf_url', 'NOT FOUND')}")
                print(f"    Classification: {detail.get('classification', 'N/A')}")
                print(f"    Summary: {detail.get('summary', 'N/A')[:100]}...")

                # Test PDF extraction
                if detail.get('pdf_url'):
                    print(f"\n  Extracting PDF text...")
                    text = self._extract_pdf_text(
                        detail['pdf_url'],
                        f"BS-COA-{entry['judgment_id']}"
                    )
                    if text:
                        print(f"    PDF text: SUCCESS ({len(text)} chars)")
                        print(f"    Sample: {text[:200]}...")
                    else:
                        print("    PDF text: FAILED")

        # Count approximate total
        print(f"\n  Checking total by sampling last pages...")
        html_last = self._fetch_listing_page(4440)
        entries_last = self._parse_listing(html_last)
        if entries_last:
            print(f"    Page at skip=4440 has {len(entries_last)} entries")
            print(f"    Approximate total: ~{4440 + len(entries_last)}")
        else:
            # Binary search for last page
            for skip in [3000, 4000, 4200, 4400]:
                html_test = self._fetch_listing_page(skip)
                entries_test = self._parse_listing(html_test)
                if entries_test:
                    print(f"    skip={skip}: {len(entries_test)} entries")

        print("\nTest completed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = BahamasCOAScraper()

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
