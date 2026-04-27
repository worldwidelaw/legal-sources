#!/usr/bin/env python3
"""
ET/FederalCourts -- Ethiopian Federal Supreme Court Cassation Decisions

Fetches cassation decisions from lawethiopia.com. The site organizes ~175+
decisions by subject matter with paginated index pages. Each decision has
an HTML article page with a link to an individual PDF containing the full
Amharic text.

Strategy:
  - Paginate the subject-matter index to collect all unique case article URLs
  - For each article, fetch the detail page to find the case-specific PDF link
  - Download and extract text from each PDF
  - Normalize into standard schema with full text

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote, quote

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ET.FederalCourts")

BASE_URL = "https://lawethiopia.com"
INDEX_URL = f"{BASE_URL}/index.php/case-law/court-decisions-by-subject-matter/federal-cassation"
PAGE_SIZE = 100
MONTHS = {
    "January": "01", "February": "02", "March": "03", "April": "04",
    "May": "05", "June": "06", "July": "07", "August": "08",
    "September": "09", "October": "10", "November": "11", "December": "12",
}


class EthiopiaFederalCourtsScraper(BaseScraper):
    """
    Scraper for ET/FederalCourts -- Ethiopian Federal Supreme Court.
    Country: ET
    URL: https://lawethiopia.com/
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        self.session.verify = False
        self._cookies_initialized = False

    def _ensure_cookies(self):
        """Visit homepage to get session cookies (required for 200 responses)."""
        if not self._cookies_initialized:
            logger.info("Initializing session cookies from homepage...")
            self.session.get(f"{BASE_URL}/", timeout=30)
            self._cookies_initialized = True

    def _get_page(self, url: str) -> str:
        """Fetch a page respecting crawl delay."""
        self._ensure_cookies()
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()
        return resp.text

    def _get_binary(self, url: str) -> bytes:
        """Fetch binary content (PDF) respecting crawl delay."""
        self._ensure_cookies()
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()
        return resp.content

    def _collect_all_cases(self, sample: bool = False) -> list:
        """Paginate the subject-matter index to collect all unique case entries."""
        cases = {}  # case_number -> entry dict
        max_pages = 3 if sample else 30
        consecutive_empty = 0

        for page_idx in range(max_pages):
            offset = page_idx * PAGE_SIZE
            url = f"{INDEX_URL}?types[0]=1&limit={PAGE_SIZE}&start={offset}"
            logger.info(f"Fetching index page offset={offset}")

            try:
                html = self._get_page(url)
            except Exception as e:
                logger.error(f"Failed to fetch index at offset {offset}: {e}")
                break

            # Match links with decision-no or case-no pattern
            links = re.findall(
                r'href="(/index\.php/volume-[^"]*?(?:decision|case)-no?-?(\d{4,6}))"[^>]*>([^<]*)',
                html
            )

            new_count = 0
            for href, case_num, title_raw in links:
                if case_num in cases:
                    continue
                vol_m = re.search(r'/volume-(\d+(?:-\d+)?)', href)
                title = re.sub(r'\s+', ' ', title_raw).strip()
                # Clean title: remove leading case number
                title = re.sub(r'^\d{4,6}\s*', '', title).strip()
                if not title:
                    title = f"Cassation Decision No. {case_num}"

                cases[case_num] = {
                    "case_number": case_num,
                    "title": title,
                    "volume": vol_m.group(1) if vol_m else None,
                    "detail_url": urljoin(BASE_URL, href),
                }
                new_count += 1

            logger.info(f"  Found {len(links)} links, {new_count} new (total unique: {len(cases)})")

            if new_count == 0:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
            else:
                consecutive_empty = 0

        logger.info(f"Collected {len(cases)} unique cases from index")
        return list(cases.values())

    def _find_case_pdf_url(self, html: str, case_number: str) -> Optional[str]:
        """Find the PDF URL for a specific case from the detail page HTML.

        The PDF links follow patterns like:
        /images/cassation/cassation decisions by number/volume 1-3/10797 Title.pdf
        """
        # Look for PDF link containing this case number
        pdf_pattern = re.compile(
            r'href="(/images/cassation/[^"]*?' + re.escape(case_number) + r'[^"]*?\.pdf)"',
            re.IGNORECASE
        )
        match = pdf_pattern.search(html)
        if match:
            return urljoin(BASE_URL, match.group(1))
        return None

    def _parse_date(self, html: str) -> Optional[str]:
        """Extract date from detail page HTML."""
        date_m = re.search(
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|'
            r'September|October|November|December)\s+(\d{4})',
            html
        )
        if date_m:
            day = date_m.group(1).zfill(2)
            month = MONTHS.get(date_m.group(2), "01")
            year = date_m.group(3)
            return f"{year}-{month}-{day}"
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes, case_number: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="ET/FederalCourts",
            source_id=case_number,
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        case_number = raw.get("case_number", "")
        _id = f"ET-FSC-{case_number}" if case_number else f"ET-FSC-{hash(raw.get('url', ''))}"

        subject_tags = []
        title = raw.get("title", "")
        if "/" in title:
            subject_tags = [t.strip() for t in title.split("/") if t.strip()]

        return {
            "_id": _id,
            "_source": "ET/FederalCourts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title or f"Cassation Decision No. {case_number}",
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "case_number": case_number,
            "volume": raw.get("volume"),
            "subject_tags": subject_tags,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions."""
        yield from self._fetch_decisions(sample=False)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield decisions modified since a date."""
        yield from self._fetch_decisions(sample=False, since=since)

    def _fetch_decisions(self, sample: bool = False, since: str = None) -> Generator[dict, None, None]:
        """Core fetcher: collect cases from index, fetch PDFs, extract text."""
        all_cases = self._collect_all_cases(sample=sample)
        max_records = 15 if sample else len(all_cases)
        count = 0
        pdf_failures = 0

        for entry in all_cases:
            if count >= max_records:
                break

            case_number = entry["case_number"]
            logger.info(f"Processing case {case_number} (volume {entry.get('volume', '?')})")

            # Fetch detail page
            try:
                detail_html = self._get_page(entry["detail_url"])
            except Exception as e:
                logger.warning(f"Failed to fetch detail page for case {case_number}: {e}")
                pdf_failures += 1
                continue

            # Find PDF URL from detail page
            pdf_url = self._find_case_pdf_url(detail_html, case_number)
            if not pdf_url:
                logger.warning(f"No PDF found for case {case_number}")
                pdf_failures += 1
                continue

            # Parse date from detail page
            date = self._parse_date(detail_html)

            # Download and extract text from PDF
            try:
                pdf_bytes = self._get_binary(pdf_url)
                if len(pdf_bytes) < 500:
                    logger.warning(f"PDF too small for case {case_number} ({len(pdf_bytes)} bytes)")
                    continue
                text = self._extract_pdf_text(pdf_bytes, case_number)
            except Exception as e:
                logger.error(f"Failed to download/extract PDF for case {case_number}: {e}")
                pdf_failures += 1
                continue

            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for case {case_number}: {len(text) if text else 0} chars")
                continue

            raw = {
                "title": entry.get("title", f"Cassation Decision No. {case_number}"),
                "text": text,
                "date": date,
                "url": entry["detail_url"],
                "case_number": case_number,
                "volume": entry.get("volume"),
            }

            record = self.normalize(raw)
            count += 1
            logger.info(f"[{count}] Case {case_number}: {len(text)} chars")
            yield record

        logger.info(f"Total records fetched: {count} ({pdf_failures} failures)")

    def test_api(self):
        """Quick connectivity check."""
        logger.info("Testing lawethiopia.com connectivity...")
        try:
            # Test index page
            url = f"{INDEX_URL}?types[0]=1&limit=10&start=0"
            html = self._get_page(url)
            links = re.findall(
                r'href="(/index\.php/volume-[^"]*?(?:decision|case)-no?-?(\d{4,6}))"',
                html
            )
            logger.info(f"OK: index page returned {len(links)} case links")

            if links:
                href, case_num = links[0]
                detail_url = urljoin(BASE_URL, href)
                logger.info(f"Testing case {case_num}: {detail_url}")

                detail_html = self._get_page(detail_url)
                pdf_url = self._find_case_pdf_url(detail_html, case_num)
                if pdf_url:
                    logger.info(f"Found PDF: {unquote(pdf_url)[:80]}...")
                    pdf_bytes = self._get_binary(pdf_url)
                    text = self._extract_pdf_text(pdf_bytes, case_num)
                    logger.info(f"OK: extracted {len(text)} chars from PDF")
                else:
                    logger.warning("No PDF URL found on detail page")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            raise


def main():
    scraper = EthiopiaFederalCourtsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper._fetch_decisions(sample=sample):
            safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
