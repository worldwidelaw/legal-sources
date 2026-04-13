#!/usr/bin/env python3
"""
JP/CourtsGoJp — Japanese Courts Case Law Database

Fetches official Japanese court decisions from courts.go.jp.

Strategy:
  - Search via HTML pages with date range filters and pagination
  - Extract case IDs from search results
  - Download full text PDFs and extract text with pdfplumber
  - 29,000+ Supreme Court cases, plus High/District courts

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update --since 2024-01-01
  python bootstrap.py test               # Quick connectivity test
"""

import io
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

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JP.CourtsGoJp")

BASE_URL = "https://www.courts.go.jp"

# Court type search pages
# courtCaseType: 1=Supreme, 2=High, 3=District/Family/Summary
SEARCH_CONFIGS = [
    {"path": "/hanrei/search2/index.html", "court_type": "1", "court_name": "Supreme Court"},
    {"path": "/hanrei/search2/index.html", "court_type": "2", "court_name": "High Court"},
    {"path": "/hanrei/search4/index.html", "court_type": "3", "court_name": "Lower Courts"},
]

PAGE_SIZE = 10  # Results per page on courts.go.jp


class CourtsGoJpScraper(BaseScraper):
    """
    Scraper for JP/CourtsGoJp — Japanese Courts Case Law Database.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ja,en;q=0.5",
        })

    # Japanese era base years
    ERA_BASES = {
        "令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867,
    }

    def _parse_japanese_date(self, date_str: str) -> Optional[str]:
        """Parse Japanese era date like '令和8年2月24日' or Western '2024/1/15'."""
        # Try Japanese era format: 令和8年2月24日
        era_match = re.search(
            r'(令和|平成|昭和|大正|明治)(\d{1,2})年(\d{1,2})月(\d{1,2})日', date_str,
        )
        if era_match:
            era, year, month, day = era_match.groups()
            western_year = self.ERA_BASES[era] + int(year)
            return f"{western_year}-{int(month):02d}-{int(day):02d}"
        # Try Western format: 2024年1月15日 or 2024/1/15
        western_match = re.search(r'(\d{4})[年/](\d{1,2})[月/](\d{1,2})', date_str)
        if western_match:
            return f"{western_match.group(1)}-{int(western_match.group(2)):02d}-{int(western_match.group(3)):02d}"
        return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="JP/CourtsGoJp",
            source_id="",
            pdf_bytes=pdf_content,
            table="case_law",
        ) or ""

    def _download_pdf(self, case_id: str, retries: int = 3) -> Optional[bytes]:
        """Download a case PDF by ID."""
        url = f"{BASE_URL}/assets/hanrei/hanrei-pdf-{case_id}.pdf"
        for attempt in range(retries):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=120)
                if resp.status_code == 200 and len(resp.content) > 500:
                    return resp.content
                if resp.status_code == 404:
                    logger.debug(f"No PDF for case {case_id}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for PDF {case_id}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF download attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_case_metadata(self, detail_html: str) -> dict:
        """Extract case metadata from a detail page."""
        metadata = {}

        # Extract from dt/dd pairs (current page structure)
        rows = re.findall(
            r'<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>',
            detail_html, re.DOTALL,
        )
        if not rows:
            # Fallback: try th/td pairs
            rows = re.findall(
                r'<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>',
                detail_html, re.DOTALL,
            )
        for key_html, val_html in rows:
            key = re.sub(r'<[^>]+>', '', key_html).strip()
            value = re.sub(r'<[^>]+>', '', val_html).strip()
            value = htmlmod.unescape(value)
            if key and value:
                metadata[key] = value

        return metadata

    def _search_cases(self, court_config: dict, offset: int = 0,
                      date_from: str = "1947-01-01",
                      date_to: str = "2026-12-31") -> tuple[list[str], int]:
        """
        Search for cases and return (list_of_case_ids, total_count).
        """
        url = f"{BASE_URL}{court_config['path']}"
        params = {
            "courtCaseType": court_config["court_type"],
            "filter[judgeDateFrom]": date_from,
            "filter[judgeDateTo]": date_to,
            "offset": str(offset),
        }

        try:
            time.sleep(2)
            resp = self.session.get(url, params=params, timeout=30)
            resp.encoding = "utf-8"
            if resp.status_code != 200:
                logger.warning(f"Search returned HTTP {resp.status_code}")
                return [], 0

            html = resp.text

            # Extract case IDs from detail links
            ids = re.findall(r'/(\d+)/detail\d*/index\.html', html)

            # Extract total count
            total = 0
            count_match = re.search(r'(\d[\d,]+)件中', html)
            if count_match:
                total = int(count_match.group(1).replace(",", ""))

            return ids, total

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return [], 0

    def _fetch_detail(self, case_id: str) -> dict:
        """Fetch case detail page and extract metadata."""
        url = f"{BASE_URL}/hanrei/{case_id}/detail2/index.html"
        try:
            time.sleep(2)
            resp = self.session.get(url, timeout=30)
            resp.encoding = "utf-8"
            if resp.status_code != 200:
                return {}
            return self._extract_case_metadata(resp.text)
        except Exception as e:
            logger.warning(f"Detail fetch failed for {case_id}: {e}")
            return {}

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Japanese court cases."""
        for court_config in SEARCH_CONFIGS:
            court_name = court_config["court_name"]
            logger.info(f"Starting search for {court_name} cases")

            # First search to get total count
            first_ids, total = self._search_cases(court_config, offset=0)
            logger.info(f"{court_name}: {total} total cases")

            if not first_ids:
                continue

            # Process first page
            for case_id in first_ids:
                record = self._process_case(case_id, court_name)
                if record:
                    yield record

            # Paginate through remaining results
            offset = PAGE_SIZE
            while offset < total:
                logger.info(f"{court_name}: fetching offset {offset}/{total}")
                ids, _ = self._search_cases(court_config, offset=offset)
                if not ids:
                    break
                for case_id in ids:
                    record = self._process_case(case_id, court_name)
                    if record:
                        yield record
                offset += PAGE_SIZE

    def _process_case(self, case_id: str, court_name: str) -> Optional[dict]:
        """Process a single case: fetch detail + PDF."""
        logger.info(f"Processing case {case_id} ({court_name})")

        # Fetch detail metadata
        metadata = self._fetch_detail(case_id)

        # Download and extract PDF text
        pdf_bytes = self._download_pdf(case_id)
        if not pdf_bytes:
            return None

        text = self._extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for case {case_id}")
            return None

        return {
            "case_id": case_id,
            "court_name": court_name,
            "metadata": metadata,
            "text": text,
        }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch cases since a given date."""
        date_from = since.strftime("%Y-%m-%d")
        for court_config in SEARCH_CONFIGS:
            court_name = court_config["court_name"]
            first_ids, total = self._search_cases(
                court_config, offset=0, date_from=date_from,
            )
            if not first_ids:
                continue
            for case_id in first_ids:
                record = self._process_case(case_id, court_name)
                if record:
                    yield record

            offset = PAGE_SIZE
            while offset < total:
                ids, _ = self._search_cases(
                    court_config, offset=offset, date_from=date_from,
                )
                if not ids:
                    break
                for case_id in ids:
                    record = self._process_case(case_id, court_name)
                    if record:
                        yield record
                offset += PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        """Transform raw case data into standardized schema."""
        case_id = raw["case_id"]
        metadata = raw.get("metadata", {})

        # Extract title from metadata (事件名 = case name)
        title = metadata.get("事件名", metadata.get("裁判年月日", f"Case {case_id}"))

        # Extract date (裁判年月日 = judgment date)
        date_str = metadata.get("裁判年月日", "")
        date = None
        if date_str:
            date = self._parse_japanese_date(date_str)

        # Extract case number (事件番号)
        case_number = metadata.get("事件番号", "")

        # Extract court name (裁判所名)
        court = metadata.get("裁判所名", raw.get("court_name", ""))

        return {
            "_id": f"JP/CourtsGoJp/{case_id}",
            "_source": "JP/CourtsGoJp",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"{BASE_URL}/hanrei/{case_id}/detail2/index.html",
            "case_number": case_number,
            "court": court,
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="JP/CourtsGoJp bootstrap")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    boot_parser = subparsers.add_parser("bootstrap", help="Full bootstrap or sample")
    boot_parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot_parser.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    update_parser = subparsers.add_parser("update", help="Incremental update")
    update_parser.add_argument("--since", required=True, help="ISO date (e.g. 2024-01-01)")

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = CourtsGoJpScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        resp = scraper.session.get(
            f"{BASE_URL}/hanrei/search2/index.html",
            params={"courtCaseType": "1", "filter[recent]": "1"},
            timeout=15,
        )
        logger.info(f"Search page: HTTP {resp.status_code}, {len(resp.content)} bytes")
        # Check for case IDs
        ids = re.findall(r'/(\d+)/detail', resp.text)
        logger.info(f"Found {len(ids)} case IDs on first page")
        if ids:
            # Test PDF download
            pdf_bytes = scraper._download_pdf(ids[0])
            if pdf_bytes:
                text = scraper._extract_text_from_pdf(pdf_bytes)
                logger.info(f"PDF text: {len(text)} chars")
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
