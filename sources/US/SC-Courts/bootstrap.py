#!/usr/bin/env python3
"""
US/SC-Courts -- South Carolina Supreme Court & Court of Appeals Opinions

Fetches case law from the SC Judicial Branch website (sccourts.org).

Strategy:
  - Scrape monthly listing pages for published SC, published COA,
    and unpublished COA opinions
  - Download opinion PDFs and extract full text
  - Navigate month-by-month using ?term=YYYY-MM parameter

Data Coverage:
  - Supreme Court published opinions (~2000-present)
  - Court of Appeals published opinions (~2000-present)
  - Court of Appeals unpublished opinions (~2000-present)
  - Language: English
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent months)
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.SC-Courts")

BASE_URL = "https://www.sccourts.org"

# Monthly listing page URLs
LISTING_URLS = {
    "sc_published": "/opinions-orders/opinions/published-opinions/supreme-court/",
    "coa_published": "/opinions-orders/opinions/published-opinions/court-of-appeals/",
    "coa_unpublished": "/opinions-orders/opinions/unpublished-opinions/court-of-appeals/",
}

COURT_NAMES = {
    "sc_published": "South Carolina Supreme Court",
    "coa_published": "South Carolina Court of Appeals",
    "coa_unpublished": "South Carolina Court of Appeals (Unpublished)",
}

# Regex patterns to extract opinion data from HTML
CASE_NUMBER_PATTERN = re.compile(
    r'<span[^>]*class="[^"]*case-number[^"]*"[^>]*>\s*([^<]+?)\s*</span>', re.IGNORECASE
)
CASE_NAME_PATTERN = re.compile(
    r'<span[^>]*class="[^"]*case-name[^"]*"[^>]*>\s*([^<]+?)\s*</span>', re.IGNORECASE
)
PDF_LINK_PATTERN = re.compile(
    r'<a[^>]+href="([^"]*?/media/opinions/[^"]*?\.pdf)"[^>]*>', re.IGNORECASE
)
DATE_HEADING_PATTERN = re.compile(
    r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}',
    re.IGNORECASE,
)

# Match an accordion-item block
ACCORDION_ITEM_PATTERN = re.compile(
    r'<div[^>]*class="[^"]*accordion-item[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>',
    re.DOTALL | re.IGNORECASE,
)


def parse_date(date_str: str) -> Optional[str]:
    """Parse date string to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip().rstrip(',')
    for fmt in ["%B %d, %Y", "%B %d %Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def parse_listing_page(html: str) -> List[Dict[str, Any]]:
    """Parse a listing page and extract opinion entries."""
    opinions = []

    # Find date sections
    date_positions = []
    for m in DATE_HEADING_PATTERN.finditer(html):
        date_positions.append((m.start(), parse_date(m.group(0))))

    if not date_positions:
        # Try to extract opinions without date context
        date_positions = [(0, None)]

    for i, (pos, pub_date) in enumerate(date_positions):
        end_pos = date_positions[i + 1][0] if i + 1 < len(date_positions) else len(html)
        section = html[pos:end_pos]

        # Find all PDF links in this section
        pdf_links = list(PDF_LINK_PATTERN.finditer(section))
        case_numbers = list(CASE_NUMBER_PATTERN.finditer(section))
        case_names = list(CASE_NAME_PATTERN.finditer(section))

        # Pair them up - each opinion should have a case number, name, and PDF link
        # Use position ordering to match them
        for j, pdf_match in enumerate(pdf_links):
            pdf_url = pdf_match.group(1)
            if not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url

            case_num = case_numbers[j].group(1).strip() if j < len(case_numbers) else ""
            case_name = case_names[j].group(1).strip() if j < len(case_names) else ""

            if not case_num:
                # Try to extract from PDF filename
                fname = pdf_url.split("/")[-1].replace(".pdf", "")
                case_num = fname

            opinions.append({
                "case_number": case_num,
                "case_name": case_name,
                "pdf_url": pdf_url,
                "publication_date": pub_date,
            })

    return opinions


class SCCourtsScraper(BaseScraper):
    """
    Scraper for US/SC-Courts -- South Carolina Supreme Court & Court of Appeals.
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)"
        })

    def _fetch_listing_page(self, court_key: str, year: int, month: int) -> str:
        """Fetch a monthly listing page."""
        url = f"{BASE_URL}{LISTING_URLS[court_key]}"
        params = {"term": f"{year:04d}-{month:02d}"}
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {court_key} {year}-{month:02d}: {e}")
            return ""

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file."""
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return None

    def _extract_text(self, pdf_data: bytes) -> str:
        """Extract text from PDF."""
        return extract_pdf_markdown(
            source="US/SC-Courts",
            source_id="",
            pdf_bytes=pdf_data,
            table="case_law",
        ) or ""

    def _generate_months(self, start_year: int = 2000, start_month: int = 1):
        """Generate (year, month) tuples from start to current date, newest first."""
        now = datetime.now()
        months = []
        y, m = start_year, start_month
        while (y, m) <= (now.year, now.month):
            months.append((y, m))
            m += 1
            if m > 12:
                m = 1
                y += 1
        months.reverse()
        return months

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all opinions from all courts, all months."""
        months = self._generate_months(start_year=2000)

        for court_key in LISTING_URLS:
            court_name = COURT_NAMES[court_key]
            empty_streak = 0

            for year, month in months:
                logger.info(f"Fetching {court_key} {year}-{month:02d}...")
                html = self._fetch_listing_page(court_key, year, month)

                if not html:
                    empty_streak += 1
                    if empty_streak > 6:
                        logger.info(f"No data for 6+ months, stopping {court_key}")
                        break
                    continue

                opinions = parse_listing_page(html)
                if not opinions:
                    empty_streak += 1
                    if empty_streak > 6:
                        logger.info(f"No opinions for 6+ months, stopping {court_key}")
                        break
                    continue

                empty_streak = 0
                logger.info(f"Found {len(opinions)} opinions for {court_key} {year}-{month:02d}")

                for opinion in opinions:
                    opinion["court"] = court_name
                    delay = self.config.get("fetch", {}).get("delay", 2.0)
                    time.sleep(delay)

                    pdf_data = self._download_pdf(opinion["pdf_url"])
                    if not pdf_data:
                        continue

                    text = self._extract_text(pdf_data)
                    if not text or len(text) < 100:
                        logger.warning(
                            f"Insufficient text for {opinion['case_number']}: "
                            f"{len(text) if text else 0} chars"
                        )
                        continue

                    opinion["text"] = text
                    yield opinion

                time.sleep(1.0)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent opinions (last 3 months)."""
        now = datetime.now()
        months = []
        for offset in range(3):
            m = now.month - offset
            y = now.year
            if m <= 0:
                m += 12
                y -= 1
            months.append((y, m))

        for court_key in LISTING_URLS:
            court_name = COURT_NAMES[court_key]
            for year, month in months:
                html = self._fetch_listing_page(court_key, year, month)
                if not html:
                    continue

                opinions = parse_listing_page(html)
                for opinion in opinions:
                    if since and opinion.get("publication_date") and opinion["publication_date"] < since:
                        continue
                    opinion["court"] = court_name
                    time.sleep(self.config.get("fetch", {}).get("delay", 2.0))

                    pdf_data = self._download_pdf(opinion["pdf_url"])
                    if not pdf_data:
                        continue

                    text = self._extract_text(pdf_data)
                    if not text or len(text) < 100:
                        continue

                    opinion["text"] = text
                    yield opinion

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw opinion record."""
        case_num = raw.get("case_number", "")
        court = raw.get("court", "")

        if "Supreme" in court:
            court_abbr = "SC"
        elif "Unpublished" in court:
            court_abbr = "COA-UP"
        else:
            court_abbr = "COA"

        doc_id = f"US-SC-{court_abbr}-{case_num}"

        return {
            "_id": doc_id,
            "_source": "US/SC-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{raw.get('case_name', '')} ({case_num})",
            "text": raw.get("text", ""),
            "date": raw.get("publication_date"),
            "url": raw.get("pdf_url", ""),
            "case_number": case_num,
            "court": court,
            "jurisdiction": "US-SC",
        }

    def test_connection(self) -> bool:
        """Test that listing pages are accessible."""
        try:
            html = self._fetch_listing_page("sc_published", 2026, 3)
            opinions = parse_listing_page(html)
            logger.info(f"Connection test: found {len(opinions)} SC opinions for 2026-03")
            return len(opinions) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/SC-Courts data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch small sample")
    parser.add_argument("--since", help="ISO date for incremental updates (YYYY-MM-DD)")
    args = parser.parse_args()

    scraper = SCCourtsScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999

        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record['_id']}: {record['title'][:60]} "
                f"({text_len} chars)"
            )
            count += 1
            if count >= target:
                break

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates(since=args.since):
            record = scraper.normalize(raw)
            safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
