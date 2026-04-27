#!/usr/bin/env python3
"""
AE/ADGM-Courts -- Abu Dhabi Global Market Courts Judgments Fetcher

Fetches case law judgments from the ADGM Courts (CFI + Court of Appeal)
by scraping the server-side rendered HTML listings page and downloading
full-text judgment PDFs from assets.adgm.com.

Strategy:
  - Bootstrap: Paginate through SSR HTML pages (psize=50), extract rows,
    download judgment PDFs for full text.
  - Update: Filter by date range using fromdate/todate params.
  - Sample: Fetch 10+ records with full text for validation.

Website: https://www.adgm.com/adgm-courts/judgments
PDFs: https://assets.adgm.com/download/assets/...

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from html import unescape
from typing import Generator, Optional
from urllib.parse import unquote

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
logger = logging.getLogger("legal-data-hunter.AE.ADGM-Courts")

BASE_URL = "https://www.adgm.com/adgm-courts/judgments"

# Months for date parsing
MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def parse_adgm_date(date_str: str) -> str:
    """Parse dates like '06 Mar 2026' to '2026-03-06'."""
    if not date_str:
        return ""
    date_str = date_str.strip()
    match = re.match(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", date_str)
    if match:
        day, mon, year = match.groups()
        mon_num = MONTHS.get(mon.lower(), "01")
        return f"{year}-{mon_num}-{day.zfill(2)}"
    return date_str


class ADGMCourtsScraper(BaseScraper):
    """
    Scraper for AE/ADGM-Courts -- ADGM Courts Judgments.
    Country: AE
    URL: https://www.adgm.com/adgm-courts/judgments

    Data types: case_law
    Auth: none (public SSR HTML + PDF downloads)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

        self.pdf_client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,
        )

    # -- HTML parsing helpers ------------------------------------------------

    def _fetch_page(self, page: int = 1, psize: int = 50, **extra) -> str:
        """Fetch a page of judgments HTML."""
        params = {
            "page": page,
            "psize": psize,
            "sortby": "date#desc",
        }
        params.update(extra)
        self.rate_limiter.wait()
        resp = self.client.get(BASE_URL, params=params)
        resp.raise_for_status()
        return resp.text

    def _get_total_items(self, html: str) -> int:
        """Extract total-items from <adgm-pagination> element."""
        match = re.search(r'total-items="(\d+)"', html)
        return int(match.group(1)) if match else 0

    def _parse_rows(self, html: str) -> list[dict]:
        """Parse judgment rows from HTML."""
        rows = []
        # Find all data rows (skip header row which has adgm-table-header-cell)
        row_pattern = re.compile(
            r'<adgm-table-row>\s*'
            r'<adgm-table-cell[^>]*>(.*?)</adgm-table-cell>\s*'  # date
            r'<adgm-table-cell[^>]*>(.*?)</adgm-table-cell>\s*'  # case number
            r'<adgm-table-cell[^>]*>(.*?)</adgm-table-cell>\s*'  # case name
            r'<adgm-table-cell[^>]*>(.*?)</adgm-table-cell>\s*'  # neutral citation + PDF link
            r'<adgm-table-cell[^>]*>(.*?)</adgm-table-cell>\s*'  # summary
            r'</adgm-table-row>',
            re.DOTALL
        )

        for m in row_pattern.finditer(html):
            date_raw = m.group(1).strip()
            case_number = m.group(2).strip()
            case_name = unescape(m.group(3).strip())
            citation_cell = m.group(4).strip()
            summary_cell = m.group(5).strip()

            # Extract judgment PDF URL and neutral citation from citation cell
            pdf_url = ""
            neutral_citation = ""
            link_match = re.search(r'href="([^"]+)"[^>]*>([^<]+)</a>', citation_cell)
            if link_match:
                pdf_url = unescape(link_match.group(1))
                neutral_citation = link_match.group(2).strip()

            # Extract summary PDF URL
            summary_pdf_url = ""
            summary_match = re.search(r'href="([^"]+)"', summary_cell)
            if summary_match:
                summary_pdf_url = unescape(summary_match.group(1))

            # Determine court from case number or citation
            court = "Court of First Instance"
            if "ADGMCA" in case_number or "ADGMCA" in neutral_citation:
                court = "Court of Appeal"

            rows.append({
                "date_raw": date_raw,
                "case_number": case_number,
                "case_name": case_name,
                "neutral_citation": neutral_citation,
                "pdf_url": pdf_url,
                "summary_pdf_url": summary_pdf_url,
                "court": court,
            })

        return rows

    def _extract_pdf_text(self, pdf_url: str, source_id: str) -> str:
        """Extract text from judgment PDF."""
        if not pdf_url:
            return ""
        return extract_pdf_markdown(
            source="AE/ADGM-Courts",
            source_id=source_id,
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments from ADGM Courts."""
        # Get first page to determine total
        html = self._fetch_page(page=1, psize=50)
        total = self._get_total_items(html)
        logger.info(f"Total judgments: {total}")

        total_pages = (total + 49) // 50  # ceil division

        # Parse first page
        rows = self._parse_rows(html)
        logger.info(f"Page 1/{total_pages}: {len(rows)} rows")
        for row in rows:
            yield row

        # Fetch remaining pages
        for page in range(2, total_pages + 1):
            time.sleep(1)
            try:
                html = self._fetch_page(page=page, psize=50)
                rows = self._parse_rows(html)
                logger.info(f"Page {page}/{total_pages}: {len(rows)} rows")
                for row in rows:
                    yield row
            except Exception as e:
                logger.error(f"Error on page {page}: {e}")
                time.sleep(5)
                try:
                    html = self._fetch_page(page=page, psize=50)
                    rows = self._parse_rows(html)
                    for row in rows:
                        yield row
                except Exception as e2:
                    logger.error(f"Retry failed for page {page}: {e2}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments published since given date."""
        since_str = since.strftime("%d %b %Y")
        logger.info(f"Fetching updates since {since_str}")
        # Use the same pagination but check dates
        for raw in self.fetch_all():
            date_str = parse_adgm_date(raw.get("date_raw", ""))
            if date_str:
                try:
                    doc_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                    if doc_date >= since:
                        yield raw
                    else:
                        # Past the cutoff, stop
                        return
                except ValueError:
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> dict:
        """Transform raw parsed row into standard schema."""
        case_number = raw.get("case_number", "")
        pdf_url = raw.get("pdf_url", "")
        source_id = f"ADGM-{case_number}" if case_number else ""

        # Extract full text from PDF
        full_text = self._extract_pdf_text(pdf_url, source_id)

        date_str = parse_adgm_date(raw.get("date_raw", ""))

        return {
            "_id": source_id,
            "_source": "AE/ADGM-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name", ""),
            "text": full_text,
            "date": date_str,
            "url": pdf_url,
            "case_number": case_number,
            "neutral_citation": raw.get("neutral_citation", ""),
            "court": raw.get("court", ""),
            "summary_pdf_url": raw.get("summary_pdf_url", ""),
            "country": "AE",
            "language": "en",
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing ADGM Courts judgments page...")

        html = self._fetch_page(page=1, psize=3)
        total = self._get_total_items(html)
        print(f"  Total judgments: {total}")

        rows = self._parse_rows(html)
        print(f"  Rows parsed from page 1: {len(rows)}")

        if rows:
            row = rows[0]
            print(f"  First judgment:")
            print(f"    Date: {row['date_raw']}")
            print(f"    Case: {row['case_number']}")
            print(f"    Name: {row['case_name'][:80]}")
            print(f"    Citation: {row['neutral_citation']}")
            print(f"    PDF: {row['pdf_url'][:100]}...")

            # Test PDF download
            print("\n  Testing PDF text extraction...")
            text = self._extract_pdf_text(
                row["pdf_url"], f"ADGM-{row['case_number']}"
            )
            if text:
                print(f"  PDF extraction: SUCCESS ({len(text)} chars)")
                print(f"  Sample: {text[:200]}...")
            else:
                print("  PDF extraction: FAILED (no text returned)")

        print("\nTest completed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = ADGMCourtsScraper()

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
