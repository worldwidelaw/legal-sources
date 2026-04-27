#!/usr/bin/env python3
"""
JE/TaxCommissioners -- Jersey Commissioners of Appeal for Taxes Decisions

Fetches anonymised tax appeal determinations from gov.je:
  - Listing page with HTML table linking to PDFs
  - Full text extracted from PDF documents
  - ~10 decisions (2021-2022)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JE.TaxCommissioners")

BASE_URL = "https://www.gov.je"
LISTING_PATH = "/TaxesMoney/IncomeTax/Technical/CommissionerOfAppealTaxes/pages/aboutcommissionerofappealfortaxes.aspx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Month name mapping for parsing dates like "Dec 2021"
MONTHS = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def parse_month_year(text: str) -> Optional[str]:
    """Parse 'Mon YYYY' or 'Month YYYY' to ISO date (first of month)."""
    text = text.strip()
    m = re.match(r"(\w+)\s+(\d{4})", text)
    if not m:
        return None
    month_str = m.group(1).lower()[:3]
    year = m.group(2)
    month_num = MONTHS.get(month_str)
    if not month_num:
        return None
    return f"{year}-{month_num}-01"


def make_decision_id(date_str: Optional[str], subject: str) -> str:
    """Create a stable synthetic ID from date and subject."""
    # Slugify subject
    slug = re.sub(r"[^a-z0-9]+", "-", subject.lower()).strip("-")[:60]
    if date_str:
        return f"COAFT-{date_str[:7]}-{slug}"
    return f"COAFT-{slug}"


class TaxCommissionersScraper(BaseScraper):
    """Scraper for Jersey Tax Commissioners determinations."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _list_decisions(self) -> list[dict]:
        """Scrape the listing page for PDF links and metadata."""
        url = BASE_URL + LISTING_PATH
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return []

        html = resp.text
        decisions = []

        def clean_cell(c: str) -> str:
            c = re.sub(r"<[^>]+>", "", c)
            # Remove zero-width chars and non-breaking spaces
            c = c.replace("\u200b", "").replace("\xa0", " ")
            return unescape(c).strip()

        # The determinations table uses <th> for subject column and <td> for others.
        # Each row with a PDF link is a determination.
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL | re.IGNORECASE)

        for row in rows:
            # Must contain a PDF link
            pdf_match = re.search(r'href="([^"]*\.pdf[^"]*)"', row, re.IGNORECASE)
            if not pdf_match:
                continue

            pdf_path = pdf_match.group(1)
            pdf_url = urljoin(BASE_URL, pdf_path)

            # Extract subject from <th> (first column)
            th_match = re.search(
                r'<th[^>]*>(.*?)</th>',
                row, re.DOTALL | re.IGNORECASE,
            )
            subject = clean_cell(th_match.group(1)) if th_match else ""

            # Extract <td> cells: cells[0] = hearing date, cells[1] = PDF link
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL | re.IGNORECASE)

            hearing_date_raw = clean_cell(cells[0]) if len(cells) > 0 else ""
            date_iso = parse_month_year(hearing_date_raw)
            # Articles are embedded in the subject (th) column itself
            articles = ""

            if not subject:
                # Fallback: derive subject from PDF filename
                subject = unquote(pdf_path.split("/")[-1]).replace(".pdf", "")

            decision_id = make_decision_id(date_iso, subject)
            pdf_filename = unquote(pdf_path.split("/")[-1])

            decisions.append({
                "decision_id": decision_id,
                "title": subject,
                "articles": articles if articles else None,
                "date": date_iso,
                "pdf_url": pdf_url,
                "pdf_filename": pdf_filename,
            })

        logger.info(f"Found {len(decisions)} tax determinations on listing page")
        return decisions

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all tax determinations with full text from PDFs."""
        decisions = self._list_decisions()

        for i, dec in enumerate(decisions):
            time.sleep(1)
            text = extract_pdf_markdown(
                source="JE/TaxCommissioners",
                source_id=dec["decision_id"],
                pdf_url=dec["pdf_url"],
                table="case_law",
            )

            if not text or len(text) < 50:
                logger.warning(f"No text from PDF for {dec['decision_id']}")
                continue

            dec["text"] = text
            dec["url"] = dec["pdf_url"]
            yield dec

            logger.info(f"Processed {i + 1}/{len(decisions)}: {dec['decision_id']}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch all decisions (small corpus, always full refresh)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        return {
            "_id": raw["decision_id"],
            "_source": "JE/TaxCommissioners",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "decision_id": raw["decision_id"],
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "articles": raw.get("articles"),
            "pdf_filename": raw.get("pdf_filename"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to gov.je listing page."""
        try:
            resp = self.session.get(BASE_URL + LISTING_PATH, timeout=30)
            resp.raise_for_status()
            if "Commissioner" in resp.text and ".pdf" in resp.text:
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: unexpected response")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = TaxCommissionersScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
