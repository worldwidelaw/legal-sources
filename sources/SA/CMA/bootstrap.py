#!/usr/bin/env python3
"""
SA/CMA -- Saudi Capital Market Authority Regulations

Fetches implementing regulations, rules, and instructions from the CMA
website (cma.gov.sa). Parses the regulations list page to extract PDF
links, downloads each PDF, and extracts full text.

Strategy:
  - GET the regulations list page and parse HTML for data-id cards
  - Each card has a data-title, data-year, data-month, data-category
  - Extract the PDF download URL from each card
  - Also fetch the Capital Market Law PDF separately
  - Download PDFs and extract text via common.pdf_extract

Data:
  - ~36 implementing regulations (instructions, rules, procedures, guides)
  - 1 Capital Market Law
  - All PDFs in English, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import logging
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SA.CMA")

BASE_URL = "https://cma.gov.sa"
REGULATIONS_PAGE = f"{BASE_URL}/en/RulesRegulations/Regulations/Pages/default.aspx"
CMA_LAW_PDF = f"{BASE_URL}/en/RulesRegulations/CMALaw/Documents/CMA_Law.pdf"

# Some PDF links use cma.org.sa, which redirects to cma.gov.sa -- both work
PDF_BASE = "https://cma.org.sa/en/RulesRegulations/Regulations/Documents/"


class CMAScraper(BaseScraper):
    """Scraper for SA/CMA -- Saudi CMA Regulations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _fetch_regulations_page(self) -> str:
        """Fetch the regulations list page HTML."""
        self.rate_limiter.wait()
        resp = self.session.get(REGULATIONS_PAGE, timeout=60)
        resp.raise_for_status()
        return resp.text

    def _parse_regulation_cards(self, html: str) -> list[dict]:
        """Parse regulation cards from the HTML page.

        Each card is a div with data-id, data-title, data-year, data-month,
        data-category attributes, containing a PDF download link.
        """
        results = []

        # Find all card divs with data attributes
        card_pattern = re.compile(
            r'<div[^>]*'
            r'data-id="(\d+)"[^>]*'
            r'data-title="([^"]+)"[^>]*'
            r'data-year="(\d+)"[^>]*'
            r'data-month="(\d+)"[^>]*'
            r'data-category="([^"]+)"[^>]*>',
            re.DOTALL
        )

        for match in card_pattern.finditer(html):
            data_id = match.group(1)
            title = unescape(match.group(2))
            year = match.group(3)
            month = match.group(4).zfill(2)
            category = unescape(match.group(5))

            # Find the PDF link in the surrounding context (next ~6000 chars)
            start = match.end()
            context = html[start:start + 6000]
            pdf_match = re.search(r'href="([^"]*\.pdf)"', context)
            if not pdf_match:
                logger.warning("No PDF link for regulation %s: %s", data_id, title[:60])
                continue

            pdf_url = pdf_match.group(1)
            # Resolve relative URLs
            if pdf_url.startswith("/"):
                pdf_url = BASE_URL + pdf_url
            elif not pdf_url.startswith("http"):
                pdf_url = PDF_BASE + pdf_url

            # Find detail page code
            detail_match = re.search(r'details\.aspx\?code=(\d+)', context)
            code = detail_match.group(1) if detail_match else data_id

            results.append({
                "data_id": data_id,
                "code": code,
                "title": title,
                "year": year,
                "month": month,
                "category": category,
                "pdf_url": pdf_url,
            })

        return results

    def _make_id(self, data_id: str, title: str) -> str:
        """Generate a unique ID from data-id and title slug."""
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", title[:60]).strip("-").lower()
        return f"cma-{data_id}-{slug}"

    def _download_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download PDF and extract text."""
        return extract_pdf_markdown(
            source="SA/CMA",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="legislation",
            force=True,
        ) or ""

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": "SA/CMA",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def _process_regulation(self, reg: dict) -> Optional[dict]:
        """Process a single regulation: download PDF and extract text."""
        doc_id = self._make_id(reg["data_id"], reg["title"])
        pdf_url = reg["pdf_url"]

        logger.info("Downloading: %s", reg["title"][:80])
        text = self._download_pdf_text(pdf_url, doc_id)
        if not text or len(text) < 100:
            logger.warning("Insufficient text (%d chars) for: %s",
                           len(text) if text else 0, reg["title"][:60])
            return None

        date_str = f"{reg['year']}-{reg['month']}-01"

        raw = {
            "_id": doc_id,
            "title": reg["title"],
            "text": text,
            "date": date_str,
            "url": pdf_url,
            "category": reg["category"],
        }
        return self.normalize(raw)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all CMA regulations."""
        # 1. Fetch the Capital Market Law
        logger.info("Fetching Capital Market Law...")
        law_id = "cma-law-capital-market-law"
        law_text = self._download_pdf_text(CMA_LAW_PDF, law_id)
        if law_text and len(law_text) >= 100:
            yield self.normalize({
                "_id": law_id,
                "title": "Capital Market Law (Royal Decree No. M/30)",
                "text": law_text,
                "date": "2003-07-31",
                "url": CMA_LAW_PDF,
                "category": "Law",
            })
        else:
            logger.warning("Could not extract Capital Market Law text")

        # 2. Fetch implementing regulations
        logger.info("Fetching regulations list page...")
        html = self._fetch_regulations_page()
        regulations = self._parse_regulation_cards(html)
        logger.info("Found %d regulations on the page", len(regulations))

        for reg in regulations:
            record = self._process_regulation(reg)
            if record:
                yield record

    def fetch_updates(self, since=None):
        """Fetch all (no incremental update available)."""
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            html = self._fetch_regulations_page()
            regs = self._parse_regulation_cards(html)
            logger.info("Connection OK: %d regulations found", len(regs))
            return len(regs) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SA/CMA Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CMAScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
