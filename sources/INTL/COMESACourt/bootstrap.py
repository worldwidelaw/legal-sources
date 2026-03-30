#!/usr/bin/env python3
"""
INTL/COMESACourt -- COMESA Court of Justice

Fetches judgments, rulings, and orders from the COMESA Court of Justice.
~39 decisions from 2000-2025 covering 21 Eastern/Southern African states.

Strategy:
  - Scrape HTML table from court-decisions page
  - Download PDFs (direct wp-content URLs or /download/ endpoints)
  - Extract full text via PyMuPDF
  - Normalize to standard schema

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap --full     # Full fetch
"""

import re
import sys
import json
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests
from bs4 import BeautifulSoup
import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.COMESACourt")

DECISIONS_URL = "https://comesacourt.org/court-decisions/"
SOURCE_ID = "INTL/COMESACourt"


class COMESACourtScraper(BaseScraper):
    """
    Scraper for INTL/COMESACourt -- COMESA Court of Justice.
    Country: INTL
    URL: https://comesacourt.org/court-decisions/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _scrape_decisions_table(self) -> list[dict]:
        """Scrape the court decisions HTML table for metadata + PDF URLs."""
        logger.info(f"Fetching decisions table from {DECISIONS_URL}")
        resp = self.session.get(DECISIONS_URL, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table")
        if not table:
            logger.error("No table found on court-decisions page")
            return []

        rows = table.find_all("tr")
        decisions = []

        for row in rows[1:]:  # skip header row
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            link = cells[0].find("a", href=True)
            if not link:
                continue

            title = link.get_text(strip=True)
            pdf_url = link["href"]
            case_number = cells[1].get_text(strip=True)
            classification = cells[2].get_text(strip=True)
            year = cells[3].get_text(strip=True)

            if classification == "-----":
                classification = None

            decisions.append({
                "title": title,
                "pdf_url": pdf_url,
                "case_number": case_number,
                "classification": classification,
                "year": year,
            })

        logger.info(f"Found {len(decisions)} decisions in table")
        return decisions

    def _download_pdf_text(self, url: str) -> str:
        """Download PDF from URL and extract text using PyMuPDF."""
        # Ensure HTTPS
        if url.startswith("http://"):
            url = "https://" + url[7:]

        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()

        content = resp.content
        if len(content) < 100:
            logger.warning(f"PDF too small ({len(content)} bytes): {url}")
            return ""

        try:
            doc = fitz.open(stream=content, filetype="pdf")
            text_parts = []
            for page in doc:
                text_parts.append(page.get_text())
            doc.close()
            text = "\n".join(text_parts).strip()
            # Clean up excessive whitespace
            text = re.sub(r"\n{3,}", "\n\n", text)
            return text
        except Exception as e:
            logger.error(f"PDF extraction failed for {url}: {e}")
            return ""

    def _make_id(self, decision: dict) -> str:
        """Generate a stable unique ID from case metadata."""
        key = f"{decision['case_number']}|{decision['title']}"
        return f"comesa-{hashlib.md5(key.encode()).hexdigest()[:12]}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions with PDF text."""
        decisions = self._scrape_decisions_table()

        for i, decision in enumerate(decisions):
            logger.info(f"[{i+1}/{len(decisions)}] Downloading: {decision['title'][:60]}...")
            time.sleep(1.0)

            try:
                text = self._download_pdf_text(decision["pdf_url"])
            except Exception as e:
                logger.error(f"Failed to download {decision['pdf_url']}: {e}")
                continue

            if not text:
                logger.warning(f"No text extracted for: {decision['title'][:60]}")
                continue

            decision["text"] = text
            yield decision

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions added since a date (re-scrapes full table, filters by year)."""
        since_year = since.year
        decisions = self._scrape_decisions_table()

        for decision in decisions:
            try:
                year = int(decision.get("year", "0"))
            except ValueError:
                continue
            if year >= since_year:
                time.sleep(1.0)
                try:
                    text = self._download_pdf_text(decision["pdf_url"])
                except Exception:
                    continue
                if text:
                    decision["text"] = text
                    yield decision

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw decision record to standard schema."""
        text = raw.get("text", "")
        if not text:
            return None

        title = raw.get("title", "").strip()
        case_number = raw.get("case_number", "").strip()
        year = raw.get("year", "").strip()

        # Try to parse a more specific date from the PDF text
        date_str = None
        if year:
            date_str = f"{year}-01-01"

        # Look for date patterns in text (e.g., "Delivered on 4th February 2025")
        date_match = re.search(
            r"(?:delivered|dated|decided|this)\s+(?:on\s+)?(\d{1,2})\w*\s+"
            r"(January|February|March|April|May|June|July|August|September|October|November|December)"
            r"\s+(\d{4})",
            text, re.IGNORECASE
        )
        if date_match:
            day, month_name, yr = date_match.groups()
            months = {
                "january": "01", "february": "02", "march": "03", "april": "04",
                "may": "05", "june": "06", "july": "07", "august": "08",
                "september": "09", "october": "10", "november": "11", "december": "12",
            }
            m = months.get(month_name.lower())
            if m:
                date_str = f"{yr}-{m}-{int(day):02d}"

        pdf_url = raw.get("pdf_url", "")
        if pdf_url.startswith("http://"):
            pdf_url = "https://" + pdf_url[7:]

        return {
            "_id": self._make_id(raw),
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": pdf_url,
            "case_number": case_number,
            "classification": raw.get("classification"),
            "court": "COMESA Court of Justice",
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/COMESACourt Data Fetcher")
    subparsers = parser.add_subparsers(dest="command")

    boot = subparsers.add_parser("bootstrap", help="Fetch data")
    boot.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = COMESACourtScraper()

    if args.command == "bootstrap":
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        elif args.full:
            stats = scraper.bootstrap(sample_mode=False)
        else:
            parser.print_help()
            return
        logger.info(f"Bootstrap stats: {json.dumps(stats, indent=2)}")
    elif args.command == "test":
        logger.info("Testing connectivity...")
        decisions = scraper._scrape_decisions_table()
        logger.info(f"Found {len(decisions)} decisions")
        if decisions:
            logger.info(f"First: {decisions[0]['title'][:60]}")
            text = scraper._download_pdf_text(decisions[0]["pdf_url"])
            logger.info(f"PDF text length: {len(text)} chars")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
