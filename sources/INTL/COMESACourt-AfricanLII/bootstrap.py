#!/usr/bin/env python3
"""
INTL/COMESACourt-AfricanLII -- COMESA Court of Justice via AfricanLII

Fetches ~45 judgments (2001-2025) from AfricanLII's COMESA Court collection.
Full text extracted from PDF downloads.

Strategy:
  - Scrape listing page for judgment links and metadata
  - Fetch each judgment detail page for citation, judges, date
  - Download PDF via /source endpoint
  - Extract full text via pdfplumber

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap --full     # Full fetch
"""

import io
import re
import sys
import json
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.COMESACourt-AfricanLII")

BASE_URL = "https://africanlii.org"
LISTING_URL = f"{BASE_URL}/judgments/COMESACJ/"
SOURCE_ID = "INTL/COMESACourt-AfricanLII"


class COMESACourtAfricanLIIScraper(BaseScraper):
    """
    Scraper for COMESA Court of Justice judgments via AfricanLII.
    Country: INTL
    URL: https://africanlii.org/judgments/COMESACJ/
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

    def _scrape_listing(self) -> list[dict]:
        """Scrape the AfricanLII listing page for judgment links and metadata."""
        logger.info(f"Fetching listing from {LISTING_URL}")
        resp = self.session.get(LISTING_URL, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        judgments = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/judgment/comesacj/" not in href:
                continue

            title_text = a.get_text(strip=True)
            if not title_text:
                continue

            full_url = href if href.startswith("http") else BASE_URL + href

            # Parse citation from title text, e.g. "[2025] COMESACJ 3 (7 November 2025)"
            citation = None
            date_str = None
            citation_match = re.search(
                r"\[(\d{4})\]\s+COMESACJ\s+\d+\s+\((\d{1,2}\s+\w+\s+\d{4})\)",
                title_text,
            )
            if citation_match:
                citation = citation_match.group(0)
                date_raw = citation_match.group(2)
                date_str = self._parse_date(date_raw)

            # Extract case number from title, e.g. "(Reference No. 1 of 2025)"
            case_match = re.search(
                r"\(((?:Reference|Appeal|Application)\s+No\.?\s*\d+\s+of\s+\d{4})\)",
                title_text, re.IGNORECASE,
            )
            case_number = case_match.group(1) if case_match else None

            # Clean title: remove citation portion
            title = title_text
            if citation:
                title = title_text.replace(citation, "").strip().rstrip("(").strip()

            judgments.append({
                "url": full_url,
                "pdf_url": full_url + "/source",
                "title": title,
                "citation": citation,
                "case_number": case_number,
                "date": date_str,
            })

        logger.info(f"Found {len(judgments)} judgments on listing page")
        return judgments

    def _parse_date(self, date_raw: str) -> Optional[str]:
        """Parse a date string like '7 November 2025' to ISO format."""
        months = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        match = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_raw.strip())
        if match:
            day, month_name, year = match.groups()
            m = months.get(month_name.lower())
            if m:
                return f"{year}-{m}-{int(day):02d}"
        return None

    def _fetch_detail_metadata(self, url: str) -> dict:
        """Fetch the detail page for extra metadata (judges, language)."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Could not fetch detail page {url}: {e}")
            return {}

        soup = BeautifulSoup(resp.text, "html.parser")
        meta = {}

        # Look for judges
        for dt in soup.find_all(["dt", "th", "strong", "b"]):
            text = dt.get_text(strip=True).lower()
            sibling = dt.find_next_sibling(["dd", "td"])
            if not sibling:
                continue
            val = sibling.get_text(strip=True)
            if "judge" in text:
                meta["judges"] = val
            elif "language" in text:
                meta["language"] = val

        return meta

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text via pdfplumber."""
        resp = self.session.get(pdf_url, timeout=120)
        resp.raise_for_status()

        if len(resp.content) < 500:
            logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
            return ""

        text_parts = []
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass

        return "\n\n".join(text_parts)

    def _make_id(self, judgment: dict) -> str:
        """Generate a stable unique ID from the URL path."""
        # Use the AKN path as the stable identifier
        url = judgment.get("url", "")
        # Extract /akn/.../comesacj/YYYY/N part
        match = re.search(r"/akn/(.+/comesacj/\d{4}/\d+)", url)
        if match:
            return f"comesacj-{match.group(1).replace('/', '-')}"
        key = judgment.get("citation", "") or judgment.get("title", "")
        return f"comesacj-{hashlib.md5(key.encode()).hexdigest()[:12]}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments with full PDF text."""
        judgments = self._scrape_listing()

        for i, judgment in enumerate(judgments):
            logger.info(f"[{i+1}/{len(judgments)}] {judgment['title'][:70]}...")
            time.sleep(1.5)

            try:
                text = self._download_pdf_text(judgment["pdf_url"])
            except Exception as e:
                logger.error(f"PDF download failed for {judgment['pdf_url']}: {e}")
                continue

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text ({len(text)} chars): {judgment['title'][:60]}")
                continue

            # Fetch detail metadata (judges, language) — light request
            time.sleep(0.5)
            detail = self._fetch_detail_metadata(judgment["url"])
            judgment.update(detail)
            judgment["text"] = text
            yield judgment

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments added since a date."""
        since_year = since.year
        for judgment in self.fetch_all():
            date = judgment.get("date", "")
            if date and date[:4].isdigit() and int(date[:4]) >= since_year:
                yield judgment

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw judgment to standard schema."""
        text = raw.get("text", "")
        if not text:
            return None

        return {
            "_id": self._make_id(raw),
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", "").strip(),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "citation": raw.get("citation"),
            "case_number": raw.get("case_number"),
            "judges": raw.get("judges"),
            "language": raw.get("language", "English"),
            "court": "COMESA Court of Justice",
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/COMESACourt-AfricanLII Data Fetcher")
    subparsers = parser.add_subparsers(dest="command")

    boot = subparsers.add_parser("bootstrap", help="Fetch data")
    boot.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = COMESACourtAfricanLIIScraper()

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
        judgments = scraper._scrape_listing()
        logger.info(f"Found {len(judgments)} judgments")
        if judgments:
            logger.info(f"First: {judgments[0]['title'][:80]}")
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
