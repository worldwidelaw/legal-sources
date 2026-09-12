#!/usr/bin/env python3
"""
INTL/EACJ-AfricanLII -- East African Court of Justice via AfricanLII

Fetches ~390 judgments (2006-2026) from AfricanLII's EACJ collection.
Full text extracted from PDF downloads.

Strategy:
  - Scrape paginated listing for judgment links and metadata
  - Download PDF via /source endpoint
  - Extract full text via pdfplumber
  - Fetch detail page for judges metadata

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
logger = logging.getLogger("legal-data-hunter.INTL.EACJ-AfricanLII")

BASE_URL = "https://africanlii.org"
LISTING_URL = f"{BASE_URL}/en/judgments/EACJ/"
SOURCE_ID = "INTL/EACJ-AfricanLII"


class EACJAfricanLIIScraper(BaseScraper):
    """
    Scraper for East African Court of Justice judgments via AfricanLII.
    Country: INTL
    URL: https://africanlii.org/en/judgments/EACJ/
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

    def _scrape_listing(self) -> list:
        """Scrape all pages of the AfricanLII EACJ listing."""
        all_judgments = []

        for page_num in range(1, 20):
            url = LISTING_URL if page_num == 1 else f"{LISTING_URL}?page={page_num}"
            logger.info(f"Fetching listing page {page_num}: {url}")

            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch page {page_num}: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            page_judgments = []

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/judgment/" not in href or "eacj" not in href.lower():
                    continue

                title_text = a.get_text(strip=True)
                if not title_text:
                    continue

                full_url = href if href.startswith("http") else BASE_URL + href

                citation = None
                date_str = None
                citation_match = re.search(
                    r"\[(\d{4})\]\s+EACJ\s+\d+\s+\((\d{1,2}\s+\w+\s+\d{4})\)",
                    title_text,
                )
                if citation_match:
                    citation = citation_match.group(0)
                    date_str = self._parse_date(citation_match.group(2))

                case_match = re.search(
                    r"\(((?:Reference|Appeal|Application|Ruling|Revision|Taxation|Tax|Review|Misc\.?\s*(?:Application|Civil))\s+No\.?\s*\d+\s+of\s+\d{4}(?:\s*;\s*\S.*?)?)\)",
                    title_text, re.IGNORECASE,
                )
                case_number = case_match.group(1) if case_match else None

                title = title_text
                if citation:
                    title = title_text.replace(citation, "").strip().rstrip("(").strip()

                page_judgments.append({
                    "url": full_url,
                    "pdf_url": full_url + "/source",
                    "title": title,
                    "citation": citation,
                    "case_number": case_number,
                    "date": date_str,
                })

            if not page_judgments:
                break

            all_judgments.extend(page_judgments)
            time.sleep(1.0)

        logger.info(f"Found {len(all_judgments)} judgments total across all pages")
        return all_judgments

    def _parse_date(self, date_raw: str) -> Optional[str]:
        """Parse a date string like '31 March 2026' to ISO format."""
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
        """Fetch the detail page for judges metadata."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Could not fetch detail page {url}: {e}")
            return {}

        soup = BeautifulSoup(resp.text, "html.parser")
        meta = {}

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
        """Generate a stable unique ID from the AKN URL path."""
        url = judgment.get("url", "")
        match = re.search(r"/akn/(.+/eacj/\d{4}/\d+)", url)
        if match:
            return f"eacj-{match.group(1).replace('/', '-')}"
        key = judgment.get("citation", "") or judgment.get("title", "")
        return f"eacj-{hashlib.md5(key.encode()).hexdigest()[:12]}"

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
            "court": "East African Court of Justice",
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/EACJ-AfricanLII Data Fetcher")
    subparsers = parser.add_subparsers(dest="command")

    boot = subparsers.add_parser("bootstrap", help="Fetch data")
    boot.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = EACJAfricanLIIScraper()

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
