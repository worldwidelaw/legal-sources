#!/usr/bin/env python3
"""
INTL/AfCHPR -- African Court on Human and Peoples' Rights

Fetches judgments, rulings, orders, and advisory opinions from AfricanLII.

Strategy:
  - Scrape listing pages from africanlii.org/en/judgments/AfCHPR/
  - Download PDFs from {akn_url}/source.pdf
  - Extract full text using PyMuPDF
  - ~479 decisions

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.AfCHPR")

BASE_URL = "https://africanlii.org"
LISTING_URL = f"{BASE_URL}/en/judgments/AfCHPR/"


class AfCHPRScraper(BaseScraper):
    """
    Scraper for INTL/AfCHPR -- African Court on Human and Peoples' Rights.
    Country: INTL
    URL: https://africanlii.org/en/judgments/AfCHPR/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _parse_listing_page(self, page: int = 0) -> list[dict]:
        """Parse a single AfricanLII listing page for judgment metadata."""
        url = f"{LISTING_URL}?page={page}" if page > 0 else LISTING_URL
        logger.info(f"Fetching listing page {page}: {url}")
        r = self.session.get(url, timeout=60)
        if r.status_code == 404:
            logger.info(f"Page {page} returned 404 — end of listing")
            return []
        r.raise_for_status()
        html = r.text

        entries = []
        # Extract judgment links and titles
        # Pattern: href="/en/akn/aa-au/judgment/afchpr/YEAR/NUM/eng@DATE"
        links = re.findall(
            r'href="(/en/akn/aa-au/judgment/afchpr/\d{4}/\d+/eng@[\d-]+)"[^>]*>\s*([^<]+)<',
            html,
        )

        seen = set()
        for path, title_raw in links:
            if path in seen:
                continue
            seen.add(path)

            title = html.unescape(title_raw.strip())
            if not title:
                continue

            # Parse metadata from title
            # e.g. "Falana v The African Union (Application No. 001/2011) [2011] AfCHPR 1 (26 June 2011)"
            case_number = ""
            m = re.search(r'\(Application No\.\s*([^)]+)\)', title)
            if m:
                case_number = f"Application No. {m.group(1).strip()}"

            citation = ""
            m = re.search(r'\[(\d{4})\]\s*AfCHPR\s*(\d+)', title)
            if m:
                citation = f"[{m.group(1)}] AfCHPR {m.group(2)}"

            # Extract date from URL path (eng@YYYY-MM-DD)
            date_match = re.search(r'eng@(\d{4}-\d{2}-\d{2})', path)
            date = date_match.group(1) if date_match else None

            # Extract parties from title (everything before the first parenthesis)
            parties = re.split(r'\s*\(', title)[0].strip()

            entries.append({
                "akn_path": path,
                "title": title,
                "parties": parties,
                "case_number": case_number,
                "citation": citation,
                "date": date,
                "pdf_url": f"{BASE_URL}{path}/source.pdf",
                "page_url": f"{BASE_URL}{path}",
            })

        logger.info(f"Page {page}: found {len(entries)} judgments")
        return entries

    def _get_all_entries(self, max_pages: int = 20) -> list[dict]:
        """Scrape all listing pages."""
        all_entries = []
        for page in range(0, max_pages):
            entries = self._parse_listing_page(page)
            if not entries:
                logger.info(f"No entries on page {page}, stopping")
                break
            all_entries.extend(entries)
            time.sleep(1.5)
        logger.info(f"Total entries found: {len(all_entries)}")
        return all_entries

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using PyMuPDF."""
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            pages = []
            for page in doc:
                text = page.get_text()
                if text.strip():
                    pages.append(text.strip())
            doc.close()
            return "\n\n".join(pages)
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text."""
        try:
            r = self.session.get(pdf_url, timeout=120)
            if r.status_code != 200:
                logger.warning(f"PDF download failed ({r.status_code}): {pdf_url}")
                return ""
            return self._extract_text_from_pdf(r.content)
        except Exception as e:
            logger.warning(f"PDF download error for {pdf_url}: {e}")
            return ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw entry into standard schema."""
        return {
            "_id": raw.get("akn_path", "").replace("/en/akn/", ""),
            "_source": "INTL/AfCHPR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("page_url", ""),
            "parties": raw.get("parties", ""),
            "case_number": raw.get("case_number", ""),
            "citation": raw.get("citation", ""),
            "court": "African Court on Human and Peoples' Rights",
            "akn_uri": raw.get("akn_path", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions with full text (raw dicts for normalize)."""
        entries = self._get_all_entries()
        for i, entry in enumerate(entries):
            logger.info(f"[{i+1}/{len(entries)}] Downloading: {entry['title'][:80]}")
            text = self._download_pdf_text(entry["pdf_url"])
            if not text:
                logger.warning(f"No text extracted for: {entry['title'][:60]}")
                continue
            entry["text"] = text
            yield entry
            time.sleep(1.5)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield decisions newer than `since` (ISO date string)."""
        since_date = datetime.fromisoformat(since).date()
        for page in range(0, 3):
            entries = self._parse_listing_page(page)
            if not entries:
                break
            found_old = False
            for entry in entries:
                if entry["date"]:
                    entry_date = datetime.strptime(entry["date"], "%Y-%m-%d").date()
                    if entry_date < since_date:
                        found_old = True
                        continue
                logger.info(f"Update: {entry['title'][:80]}")
                text = self._download_pdf_text(entry["pdf_url"])
                if not text:
                    continue
                entry["text"] = text
                yield entry
                time.sleep(1.5)
            if found_old:
                break
            time.sleep(1.5)

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="INTL/AfCHPR -- African Court on Human and Peoples' Rights"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = AfCHPRScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            entries = scraper._parse_listing_page(0)
            logger.info(f"OK: Found {len(entries)} entries on first page")
            if entries:
                logger.info(f"First: {entries[0]['title'][:80]}")
                pdf_url = entries[0]["pdf_url"]
                logger.info(f"Testing PDF download: {pdf_url}")
                text = scraper._download_pdf_text(pdf_url)
                if text:
                    logger.info(f"PDF text extracted: {len(text)} chars")
                    logger.info(f"Preview: {text[:200]}")
                    logger.info("Connectivity test passed!")
                else:
                    logger.error("Failed to extract PDF text")
                    sys.exit(1)
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
