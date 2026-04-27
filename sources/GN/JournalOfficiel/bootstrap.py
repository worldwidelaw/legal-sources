#!/usr/bin/env python3
"""
GN/JournalOfficiel -- Guinea Official Journal (Journal Officiel)

Fetches journal issues from journal-officiel.sgg.gov.gn. The site provides
a paginated HTML archive listing ~1,844 journal issues with direct PDF links.

Strategy:
  - Paginate through the HTML archive (20 items per page, ~92 pages)
  - For each journal issue, extract PDF URL, title, and date
  - Download PDF and extract full text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Fetch all journal issues
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GN.JournalOfficiel")

BASE_URL = "https://journal-officiel.sgg.gov.gn"
ARCHIVE_URL = f"{BASE_URL}/fr/journal-officiel/le-journal-officiel.html"
ITEMS_PER_PAGE = 20


def _parse_date(text: str) -> str:
    """Extract ISO date from title like 'Journal officiel n°2026-4 spécial du 31/03/2026'."""
    m = re.search(r"(\d{2})/(\d{2})/(\d{4})", text)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return ""


def _make_doc_id(pdf_url: str) -> str:
    """Create a stable document ID from the PDF filename."""
    filename = pdf_url.rsplit("/", 1)[-1].replace(".pdf", "")
    return f"GN-JO-{filename}"


class GuineaJOScraper(BaseScraper):
    """Scraper for GN/JournalOfficiel -- Guinea Official Journal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
        })

    def _fetch_page(self, page: int = 0) -> list:
        """Fetch one page of the archive listing and return journal entries."""
        url = ARCHIVE_URL
        if page > 0:
            url = f"{ARCHIVE_URL}?page={page}&row=1844"

        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"Page {page} attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    return []

        soup = BeautifulSoup(resp.text, "html.parser")
        entries = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if ".pdf" not in href.lower() or "/JO/" not in href:
                continue

            pdf_url = href if href.startswith("http") else urljoin(BASE_URL, href)
            title = re.sub(r"^JO\s*", "", a.get_text(strip=True))
            date = _parse_date(title)
            doc_id = _make_doc_id(pdf_url)

            entries.append({
                "doc_id": doc_id,
                "title": title,
                "date": date,
                "pdf_url": pdf_url,
            })

        return entries

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw["doc_id"],
            "_source": "GN/JournalOfficiel",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["pdf_url"],
            "issue_number": raw.get("issue_number", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all journal issues by paginating through the archive."""
        existing = preload_existing_ids("GN/JournalOfficiel", table="legislation")
        count = 0
        page = 0

        while True:
            entries = self._fetch_page(page)
            if not entries:
                logger.info(f"No more entries at page {page}")
                break

            logger.info(f"Page {page}: {len(entries)} journal entries")

            for entry in entries:
                if entry["doc_id"] in existing:
                    logger.debug(f"Skipping {entry['doc_id']} — already in Neon")
                    continue

                logger.info(f"Extracting: {entry['title'][:60]}")
                try:
                    text = extract_pdf_markdown(
                        source="GN/JournalOfficiel",
                        source_id=entry["doc_id"],
                        pdf_url=entry["pdf_url"],
                        table="legislation",
                    )
                except Exception as e:
                    logger.warning(f"PDF extraction failed for {entry['title']}: {e}")
                    text = None

                if not text or len(text) < 50:
                    logger.warning(f"Insufficient text for {entry['title']}: {len(text) if text else 0} chars")
                    continue

                issue_m = re.search(r"n°\s*(\S+)", entry["title"])
                entry["issue_number"] = issue_m.group(1) if issue_m else ""
                entry["text"] = text
                count += 1
                yield entry

            page += 1
            if page > 100:
                break

        logger.info(f"Completed: {count} journal issues fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent journal issues (first 2 pages)."""
        existing = preload_existing_ids("GN/JournalOfficiel", table="legislation")
        count = 0

        for page in range(2):
            entries = self._fetch_page(page)
            for entry in entries:
                if entry["doc_id"] in existing:
                    continue

                try:
                    text = extract_pdf_markdown(
                        source="GN/JournalOfficiel",
                        source_id=entry["doc_id"],
                        pdf_url=entry["pdf_url"],
                        table="legislation",
                    )
                except Exception as e:
                    logger.warning(f"PDF extraction failed: {e}")
                    text = None

                if not text or len(text) < 50:
                    continue

                issue_m = re.search(r"n°\s*(\S+)", entry["title"])
                entry["issue_number"] = issue_m.group(1) if issue_m else ""
                entry["text"] = text
                count += 1
                yield entry

        logger.info(f"Updates: {count} new journal issues")

    def test(self) -> bool:
        """Quick connectivity test."""
        entries = self._fetch_page(0)
        if not entries:
            logger.error("Cannot fetch archive page")
            return False

        logger.info(f"Archive OK: {len(entries)} entries on first page")
        test_entry = entries[0]
        logger.info(f"Sample: {test_entry['title'][:60]} — {test_entry['pdf_url'][:80]}")
        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GN/JournalOfficiel data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GuineaJOScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
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
    main()
