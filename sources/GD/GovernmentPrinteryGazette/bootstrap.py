#!/usr/bin/env python3
"""
GD/GovernmentPrinteryGazette -- Grenada Government Printery Gazette

Fetches ~604 Grenada Government Gazette issues (2017-2026 + historical) with
full text from gazettes.gov.gd. PDFs are downloaded via eDocman viewdocument
URLs and text extracted via pdfplumber.

Strategy:
  - Scrape year-based category pages (/index.php/publications/{year})
  - Each document has a viewdocument/{id} URL that serves the PDF directly
  - Download PDF and extract text with pdfplumber

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GD.GovernmentPrinteryGazette")

BASE_URL = "https://gazettes.gov.gd"
PUBLICATIONS_URL = f"{BASE_URL}/index.php/publications"
YEAR_PAGES = [2026, 2025, 2024, 2023, 2022, 2021, 2020, 2019, 2018, 2017, 2015, 1983, 1953]
MAX_PDF_SIZE = 50 * 1024 * 1024  # 50MB


class GDGovernmentPrinteryGazetteScraper(BaseScraper):
    """Scraper for GD/GovernmentPrinteryGazette."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _request(self, url: str, timeout: int = 60, stream: bool = False) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout, stream=stream)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_year_page(self, year: int) -> List[Dict[str, str]]:
        """Parse a year page and return list of document dicts with title, url, doc_id."""
        url = f"{PUBLICATIONS_URL}/{year}"
        resp = self._request(url)
        if resp is None:
            return []

        documents = []
        html = resp.text

        # Parse document blocks: each has a title link and a viewdocument link
        # Title: <a ... class="edocman-document-title-link" ...>TITLE</a>
        # View URL: href="/index.php/publications/SLUG/viewdocument/ID"
        doc_blocks = re.split(r'class="edocman-document "', html)

        for block in doc_blocks[1:]:  # skip the part before first document
            # Extract viewdocument URL and ID
            view_match = re.search(r'href="([^"]*?/viewdocument/(\d+))"', block)
            if not view_match:
                continue

            view_url = view_match.group(1)
            doc_id = view_match.group(2)

            if view_url.startswith("/"):
                view_url = BASE_URL + view_url

            # Extract title from edocman-document-title-link
            title_match = re.search(
                r'class="edocman-document-title-link"[^>]*>\s*'
                r'(?:<[^>]*>)?\s*'  # optional icon tag
                r'(.*?)\s*</a>',
                block, re.DOTALL
            )
            if title_match:
                title_text = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
            else:
                # Fallback: derive title from URL slug
                slug_match = re.search(r'/publications/([^/]+)/viewdocument', view_url)
                if slug_match:
                    title_text = slug_match.group(1).replace("-", " ").title()
                else:
                    title_text = f"Gazette {year} (doc {doc_id})"

            documents.append({
                "title": title_text,
                "url": view_url,
                "doc_id": doc_id,
                "year": str(year),
            })

        # Deduplicate by doc_id
        seen = set()
        unique = []
        for doc in documents:
            if doc["doc_id"] not in seen:
                seen.add(doc["doc_id"])
                unique.append(doc)

        return unique

    def _extract_date_from_title(self, title: str, year: str) -> str:
        """Extract ISO date from gazette title like 'No 25 of 2026 Friday 29th May, 2026'."""
        months = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }

        title_lower = title.lower()

        # Pattern: "29th May, 2026" or "8th May 2026" (day month year)
        for m in re.finditer(r'(\d{1,2})(?:st|nd|rd|th)\s+(\w+)[,\s]+(\d{4})', title_lower):
            day, month_name, yr = m.group(1), m.group(2), m.group(3)
            month_num = months.get(month_name)
            if month_num:
                return f"{yr}-{month_num}-{int(day):02d}"

        # Pattern: "May 19th, 2026" or "January 23, 2026" (month day year)
        for m in re.finditer(r'(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?[,\s]+(\d{4})', title_lower):
            month_name, day, yr = m.group(1), m.group(2), m.group(3)
            month_num = months.get(month_name)
            if month_num:
                return f"{yr}-{month_num}-{int(day):02d}"

        return f"{year}-01-01"

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Download PDF from viewdocument URL and extract text."""
        resp = self._request(pdf_url, timeout=120, stream=True)
        if resp is None:
            return ""

        content_type = resp.headers.get("Content-Type", "")
        # viewdocument URLs serve PDF directly
        pdf_bytes = resp.content
        if len(pdf_bytes) > MAX_PDF_SIZE:
            logger.warning(f"PDF too large ({len(pdf_bytes)} bytes): {pdf_url}")
            return ""

        if len(pdf_bytes) < 100:
            logger.warning(f"PDF too small ({len(pdf_bytes)} bytes): {pdf_url}")
            return ""

        try:
            pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
            parts = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    parts.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return "\n\n".join(parts).strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id_str", ""),
            "_source": "GD/GovernmentPrinteryGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "doc_id": raw.get("doc_id", 0),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        count = 0
        seen_ids = set()

        for year in YEAR_PAGES:
            if max_records and count >= max_records:
                return

            docs = self._parse_year_page(year)
            if not docs:
                logger.info(f"No documents found for year {year}")
                continue

            logger.info(f"Year {year}: {len(docs)} documents found")

            for doc in docs:
                if max_records and count >= max_records:
                    return

                if doc["doc_id"] in seen_ids:
                    continue
                seen_ids.add(doc["doc_id"])

                text = self._extract_pdf_text(doc["url"])
                if not text or len(text) < 100:
                    logger.warning(
                        f"Insufficient text ({len(text)} chars): {doc['title']}"
                    )
                    continue

                date = self._extract_date_from_title(doc["title"], doc["year"])

                raw = {
                    "doc_id_str": f"GD-printery-gazette-{doc['doc_id']}",
                    "doc_id": int(doc["doc_id"]),
                    "title": doc["title"],
                    "text": text,
                    "date": date,
                    "url": doc["url"],
                    "year": doc["year"],
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=20)

    def test(self) -> bool:
        docs = self._parse_year_page(2026)
        if not docs:
            logger.error("Cannot parse year page for 2026")
            return False

        logger.info(f"Year 2026 page OK: {len(docs)} documents")

        doc = docs[0]
        logger.info(f"Testing PDF download: {doc['url']}")
        text = self._extract_pdf_text(doc["url"])
        logger.info(f"PDF text: {len(text)} chars")

        return len(text) > 100


def main():
    parser = argparse.ArgumentParser(description="GD/GovernmentPrinteryGazette data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GDGovernmentPrinteryGazetteScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            text_len = len(normalized.get("text", ""))
            logger.info(
                f"[{count + 1}] {normalized.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
