#!/usr/bin/env python3
"""
US/NCUA-Enforcement -- NCUA Administrative Orders (Credit Union Enforcement)

Fetches enforcement actions from the National Credit Union Administration.

Strategy:
  - Download the CSV index of all 1,428 administrative orders
  - For HTML orders (post-~2019): extract text from the web page body
  - For PDF orders (pre-~2019): download and extract text via pdf_extract

Data Coverage:
  - ~1,428 enforcement actions since 1991
  - Cease-and-desist orders, prohibition orders, civil money penalties
  - Actions against credit unions and affiliated individuals

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import csv
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NCUA-Enforcement")

CSV_URL = "https://ncua.gov/sites/default/files/list_csv/administrative-orders.csv"


class NCUAEnforcementScraper(BaseScraper):
    """Scraper for NCUA enforcement actions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _fetch_csv_index(self) -> list[dict]:
        """Download and parse the CSV index of all enforcement actions."""
        resp = self.session.get(CSV_URL, timeout=30)
        resp.raise_for_status()
        reader = csv.DictReader(io.StringIO(resp.text))
        return list(reader)

    def _extract_html_text(self, url: str) -> Optional[str]:
        """Fetch an HTML order page and extract the body text."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch HTML page %s: %s", url, e)
            return None

        # Look for the body content div
        match = re.search(
            r'<div class="body field-type-text_with_summary">(.*?)</div>',
            resp.text, re.DOTALL
        )
        if not match:
            # Try alternate selectors
            match = re.search(
                r'<div[^>]*class="[^"]*field--name-body[^"]*"[^>]*>(.*?)</div>',
                resp.text, re.DOTALL
            )
        if not match:
            # Try broader article body
            match = re.search(
                r'<article[^>]*>(.*?)</article>',
                resp.text, re.DOTALL
            )

        if not match:
            logger.warning("No content found in HTML page: %s", url)
            return None

        html_content = match.group(1)
        # Strip HTML tags
        text = re.sub(r'<[^>]+>', ' ', html_content)
        text = unescape(text)
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text).strip()

        # Filter out error pages
        if "sorry you cannot find" in text.lower() or len(text) < 50:
            return None

        return text

    def _extract_pdf_text(self, url: str, docket: str) -> Optional[str]:
        """Download a PDF order and extract text."""
        text = extract_pdf_markdown(
            source="US/NCUA-Enforcement",
            source_id=docket,
            pdf_url=url,
            table="case_law",
        )
        if text and len(text.strip()) > 50:
            return text
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all enforcement actions with full text."""
        records = self._fetch_csv_index()
        total = len(records)
        logger.info("Total enforcement actions in CSV: %d", total)

        yielded = 0
        for i, record in enumerate(records):
            url = record.get("URL", "").strip()
            docket = record.get("Docket Number", "").strip()

            if not url or not docket:
                continue

            if url.lower().endswith(".pdf"):
                text = self._extract_pdf_text(url, docket)
            else:
                text = self._extract_html_text(url)

            if not text:
                logger.warning("No text for docket %s, skipping", docket)
                continue

            record["_text"] = text
            yield record
            yielded += 1

            if (i + 1) % 50 == 0:
                logger.info("Progress: %d/%d processed, %d yielded", i + 1, total, yielded)

            time.sleep(0.5)

        logger.info("Completed: %d documents with full text out of %d total", yielded, total)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch enforcement actions, stopping at records older than `since`."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        if not since:
            yield from self.fetch_all()
            return

        try:
            since_year = int(since[:4])
        except (ValueError, TypeError):
            yield from self.fetch_all()
            return

        records = self._fetch_csv_index()
        total = len(records)
        logger.info("Total enforcement actions: %d, fetching since year %d", total, since_year)

        for record in records:
            year_str = record.get("Year", "").strip()
            try:
                year = int(year_str)
            except (ValueError, TypeError):
                continue

            if year < since_year:
                return

            url = record.get("URL", "").strip()
            docket = record.get("Docket Number", "").strip()

            if not url or not docket:
                continue

            if url.lower().endswith(".pdf"):
                text = self._extract_pdf_text(url, docket)
            else:
                text = self._extract_html_text(url)

            if not text:
                continue

            record["_text"] = text
            yield record
            time.sleep(0.5)

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw enforcement action record into standard schema."""
        docket = raw.get("Docket Number", "").strip()
        year = raw.get("Year", "").strip()
        first_name = raw.get("First Name", "").strip()
        last_name = raw.get("Last Name", "").strip()
        institution = raw.get("Institution", "").strip()

        # Build title
        name = f"{first_name} {last_name}".strip()
        if name and institution:
            title = f"Administrative Order: {name} — {institution} ({docket})"
        elif name:
            title = f"Administrative Order: {name} ({docket})"
        elif institution:
            title = f"Administrative Order: {institution} ({docket})"
        else:
            title = f"Administrative Order {docket}"

        return {
            "_id": docket,
            "_source": "US/NCUA-Enforcement",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": f"{year}-01-01" if year else None,
            "url": raw.get("URL", "").strip(),
            "docket_number": docket,
            "first_name": first_name,
            "last_name": last_name,
            "institution": institution,
            "relationship": raw.get("Relationship", "").strip(),
            "city": raw.get("City", "").strip(),
            "state": raw.get("State", "").strip(),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NCUA Enforcement bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = NCUAEnforcementScraper()

    if args.command == "test":
        print("Testing NCUA enforcement actions...")
        try:
            records = scraper._fetch_csv_index()
            print(f"OK: {len(records)} enforcement actions in CSV")
            if records:
                r = records[0]
                print(f"  First: {r.get('Docket Number')} | {r.get('Year')} | {r.get('Last Name')}")
                url = r.get("URL", "")
                if url.endswith(".pdf"):
                    text = scraper._extract_pdf_text(url, r.get("Docket Number", ""))
                else:
                    text = scraper._extract_html_text(url)
                if text:
                    print(f"  Text extraction: OK ({len(text)} chars)")
                else:
                    print("  Text extraction: FAILED")
        except Exception as e:
            print(f"FAIL: {e}")
            import traceback
            traceback.print_exc()
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
