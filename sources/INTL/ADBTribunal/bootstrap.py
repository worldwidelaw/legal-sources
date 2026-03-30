#!/usr/bin/env python3
"""
INTL/ADBTribunal -- Asian Development Bank Administrative Tribunal Decisions

Fetches decisions from the ADB Administrative Tribunal website.

Strategy:
  - Parse tribunal listing page for metadata (decision number, title, date)
  - Download PDFs from adb.org/sites/default/files/microcontent/
  - Extract full text from PDFs using PyMuPDF
  - ~136 decisions since 1992

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
logger = logging.getLogger("legal-data-hunter.INTL.ADBTribunal")

LISTING_URL = "https://www.adb.org/who-we-are/organization/administrative-tribunal"
PDF_BASE = "https://www.adb.org/sites/default/files/microcontent"

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
}


def parse_date(date_str: str) -> Optional[str]:
    """Parse ADB date format like '26 Nov 2025' to ISO 8601."""
    date_str = date_str.strip()
    m = re.match(r"(\d{1,2})\s+(\w{3})\s+(\d{4})", date_str)
    if not m:
        return None
    day, month_abbr, year = m.groups()
    month_num = MONTH_MAP.get(month_abbr.lower())
    if not month_num:
        return None
    return f"{year}-{month_num}-{int(day):02d}"


class ADBTribunalScraper(BaseScraper):
    """
    Scraper for INTL/ADBTribunal -- Asian Development Bank Administrative Tribunal.
    Country: INTL
    URL: https://www.adb.org/who-we-are/organization/administrative-tribunal

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

    def _parse_listing(self) -> list[dict]:
        """Parse the tribunal listing page for decision metadata."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text

        decisions = []
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)

        for row in rows:
            pdf_match = re.search(r"(adbt(\d+)\.pdf)", row)
            if not pdf_match:
                continue

            pdf_filename = pdf_match.group(1)
            decision_num = int(pdf_match.group(2))

            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            if len(cells) < 3:
                continue

            # Cell 0: decision number, Cell 1: title + keywords, Cell 2: date
            raw_title = re.sub(r"<[^>]+>", "", cells[1]).strip()
            # Split title from keywords (keywords are lowercase appended text)
            # The case name is before the first keyword-style text
            # Pattern: "Ms Q v. Asian Development Bankstaff retirement plan, ..."
            # We want just the case name part
            title_match = re.match(r"(.*?v\.\s*Asian Development Bank(?:\s*\(No\.\s*\d+\))?)", raw_title)
            if title_match:
                title = title_match.group(1).strip()
                keywords = raw_title[len(title):].strip().strip(",").strip()
            else:
                title = raw_title.split(",")[0].strip() if "," in raw_title else raw_title
                keywords = ""

            date_str = re.sub(r"<[^>]+>", "", cells[2]).strip()
            iso_date = parse_date(date_str)

            decisions.append({
                "decision_number": decision_num,
                "title": title,
                "keywords": keywords,
                "date": iso_date,
                "date_raw": date_str,
                "pdf_filename": pdf_filename,
                "pdf_url": f"{PDF_BASE}/{pdf_filename}",
            })

        # Sort by decision number ascending
        decisions.sort(key=lambda d: d["decision_number"])
        logger.info(f"Parsed {len(decisions)} decisions from listing page")
        return decisions

    def _download_and_extract_pdf(self, url: str) -> Optional[str]:
        """Download a PDF and extract text using PyMuPDF."""
        try:
            resp = self.session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed: {url} -> HTTP {resp.status_code}")
                return None
            if not resp.content[:4] == b"%PDF":
                logger.warning(f"Not a PDF: {url}")
                return None

            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
                f.write(resp.content)
                tmp_path = f.name

            try:
                doc = fitz.open(tmp_path)
                text_parts = []
                for page in doc:
                    text_parts.append(page.get_text())
                doc.close()
                text = "\n".join(text_parts).strip()
                return text if text else None
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        except Exception as e:
            logger.error(f"Error processing PDF {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all tribunal decisions with full text."""
        decisions = self._parse_listing()

        for i, decision in enumerate(decisions):
            logger.info(
                f"Fetching decision {decision['decision_number']} "
                f"({i+1}/{len(decisions)})"
            )
            # Generous delay to avoid Cloudflare
            if i > 0:
                time.sleep(5)

            text = self._download_and_extract_pdf(decision["pdf_url"])
            if text:
                decision["text"] = text
                yield decision
            else:
                logger.warning(
                    f"No text extracted for decision {decision['decision_number']}"
                )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions added since a given date."""
        decisions = self._parse_listing()
        for decision in decisions:
            if decision["date"] and decision["date"] >= since.strftime("%Y-%m-%d"):
                time.sleep(5)
                text = self._download_and_extract_pdf(decision["pdf_url"])
                if text:
                    decision["text"] = text
                    yield decision

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        decision_num = raw["decision_number"]
        return {
            "_id": f"ADBT-{decision_num:04d}",
            "_source": "INTL/ADBTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", f"Decision No. {decision_num}"),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", f"{PDF_BASE}/adbt{decision_num:04d}.pdf"),
            "decision_number": decision_num,
            "keywords": raw.get("keywords", ""),
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ADBTribunal data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = ADBTribunalScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            decisions = scraper._parse_listing()
            logger.info(f"OK: {len(decisions)} decisions found")
            if decisions:
                d = decisions[-1]  # Most recent
                logger.info(f"Latest: Decision {d['decision_number']} - {d['title'][:80]}")
                text = scraper._download_and_extract_pdf(d["pdf_url"])
                if text:
                    logger.info(f"PDF text extracted: {len(text)} chars")
                    logger.info(f"Preview: {text[:200]}")
                else:
                    logger.error("Failed to extract PDF text")
                    sys.exit(1)
            logger.info("Connectivity test passed!")
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
