#!/usr/bin/env python3
"""
INTL/ICJDecisions -- International Court of Justice Decisions

Fetches judgments, advisory opinions, and orders from the ICJ website.

Strategy:
  - Parse decisions list page at icj-cij.org/decisions for metadata + PDF URLs
  - Download PDFs from UN cloud CDN (icj-web.leman.un-icc.cloud) to bypass Cloudflare
  - Extract full text from PDFs using PyMuPDF
  - ~870 decisions since 1946

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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ICJDecisions")

DECISIONS_URL = "https://icj-cij.org/decisions"
PDF_CDN = "https://icj-web.leman.un-icc.cloud"
# Decision type IDs for the decisions page: 1=All, 2=Judgments, 3=Orders, 4=Advisory Opinions
TYPE_MAP = {
    "judgment": "2",
    "order": "3",
    "advisory_opinion": "4",
}


class ICJDecisionsScraper(BaseScraper):
    """
    Scraper for INTL/ICJDecisions -- International Court of Justice.
    Country: INTL
    URL: https://icj-cij.org/decisions

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

    def _parse_decisions_page(self, decision_type: str = "1") -> list[dict]:
        """Parse the decisions list page for metadata and PDF URLs.

        Args:
            decision_type: "1"=All, "2"=Judgments, "3"=Orders, "4"=Advisory Opinions
        """
        url = f"{DECISIONS_URL}?type={decision_type}&from=1946"
        logger.info(f"Fetching decisions list: {url}")
        r = self.session.get(url, timeout=60)
        r.raise_for_status()
        html = r.text

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        decisions = []
        rows = soup.select("div.views-row")
        logger.info(f"Found {len(rows)} decision rows")

        for row in rows:
            # Title and PDF link
            title_div = row.select_one("div.views-field-field-document-long-title")
            if not title_div:
                continue

            title_link = title_div.select_one("a")
            title = ""
            pdf_path = ""
            if title_link:
                title = title_link.get_text(strip=True)
                href = title_link.get("href", "")
                if href.endswith(".pdf"):
                    pdf_path = href

            # Case name and case link
            case_div = row.select_one("div.views-field-field-case-long-title")
            case_name = ""
            case_id = ""
            if case_div:
                case_link = case_div.select_one("a")
                if case_link:
                    case_name = case_link.get_text(strip=True)
                    case_href = case_link.get("href", "")
                    m = re.search(r"/case/(\d+)", case_href)
                    if m:
                        case_id = m.group(1)

            # Subtitle
            subtitle_div = row.select_one("div.views-field-field-icj-document-subtitle")
            subtitle = ""
            if subtitle_div:
                subtitle = subtitle_div.get_text(strip=True)

            # Extract English PDF link from the available languages section
            en_links = row.select("div.english-buttons a")
            if en_links and not pdf_path:
                for link in en_links:
                    href = link.get("href", "")
                    if href.endswith(".pdf") and ("EN" in href.upper() or "en" in href):
                        pdf_path = href
                        break

            if not pdf_path:
                # Try any PDF link in the row
                all_links = row.select("a[href$='.pdf']")
                for link in all_links:
                    href = link.get("href", "")
                    if "en" in href.lower():
                        pdf_path = href
                        break

            if not pdf_path or not title:
                continue

            # Parse date from title or PDF filename
            date = self._extract_date(title, pdf_path)

            # Determine decision type from title
            dtype = "decision"
            title_lower = title.lower()
            if "judgment" in title_lower or "arrêt" in title_lower:
                dtype = "judgment"
            elif "advisory opinion" in title_lower or "avis consultatif" in title_lower:
                dtype = "advisory_opinion"
            elif "order" in title_lower or "ordonnance" in title_lower:
                dtype = "order"

            # Extract PDF filename for ID
            pdf_filename = pdf_path.split("/")[-1] if pdf_path else ""

            decisions.append({
                "title": title,
                "case_name": case_name,
                "case_id": case_id,
                "subtitle": subtitle,
                "pdf_path": pdf_path,
                "pdf_filename": pdf_filename,
                "date": date,
                "decision_type": dtype,
            })

        return decisions

    def _extract_date(self, title: str, pdf_path: str) -> Optional[str]:
        """Extract date from decision title or PDF filename."""
        # Try PDF filename pattern: NNN-YYYYMMDD-...
        m = re.search(r"(\d{3})-(\d{4})(\d{2})(\d{2})-", pdf_path)
        if m:
            return f"{m.group(2)}-{m.group(3)}-{m.group(4)}"

        # Try date patterns in title
        months = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", title)
        if m:
            day = m.group(1).zfill(2)
            month_name = m.group(2).lower()
            year = m.group(3)
            if month_name in months:
                return f"{year}-{months[month_name]}-{day}"

        # Try "Month Day, Year" pattern
        m = re.search(r"(\w+)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})", title)
        if m:
            month_name = m.group(1).lower()
            day = m.group(2).zfill(2)
            year = m.group(3)
            if month_name in months:
                return f"{year}-{months[month_name]}-{day}"

        return None

    def _download_and_extract_pdf(self, pdf_path: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/ICJDecisions",
            source_id="",
            pdf_url=pdf_path,
            table="case_law",
        ) or ""

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw decision record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        title = raw.get("title", "").strip()
        case_name = raw.get("case_name", "").strip()
        if case_name and title:
            full_title = f"{title} — {case_name}"
        else:
            full_title = title or case_name or "ICJ Decision"

        if raw.get("subtitle"):
            full_title += f" ({raw['subtitle']})"

        pdf_filename = raw.get("pdf_filename", "")
        case_id = raw.get("case_id", "")

        # Build URL
        url = f"https://icj-cij.org/case/{case_id}" if case_id else "https://icj-cij.org/decisions"

        return {
            "_id": f"ICJ-{pdf_filename}" if pdf_filename else f"ICJ-{hash(full_title)}",
            "_source": "INTL/ICJDecisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "case_name": case_name,
            "case_id": case_id,
            "decision_type": raw.get("decision_type", "decision"),
            "pdf_filename": pdf_filename,
            "court": "International Court of Justice",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all ICJ decisions."""
        # Parse all decisions (type=1)
        decisions = self._parse_decisions_page("1")
        total = len(decisions)
        logger.info(f"Total decisions found: {total}")

        for i, decision in enumerate(decisions):
            logger.info(f"[{i+1}/{total}] Downloading: {decision['title'][:80]}")

            text = self._download_and_extract_pdf(decision["pdf_path"])
            if text:
                decision["text"] = text
                yield decision
            else:
                logger.warning(f"Skipping (no text): {decision['title'][:80]}")

            # Rate limit
            time.sleep(1.5)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions newer than given date."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching decisions since {since_str}")

        decisions = self._parse_decisions_page("1")
        for decision in decisions:
            if decision.get("date") and decision["date"] >= since_str:
                text = self._download_and_extract_pdf(decision["pdf_path"])
                if text:
                    decision["text"] = text
                    yield decision
                time.sleep(1.5)
            else:
                # Decisions are sorted newest first, so we can stop
                break


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ICJDecisions data fetcher")
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

    scraper = ICJDecisionsScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            decisions = scraper._parse_decisions_page("2")  # Just judgments
            logger.info(f"OK: {len(decisions)} judgments found")
            if decisions:
                d = decisions[0]
                logger.info(f"First: {d['title'][:80]}")
                text = scraper._download_and_extract_pdf(d["pdf_path"])
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
