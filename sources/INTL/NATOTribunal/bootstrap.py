#!/usr/bin/env python3
"""
INTL/NATOTribunal -- NATO Administrative Tribunal

Fetches judgments and orders from the NATO Administrative Tribunal.

Strategy:
  - Download annual PDF compilations (2013-2025) from nato.int
  - Extract text from each PDF via pdfplumber
  - Split into individual judgments/orders using AT-J/AT(PRE-O)/AT(TRI-O) ID patterns
  - Parse date, case number, keywords from each decision header
  - ~150+ decisions total across all years

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import json
import re
import sys
import time
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.NATOTribunal")

BASE_URL = "https://www.nato.int"
TRIBUNAL_PAGE = f"{BASE_URL}/cps/en/natohq/topics_114072.htm"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

# Annual PDF compilations (year -> relative path)
PDF_COMPILATIONS = {
    2025: "/content/dam/nato/webready/documents/Administrative-Tribunal/at-2025-judgments-orders-en.pdf",
    2024: "/content/dam/nato/webready/documents/Administrative-Tribunal/2024_EN_judgments_orders.pdf",
    2023: "/content/dam/nato/webready/documents/Administrative-Tribunal/2023_EN_judgments_orders.pdf",
    2022: "/content/dam/nato/webready/documents/Administrative-Tribunal/2022_EN_judgments_orders_rd.pdf",
    2021: "/content/dam/nato/webready/documents/Administrative-Tribunal/2021_EN_judgments_orders_reduced.pdf",
    2020: "/content/dam/nato/webready/documents/Administrative-Tribunal/2020_EN_judgments_orders.pdf",
    2019: "/content/dam/nato/webready/documents/Administrative-Tribunal/2019_EN_judgments_orders.pdf",
    2018: "/content/dam/nato/webready/documents/Administrative-Tribunal/20190531_2018-AT-Judgements-Orders-v2.pdf",
    2017: "/content/dam/nato/webready/documents/Administrative-Tribunal/2017_EN_judgments_orders.pdf",
    2016: "/content/dam/nato/webready/documents/Administrative-Tribunal/2016_EN_judgments_orders.pdf",
    2015: "/content/dam/nato/webready/documents/Administrative-Tribunal/20160606_1600606-2015-ATjudgments-en.pdf",
    2014: "/content/dam/nato/webready/documents/Administrative-Tribunal/2014_EN_judgments.pdf",
    2013: "/content/dam/nato/webready/documents/Administrative-Tribunal/20160104_2013_EN_ATjudgments.pdf",
}

MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}

# Matches judgment start: "16 May 2025 AT-J(2025)0001"
JUDGMENT_START_RE = re.compile(
    r"^\s*(\d{1,2}\s+\w+\s+\d{4})\s+"
    r"(AT-J\(\d{4}\)\d+)",
    re.MULTILINE,
)

# Matches order start: "AT(PRE-O)(2025)0001" or "AT(TRI-O)(2025)0003"
ORDER_START_RE = re.compile(
    r"^\s*(AT\([A-Z-]+\)\(\d{4}\)\d+)\s*$",
    re.MULTILINE,
)

# Matches any decision ID at the top of continuation pages
DECISION_ID_RE = re.compile(
    r"^(AT-J\(\d{4}\)\d+|AT\([A-Z-]+\)\(\d{4}\)\d+)$",
    re.MULTILINE,
)


class NATOTribunalScraper(BaseScraper):
    """
    Scraper for INTL/NATOTribunal -- NATO Administrative Tribunal.
    Country: INTL
    URL: https://www.nato.int/cps/en/natohq/topics_114072.htm

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _download_pdf(self, year: int) -> Optional[bytes]:
        """Download a PDF compilation for a given year."""
        path = PDF_COMPILATIONS.get(year)
        if not path:
            return None
        url = f"{BASE_URL}{path}"
        try:
            r = self.session.get(url, timeout=120)
            r.raise_for_status()
            return r.content
        except requests.RequestException as e:
            logger.warning(f"Failed to download PDF for {year}: {e}")
            return None

    def _extract_pages(self, pdf_bytes: bytes) -> list[str]:
        """Extract text from each page of a PDF."""
        with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
            f.write(pdf_bytes)
            f.flush()
            pdf = pdfplumber.open(f.name)
            pages = []
            for page in pdf.pages:
                text = page.extract_text() or ""
                pages.append(text)
            pdf.close()
            return pages

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse 'DD Month YYYY' to ISO date."""
        parts = date_str.strip().split()
        if len(parts) != 3:
            return None
        day, month_name, year = parts
        month = MONTH_MAP.get(month_name.lower())
        if not month:
            return None
        return f"{year}-{month}-{day.zfill(2)}"

    def _split_decisions(self, pages: list[str], year: int) -> list[dict]:
        """Split PDF pages into individual decisions."""
        decisions = []
        current = None

        for page_idx, page_text in enumerate(pages):
            first_200 = page_text[:300]

            # Check for judgment start (date + ID on first line)
            jm = JUDGMENT_START_RE.search(first_200)
            if jm:
                if current:
                    decisions.append(current)
                current = {
                    "decision_id": jm.group(2),
                    "date_str": jm.group(1),
                    "doc_type": "Judgment",
                    "pages": [page_text],
                    "start_page": page_idx,
                }
                continue

            # Check for order start (ID alone on a line, followed by "Order")
            om = ORDER_START_RE.search(first_200)
            if om and "Order" in first_200[:200]:
                if current:
                    decisions.append(current)
                current = {
                    "decision_id": om.group(1),
                    "date_str": None,
                    "doc_type": "Order",
                    "pages": [page_text],
                    "start_page": page_idx,
                }
                continue

            # Continuation page — check if it belongs to current decision
            if current:
                current["pages"].append(page_text)

        if current:
            decisions.append(current)

        return decisions

    def _clean_page_headers(self, text: str, decision_id: str) -> str:
        """Remove repeated decision ID headers from page text."""
        lines = text.split("\n")
        cleaned = []
        for line in lines:
            stripped = line.strip()
            # Skip lines that are just the decision ID
            if stripped == decision_id:
                continue
            # Skip page numbers like "- 2 -" or "- 15 -"
            if re.match(r"^-\s*\d+\s*-$", stripped):
                continue
            cleaned.append(line)
        return "\n".join(cleaned)

    def _parse_decision_metadata(self, text: str, decision: dict) -> dict:
        """Extract case number, keywords, respondent from the decision text."""
        meta = {}

        # Case number: "Case No. 2024/1394"
        case_m = re.search(r"Case\s+No\.?\s*([\d/,\s]+(?:and\s+Case\s+No\.?\s*[\d/]+)*)", text)
        if case_m:
            meta["case_number"] = case_m.group(1).strip()

        # Keywords
        kw_m = re.search(r"Keywords?:\s*(.+?)(?:\n|North Atlantic)", text, re.DOTALL)
        if kw_m:
            meta["keywords"] = kw_m.group(1).strip().rstrip(".")

        # Respondent (after "v." and before "Respondent")
        resp_m = re.search(r"v\.\s*\n?(.*?)\s*\n\s*Respondent", text, re.DOTALL)
        if resp_m:
            meta["respondent"] = resp_m.group(1).strip()

        # Date for orders (look for "Brussels, DD Month YYYY")
        if not decision.get("date_str"):
            date_m = re.search(r"Brussels,\s*(\d{1,2}\s+\w+\s+\d{4})", text)
            if date_m:
                decision["date_str"] = date_m.group(1)

        return meta

    def _process_year(self, year: int) -> list[dict]:
        """Download and process one year's PDF compilation."""
        logger.info(f"Processing year {year}...")
        pdf_bytes = self._download_pdf(year)
        if not pdf_bytes:
            return []

        pages = self._extract_pages(pdf_bytes)
        logger.info(f"  {year}: {len(pages)} pages extracted")

        decisions = self._split_decisions(pages, year)
        logger.info(f"  {year}: {len(decisions)} decisions found")

        records = []
        for dec in decisions:
            decision_id = dec["decision_id"]
            # Combine and clean pages
            combined = "\n\n".join(
                self._clean_page_headers(p, decision_id) for p in dec["pages"]
            )

            # Parse metadata from the text
            meta = self._parse_decision_metadata(combined, dec)

            # Build title
            case_num = meta.get("case_number", "")
            respondent = meta.get("respondent", "")
            title_parts = [dec["doc_type"], decision_id]
            if case_num:
                title_parts.append(f"Case No. {case_num}")
            if respondent:
                title_parts.append(f"v. {respondent}")
            title = " — ".join(title_parts)

            # Parse date
            date = self._parse_date(dec["date_str"]) if dec.get("date_str") else None

            records.append({
                "decision_id": decision_id,
                "title": title,
                "text": combined.strip(),
                "date": date,
                "case_number": meta.get("case_number", ""),
                "keywords": meta.get("keywords", ""),
                "respondent": meta.get("respondent", ""),
                "doc_type": dec["doc_type"],
                "year": year,
                "page_url": TRIBUNAL_PAGE,
            })

        return records

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw decision record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        decision_id = raw.get("decision_id", "")

        return {
            "_id": decision_id,
            "_source": "INTL/NATOTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("page_url", TRIBUNAL_PAGE),
            "case_number": raw.get("case_number", ""),
            "keywords": raw.get("keywords", ""),
            "respondent": raw.get("respondent", ""),
            "doc_type": raw.get("doc_type", ""),
            "court": "NATO Administrative Tribunal",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all decisions from all years."""
        yielded = 0
        for year in sorted(PDF_COMPILATIONS.keys()):
            records = self._process_year(year)
            for record in records:
                yield record
                yielded += 1
            time.sleep(2)

        logger.info(f"Finished: {yielded} decisions yielded with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions (latest year only)."""
        latest_year = max(PDF_COMPILATIONS.keys())
        records = self._process_year(latest_year)
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        for record in records:
            if record.get("date") and record["date"] >= since_str:
                yield record


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/NATOTribunal data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = NATOTribunalScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            pdf_bytes = scraper._download_pdf(2025)
            if pdf_bytes:
                logger.info(f"OK: Downloaded 2025 PDF ({len(pdf_bytes)} bytes)")
                pages = scraper._extract_pages(pdf_bytes)
                logger.info(f"OK: Extracted {len(pages)} pages")
                decisions = scraper._split_decisions(pages, 2025)
                logger.info(f"OK: Found {len(decisions)} decisions")
                if decisions:
                    d = decisions[0]
                    logger.info(f"First: {d['decision_id']} ({d['doc_type']})")
            else:
                logger.error("Failed to download 2025 PDF")
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
