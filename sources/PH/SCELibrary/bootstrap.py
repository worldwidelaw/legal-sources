#!/usr/bin/env python3
"""
PH/SCELibrary -- Philippines Supreme Court E-Library Fetcher

Fetches Supreme Court decisions from the official E-Library at
elibrary.judiciary.gov.ph. Decisions are organized by month/year
from 1996 to present.

Strategy:
  - Bootstrap: Iterate month/year pages to discover decision links
  - Each monthly page at /thebookshelf/docmonth/{Mon}/{Year}/1 lists decisions
  - Decision links follow pattern /thebookshelf/showdocs/1/{doc_id}
  - Full text extracted from div.single_content on each decision page

Data:
  - Supreme Court decisions and signed resolutions
  - Coverage: 1996 to present (~30,000+ decisions)
  - Language: English
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PH.SCELibrary")

BASE_URL = "https://elibrary.judiciary.gov.ph"

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Month name to number mapping for date parsing
MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


class ContentExtractor(HTMLParser):
    """Extract text from div.single_content."""

    def __init__(self):
        super().__init__()
        self.in_content = False
        self.depth = 0
        self.parts = []
        self.skip_tags = {"script", "style"}
        self.in_skip = 0

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        if attrs_dict.get("class", "") == "single_content":
            self.in_content = True
            self.depth = 1
            return
        if self.in_content:
            self.depth += 1
            if tag in self.skip_tags:
                self.in_skip += 1
            if tag in ("br", "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "tr", "li"):
                self.parts.append("\n")

    def handle_endtag(self, tag):
        if self.in_content:
            if tag in self.skip_tags and self.in_skip > 0:
                self.in_skip -= 1
            self.depth -= 1
            if self.depth <= 0:
                self.in_content = False

    def handle_data(self, data):
        if self.in_content and self.in_skip == 0:
            self.parts.append(data)


def extract_content(html: str) -> str:
    """Extract clean text from decision HTML page."""
    parser = ContentExtractor()
    parser.feed(html)
    text = "".join(parser.parts).strip()
    # Clean up: collapse whitespace within lines, remove leading "View printer friendly version"
    text = re.sub(r"View printer friendly version\s*", "", text, count=1)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    # Strip trailing E-Library copyright
    text = re.sub(r"\s*©\s*Supreme Court E-Library.*$", "", text, flags=re.DOTALL)
    return text.strip()


def parse_decision_date(text: str) -> Optional[str]:
    """
    Extract decision date from text. Looks for patterns like:
    [ G.R. No. 246027, January 28, 2025 ]
    """
    match = re.search(
        r",\s*(\w+)\s+(\d{1,2}),\s*(\d{4})\s*\]",
        text[:2000]
    )
    if match:
        month_name, day, year = match.group(1).lower(), match.group(2), match.group(3)
        month_num = MONTH_MAP.get(month_name)
        if month_num:
            return f"{year}-{month_num:02d}-{int(day):02d}"
    return None


def extract_gr_number(text: str) -> Optional[str]:
    """Extract GR/AM/AC/UDK number from text header."""
    match = re.search(
        r"\[\s*((?:G\.R\.|A\.M\.|A\.C\.|UDK)[^,\]]+)",
        text[:2000]
    )
    if match:
        return match.group(1).strip()
    return None


def extract_title(text: str) -> Optional[str]:
    """Extract case title (parties) from text header."""
    # Pattern: after the date bracket, the title appears on the next line(s)
    # e.g. "SECURITIES AND EXCHANGE COMMISSION, PETITIONER, VS. ..."
    match = re.search(
        r"\]\s*\n\s*(.+?)(?:\n\s*\n|\nD\s*E\s*C\s*I\s*S\s*I\s*O\s*N|\nR\s*E\s*S\s*O\s*L\s*U\s*T\s*I\s*O\s*N)",
        text[:5000],
        re.DOTALL,
    )
    if match:
        title = match.group(1).strip()
        # Clean up whitespace
        title = re.sub(r"\s+", " ", title)
        if len(title) > 10:
            return title
    return None


class PHSCELibraryScraper(BaseScraper):
    """
    Scraper for PH/SCELibrary -- Philippines Supreme Court E-Library.
    Country: PH
    URL: https://elibrary.judiciary.gov.ph

    Data types: case_law
    Auth: none (Free public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=60,
        )

    def _get_monthly_decisions(self, month: str, year: int) -> list:
        """
        Fetch a monthly index page and extract decision links.
        Returns list of dicts with doc_id, gr_number, title_raw.
        """
        url = f"/thebookshelf/docmonth/{month}/{year}/1"
        self.rate_limiter.wait()

        try:
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"Monthly page {month}/{year} returned {resp.status_code}")
                return []
        except Exception as e:
            logger.warning(f"Failed to fetch {month}/{year}: {e}")
            return []

        html = resp.text
        decisions = []

        # Extract all decision links: showdocs/1/{doc_id}
        # Pattern in HTML: <a href='...showdocs/1/69834'><STRONG>G.R. No. 246027</STRONG><br>
        # <small>TITLE TEXT...
        pattern = re.compile(
            r"showdocs/1/(\d+)['\"]>\s*<STRONG>([^<]*)</STRONG>\s*<br>\s*"
            r"<small>([^<]*)",
            re.IGNORECASE,
        )

        for match in pattern.finditer(html):
            doc_id = match.group(1)
            gr_number = match.group(2).strip()
            title_raw = match.group(3).strip()
            decisions.append({
                "doc_id": doc_id,
                "gr_number": gr_number,
                "title_raw": title_raw,
            })

        logger.info(f"{month} {year}: found {len(decisions)} decisions")
        return decisions

    def _fetch_decision(self, doc_id: str) -> Optional[str]:
        """Fetch the full HTML of a single decision page."""
        url = f"/thebookshelf/showdocs/1/{doc_id}"
        self.rate_limiter.wait()

        try:
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"Decision {doc_id} returned {resp.status_code}")
                return None
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch decision {doc_id}: {e}")
            return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """
        Yield all Supreme Court decisions with full text.
        Iterates from most recent month backwards to 1996.
        BaseScraper.bootstrap() handles sample truncation.
        """
        current_year = datetime.now().year

        # Most recent first — sample mode in BaseScraper will stop early
        years_months = []
        for y in range(current_year, 1995, -1):
            for m in reversed(MONTHS):
                years_months.append((m, y))

        total_yielded = 0

        for month, year in years_months:
            decisions = self._get_monthly_decisions(month, year)

            for dec in decisions:
                html = self._fetch_decision(dec["doc_id"])
                if not html:
                    continue

                text = extract_content(html)
                if not text or len(text) < 100:
                    logger.warning(f"Decision {dec['doc_id']}: text too short ({len(text) if text else 0} chars), skipping")
                    continue

                # Extract metadata from full text (more reliable than listing page)
                gr_from_text = extract_gr_number(text)
                date_str = parse_decision_date(text)
                title_from_text = extract_title(text)

                raw = {
                    "doc_id": dec["doc_id"],
                    "gr_number": gr_from_text or dec["gr_number"],
                    "title": title_from_text or dec["title_raw"],
                    "date": date_str,
                    "month": month,
                    "year": year,
                    "text": text,
                    "url": f"{BASE_URL}/thebookshelf/showdocs/1/{dec['doc_id']}",
                }

                yield raw
                total_yielded += 1

        logger.info(f"Fetch complete: {total_yielded} decisions yielded")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions from recent months only."""
        current_year = datetime.now().year
        current_month = datetime.now().month

        # Fetch last 3 months
        months_to_check = []
        for i in range(3):
            m = current_month - i
            y = current_year
            if m <= 0:
                m += 12
                y -= 1
            months_to_check.append((MONTHS[m - 1], y))

        for month, year in months_to_check:
            decisions = self._get_monthly_decisions(month, year)
            for dec in decisions:
                html = self._fetch_decision(dec["doc_id"])
                if not html:
                    continue

                text = extract_content(html)
                if not text or len(text) < 100:
                    continue

                gr_from_text = extract_gr_number(text)
                date_str = parse_decision_date(text)
                title_from_text = extract_title(text)

                yield {
                    "doc_id": dec["doc_id"],
                    "gr_number": gr_from_text or dec["gr_number"],
                    "title": title_from_text or dec["title_raw"],
                    "date": date_str,
                    "month": month,
                    "year": year,
                    "text": text,
                    "url": f"{BASE_URL}/thebookshelf/showdocs/1/{dec['doc_id']}",
                }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw decision data into standard schema."""
        doc_id = raw.get("doc_id", "unknown")

        return {
            "_id": f"PH_SC_{doc_id}",
            "_source": "PH/SCELibrary",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "gr_number": raw.get("gr_number"),
            "title": raw.get("title"),
            "date": raw.get("date"),
            "text": raw.get("text", ""),
            "url": raw.get("url"),
        }


# ---- CLI entry point -------------------------------------------------------

if __name__ == "__main__":
    scraper = PHSCELibraryScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to E-Library...")
        try:
            resp = scraper.client.get("/")
            logger.info(f"Homepage: HTTP {resp.status_code}")
            decisions = scraper._get_monthly_decisions("Jan", 2025)
            logger.info(f"Jan 2025 index: {len(decisions)} decisions found")
            if decisions:
                html = scraper._fetch_decision(decisions[0]["doc_id"])
                if html:
                    text = extract_content(html)
                    logger.info(f"First decision text: {len(text)} chars")
                    logger.info("Test PASSED: full text accessible")
                else:
                    logger.error("Test FAILED: could not fetch decision page")
            else:
                logger.error("Test FAILED: no decisions found in index")
        except Exception as e:
            logger.error(f"Test FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        if sample:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")

    elif command == "update":
        logger.info("Running incremental update")
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
