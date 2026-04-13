#!/usr/bin/env python3
"""
BY/Pravo-Legislation -- Belarus National Legal Internet Portal Data Fetcher

Fetches all legislation from pravo.by with full text extracted from PDFs.

Strategy:
  - Enumerate documents via the "official publication / new arrivals" endpoint
    with print=1 parameter (returns all results on a single page)
  - Iterate month-by-month from 2020-01 to present
  - For each document, fetch the info page (guid=12551) to extract PDF URL
  - Download PDF and extract text via common/pdf_extract

Document types covered (by regnum prefix):
  H  = Laws                    P = Presidential Decrees
  Hk = Codes                   C = Council of Ministers Resolutions
  W  = Ministry-level acts     B = National Bank resolutions
  T  = Presidential subordinates  S = Supreme Court Plenum
  K  = Constitutional Court    I = International treaties
  D  = Local council decisions  R = Local executive decisions
  L  = Council of the Republic  N/A/F = Miscellaneous

Endpoints:
  - New arrivals: /ofitsialnoe-opublikovanie/novye-postupleniya/?p0={DD.MM.YYYY}&p1={DD.MM.YYYY}&print=1
  - Document info: /document/?guid=12551&p0={regnum}
  - PDF download: /upload/docs/op/{regnum}_{timestamp}.pdf

Usage:
  python bootstrap.py bootstrap           # Full initial pull (2020-present)
  python bootstrap.py bootstrap --sample  # Fetch 15 sample records
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple
from calendar import monthrange

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BY.Pravo-Legislation")

BASE_URL = "https://pravo.by"
NEW_ARRIVALS = "/ofitsialnoe-opublikovanie/novye-postupleniya/"
DOC_INFO = "/document/?guid=12551&p0="

# Start year for full bootstrap (older docs return 403 — PDFs only available recent years)
START_YEAR = 2025

# Document type mapping by regnum prefix
DOC_TYPE_MAP = {
    "H": "law",
    "P": "presidential_decree",
    "C": "council_of_ministers_resolution",
    "W": "ministry_act",
    "B": "national_bank_resolution",
    "T": "presidential_subordinate_act",
    "S": "supreme_court_plenum",
    "K": "constitutional_court_act",
    "L": "council_of_republic",
    "I": "international_treaty",
    "D": "local_council_decision",
    "R": "local_executive_decision",
    "N": "miscellaneous",
    "A": "miscellaneous",
    "F": "miscellaneous",
}


class PravoLegislationScraper(BaseScraper):
    """
    Scraper for BY/Pravo-Legislation -- Belarus National Legal Internet Portal.
    Country: BY
    URL: https://pravo.by

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self._session_primed = False
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.9",
        })

    def _prime_session(self):
        """Visit main page once to obtain session cookies."""
        if self._session_primed:
            return
        try:
            self.session.get(BASE_URL, timeout=15)
            self._session_primed = True
        except Exception:
            pass

    def _get(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """HTTP GET with rate limiting, backoff on 403, and error handling."""
        self._prime_session()
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 403 and attempt < 2:
                    wait = 10 * (attempt + 1)
                    logger.info(f"Got 403, backing off {wait}s (attempt {attempt+1})")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                resp.encoding = "utf-8"
                return resp
            except Exception as e:
                if attempt < 2 and "403" in str(e):
                    wait = 10 * (attempt + 1)
                    logger.info(f"Got 403, backing off {wait}s (attempt {attempt+1})")
                    time.sleep(wait)
                    continue
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        return None

    def _generate_month_ranges(self, start_year: int = START_YEAR) -> List[Tuple[str, str]]:
        """Generate (start_date, end_date) pairs for each month from start_year to now."""
        now = datetime.now()
        ranges = []
        year = start_year
        month = 1
        while year < now.year or (year == now.year and month <= now.month):
            _, last_day = monthrange(year, month)
            start = f"{1:02d}.{month:02d}.{year}"
            end = f"{last_day:02d}.{month:02d}.{year}"
            ranges.append((start, end))
            month += 1
            if month > 12:
                month = 1
                year += 1
        return ranges

    def _enumerate_documents(self, start_date: str, end_date: str) -> List[Dict[str, str]]:
        """
        Fetch the new arrivals page for a date range and extract document references.

        Returns list of dicts with: regnum, title, href
        """
        url = f"{BASE_URL}{NEW_ARRIVALS}?p0={start_date}&p1={end_date}&print=1"
        resp = self._get(url, timeout=60)
        if not resp:
            return []

        # Extract document links: <a href="/document/?guid=...&p0=REGNUM">Title</a>
        pattern = r'<a[^>]*href="(/document/[^"]*p0=([A-Za-z0-9]+))"[^>]*>(.*?)</a>'
        matches = re.findall(pattern, resp.text, re.DOTALL)

        docs = []
        seen = set()
        for href, regnum, title_html in matches:
            if regnum in seen:
                continue
            seen.add(regnum)
            title = re.sub(r"<[^>]+>", "", title_html).strip()
            title = html.unescape(title)
            docs.append({
                "regnum": regnum,
                "title": title,
                "href": href,
            })

        logger.info(f"  {start_date}-{end_date}: {len(docs)} documents")
        return docs

    def _fetch_doc_info(self, regnum: str) -> Optional[Dict[str, Any]]:
        """Fetch document info page and extract PDF URL and metadata."""
        url = f"{BASE_URL}{DOC_INFO}{regnum}"
        resp = self._get(url, timeout=30)
        if not resp:
            return None

        content = resp.text
        result = {"info_url": url}

        # Extract PDF link
        pdf_match = re.search(r'href="(/upload/docs/op/[^"]*\.pdf)"', content)
        if pdf_match:
            result["pdf_path"] = pdf_match.group(1)
            result["pdf_url"] = f"{BASE_URL}{pdf_match.group(1)}"

        # Extract title from <title> tag
        title_match = re.search(r"<title>([^<]+)</title>", content)
        if title_match:
            raw_title = html.unescape(title_match.group(1).strip())
            raw_title = re.sub(r"\s*–\s*Pravo\.by\s*$", "", raw_title)
            result["page_title"] = raw_title

        # Extract date from title (pattern: "от DD.MM.YYYY г.")
        date_match = re.search(r"от\s+(\d{2})\.(\d{2})\.(\d{4})\s*г\.", content)
        if date_match:
            result["date"] = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}"

        # Extract document number from title (pattern: "№ NNN")
        num_match = re.search(r"№\s*([\d\-/А-Яа-яA-Za-z]+)", content)
        if num_match:
            result["doc_number"] = num_match.group(1).strip()

        return result

    def _extract_pdf_text(self, pdf_url: str, regnum: str) -> Optional[str]:
        """Download PDF and extract text via centralized extractor."""
        return extract_pdf_markdown(
            source="BY/Pravo-Legislation",
            source_id=regnum,
            pdf_url=pdf_url,
            table="legislation",
        )

    def _classify_doc_type(self, regnum: str) -> str:
        """Classify document type based on regnum prefix."""
        if not regnum:
            return "unknown"
        prefix = regnum[0].upper()
        # Check for code (Hk/HK)
        if len(regnum) > 1 and prefix == "H" and regnum[1].lower() == "k":
            return "code"
        return DOC_TYPE_MAP.get(prefix, "unknown")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation documents from pravo.by.

        Iterates month-by-month from START_YEAR to present,
        enumerates documents, fetches PDFs, and extracts text.
        """
        month_ranges = self._generate_month_ranges()
        logger.info(f"Starting full legislation fetch: {len(month_ranges)} months "
                     f"from {START_YEAR} to present")

        total = 0
        for start_date, end_date in month_ranges:
            docs = self._enumerate_documents(start_date, end_date)

            for doc in docs:
                regnum = doc["regnum"]

                # Fetch document info page for PDF URL
                info = self._fetch_doc_info(regnum)
                if not info or not info.get("pdf_url"):
                    logger.warning(f"No PDF found for {regnum}")
                    continue

                # Extract text from PDF
                text = self._extract_pdf_text(info["pdf_url"], regnum)
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for {regnum}: "
                                   f"{len(text) if text else 0} chars")
                    continue

                total += 1
                yield {
                    "regnum": regnum,
                    "title": info.get("page_title") or doc.get("title", ""),
                    "date": info.get("date", ""),
                    "doc_number": info.get("doc_number", ""),
                    "doc_type": self._classify_doc_type(regnum),
                    "pdf_url": info["pdf_url"],
                    "info_url": info["info_url"],
                    "text": text,
                    "text_length": len(text),
                }

        logger.info(f"Fetch complete: {total} documents with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given date."""
        logger.info(f"Fetching updates since {since.date()}...")
        now = datetime.now()
        start = f"{since.day:02d}.{since.month:02d}.{since.year}"
        end = f"{now.day:02d}.{now.month:02d}.{now.year}"

        docs = self._enumerate_documents(start, end)
        for doc in docs:
            regnum = doc["regnum"]
            info = self._fetch_doc_info(regnum)
            if not info or not info.get("pdf_url"):
                continue

            text = self._extract_pdf_text(info["pdf_url"], regnum)
            if not text or len(text) < 100:
                continue

            yield {
                "regnum": regnum,
                "title": info.get("page_title") or doc.get("title", ""),
                "date": info.get("date", ""),
                "doc_number": info.get("doc_number", ""),
                "doc_type": self._classify_doc_type(regnum),
                "pdf_url": info["pdf_url"],
                "info_url": info["info_url"],
                "text": text,
                "text_length": len(text),
            }

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        regnum = raw.get("regnum", "")
        title = raw.get("title", "")

        # Clean title
        title = re.sub(r"\s+", " ", title).strip()

        return {
            "_id": f"BY/leg/{regnum}",
            "_source": "BY/Pravo-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("info_url", ""),
            "regnum": regnum,
            "doc_number": raw.get("doc_number", ""),
            "doc_type": raw.get("doc_type", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "text_length": raw.get("text_length", 0),
            "language": "ru",
            "jurisdiction": "BY",
        }

    def test_api(self):
        """Quick connectivity and endpoint test."""
        print("Testing pravo.by access...")

        # 1. Main page
        print("\n1. Main page connectivity...")
        resp = self._get(BASE_URL, timeout=15)
        if resp:
            print(f"   OK: {len(resp.text)} bytes")
        else:
            print("   FAIL: Cannot reach pravo.by")
            return

        # 2. New arrivals endpoint
        print("\n2. New arrivals endpoint (last 7 days)...")
        now = datetime.now()
        week_ago = now - timedelta(days=7)
        start = f"{week_ago.day:02d}.{week_ago.month:02d}.{week_ago.year}"
        end = f"{now.day:02d}.{now.month:02d}.{now.year}"
        docs = self._enumerate_documents(start, end)
        print(f"   Found {len(docs)} documents")
        if docs:
            print(f"   Sample: {docs[0]['regnum']} - {docs[0]['title'][:60]}")

        # 3. Document info + PDF
        if docs:
            print(f"\n3. Document info page for {docs[0]['regnum']}...")
            info = self._fetch_doc_info(docs[0]["regnum"])
            if info:
                print(f"   Title: {info.get('page_title', 'N/A')[:70]}")
                print(f"   Date: {info.get('date', 'N/A')}")
                print(f"   PDF URL: {info.get('pdf_url', 'N/A')}")

                if info.get("pdf_url"):
                    print(f"\n4. PDF text extraction...")
                    text = self._extract_pdf_text(info["pdf_url"], docs[0]["regnum"])
                    if text:
                        print(f"   Extracted {len(text)} chars")
                        print(f"   Preview: {text[:200]}...")
                    else:
                        print("   FAIL: No text extracted")

        print("\nAPI test complete!")


def main():
    scraper = PravoLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: "
                  f"{stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
