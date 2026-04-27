#!/usr/bin/env python3
"""
US/IRS-PrivateLetterRulings -- IRS Written Determinations (Section 6110)

Fetches Private Letter Rulings, Technical Advice Memoranda, Determination
Letters, and Chief Counsel Advice from the IRS. ~77,000+ documents from 1999
to present, released every Friday.

Strategy:
  - Enumerate PDFs via predictable URL patterns: /pub/irs-wd/{YYYYWWNNN}.pdf
  - For sample mode: fetch recent weeks only
  - For full mode: crawl directory listing at /downloads/irs-wd?page=N
  - Extract text from PDFs using common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch recent weeks only
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from html.parser import HTMLParser

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IRS-PrivateLetterRulings")

USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"
PDF_BASE = "https://www.irs.gov/pub/irs-wd"
DIR_LISTING_BASE = "https://www.irs.gov/downloads/irs-wd"

# Current year and approximate week for enumeration
CURRENT_YEAR = 2026
MAX_WEEK = 53
MAX_DOC_PER_WEEK = 50


class DirectoryListingParser(HTMLParser):
    """Parse the IRS directory listing page to extract PDF filenames."""

    def __init__(self):
        super().__init__()
        self.filenames = []
        self._in_link = False
        self._current_href = ""

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value and value.endswith(".pdf"):
                    # Extract just the filename
                    fname = value.rsplit("/", 1)[-1]
                    if fname and not fname.endswith("_idx.pdf"):
                        self.filenames.append(fname)

    def error(self, message):
        pass


def _fetch_url(url: str, timeout: int = 30) -> Optional[bytes]:
    """Fetch URL and return bytes."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read()
    except (HTTPError, URLError) as e:
        logger.debug(f"Fetch failed for {url}: {e}")
        return None


def _fetch_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    """Fetch PDF and verify it's actually a PDF."""
    data = _fetch_url(url, timeout)
    if data and b"%PDF" in data[:20]:
        return data
    return None


def _extract_text(pdf_bytes: bytes, source_id: str) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/IRS-PrivateLetterRulings",
        source_id=source_id,
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""


def _parse_doc_number(filename: str) -> Optional[dict]:
    """Parse document number from filename like 202616008.pdf or 0637023.pdf."""
    name = filename.replace(".pdf", "").replace(".PDF", "")

    # 9-digit format: YYYYWWNNN (2020+)
    if len(name) == 9 and name.isdigit():
        return {
            "year": int(name[:4]),
            "week": int(name[4:6]),
            "seq": int(name[6:9]),
            "document_number": name,
        }
    # 7-digit format: YYWWNNN (pre-2020)
    elif len(name) == 7 and name.isdigit():
        yy = int(name[:2])
        year = 1900 + yy if yy >= 99 else 2000 + yy
        return {
            "year": year,
            "week": int(name[2:4]),
            "seq": int(name[4:7]),
            "document_number": f"{year}{name[2:]}",
        }
    return None


def _determine_doc_type(text: str) -> str:
    """Attempt to determine document type from content."""
    upper = text[:2000].upper()
    if "PRIVATE LETTER RULING" in upper or "PLR" in upper:
        return "Private Letter Ruling"
    elif "TECHNICAL ADVICE MEMORAND" in upper:
        return "Technical Advice Memorandum"
    elif "CHIEF COUNSEL ADVICE" in upper or "CCA" in upper:
        return "Chief Counsel Advice"
    elif "DETERMINATION LETTER" in upper:
        return "Determination Letter"
    elif "REVENUE RULING" in upper:
        return "Revenue Ruling"
    elif "REVENUE PROCEDURE" in upper:
        return "Revenue Procedure"
    return "Written Determination"


def _extract_uilc(text: str) -> Optional[str]:
    """Extract UILC (Uniform Issue List Code) from text."""
    match = re.search(r"UIL(?:\s*Code)?(?:\s*:)?\s*([\d.]+)", text[:3000])
    if match:
        return match.group(1)
    return None


def _extract_release_date(text: str) -> Optional[str]:
    """Extract release/issue date from document text."""
    # Try various date patterns in the header area
    patterns = [
        r"(?:Date|Release[d]?|Issue[d]?)[\s:]*(\w+ \d{1,2},?\s*\d{4})",
        r"(\w+ \d{1,2},?\s*\d{4})",
    ]
    for pat in patterns:
        match = re.search(pat, text[:2000])
        if match:
            date_str = match.group(1)
            for fmt in ["%B %d, %Y", "%B %d %Y", "%b %d, %Y", "%b %d %Y"]:
                try:
                    return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
    return None


class IRSWrittenDeterminationsScraper(BaseScraper):
    """
    Scraper for US/IRS-PrivateLetterRulings.
    Country: US
    URL: https://www.irs.gov/privacy-disclosure/irs-written-determinations

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _enumerate_week(self, year: int, week: int, max_docs: int = MAX_DOC_PER_WEEK) -> Generator[dict, None, None]:
        """Enumerate documents for a given year/week."""
        consecutive_misses = 0

        for seq in range(1, max_docs + 1):
            if year >= 2020:
                filename = f"{year}{week:02d}{seq:03d}.pdf"
            else:
                yy = year % 100
                filename = f"{yy:02d}{week:02d}{seq:03d}.pdf"

            url = f"{PDF_BASE}/{filename}"
            self.rate_limiter.wait()

            pdf_bytes = _fetch_pdf(url)
            if pdf_bytes:
                consecutive_misses = 0
                source_id = filename.replace(".pdf", "")
                text = _extract_text(pdf_bytes, source_id)

                if not text or len(text) < 100:
                    logger.warning(f"Text too short for {filename}")
                    continue

                doc_type = _determine_doc_type(text)
                uilc = _extract_uilc(text)
                release_date = _extract_release_date(text)

                yield {
                    "source_id": source_id,
                    "document_number": source_id,
                    "year": year,
                    "week": week,
                    "seq": seq,
                    "doc_type": doc_type,
                    "uilc": uilc,
                    "date": release_date,
                    "text": text,
                    "url": url,
                }
                logger.info(f"  Found {filename}: {doc_type} ({len(text)} chars)")
            else:
                consecutive_misses += 1
                if consecutive_misses >= 5:
                    break

    def _get_recent_weeks(self, num_weeks: int = 4) -> List[Tuple[int, int]]:
        """Get the most recent N weeks to check."""
        # Start from current approximate week and go backwards
        now = datetime.now()
        current_week = now.isocalendar()[1]
        current_year = now.year

        weeks = []
        year, week = current_year, current_week
        for _ in range(num_weeks):
            weeks.append((year, week))
            week -= 1
            if week <= 0:
                year -= 1
                week = 52
        return weeks

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IRS written determinations with full text.

        For full mode, enumerates recent years by week/seq pattern.
        """
        # Start from 2020 (new format) through current year
        for year in range(CURRENT_YEAR, 2019, -1):
            logger.info(f"=== Year {year} ===")
            for week in range(1, MAX_WEEK + 1):
                logger.info(f"  Week {week}...")
                count = 0
                for doc in self._enumerate_week(year, week):
                    count += 1
                    yield doc
                if count > 0:
                    logger.info(f"  Week {year}-W{week:02d}: {count} documents")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch documents from recent weeks."""
        weeks = self._get_recent_weeks(num_weeks=8)
        for year, week in weeks:
            for doc in self._enumerate_week(year, week):
                yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw IRS document into standard schema."""
        title = f"IRS {raw['doc_type']} {raw['document_number']}"
        if raw.get("uilc"):
            title += f" (UILC {raw['uilc']})"

        # Estimate date from year/week if not extracted from text
        date = raw.get("date")
        if not date:
            try:
                from datetime import date as dt_date
                d = dt_date.fromisocalendar(raw["year"], raw["week"], 5)  # Friday
                date = d.isoformat()
            except (ValueError, AttributeError):
                date = f"{raw['year']}-01-01"

        return {
            "_id": raw["source_id"],
            "_source": "US/IRS-PrivateLetterRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": raw.get("url", ""),
            "document_number": raw.get("document_number"),
            "year": raw.get("year"),
            "week": raw.get("week"),
            "doc_type": raw.get("doc_type"),
            "uilc": raw.get("uilc"),
        }


# === CLI entry point ===
if __name__ == "__main__":
    scraper = IRSWrittenDeterminationsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        # Quick connectivity test with a known recent document
        test_url = f"{PDF_BASE}/202616001.pdf"
        req = Request(test_url, headers={"User-Agent": USER_AGENT}, method="HEAD")
        try:
            resp = urlopen(req, timeout=15)
            print(f"OK: {test_url} → {resp.status}")
        except Exception as e:
            print(f"FAIL: {test_url} → {e}")
            sys.exit(1)

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 12 if sample else 999999

        if sample:
            # Sample mode: fetch from a recent known week
            logger.info("=== SAMPLE MODE: fetching ~12 records from recent weeks ===")
            weeks = scraper._get_recent_weeks(num_weeks=6)
            for year, week in weeks:
                logger.info(f"Trying {year}-W{week:02d}...")
                for raw in scraper._enumerate_week(year, week, max_docs=20):
                    record = scraper.normalize(raw)
                    out_file = sample_dir / f"{record['_id']}.json"
                    out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                    count += 1
                    logger.info(f"Saved [{count}]: {record['title'][:70]}")
                    if count >= max_records:
                        break
                if count >= max_records:
                    break
        elif command == "update":
            for raw in scraper.fetch_updates(""):
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:70]}")
        else:
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:70]}")

        logger.info(f"Done. Total records: {count}")
        if count == 0:
            logger.error("No records fetched — check connectivity")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
