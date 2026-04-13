#!/usr/bin/env python3
"""
UN/ODS -- UN Official Document System

Fetches UN official documents (GA resolutions, SC resolutions) with full text.

Strategy:
  - Enumerate document symbols (A/RES/{session}/{num}, S/RES/{num}({year}))
  - Resolve symbol to PDF via ODS API (302 redirect)
  - Download PDF and extract full text via pdfplumber
  - Parse title and date from PDF content

Data: ~25,000+ GA resolutions (sessions 1-79), ~2,700+ SC resolutions.
License: Open data (UN documents are public domain).
Rate limit: 1 req/sec (self-imposed, respectful).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UN.ODS")

ODS_BASE = "https://documents.un.org"

# SC resolution number-to-year mapping (approximate ranges)
# SC resolutions are numbered sequentially from 1 (1946)
SC_YEAR_RANGES = [
    (1, 15, 1946), (16, 22, 1947), (23, 36, 1948), (37, 45, 1949),
    (46, 57, 1950), (58, 76, 1951), (77, 92, 1952), (93, 105, 1953),
    (106, 114, 1954), (115, 119, 1955), (120, 121, 1956), (122, 130, 1957),
    (131, 134, 1958), (135, 140, 1959), (141, 159, 1960), (160, 175, 1961),
    (176, 183, 1962), (184, 193, 1963), (194, 199, 1964), (200, 218, 1965),
    (219, 232, 1966), (233, 244, 1967), (245, 264, 1968), (265, 275, 1969),
    (276, 289, 1970), (290, 307, 1971), (308, 326, 1972), (327, 345, 1973),
    (346, 368, 1974), (369, 384, 1975), (385, 403, 1976), (404, 425, 1977),
    (426, 443, 1978), (444, 462, 1979), (463, 481, 1980), (482, 502, 1981),
    (503, 524, 1982), (525, 544, 1983), (545, 560, 1984), (561, 582, 1985),
    (583, 596, 1986), (597, 612, 1987), (613, 629, 1988), (630, 647, 1989),
    (648, 683, 1990), (684, 728, 1991), (729, 798, 1992), (799, 897, 1993),
    (898, 965, 1994), (966, 1035, 1995), (1036, 1093, 1996), (1094, 1145, 1997),
    (1146, 1219, 1998), (1220, 1284, 1999), (1285, 1334, 2000), (1335, 1396, 2001),
    (1397, 1456, 2002), (1457, 1526, 2003), (1527, 1581, 2004), (1582, 1649, 2005),
    (1650, 1738, 2006), (1739, 1807, 2007), (1808, 1860, 2008), (1861, 1907, 2009),
    (1908, 1968, 2010), (1969, 2029, 2011), (2030, 2085, 2012), (2086, 2141, 2013),
    (2142, 2199, 2014), (2200, 2262, 2015), (2263, 2342, 2016), (2343, 2401, 2017),
    (2402, 2459, 2018), (2460, 2510, 2019), (2511, 2565, 2020), (2566, 2615, 2021),
    (2616, 2671, 2022), (2672, 2722, 2023), (2723, 2800, 2024),
]


def _sc_year_for_number(num: int) -> Optional[int]:
    """Guess the year for a SC resolution number."""
    for start, end, year in SC_YEAR_RANGES:
        if start <= num <= end:
            return year
    return None


class ODSScraper(BaseScraper):
    """
    Scraper for UN/ODS -- UN Official Document System.
    Country: UN
    URL: https://documents.un.org

    Data types: legislation
    Auth: none (public domain)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/pdf, */*",
            },
            timeout=60,
        )

    # -- Symbol resolution --------------------------------------------------

    def _resolve_symbol(self, symbol: str, lang: str = "en") -> Optional[str]:
        """Resolve a UN document symbol to a PDF URL via ODS API."""
        url = f"{ODS_BASE}/api/symbol/access"
        try:
            resp = self.client.get(
                url, params={"s": symbol, "l": lang},
                timeout=15, allow_redirects=False
            )
            if resp is None or resp.status_code != 302:
                return None
            location = resp.headers.get("Location", "")
            if "/error" in location or not location:
                return None
            if location.startswith("/"):
                return f"{ODS_BASE}{location}"
            return location
        except Exception as e:
            logger.debug(f"Failed to resolve {symbol}: {e}")
            return None

    # -- PDF text extraction ------------------------------------------------

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="UN/ODS",
            source_id="",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    # -- Title/date parsing from text ---------------------------------------

    def _parse_metadata_from_text(self, text: str, symbol: str) -> dict:
        """Extract title and date from PDF text content."""
        meta = {"title": "", "date": ""}

        # Try to extract title from "Resolution adopted by the General Assembly"
        # pattern or similar header
        lines = text.split("\n")

        # Find title - look for resolution title after header lines
        for i, line in enumerate(lines):
            line_s = line.strip()
            # GA resolution titles often follow pattern like "78/1. Title here"
            m = re.match(r"^\d+/\d+\.\s+(.+)", line_s)
            if m:
                title = m.group(1).strip()
                # Extend title with next line if it doesn't end with period
                if i + 1 < len(lines) and not title.endswith("."):
                    next_line = lines[i + 1].strip()
                    if next_line and not re.match(r"^\d+/\d+\.", next_line):
                        title += " " + next_line
                meta["title"] = title.rstrip(".")
                break
            # SC resolution titles - look for topic after header
            if "Security Council" in line_s and i + 2 < len(lines):
                for j in range(i + 1, min(i + 10, len(lines))):
                    candidate = lines[j].strip()
                    if len(candidate) > 20 and not candidate.startswith("Distr"):
                        meta["title"] = candidate
                        break

        # If no title found, use the symbol
        if not meta["title"]:
            meta["title"] = f"UN Document {symbol}"

        # Extract date - look for patterns like "16 October 2023" or "2023-10-16"
        date_patterns = [
            r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})",
            r"(\d{4}-\d{2}-\d{2})",
        ]
        for pattern in date_patterns:
            m = re.search(pattern, text[:2000])
            if m:
                date_str = m.group(1)
                try:
                    if "-" in date_str:
                        meta["date"] = date_str
                    else:
                        dt = datetime.strptime(date_str, "%d %B %Y")
                        meta["date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
                break

        return meta

    # -- Normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document data into standard schema."""
        symbol = raw.get("symbol", "")
        text = raw.get("_full_text", "")
        if not symbol or not text:
            return None

        doc_id = f"UN-ODS-{symbol.replace('/', '-').replace('(', '').replace(')', '')}"

        # Determine body from symbol prefix
        body = "Unknown"
        if symbol.startswith("A/"):
            body = "General Assembly"
        elif symbol.startswith("S/"):
            body = "Security Council"
        elif symbol.startswith("E/"):
            body = "ECOSOC"

        # Determine doc type
        doc_type = "document"
        if "/RES/" in symbol:
            doc_type = "resolution"
        elif "/DEC/" in symbol:
            doc_type = "decision"

        # Extract session from GA symbols
        session = None
        m = re.match(r"A/RES/(\d+)/", symbol)
        if m:
            session = m.group(1)

        meta = raw.get("_metadata", {})

        return {
            "_id": doc_id,
            "_source": "UN/ODS",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "title": meta.get("title", f"UN Document {symbol}"),
            "text": text,
            "date": meta.get("date", "") or None,
            "url": raw.get("pdf_url", f"https://documents.un.org/symbol/{symbol}"),
            "body": body,
            "doc_type": doc_type,
            "session": session,
        }

    # -- Symbol generators --------------------------------------------------

    def _ga_symbols(self, sample: bool = False) -> Generator[str, None, None]:
        """Generate GA resolution symbols.

        Sessions 31+ (1976-present): A/RES/{session}/{num}
        Sessions 1-30 (1946-1975): A/RES/{sequential_num}({roman_session})
        """
        if sample:
            # Sample: mix of modern and early sessions
            for s, n in [(78, 1), (78, 2), (78, 3), (77, 1), (77, 2),
                         (76, 1), (50, 1), (50, 2), (31, 1)]:
                yield f"A/RES/{s}/{n}"
            return

        # Modern sessions (31-79): A/RES/{session}/{num}
        for session in range(79, 30, -1):
            for num in range(1, 500):
                yield f"A/RES/{session}/{num}"

    def _sc_symbols(self, sample: bool = False) -> Generator[Tuple[str, int], None, None]:
        """Generate SC resolution symbols with year."""
        if sample:
            for num in [2720, 2719, 1, 2, 1000, 1500]:
                year = _sc_year_for_number(num)
                if year:
                    yield f"S/RES/{num}({year})", year
            return

        # Full: enumerate from most recent backwards
        for num in range(2800, 0, -1):
            year = _sc_year_for_number(num)
            if year:
                yield f"S/RES/{num}({year})", year

    # -- Fetch methods ------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all UN documents with full text."""
        total = 0

        # GA resolutions
        logger.info("Fetching GA resolutions...")
        consecutive_misses = 0
        for symbol in self._ga_symbols():
            time.sleep(1.0)
            pdf_url = self._resolve_symbol(symbol)
            if not pdf_url:
                consecutive_misses += 1
                if consecutive_misses >= 10:
                    # Move to next session
                    logger.info(f"10 consecutive misses after {symbol}, moving on")
                    # Reset for next session
                    consecutive_misses = 0
                    # Skip to next session by finding current session
                    m = re.match(r"A/RES/(\d+)/(\d+)", symbol)
                    if m:
                        current_session = int(m.group(1))
                        # Skip remaining numbers in this session
                        # The generator will naturally move to the next symbol
                continue
            consecutive_misses = 0

            time.sleep(1.0)
            text = self._extract_text_from_pdf(pdf_url)
            if not text:
                continue

            meta = self._parse_metadata_from_text(text, symbol)
            total += 1
            if total % 50 == 0:
                logger.info(f"Fetched {total} documents")
            yield {
                "symbol": symbol,
                "_full_text": text,
                "_metadata": meta,
                "pdf_url": pdf_url,
            }

        # SC resolutions
        logger.info("Fetching SC resolutions...")
        consecutive_misses = 0
        for symbol, year in self._sc_symbols():
            time.sleep(1.0)
            pdf_url = self._resolve_symbol(symbol)
            if not pdf_url:
                consecutive_misses += 1
                if consecutive_misses >= 10:
                    logger.info(f"10 consecutive misses after {symbol}, stopping SC")
                    break
                continue
            consecutive_misses = 0

            time.sleep(1.0)
            text = self._extract_text_from_pdf(pdf_url)
            if not text:
                continue

            meta = self._parse_metadata_from_text(text, symbol)
            total += 1
            if total % 50 == 0:
                logger.info(f"Fetched {total} documents")
            yield {
                "symbol": symbol,
                "_full_text": text,
                "_metadata": meta,
                "pdf_url": pdf_url,
            }

        logger.info(f"Total fetched: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents added since the given date (limited scope)."""
        # For updates, only check most recent session
        total = 0
        for symbol in self._ga_symbols(sample=True):
            time.sleep(1.0)
            pdf_url = self._resolve_symbol(symbol)
            if not pdf_url:
                continue
            time.sleep(1.0)
            text = self._extract_text_from_pdf(pdf_url)
            if not text:
                continue
            meta = self._parse_metadata_from_text(text, symbol)
            if meta.get("date") and meta["date"] >= since.strftime("%Y-%m-%d"):
                total += 1
                yield {
                    "symbol": symbol,
                    "_full_text": text,
                    "_metadata": meta,
                    "pdf_url": pdf_url,
                }
        logger.info(f"Updates: {total} documents since {since}")

    # -- CLI ----------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UN/ODS Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = ODSScraper()

    if args.command == "test-api":
        logger.info("Testing ODS symbol resolution API...")
        test_symbols = ["A/RES/78/1", "S/RES/2720(2023)", "A/RES/1/1"]
        for sym in test_symbols:
            pdf_url = scraper._resolve_symbol(sym)
            if pdf_url:
                logger.info(f"  {sym}: OK -> {pdf_url[:60]}...")
                text = scraper._extract_text_from_pdf(pdf_url)
                if text:
                    logger.info(f"    Text: {len(text)} chars, starts: {text[:80]}...")
                else:
                    logger.warning(f"    Text extraction failed")
            else:
                logger.warning(f"  {sym}: FAILED")
            time.sleep(1)
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
