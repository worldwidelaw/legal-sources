#!/usr/bin/env python3
"""
INTL/IRMCTCaseLaw -- IRMCT Case Law Database (ICTY/ICTR/IRMCT)

Fetches case law extracts from the International Residual Mechanism for
Criminal Tribunals, covering ICTY, ICTR, and IRMCT jurisdictions.

Strategy:
  - Download index from assets/homedata.json (full catalogue, ~2,500 extracts)
  - Fetch extract text via home/jsonExtract/{id} for each entry
  - Extract text is HTML, cleaned to plain text
  - No authentication required

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
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IRMCTCaseLaw")

INDEX_URL = "https://cld.irmct.org/assets/homedata.json"
EXTRACT_URL = "https://cld.irmct.org/home/jsonExtract/{id}"
BASE_URL = "https://cld.irmct.org"

# Column indices in homedata.json rows
COL_ID = 0
COL_NOTIONS = 1
COL_CASE = 2
COL_FILING_TITLE = 3
COL_DATE = 4
COL_ICTR_STATUTE = 5
COL_ICTY_STATUTE = 6
COL_IRMCT_STATUTE = 7
COL_ICTR_RULES = 8
COL_ICTY_RULES = 9
COL_IRMCT_RULES = 10
COL_OTHER_INSTRUMENTS = 11
COL_PDF_PATH = 12


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, preserving paragraph breaks."""
    if not text:
        return ""
    # Replace block elements with newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    # Remove all remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = html.unescape(text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> str:
    """Parse date from various formats to ISO 8601."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d", "%d %B %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return date_str


class IRMCTCaseLawScraper(BaseScraper):
    """
    Scraper for INTL/IRMCTCaseLaw -- IRMCT Case Law Database.
    Country: INTL
    URL: https://cld.irmct.org/

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
            "Accept": "application/json, text/html, */*",
        })

    def _fetch_index(self) -> list:
        """Download the full index from homedata.json."""
        logger.info("Fetching index from %s", INDEX_URL)
        resp = self.session.get(INDEX_URL, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        rows = data.get("data", [])
        logger.info("Index contains %d extract entries", len(rows))
        return rows

    def _fetch_extract_text(self, extract_id: int) -> str:
        """Fetch the extract text for a given ID."""
        url = EXTRACT_URL.format(id=extract_id)
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            raw_html = data.get("extract", "")
            return strip_html(raw_html)
        except Exception as e:
            logger.warning("Failed to fetch extract %d: %s", extract_id, e)
            return ""

    def _parse_notions(self, notions_html: str) -> list:
        """Extract legal notions from HTML formatted field."""
        if not notions_html:
            return []
        # Notions are typically in <a> tags or as plain text
        links = re.findall(r'>([^<]+)<', notions_html)
        if links:
            return [n.strip() for n in links if n.strip()]
        return [n.strip() for n in strip_html(notions_html).split(',') if n.strip()]

    def _parse_statutes(self, row: list) -> dict:
        """Extract statute and rule references from row."""
        refs = {}
        for col, key in [
            (COL_ICTR_STATUTE, "ictr_statute"),
            (COL_ICTY_STATUTE, "icty_statute"),
            (COL_IRMCT_STATUTE, "irmct_statute"),
            (COL_ICTR_RULES, "ictr_rules"),
            (COL_ICTY_RULES, "icty_rules"),
            (COL_IRMCT_RULES, "irmct_rules"),
            (COL_OTHER_INSTRUMENTS, "other_instruments"),
        ]:
            val = row[col] if col < len(row) else ""
            if val and str(val).strip():
                refs[key] = strip_html(str(val))
        return refs

    def _determine_tribunal(self, case_name: str, statutes: dict) -> str:
        """Determine which tribunal (ICTY/ICTR/IRMCT) from context."""
        case_upper = (case_name or "").upper()
        if "ICTR" in case_upper or "ictr_statute" in statutes or "ictr_rules" in statutes:
            return "ICTR"
        if "ICTY" in case_upper or "icty_statute" in statutes or "icty_rules" in statutes:
            return "ICTY"
        if "MICT" in case_upper or "irmct_statute" in statutes or "irmct_rules" in statutes:
            return "IRMCT"
        return "IRMCT"  # default

    def _row_to_raw(self, row: list) -> dict:
        """Convert an index row to a raw document dict."""
        extract_id = row[COL_ID]
        case_name = strip_html(str(row[COL_CASE])) if row[COL_CASE] else ""
        filing_title = strip_html(str(row[COL_FILING_TITLE])) if row[COL_FILING_TITLE] else ""
        date_str = str(row[COL_DATE]).strip() if row[COL_DATE] else ""
        pdf_path = str(row[COL_PDF_PATH]).strip() if len(row) > COL_PDF_PATH and row[COL_PDF_PATH] else ""
        notions = self._parse_notions(str(row[COL_NOTIONS]) if row[COL_NOTIONS] else "")
        statutes = self._parse_statutes(row)

        # Build PDF URL
        pdf_url = ""
        if pdf_path:
            if pdf_path.startswith("http"):
                pdf_url = pdf_path
            else:
                pdf_url = f"{BASE_URL}/{pdf_path.lstrip('/')}"

        return {
            "extract_id": extract_id,
            "case_name": case_name,
            "filing_title": filing_title,
            "date": date_str,
            "notions": notions,
            "statutes": statutes,
            "pdf_url": pdf_url,
            "tribunal": self._determine_tribunal(case_name, statutes),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all case law extracts."""
        rows = self._fetch_index()
        for i, row in enumerate(rows):
            raw = self._row_to_raw(row)
            # Fetch the extract text
            raw["extract_text"] = self._fetch_extract_text(raw["extract_id"])
            if (i + 1) % 100 == 0:
                logger.info("Fetched %d / %d extracts", i + 1, len(rows))
            yield raw

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch extracts updated since a given date."""
        rows = self._fetch_index()
        for row in rows:
            date_str = str(row[COL_DATE]).strip() if row[COL_DATE] else ""
            parsed = parse_date(date_str)
            if parsed:
                try:
                    doc_date = datetime.strptime(parsed, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if doc_date >= since:
                        raw = self._row_to_raw(row)
                        raw["extract_text"] = self._fetch_extract_text(raw["extract_id"])
                        yield raw
                except ValueError:
                    continue

    def normalize(self, raw: dict) -> dict:
        """Transform raw extract data into standard schema."""
        extract_id = raw["extract_id"]
        date = parse_date(raw.get("date", ""))

        return {
            "_id": f"IRMCT-{extract_id}",
            "_source": "INTL/IRMCTCaseLaw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("filing_title", ""),
            "text": raw.get("extract_text", ""),
            "date": date,
            "url": raw.get("pdf_url", f"{BASE_URL}/?extract={extract_id}"),
            "case_name": raw.get("case_name", ""),
            "tribunal": raw.get("tribunal", ""),
            "notions": raw.get("notions", []),
            "statutes": raw.get("statutes", {}),
            "extract_id": extract_id,
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = IRMCTCaseLawScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing IRMCT CLD connectivity...")
        resp = requests.get(INDEX_URL, timeout=30)
        data = resp.json()
        rows = data.get("data", [])
        print(f"  Index: {len(rows)} extracts")
        if rows:
            first_id = rows[0][COL_ID]
            ext_resp = requests.get(EXTRACT_URL.format(id=first_id), timeout=30)
            ext_data = ext_resp.json()
            text = strip_html(ext_data.get("extract", ""))
            print(f"  First extract (ID {first_id}): {len(text)} chars")
            print(f"  Preview: {text[:200]}...")
        print("OK")

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=90)
        result = scraper.update(since=since)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
