#!/usr/bin/env python3
"""
INTL/CCJ -- Caribbean Court of Justice

Fetches judgments from the CCJ website via Ninja Tables AJAX endpoints.

Strategy:
  - Fetch judgment listings from two Ninja Tables (Appellate + Original jurisdiction)
  - Each table record includes parties, dates, citation, and PDF link
  - Download PDFs and extract full text using PyMuPDF
  - ~276 judgments (Appellate 2005+, Original 2008+)

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
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.CCJ")

# Ninja Tables AJAX endpoints for judgment listings
AJAX_BASE = "https://ccj.org/wp-admin/admin-ajax.php"
AJ_TABLE_ID = "8856"   # Appellate Jurisdiction
OJ_TABLE_ID = "18497"  # Original Jurisdiction

# Extract PDF URL from the 'details' HTML field
PDF_URL_RE = re.compile(r'href="([^"]+\.pdf)"', re.IGNORECASE)

# Parse citation like [2025] CCJ 16 (AJ) BZ
CITATION_RE = re.compile(
    r'\[?\s*(\d{4})\s*\]?\s*CCJ\s+(\d+)\s*\(\s*(AJ|OJ)\s*\)',
    re.IGNORECASE
)


class CCJScraper(BaseScraper):
    """
    Scraper for INTL/CCJ -- Caribbean Court of Justice.
    Country: INTL
    URL: https://ccj.org/judgments-proceedings/

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
            "Accept": "application/json, text/plain, */*",
        })

    def _fetch_table_data(self, table_id: str) -> list:
        """Fetch all records from a Ninja Table via AJAX."""
        params = {
            "action": "wp_ajax_ninja_tables_public_action",
            "table_id": table_id,
            "target_action": "get-all-data",
            "default_sorting": "new_first",
            "skip_rows": "0",
            "limit_rows": "0",
        }
        logger.info(f"Fetching Ninja Table {table_id}")
        r = self.session.get(AJAX_BASE, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        logger.info(f"Table {table_id}: {len(data)} records")
        return data

    def _clean_html(self, html: str) -> str:
        """Strip HTML tags and clean whitespace."""
        text = re.sub(r'<br\s*/?>', '\n', html)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&#8217;', "'", text)
        text = re.sub(r'&nbsp;', ' ', text)
        return text.strip()

    def _parse_record(self, record: dict, jurisdiction_type: str) -> Optional[dict]:
        """Parse a Ninja Table record into structured data."""
        val = record.get("value", {})
        if not val:
            return None

        citation_raw = val.get("neutralcitationnumber", "").strip()
        # Clean HTML tags from citation
        citation_raw = re.sub(r'<[^>]+>', '', citation_raw).strip()
        # Fix common typos: {YYYY] -> [YYYY], (0J) -> (OJ)
        citation_raw = re.sub(r'\{(\d{4})\]', r'[\1]', citation_raw)
        citation_raw = citation_raw.replace('(0J)', '(OJ)')

        details = val.get("details", "")
        date_raw = val.get("deliverydate", "")

        # Extract PDF URL from details HTML
        pdf_match = PDF_URL_RE.search(details)
        pdf_url = pdf_match.group(1) if pdf_match else ""
        if not pdf_url:
            return None

        # Normalize PDF URL: ensure ccj.org domain (not www.ccj.org or caribbeancourtofjustice.org)
        pdf_url = re.sub(
            r'https?://(?:www\.)?(?:caribbeancourtofjustice\.org|ccj\.org)',
            'https://ccj.org',
            pdf_url
        )

        # Parse citation
        m = CITATION_RE.search(citation_raw)
        year = m.group(1) if m else ""
        number = m.group(2) if m else ""
        jtype = m.group(3).upper() if m else jurisdiction_type

        # Normalize date
        date_str = ""
        if date_raw:
            for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y", "%m/%d/%Y"):
                try:
                    date_str = datetime.strptime(date_raw.strip(), fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        # Extract parties based on jurisdiction type
        if jurisdiction_type == "AJ":
            appellant = self._clean_html(val.get("appellant", ""))
            respondent = self._clean_html(val.get("respondent", ""))
            parties = f"{appellant} v {respondent}" if appellant and respondent else appellant or respondent or ""
        else:
            claimant = self._clean_html(val.get("claimant", ""))
            defendant = self._clean_html(val.get("defendant", ""))
            parties = f"{claimant} v {defendant}" if claimant and defendant else claimant or defendant or ""

        # Country code from citation or filename
        country_code = ""
        country_m = re.search(r'(?:AJ|OJ)\)?\s+([A-Z]{2})', citation_raw)
        if not country_m:
            country_m = re.search(r'(?:AJ|OJ)[-_ ]+([A-Z]{2})', pdf_url, re.IGNORECASE)
        if country_m:
            country_code = country_m.group(1).upper()

        return {
            "citation": citation_raw.strip(),
            "year": year,
            "number": number,
            "jurisdiction_type": jtype,
            "country_code": country_code,
            "parties": parties,
            "date": date_str,
            "pdf_url": pdf_url,
            "keywords": self._clean_html(val.get("keywords", "")),
            "table_id": val.get("___id___", ""),
        }

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/CCJ",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _download_pdf_text(self, url: str) -> str:
        """Download a PDF and extract its text."""
        try:
            r = self.session.get(url, timeout=120)
            if r.status_code != 200:
                logger.warning(f"PDF download failed ({r.status_code}): {url}")
                return ""
            ct = r.headers.get("Content-Type", "")
            if "pdf" not in ct and "octet" not in ct:
                logger.warning(f"Not a PDF response ({ct}): {url}")
                return ""
            return self._extract_text_from_pdf(r.content)
        except Exception as e:
            logger.warning(f"PDF download error for {url}: {e}")
            return ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw item into standard schema."""
        citation = raw.get("citation", "")
        year = raw.get("year", "")
        number = raw.get("number", "")
        jtype = raw.get("jurisdiction_type", "")
        country = raw.get("country_code", "")

        _id = f"CCJ/{year}-{number}-{jtype}"
        if country:
            _id += f"-{country}"

        jtype_full = {
            "AJ": "Appellate Jurisdiction",
            "OJ": "Original Jurisdiction",
        }.get(jtype, jtype)

        title = citation
        parties = raw.get("parties", "")
        if parties:
            title = f"{citation} — {parties}"

        return {
            "_id": _id,
            "_source": "INTL/CCJ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date") or None,
            "url": raw.get("pdf_url", ""),
            "citation": citation,
            "court": "Caribbean Court of Justice",
            "jurisdiction_type": jtype_full,
            "country_code": country,
            "parties": parties,
            "year": year,
            "keywords": raw.get("keywords", ""),
        }

    def _get_all_judgments(self) -> Generator[dict, None, None]:
        """Fetch all judgments from both AJ and OJ tables."""
        # Appellate Jurisdiction
        aj_data = self._fetch_table_data(AJ_TABLE_ID)
        for record in aj_data:
            parsed = self._parse_record(record, "AJ")
            if parsed:
                yield parsed

        # Original Jurisdiction
        oj_data = self._fetch_table_data(OJ_TABLE_ID)
        for record in oj_data:
            parsed = self._parse_record(record, "OJ")
            if parsed:
                yield parsed

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments with full text."""
        idx = 0
        seen = set()

        for info in self._get_all_judgments():
            # Deduplicate by citation
            ckey = info["citation"].lower().strip()
            if ckey in seen:
                continue
            seen.add(ckey)

            idx += 1
            logger.info(f"[{idx}] Processing: {info['citation']}")

            text = self._download_pdf_text(info["pdf_url"])
            if not text:
                logger.warning(f"No text for: {info['citation']}")
                continue

            info["text"] = text
            yield info
            time.sleep(1.5)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield judgments newer than `since` (ISO date string)."""
        since_date = datetime.fromisoformat(since).date()
        idx = 0

        for info in self._get_all_judgments():
            if info.get("date"):
                try:
                    item_date = datetime.strptime(info["date"], "%Y-%m-%d").date()
                    if item_date < since_date:
                        continue
                except ValueError:
                    pass

            idx += 1
            logger.info(f"[{idx}] Processing update: {info['citation']}")

            text = self._download_pdf_text(info["pdf_url"])
            if not text:
                continue

            info["text"] = text
            yield info
            time.sleep(1.5)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="INTL/CCJ -- Caribbean Court of Justice"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = CCJScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            aj_data = scraper._fetch_table_data(AJ_TABLE_ID)
            oj_data = scraper._fetch_table_data(OJ_TABLE_ID)
            logger.info(f"AJ records: {len(aj_data)}, OJ records: {len(oj_data)}")

            # Test PDF download on first AJ record
            if aj_data:
                parsed = scraper._parse_record(aj_data[0], "AJ")
                if parsed:
                    logger.info(f"First judgment: {parsed['citation']}")
                    logger.info(f"PDF URL: {parsed['pdf_url']}")
                    text = scraper._download_pdf_text(parsed["pdf_url"])
                    if text:
                        logger.info(f"PDF text extracted: {len(text)} chars")
                        logger.info(f"Preview: {text[:200]}")
                        logger.info("Connectivity test passed!")
                    else:
                        logger.error("Failed to extract PDF text")
                        sys.exit(1)
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
