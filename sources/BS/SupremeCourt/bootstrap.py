#!/usr/bin/env python3
"""
BS/SupremeCourt -- Bahamas Supreme Court Judgments

Fetches court judgments from courts.bs. The site is WordPress-based with
year-filtered listing pages containing PDF download links.

Strategy:
  - Iterate through years (2000-present) via ?judgement_year=YYYY
  - Parse HTML to extract case title, judge, date, and PDF URL
  - Download PDFs and extract full text

HTML structure:
  <ul class="judgements__row">
    <li><div>JUDGMENT</div><a href="javascript:void(0)">BETWEEN ... - case_number</a></li>
    <li><div>JUDGE</div>Judge Name</li>
    <li><div>DATE</div>Month DD, YYYY</li>
    <li><div>ACTION</div><a href="http://courts.bs/wp-content/uploads/..." download>...</a></li>
  </ul>

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BS.SupremeCourt")

BASE_URL = "https://courts.bs"
START_YEAR = 2000

# Match each judgement row block
ROW_RE = re.compile(
    r'<ul\s+class="judgements__row">(.*?)</ul>',
    re.DOTALL | re.IGNORECASE,
)

# Extract title from JUDGMENT li
TITLE_RE = re.compile(
    r'<div>JUDGMENT</div>\s*<a[^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

# Extract judge from JUDGE li
JUDGE_RE = re.compile(
    r'<div>JUDGE</div>\s*(.*?)(?:<li|</ul)',
    re.DOTALL | re.IGNORECASE,
)

# Extract date from DATE li
DATE_RE = re.compile(
    r'<div>DATE</div>\s*(.*?)</li>',
    re.DOTALL | re.IGNORECASE,
)

# Extract PDF URL from ACTION li
PDF_RE = re.compile(
    r'<div>ACTION</div>\s*<a\s+href="([^"]+\.pdf)"',
    re.DOTALL | re.IGNORECASE,
)

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def parse_date(date_str: str) -> Optional[str]:
    """Parse 'Month DD, YYYY' to ISO date."""
    date_str = re.sub(r'<[^>]+>', '', date_str).strip()
    match = re.match(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', date_str)
    if match:
        month_name, day, year = match.groups()
        month = MONTH_MAP.get(month_name.lower())
        if month:
            return f"{year}-{month:02d}-{int(day):02d}"
    return None


def clean_html(text: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&#8211;|&ndash;', '–', text)
    text = re.sub(r'&#8212;|&mdash;', '—', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


class BSSupremeCourtScraper(BaseScraper):
    """
    Scraper for BS/SupremeCourt -- Bahamas Supreme Court Judgments.
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
            timeout=120,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        return extract_pdf_markdown(
            source="BS/SupremeCourt",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _parse_year_page(self, year: int) -> List[Dict[str, Any]]:
        """Fetch and parse a year's judgment listing."""
        url = f"/judgments/?judgement_year={year}"
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch year {year}: {e}")
            return []

        results = []
        for row_match in ROW_RE.finditer(resp.text):
            row_html = row_match.group(1)

            entry = {"year": year}

            # Title
            title_match = TITLE_RE.search(row_html)
            if title_match:
                entry["title"] = clean_html(title_match.group(1))

            # Judge
            judge_match = JUDGE_RE.search(row_html)
            if judge_match:
                entry["judge"] = clean_html(judge_match.group(1))

            # Date
            date_match = DATE_RE.search(row_html)
            if date_match:
                raw_date = date_match.group(1)
                entry["date"] = parse_date(raw_date)
                entry["raw_date"] = clean_html(raw_date)

            # PDF URL
            pdf_match = PDF_RE.search(row_html)
            if pdf_match:
                pdf_url = pdf_match.group(1)
                # Ensure HTTPS
                entry["pdf_url"] = pdf_url.replace("http://", "https://")

            if entry.get("pdf_url"):
                results.append(entry)

        return results

    def _make_doc_id(self, pdf_url: str) -> str:
        return hashlib.sha256(pdf_url.encode()).hexdigest()[:16]

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        pdf_url = raw.get("pdf_url", "")
        doc_id = self._make_doc_id(pdf_url)

        return {
            "_id": f"BS/SupremeCourt/{doc_id}",
            "_source": "BS/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": now,
            "title": raw.get("title", "Unknown"),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": pdf_url,
            "doc_id": doc_id,
            "judge": raw.get("judge"),
            "decision_date": raw.get("date"),
            "case_number": raw.get("case_number"),
            "file_url": pdf_url,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now().year
        limit = 15 if sample else None
        count = 0

        for year in range(current_year, START_YEAR - 1, -1):
            if limit and count >= limit:
                break

            logger.info(f"Fetching judgments for {year}...")
            entries = self._parse_year_page(year)
            logger.info(f"  Found {len(entries)} entries for {year}")

            for entry in entries:
                if limit and count >= limit:
                    break

                pdf_url = entry["pdf_url"]
                title = entry.get("title", "?")
                logger.info(f"  [{count + 1}] Downloading: {title[:60]}")

                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(pdf_url)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"  Failed to download: {e}")
                    continue

                if resp.content[:5] != b"%PDF-":
                    logger.warning(f"  Not a PDF: {pdf_url}")
                    continue

                text = self._extract_pdf_text(resp.content)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  No meaningful text from {title[:40]}")
                    continue

                entry["text"] = text
                yield entry
                count += 1

        logger.info(f"Fetched {count} judgments total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now().year
        logger.info(f"Fetching updates for {current_year}...")
        entries = self._parse_year_page(current_year)

        for entry in entries:
            pdf_url = entry["pdf_url"]
            try:
                self.rate_limiter.wait()
                resp = self.client.get(pdf_url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"  Failed: {e}")
                continue

            if resp.content[:5] != b"%PDF-":
                continue

            text = self._extract_pdf_text(resp.content)
            if not text or len(text.strip()) < 50:
                continue

            entry["text"] = text
            yield entry


if __name__ == "__main__":
    scraper = BSSupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
