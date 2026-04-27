#!/usr/bin/env python3
"""
BM/SupremeCourt -- Bermuda Supreme Court & Court of Appeal Judgments

Fetches court judgments from gov.bm. Judgments are published as PDFs organized
by year on separate pages (court-judgments for current year, court-judgments-YYYY
for previous years).

Strategy:
  - Iterate through year pages (2017-present)
  - Extract PDF links and case metadata from link text
  - Download PDFs and extract full text

Endpoints:
  - Current year: https://www.gov.bm/court-judgments
  - Past years: https://www.gov.bm/court-judgments-{YYYY}

Data:
  - ~100-150 judgments per year (2017-present)
  - Supreme Court + Court of Appeal
  - Language: English
  - Format: PDF

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
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
from urllib.parse import urljoin, unquote
import html as html_mod

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BM.SupremeCourt")

BASE_URL = "https://www.gov.bm"
START_YEAR = 2017
# Regex to extract PDF links with their anchor text
PDF_LINK_RE = re.compile(
    r'<a[^>]*href="(/sites/default/files/[^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
# Regex to extract date from link text (DD Month YYYY or similar)
DATE_RE = re.compile(
    r'\((\d{1,2}\s+\w+\s+\d{4})\)',
)
# Regex to extract citation
CITATION_RE = re.compile(
    r'\[(\d{4})\]\s*(CA|SC)\s*\(Bda\)\s*(\d+)\s*(\w+)',
    re.IGNORECASE,
)

MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


class BMSupremeCourtScraper(BaseScraper):
    """
    Scraper for BM/SupremeCourt -- Bermuda Supreme Court & Court of Appeal.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en,en-US;q=0.9",
            },
            timeout=120,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        return extract_pdf_markdown(
            source="BM/SupremeCourt",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse 'DD Month YYYY' to ISO date."""
        try:
            parts = date_str.strip().split()
            if len(parts) == 3:
                day = int(parts[0])
                month = MONTH_MAP.get(parts[1].lower())
                year = parts[2]
                if month:
                    return f"{year}-{month}-{day:02d}"
        except Exception:
            pass
        return None

    def _parse_link_text(self, text: str) -> Dict[str, str]:
        """Parse case name, citation, court, and date from link text."""
        text = html_mod.unescape(text).strip()
        text = re.sub(r'\s+', ' ', text)

        result = {"raw_title": text}

        # Extract date
        date_match = DATE_RE.search(text)
        if date_match:
            result["date_str"] = date_match.group(1)
            result["date"] = self._parse_date(date_match.group(1))

        # Extract citation
        cite_match = CITATION_RE.search(text)
        if cite_match:
            court_code = cite_match.group(2).upper()
            result["citation"] = cite_match.group(0)
            result["court"] = "Court of Appeal" if court_code == "CA" else "Supreme Court"

        # Extract case name (everything before the citation bracket)
        name_part = re.split(r'\s*\[\d{4}\]', text)[0].strip()
        if name_part:
            result["case_name"] = name_part

        return result

    def _get_year_url(self, year: int) -> str:
        current_year = datetime.now().year
        if year == current_year:
            return "/court-judgments"
        return f"/court-judgments-{year}"

    def _collect_judgments_for_year(self, year: int) -> List[Dict[str, Any]]:
        """Fetch a year page and extract all judgment links."""
        url = self._get_year_url(year)
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Failed to fetch {year}: {e}")
            return []

        results = []
        for match in PDF_LINK_RE.finditer(resp.text):
            pdf_path = match.group(1)
            link_text = match.group(2)
            meta = self._parse_link_text(link_text)
            meta["pdf_path"] = pdf_path
            meta["year"] = year
            results.append(meta)

        return results

    def _make_doc_id(self, pdf_path: str) -> str:
        return hashlib.sha256(pdf_path.encode()).hexdigest()[:16]

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        pdf_path = raw.get("pdf_path", "")
        doc_id = self._make_doc_id(pdf_path)
        title = raw.get("case_name") or raw.get("raw_title", "Unknown")
        full_url = f"{BASE_URL}{pdf_path}" if pdf_path else ""

        return {
            "_id": f"BM/SupremeCourt/{doc_id}",
            "_source": "BM/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": full_url,
            "doc_id": doc_id,
            "citation": raw.get("citation"),
            "court": raw.get("court", "Supreme Court"),
            "decision_date": raw.get("date"),
            "file_url": full_url,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now().year
        limit = 15 if sample else None
        count = 0

        for year in range(current_year, START_YEAR - 1, -1):
            if limit and count >= limit:
                break

            logger.info(f"Fetching judgments for {year}...")
            judgments = self._collect_judgments_for_year(year)
            logger.info(f"  Found {len(judgments)} judgments for {year}")

            for meta in judgments:
                if limit and count >= limit:
                    break

                pdf_path = meta["pdf_path"]
                title = meta.get("case_name") or meta.get("raw_title", "?")
                logger.info(f"  [{count + 1}] Downloading: {title[:60]}")

                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(pdf_path)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"  Failed to download {pdf_path}: {e}")
                    continue

                if resp.content[:5] != b"%PDF-":
                    logger.warning(f"  Not a PDF: {pdf_path}")
                    continue

                text = self._extract_pdf_text(resp.content)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  No meaningful text from {title[:40]}")
                    continue

                meta["text"] = text
                yield meta
                count += 1

        logger.info(f"Fetched {count} judgments total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now().year
        logger.info(f"Fetching updates for {current_year}...")
        judgments = self._collect_judgments_for_year(current_year)

        for meta in judgments:
            pdf_path = meta["pdf_path"]
            try:
                self.rate_limiter.wait()
                resp = self.client.get(pdf_path)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"  Failed: {e}")
                continue

            if resp.content[:5] != b"%PDF-":
                continue

            text = self._extract_pdf_text(resp.content)
            if not text or len(text.strip()) < 50:
                continue

            meta["text"] = text
            yield meta


if __name__ == "__main__":
    scraper = BMSupremeCourtScraper()

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
