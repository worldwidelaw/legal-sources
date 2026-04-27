#!/usr/bin/env python3
"""
HK/BOR — Hong Kong Board of Review (Inland Revenue) Decisions

Fetches tax appeal decisions from the Hong Kong Board of Review under the
Inland Revenue Ordinance (Cap 112). ~2000 decisions from 1971–2025.

Strategy:
  - Discovery: published-decisions.php lists all volumes + supplements
  - Each volume page lists decisions with PDF links
  - Download English PDFs and extract full text via pdfplumber

Endpoints:
  - Index: https://www.info.gov.hk/bor/en/decisions/published-decisions.php
  - Volume pages: https://www.info.gov.hk/bor/en/decisions/decision-{vol}.htm
  - PDFs: https://www.info.gov.hk/bor/en/decisions/{path}.pdf

Data:
  - ~2000 Board of Review decisions (1971–2025)
  - No authentication required
  - Full text in PDF (English + some Chinese-only)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py bootstrap --full     # Full fetch
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HK.BOR")

BASE_URL = "https://www.info.gov.hk/bor/en/decisions/"
INDEX_URL = BASE_URL + "published-decisions.php"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber, fallback to pypdf."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"pdfplumber failed: {e}, trying pypdf")

    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n".join(pages)
    except Exception as e:
        logger.error(f"All PDF extraction failed: {e}")
        return ""


class HKBORScraper(BaseScraper):
    """
    Scraper for HK/BOR — Hong Kong Board of Review (Inland Revenue).
    Country: HK
    URL: https://www.info.gov.hk/bor/en/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 60) -> requests.Response:
        """Make HTTP GET request with rate limiting."""
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp

    def _discover_volume_pages(self) -> List[str]:
        """Get all volume/supplement page URLs from the index."""
        resp = self._get(INDEX_URL)
        html = resp.text
        pages = sorted(set(re.findall(r'decision-[\w-]+\.htm', html)))
        urls = [urljoin(BASE_URL, p) for p in pages]
        logger.info(f"Found {len(urls)} volume pages")
        return urls

    def _parse_volume_page(self, url: str) -> List[Dict[str, str]]:
        """Parse a volume page to extract decision metadata and PDF URLs."""
        try:
            resp = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch volume page {url}: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        decisions = []

        # Find all English PDF links
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            # Only English PDFs (not tc/ Chinese versions)
            if ".pdf" not in href.lower():
                continue
            if "/tc/" in href:
                continue

            # Resolve relative URL
            pdf_url = urljoin(url, href)

            # Extract case number from filename
            # Patterns: br571.pdf, d2605.pdf, D619.pdf, D924.pdf, D3022.pdf
            filename = href.split("/")[-1].replace(".pdf", "")
            case_num = self._parse_case_number(filename)

            if case_num:
                decisions.append({
                    "case_number": case_num,
                    "pdf_url": pdf_url,
                    "volume_page": url,
                })

        return decisions

    def _parse_case_number(self, filename: str) -> Optional[str]:
        """Parse case number from PDF filename.

        Examples:
            br571   -> D5/71    (old format: "br" prefix)
            d2605   -> D26/05   (mid format: lowercase "d")
            D619    -> D6/19    (new format: uppercase "D")
            D3022   -> D30/22
        """
        # Strip prefix: br, d, or D
        name = filename
        for prefix in ["br", "BR", "d", "D"]:
            if name.startswith(prefix) and len(name) > len(prefix):
                name = name[len(prefix):]
                break
        else:
            return None

        # Must be digits only
        if not name.isdigit():
            return None

        # Last 2 digits are the year, rest is case number
        if len(name) < 3:
            return None

        year_part = name[-2:]
        case_part = name[:-2]
        return f"D{case_part}/{year_part}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BOR decisions with metadata and PDF URLs."""
        volume_pages = self._discover_volume_pages()
        seen = set()
        total = 0

        for i, vp_url in enumerate(volume_pages, 1):
            logger.info(f"Processing volume page {i}/{len(volume_pages)}: {vp_url}")
            decisions = self._parse_volume_page(vp_url)

            for dec in decisions:
                case_num = dec["case_number"]
                if case_num in seen:
                    continue
                seen.add(case_num)
                total += 1
                yield dec

        logger.info(f"Discovery complete: {total} unique decisions found")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions (same as fetch_all for this small source)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download PDF and normalize into standard schema."""
        case_number = raw["case_number"]
        pdf_url = raw["pdf_url"]

        # Download PDF
        try:
            resp = self._get(pdf_url, timeout=120)
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF for {case_number}: {e}")
            return None

        if len(pdf_bytes) < 500:
            logger.warning(f"PDF too small for {case_number}: {len(pdf_bytes)} bytes")
            return None

        # Extract text
        text = extract_pdf_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for {case_number}: {len(text)} chars")
            return None

        # Parse year from case number (D{num}/{yy})
        match = re.match(r"D(\d+)/(\d{2})", case_number)
        if not match:
            return None

        year_suffix = int(match.group(2))
        year = 1900 + year_suffix if year_suffix >= 68 else 2000 + year_suffix

        # Try to extract decision date from text
        date_str = self._extract_date(text, year)

        # Build title
        title = f"Board of Review Decision {case_number}"
        # Try to extract subject from first lines
        subject = self._extract_subject(text)
        if subject:
            title = f"{title} — {subject}"

        return {
            "_id": f"HK-BOR-{case_number.replace('/', '-')}",
            "_source": "HK/BOR",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": pdf_url,
            "case_number": case_number,
            "court": "Board of Review (Inland Revenue Ordinance)",
            "jurisdiction": "Hong Kong",
            "year": year,
        }

    def _extract_date(self, text: str, fallback_year: int) -> str:
        """Try to extract decision date from text."""
        # Common patterns: "15th November 1971", "23 September 2024"
        patterns = [
            r'(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        ]
        for pat in patterns:
            m = re.search(pat, text[:2000])
            if m:
                day, month, year = m.group(1), m.group(2), m.group(3)
                try:
                    dt = datetime.strptime(f"{day} {month} {year}", "%d %B %Y")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
        # Fallback to January 1 of the case year
        return f"{fallback_year}-01-01"

    def _extract_subject(self, text: str) -> Optional[str]:
        """Try to extract subject line from decision text."""
        lines = text[:1500].split("\n")
        for line in lines:
            line = line.strip()
            # Subject lines often mention tax concepts
            if len(line) > 30 and any(kw in line.lower() for kw in
                ["tax", "profit", "salary", "deduction", "assessment",
                 "penalty", "depreciation", "allowance", "income",
                 "exemption", "relief", "stamp duty", "property"]):
                # Truncate long subjects
                if len(line) > 120:
                    line = line[:117] + "..."
                return line
        return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HK/BOR Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test", "status"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (12 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = HKBORScraper()

    if args.command == "test":
        print("Testing connectivity...")
        resp = scraper._get(INDEX_URL)
        pages = re.findall(r'decision-[\w-]+\.htm', resp.text)
        print(f"OK — Found {len(pages)} volume pages")
        # Test one PDF
        test_url = "https://www.info.gov.hk/bor/en/decisions/D924.pdf"
        resp = scraper._get(test_url)
        print(f"PDF download OK — {len(resp.content)} bytes")
        sys.exit(0)

    if args.command == "status":
        print(json.dumps(scraper.status, indent=2, default=str))
        sys.exit(0)

    if args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        result = scraper.bootstrap(
            sample_mode=sample_mode,
            sample_size=12 if sample_mode else 999999,
        )
        print(json.dumps(result, indent=2, default=str))
