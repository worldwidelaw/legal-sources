#!/usr/bin/env python3
"""
HK/IRD-TaxDoctrine -- Hong Kong IRD Tax Guidance

Fetches tax doctrine from the Hong Kong Inland Revenue Department:
  - Departmental Interpretation and Practice Notes (DIPNs): 64 PDFs
  - Stamp Office Interpretation and Practice Notes (SOIPNs): 8 PDFs
  - Advance Ruling Cases: 78 HTML pages

Strategy:
  - Enumerate known document numbers for each type
  - DIPNs/SOIPNs: download PDFs, extract text with pdfplumber
  - Advance Rulings: fetch HTML, extract text content
  - Parse index pages for titles and dates

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Re-fetch all (static collection)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import io
import json
import logging
import time
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HK.IRD-TaxDoctrine")

BASE_URL = "https://www.ird.gov.hk"
USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

# Document ranges
DIPN_NUMBERS = list(range(1, 64)) + ["13a"]  # DIPN 01-63 + 13A
SOIPN_NUMBERS = list(range(1, 9))  # SOIPN 01-08
ADVANCE_RULING_NUMBERS = list(range(1, 79))  # Cases 1-78


def _fetch_url(url: str, timeout: int = 30, binary: bool = False):
    """Fetch URL content. Returns bytes if binary=True, else str."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        if binary:
            return data
        return data.decode("utf-8", errors="replace")
    except HTTPError as e:
        if e.code in (404, 410, 403):
            return None
        raise
    except URLError:
        return None


def _strip_html(html_text: str) -> str:
    """Strip HTML tags and decode entities from text."""
    text = re.sub(r"<script[^>]*>.*?</script>", "", html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="HK/IRD-TaxDoctrine",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="doctrine",
    ) or ""

def _parse_dipn_index() -> Dict[str, Dict[str, str]]:
    """Parse the DIPN index page for titles and revision dates."""
    url = f"{BASE_URL}/eng/ppr/dip.htm"
    page = _fetch_url(url)
    if not page:
        return {}

    results = {}
    # Each DIPN is in a table row with 3 cells: number, title, date
    # Match rows containing dipnNN.pdf links
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', page, re.DOTALL | re.IGNORECASE)
    for row in rows:
        num_match = re.search(r'dipn(\d+a?)\.pdf', row, re.IGNORECASE)
        if not num_match:
            continue
        num = num_match.group(1).lower()

        # Extract cells
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        title = ""
        date = ""
        if len(cells) >= 2:
            # Second cell contains the title
            title = _strip_html(cells[1]).strip()
            # Clean up multiple spaces
            title = re.sub(r'\s+', ' ', title)
        if len(cells) >= 3:
            # Third cell contains the date
            date = _strip_html(cells[2]).strip()

        results[num] = {"title": title, "date": date}

    return results


def _parse_advance_ruling_index() -> Dict[int, Dict[str, str]]:
    """Parse the advance ruling index page for case titles."""
    url = f"{BASE_URL}/eng/ppr/arc.htm"
    page = _fetch_url(url)
    if not page:
        return {}

    results = {}
    # Find links to advance ruling pages
    matches = re.findall(
        r'advance(\d+)\.htm["\'][^>]*>(.*?)</a>',
        page, re.DOTALL | re.IGNORECASE
    )
    for num_str, title_html in matches:
        num = int(num_str)
        title = _strip_html(title_html).strip()
        results[num] = {"title": title}

    return results


class HongKongIRDTaxDoctrineScraper(BaseScraper):
    """
    Scraper for HK/IRD-TaxDoctrine.
    Country: HK
    URL: https://www.ird.gov.hk/eng/ppr/dip.htm

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._dipn_index = None
        self._ar_index = None

    def _get_dipn_index(self):
        if self._dipn_index is None:
            self._dipn_index = _parse_dipn_index()
        return self._dipn_index

    def _get_ar_index(self):
        if self._ar_index is None:
            self._ar_index = _parse_advance_ruling_index()
        return self._ar_index

    def _fetch_dipn(self, num) -> Optional[dict]:
        """Fetch a single DIPN document."""
        num_str = str(num).lower()
        padded = num_str.zfill(2) if num_str != "13a" else "13a"
        url = f"{BASE_URL}/eng/pdf/dipn{padded}.pdf"

        self.rate_limiter.wait()
        pdf_bytes = _fetch_url(url, binary=True)
        if not pdf_bytes:
            logger.debug(f"DIPN {padded}: not found")
            return None

        text = _extract_pdf_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"DIPN {padded}: PDF text extraction failed or too short")
            return None

        # Get title and date from index page
        idx = self._get_dipn_index()
        # Index keys are zero-padded (01, 02, ...) or "13a"
        lookup_key = padded if num_str == "13a" else str(num).zfill(2) if isinstance(num, int) else num_str.zfill(2)
        info = idx.get(lookup_key, {})
        title = info.get("title", "")
        date_str = info.get("date", "")

        if not title:
            title = f"DIPN No. {padded}"

        return {
            "doc_id": f"DIPN-{padded.upper()}",
            "doc_type": "DIPN",
            "title": f"DIPN {padded.upper()}: {title}",
            "text": text,
            "url": url,
            "num": num_str,
            "index_date": date_str,
        }

    def _fetch_soipn(self, num: int) -> Optional[dict]:
        """Fetch a single SOIPN document."""
        padded = str(num).zfill(2)
        url = f"{BASE_URL}/eng/pdf/soipn{padded}.pdf"

        self.rate_limiter.wait()
        pdf_bytes = _fetch_url(url, binary=True)
        if not pdf_bytes:
            logger.debug(f"SOIPN {padded}: not found")
            return None

        text = _extract_pdf_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"SOIPN {padded}: PDF text extraction failed or too short")
            return None

        # Extract title from first meaningful line
        title = ""
        lines = text.split("\n")
        for line in lines[:15]:
            line = line.strip()
            if len(line) > 20 and "stamp office" not in line.lower() and "inland revenue" not in line.lower():
                title = line
                break
        if not title:
            title = f"SOIPN No. {num}"

        return {
            "doc_id": f"SOIPN-{padded}",
            "doc_type": "SOIPN",
            "title": f"SOIPN {padded}: {title}",
            "text": text,
            "url": url,
            "num": str(num),
            "index_date": "",
        }

    def _fetch_advance_ruling(self, num: int) -> Optional[dict]:
        """Fetch a single advance ruling case (HTML)."""
        url = f"{BASE_URL}/eng/ppr/advance{num}.htm"

        self.rate_limiter.wait()
        page = _fetch_url(url)
        if not page:
            logger.debug(f"Advance Ruling {num}: not found")
            return None

        # Extract main content
        # Try to find the main content area
        content = page
        # Remove header/nav/footer
        for pattern in [
            r'<div[^>]*class="[^"]*header[^"]*"[^>]*>.*?</div>',
            r'<div[^>]*class="[^"]*footer[^"]*"[^>]*>.*?</div>',
            r'<div[^>]*class="[^"]*nav[^"]*"[^>]*>.*?</div>',
        ]:
            content = re.sub(pattern, "", content, flags=re.DOTALL | re.IGNORECASE)

        text = _strip_html(content)
        if not text or len(text) < 100:
            logger.warning(f"Advance Ruling {num}: content too short")
            return None

        # Get title from index or from page
        idx = self._get_ar_index()
        ar_info = idx.get(num, {})
        title = ar_info.get("title", "")

        if not title:
            # Try title tag
            m = re.search(r"<title>([^<]+)</title>", page)
            if m:
                title = html.unescape(m.group(1)).strip()
            else:
                title = f"Advance Ruling Case No. {num}"

        return {
            "doc_id": f"AR-{num}",
            "doc_type": "AdvanceRuling",
            "title": title,
            "text": text,
            "url": url,
            "num": str(num),
            "index_date": "",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all HK IRD tax doctrine documents."""
        # Fetch DIPNs (PDFs)
        logger.info("Fetching DIPNs (64 documents)...")
        for num in DIPN_NUMBERS:
            doc = self._fetch_dipn(num)
            if doc:
                yield doc

        # Fetch SOIPNs (PDFs)
        logger.info("Fetching SOIPNs (8 documents)...")
        for num in SOIPN_NUMBERS:
            doc = self._fetch_soipn(num)
            if doc:
                yield doc

        # Fetch Advance Rulings (HTML)
        logger.info("Fetching Advance Rulings (78 documents)...")
        for num in ADVANCE_RULING_NUMBERS:
            doc = self._fetch_advance_ruling(num)
            if doc:
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all documents (collection is mostly static)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize a raw document into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        # Try index date first, then extract from text
        date = None
        index_date = raw.get("index_date", "")
        if index_date:
            for fmt in ["%B %Y", "%d %B %Y", "%d %b %Y", "%Y-%m-%d"]:
                try:
                    date = datetime.strptime(index_date.strip(), fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        if not date:
            date_patterns = [
                r'(?:Revised|Updated|Issued|Date)[:\s]*(\d{1,2}\s+\w+\s+\d{4})',
                r'(\w+\s+\d{4})\s*$',
                r'(\d{4}-\d{2}-\d{2})',
            ]
            for pat in date_patterns:
                m = re.search(pat, text[:2000])
                if m:
                    date_str = m.group(1).strip()
                    for fmt in ["%d %B %Y", "%d %b %Y", "%Y-%m-%d", "%B %Y"]:
                        try:
                            date = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            continue
                    if date:
                        break

        return {
            "_id": raw["doc_id"],
            "_source": "HK/IRD-TaxDoctrine",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "doc_type": raw["doc_type"],
            "title": raw["title"],
            "text": text,
            "date": date,
            "url": raw["url"],
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="HK/IRD-TaxDoctrine bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--sample-size", type=int, default=15, help="Sample size")
    args = parser.parse_args()

    scraper = HongKongIRDTaxDoctrineScraper()

    if args.command == "test":
        logger.info("Testing connectivity to ird.gov.hk...")
        page = _fetch_url(f"{BASE_URL}/eng/ppr/advance1.htm")
        if page and len(page) > 500:
            logger.info(f"SUCCESS: ird.gov.hk accessible, page size={len(page)} bytes")
        else:
            logger.error("FAILED: Could not fetch test document")
            sys.exit(1)

    elif args.command == "bootstrap":
        result = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap result: {json.dumps(result, indent=2, default=str)}")

    elif args.command == "update":
        result = scraper.update()
        logger.info(f"Update result: {json.dumps(result, indent=2, default=str)}")


if __name__ == "__main__":
    main()
