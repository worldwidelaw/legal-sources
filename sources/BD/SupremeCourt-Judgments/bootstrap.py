#!/usr/bin/env python3
"""
BD/SupremeCourt-Judgments -- Bangladesh Supreme Court Judgments Fetcher

Fetches judgments from the Supreme Court of Bangladesh (supremecourt.gov.bd).

Strategy:
  - Paginate the judgment listing pages for both divisions (div_id=1: Appellate, div_id=2: High Court)
  - Parse HTML table rows to extract case metadata
  - Download each judgment's PDF from /resources/documents/
  - Extract full text using PyMuPDF (fitz)

Endpoints:
  - Listing: https://www.supremecourt.gov.bd/web/?page=judgments.php&menu=00&div_id={1|2}&start={offset}
  - PDF: https://www.supremecourt.gov.bd/resources/documents/{filename}.pdf

Data:
  - Appellate Division: ~354 judgments
  - High Court Division: ~8,700+ judgments
  - Full text in English (primary) and Bengali
  - License: Public court records

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import logging
import hashlib
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BD.SupremeCourt-Judgments")

BASE_URL = "https://www.supremecourt.gov.bd"
LISTING_URL = BASE_URL + "/web/?page=judgments.php&menu=00&div_id={div_id}&start={start}"
PDF_BASE_URL = BASE_URL + "/resources/documents/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,bn;q=0.8",
}

DIVISIONS = {
    1: "Appellate Division",
    2: "High Court Division",
}

PAGE_SIZE = 50  # Appellate Division uses 50, HCD uses ~25 but start increments by 50


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="BD/SupremeCourt-Judgments",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def parse_listing_page(html_content: str, div_id: int) -> List[Dict[str, Any]]:
    """Parse a judgment listing page and return list of judgment metadata dicts."""
    results = []

    # Find all table rows containing PDF links
    row_pattern = r"<tr[^>]*>(.*?)</tr>"
    rows = re.findall(row_pattern, html_content, re.DOTALL)

    for row in rows:
        if "resources/documents" not in row:
            continue

        # Extract table cells
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        if len(cells) < 4:
            continue

        # Clean cell content
        def clean_cell(c):
            c = re.sub(r"<[^>]+>", " ", c)
            c = html_mod.unescape(c)
            c = re.sub(r"\s+", " ", c).strip()
            return c

        serial = clean_cell(cells[0])
        case_info = clean_cell(cells[1])
        parties = clean_cell(cells[2])
        summary = clean_cell(cells[3])

        # Extract PDF URLs (direct download, not the translation one)
        pdf_links = re.findall(
            r'href=["\'](\.\./resources/documents/[^"\']+\.pdf)["\']', row
        )
        if not pdf_links:
            # Try absolute URL pattern
            pdf_links = re.findall(
                r'href=["\'](https?://[^"\']*resources/documents/[^"\']+\.pdf)["\']', row
            )

        pdf_url = None
        pdf_filename = None
        if pdf_links:
            raw_link = pdf_links[0]
            if raw_link.startswith("../"):
                pdf_filename = raw_link.replace("../resources/documents/", "")
                pdf_url = PDF_BASE_URL + pdf_filename
            elif raw_link.startswith("http"):
                pdf_url = raw_link
                pdf_filename = raw_link.split("/")[-1]

        if not pdf_url:
            continue

        # Parse case number and year from case_info
        case_number = case_info.strip()
        year_match = re.search(r"(?:of|/)\s*(\d{4})", case_info)
        year = year_match.group(1) if year_match else None

        # Generate unique ID from filename
        case_id = pdf_filename if pdf_filename else hashlib.md5(
            f"{div_id}:{case_number}:{parties}".encode()
        ).hexdigest()

        results.append({
            "case_id": case_id,
            "div_id": div_id,
            "division": DIVISIONS[div_id],
            "case_number": case_number,
            "parties": parties,
            "summary": summary,
            "year": year,
            "pdf_url": pdf_url,
            "pdf_filename": pdf_filename,
        })

    return results


class BDSupremeCourtScraper(BaseScraper):
    """
    Scraper for BD/SupremeCourt-Judgments -- Bangladesh Supreme Court.
    Country: BD
    URL: https://www.supremecourt.gov.bd

    Data types: case_law
    Auth: none (public court records)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_listing_page(self, div_id: int, start: int) -> str:
        """Fetch a single listing page."""
        url = LISTING_URL.format(div_id=div_id, start=start)
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.text

    def _fetch_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type and not resp.content[:5] == b"%PDF-":
                logger.warning(f"Not a PDF response from {pdf_url}: {content_type}")
                return None

            text = extract_pdf_text(resp.content)
            if len(text.strip()) < 50:
                logger.warning(f"Very short text extracted from {pdf_url}: {len(text)} chars")
                return None

            return text

        except Exception as e:
            logger.error(f"Failed to fetch/extract PDF {pdf_url}: {e}")
            return None

    def _iter_judgments(self, div_id: int) -> Generator[Dict[str, Any], None, None]:
        """Iterate over all judgments for a given division."""
        start = 0
        consecutive_empty = 0

        while True:
            logger.info(f"Fetching {DIVISIONS[div_id]} listing page start={start}")
            try:
                html_content = self._fetch_listing_page(div_id, start)
            except Exception as e:
                logger.error(f"Failed to fetch listing page div={div_id} start={start}: {e}")
                break

            entries = parse_listing_page(html_content, div_id)
            if not entries:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    logger.info(f"No more entries for {DIVISIONS[div_id]} after start={start}")
                    break
                start += PAGE_SIZE
                continue

            consecutive_empty = 0
            for entry in entries:
                yield entry

            start += PAGE_SIZE

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments from both divisions with full text."""
        for div_id in [1, 2]:
            logger.info(f"Starting {DIVISIONS[div_id]} fetch...")
            for meta in self._iter_judgments(div_id):
                # Download PDF and extract text
                text = self._fetch_pdf_text(meta["pdf_url"])
                if not text:
                    logger.warning(f"Skipping {meta['case_id']} - no text extracted")
                    continue

                meta["text"] = text
                yield meta

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent judgments (first few pages of each division)."""
        for div_id in [1, 2]:
            logger.info(f"Fetching updates for {DIVISIONS[div_id]}...")
            # Only check first 2 pages (most recent)
            for start in [0, PAGE_SIZE]:
                try:
                    html_content = self._fetch_listing_page(div_id, start)
                except Exception as e:
                    logger.error(f"Failed to fetch listing: {e}")
                    continue

                entries = parse_listing_page(html_content, div_id)
                for meta in entries:
                    text = self._fetch_pdf_text(meta["pdf_url"])
                    if not text:
                        continue
                    meta["text"] = text
                    yield meta

    def normalize(self, raw: dict) -> dict:
        """Transform raw judgment data into standard schema."""
        case_id = raw.get("case_id", "")
        division = raw.get("division", "")
        case_number = raw.get("case_number", "")
        parties = raw.get("parties", "")
        summary = raw.get("summary", "")
        text = raw.get("text", "")
        year = raw.get("year")
        pdf_url = raw.get("pdf_url", "")

        # Build title from parties and case number
        title = f"{case_number}"
        if parties:
            title = f"{parties} ({case_number})"

        # Try to extract a date
        date_str = None
        if year:
            date_str = f"{year}-01-01"

        return {
            "_id": f"BD-SC-{hashlib.md5(case_id.encode()).hexdigest()[:12]}",
            "_source": "BD/SupremeCourt-Judgments",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": case_id,
            "title": title,
            "text": text,
            "date": date_str,
            "division": division,
            "case_type": raw.get("case_number", "").split()[0] if raw.get("case_number") else None,
            "case_number": case_number,
            "parties": parties,
            "summary": summary,
            "url": pdf_url,
        }


def main():
    scraper = BDSupremeCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        # Quick connectivity test
        try:
            resp = scraper.session.get(
                BASE_URL + "/web/?page=judgments.php&menu=00&div_id=1",
                timeout=30,
            )
            resp.raise_for_status()
            print(f"Connection OK - status {resp.status_code}, {len(resp.text)} bytes")
        except Exception as e:
            print(f"Connection FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )

    elif command == "update":
        from datetime import timedelta

        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.bootstrap()
        print(f"\nUpdate complete: {stats}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
