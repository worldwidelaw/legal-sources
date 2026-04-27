#!/usr/bin/env python3
"""
MU/SupremeCourt -- Mauritius Supreme Court Judgments

Fetches judgments from the official Judiciary of Mauritius website.
The site is a Drupal platform with a table-based judgment search view.
~11,000+ judgments with full text available as PDF.

Strategy:
  - Paginate through /judgment-search?page=N (10 results per page, ~1100 pages)
  - Parse HTML table rows to extract: title, document number, date, judge, PDF URL
  - Download PDF via /downloadPDF/judgment/{document_id}
  - Extract text from PDF using common/pdf_extract

HTML structure (each row in table):
  - Title cell: <span class="document-title">..title..</span>
                <span class="delivered-by">..judge, court..</span>
  - Doc number cell: <li>2026 INT 85</li>
  - Date cell: DD/MM/YYYY
  - Download cell: href='/downloadPDF/judgment/{doc_id}'

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MU.SupremeCourt")

BASE_URL = "https://supremecourt.govmu.org"
SEARCH_PATH = "/judgment-search"


def _clean_html(text: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&#?\w+;", "", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Convert DD/MM/YYYY to ISO 8601 date string."""
    date_str = date_str.strip()
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
    if m:
        day, month, year = m.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    return None


def _parse_records_from_page(html: str) -> List[Dict[str, Any]]:
    """Parse judgment records from the search results table HTML."""
    records = []

    # Each record is a table row with 4 cells:
    # 1) title (with document-title + delivered-by spans)
    # 2) document number (with <li> items)
    # 3) delivered date
    # 4) download link

    # Split by table rows - each data row starts with views-field-title
    row_pattern = re.compile(
        r'<td[^>]*class="views-field views-field-title"[^>]*>(.*?)</td>'
        r'.*?'
        r'<td[^>]*class="[^"]*views-field-field-document-number-hidden"[^>]*>(.*?)</td>'
        r'.*?'
        r'<td[^>]*class="[^"]*views-field-field-delivered-on[^"]*"[^>]*>(.*?)</td>'
        r'.*?'
        r'<td[^>]*class="views-field views-field-nothing-1"[^>]*>(.*?)</td>',
        re.DOTALL,
    )

    for match in row_pattern.finditer(html):
        title_cell, docnum_cell, date_cell, download_cell = match.groups()
        record = {}

        # Extract title
        title_m = re.search(r'class="document-title">(.*?)</span>', title_cell, re.DOTALL)
        if title_m:
            record["title"] = _clean_html(title_m.group(1))

        # Extract judge / court
        judge_m = re.search(r'class="delivered-by">(.*?)</span>', title_cell, re.DOTALL)
        if judge_m:
            record["delivered_by"] = _clean_html(judge_m.group(1))

        # Extract document ID from the view_document link
        doc_id_m = re.search(r'href="/view_document/(\d+)/(\d+)', title_cell)
        if doc_id_m:
            record["document_id"] = doc_id_m.group(1)
            record["node_id"] = doc_id_m.group(2)

        # Extract document number (e.g., "2026 INT 85")
        docnum_m = re.search(r"<li>(.*?)</li>", docnum_cell)
        if docnum_m:
            record["document_number"] = _clean_html(docnum_m.group(1))

        # Extract delivery date
        date_text = _clean_html(date_cell)
        # The date is typically the first DD/MM/YYYY in the cell
        date_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", date_text)
        if date_match:
            record["date_raw"] = date_match.group(0)

        # Extract download URL
        dl_m = re.search(r"href='(/downloadPDF/judgment/\d+)'", download_cell)
        if dl_m:
            record["download_path"] = dl_m.group(1)

        # Must have at minimum document_id and download_path
        if record.get("document_id") and record.get("download_path"):
            records.append(record)

    return records


def _detect_last_page(html: str) -> int:
    """Detect last page number from pagination HTML."""
    # Look for the "last page" link: pager__item--last ... page=N
    # Note: & appears as &amp; in HTML
    last_m = re.search(
        r'pager__item--last.*?(?:[?&]|&amp;)page=(\d+)', html, re.DOTALL
    )
    if last_m:
        return int(last_m.group(1))
    # Fallback: find highest page= value in pager links
    pages = re.findall(r'class="pager__item[^"]*".*?(?:[?&]|&amp;)page=(\d+)', html, re.DOTALL)
    if pages:
        return max(int(p) for p in pages)
    return 0


class MauritiusSupremeCourtScraper(BaseScraper):
    """Scraper for MU/SupremeCourt -- Mauritius Judiciary Judgments."""

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
        return (
            extract_pdf_markdown(
                source="MU/SupremeCourt",
                source_id="",
                pdf_bytes=pdf_bytes,
                table="case_law",
            )
            or ""
        )

    def _fetch_page(self, page: int) -> tuple:
        """Fetch and parse a single search results page. Returns (records, html)."""
        url = f"{SEARCH_PATH}?page={page}"
        logger.info(f"  Fetching page {page}...")
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Failed to fetch page {page}: {e}")
            return [], ""

        records = _parse_records_from_page(resp.text)
        logger.info(f"  Page {page}: {len(records)} records found")
        return records, resp.text

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_num = raw.get("document_number", "")
        doc_id = raw.get("document_id", "")
        safe_id = re.sub(r"[^\w\-.]", "_", doc_num) if doc_num else doc_id

        title = raw.get("title") or doc_num or f"Judgment {doc_id}"

        # Parse date
        date_iso = _parse_date(raw.get("date_raw", ""))

        # Parse judge and court from delivered_by
        delivered_by = raw.get("delivered_by", "")
        judge = ""
        court = ""
        if delivered_by:
            # Pattern: "Mrs Y Nathire Beebeejaun, Magistrate Intermediate Court"
            parts = delivered_by.split(",", 1)
            judge = parts[0].strip()
            if len(parts) > 1:
                court = parts[1].strip()

        return {
            "_id": f"MU/SupremeCourt/{safe_id}",
            "_source": "MU/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": date_iso,
            "url": f"{BASE_URL}{raw.get('download_path', '')}",
            "document_number": doc_num,
            "judge": judge,
            "court": court,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        # Fetch first page to detect total pages
        logger.info("Fetching first page to detect pagination...")
        records, html = self._fetch_page(0)
        if not records:
            logger.error("No records found on first page")
            return

        last_page = _detect_last_page(html)
        logger.info(f"Detected {last_page + 1} pages (0..{last_page}). First page: {len(records)} records.")

        count = 0
        errors = 0

        # Process all pages starting from 0
        all_pages = [(0, records)] + [(p, None) for p in range(1, last_page + 1)]

        for page_num, page_records in all_pages:
            if page_records is None:
                page_records, _ = self._fetch_page(page_num)

            for record in page_records:
                download_path = record.get("download_path", "")
                doc_num = record.get("document_number", "?")

                logger.info(f"  [{count + errors + 1}] Downloading PDF: {doc_num}")

                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(download_path)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"  Failed to download PDF for {doc_num}: {e}")
                    errors += 1
                    continue

                if not resp.content or resp.content[:5] != b"%PDF-":
                    logger.warning(f"  Not a PDF: {doc_num}")
                    errors += 1
                    continue

                text = self._extract_pdf_text(resp.content)
                if not text or len(text.strip()) < 50:
                    logger.warning(
                        f"  Insufficient text from {doc_num}: {len(text) if text else 0} chars"
                    )
                    errors += 1
                    continue

                record["text"] = text
                yield record
                count += 1

            logger.info(f"Page {page_num} done. Total: {count} records, {errors} errors")

        logger.info(f"Fetched {count} judgments ({errors} errors)")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = MauritiusSupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing search page...")
        records, html = scraper._fetch_page(0)
        if not records:
            logger.error("FAILED — no records found on page 0")
            sys.exit(1)
        logger.info(f"OK — {len(records)} records on page 0")

        last_page = _detect_last_page(html)
        logger.info(f"Last page: {last_page} ({(last_page + 1) * 10} est. records)")

        logger.info("Testing PDF download...")
        first = records[0]
        import requests

        resp = requests.get(
            f"{BASE_URL}{first['download_path']}",
            timeout=60,
            headers={"User-Agent": "LegalDataHunter/1.0"},
        )
        if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
            text = scraper._extract_pdf_text(resp.content)
            logger.info(f"OK — PDF download works, {len(text)} chars extracted")
        else:
            logger.error(f"FAILED — status {resp.status_code}")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
