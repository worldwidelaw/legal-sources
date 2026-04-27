#!/usr/bin/env python3
"""
KY/Judgments -- Cayman Islands Court Judgments (judicial.ky)

Fetches unreported judgments from the official Cayman Islands Judicial website.
The site uses a WordPress Participants Database (PDB) plugin with ~430 pages
of records, each containing ~10 judgments with PDF download links.

Strategy:
  - Paginate through /judgments/unreported-judgments-advanced-search/?listpage=N&instance=1
  - Parse HTML to extract: download URL, neutral citation, cause number, date, parties, court, judge
  - Download PDF from judicial.ky/n0c-storage/judgments-repository3/
  - Extract text from PDF using pdfplumber/pypdf

HTML record structure (each record in the PDB list):
  Fields: download (PDF link), neutral_citation, cause_number, judgment_date,
          parties, court, judge

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
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
logger = logging.getLogger("legal-data-hunter.KY.Judgments")

BASE_URL = "https://judicial.ky"
LISTING_PATH = "/judgments/unreported-judgments-advanced-search/"
TOTAL_PAGES = 430


def _clean_html(text: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&#?\w+;', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _parse_records_from_page(html: str) -> List[Dict[str, Any]]:
    """Parse judgment records from a single list page HTML."""
    records = []

    # Split by download-field sections (each record starts with one)
    parts = html.split('download-field')
    if len(parts) < 2:
        return records

    for part in parts[1:]:
        record = {}

        # Extract download URL
        dl_match = re.search(r'href="([^"]+)"', part)
        if not dl_match:
            continue
        record['download_url'] = dl_match.group(1)

        # Extract fields: class="pdb-field FIELDNAME-field" ... pdb-field-data>VALUE</span>
        field_data = re.findall(
            r'class="pdb-field\s+(\w[\w_]*)-field".*?pdb-field-data[^>]*>(.*?)</span>',
            part, re.DOTALL
        )

        for field_name, raw_value in field_data:
            clean_value = _clean_html(raw_value)
            if field_name == 'neutral_citation':
                record['neutral_citation'] = clean_value
            elif field_name == 'cause_number':
                record['cause_number'] = clean_value
            elif field_name == 'judgment_date':
                record['judgment_date'] = clean_value
            elif field_name == 'parties':
                record['parties'] = clean_value
            elif field_name == 'court':
                record['court'] = clean_value
            elif field_name == 'judge':
                record['judge'] = clean_value
            elif field_name == 'subject':
                record['subject'] = clean_value

        # Must have at least neutral_citation and download_url
        if record.get('neutral_citation') and record.get('download_url'):
            records.append(record)

    return records


class CaymanJudgmentsScraper(BaseScraper):
    """Scraper for KY/Judgments -- Cayman Islands Court Judgments."""

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
            source="KY/Judgments",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _fetch_page(self, page: int) -> List[Dict[str, Any]]:
        """Fetch and parse a single page of judgment records."""
        url = f"{LISTING_PATH}?listpage={page}&instance=1"
        logger.info(f"  Fetching page {page}...")
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Failed to fetch page {page}: {e}")
            return []

        records = _parse_records_from_page(resp.text)
        logger.info(f"  Page {page}: {len(records)} records found")
        return records

    def _detect_total_pages(self, html: str) -> int:
        """Detect total page count from pagination HTML."""
        pages = re.findall(r'data-page="(\d+)"', html)
        if pages:
            return max(int(p) for p in pages)
        return TOTAL_PAGES

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        neutral_citation = raw.get("neutral_citation", "")
        safe_id = re.sub(r'[^\w\-.]', '_', neutral_citation)

        # Use parties as title, fall back to neutral citation
        title = raw.get("parties") or neutral_citation

        return {
            "_id": f"KY/Judgments/{safe_id}",
            "_source": "KY/Judgments",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("judgment_date"),
            "url": raw.get("download_url", ""),
            "neutral_citation": neutral_citation,
            "cause_number": raw.get("cause_number", ""),
            "court": raw.get("court", ""),
            "judge": raw.get("judge", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        # Fetch first page to detect total pages
        logger.info("Fetching first page to detect pagination...")
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"{LISTING_PATH}?listpage=1&instance=1")
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch first page: {e}")
            return

        total_pages = self._detect_total_pages(resp.text)
        first_records = _parse_records_from_page(resp.text)
        logger.info(f"Detected {total_pages} pages. First page: {len(first_records)} records.")

        count = 0
        errors = 0

        # Process all pages
        all_pages = [(1, first_records)] + [(p, None) for p in range(2, total_pages + 1)]

        for page_num, records in all_pages:
            if records is None:
                records = self._fetch_page(page_num)

            for record in records:
                download_url = record.get('download_url', '')
                citation = record.get('neutral_citation', '?')

                logger.info(f"  [{count + errors + 1}] Downloading PDF: {citation}")

                try:
                    self.rate_limiter.wait()
                    resp = self.client.get(download_url)
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"  Failed to download PDF for {citation}: {e}")
                    errors += 1
                    continue

                if not resp.content or resp.content[:5] != b"%PDF-":
                    logger.warning(f"  Not a PDF: {citation}")
                    errors += 1
                    continue

                text = self._extract_pdf_text(resp.content)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"  Insufficient text from {citation}: {len(text) if text else 0} chars")
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
    scraper = CaymanJudgmentsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing listing page...")
        records = scraper._fetch_page(1)
        if not records:
            logger.error("FAILED — no records found on page 1")
            sys.exit(1)
        logger.info(f"OK — {len(records)} records on page 1")

        logger.info("Testing PDF download...")
        first = records[0]
        import requests
        resp = requests.get(first['download_url'], timeout=60,
                            headers={"User-Agent": "LegalDataHunter/1.0"})
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
