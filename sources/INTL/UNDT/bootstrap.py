#!/usr/bin/env python3
"""
INTL/UNDT -- UN Dispute Tribunal Judgments

Fetches judgments from the United Nations Dispute Tribunal (UNDT).

Strategy:
  - Scrape yearly index pages (judgments_YYYY.shtml) for PDF links
  - Filter for UNDT judgment PDFs (pattern: undt-YYYY-NNN.pdf)
  - Download each PDF and extract full text via common/pdf_extract

Data Coverage:
  - ~2769 judgments from 2009 to present
  - First-instance UN staff employment disputes
  - English and French

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.UNDT")

BASE_URL = "https://www.un.org"
INDEX_URL = f"{BASE_URL}/en/internaljustice/undt/judgments-orders.shtml"
YEAR_URL_TEMPLATE = f"{BASE_URL}/en/internaljustice/undt/judgments_{{year}}.shtml"
MAX_PDF_BYTES = 50 * 1024 * 1024
START_YEAR = 2009


class UNDTScraper(BaseScraper):
    """Scraper for UN Dispute Tribunal judgments."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en",
        })

    def _get_year_range(self) -> list[int]:
        """Get available years from the index page."""
        try:
            resp = self.session.get(INDEX_URL, timeout=30)
            resp.raise_for_status()
            years = re.findall(r'href="judgments_(\d{4})\.shtml"', resp.text)
            return sorted(set(int(y) for y in years))
        except Exception as e:
            logger.warning(f"Could not fetch index page: {e}")
            current_year = datetime.now().year
            return list(range(START_YEAR, current_year + 1))

    def _get_judgment_pdfs(self, year: int) -> list[dict]:
        """Get all UNDT judgment PDF links for a given year."""
        url = YEAR_URL_TEMPLATE.format(year=year)
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Could not fetch year {year}: {e}")
            return []

        # Find UNDT judgment PDFs (exclude French versions -FR.pdf and orders)
        pdf_pattern = r'href="([^"]*undt-\d{4}-\d+\.pdf)"'
        matches = re.findall(pdf_pattern, resp.text, re.IGNORECASE)

        entries = []
        seen = set()
        for pdf_path in matches:
            # Skip French translations
            if pdf_path.upper().endswith("-FR.PDF"):
                continue

            full_url = urljoin(url, pdf_path)
            if full_url in seen:
                continue
            seen.add(full_url)

            # Extract judgment number from filename
            num_match = re.search(r'undt-(\d{4})-(\d+)', pdf_path, re.IGNORECASE)
            if num_match:
                j_year = num_match.group(1)
                j_num = num_match.group(2)
                judgment_id = f"UNDT/{j_year}/{j_num}"
            else:
                judgment_id = pdf_path.split("/")[-1].replace(".pdf", "")

            entries.append({
                "judgment_id": judgment_id,
                "year": year,
                "pdf_url": full_url,
                "title": f"UNDT Judgment {judgment_id}",
            })

        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF."""
        try:
            time.sleep(1)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            if len(resp.content) < 500:
                logger.warning(f"  PDF too small ({len(resp.content)} bytes), likely error")
                return None
            return resp.content
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None

    def _extract_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        """Extract text from PDF bytes."""
        text = extract_pdf_markdown(
            source="INTL/UNDT",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if text and len(text.strip()) >= 100:
            return text

        import io
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p)
                if text and len(text.strip()) >= 100:
                    return text
        except Exception:
            pass
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = [p.extract_text() or "" for p in reader.pages]
            text = "\n\n".join(p for p in pages if p)
            if text and len(text.strip()) >= 100:
                return text
        except Exception:
            pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments with full text from PDFs."""
        years = self._get_year_range()
        logger.info(f"Processing years: {years[0]}-{years[-1]}")

        all_entries = []
        for year in years:
            entries = self._get_judgment_pdfs(year)
            logger.info(f"Year {year}: {len(entries)} UNDT judgments")
            all_entries.extend(entries)
            time.sleep(0.5)

        logger.info(f"Total entries to process: {len(all_entries)}")

        for i, entry in enumerate(all_entries):
            try:
                logger.info(
                    f"[{i+1}/{len(all_entries)}] Downloading {entry['judgment_id']} ..."
                )
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue

                text = self._extract_text(pdf_bytes, entry["judgment_id"])
                if not text:
                    logger.warning(f"  Insufficient text for {entry['judgment_id']}, skipping")
                    continue

                entry["_extracted_text"] = text
                yield entry

            except Exception as e:
                logger.error(f"  Error processing {entry['judgment_id']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield judgments from recent years."""
        since_year = since.year
        years = [y for y in self._get_year_range() if y >= since_year]
        all_entries = []
        for year in years:
            entries = self._get_judgment_pdfs(year)
            all_entries.extend(entries)
            time.sleep(0.5)

        for entry in all_entries:
            pdf_bytes = self._download_pdf(entry["pdf_url"])
            if not pdf_bytes:
                continue
            text = self._extract_text(pdf_bytes, entry["judgment_id"])
            if not text:
                continue
            entry["_extracted_text"] = text
            yield entry

    def normalize(self, raw: dict) -> dict:
        """Transform raw judgment data into standard schema."""
        judgment_id = raw.get("judgment_id", "")
        uid_slug = judgment_id.lower().replace("/", "-").replace(" ", "-")

        return {
            "_id": f"undt-{uid_slug}",
            "_source": "INTL/UNDT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": None,  # Date is in PDF content, not metadata
            "url": raw.get("pdf_url", ""),
            "judgment_number": judgment_id,
            "year": raw.get("year"),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = UNDTScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        years = scraper._get_year_range()
        total = 0
        for year in years:
            entries = scraper._get_judgment_pdfs(year)
            total += len(entries)
            print(f"Year {year}: {len(entries)} judgments")
            time.sleep(0.3)
        print(f"Total: {total}")
        sys.exit(0)

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
