#!/usr/bin/env python3
"""
CO/SIC -- Colombia Superintendencia de Industria y Comercio

Fetches competition and consumer protection decisions from the SIC website.

Strategy:
  - Paginate the Drupal Views Ajax endpoint for decision listings
  - Parse PDF URLs from the HTML responses
  - Download PDFs and extract full text using PyMuPDF (fitz)
  - Skip scanned PDFs (older documents with no text layer)

Sections:
  - page_1: Delegatura decisions (competition enforcement)
  - page_2: Superintendente decisions (appeals, sanctions)

Data: ~2,500+ decision PDFs, with extractable text from ~2016 onwards.
License: Open data (public administrative decisions).
Rate limit: 1.5 req/sec (self-imposed).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py update --since DATE  # Fetch decisions after DATE
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.SIC")

BASE_URL = "https://www.sic.gov.co"
VIEWS_URL = f"{BASE_URL}/views/ajax"

# Sections to crawl: (view_name, display_id, label, max_pages)
SECTIONS = [
    ("estados", "page_1", "Delegatura", 40),
    ("estados", "page_2", "Superintendente", 200),
]

MIN_TEXT_LENGTH = 200  # Skip PDFs with less than this many chars of extracted text


class SICScraper(BaseScraper):
    """
    Scraper for CO/SIC -- Colombia Competition & Consumer Protection Authority.
    Country: CO
    URL: https://www.sic.gov.co

    Data types: case_law
    Auth: none (public data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, text/html, */*",
            },
            timeout=60,
        )

    # -- Drupal Views pagination ----------------------------------------------

    def _fetch_view_page(self, view_name: str, display_id: str, page: int) -> Optional[str]:
        """Fetch a page from the Drupal Views Ajax endpoint, return HTML content."""
        try:
            resp = self.client.post(
                VIEWS_URL,
                data={
                    "view_name": view_name,
                    "view_display_id": display_id,
                    "page": page,
                },
                headers={"Accept": "application/json"},
                timeout=30,
            )
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            for item in data:
                if isinstance(item, dict) and "data" in item:
                    return item["data"]
            return None
        except Exception as e:
            logger.debug(f"Views fetch failed for {view_name}/{display_id} page {page}: {e}")
            return None

    def _extract_pdf_entries(self, html: str) -> List[dict]:
        """Extract PDF URLs and metadata from Drupal Views HTML."""
        entries = []
        seen_urls = set()
        pdf_urls = re.findall(r'href="([^"]+\.pdf)"', html)

        for pdf_url in pdf_urls:
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)
            if not pdf_url.startswith("http"):
                pdf_url = BASE_URL + pdf_url

            # Extract resolution info from filename
            filename = unquote(pdf_url.split("/")[-1])
            # Try to extract resolution number and date from filename
            res_match = re.search(
                r'(?:RESOLUCI[OÓ]N|RE|Resolucion)[_\s]*(\d+)',
                filename, re.IGNORECASE
            )
            resolution_number = res_match.group(1) if res_match else None

            date_match = re.search(
                r'(\d{1,2})[_\-\s]+(?:DE\s+)?(\d{1,2})[_\-\s]+(?:DE\s+)?(\d{4})',
                filename, re.IGNORECASE
            )
            date_str = None
            if date_match:
                day, month, year = date_match.group(1), date_match.group(2), date_match.group(3)
                try:
                    dt = datetime(int(year), int(month), int(day))
                    date_str = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            # Also try YYYY pattern for year
            if not date_str:
                year_match = re.search(r'(?:DE\s+)?(\d{4})', filename)
                if year_match:
                    year = int(year_match.group(1))
                    if 2000 <= year <= 2030:
                        date_str = f"{year}-01-01"

            entries.append({
                "pdf_url": pdf_url,
                "filename": filename,
                "resolution_number": resolution_number,
                "date_from_filename": date_str,
            })

        return entries

    # -- PDF text extraction --------------------------------------------------

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="CO/SIC",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    # -- Parse date from PDF text ---------------------------------------------

    @staticmethod
    def _parse_date_from_text(text: str) -> Optional[str]:
        """Try to extract the resolution date from the document text."""
        months_es = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12',
        }
        # Pattern: (DD de MES de YYYY) or (DD MES YYYY)
        for m_name, m_num in months_es.items():
            pattern = rf'(\d{{1,2}})\s+(?:de\s+)?{m_name}\s+(?:de\s+)?(\d{{4}})'
            match = re.search(pattern, text[:2000], re.IGNORECASE)
            if match:
                day = int(match.group(1))
                year = int(match.group(2))
                if 2000 <= year <= 2030 and 1 <= day <= 31:
                    try:
                        datetime(year, int(m_num), day)
                        return f"{year}-{m_num}-{day:02d}"
                    except ValueError:
                        pass
        return None

    # -- Build record ---------------------------------------------------------

    def _build_record(self, entry: dict, section_label: str) -> Optional[dict]:
        """Download PDF, extract text, and build a raw record."""
        pdf_url = entry["pdf_url"]

        self.rate_limiter.wait()
        text = self._extract_text_from_pdf(pdf_url)
        if not text:
            return None

        # Try to get date from text, fall back to filename
        date = self._parse_date_from_text(text) or entry.get("date_from_filename")

        # Try to get resolution number from text if not in filename
        resolution_number = entry.get("resolution_number")
        if not resolution_number:
            res_match = re.search(
                r'RESOLUCI[OÓ]N\s+(?:N[UÚ]MERO\s+)?(\d+)',
                text[:1000], re.IGNORECASE
            )
            if res_match:
                resolution_number = res_match.group(1)

        # Build title from filename or resolution number
        filename = entry.get("filename", "")
        title = filename.replace(".pdf", "").replace("_", " ").strip()
        if not title and resolution_number:
            title = f"Resolución {resolution_number}"

        return {
            "pdf_url": pdf_url,
            "filename": filename,
            "title": title,
            "text": text,
            "date": date,
            "resolution_number": resolution_number,
            "section": section_label,
        }

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all SIC decisions by crawling Drupal Views pages."""
        total_found = 0
        total_skipped = 0

        for view_name, display_id, label, max_pages in SECTIONS:
            logger.info(f"Crawling section: {label} ({view_name}/{display_id})")
            consecutive_empty = 0

            for page_num in range(max_pages):
                self.rate_limiter.wait()
                html = self._fetch_view_page(view_name, display_id, page_num)

                if not html:
                    consecutive_empty += 1
                    if consecutive_empty > 3:
                        logger.info(f"No more pages in {label}, stopping at page {page_num}")
                        break
                    continue

                consecutive_empty = 0
                entries = self._extract_pdf_entries(html)

                if not entries:
                    logger.info(f"No PDFs on page {page_num} of {label}, stopping")
                    break

                for entry in entries:
                    record = self._build_record(entry, label)
                    if record:
                        total_found += 1
                        yield record
                    else:
                        total_skipped += 1

                if page_num % 10 == 0:
                    logger.info(
                        f"{label} page {page_num}: {total_found} found, "
                        f"{total_skipped} skipped (scanned/no text)"
                    )

        logger.info(f"Crawl complete: {total_found} records, {total_skipped} skipped")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions (first pages of each section)."""
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_str}")
        found = 0

        for view_name, display_id, label, _ in SECTIONS:
            # Only check first 10 pages for updates
            for page_num in range(10):
                self.rate_limiter.wait()
                html = self._fetch_view_page(view_name, display_id, page_num)
                if not html:
                    break

                entries = self._extract_pdf_entries(html)
                if not entries:
                    break

                page_has_old = False
                for entry in entries:
                    record = self._build_record(entry, label)
                    if record:
                        if record.get("date") and record["date"] < since_str:
                            page_has_old = True
                            continue
                        found += 1
                        yield record

                if page_has_old:
                    break

        logger.info(f"Update complete: {found} new records")

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample records from the most recent pages."""
        found = 0

        for view_name, display_id, label, _ in SECTIONS:
            if found >= count:
                break

            for page_num in range(5):  # Check first 5 pages
                if found >= count:
                    break

                self.rate_limiter.wait()
                html = self._fetch_view_page(view_name, display_id, page_num)
                if not html:
                    continue

                entries = self._extract_pdf_entries(html)
                for entry in entries:
                    if found >= count:
                        break
                    record = self._build_record(entry, label)
                    if record:
                        found += 1
                        logger.info(
                            f"Sample {found}/{count}: {record['title'][:60]} "
                            f"({record.get('date', 'no date')})"
                        )
                        yield record

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to standard schema."""
        title = raw.get("title") or raw.get("filename", "").replace(".pdf", "")
        title = re.sub(r"\s+", " ", title).strip()

        return {
            "_id": f"CO-SIC-{raw.get('resolution_number') or hash(raw['pdf_url']) % 10**9}",
            "_source": "CO/SIC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "resolution_number": raw.get("resolution_number"),
            "section": raw.get("section"),
        }

    def test_api(self) -> bool:
        """Test connectivity to SIC Drupal Views endpoint."""
        logger.info("Testing SIC Drupal Views Ajax endpoint...")

        html = self._fetch_view_page("estados", "page_1", 0)
        if not html:
            logger.error("Views endpoint failed")
            return False

        entries = self._extract_pdf_entries(html)
        logger.info(f"Views OK: {len(entries)} PDF entries on first page")

        if entries:
            # Test PDF download and text extraction
            text = self._extract_text_from_pdf(entries[0]["pdf_url"])
            if text:
                logger.info(f"PDF extraction OK: {len(text)} chars")
            else:
                logger.warning("PDF text extraction returned empty (may be scanned)")

        return True


if __name__ == "__main__":
    scraper = SICScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N] [--since DATE]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == "--since" and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]

        if not since_str:
            print("Usage: python bootstrap.py update --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        gen = scraper.fetch_updates(since)

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
