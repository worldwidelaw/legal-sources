#!/usr/bin/env python3
"""
HR/AZTN — Croatian Competition Agency (AZTN) Data Fetcher

Fetches competition and unfair trading practices decisions from AZTN
(Agencija za zaštitu tržišnog natjecanja) via their custom WordPress REST API.

Strategy:
  - Query /wp-json/wp/ea/decision year-by-year (2003–current)
  - Two groups: agency decisions (state_decision + decision) and court decisions
  - Download PDFs from document_file_hr for full text extraction
  - Extract text via pdfplumber

Endpoints:
  - API: https://www.aztn.hr/wp-json/wp/ea/decision
  - PDFs: https://www.aztn.hr/ea/wp-content/uploads/...

Data:
  - ~1,800 agency decisions, ~220 court decisions
  - Language: Croatian (HR)
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("HR/AZTN")

API_URL = "https://www.aztn.hr/wp-json/wp/ea/decision"
YEAR_START = 2003
DELAY = 1.0  # seconds between API requests
PDF_DELAY = 0.5  # seconds between PDF downloads


class CroatianAZTNScraper(BaseScraper):
    """
    Scraper for HR/AZTN — Croatian Competition Agency decisions.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
            "Accept-Language": "hr,en;q=0.5",
        })

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions across all years (newest first)."""
        current_year = datetime.now().year
        years = list(range(current_year, YEAR_START - 1, -1))
        # Agency decisions (state_decision + decision)
        for year in years:
            logger.info(f"Fetching agency decisions for {year}")
            yield from self._fetch_year(year, ["state_decision", "decision"], "agency")
        # Court decisions (2018+)
        for year in years:
            logger.info(f"Fetching court decisions for {year}")
            yield from self._fetch_year(year, ["court_decision"], "court")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since a given date."""
        current_year = datetime.now().year
        since_year = since.year if since else current_year - 1
        for year in range(current_year, since_year - 1, -1):
            yield from self._fetch_year(year, ["state_decision", "decision"], "agency")
            yield from self._fetch_year(year, ["court_decision"], "court")

    def _fetch_year(
        self, year: int, post_types: list, category: str
    ) -> Generator[dict, None, None]:
        """Fetch all decisions for a given year and post type group."""
        params = {
            "page": "1",
            "year": str(year),
            "lastyear": str(year),
            "lng": "hr",
            "lang": "hr",
        }
        for i, pt in enumerate(post_types):
            params[f"decisions[{i}]"] = pt

        try:
            time.sleep(DELAY)
            resp = self.session.get(API_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.error(f"API error for year {year} ({category}): {e}")
            return

        posts = data.get("posts", [])
        if not posts:
            return

        logger.info(f"  {year} ({category}): {len(posts)} decisions")
        for post in posts:
            raw = {**post, "_year": year, "_category": category}
            yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw API data + PDF text into standard schema."""
        case_number = (raw.get("title") or "").strip()
        if not case_number:
            return None

        # Parse date
        date_str = raw.get("decision_date") or ""
        iso_date = self._parse_date(date_str)

        # Build URL — no permalink in API, construct from case number
        pdf_url = raw.get("document_file_hr") or raw.get("document_url") or ""

        # Download and extract PDF text
        text = ""
        if pdf_url:
            text = self._download_pdf_text(pdf_url)

        if not text:
            if pdf_url:
                logger.warning(f"No extractable text in PDF for {case_number}")
            else:
                logger.debug(f"No PDF available for {case_number}")
            return None

        parties = (raw.get("short_text") or "").strip()
        decision_area = (raw.get("decision_area") or "").strip()
        decision_type = (raw.get("decision_type") or "").strip()
        category = raw.get("_category") or "agency"

        # Build a descriptive title
        title_parts = [case_number]
        if parties:
            title_parts.append(parties)
        title = " — ".join(title_parts)

        doc_id = re.sub(r"[^a-zA-Z0-9_/-]", "_", case_number).strip("_")
        source_url = pdf_url if pdf_url else "https://www.aztn.hr/odluke/"

        return {
            "_id": f"HR/AZTN/{doc_id}",
            "_source": "HR/AZTN",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": source_url,
            "case_number": case_number,
            "parties": parties,
            "decision_area": decision_area,
            "decision_type": decision_type,
            "category": category,
            "language": "hr",
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse DD.MM.YYYY to ISO 8601."""
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str.strip(), "%d.%m.%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def _download_pdf_text(self, url: str) -> str:
        """Download PDF and extract text via pdfplumber."""
        try:
            time.sleep(PDF_DELAY)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if b"%PDF" not in resp.content[:20]:
                logger.warning(f"Not a PDF: {url[:80]}")
                return ""
            return self._extract_text_from_pdf(resp.content)
        except Exception as e:
            logger.error(f"PDF download failed: {url[:80]}: {e}")
            return ""

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        try:
            import pdfplumber

            text_parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            text = "\n\n".join(text_parts)
            text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
            text = text.replace("\xa0", " ")
            return text.strip()

        except Exception as e:
            logger.error(f"PDF extraction failed: {e}")
            return ""


def main():
    scraper = CroatianAZTNScraper()

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
        scraper.test_connection()

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
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
