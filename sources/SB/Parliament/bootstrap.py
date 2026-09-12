#!/usr/bin/env python3
"""
SB/Parliament -- Solomon Islands Parliament Acts

Fetches enacted legislation from the National Parliament of Solomon Islands
(parliament.gov.sb). Acts are organized by year (1981-present) and served
via a JSON API. Documents are PDFs; text is extracted via pdfplumber.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import io
import json
import logging
import hashlib
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SB.Parliament")

BASE_URL = "https://parliament.gov.sb"
ACTS_API = BASE_URL + "/business/acts/get_acts.php"
ACTS_FILES_BASE = BASE_URL + "/business/acts/"

YEAR_RANGE = range(2025, 1980, -1)  # Newest first — older PDFs are often scanned images

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/)",
    "Accept": "application/json, text/html, */*",
    "Referer": BASE_URL + "/business/acts/",
}


class SBParliamentScraper(BaseScraper):
    """Scraper for SB/Parliament - Solomon Islands Parliament Acts."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            self.session = requests.Session()
            self.session.headers.update(_HEADERS)

            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        return self.session

    def _fetch_acts_for_year(self, year: int) -> list:
        """Fetch the JSON list of acts for a given year."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(f"{ACTS_API}?year={year}", timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):
                return data
            return []
        except Exception as e:
            logger.warning(f"Failed to fetch acts for year {year}: {e}")
            return []

    def _fetch_pdf_bytes(self, url: str) -> Optional[bytes]:
        """Download a PDF. Returns bytes or None on error."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        import pdfplumber

        text_parts = []
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning(f"PDF extraction error: {e}")
            return ""
        return "\n\n".join(text_parts)

    def _build_pdf_url(self, relative_url: str) -> str:
        """Build full PDF URL from the relative path in JSON response."""
        # The JSON returns paths like "files/2023/Education Act 2023.pdf"
        # with escaped slashes. Clean them up.
        clean = relative_url.replace("\\/", "/")
        # URL-encode the filename portion (spaces, parens, etc.)
        parts = clean.split("/")
        encoded_parts = []
        for part in parts:
            encoded_parts.append(quote(part, safe=""))
        encoded_path = "/".join(encoded_parts)
        return ACTS_FILES_BASE + encoded_path

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all acts across all years."""
        for year in YEAR_RANGE:
            acts = self._fetch_acts_for_year(year)
            if not acts:
                continue
            logger.info(f"Year {year}: {len(acts)} acts found")
            for act in acts:
                name = act.get("name", "")
                relative_url = act.get("url", "")
                if not relative_url:
                    continue
                pdf_url = self._build_pdf_url(relative_url)
                pdf_bytes = self._fetch_pdf_bytes(pdf_url)
                if pdf_bytes:
                    yield {
                        "name": name,
                        "filename": act.get("filename", ""),
                        "year": year,
                        "pdf_url": pdf_url,
                        "pdf_bytes": pdf_bytes,
                    }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental updates - fetch only the current and previous year."""
        current_year = datetime.now().year
        for year in [current_year - 1, current_year]:
            acts = self._fetch_acts_for_year(year)
            if not acts:
                continue
            logger.info(f"Year {year}: {len(acts)} acts found")
            for act in acts:
                name = act.get("name", "")
                relative_url = act.get("url", "")
                if not relative_url:
                    continue
                pdf_url = self._build_pdf_url(relative_url)
                pdf_bytes = self._fetch_pdf_bytes(pdf_url)
                if pdf_bytes:
                    yield {
                        "name": name,
                        "filename": act.get("filename", ""),
                        "year": year,
                        "pdf_url": pdf_url,
                        "pdf_bytes": pdf_bytes,
                    }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw PDF data into standardized record."""
        pdf_bytes = raw.get("pdf_bytes")
        if not pdf_bytes:
            return None

        text = self._extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text ({len(text)} chars) from {raw['pdf_url']}")
            return None

        name = raw.get("name", "")
        year = raw.get("year", 0)

        # Generate stable ID from URL
        url_hash = hashlib.md5(raw["pdf_url"].encode()).hexdigest()[:12]
        doc_id = f"SB-parliament-{year}-{url_hash}"

        # Try to extract act number from name
        act_number = None
        m = re.search(r'\(No\.\s*(\d+)\s+of\s+(\d{4})\)', name)
        if m:
            act_number = f"No. {m.group(1)} of {m.group(2)}"

        return {
            "_id": doc_id,
            "_source": "SB/Parliament",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": name,
            "text": text,
            "date": f"{year}-01-01" if year else None,
            "url": raw["pdf_url"],
            "year": year,
            "act_number": act_number,
        }


# ── CLI ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = SBParliamentScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        import requests
        try:
            resp = requests.get(
                f"{ACTS_API}?year=2023",
                headers=_HEADERS,
                timeout=15,
            )
            print(f"API response: HTTP {resp.status_code}")
            data = resp.json()
            print(f"Acts for 2023: {len(data)} records")
            if data:
                print(f"First act: {data[0].get('name', 'N/A')}")
        except Exception as e:
            print(f"Connection FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(f"\nBootstrap complete:")
        print(f"  Records fetched: {stats['records_fetched']}")
        if sample_mode:
            print(f"  Sample records saved: {stats.get('sample_records_saved', 0)}")
        else:
            print(f"  New: {stats['records_new']}")
            print(f"  Updated: {stats['records_updated']}")
            print(f"  Skipped: {stats['records_skipped']}")
        print(f"  Errors: {stats['errors']}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
