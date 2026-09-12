#!/usr/bin/env python3
"""
HN/TSC-Biblioteca -- Honduras TSC Virtual Library

Fetches legislation from the Tribunal Superior de Cuentas Biblioteca Virtual
(tsc.gob.hn/biblioteca). Laws are listed across paginated HTML pages with
direct PDF download links. Text is extracted via pdfplumber.

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
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HN.TSC-Biblioteca")

BASE_URL = "https://www.tsc.gob.hn"
LIST_URL = BASE_URL + "/biblioteca/index.php/leyes"
ITEMS_PER_PAGE = 12
TOTAL_PAGES = 25

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/)",
    "Accept": "text/html, */*",
    "Accept-Language": "es,en;q=0.5",
}


class HNTSCBibliotecaScraper(BaseScraper):
    """Scraper for HN/TSC-Biblioteca - Honduras TSC Virtual Library."""

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

    def _fetch_list_page(self, start: int) -> str:
        """Fetch a paginated list page. Returns HTML string."""
        self.rate_limiter.wait()
        sess = self._get_session()
        url = LIST_URL if start == 0 else f"{LIST_URL}?start={start}"
        try:
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch list page start={start}: {e}")
            return ""

    def _parse_list_page(self, html: str) -> list:
        """Parse a list page and extract law entries using regex.

        Returns list of dicts with keys: title, detail_url, pdf_url, decreto
        """
        entries = []

        # Find all h2 title links: <h2...><a href="/biblioteca/index.php/leyes/NNN-slug">Title</a></h2>
        title_pattern = re.compile(
            r'<h2[^>]*>\s*<a\s+href="(/biblioteca/index\.php/leyes/\d+-[^"]+)"[^>]*>([^<]+)</a>',
            re.IGNORECASE
        )

        titles = list(title_pattern.finditer(html))
        if not titles:
            return entries

        for i, match in enumerate(titles):
            detail_path = match.group(1)
            title = match.group(2).strip()
            detail_url = BASE_URL + detail_path

            # Get the HTML chunk between this title and the next (or end)
            start_pos = match.end()
            end_pos = titles[i + 1].start() if i + 1 < len(titles) else len(html)
            chunk = html[start_pos:end_pos]

            # Find first PDF link in this chunk
            pdf_match = re.search(r'href="([^"]*\.pdf)"', chunk, re.IGNORECASE)
            pdf_url = None
            if pdf_match:
                href = pdf_match.group(1)
                pdf_url = urljoin(BASE_URL + "/", href)

            # Find decreto number
            decreto = ""
            decreto_match = re.search(r'((?:Decreto|PCM)\s+No\.?\s*[\w\-]+)', chunk, re.IGNORECASE)
            if decreto_match:
                decreto = decreto_match.group(1).strip()

            entries.append({
                "title": title,
                "detail_url": detail_url,
                "pdf_url": pdf_url,
                "decreto": decreto,
            })

        return entries

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

    def _parse_detail_page(self, html: str) -> Optional[str]:
        """Parse a detail page to find PDF download URL if not found on list."""
        pdf_pattern = re.compile(r'href=["\']([^"\']*\.pdf)["\']', re.IGNORECASE)
        match = pdf_pattern.search(html)
        if match:
            href = match.group(1).replace("/../", "/")
            return urljoin(BASE_URL + "/biblioteca/", href)
        return None

    def _fetch_detail_for_pdf(self, detail_url: str) -> Optional[str]:
        """Visit a detail page to find the PDF URL."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(detail_url, timeout=30)
            resp.raise_for_status()
            return self._parse_detail_page(resp.text)
        except Exception as e:
            logger.warning(f"Failed to fetch detail page {detail_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all law entries across all paginated pages."""
        seen_titles = set()
        for page_num in range(TOTAL_PAGES):
            start = page_num * ITEMS_PER_PAGE
            html = self._fetch_list_page(start)
            if not html:
                continue

            entries = self._parse_list_page(html)
            if not entries:
                logger.info(f"No entries found on page {page_num + 1}, stopping.")
                break

            logger.info(f"Page {page_num + 1}/{TOTAL_PAGES}: {len(entries)} entries")

            for entry in entries:
                title = entry.get("title", "")
                if title in seen_titles:
                    continue
                seen_titles.add(title)

                pdf_url = entry.get("pdf_url")
                if not pdf_url:
                    detail_url = entry.get("detail_url")
                    if detail_url:
                        pdf_url = self._fetch_detail_for_pdf(detail_url)
                    if not pdf_url:
                        logger.warning(f"No PDF URL for: {title}")
                        continue

                pdf_bytes = self._fetch_pdf_bytes(pdf_url)
                if pdf_bytes:
                    yield {
                        "title": title,
                        "decreto": entry.get("decreto", ""),
                        "detail_url": entry.get("detail_url", ""),
                        "pdf_url": pdf_url,
                        "pdf_bytes": pdf_bytes,
                    }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental updates - fetch first 3 pages (most recent)."""
        for page_num in range(3):
            start = page_num * ITEMS_PER_PAGE
            html = self._fetch_list_page(start)
            if not html:
                continue

            entries = self._parse_list_page(html)
            if not entries:
                break

            logger.info(f"Update page {page_num + 1}: {len(entries)} entries")
            for entry in entries:
                pdf_url = entry.get("pdf_url")
                if not pdf_url:
                    detail_url = entry.get("detail_url")
                    if detail_url:
                        pdf_url = self._fetch_detail_for_pdf(detail_url)
                    if not pdf_url:
                        continue

                pdf_bytes = self._fetch_pdf_bytes(pdf_url)
                if pdf_bytes:
                    yield {
                        "title": entry.get("title", ""),
                        "decreto": entry.get("decreto", ""),
                        "detail_url": entry.get("detail_url", ""),
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
            logger.warning(f"Insufficient text ({len(text)} chars) from {raw.get('pdf_url', '?')}")
            return None

        title = raw.get("title", "")
        decreto = raw.get("decreto", "")
        pdf_url = raw.get("pdf_url", "")

        # Generate stable ID from PDF URL
        url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
        doc_id = f"HN-TSC-{url_hash}"

        # Try to find a precise date in the text header (La Gaceta date)
        date_str = None
        gaceta_match = re.search(
            r'(\d{1,2})\s+DE\s+(ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE)\s+(?:DEL?\s+)?(\d{4})',
            text[:500],
            re.IGNORECASE
        )
        if gaceta_match:
            day = int(gaceta_match.group(1))
            month_name = gaceta_match.group(2).upper()
            year = int(gaceta_match.group(3))
            months = {
                "ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4,
                "MAYO": 5, "JUNIO": 6, "JULIO": 7, "AGOSTO": 8,
                "SEPTIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12,
            }
            month = months.get(month_name, 1)
            date_str = f"{year}-{month:02d}-{day:02d}"

        # Fall back to year from decreto (e.g., "Decreto No. 45-2026")
        if not date_str:
            year_match = re.search(r'(\d{4})', decreto)
            if year_match:
                year = int(year_match.group(1))
                if 1900 <= year <= 2030:
                    date_str = f"{year}-01-01"

        return {
            "_id": doc_id,
            "_source": "HN/TSC-Biblioteca",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": raw.get("detail_url") or pdf_url,
            "decreto": decreto,
            "pdf_url": pdf_url,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = HNTSCBibliotecaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        import requests
        try:
            resp = requests.get(LIST_URL, headers=_HEADERS, timeout=15)
            print(f"HTTP {resp.status_code}")
            print(f"Page size: {len(resp.text)} chars")
            entries = scraper._parse_list_page(resp.text)
            print(f"Entries on first page: {len(entries)}")
            if entries:
                print(f"First entry: {entries[0].get('title', 'N/A')}")
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
