#!/usr/bin/env python3
"""
US/CO-PUC -- Colorado Public Utilities Commission Decisions

Fetches the full text of Decisions issued by the Colorado Public Utilities
Commission (PUC) adjudicating utility proceedings -- rate cases, tariff
filings, certificates (CPCN), complaints and rulemakings across the electric,
gas, water and transportation industries. Each Decision is the Commission's
administrative adjudication of a specific proceeding = case_law (public domain,
US state government edict).

Strategy (official DORA E-Filings "EFI" system, www.dora.state.co.us/pls/efi,
an Oracle PL/SQL web app):
  1. The decision search results endpoint EFI_SEARCH_UI.getDecisionResults is a
     plain GET. Although the interactive search form carries a p_session_id
     hidden field, the results endpoint works with an EMPTY p_session_id: a GET
     with p_after=MM/DD/YYYY & p_before=MM/DD/YYYY (plus empty filter params)
     returns a server-rendered <table> of every decision issued in that window
     -- decision number, a Show_Decision?p_dec={id} link + title, the issued
     date and the proceeding number. No authentication, cookie or token.
  2. The corpus is walked one year at a time (newest first) back to the EFI
     floor year (2000). ~800-900 decisions/year (~20K total).
  3. normalize() fetches the Show_Decision detail page, resolves the primary
     Decision PDF via efi_p2_v2_demo.show_document?p_dms_document_id={id},
     downloads it and extracts the full text (fitz/PyMuPDF; born-digital text
     layer, Tesseract OCR fallback for the rare image-only scan).

Newest-first iteration means the framework's sample pull draws from clean
modern born-digital Decisions.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fitz  # PyMuPDF
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CO-PUC")

BASE_URL = "https://www.dora.state.co.us/pls/efi"
SEARCH_URL = f"{BASE_URL}/EFI_SEARCH_UI.getDecisionResults"
DETAIL_URL = f"{BASE_URL}/EFI_Search_UI.Show_Decision"

# EFI electronic corpus floor: decisions begin in 2000 (1999 and earlier
# return 0 rows).
FIRST_YEAR = 2000

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

SHOWDEC_RE = re.compile(r"Show_Decision", re.I)
PDEC_RE = re.compile(r"p_dec=(\d+)")
DMS_RE = re.compile(r"show_document\?p_dms_document_id=(\d+)", re.I)
DECNUM_RE = re.compile(r"^[A-Z]\d{2}-\d{3,4}(?:-[A-Z]+)?$")
DATE_RE = re.compile(r"([A-Za-z]{3})\w*\s+(\d{1,2}),?\s+(\d{4})")


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_date(raw: str) -> str | None:
    """Turn 'Jan 09, 2025' into '2025-01-09'."""
    m = DATE_RE.search(raw or "")
    if not m:
        return None
    mon = MONTHS.get(m.group(1).lower())
    if not mon:
        return None
    day = int(m.group(2))
    year = int(m.group(3))
    return f"{year:04d}-{mon:02d}-{min(max(day, 1), 28):02d}"


class COPUCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- low-level fetch helpers -------------------------------------------

    def _get(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.content
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _search_year(self, year: int) -> str:
        """GET the decision results for a full calendar year."""
        params = (
            f"?p_session_id=&p_proceeding=&p_title=&p_industry="
            f"&p_after=01/01/{year}&p_before=01/01/{year + 1}"
            f"&p_decision_number=&p_decision_type=&p_decision_author="
            f"&p_cache=&p_sort=&p_direction="
        )
        data = self._get(SEARCH_URL + params)
        return data.decode("utf-8", "replace") if data else ""

    # ---- parsing -----------------------------------------------------------

    def parse_results(self, html: str) -> list:
        """Parse a decision-search results page into raw metadata dicts."""
        items = []
        soup = BeautifulSoup(html or "", "html.parser")
        for tr in soup.find_all("tr"):
            a = tr.find("a", href=SHOWDEC_RE)
            if not a:
                continue
            m = PDEC_RE.search(a.get("href", ""))
            if not m:
                continue
            p_dec = m.group(1)
            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            cells = [c for c in cells if c]
            if not cells:
                continue
            decision_number = cells[0]
            if not DECNUM_RE.match(decision_number):
                continue
            title = a.get_text(" ", strip=True) or (cells[1] if len(cells) > 1 else "")
            date = None
            proceeding = None
            for c in cells[1:]:
                if date is None and DATE_RE.search(c):
                    date = _parse_date(c)
                elif proceeding is None and re.match(r"^\d{2}[A-Z]", c):
                    proceeding = c
            items.append(
                {
                    "decision_number": decision_number,
                    "p_dec": p_dec,
                    "title": title,
                    "proceeding_number": proceeding,
                    "date": date,
                    "detail_url": f"{DETAIL_URL}?p_session_id=&p_dec={p_dec}",
                }
            )
        return items

    def _detail_pdf_url(self, p_dec: str) -> str | None:
        """Fetch the Show_Decision detail page; return the primary PDF URL."""
        data = self._get(f"{DETAIL_URL}?p_session_id=&p_dec={p_dec}")
        if not data:
            return None
        m = DMS_RE.search(data.decode("utf-8", "replace"))
        if not m:
            return None
        return f"{BASE_URL}/efi_p2_v2_demo.show_document?p_dms_document_id={m.group(1)}"

    # ---- PDF extraction ----------------------------------------------------

    def _ocr_pdf(self, doc) -> str:
        try:
            import pytesseract
            from PIL import Image
            import io
        except Exception:
            return ""
        parts = []
        for page in doc:
            try:
                pix = page.get_pixmap(dpi=200)
                img = Image.open(io.BytesIO(pix.tobytes("png")))
                parts.append(pytesseract.image_to_string(img))
            except Exception as e:
                logger.debug(f"OCR page failed: {e}")
        return "\n".join(parts)

    def extract_pdf_text(self, pdf_bytes: bytes) -> str:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            logger.warning(f"PDF open failed: {e}")
            return ""
        try:
            parts = [page.get_text() for page in doc]
            text = clean_text("\n".join(parts))
            if len(text) < 200:
                ocr = clean_text(self._ocr_pdf(doc))
                if len(ocr) > len(text):
                    text = ocr
            return text
        finally:
            doc.close()

    # ---- normalize ---------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_url = self._detail_pdf_url(raw["p_dec"])
        if not pdf_url:
            logger.debug(f"No PDF on detail page p_dec={raw['p_dec']}")
            return None
        pdf_bytes = self._get(pdf_url)
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for decision {raw['decision_number']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        decision_number = raw["decision_number"]
        description = re.sub(r"\s+", " ", (raw.get("title") or "")).strip()
        title = f"Colorado PUC Decision {decision_number}"
        if description:
            short = description if len(description) <= 200 else description[:197] + "..."
            title = f"{title}: {short}"

        return {
            "_id": f"US/CO-PUC/{decision_number}",
            "_source": "US/CO-PUC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "decision_number": decision_number,
            "proceeding_number": raw.get("proceeding_number"),
            "p_dec": raw["p_dec"],
            "title": title,
            "summary": description or None,
            "text": text,
            "url": raw["detail_url"],
            "pdf_url": pdf_url,
            "date": raw.get("date"),
        }

    # ---- discovery ---------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing DORA EFI decision search...")
        try:
            year = datetime.now(timezone.utc).year
            rows = []
            for y in range(year, FIRST_YEAR - 1, -1):
                rows = self.parse_results(self._search_year(y))
                if rows:
                    logger.info(f"  Year {y}: {len(rows)} decisions")
                    break
            if not rows:
                logger.error("  No decisions found")
                return False
            rec = None
            for it in rows:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"decision={rec.get('decision_number')}, date={rec.get('date')})"
                )
            else:
                logger.error("  Full-text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw Decision metadata dicts, newest year first.

        The framework calls normalize() on each raw dict, which fetches the
        detail page, downloads the PDF and extracts the full text.
        """
        seen = set()
        year = datetime.now(timezone.utc).year
        for y in range(year, FIRST_YEAR - 1, -1):
            rows = self.parse_results(self._search_year(y))
            if not rows:
                continue
            logger.info(f"Year {y}: {len(rows)} decisions")
            for r in rows:
                key = r["decision_number"]
                if key in seen:
                    continue
                seen.add(key)
                yield r

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield raw Decision metadata issued on/after `since` (ISO date)."""
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CO-PUC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = COPUCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
