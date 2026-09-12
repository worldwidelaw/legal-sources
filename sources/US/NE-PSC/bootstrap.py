#!/usr/bin/env python3
"""
US/NE-PSC -- Nebraska Public Service Commission Orders

Fetches the full text of Orders issued by the Nebraska Public Service
Commission (NPSC) adjudicating dockets across telecommunications, natural
gas, grain warehouse / grain dealer matters, manufactured / modular housing,
transportation (household-goods and passenger carriers), State 911 and
administrative rules-and-regulations proceedings. Each Order is an
administrative adjudication / edict of a specific docket = case_law. Public
domain (US state government edict).

Strategy (official NPSC Order Search: nebraska.gov/psc/ordersearch):
  1. The Order Search page /psc/ordersearch/user/index.cgi exposes a
     keyword full-text search. A POST with sbkw={keyword} returns
     server-rendered result "cards", each carrying a direct link to the
     born-digital Order PDF at /psc/orders/{subdir}/{file}.pdf plus a text
     snippet. Pagination is an AJAX POST with page/size/next params
     (10 hits/page, server-fixed).
  2. Every genuine NPSC Order PDF carries the "NEBRASKA PUBLIC SERVICE
     COMMISSION" header, so the broad keyword "commission" is the site's
     own full-text index over the whole corpus (~14,400 Orders back to the
     May 27, 1980 floor). fetch_all() walks the keyword result pages and
     yields one raw dict per Order PDF (de-duplicated by PDF path).
  3. normalize() downloads each Order PDF directly and extracts full text
     via fitz/PyMuPDF (Tesseract OCR fallback for the rare image-only
     scan); the entered date and docket number are parsed from the PDF
     body / filename.

Directory browsing of the /psc/orders/ subdirectories is disabled (403), so
the keyword index is the enumeration path; individual PDF files are served
openly (200).

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
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
logger = logging.getLogger("legal-data-hunter.US.NE-PSC")

BASE_URL = "https://www.nebraska.gov"
SEARCH_URL = f"{BASE_URL}/psc/ordersearch/user/index.cgi"
ORDERS_PREFIX = "/psc/orders/"

# The broad keyword every NPSC Order header matches -> full-corpus enumerator.
ENUM_KEYWORD = "commission"

# Map storage subdirectory -> NPSC department/division label.
DEPT_BY_SUBDIR = {
    "telecom": "Telecommunications",
    "ntips": "Telecommunications",
    "natgas": "Natural Gas",
    "grain": "Grain",
    "housing": "Housing",
    "tran": "Transportation",
    "trans": "Transportation",
    "transportation": "Transportation",
    "admin": "Administration",
    "state911": "State 911",
    "911": "State 911",
}

# NPSC docket ids look like NUSF-77, C-4972, RR-171, NG-0087, PI-201, FC-1450.
DOCKET_RE = re.compile(r"\b([A-Z]{1,5}-\d{1,4})\b")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
# "Entered: October 12, 2011" / "Dated: October 12, 2011" / "Entered October 12, 2011"
DATE_RE = re.compile(
    r"(?:Entered|Dated|Issued|Adopted)\s*:?\s*"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),?\s+(\d{4})",
    re.I,
)


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_entered_date(text: str) -> str | None:
    """Parse an 'Entered: Month D, YYYY' date from the PDF body to ISO."""
    m = DATE_RE.search(text or "")
    if not m:
        return None
    mon = MONTHS.get(m.group(1).lower())
    if not mon:
        return None
    day, year = int(m.group(2)), int(m.group(3))
    if not (1 <= day <= 31 and 1900 <= year <= 2100):
        return None
    return f"{year:04d}-{mon:02d}-{day:02d}"


def subject_line(text: str) -> str | None:
    """Extract the 'In the Matter of ...' subject from the order body."""
    m = re.search(r"In the Matter of\b[^\n]*", text or "", re.I)
    if not m:
        return None
    subj = re.sub(r"\s+", " ", m.group(0)).strip()
    # Trim the ')' column markers that follow the caption.
    subj = re.split(r"\s*\)\s*", subj)[0].strip()
    return subj or None


class NEPSCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "X-Requested-With": "XMLHttpRequest",
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

    def _post(self, url: str, data: dict, retries: int = 4) -> str:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.post(url, data=data)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} POST {url}")
            except Exception as e:
                logger.warning(f"Error POST {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    # ---- results parsing ----------------------------------------------------

    def _search_page(self, page: int) -> tuple[list, int]:
        """POST one keyword-search result page. Returns (raw_rows, total_hits)."""
        data = {
            "sbkw": ENUM_KEYWORD,
            "page": str(page),
            "size": "10",
            "next": "next",
        }
        html = self._post(SEARCH_URL, data)
        total = 0
        mt = re.search(r"of\s+([\d,]+)\s+hits", html)
        if mt:
            total = int(mt.group(1).replace(",", ""))
        return self._parse_cards(html), total

    def _parse_cards(self, html: str) -> list:
        """Parse keyword-search result cards into raw metadata dicts."""
        items = []
        soup = BeautifulSoup(html or "", "html.parser")
        for card in soup.select("div.card-body"):
            a = card.find("a", href=True)
            if not a:
                continue
            href = a["href"].strip()
            if ORDERS_PREFIX not in href or not href.lower().endswith(".pdf"):
                continue
            rel = href.split(ORDERS_PREFIX, 1)[1]  # e.g. "telecom/NUSF-77.03.1.pdf"
            doc_key = re.sub(r"\.pdf$", "", rel, flags=re.I)
            subdir = rel.split("/", 1)[0].lower() if "/" in rel else ""
            snip = card.find("p", class_="form-text")
            snippet = snip.get_text(" ", strip=True) if snip else ""
            items.append(
                {
                    "doc_key": doc_key,
                    "filename": rel.rsplit("/", 1)[-1],
                    "subdir": subdir,
                    "pdf_url": f"{BASE_URL}{href}" if href.startswith("/") else href,
                    "snippet": snippet or None,
                }
            )
        return items

    # ---- PDF text extraction ------------------------------------------------

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

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_bytes = self._get(raw["pdf_url"])
        if not pdf_bytes:
            return None
        if pdf_bytes[:5] != b"%PDF-":
            logger.debug(f"{raw['doc_key']}: not a PDF payload")
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for {raw['doc_key']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        doc_key = raw["doc_key"]
        filename = raw.get("filename") or ""
        subdir = raw.get("subdir") or ""
        department = DEPT_BY_SUBDIR.get(subdir)

        # Docket number: prefer the filename stem, else parse the body.
        docket = None
        dm = DOCKET_RE.search(filename)
        if dm:
            docket = dm.group(1)
        else:
            dm = DOCKET_RE.search(text[:3000])
            if dm:
                docket = dm.group(1)

        date = parse_entered_date(text)
        subj = subject_line(text)

        title = f"NE PSC Order — {docket}" if docket else "NE PSC Order"
        if subj:
            short = subj if len(subj) <= 200 else subj[:197] + "..."
            title += f" ({short})"

        return {
            "_id": f"US/NE-PSC/{doc_key}",
            "_source": "US/NE-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_key": doc_key,
            "docket_number": docket,
            "department": department,
            "title": title,
            "text": text,
            "url": raw["pdf_url"],
            "pdf_url": raw["pdf_url"],
            "date": date,
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing NE PSC Order Search...")
        try:
            rows, total = self._search_page(1)
            if not rows:
                logger.error("  No result cards on page 1")
                return False
            logger.info(f"  Page 1: {len(rows)} cards, {total} total hits")
            rec = None
            for it in rows:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"key={rec['doc_key']}, docket={rec.get('docket_number')}, "
                    f"date={rec.get('date')})"
                )
            else:
                logger.error("  Full-text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw Order metadata dicts by walking keyword result pages.

        The framework calls normalize() on each raw dict, which downloads the
        PDF and extracts the full text.
        """
        seen = set()
        page = 1
        total = None
        empty_streak = 0
        while True:
            rows, hits = self._search_page(page)
            if total is None and hits:
                total = hits
                logger.info(f"Enumerating ~{total} Orders (keyword='{ENUM_KEYWORD}')")
            if not rows:
                empty_streak += 1
                if empty_streak >= 3:
                    logger.info(f"No results for {empty_streak} pages; stopping.")
                    break
                page += 1
                continue
            empty_streak = 0
            new_on_page = 0
            for r in rows:
                key = r["doc_key"]
                if key in seen:
                    continue
                seen.add(key)
                new_on_page += 1
                yield r
            # Stop when we've paged past the reported total.
            if total and page * 10 >= total:
                logger.info(f"Reached reported total ({total}); stopping at page {page}.")
                break
            page += 1

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NE-PSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NEPSCScraper()

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
