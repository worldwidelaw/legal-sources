#!/usr/bin/env python3
"""
US/AR-PSC -- Arkansas Public Service Commission Orders

Fetches the full text of Orders issued by the Arkansas Public Service
Commission (APSC) adjudicating utility dockets — electric, natural gas,
water/sewer, telecommunications rate cases, certificates of public
convenience and necessity (CCN), fuel/purchased-gas adjustment riders,
formula-rate reviews, tariff filings and rulemaking. Each Commission (or
Administrative Law Judge) Order is an administrative adjudication / edict
of a specific docket = case_law. Public domain (US state government edict).

Strategy (official APSC Online Services eFiling system, OLS v2 at
apps.apsc.arkansas.gov/olsv2):

  1. Dockets are addressed as ``YY-NNN-X`` (e.g. 22-064-U, 07-016-U). The
     docket-search page (``docket_search.asp``) exposes a ``CaseNumber``
     <select> pre-populated with several hundred dockets that have recent
     activity — that list seeds enumeration. Any other historic docket
     number also resolves directly, so fetch_all() additionally sweeps the
     ``YY-NNN-X`` id space per year/suffix with gap tolerance (a
     non-existent docket returns a short empty shell page, easy to detect).

  2. For each docket, ``docket_search_results.asp?casenumber={docket}``
     returns the full filing log. Commission Orders are the rows whose
     description begins ``N. ORDER NO. M (COMMISSION)`` (or an ALJ name).
     fetch_all() yields one raw dict per Order row (docket, doc number,
     file date, description).

  3. ``Docket_Search_Documents.asp?Docket={docket}&DocNumVal={n}`` lists the
     PDF part(s) of that document as ``pdfview.asp?document={file}.pdf``.
     normalize() downloads each part (raw ``application/pdf`` served by
     ``viewdoc/pdfview.asp``) and extracts full text via fitz/PyMuPDF
     (Tesseract OCR fallback for the rare scanned order), concatenating
     multi-part orders.

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
logger = logging.getLogger("legal-data-hunter.US.AR-PSC")

BASE = "https://apps.apsc.arkansas.gov/olsv2"
SEARCH_FORM_URL = f"{BASE}/docket_search.asp"
RESULTS_URL = f"{BASE}/docket_search_results.asp"
DOC_URL = f"{BASE}/Docket_Search_Documents.asp"
PDFVIEW_URL = f"{BASE}/viewdoc/pdfview.asp"

# A non-existent docket returns a short "empty shell" page (~13.4 KB with no
# document rows); real docket result pages are much larger and carry
# DocNumVal links. We rely on the presence of DocNumVal links, not length.

# Docket-id sweep parameters. Docket format is YY-NNN-X.
# Two-digit "year" prefixes observed span legacy ids too (e.g. 30, 39), but the
# dropdown covers those; the sweep targets modern year-based dockets.
SWEEP_YEARS = [f"{y:02d}" for y in list(range(0, 27)) + list(range(77, 100))]
# Common APSC docket type suffixes.
SWEEP_SUFFIXES = ["U", "A", "TF", "T", "F", "FR", "MR", "PR", "G", "C", "R", "N"]
SWEEP_MAX_SEQ = 400   # max sequence number probed per (year, suffix)
SWEEP_GAP = 12        # stop a (year, suffix) run after this many consecutive misses

# Row of a docket result page: a document link (time/date) + description span.
ROW_RE = re.compile(
    r'Docket_Search_Documents\.asp\?Docket=([^&"]+)&amp;DocNumVal=(\d+)">'
    r'([^<]*)<br\s*/></a><span[^>]*>([^<]*)</span>'
    r'.*?<div class="fivesixth gutterless">\s*<span[^>]*>(.*?)</span>',
    re.S,
)

# A Commission/ALJ Order row description begins "N. ORDER NO. M ...".
ORDER_DESC_RE = re.compile(r'^\s*\d+\.\s*ORDER\s+NO', re.I)
# Order number + authority, e.g. "ORDER NO. 13 (COMMISSION)" / "(HUNT)".
ORDER_NUM_RE = re.compile(r'ORDER\s+NO\.?\s*(\d+)\s*(?:\(([^)]+)\))?', re.I)

PDF_PART_RE = re.compile(r'pdfview\.asp\?document=([^"&\'>\s]+\.pdf)', re.I)

MDY_RE = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{4})')

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
BODY_DATE_RE = re.compile(
    r"(?:Entered|Dated|Issued|Adopted)\s*:?\s*"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),?\s+(\d{4})",
    re.I,
)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def mdy_to_iso(s: str) -> str | None:
    m = MDY_RE.search(s or "")
    if not m:
        return None
    mo, da, yr = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1 <= mo <= 12 and 1 <= da <= 31 and 1900 <= yr <= 2100):
        return None
    return f"{yr:04d}-{mo:02d}-{da:02d}"


def body_date_to_iso(text: str) -> str | None:
    m = BODY_DATE_RE.search(text or "")
    if not m:
        return None
    mon = MONTHS.get(m.group(1).lower())
    if not mon:
        return None
    day, year = int(m.group(2)), int(m.group(3))
    if not (1 <= day <= 31 and 1900 <= year <= 2100):
        return None
    return f"{year:04d}-{mon:02d}-{day:02d}"


class ARPSCScraper(BaseScraper):

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

    def _get_text(self, url: str) -> str:
        raw = self._get(url)
        if not raw:
            return ""
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return ""

    # ---- docket enumeration -------------------------------------------------

    def _dropdown_dockets(self) -> list[str]:
        """Read the CaseNumber <select> on the docket-search form."""
        html = self._get_text(SEARCH_FORM_URL)
        dockets = []
        m = re.search(r'<select[^>]*name="CaseNumber"[^>]*>(.*?)</select>', html, re.S)
        if m:
            for val, _ in re.findall(
                r'<option[^>]*value="([^"]*)"[^>]*>([^<]*)</option>', m.group(1)
            ):
                val = val.strip()
                if re.match(r'^\d{2}-\d{3}-[A-Z]+$', val):
                    dockets.append(val)
        return dockets

    def _order_rows(self, docket: str) -> list[dict]:
        """Return Order rows for a docket, or [] if the docket has none/does not exist."""
        html = self._get_text(f"{RESULTS_URL}?casenumber={docket}")
        if not html or "DocNumVal=" not in html:
            return []
        rows = []
        for dk, docnum, _tm, dt, desc in ROW_RE.findall(html):
            desc_txt = clean_text(re.sub(r"<[^>]+>", " ", desc))
            if not ORDER_DESC_RE.match(desc_txt):
                continue
            rows.append(
                {
                    "docket": dk.strip(),
                    "docnum": docnum.strip(),
                    "date_filed": mdy_to_iso(dt),
                    "description": desc_txt,
                }
            )
        return rows

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

    def _extract_pdf(self, pdf_bytes: bytes) -> str:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        except Exception as e:
            logger.warning(f"PDF open failed: {e}")
            return ""
        try:
            text = clean_text("\n".join(page.get_text() for page in doc))
            if len(text) < 200:
                ocr = clean_text(self._ocr_pdf(doc))
                if len(ocr) > len(text):
                    text = ocr
            return text
        finally:
            doc.close()

    def _part_files(self, docket: str, docnum: str) -> list[str]:
        html = self._get_text(f"{DOC_URL}?Docket={docket}&DocNumVal={docnum}")
        # Preserve order, dedupe.
        seen, parts = set(), []
        for f in PDF_PART_RE.findall(html):
            if f not in seen:
                seen.add(f)
                parts.append(f)
        return parts

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        docket = raw["docket"]
        docnum = raw["docnum"]
        parts = self._part_files(docket, docnum)
        if not parts:
            return None

        texts = []
        for f in parts:
            pdf = self._get(f"{PDFVIEW_URL}?document={f}")
            if not pdf or pdf[:5] != b"%PDF-":
                continue
            t = self._extract_pdf(pdf)
            if t:
                texts.append(t)
        text = clean_text("\n\n".join(texts))
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for {docket} doc {docnum} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        desc = raw.get("description") or ""
        om = ORDER_NUM_RE.search(desc)
        order_no = om.group(1) if om else None
        authority = (om.group(2).strip() if om and om.group(2) else None)

        date = raw.get("date_filed") or body_date_to_iso(text)

        title = f"AR PSC Docket {docket} — Order No. {order_no}" if order_no \
            else f"AR PSC Docket {docket} — Order"
        if authority:
            title += f" ({authority})"

        return {
            "_id": f"US/AR-PSC/{docket}_{docnum}",
            "_source": "US/AR-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "docket_number": docket,
            "doc_number": docnum,
            "order_number": order_no,
            "authority": authority,
            "title": title,
            "text": text,
            "url": f"{DOC_URL}?Docket={docket}&DocNumVal={docnum}",
            "pdf_url": f"{PDFVIEW_URL}?document={parts[0]}",
            "date": date,
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing APSC Online Services docket search...")
        try:
            rows = self._order_rows("22-064-U")
            if not rows:
                logger.error("  No Order rows for test docket 22-064-U")
                return False
            logger.info(f"  Docket 22-064-U: {len(rows)} Order rows")
            rec = None
            for r in rows:
                rec = self.normalize(r)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"order={rec.get('order_number')}, date={rec.get('date')})"
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
        seen_orders = set()
        # Dropdown dockets.
        dropdown = self._dropdown_dockets()
        logger.info(f"Enumerating Orders across {len(dropdown)} dropdown dockets + id sweep")
        emitted_dockets = set()

        def emit(docket: str):
            if docket in emitted_dockets:
                return
            emitted_dockets.add(docket)
            for r in self._order_rows(docket):
                key = (r["docket"], r["docnum"])
                if key in seen_orders:
                    continue
                seen_orders.add(key)
                yield r

        for d in dropdown:
            yield from emit(d)

        # Id-space sweep for dockets not in the dropdown.
        for yy in SWEEP_YEARS:
            for suf in SWEEP_SUFFIXES:
                misses = 0
                for seq in range(1, SWEEP_MAX_SEQ + 1):
                    if misses >= SWEEP_GAP:
                        break
                    docket = f"{yy}-{seq:03d}-{suf}"
                    if docket in emitted_dockets:
                        misses = 0
                        continue
                    html = self._get_text(f"{RESULTS_URL}?casenumber={docket}")
                    if "DocNumVal=" not in html:
                        misses += 1
                        continue
                    misses = 0
                    emitted_dockets.add(docket)
                    for dk, docnum, _tm, dt, desc in ROW_RE.findall(html):
                        desc_txt = clean_text(re.sub(r"<[^>]+>", " ", desc))
                        if not ORDER_DESC_RE.match(desc_txt):
                            continue
                        key = (dk.strip(), docnum.strip())
                        if key in seen_orders:
                            continue
                        seen_orders.add(key)
                        yield {
                            "docket": dk.strip(),
                            "docnum": docnum.strip(),
                            "date_filed": mdy_to_iso(dt),
                            "description": desc_txt,
                        }

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/AR-PSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ARPSCScraper()

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
