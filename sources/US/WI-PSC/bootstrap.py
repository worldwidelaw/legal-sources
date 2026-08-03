#!/usr/bin/env python3
"""
US/WI-PSC -- Public Service Commission of Wisconsin Orders

Fetches the full text of Orders (and ALJ Orders) issued by the Public Service
Commission of Wisconsin (PSC) adjudicating utility rate cases, certificate of
authority (CE/CW) applications, complaints and other dockets across the
electric, natural-gas, water, telecommunications and related industries. Each
Order is an administrative adjudication of a specific docket by the Commission
(or by an Administrative Law Judge) = case_law. Public domain (US state
government edict).

Strategy (official ERF: apps.psc.wi.gov, ASP.NET WebForms):
  1. The Electronic Records Filing (ERF) advanced search
     /ERF/ERFsearch/default.aspx is an ASP.NET WebForms page whose document
     search filters by document type (checkboxes) and a received-date window.
     Checking the "Order" (ckb_doc_type_cd$21) and "ALJ Order"
     (ckb_doc_type_cd$1) types with a date window returns a server-rendered
     HTML results table: one row per document carrying the PSC Ref# (docid),
     the description, the document type, the docket/utility id and the
     received date.
  2. The ASP.NET __VIEWSTATE / __EVENTVALIDATION captured once from the search
     page can be reused across every window POST (the control set is static),
     so fetch_all() walks the corpus one month at a time (newest first) back to
     the ERF floor, keeping each result set small.
  3. For each result row the born-digital Order PDF is downloaded directly from
     /ERF/ERFview/viewdoc.aspx?docid={docid} and the full text is extracted
     (fitz/PyMuPDF; Tesseract OCR fallback for the rare image-only scan).

Newest-first iteration means the framework's sample pull draws from clean
modern born-digital Orders.

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
import calendar
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
logger = logging.getLogger("legal-data-hunter.US.WI-PSC")

BASE_URL = "https://apps.psc.wi.gov"
SEARCH_URL = f"{BASE_URL}/ERF/ERFsearch/default.aspx"
VIEWDOC_URL = f"{BASE_URL}/ERF/ERFview/viewdoc.aspx?docid="

# Document-type checkbox field names for the adjudicative Order types.
ORDER_TYPE_FIELDS = ["ckb_doc_type_cd$21", "ckb_doc_type_cd$1"]  # Order, ALJ Order

# Corpus floor: the WI PSC ERF Orders series thins out before the early 2000s.
FIRST_YEAR = 1996

DOCID_RE = re.compile(r"docid=(\d+)", re.I)
# WI docket ids look like 9814-CE-100, 5-CE-156, 4480-CW-113, 05-EI-100, etc.
DOCKET_RE = re.compile(r"\b(\d{1,4}-[A-Z]{2}-\d{1,4})\b")


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(cell: str) -> str | None:
    """Convert an 'M/D/YYYY [time]' cell to an ISO date."""
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", cell or "")
    if not m:
        return None
    mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    except Exception:
        return None


def _month_windows():
    """Yield (start, end) 'M/D/YYYY' strings, newest month first, to FIRST_YEAR."""
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month
    while year >= FIRST_YEAR:
        last = calendar.monthrange(year, month)[1]
        yield (f"{month}/1/{year}", f"{month}/{last}/{year}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1


class WIPSCScraper(BaseScraper):

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
        self._form = None  # cached hidden ASP.NET form fields

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

    def _load_form(self) -> dict:
        """Fetch the search page once and cache its hidden ASP.NET fields."""
        if self._form is not None:
            return self._form
        data = self._get(SEARCH_URL)
        html = data.decode("utf-8", "replace") if data else ""
        fields = {}
        for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
            m = re.search(r'name="' + name + r'"[^>]*value="([^"]*)"', html)
            fields[name] = m.group(1) if m else ""
        if not fields.get("__VIEWSTATE"):
            raise RuntimeError("Could not load ERF search form __VIEWSTATE")
        self._form = fields
        return fields

    # ---- results parsing ----------------------------------------------------

    def parse_results(self, html: str) -> list:
        """Parse an ERF search results table into raw metadata dicts."""
        items = []
        soup = BeautifulSoup(html or "", "html.parser")
        # Find the results table: the one whose header row names 'PSC Ref'.
        target = None
        for table in soup.find_all("table"):
            head = table.get_text(" ", strip=True)[:200]
            if "PSC Ref" in head and "Document Type" in head:
                target = table
                break
        if target is None:
            return items

        rows = target.find_all("tr")
        if not rows:
            return items
        # Map header -> column index from the first row.
        header_cells = rows[0].find_all(["th", "td"])
        headers = [c.get_text(" ", strip=True) for c in header_cells]

        def col(name_frag: str, default: int) -> int:
            for i, h in enumerate(headers):
                if name_frag.lower() in h.lower():
                    return i
            return default

        i_ref = col("PSC Ref", 1)
        i_desc = col("Description", 2)
        i_type = col("Document Type", 3)
        i_docket = col("Docket", 4)
        i_date = col("Received Date", 5)

        for tr in rows[1:]:
            tds = tr.find_all("td")
            if len(tds) <= i_date:
                continue
            # docid from the viewdoc link anywhere in the row
            link = tr.find("a", href=DOCID_RE)
            m = DOCID_RE.search(link.get("href", "")) if link else None
            if not m:
                m = DOCID_RE.search(str(tr))
            if not m:
                continue
            docid = m.group(1)

            doc_type = tds[i_type].get_text(" ", strip=True) if i_type < len(tds) else ""
            if doc_type and "order" not in doc_type.lower():
                continue

            desc = tds[i_desc].get_text(" ", strip=True) if i_desc < len(tds) else ""
            docket = tds[i_docket].get_text(" ", strip=True) if i_docket < len(tds) else ""
            date = parse_date(tds[i_date].get_text(" ", strip=True)) if i_date < len(tds) else None

            items.append(
                {
                    "docid": docid,
                    "description": desc or None,
                    "doc_type": doc_type or "Order",
                    "docket": docket or None,
                    "date": date,
                    "pdf_url": f"{VIEWDOC_URL}{docid}",
                    "url": (
                        f"{BASE_URL}/ERF/ERFsearch/content/documentInfo.aspx?docid={docid}"
                    ),
                }
            )
        return items

    def _fetch_window(self, start: str, end: str) -> list:
        form = self._load_form()
        data = {
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": form["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": form["__VIEWSTATEGENERATOR"],
            "__EVENTVALIDATION": form["__EVENTVALIDATION"],
            "datepicker_start": start,
            "datepicker_end": end,
            "txt_keyword_phase": "",
            "docket_util_id": "",
            "docket_case_type_cd": "",
            "docket_seq_num": "",
            "btn_search": "Search",
        }
        for field in ORDER_TYPE_FIELDS:
            data[field] = "on"
        html = self._post(SEARCH_URL, data)
        rows = self.parse_results(html)
        if len(rows) >= 1000:
            logger.warning(
                f"Window {start}..{end} hit ~1000 rows; may be truncated"
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
        # Guard: viewdoc must return a real PDF (born-digital).
        if pdf_bytes[:5] != b"%PDF-":
            logger.debug(f"docid {raw['docid']}: not a PDF payload")
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for docid {raw['docid']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        docid = raw["docid"]
        desc = re.sub(r"\s+", " ", raw.get("description") or "").strip()
        docket = raw.get("docket")
        # Fall back to a docket parsed from the PDF body if the listing lacked one.
        if not docket:
            dm = DOCKET_RE.search(text[:3000])
            if dm:
                docket = dm.group(1)

        doc_type = raw.get("doc_type") or "Order"
        title = f"WI PSC {doc_type}"
        if docket:
            title += f" — Docket {docket}"
        if desc:
            short = desc if len(desc) <= 200 else desc[:197] + "..."
            title += f" ({short})"

        return {
            "_id": f"US/WI-PSC/{docid}",
            "_source": "US/WI-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "psc_ref": docid,
            "docket_number": docket,
            "doc_type": doc_type,
            "title": title,
            "description": desc or None,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": raw.get("date"),
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing WI PSC ERF Orders search...")
        try:
            rows = []
            for start, end in _month_windows():
                rows = self._fetch_window(start, end)
                if rows:
                    logger.info(f"  Window {start}..{end}: {len(rows)} Orders")
                    break
            if not rows:
                logger.error("  No Orders found in recent windows")
                return False
            rec = None
            for it in rows:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"docid {rec['psc_ref']}, docket={rec.get('docket_number')}, "
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
        """Yield raw Order metadata dicts, newest month first.

        The framework calls normalize() on each raw dict, which downloads the
        PDF and extracts the full text.
        """
        seen = set()
        empty_streak = 0
        for start, end in _month_windows():
            rows = self._fetch_window(start, end)
            if not rows:
                empty_streak += 1
                if empty_streak >= 24:
                    logger.info("Reached 24 consecutive empty months; stopping.")
                    break
                continue
            empty_streak = 0
            for r in rows:
                key = r["docid"]
                if key in seen:
                    continue
                seen.add(key)
                yield r

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WI-PSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WIPSCScraper()

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
