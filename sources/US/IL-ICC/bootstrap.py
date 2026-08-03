#!/usr/bin/env python3
"""
US/IL-ICC -- Illinois Commerce Commission Orders

Fetches the full text of Orders issued by the Illinois Commerce Commission
(ICC) adjudicating utility dockets -- rate cases, complaints, certificate and
reorganization proceedings across the electric, gas, water/sewer, telecom and
transportation (motor carrier) industries. Each Order is the Commission's
administrative adjudication of a specific docket = case_law (public domain,
US state government edict).

Strategy (official ICC e-Docket system, icc.illinois.gov, ASP.NET MVC):
  1. The public document search endpoint /docket/search/documents/results is a
     POST form. Filtering by SelectedYear + selectedDocumentType returns a
     server-rendered list of every matching document for that year: the
     document type label, a short description, the party names, the issue date
     and a link to the document detail page /docket/{case}/documents/{docid}.
     No authentication, cookie or anti-forgery token is required for the search
     (only the interactive e-filing side needs a login).
  2. The corpus is walked one year at a time (newest first) back to the
     e-Docket floor year (2000). For each order-type document, the detail page
     exposes one or more born-digital Order PDFs at
     /docket/{case}/documents/{docid}/files/{fileid}.pdf.
  3. normalize() fetches the detail page, downloads the primary Order PDF and
     extracts the full text (fitz/PyMuPDF; born-digital text layer, Tesseract
     OCR fallback for the rare image-only scan). The docket number is the ICC
     case number parsed from the detail URL / PDF body.

Only adjudicative Order document types are collected: "Order - Final" (the
dominant series, ~800-900/year), plus "Order", "Order on Rehearing" and
"Order on Remand".

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
logger = logging.getLogger("legal-data-hunter.US.IL-ICC")

BASE_URL = "https://icc.illinois.gov"
SEARCH_URL = f"{BASE_URL}/docket/search/documents/results"

# e-Docket electronic corpus floor: Order documents begin in 2000
# (1999 and earlier return 0 rows).
FIRST_YEAR = 2000

# Adjudicative order document types (value -> label) from the search form's
# SelectedDocumentType dropdown. "Order - Final" (21) is the dominant series.
ORDER_DOC_TYPES = {
    "21": "Order - Final",
    "42": "Order",
    "44": "Order on Rehearing",
    "45": "Order on Remand",
}

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

DOCID_RE = re.compile(r"^/docket/([^/]+)/documents/(\d+)/?$")
PDFFILE_RE = re.compile(r"/docket/[^/]+/documents/\d+/files/\d+\.pdf$")
DOCKET_RE = re.compile(r"Docket\s+No\.?\s*([0-9]{2}-[0-9]{3,4})", re.I)


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _case_to_docket(case_slug: str) -> str | None:
    """Turn a URL case slug like 'P2025-0175' into the ICC docket '25-0175'."""
    m = re.match(r"[A-Za-z]?(\d{4})-(\d+)", case_slug or "")
    if not m:
        return None
    yy = m.group(1)[2:]
    return f"{yy}-{m.group(2)}"


class ILICCScraper(BaseScraper):

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

    def _post_search(self, doc_type: str, year: int, retries: int = 4) -> str:
        payload = {"selectedDocumentType": doc_type, "SelectedYear": str(year)}
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.post(SEARCH_URL, data=payload)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(
                    f"HTTP {resp.status_code} for search {doc_type}/{year}"
                )
            except Exception as e:
                logger.warning(
                    f"Error searching {doc_type}/{year} (attempt {attempt + 1}): {e}"
                )
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    # ---- parsing -----------------------------------------------------------

    def parse_results(self, html: str, doc_type_label: str) -> list:
        """Parse a document-search results page into raw metadata dicts."""
        items = []
        soup = BeautifulSoup(html or "", "html.parser")
        for li in soup.select("li.soi-icc-card-list-item"):
            a = li.find("a", href=DOCID_RE)
            if not a:
                continue
            m = DOCID_RE.match(a.get("href", ""))
            if not m:
                continue
            case_slug, docid = m.group(1), m.group(2)

            # Date from the calendar card header: "<div>Jul - 2026</div><div>1</div>"
            date = None
            header = li.find(class_="card-header")
            if header:
                divs = header.find_all("div")
                my = divs[0].get_text(" ", strip=True) if divs else ""
                day = divs[1].get_text(" ", strip=True) if len(divs) > 1 else ""
                dm = re.match(r"([A-Za-z]{3})\w*\s*-\s*(\d{4})", my)
                if dm:
                    mon = MONTHS.get(dm.group(1).lower())
                    yr = int(dm.group(2))
                    try:
                        dd = int(re.sub(r"\D", "", day) or "1")
                    except ValueError:
                        dd = 1
                    if mon:
                        date = f"{yr:04d}-{mon:02d}-{min(max(dd, 1), 28):02d}"

            # Free-text lines under the heading (description + party names)
            body = li.find(class_="card-body")
            lines = []
            if body:
                for sp in body.find_all("span", class_="d-block"):
                    t = sp.get_text(" ", strip=True)
                    if t:
                        lines.append(t)
            description = lines[0] if lines else None
            parties = [ln for ln in lines[1:] if ln]

            items.append(
                {
                    "case_slug": case_slug,
                    "doc_id": docid,
                    "doc_type": doc_type_label,
                    "description": description,
                    "parties": parties,
                    "date": date,
                    "detail_url": f"{BASE_URL}/docket/{case_slug}/documents/{docid}",
                }
            )
        return items

    def _detail_pdf_url(self, detail_url: str) -> str | None:
        """Fetch the document detail page and return the primary Order PDF URL."""
        data = self._get(detail_url)
        if not data:
            return None
        soup = BeautifulSoup(data.decode("utf-8", "replace"), "html.parser")
        for a in soup.find_all("a", href=PDFFILE_RE):
            href = a.get("href")
            if href:
                return BASE_URL + href
        return None

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
        pdf_url = self._detail_pdf_url(raw["detail_url"])
        if not pdf_url:
            logger.debug(f"No PDF on detail page {raw['detail_url']}")
            return None
        pdf_bytes = self._get(pdf_url)
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for doc {raw['doc_id']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        docket = _case_to_docket(raw["case_slug"])
        # Prefer a docket parsed from the PDF body if present and well-formed.
        dm = DOCKET_RE.search(text[:2000])
        if dm:
            docket = dm.group(1)

        doc_type = raw.get("doc_type") or "Order"
        description = re.sub(r"\s+", " ", (raw.get("description") or "")).strip()
        title = f"ICC Docket {docket} — {doc_type}" if docket else doc_type
        if description and description.lower() != doc_type.lower():
            short = description if len(description) <= 200 else description[:197] + "..."
            title = f"{title}: {short}"

        return {
            "_id": f"US/IL-ICC/{docket or raw['case_slug']}-{raw['doc_id']}",
            "_source": "US/IL-ICC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "docket_number": docket,
            "doc_type": doc_type,
            "parties": raw.get("parties") or None,
            "title": title,
            "summary": description or None,
            "text": text,
            "url": raw["detail_url"],
            "pdf_url": pdf_url,
            "date": raw.get("date"),
        }

    # ---- discovery ---------------------------------------------------------

    def _year_docs(self, year: int) -> list:
        rows = []
        for dt, label in ORDER_DOC_TYPES.items():
            html = self._post_search(dt, year)
            rows.extend(self.parse_results(html, label))
        return rows

    def test_api(self) -> bool:
        logger.info("Testing ICC e-Docket document search...")
        try:
            year = datetime.now(timezone.utc).year
            rows = []
            for y in range(year, FIRST_YEAR - 1, -1):
                rows = self.parse_results(
                    self._post_search("21", y), "Order - Final"
                )
                if rows:
                    logger.info(f"  Year {y}: {len(rows)} Order - Final documents")
                    break
            if not rows:
                logger.error("  No Order documents found")
                return False
            rec = None
            for it in rows:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"docket={rec.get('docket_number')}, date={rec.get('date')})"
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
        """Yield raw Order metadata dicts, newest year first.

        The framework calls normalize() on each raw dict, which fetches the
        detail page, downloads the PDF and extracts the full text.
        """
        seen = set()
        year = datetime.now(timezone.utc).year
        for y in range(year, FIRST_YEAR - 1, -1):
            rows = self._year_docs(y)
            if not rows:
                continue
            logger.info(f"Year {y}: {len(rows)} order documents")
            for r in rows:
                key = r["doc_id"]
                if key in seen:
                    continue
                seen.add(key)
                yield r

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield raw Order metadata issued on/after `since` (ISO date)."""
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/IL-ICC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ILICCScraper()

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
