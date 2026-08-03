#!/usr/bin/env python3
"""
US/AZ-CorpCommission -- Arizona Corporation Commission Decisions & Orders

Fetches the full text of Decisions and Procedural Orders issued by the Arizona
Corporation Commission (ACC) adjudicating utility dockets (electric,
natural-gas, water/sewer, telecommunications, pipeline/railroad safety) —
rate cases, certificates of convenience & necessity, complaints, financings and
other matters. Each Decision/Order is an administrative adjudication of a
specific case by the Commission = case_law. Public domain (US state government
edict).

Strategy (official eDocket system, edocket.azcc.gov / efiling.azcc.gov):
  1. The eDocket Angular app is backed by a PUBLIC (no-auth) JSON API at
     https://efiling.azcc.gov/api/edocket/. The document search endpoint
     POST /documentSearchRequest accepts a filing-date range plus a document
     code and returns document metadata (documentID, imageNumber, filedDate,
     docketSummaries[{docketNumber, companyName, description, caseType}]).
  2. The adjudicative documents are two document codes:
       723 = "Decision - ..." (the substantive Commission Decisions)
       727 = "Procedural Order - ..." (orders setting hearings, granting
             extensions, closing dockets, etc.)
     Both are Commission orders resolving specific dockets = case_law.
  3. Each document's born-digital PDF is served openly at
     https://docket.images.azcc.gov/{imageNumber}.pdf. normalize() downloads
     the PDF and extracts full text (fitz/PyMuPDF; Tesseract OCR fallback for
     older image-only scans). The Decision number is parsed from the body.

fetch_all() walks filing years newest-first so the framework's sample pull
draws from clean modern born-digital Decisions.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Decisions/Orders)
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AZ-CorpCommission")

API_BASE = "https://efiling.azcc.gov/api/edocket"
SEARCH_URL = f"{API_BASE}/documentSearchRequest"
IMAGE_BASE = "https://docket.images.azcc.gov"
# eDocket web UI permalink for a document (human-facing reference).
DOC_VIEW_URL = "https://edocket.azcc.gov/search/docket-search/item-detail"

# Document codes for adjudicative Commission orders (case_law).
#   723 = Decision      727 = Procedural Order
DOC_CODES = (723, 727)
CODE_LABEL = {723: "Decision", 727: "Procedural Order"}

# eDocket digital images go back to the early 1990s (older decisions are
# image-only scans handled by the OCR fallback). The empty-year streak stops
# the walk once a run of years yields nothing.
FIRST_YEAR = 1990
PAGE_SIZE = 100

DECISIONNO_RE = re.compile(r"DECISION\s+NO\.?\s*[:\s]*([0-9]{4,6})", re.I)
DOCKETNO_RE = re.compile(r"DOCKET\s+NO\.?\s*[:\s]*([A-Z]-[0-9A-Z-]+)", re.I)


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_iso_date(raw: str | None) -> str | None:
    """Extract the date portion (YYYY-MM-DD) from an ISO-ish timestamp."""
    if not raw or not isinstance(raw, str):
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1900 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31):
        return None
    return f"{y:04d}-{mo:02d}-{d:02d}"


class AZCorpCommissionScraper(BaseScraper):

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
                "Accept": "application/json, text/plain, */*",
                "Content-Type": "application/json",
                "Origin": "https://edocket.azcc.gov",
                "Referer": "https://edocket.azcc.gov/",
            },
            timeout=90,
        )
        self.delay = 1.0

    # -- HTTP helpers -------------------------------------------------------

    def _search(self, code: int, year: int, page_index: int, retries: int = 4):
        """POST one page of the document search for a code + filing year."""
        body = {
            "companyID": None,
            "yearMatter": None,
            "docketNumber": None,
            "filingDateSearchFrom": f"{year}-01-01",
            "filingDateSearchTo": f"{year}-12-31",
            "barCode": None,
            "filedByname": None,
            "filedForname": None,
            "decisionNumber": None,
            "documentCodeID": code,
            "documentSubCodeID": None,
            "currentPageIndex": page_index,
            "rowsPerPage": PAGE_SIZE,
            "rowsToSkip": page_index * PAGE_SIZE,
        }
        payload = json.dumps(body)
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.post(SEARCH_URL, data=payload)
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except Exception:
                        return json.loads(resp.content.decode("utf-8", "replace"))
                logger.warning(f"HTTP {resp.status_code} for search code={code} year={year} p={page_index}")
            except Exception as e:
                logger.warning(f"Search error code={code} year={year} p={page_index} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_pdf(self, url: str, retries: int = 4) -> bytes | None:
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

    # -- listing ------------------------------------------------------------

    def _raw_from_result(self, code: int, x: dict) -> dict | None:
        """Build a raw metadata dict from one search-result row."""
        img = x.get("imageNumber")
        if not img or not str(img).strip():
            # Confidential / withheld documents have no public image.
            return None
        img = str(img).strip()
        dockets = x.get("docketSummaries") or []
        d0 = dockets[0] if dockets else {}
        return {
            "document_id": x.get("documentID"),
            "image_number": img,
            "code": code,
            "code_label": CODE_LABEL.get(code, "Order"),
            "description": (x.get("description") or "").strip(),
            "date": parse_iso_date(x.get("filedDate")),
            "docket_number": d0.get("docketNumber"),
            "company_name": d0.get("companyName"),
            "docket_desc": (d0.get("description") or "").strip(),
            "case_type": d0.get("caseType"),
            "pdf_url": f"{IMAGE_BASE}/{img}.pdf",
            "url": f"{DOC_VIEW_URL}/{x.get('documentID')}",
        }

    def _list_year_code(self, year: int, code: int) -> list:
        """Page through every document of a given code filed in a year."""
        items = []
        page = 0
        while True:
            data = self._search(code, year, page)
            if not data:
                break
            rows = data.get("searchResult") or []
            total = data.get("totalRowCount") or 0
            if not rows:
                break
            for x in rows:
                raw = self._raw_from_result(code, x)
                if raw:
                    items.append(raw)
            page += 1
            if page * PAGE_SIZE >= total or page > 200:
                break
        return items

    # -- PDF extraction -----------------------------------------------------

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

    # -- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_bytes = self._get_pdf(raw["pdf_url"])
        if not pdf_bytes:
            return None
        if pdf_bytes[:5] != b"%PDF-":
            logger.debug(f"{raw['image_number']}: not a PDF payload")
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for {raw['image_number']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        # Decision number from the body (definitive Decisions only); the
        # "Decision No. NNNNN" stamp can sit near the end of short orders.
        decision_number = None
        dm = DECISIONNO_RE.search(text)
        if dm:
            decision_number = dm.group(1)

        # Docket number: prefer the clean, authoritative value from the
        # eDocket docketSummaries metadata; parse from the body only as a
        # fallback (PDF layout can split the token, e.g. "S-2 0890A...").
        docket_number = raw.get("docket_number")
        if not docket_number:
            km = DOCKETNO_RE.search(text[:2500])
            if km:
                docket_number = km.group(1).rstrip("-")

        label = raw.get("code_label", "Order")
        # Build a descriptive title.
        head = f"AZ Corporation Commission {label}"
        if decision_number:
            head = f"AZ Corporation Commission Decision No. {decision_number}"
        parts = [head]
        if docket_number:
            parts.append(f"Docket {docket_number}")
        caption = raw.get("company_name") or raw.get("docket_desc") or raw.get("description")
        if caption:
            caption = re.sub(r"\s+", " ", caption).strip()
            if len(caption) > 200:
                caption = caption[:197].rstrip() + "..."
            parts.append(caption)
        title = " — ".join(parts)

        return {
            "_id": f"US/AZ-CorpCommission/{raw['document_id']}",
            "_source": "US/AZ-CorpCommission",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "decision_number": decision_number,
            "docket_number": docket_number,
            "document_type": label,
            "case_type": raw.get("case_type"),
            "title": title,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": raw.get("date"),
        }

    # -- driver methods -----------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing ACC eDocket document search API...")
        try:
            year = datetime.now(timezone.utc).year
            rows = []
            while year >= FIRST_YEAR and not rows:
                rows = self._list_year_code(year, 723)
                if rows:
                    logger.info(f"  {year} Decisions: {len(rows)} documents")
                    break
                year -= 1
            if not rows:
                logger.error("  No Decisions found in recent years")
                return False
            rec = None
            for it in rows:
                rec = self.normalize(it)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"decision={rec.get('decision_number')}, "
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
        """Yield raw document metadata dicts, newest filing year first.

        The framework calls normalize() on each raw dict, which downloads the
        PDF and extracts the full text.
        """
        seen = set()
        empty_streak = 0
        year = datetime.now(timezone.utc).year
        while year >= FIRST_YEAR:
            year_items = []
            for code in DOC_CODES:
                year_items.extend(self._list_year_code(year, code))
            if not year_items:
                empty_streak += 1
                if empty_streak >= 5:
                    logger.info("Reached 5 consecutive empty years; stopping.")
                    break
                year -= 1
                continue
            empty_streak = 0
            for r in year_items:
                key = r["document_id"]
                if key in seen:
                    continue
                seen.add(key)
                yield r
            year -= 1

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/AZ-CorpCommission bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AZCorpCommissionScraper()

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
