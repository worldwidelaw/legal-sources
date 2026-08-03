#!/usr/bin/env python3
"""
US/WA-UTC -- Washington Utilities & Transportation Commission Orders

Fetches the full text of Orders issued by the Washington Utilities and
Transportation Commission (UTC) adjudicating utility dockets -- rate cases,
tariff filings, complaints, penalties and rulemakings across the electric,
gas, water, telecommunications and transportation industries. Each Order is
the Commission's administrative adjudication of a specific docket = case_law
(public domain, US state government edict).

Strategy (official UTC case-docket system, utc.wa.gov Drupal front end backed
by the apiproxy.utc.wa.gov document API):
  1. The docket universe is a Drupal Views listing at
     /documents-and-proceedings/dockets, paginated 50 dockets/page (newest
     docket first, ~729 pages). Each row links to /casedocket/{year}/{docket}.
  2. For each docket the Orders tab /casedocket/{year}/{docket}/orders is a
     server-rendered <table> of every Order in that docket: service date,
     filename, order type ("Order - Open Meeting Final", "Order - Other", ...),
     a description and the document link
     https://apiproxy.utc.wa.gov/cases/GetDocument?docID={id}&year={y}&docketNumber={d}.
  3. normalize() downloads the GetDocument PDF (born-digital text layer;
     Tesseract OCR fallback for the rare image-only scan) and extracts full
     text. No authentication, cookie or token is required.

Newest-first iteration (page 0 = newest dockets) means the framework's sample
pull draws from clean modern born-digital Orders.

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
logger = logging.getLogger("legal-data-hunter.US.WA-UTC")

BASE_URL = "https://www.utc.wa.gov"
DOCKET_LIST_URL = f"{BASE_URL}/documents-and-proceedings/dockets"
API_BASE = "https://apiproxy.utc.wa.gov"

# Drupal Views docket list: ~729 pages of 50 dockets each. A generous cap;
# fetch_all() stops early when a page yields no docket links.
MAX_LIST_PAGES = 900

DOCKET_LINK_RE = re.compile(r"^/casedocket/(\d{4})/(\d+)/?$")
GETDOC_RE = re.compile(
    r"GetDocument\?docID=(\d+)&year=(\d+)&docketNumber=(\d+)", re.I
)
DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})")

# Proof-of-service / affidavit attachments are (mis)filed under the "Order"
# document types; they are not orders and would ~double the corpus with
# near-duplicate affidavit noise, so they are filtered out by filename.
POS_TOKEN_RE = re.compile(r"(?<![A-Z0-9])P0?S(?![A-Z0-9])")
NON_ORDER_FILENAME_RE = re.compile(
    r"PROOF\s+OF\s+SERVICE|CERT(?:IFICATE)?\.?\s+OF\s+SERVICE|"
    r"AFFIDAVIT|DECLARATION\s+OF\s+SERVICE",
    re.I,
)


def _is_proof_of_service(filename: str) -> bool:
    """True if the attachment is a proof-of-service / affidavit, not an Order."""
    fn = (filename or "").upper()
    if NON_ORDER_FILENAME_RE.search(fn):
        return True
    stem = fn.rsplit(".", 1)[0]
    return bool(POS_TOKEN_RE.search(stem))


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
    """Turn 'MM/DD/YYYY' into 'YYYY-MM-DD'."""
    m = DATE_RE.search(raw or "")
    if not m:
        return None
    mm, dd, yyyy = m.group(1), m.group(2), m.group(3)
    return f"{yyyy}-{mm}-{dd}"


class WAUTCScraper(BaseScraper):

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

    # ---- parsing -----------------------------------------------------------

    def _list_page_dockets(self, page: int) -> list:
        """Return (year, docket_number) pairs from one docket-list page."""
        data = self._get(f"{DOCKET_LIST_URL}?page={page}")
        if not data:
            return []
        soup = BeautifulSoup(data.decode("utf-8", "replace"), "html.parser")
        seen = []
        found = set()
        for a in soup.find_all("a", href=DOCKET_LINK_RE):
            m = DOCKET_LINK_RE.match(a.get("href", ""))
            if not m:
                continue
            key = (m.group(1), m.group(2))
            if key in found:
                continue
            found.add(key)
            seen.append(key)
        return seen

    def _docket_orders(self, year: str, docket: str) -> list:
        """Parse the Orders tab of a docket into raw metadata dicts."""
        url = f"{BASE_URL}/casedocket/{year}/{docket}/orders"
        data = self._get(url)
        if not data:
            return []
        soup = BeautifulSoup(data.decode("utf-8", "replace"), "html.parser")
        items = []
        for tr in soup.find_all("tr"):
            a = tr.find("a", href=GETDOC_RE)
            if not a:
                continue
            m = GETDOC_RE.search(a.get("href", ""))
            if not m:
                continue
            doc_id, y, dnum = m.group(1), m.group(2), m.group(3)
            tds = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            date = None
            filename = None
            doc_type = None
            description = None
            if tds:
                date = _parse_date(tds[0])
                if len(tds) > 1:
                    filename = tds[1]
                if len(tds) > 2:
                    doc_type = tds[2]
                if len(tds) > 3:
                    description = tds[3]
            # Only actual Order documents (the /orders tab also lists the
            # proof-of-service affidavits filed alongside each order).
            if doc_type and not doc_type.lower().startswith("order"):
                continue
            # Skip WordPerfect/Word bodies (pre-~2004) we cannot extract here.
            if filename and not filename.lower().endswith(".pdf"):
                continue
            if _is_proof_of_service(filename):
                continue
            items.append(
                {
                    "doc_id": doc_id,
                    "year": y,
                    "docket_number": dnum,
                    "filename": filename,
                    "doc_type": doc_type,
                    "description": description,
                    "date": date,
                    "pdf_url": (
                        f"{API_BASE}/cases/GetDocument?docID={doc_id}"
                        f"&year={y}&docketNumber={dnum}"
                    ),
                    "detail_url": f"{BASE_URL}/casedocket/{year}/{docket}/orders",
                }
            )
        return items

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
        pdf_bytes = self._get(raw["pdf_url"])
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for doc {raw['doc_id']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        # Safety net: a proof-of-service that slipped past the filename filter.
        head = text[:400].upper()
        if "PROOF OF SERVICE" in head and "ORDER" not in head:
            return None

        dnum = raw["docket_number"]
        doc_type = raw.get("doc_type") or "Order"
        description = re.sub(r"\s+", " ", (raw.get("description") or "")).strip()
        title = f"WA UTC Docket {dnum} — {doc_type}"
        if description and description.lower() != doc_type.lower():
            short = description if len(description) <= 200 else description[:197] + "..."
            title = f"{title}: {short}"

        return {
            "_id": f"US/WA-UTC/{dnum}-{raw['doc_id']}",
            "_source": "US/WA-UTC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "docket_number": dnum,
            "doc_type": doc_type,
            "filename": raw.get("filename") or None,
            "title": title,
            "summary": description or None,
            "text": text,
            "url": raw["detail_url"],
            "pdf_url": raw["pdf_url"],
            "date": raw.get("date"),
        }

    # ---- discovery ---------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing UTC docket list + orders + document API...")
        try:
            rec = None
            for page in range(0, 5):
                dockets = self._list_page_dockets(page)
                if not dockets:
                    continue
                logger.info(f"  List page {page}: {len(dockets)} dockets")
                for year, dnum in dockets:
                    orders = self._docket_orders(year, dnum)
                    if not orders:
                        continue
                    for it in orders:
                        rec = self.normalize(it)
                        if rec:
                            break
                    if rec:
                        break
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"docket={rec.get('docket_number')}, date={rec.get('date')})"
                )
                logger.info("API test PASSED")
                return True
            logger.error("  Full-text extraction failed or no orders found")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw Order metadata dicts, newest docket first.

        Walks the paginated docket-list Views, then each docket's Orders tab.
        The framework calls normalize() on each raw dict, which downloads the
        PDF and extracts the full text.
        """
        seen_docs = set()
        empty_pages = 0
        for page in range(0, MAX_LIST_PAGES):
            dockets = self._list_page_dockets(page)
            if not dockets:
                empty_pages += 1
                if empty_pages >= 3:
                    break
                continue
            empty_pages = 0
            for year, dnum in dockets:
                orders = self._docket_orders(year, dnum)
                if not orders:
                    continue
                for r in orders:
                    if r["doc_id"] in seen_docs:
                        continue
                    seen_docs.add(r["doc_id"])
                    yield r

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield raw Order metadata issued on/after `since` (ISO date)."""
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WA-UTC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WAUTCScraper()

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
