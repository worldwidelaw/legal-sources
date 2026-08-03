#!/usr/bin/env python3
"""
US/NV-PUC -- Public Utilities Commission of Nevada (PUCN) Orders

Fetches the full text of Orders issued by the Public Utilities Commission of
Nevada (PUCN) resolving utility dockets (electric, gas, water,
telecommunications, railroad, renewable/clean-energy rate cases, certificate &
resource-plan applications, complaints). Each Order is a final or procedural
administrative adjudication of a specific docket by the Commission = case_law.
Public domain (US state government edict).

Strategy (official public document search: puc-onbase.nv.gov, a Hyland OnBase
Public Access "OBPA" Angular app backed by a public JSON API):
  1. POST /api/CustomQuery/KeywordSearch with the built-in public custom query
     "PUC - Public Search - Dockets" (QueryID 125), a Filing-Date window
     (FromDate/ToDate as M/D/YYYY) and QueryLimit. The response is a JSON list
     of documents: each carries an opaque OnBase document ID plus display
     columns [Docket Number, Category, Filing Date, Description]. Keep only the
     rows whose Category == "ORDER".
  2. For each Order, GET /api/Document/{urlencoded-id}/?ViewerMode=PDF&
     ForceDownload=true (public, no auth) to download the Order PDF and extract
     the full text.

The PUCN Order PDFs are scanned images (no born-digital text layer), so full
text is produced by rasterizing each page (fitz/PyMuPDF) and running Tesseract
OCR. fetch_all() walks the corpus one month at a time (newest first) back to
the ~2002 floor.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import io
import json
import hashlib
import logging
import re
import time
import calendar
import urllib.parse
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
logger = logging.getLogger("legal-data-hunter.US.NV-PUC")

BASE_URL = "https://puc-onbase.nv.gov"
API = f"{BASE_URL}/api"
SEARCH_URL = f"{API}/CustomQuery/KeywordSearch"
# Built-in public custom query "PUC - Public Search - Dockets".
DOCKETS_QUERY_ID = 125

# The OnBase corpus of docket documents thins to nothing before ~2002.
FIRST_YEAR = 2002

# NV PUCN docket ids look like 25-02016, 23-08015, 04-11033, etc.
DOCKET_RE = re.compile(r"\b(\d{2}-\d{4,6})\b")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(cell: str) -> str | None:
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", cell or "")
    if not m:
        return None
    mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if not (1 <= mm <= 12 and 1 <= dd <= 31):
        return None
    return f"{yyyy:04d}-{mm:02d}-{dd:02d}"


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


class NVPUCScraper(BaseScraper):

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
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- low-level helpers --------------------------------------------------

    def _search(self, start: str, end: str, retries: int = 4) -> list:
        """Run one Dockets custom-query window; return the raw document list."""
        payload = {
            "QueryID": DOCKETS_QUERY_ID,
            "Keywords": [],
            "FromDate": start,
            "ToDate": end,
            "QueryLimit": 1000,
        }
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.post(
                    SEARCH_URL, json_data=payload,
                    headers={"Referer": BASE_URL + "/",
                             "Content-Type": "application/json"},
                )
                if resp.status_code == 200:
                    data = resp.json().get("Data", [])
                    return data
                logger.warning(
                    f"Search {start}..{end} attempt {attempt+1}: "
                    f"status={resp.status_code}"
                )
            except Exception as e:
                logger.warning(f"Search {start}..{end} attempt {attempt+1} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return []

    @staticmethod
    def _cols(doc: dict) -> list:
        return [c.get("Value") for c in doc.get("DisplayColumnValues", [])]

    def _parse_window(self, start: str, end: str) -> list:
        """Return raw metadata dicts for the ORDER documents in a window."""
        items = []
        for doc in self._search(start, end):
            cols = self._cols(doc)
            # Columns: [Docket Number, Category, Filing Date, Description]
            if len(cols) < 2 or (cols[1] or "").strip().upper() != "ORDER":
                continue
            docid = doc.get("ID")
            if not docid:
                continue
            docket = (cols[0] or "").strip() or None
            date = parse_date(cols[2]) if len(cols) > 2 else None
            desc = (cols[3] or "").strip() if len(cols) > 3 else None
            items.append({
                "docid": docid,
                "name": doc.get("Name") or "",
                "docket": docket,
                "date": date,
                "description": desc or None,
                "url": f"{BASE_URL}/?docid={urllib.parse.quote(docid, safe='')}",
                "pdf_url": (
                    f"{API}/Document/{urllib.parse.quote(docid, safe='')}/"
                    "?ViewerMode=PDF&ForceDownload=true"
                ),
            })
        return items

    # ---- PDF text extraction (OCR-primary: PUCN Orders are scans) -----------

    def _ocr_pdf(self, doc) -> str:
        try:
            import pytesseract
            from PIL import Image
        except Exception as e:
            logger.warning(f"OCR unavailable (pytesseract/PIL): {e}")
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
            text = clean_text("\n".join(page.get_text() for page in doc))
            if len(text) < 200:  # scanned Order -> OCR
                ocr = clean_text(self._ocr_pdf(doc))
                if len(ocr) > len(text):
                    text = ocr
            return text
        finally:
            doc.close()

    def _download(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url, headers={"Referer": BASE_URL + "/"})
                if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
                    return resp.content
                logger.debug(f"download {url} status={resp.status_code}")
            except Exception as e:
                logger.warning(f"download attempt {attempt+1} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return None

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_bytes = self._download(raw["pdf_url"])
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(f"Short/empty text for {raw.get('docket')} ({raw['name']})")
            return None

        docket = raw.get("docket")
        if not docket:
            dm = DOCKET_RE.search(text[:3000])
            if dm:
                docket = dm.group(1)

        slug = hashlib.sha1(
            (raw["name"] or raw["docid"]).encode("utf-8", "replace")
        ).hexdigest()[:12]
        _id = f"US/NV-PUC/{docket or 'na'}-{slug}"

        title = "PUCN Order"
        if docket:
            title += f" — Docket {docket}"
        if raw.get("date"):
            title += f" ({raw['date']})"

        return {
            "_id": _id,
            "_source": "US/NV-PUC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "docket_number": docket,
            "doc_type": "Order",
            "title": title,
            "description": raw.get("description"),
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": raw.get("date"),
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing PUCN OnBase document search (Orders)...")
        try:
            rows = []
            for start, end in _month_windows():
                rows = self._parse_window(start, end)
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
                    f"docket={rec.get('docket_number')}, date={rec.get('date')})"
                )
                logger.info("API test PASSED")
                return True
            logger.error("  Full-text extraction failed or too short")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # ---- iteration ----------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        seen = set()
        empty_streak = 0
        for start, end in _month_windows():
            rows = self._parse_window(start, end)
            if not rows:
                empty_streak += 1
                if empty_streak >= 24:
                    logger.info("Reached 24 consecutive empty months; stopping.")
                    break
                continue
            empty_streak = 0
            for r in rows:
                if r["docid"] in seen:
                    continue
                seen.add(r["docid"])
                yield r

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NV-PUC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NVPUCScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
