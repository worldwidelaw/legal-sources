#!/usr/bin/env python3
"""
US/LA-PSC -- Louisiana Public Service Commission Orders

Fetches the full text of Orders issued by the Louisiana Public Service
Commission (LPSC) adjudicating utility and motor-carrier dockets
(electric, gas, water, telecommunications, transportation). Each Order is
an administrative adjudication / disposition of a specific docket by the
Commission = case_law. Public domain (US state government edict).

Strategy (official public LPSC portal, lpscpubvalence.lpsc.louisiana.gov,
the public face of the LPSC "STAR" case-management system -- an ASP.NET
MVC app whose Kendo UI grids are backed by plain JSON endpoints under
/portal/PSC/):

  1. POST /portal/PSC/OrderSearch (a Kendo aspnetmvc-ajax grid read) with a
     Received/Order-date window (paramSet[StartDate] / paramSet[EndDate] as
     M/D/YYYY) plus the standard grid paging params. The server returns
     {"Data": [...], "Total": N}: one row per Order carrying OrderId,
     DocumentNumber (the Order number, e.g. "U-37595", "02-2020"),
     OrderDate (MS-JSON "/Date(ms)/"), Description, Synopsis and the
     associated Dockets [{MatterNumber, MatterId}].
  2. Page through pageSize=100 until all rows for the window are collected;
     fetch_all() walks month/quarter windows newest-first to the ~1990 floor.
  3. Each Order's document page is /portal/PSC/DocumentDetails?documentId=
     {OrderId}; it embeds the download link /portal/PSC/ViewFile?fileId=
     {opaque-encoded-id}. GET that link streams the born-digital Order PDF.
  4. Full text is extracted from the PDF (fitz/PyMuPDF; Tesseract OCR
     fallback for the rare image-only scan).

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
import logging
import re
import time
import calendar
import html as htmllib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.LA-PSC")

BASE = "https://lpscpubvalence.lpsc.louisiana.gov"
ORDER_SEARCH = BASE + "/portal/PSC/OrderSearch"
DOC_DETAILS = BASE + "/portal/PSC/DocumentDetails?documentId="
VIEW_FILE = BASE + "/portal/PSC/ViewFile?fileId="

# Corpus floor: the LPSC electronic order record thins out before ~1990.
FIRST_YEAR = 1990

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

VIEWFILE_RE = re.compile(r"ViewFile\?fileId=([^\"'&<>\s]+)")
MSDATE_RE = re.compile(r"/Date\((-?\d+)")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_msdate(val: str) -> Optional[str]:
    """Parse a Kendo/MS-JSON '/Date(1579154400000)/' string to ISO date."""
    if not val:
        return None
    m = MSDATE_RE.search(val)
    if not m:
        return None
    try:
        dt = datetime.fromtimestamp(int(m.group(1)) / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
    except (ValueError, OverflowError, OSError):
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


class LAPSCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": UA,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": BASE + "/",
            },
            timeout=90,
        )
        self.delay = 1.2

    # ---- low-level helpers --------------------------------------------------

    def _order_search(self, start: str, end: str, page: int, page_size: int = 100) -> Optional[dict]:
        """POST the OrderSearch Kendo grid read for a date window / page."""
        data = {
            "sort": "",
            "page": str(page),
            "pageSize": str(page_size),
            "group": "",
            "filter": "",
            "paramSet[OrderNumber]": "",
            "paramSet[FullText]": "",
            "paramSet[DocketNumber]": "",
            "paramSet[CompanyName]": "",
            "paramSet[StartDate]": start,
            "paramSet[EndDate]": end,
        }
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                r = self.http.post(
                    ORDER_SEARCH, data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"},
                )
                if r.status_code == 200:
                    try:
                        return r.json()
                    except ValueError:
                        logger.warning(f"OrderSearch {start}..{end} p{page}: non-JSON response")
                else:
                    logger.warning(f"OrderSearch {start}..{end} p{page}: HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"OrderSearch {start}..{end} p{page} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return None

    def _file_id(self, order_id) -> Optional[str]:
        """Resolve an OrderId to its ViewFile fileId via the DocumentDetails page."""
        try:
            time.sleep(self.delay)
            r = self.http.get(DOC_DETAILS + str(order_id))
            if r.status_code != 200:
                logger.debug(f"DocumentDetails {order_id}: HTTP {r.status_code}")
                return None
            m = VIEWFILE_RE.search(r.text)
            if not m:
                return None
            return m.group(1)
        except Exception as e:
            logger.debug(f"DocumentDetails {order_id} error: {e}")
            return None

    def _download(self, file_id: str) -> Optional[bytes]:
        """Download the born-digital Order PDF for a fileId."""
        try:
            time.sleep(self.delay)
            r = self.http.get(VIEW_FILE + file_id)
            ctype = r.headers.get("Content-Type", "")
            if r.status_code == 200 and ("pdf" in ctype or r.content[:4] == b"%PDF"):
                return r.content
            logger.debug(f"ViewFile unexpected: HTTP {r.status_code} type={ctype}")
        except Exception as e:
            logger.debug(f"ViewFile error: {e}")
        return None

    @staticmethod
    def _extract_text(content: bytes) -> str:
        """Extract full text from a PDF (fitz, OCR fallback for scans)."""
        try:
            doc = fitz.open(stream=content, filetype="pdf")
        except Exception as e:
            logger.warning(f"fitz open failed: {e}")
            return ""
        text = "".join(page.get_text() for page in doc)
        if len(text.strip()) < 100:
            try:
                import pytesseract
                from PIL import Image
                ocr = []
                for page in doc:
                    pix = page.get_pixmap(dpi=200)
                    img = Image.open(io.BytesIO(pix.tobytes("png")))
                    ocr.append(pytesseract.image_to_string(img))
                text = "\n".join(ocr)
            except Exception as e:
                logger.debug(f"OCR unavailable/failed: {e}")
        doc.close()
        return clean_text(text)

    # ---- BaseScraper API ----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        seen = set()
        for start, end in _month_windows():
            page = 1
            total = None
            fetched = 0
            while True:
                result = self._order_search(start, end, page)
                if not result:
                    break
                if total is None:
                    total = result.get("Total", 0)
                    if total == 0:
                        break
                rows = result.get("Data", []) or []
                if not rows:
                    break
                for row in rows:
                    order_id = row.get("OrderId")
                    if order_id is None or order_id in seen:
                        continue
                    seen.add(order_id)
                    file_id = self._file_id(order_id)
                    if not file_id:
                        continue
                    content = self._download(file_id)
                    if not content:
                        continue
                    text = self._extract_text(content)
                    if len(text.strip()) < 100:
                        logger.debug(f"Insufficient text for order {order_id}, skipping")
                        continue
                    dockets = row.get("Dockets") or []
                    docket_nums = [
                        d.get("MatterNumber", "") for d in dockets if d.get("MatterNumber")
                    ]
                    yield {
                        "order_id": order_id,
                        "order_number": row.get("DocumentNumber", ""),
                        "description": row.get("Description", ""),
                        "synopsis": row.get("Synopsis", ""),
                        "order_date": parse_msdate(row.get("OrderDate", "")),
                        "dockets": docket_nums,
                        "text": text,
                    }
                fetched += len(rows)
                if total is not None and fetched >= total:
                    break
                page += 1
                if page > 60:  # safety cap per window
                    break

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("order_date") and raw["order_date"] >= since):
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        text = raw.get("text", "")
        if not text or len(text.strip()) < 100:
            return None
        order_no = raw.get("order_number", "")
        desc = raw.get("description", "")
        title = f"Order No. {order_no}" if order_no else "Order"
        if desc:
            title = f"{title} — {desc}"
        return {
            "_id": str(raw.get("order_id")),
            "_source": "US/LA-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": raw.get("order_date"),
            "url": DOC_DETAILS + str(raw.get("order_id")),
            "order_id": str(raw.get("order_id")),
            "order_number": order_no,
            "docket_numbers": raw.get("dockets", []),
            "synopsis": raw.get("synopsis", ""),
            "jurisdiction": "US-LA",
        }

    # ---- diagnostics --------------------------------------------------------

    def test_api(self) -> bool:
        now = datetime.now(timezone.utc)
        start = f"{now.month}/1/{now.year}"
        end = f"{now.month}/{calendar.monthrange(now.year, now.month)[1]}/{now.year}"
        result = self._order_search(start, end, 1, page_size=10)
        if not result:
            print("FAIL: OrderSearch returned nothing")
            return False
        rows = result.get("Data", []) or []
        print(f"OK: {result.get('Total')} orders total in {start}..{end}, {len(rows)} on page 1")
        for row in rows[:5]:
            oid = row.get("OrderId")
            fid = self._file_id(oid)
            print(f"  {parse_msdate(row.get('OrderDate',''))} {row.get('DocumentNumber')} "
                  f"(OrderId={oid}, fileId={'yes' if fid else 'NO'})")
        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/LA-PSC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LAPSCScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
