#!/usr/bin/env python3
"""
US/DC-PSC -- Public Service Commission of the District of Columbia -- Orders

Fetches the full text of Orders issued by the Public Service Commission of the
District of Columbia (DCPSC) resolving formal cases and dockets (electric,
natural-gas, telecommunications rate cases, certificate/merger applications,
complaints, rulemakings). Each Commission Order is a final or procedural
administrative adjudication of a specific matter = case_law. Public domain
(US government edict — the District of Columbia is a federal district).

The Order series is sequentially numbered and runs from Order No. 1 (issued
1913-03-11) through the present (~22,900 Orders), making this a deep historical
corpus of DC utility case law.

Strategy (official public e-docket JSON API — edocket.dcpsc.org, an Angular
SPA backed by /apis/api, no auth required for the public search & download):

  1. Determine the current maximum Order number by paging the public filing
     search (Filing/GetFilings, ordered by receivedDate desc) until the first
     row whose `isOrder` flag is set — its `order_number` is the newest Order.

  2. Walk Order numbers newest-first down to 1. For each number N,
     GET /apis/api/Filing/GetFilings?orderNumber=N (with paging params) returns
     the filing(s) carrying that Order. Keep the non-confidential row whose
     `isOrder` is true; it exposes `attachmentId` and `attachment` (either a
     GUID `<guid>.pdf` for modern Orders or an archived path `OA/<n>.pdf` for
     the pre-2001 scanned back-catalogue), plus receivedDate, description,
     company and docket number.

  3. GET /apis/api/Filing/download?attachId=<attachmentId>&guidFileName=<attachment>
     (public, no auth) streams the Order PDF. Full text is extracted with
     fitz/PyMuPDF; the older scanned Orders already carry an embedded (OCR'd)
     text layer, and the rare image-only page falls back to Tesseract OCR.

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
logger = logging.getLogger("legal-data-hunter.US.DC-PSC")

BASE_URL = "https://edocket.dcpsc.org"
API = f"{BASE_URL}/apis/api"
SEARCH_URL = f"{API}/Filing/GetFilings"
DOWNLOAD_URL = f"{API}/Filing/download"

# Order No. 1 was issued 1913-03-11; the series is contiguous but a few numbers
# may be missing, so we probe every number and simply skip empty responses.
FIRST_ORDER = 1
# Fallback ceiling if the "newest order" probe fails for any reason.
MAX_ORDER_FALLBACK = 23000

DOCKET_RE = re.compile(r"\b((?:FC|EA|CI|TAC|GT|PC|GD|RM)\s?\d{2,5}(?:-\d+)?)\b", re.I)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def strip_html(html: str) -> str:
    if not html:
        return ""
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = (txt.replace("&nbsp;", " ").replace("&amp;", "&")
              .replace("&#39;", "'").replace("&quot;", '"')
              .replace("&lt;", "<").replace("&gt;", ">"))
    return re.sub(r"\s+", " ", txt).strip()


def parse_date(val: str) -> str | None:
    """Turn '2026-06-01T10:20:00' or '06/01/2026' into 'YYYY-MM-DD'."""
    if not val:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", val)
    if m:
        return m.group(0)
    m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", val)
    if m:
        mm, dd, yyyy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{yyyy:04d}-{mm:02d}-{dd:02d}"
    return None


class DCPSCScraper(BaseScraper):

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
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": BASE_URL + "/",
            },
            timeout=90,
        )
        self.delay = 0.8

    # ---- low-level API helpers ---------------------------------------------

    def _get_filings(self, params: dict, retries: int = 4) -> dict:
        base = {
            "orderByColumn": "receivedDate",
            "sortBy": "desc",
            "recordsToSkip": 0,
            "recordsToShow": 5,
        }
        base.update(params)
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(SEARCH_URL, params=base)
                if resp.status_code == 200:
                    return resp.json()
                logger.debug(f"GetFilings {params} attempt {attempt+1}: "
                             f"status={resp.status_code}")
            except Exception as e:
                logger.warning(f"GetFilings {params} attempt {attempt+1} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return {}

    def _newest_order_number(self) -> int:
        """Page recent filings until the first `isOrder` row; return its number."""
        skip = 0
        for _ in range(40):  # up to 40 * 200 = 8000 recent filings
            data = self._get_filings({"recordsToSkip": skip, "recordsToShow": 200})
            rows = data.get("resultsSet") or []
            if not rows:
                break
            for r in rows:
                if r.get("isOrder") and r.get("order_number"):
                    try:
                        return int(str(r["order_number"]).strip())
                    except ValueError:
                        continue
            skip += 200
        logger.warning("Could not determine newest Order number; using fallback.")
        return MAX_ORDER_FALLBACK

    @staticmethod
    def _order_row(data: dict) -> dict | None:
        """Return the first usable, non-confidential Order row from a response."""
        for r in data.get("resultsSet") or []:
            if not r.get("isOrder"):
                continue
            att = r.get("attachment")
            if not att or str(att).strip().lower() == "confidential":
                continue
            if r.get("isConfidential"):
                continue
            if not r.get("attachmentId"):
                continue
            return r
        return None

    # ---- PDF text extraction -----------------------------------------------

    def _ocr_pdf(self, doc) -> str:
        try:
            import pytesseract
            from PIL import Image
        except Exception as e:
            logger.debug(f"OCR unavailable (pytesseract/PIL): {e}")
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
            if len(text) < 200:  # image-only scan -> OCR
                ocr = clean_text(self._ocr_pdf(doc))
                if len(ocr) > len(text):
                    text = ocr
            return text
        finally:
            doc.close()

    def _download(self, attach_id, attachment: str, retries: int = 4) -> bytes | None:
        # Preserve the literal '/' in archived 'OA/<n>.pdf' names.
        gfn = urllib.parse.quote(str(attachment), safe="/")
        url = f"{DOWNLOAD_URL}?attachId={urllib.parse.quote(str(attach_id))}&guidFileName={gfn}"
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
                    return resp.content
                logger.debug(f"download {url} status={resp.status_code}")
            except Exception as e:
                logger.warning(f"download attempt {attempt+1} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return None

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_bytes = self._download(raw["attachment_id"], raw["attachment"])
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 120:
            logger.debug(f"Short/empty text for Order {raw.get('order_number')}")
            return None

        onum = raw["order_number"]
        docket = raw.get("docket_number")
        if not docket or docket.lower() in ("not available", "none"):
            dm = DOCKET_RE.search(text[:3000])
            docket = dm.group(1).upper().replace(" ", "") if dm else None

        title = f"DCPSC Order No. {onum}"
        if raw.get("date"):
            title += f" ({raw['date']})"
        desc = strip_html(raw.get("description") or "")
        if desc:
            title += f" — {desc[:160]}"

        return {
            "_id": f"US/DC-PSC/order-{onum}",
            "_source": "US/DC-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "order_number": str(onum),
            "docket_number": docket,
            "doc_type": "Order",
            "title": title,
            "description": desc or None,
            "company": (raw.get("company") or None),
            "text": text,
            "url": BASE_URL + "/",
            "pdf_url": (
                f"{DOWNLOAD_URL}?attachId={raw['attachment_id']}"
                f"&guidFileName={urllib.parse.quote(str(raw['attachment']), safe='/')}"
            ),
            "date": raw.get("date"),
        }

    # ---- iteration ----------------------------------------------------------

    def _raw_for_order(self, n: int) -> dict | None:
        data = self._get_filings({"orderNumber": n})
        if not data or not data.get("totalRecords"):
            return None
        row = self._order_row(data)
        if not row:
            return None
        return {
            "order_number": str(row.get("order_number") or n).strip(),
            "attachment": row.get("attachment"),
            "attachment_id": row.get("attachmentId"),
            "date": parse_date(row.get("receivedDate") or row.get("rDate")),
            "description": row.get("description"),
            "company": (row.get("companyOrIndividual") or "").strip() or None,
            "docket_number": (row.get("docketNumber") or "").strip() or None,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        top = self._newest_order_number()
        logger.info(f"Newest Order number: {top}; walking down to {FIRST_ORDER}.")
        for n in range(top, FIRST_ORDER - 1, -1):
            raw = self._raw_for_order(n)
            if raw:
                yield raw

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing DCPSC e-docket API (Orders)...")
        try:
            top = self._newest_order_number()
            logger.info(f"  Newest Order number: {top}")
            rec = None
            for n in range(top, top - 25, -1):
                raw = self._raw_for_order(n)
                if not raw:
                    continue
                rec = self.normalize(raw)
                if rec:
                    break
            if rec and len(rec["text"]) > 120:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"Order {rec['order_number']}, docket={rec.get('docket_number')}, "
                    f"date={rec.get('date')})"
                )
                logger.info("API test PASSED")
                return True
            logger.error("  Full-text extraction failed or too short")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DC-PSC bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DCPSCScraper()

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
