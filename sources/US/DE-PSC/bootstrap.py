#!/usr/bin/env python3
"""
US/DE-PSC -- Delaware Public Service Commission Orders

Fetches the full text of Orders issued by the Delaware Public Service
Commission (PSC) adjudicating utility dockets — electric, natural gas,
water, wastewater, cable and telecommunications rate cases, certificates
of public convenience and necessity, § 215 filings, renewable / community
energy applications, tariff filings and rulemaking. Each Commission Order
is an administrative adjudication / edict of a specific docket = case_law.
Public domain (US state government edict).

Strategy (official DelaFile e-filing system, delafile.delaware.gov):

  1. Dockets are addressed ``YY-NNNN`` (e.g. 21-0436, 24-0365). DelaFile's
     "advanced search by case number" resolver
     ``AdvancedSearch/AdvancedSearchDocket.aspx?CNo={base64(YY-NNNN)}``
     returns the docket page link carrying the internal ``MatterId`` GUID
     (a non-existent docket yields "Sorry, no results were found" with no
     DocketPage link). fetch_all() sweeps the ``YY-NNNN`` id space per year
     with gap tolerance, and also yields a curated seed list first so that
     ``--sample`` mode finds Orders quickly.

  2. For each docket, ``CaseManagement/DocketPage.aspx?...&MatterNo={mn}
     &MatterId={guid}`` renders the docket sheet with a "Supporting
     Documents" grid (``grdDocumentDetails``). Every row whose Document
     Type is "Order" is a Commission Order; its attachment link embeds a
     FileNet document GUID (``ViewFileNetDocument.aspx?Id={guid}``). The
     docket sheet also carries the caption / utility type / company for
     metadata.

  3. ``CaseManagement/ViewFileNetDocument.aspx?Id={guid}`` serves the raw
     ``application/pdf``. normalize() downloads it and extracts full text
     via fitz/PyMuPDF (Tesseract OCR fallback for the scanned substantive
     orders — many procedural orders are born-digital and extract cleanly).

Usage:
  python bootstrap.py bootstrap            # Full pull (all Orders)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import base64
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
logger = logging.getLogger("legal-data-hunter.US.DE-PSC")

BASE = "https://delafile.delaware.gov"
ADV_URL = f"{BASE}/AdvancedSearch/AdvancedSearchDocket.aspx"
DOCKET_URL = f"{BASE}/CaseManagement/DocketPage.aspx"
VIEWDOC_URL = f"{BASE}/CaseManagement/ViewFileNetDocument.aspx"

# DelaFile holds dockets from 2016 onward (the e-filing era). Sweep those
# years; per-year sequence numbers are 4 digits and can exceed 1300.
SWEEP_YEARS = [f"{y:02d}" for y in range(16, 27)]
SWEEP_MAX_SEQ = 1600   # max sequence number probed per year
SWEEP_GAP = 45         # stop a year's run after this many consecutive misses

# Curated seed dockets known to carry Orders (rate cases / § 215 filings).
# Yielded first so --sample finds full-text Orders without a long sweep.
SEED_DOCKETS = [
    "21-0436", "24-0365", "19-0181", "21-1088", "20-0331",
    "22-0897", "23-0257", "25-0700", "20-0640", "18-1051",
    "17-0977", "16-0649",
]

GUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
VF_ID_RE = re.compile(r"ViewFileNetDocument\.aspx\?Id=([0-9a-f-]{36})", re.I)
MATTERID_RE = re.compile(r"MatterId=([0-9a-f-]{36})", re.I)
MDY_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
BODY_DATE_RE = re.compile(
    r"(?:Entered|Dated|Issued|Adopted|DONE\s+AND\s+ORDERED)\s*(?:this)?\s*:?\s*"
    r"(?:the\s+)?(\d{1,2})(?:st|nd|rd|th)?\s+(?:day\s+of\s+)?"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r",?\s+(\d{4})",
    re.I,
)
BODY_DATE_RE2 = re.compile(
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
    if m:
        mon = MONTHS.get(m.group(2).lower())
        day, year = int(m.group(1)), int(m.group(3))
        if mon and 1 <= day <= 31 and 1900 <= year <= 2100:
            return f"{year:04d}-{mon:02d}-{day:02d}"
    m = BODY_DATE_RE2.search(text or "")
    if m:
        mon = MONTHS.get(m.group(1).lower())
        day, year = int(m.group(2)), int(m.group(3))
        if mon and 1 <= day <= 31 and 1900 <= year <= 2100:
            return f"{year:04d}-{mon:02d}-{day:02d}"
    return None


class DEPSCScraper(BaseScraper):

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

    # ---- docket resolution / parsing ---------------------------------------

    def _resolve_matter_id(self, docket: str) -> str | None:
        """Return the internal MatterId GUID for a YY-NNNN docket, or None."""
        cno = base64.b64encode(docket.encode()).decode()
        html = self._get_text(f"{ADV_URL}?CNo={cno}")
        if "DocketPage.aspx" not in html:
            return None
        m = MATTERID_RE.search(html)
        return m.group(1) if m else None

    def _docket_meta_and_orders(self, docket: str, matter_id: str) -> tuple[dict, list[dict]]:
        """Return (docket-level metadata, list of Order rows) for a docket."""
        url = (
            f"{DOCKET_URL}?from=docket&ViewDocketPage=ViewDocketPage"
            f"&MatterNo={docket}&MatterId={matter_id}&Type=Docket"
        )
        html = self._get_text(url)
        soup = BeautifulSoup(html, "html.parser")

        def lbl(lid: str) -> str:
            e = soup.find(id=lid)
            return e.get_text(" ", strip=True) if e else ""

        meta = {
            "utility_type": lbl("ctl00_cphMaster_lblUtilityType"),
            "company": lbl("ctl00_cphMaster_lblCompanyName"),
            "docket_type": lbl("ctl00_cphMaster_lblDocketType"),
            "caption": lbl("ctl00_cphMaster_lblDesc"),
            "filing_date": mdy_to_iso(lbl("ctl00_cphMaster_lblbFilingDate")),
            "page_url": url,
        }

        orders: list[dict] = []
        grid = soup.find(id="ctl00_cphMaster_grdDocumentDetails")
        if not grid:
            return meta, orders
        for row in grid.find_all("tr"):
            tds = row.find_all("td")
            if len(tds) < 7:
                continue
            dtype = tds[1].get_text(" ", strip=True)
            if dtype.strip().lower() != "order":
                continue
            a = row.find("a", onclick=VF_ID_RE)
            guid = None
            if a and a.get("onclick"):
                gm = VF_ID_RE.search(a.get("onclick"))
                guid = gm.group(1) if gm else None
            if not guid:
                continue
            orders.append(
                {
                    "docket": docket,
                    "item_no": tds[0].get_text(strip=True),
                    "doc_guid": guid,
                    "filing_date": mdy_to_iso(tds[2].get_text(" ", strip=True)),
                    "description": tds[5].get_text(" ", strip=True),
                    "_meta": meta,
                }
            )
        return meta, orders

    def _order_rows(self, docket: str) -> list[dict]:
        mid = self._resolve_matter_id(docket)
        if not mid:
            return []
        _, orders = self._docket_meta_and_orders(docket, mid)
        return orders

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

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        guid = raw["doc_guid"]
        pdf = self._get(f"{VIEWDOC_URL}?Id={guid}")
        if not pdf or pdf[:5] != b"%PDF-":
            return None
        text = self._extract_pdf(pdf)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for {raw['docket']} item {raw.get('item_no')} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        meta = raw.get("_meta") or {}
        docket = raw["docket"]
        desc = (raw.get("description") or "").strip()

        # Order number, if present in the row description or the body.
        order_no = None
        om = re.search(r"Order\s+No\.?\s*([0-9]+)", desc, re.I)
        if not om:
            om = re.search(r"Order\s+No\.?\s*([0-9]+)", text[:2000], re.I)
        if om:
            order_no = om.group(1)

        date = raw.get("filing_date") or body_date_to_iso(text)

        title = f"DE PSC Docket {docket}"
        if order_no:
            title += f" — Order No. {order_no}"
        elif desc:
            title += f" — {desc[:120]}"
        else:
            title += " — Order"

        return {
            "_id": f"US/DE-PSC/{docket}_{guid}",
            "_source": "US/DE-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "docket_number": docket,
            "order_number": order_no,
            "utility_type": meta.get("utility_type") or None,
            "company": meta.get("company") or None,
            "docket_type": meta.get("docket_type") or None,
            "caption": meta.get("caption") or None,
            "description": desc or None,
            "title": title,
            "text": text,
            "url": meta.get("page_url") or f"{VIEWDOC_URL}?Id={guid}",
            "pdf_url": f"{VIEWDOC_URL}?Id={guid}",
            "date": date,
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing DelaFile docket resolver + document viewer...")
        try:
            rows = self._order_rows("21-0436")
            if not rows:
                logger.error("  No Order rows for test docket 21-0436")
                return False
            logger.info(f"  Docket 21-0436: {len(rows)} Order rows")
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
        seen_orders: set[str] = set()
        emitted_dockets: set[str] = set()

        def emit(docket: str) -> Generator[dict, None, None]:
            if docket in emitted_dockets:
                return
            emitted_dockets.add(docket)
            for r in self._order_rows(docket):
                key = f"{r['docket']}_{r['doc_guid']}"
                if key in seen_orders:
                    continue
                seen_orders.add(key)
                yield r

        # Seed dockets first (fast path for --sample).
        for d in SEED_DOCKETS:
            yield from emit(d)

        # Id-space sweep across DelaFile years.
        for yy in SWEEP_YEARS:
            misses = 0
            for seq in range(1, SWEEP_MAX_SEQ + 1):
                if misses >= SWEEP_GAP:
                    break
                docket = f"{yy}-{seq:04d}"
                if docket in emitted_dockets:
                    misses = 0
                    continue
                mid = self._resolve_matter_id(docket)
                if not mid:
                    misses += 1
                    continue
                misses = 0
                emitted_dockets.add(docket)
                _, orders = self._docket_meta_and_orders(docket, mid)
                for r in orders:
                    key = f"{r['docket']}_{r['doc_guid']}"
                    if key in seen_orders:
                        continue
                    seen_orders.add(key)
                    yield r

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DE-PSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DEPSCScraper()

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
