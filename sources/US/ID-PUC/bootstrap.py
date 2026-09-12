#!/usr/bin/env python3
"""
US/ID-PUC -- Idaho Public Utilities Commission Orders

Fetches the full text of Orders issued by the Idaho Public Utilities
Commission (IPUC) adjudicating utility cases — electric, natural gas,
water, telecommunications and railroad matters: rate cases, certificates
of public convenience and necessity, integrated resource plans, PURPA /
avoided-cost dockets, fuel and power-cost adjustments, tariff filings and
rulemaking. Each Commission Order is an administrative adjudication /
edict of a specific case = case_law. Public domain (US state government
edict).

Strategy (Internet Archive / Wayback Machine):

  puc.idaho.gov firewall-drops all traffic from non-Idaho / datacenter
  vantages (no ICMP, TCP 443 filtered; HTTP 000 from every build vantage
  and from Anthropic's fetch vantage as of 2026-07-23). The corpus is
  therefore read from the Internet Archive, which has a substantial crawl
  of the Commission's public Fileroom.

  1. The IPUC serves order PDFs at two historical path schemes:
       new:  /Fileroom/PublicFiles/{TYPE}/{UTIL}/{CASESEG}/OrdNotc/{file}.pdf
       old:  /fileroom/cases/{type}/{util}/{caseseg}/ordnotc/{file}.pdf
     where {CASESEG} encodes the case number (e.g. IPCE2603 => IPC-E-26-03,
     ATLE0201 => ATL-E-02-01) and {file} is prefixed with the service date
     (YYYYMMDD) followed by the document title (e.g. "Final Order No 37018").

  2. fetch_all() enumerates every archived ``.../OrdNotc/*.pdf`` snapshot
     via the Wayback CDX API (case-insensitive urlkey filter, statuscode
     200, collapsed to one snapshot per URL), keeps only genuine Orders
     (filename carries an "Order No NNNNN"; procedural "Notice of ..."
     documents are dropped) and de-duplicates on (case number, order
     number), preferring the final/amended order over a workshop notice and
     the newer Fileroom scheme.

  3. normalize() downloads the raw archived PDF
     (``https://web.archive.org/web/{ts}id_/{original}``) and extracts full
     text via fitz/PyMuPDF (Tesseract OCR fallback for the rare scanned
     order). Date and order number are parsed from the filename; the case
     number is reconstructed from the path segment.

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
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

import fitz  # PyMuPDF
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.ID-PUC")

CDX_URL = "http://web.archive.org/cdx/search/cdx"
WB_RAW = "https://web.archive.org/web/{ts}id_/{url}"

# Match order documents in either path scheme (case-insensitive urlkey).
CDX_PARAMS = {
    "url": "puc.idaho.gov",
    "matchType": "domain",
    "filter": ["urlkey:.*ordnotc.*\\.pdf", "statuscode:200"],
    "collapse": "urlkey",
    "output": "text",
    "fl": "original,timestamp",
}

# "Order No 37018" / "Order_No_37018" / "ORDER NO. 29059".
ORDER_NO_RE = re.compile(r"order[\s_]*no\.?[\s_]*(\d{3,6})", re.I)
# Filename date prefix, YYYYMMDD.
DATE_PREFIX_RE = re.compile(r"^\s*(\d{8})[_\s-]*")
# A genuine order title (after the date prefix) starts with these.
ORDER_START_RE = re.compile(
    r"^\s*(FINAL|AMENDED|CORRECTED|SUPPLEMENTAL|SECOND|THIRD|FOURTH|FIFTH|"
    r"INTERIM|CLARIFYING|ERRATA)?\s*ORDER",
    re.I,
)
NOTICE_START_RE = re.compile(r"^\s*NOTICE", re.I)
# Case segment => IPC-E-26-03 (letters+2yr+2seq+optional suffix).
CASE_SEG_RE = re.compile(r"^([A-Z]+)(\d{2})(\d{2})([A-Z0-9]*)$")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
# IPUC orders carry "Service Date\n{Month DD, YYYY}" near the top; fall back to
# "DATED/ENTERED/ISSUED this Nth day of {Month} YYYY" or a bare "Month DD, YYYY".
BODY_DATE_RE = re.compile(
    r"(?:Service\s+Date|Dated|Entered|Issued|Adopted)\s*:?\s*"
    r"(January|February|March|April|May|June|July|August|September|October|November|December)"
    r"\s+(\d{1,2}),?\s+(\d{4})",
    re.I,
)


def body_date_to_iso(text: str) -> str | None:
    m = BODY_DATE_RE.search(text or "")
    if not m:
        return None
    mon = MONTHS.get(m.group(1).lower())
    if not mon:
        return None
    day, year = int(m.group(2)), int(m.group(3))
    if not (1 <= day <= 31 and 1980 <= year <= 2100):
        return None
    return f"{year:04d}-{mon:02d}-{day:02d}"


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def reconstruct_case(case_seg: str) -> str:
    """IPCE2603 -> IPC-E-26-03 ; ATLE0201 -> ATL-E-02-01."""
    m = CASE_SEG_RE.match(case_seg)
    if m:
        letters, yy, nn, suf = m.groups()
        if len(letters) >= 2:
            comp, typ = letters[:-1], letters[-1]
            base = f"{comp}-{typ}-{yy}-{nn}"
            return base + (f"-{suf}" if suf else "")
    return case_seg


def date_prefix_to_iso(filename: str) -> str | None:
    m = DATE_PREFIX_RE.match(filename)
    if not m:
        return None
    s = m.group(1)
    yr, mo, da = int(s[:4]), int(s[4:6]), int(s[6:8])
    if not (1980 <= yr <= 2100 and 1 <= mo <= 12 and 1 <= da <= 31):
        return None
    return f"{yr:04d}-{mo:02d}-{da:02d}"


class IDPUCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            }
        )
        self.delay = 0.4

    # ---- low-level fetch ----------------------------------------------------

    def _get(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.session.get(url, timeout=90)
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

    # ---- CDX enumeration ----------------------------------------------------

    def _cdx_rows(self) -> list[tuple[str, str]]:
        for attempt in range(5):
            try:
                resp = self.session.get(CDX_URL, params=CDX_PARAMS, timeout=180)
                if resp.status_code == 200:
                    rows = []
                    for line in resp.text.splitlines():
                        parts = line.split()
                        if len(parts) >= 2:
                            rows.append((parts[0], parts[1]))
                    return rows
                logger.warning(f"CDX HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"CDX error (attempt {attempt + 1}): {e}")
            time.sleep(3 * (attempt + 1))
        return []

    def _enumerate_orders(self) -> list[dict]:
        """Return deduplicated order records (best snapshot per case+order)."""
        rows = self._cdx_rows()
        logger.info(f"CDX returned {len(rows)} archived OrdNotc PDF snapshots")
        best: dict[tuple[str, str], dict] = {}
        for orig, ts in rows:
            fn = urllib.parse.unquote(orig.rsplit("/", 1)[-1])
            parts = orig.split("/")
            low = [p.lower() for p in parts]
            try:
                i = low.index("ordnotc")
            except ValueError:
                continue
            case_seg = parts[i - 1].upper()
            body = DATE_PREFIX_RE.sub("", fn)
            if NOTICE_START_RE.match(body):
                continue  # procedural notice, not an order
            m = ORDER_NO_RE.search(fn)
            if not m:
                continue
            order_no = m.group(1)
            key = (case_seg, order_no)
            score = 0
            if ORDER_START_RE.match(body):
                score += 2
            if "publicfiles" in orig.lower():
                score += 1
            cur = best.get(key)
            cand = {
                "orig": orig,
                "ts": ts,
                "score": score,
                "case_seg": case_seg,
                "case_number": reconstruct_case(case_seg),
                "order_number": order_no,
                "filename": fn,
                "date": date_prefix_to_iso(fn),
            }
            if cur is None or (score, ts) > (cur["score"], cur["ts"]):
                best[key] = cand
        records = sorted(
            best.values(), key=lambda r: (r["date"] or "", r["case_number"], r["order_number"])
        )
        logger.info(f"{len(records)} unique Orders after dedup")
        return records

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
        wb = WB_RAW.format(ts=raw["ts"], url=raw["orig"])
        pdf = self._get(wb)
        if not pdf or pdf[:5] != b"%PDF-":
            logger.debug(f"No PDF for {raw['case_number']} order {raw['order_number']}")
            return None
        text = self._extract_pdf(pdf)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for {raw['case_number']} order "
                f"{raw['order_number']} ({len(text) if text else 0} chars)"
            )
            return None

        case = raw["case_number"]
        order_no = raw["order_number"]
        date = raw.get("date") or body_date_to_iso(text)
        title = f"IPUC Case {case} — Order No. {order_no}"

        # Original (canonical) source URL, un-cased to the live host path.
        orig_url = raw["orig"]

        return {
            "_id": f"US/ID-PUC/{raw['case_seg']}_{order_no}",
            "_source": "US/ID-PUC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": case,
            "order_number": order_no,
            "title": title,
            "text": text,
            "url": orig_url,
            "archive_url": wb,
            "date": date,
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Idaho PUC order enumeration via Wayback CDX...")
        try:
            records = self._enumerate_orders()
            if len(records) < 100:
                logger.error(f"  Too few orders enumerated: {len(records)}")
                return False
            logger.info(f"  Enumerated {len(records)} orders")
            rec = None
            for r in records[-20:]:  # recent orders extract cleanly
                rec = self.normalize(r)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"case={rec['case_number']}, order={rec['order_number']}, "
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
        yield from self._enumerate_orders()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        since = as_date_str(since)  # update() passes a datetime; #1512
        for raw in self._enumerate_orders():
            if not since or (raw.get("date") or "") >= since:
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/ID-PUC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IDPUCScraper()

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
