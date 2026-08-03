#!/usr/bin/env python3
"""
US/CT-PURA -- Connecticut Public Utilities Regulatory Authority (PURA) Final Decisions

Fetches the full text of Final Decisions issued by the Connecticut Public
Utilities Regulatory Authority (PURA, formerly DPUC) adjudicating utility
dockets across the electric, gas, water, telecommunications, CATV, CBYD and
related industries (rate cases, certificate/qualification applications,
complaints, restructuring, renewables and procedural + final decisions). Each
Final Decision is an administrative adjudication of a specific docket by the
Authority = case_law. Public domain (US state government edict).

Strategy (public "Final Decision Database", a Lotus Domino app at
www.dpuc.state.ct.us/FINALDEC.NSF):
  1. DISCOVERY: Domino exposes the categorized view "UtilityByDecisionDateView"
     as machine-readable XML via `?ReadViewEntries`. A plain call lists the ~20
     industry categories with their descendant counts; per-category pagination
     (`&RestrictToCategory={cat}&Start={n}&Count={c}`) walks every leaf
     document (Domino caps a single response at 1000 entries, so we page). Each
     leaf viewentry carries the document `unid` plus columns AbbrevDckTitle
     (title), Decision_Date (YYYYMMDD) and DocketNumber. ~24,000 decisions.
  2. FULL TEXT: normalize() opens the Domino document
     (/FINALDEC.NSF/0/{unid}?OpenDocument), parses the attached born-digital
     decision PDF href (.../$FILE/{name}.pdf), downloads it and extracts the
     full text via fitz/PyMuPDF (Tesseract OCR fallback for the rare scan).

No auth. The Domino host serves ReadViewEntries and the $FILE attachments to
anonymous clients.

Usage:
  python bootstrap.py bootstrap            # Full pull (all Final Decisions)
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
from urllib.parse import quote
from html import unescape

import fitz  # PyMuPDF
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CT-PURA")

BASE = "https://www.dpuc.state.ct.us"
DB = "/FINALDEC.NSF"
VIEW = "UtilityByDecisionDateView"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

PAGE = 200  # entries per ReadViewEntries page (Domino caps at 1000)

VIEWENTRY_RE = re.compile(r"<viewentry\b(.*?)</viewentry>", re.S)
UNID_RE = re.compile(r'unid="([0-9A-Fa-f]+)"')
FILE_PDF_RE = re.compile(r'href="([^"]*\$[Ff][Ii][Ll][Ee]/[^"]+\.pdf)"')


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _entry_field(block: str, name: str) -> str:
    """Pull the <text>/<datetime> value of an <entrydata name="{name}">."""
    m = re.search(
        r'<entrydata[^>]*name="' + re.escape(name) + r'"[^>]*>\s*'
        r'<(?:text|datetime)[^>]*>([^<]*)</(?:text|datetime)>',
        block,
        re.S,
    )
    return unescape(m.group(1).strip()) if m else ""


def _norm_date(raw: str) -> str | None:
    """Domino Decision_Date is 'YYYYMMDD' or 'YYYYMMDDThhmmss,ss-04'."""
    if not raw:
        return None
    digits = re.match(r"(\d{4})(\d{2})(\d{2})", raw)
    if not digits:
        return None
    y, mo, d = digits.groups()
    if not ("1900" <= y <= "2100" and "01" <= mo <= "12" and "01" <= d <= "31"):
        return None
    return f"{y}-{mo}-{d}"


class CTPURAScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA})
        self.session.verify = False
        self.delay = 0.3
        self.ckpt_path = Path(source_dir) / "data" / "ct_pura_checkpoint.json"

    # ---- Domino plumbing ----------------------------------------------

    def _read_view(self, params: str, retries: int = 4) -> str:
        url = f"{BASE}{DB}/{VIEW}?ReadViewEntries&{params}"
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90)
                if r.status_code == 200:
                    return r.text
                logger.warning(f"ReadViewEntries HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"ReadViewEntries error (try {attempt+1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _categories(self) -> list:
        """List industry categories (text + descendant count)."""
        xml = self._read_view("Count=100")
        cats = []
        for blk in VIEWENTRY_RE.findall(xml):
            if 'category="true"' not in blk:
                continue
            m = re.search(r'descendants="(\d+)"', blk)
            t = re.search(r"<text>([^<]*)</text>", blk)
            if m and t and int(m.group(1)) > 0:
                cats.append((unescape(t.group(1)).strip(), int(m.group(1))))
        return cats

    def _leaves(self, xml: str) -> list:
        out = []
        for blk in VIEWENTRY_RE.findall(xml):
            u = UNID_RE.search(blk)
            if not u:  # category row
                continue
            out.append({
                "unid": u.group(1).upper(),
                "title": _entry_field(blk, "AbbrevDckTitle"),
                "date": _norm_date(_entry_field(blk, "Decision_Date")),
                "docket": _entry_field(blk, "DocketNumber"),
            })
        return out

    def _category_leaves(self, cat: str, start: int) -> list:
        params = f"RestrictToCategory={quote(cat, safe='')}&Start={start}&Count={PAGE}"
        xml = self._read_view(params)
        if not xml:
            return []
        return self._leaves(xml)

    # ---- Checkpoint ----------------------------------------------------

    def _load_ckpt(self) -> dict:
        try:
            return json.loads(self.ckpt_path.read_text())
        except Exception:
            return {"done_cats": [], "cat": None, "start": 1}

    def _save_ckpt(self, ck: dict):
        try:
            self.ckpt_path.parent.mkdir(parents=True, exist_ok=True)
            self.ckpt_path.write_text(json.dumps(ck))
        except Exception as e:
            logger.debug(f"checkpoint save failed: {e}")

    # ---- PDF text ------------------------------------------------------

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

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
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

    def _get(self, url: str, retries: int = 3, expect_pdf: bool = False):
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.session.get(url, timeout=90, allow_redirects=True)
                if r.status_code == 200:
                    if expect_pdf and r.content[:5] != b"%PDF-":
                        return None
                    return r
                if r.status_code == 404:
                    return None
            except Exception as e:
                logger.warning(f"GET error (try {attempt+1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _pdf_url_for(self, unid: str) -> str | None:
        r = self._get(f"{BASE}{DB}/0/{unid}?OpenDocument")
        if not r:
            return None
        m = FILE_PDF_RE.search(r.text)
        if not m:
            return None
        href = unescape(m.group(1))
        if href.startswith("http"):
            return href
        if href.startswith("/"):
            return BASE + href
        return f"{BASE}{DB}/0/{unid}/" + href.lstrip("/")

    # ---- Framework hooks -----------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_url = self._pdf_url_for(raw["unid"])
        if not pdf_url:
            return None
        r = self._get(pdf_url, expect_pdf=True)
        if not r:
            return None
        text = self._extract_pdf_text(r.content)
        if not text or len(text) < 200:
            logger.debug(f"Short/empty text for {raw['unid']}")
            return None

        docket = (raw.get("docket") or "").strip()
        title = (raw.get("title") or "").strip()
        if docket and title:
            full_title = f"PURA Final Decision — Docket {docket}: {title}"
        elif docket:
            full_title = f"PURA Final Decision — Docket {docket}"
        elif title:
            full_title = f"PURA Final Decision — {title}"
        else:
            full_title = f"PURA Final Decision — {raw['unid']}"

        return {
            "_id": f"US/CT-PURA/{raw['unid']}",
            "_source": "US/CT-PURA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "docket": docket or None,
            "title": full_title,
            "text": text,
            "url": f"{BASE}{DB}/0/{raw['unid']}?OpenDocument",
            "pdf_url": pdf_url,
            "date": raw.get("date"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        cats = self._categories()
        logger.info(f"{len(cats)} industry categories, "
                    f"~{sum(c for _, c in cats)} decision rows")
        ck = self._load_ckpt()
        done = set(ck.get("done_cats", []))
        seen = set()
        for cat, count in cats:
            if cat in done:
                continue
            start = ck["start"] if ck.get("cat") == cat else 1
            logger.info(f"category '{cat}' ({count} rows) from start={start}")
            while True:
                leaves = self._category_leaves(cat, start)
                if not leaves:
                    break
                for lf in leaves:
                    if lf["unid"] in seen:
                        continue
                    seen.add(lf["unid"])
                    yield lf
                start += PAGE
                self._save_ckpt(
                    {"done_cats": sorted(done), "cat": cat, "start": start}
                )
                if len(leaves) < PAGE:
                    break
            done.add(cat)
            self._save_ckpt({"done_cats": sorted(done), "cat": None, "start": 1})

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw

    def test_api(self) -> bool:
        logger.info("Testing CT PURA Final Decision Database (Domino)...")
        try:
            cats = self._categories()
            if not cats:
                logger.error("  No categories found")
                return False
            logger.info(f"  {len(cats)} categories, "
                        f"~{sum(c for _, c in cats)} rows")
            # Pull leaves from the largest category and normalize one.
            cat = max(cats, key=lambda c: c[1])[0]
            leaves = self._category_leaves(cat, 1)
            logger.info(f"  '{cat}' page 1: {len(leaves)} leaves")
            rec = None
            for lf in leaves[:15]:
                rec = self.normalize(lf)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"docket {rec.get('docket')}, date={rec.get('date')})"
                )
                logger.info("API test PASSED")
                return True
            logger.error("  Could not extract a full-text decision")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CT-PURA bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"]
    )
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = CTPURAScraper()

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
