#!/usr/bin/env python3
"""
US/SD-PUC -- South Dakota Public Utilities Commission Orders

Fetches the full text of Orders issued by the South Dakota Public Utilities
Commission (PUC) adjudicating utility dockets across the electric, natural-gas,
combined gas/electric and telecommunications industries. Each Order is an
administrative adjudication of a specific docket by the Commission = case_law.
Public domain (US state government edict).

Strategy (official docket archive, puc.sd.gov):
  1. The PUC publishes every docket at
     https://puc.sd.gov/Dockets/{Type}/{YYYY}/{DOCKET}.aspx where Type is one
     of Electric, NaturalGas, GasElectric, Telecom. Each type's landing page
     (/Dockets/{Type}/default.aspx) lists year sub-pages
     ({YYYY}/default.aspx), and each year page lists that year's docket pages
     ({DOCKET}.aspx, e.g. EL24-001.aspx, NG24-003.aspx, TC24-002.aspx,
     GE24-001.aspx).
  2. Each docket page carries an "Orders:" section — an <ul> of
     <li><a href="/commission/dockets/.../X.pdf">MM/DD/YY - description</a></li>
     links to the born-digital Order PDFs (distinct from the "Filed Documents"
     section, which holds party filings, exhibits and data requests).
  3. fetch_all() walks the four docket types, newest year first, yields one raw
     dict per Order PDF. normalize() downloads the PDF and extracts the full
     text (fitz/PyMuPDF; Tesseract OCR fallback for the rare image-only scan).

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.SD-PUC")

BASE_URL = "https://puc.sd.gov"

# The four docket industries the PUC archives.
DOCKET_TYPES = ["Electric", "NaturalGas", "GasElectric", "Telecom"]

# Corpus floor: docket year folders run from 2000.
FIRST_YEAR = 2000

# Year sub-page links on a type landing page: e.g. href="2024/default.aspx"
YEAR_RE = re.compile(r'href="(\d{4})/default\.aspx"', re.I)
# Docket page links on a year page: e.g. href="EL24-001.aspx"
DOCKET_RE = re.compile(r'href="([A-Z]{2}\d{2}-\d+)\.aspx"', re.I)
# <a href="...pdf">MM/DD/YY - description</a> inside the Orders block.
ORDER_LINK_RE = re.compile(
    r'href="([^"]+?\.pdf)"[^>]*>\s*(.*?)</a>', re.I | re.S
)
# The docket caption: "EL24-001 - In the Matter of ..."
CAPTION_RE = re.compile(r"\)\s*ORDER", re.I)
DATE_PREFIX_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{2,4})")


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted PDF text."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def strip_tags(html: str) -> str:
    txt = re.sub(r"<[^>]+>", " ", html)
    txt = (
        txt.replace("&amp;", "&")
        .replace("&nbsp;", " ")
        .replace("&diams;", " ")
        .replace("&#9830;", " ")
    )
    return re.sub(r"\s+", " ", txt).strip()


def parse_mdy(m: int, d: int, y: int) -> str | None:
    try:
        if y < 100:
            y += 2000 if y < 70 else 1900
        if not (1 <= m <= 12 and 1 <= d <= 31 and 1990 <= y <= 2100):
            return None
        return f"{y:04d}-{m:02d}-{d:02d}"
    except Exception:
        return None


class SDPUCScraper(BaseScraper):

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
        self.delay = 0.8

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

    def _get_text(self, url: str, retries: int = 4) -> str:
        data = self._get(url, retries=retries)
        return data.decode("utf-8", "replace") if data else ""

    def _list_years(self, dtype: str) -> list[int]:
        html = self._get_text(f"{BASE_URL}/Dockets/{dtype}/default.aspx")
        years = sorted(
            {int(m.group(1)) for m in YEAR_RE.finditer(html) if int(m.group(1)) >= FIRST_YEAR},
            reverse=True,
        )
        return years

    def _list_dockets(self, dtype: str, year: int) -> list[str]:
        html = self._get_text(f"{BASE_URL}/Dockets/{dtype}/{year}/default.aspx")
        seen = []
        for m in DOCKET_RE.finditer(html):
            dk = m.group(1).upper()
            if dk not in seen:
                seen.append(dk)
        return seen

    def _orders_for_docket(self, dtype: str, year: int, docket: str) -> list[dict]:
        """Parse the 'Orders:' section of a docket page into raw order dicts."""
        url = f"{BASE_URL}/Dockets/{dtype}/{year}/{docket}.aspx"
        html = self._get_text(url)
        if not html:
            return []

        # Docket caption (title): "{DOCKET} - In the Matter of ...".
        docket_title = ""
        cm = re.search(
            re.escape(docket) + r"\s*-\s*(.+?)</(?:strong|b|p)>", html, re.I | re.S
        )
        if cm:
            docket_title = strip_tags(cm.group(1)).strip(" .-")

        # Isolate the Orders block: from the "Orders:" label to the next
        # section label ("Filed Documents", "Notices", "Proposed", etc.).
        om = re.search(r"Orders?:\s*</strong>(.*)", html, re.I | re.S)
        if not om:
            return []
        block = om.group(1)
        end = re.search(
            r"<strong>\s*(?:Filed\s+Documents|Notices?|Proposed|Transcripts|"
            r"Exhibits|Testimony|Motions|Correspondence|Applications?):",
            block,
            re.I,
        )
        if end:
            block = block[: end.start()]

        items = []
        seen = set()
        for lm in ORDER_LINK_RE.finditer(block):
            href, label = lm.group(1), strip_tags(lm.group(2))
            if not href.lower().endswith(".pdf"):
                continue
            pdf_url = href if href.startswith("http") else BASE_URL + (
                href if href.startswith("/") else "/" + href
            )
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            date = None
            dm = DATE_PREFIX_RE.search(label)
            if dm:
                date = parse_mdy(int(dm.group(1)), int(dm.group(2)), int(dm.group(3)))
            # Description = label minus the leading "MM/DD/YY - " prefix.
            desc = re.sub(r"^\s*\d{1,2}/\d{1,2}/\d{2,4}\s*[-–]\s*", "", label).strip()
            stem = pdf_url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            items.append(
                {
                    "docket": docket,
                    "dtype": dtype,
                    "docket_title": docket_title,
                    "description": desc,
                    "date": date,
                    "stem": stem,
                    "pdf_url": pdf_url,
                    "url": url,
                }
            )
        return items

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

    def normalize(self, raw: dict) -> dict | None:
        pdf_bytes = self._get(raw["pdf_url"])
        if not pdf_bytes:
            return None
        if pdf_bytes[:5] != b"%PDF-":
            logger.debug(f"{raw['stem']}: not a PDF payload")
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(
                f"Short/empty text for {raw['stem']} "
                f"({len(text) if text else 0} chars)"
            )
            return None

        docket = raw["docket"]
        title = f"SD PUC Order — Docket {docket}"
        caption = raw.get("docket_title") or ""
        if 5 <= len(caption) <= 400:
            title = f"SD PUC Order — Docket {docket}: {caption}"
        desc = raw.get("description") or ""
        if 3 <= len(desc) <= 300:
            title += f" ({desc})"

        # Fallback date from the Order body if the link carried none. SD PUC
        # Orders close with "Dated at Pierre, South Dakota, this Nth day of
        # Month YYYY." — but the day glyph is sometimes OCR-garbled, so accept
        # a non-numeric day and fall back to the 1st of the signature month.
        date = raw.get("date")
        if not date:
            months = {
                m: i
                for i, m in enumerate(
                    [
                        "january", "february", "march", "april", "may",
                        "june", "july", "august", "september", "october",
                        "november", "december",
                    ],
                    1,
                )
            }
            bm = re.search(
                r"day\s+of\s+([A-Za-z]+),?\s+(\d{4})", text, re.I
            )
            if bm:
                mon = months.get(bm.group(1).lower())
                if mon:
                    # Try to recover a numeric day immediately before "day of".
                    dm = re.search(
                        r"(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+"
                        + re.escape(bm.group(1)),
                        text,
                        re.I,
                    )
                    day = int(dm.group(1)) if dm else 1
                    date = parse_mdy(mon, day, int(bm.group(2)))

        return {
            "_id": f"US/SD-PUC/{docket}/{raw['stem']}",
            "_source": "US/SD-PUC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "docket": docket,
            "industry": raw.get("dtype"),
            "title": title,
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": date,
        }

    def test_api(self) -> bool:
        logger.info("Testing SD PUC docket archive...")
        try:
            rec = None
            for dtype in DOCKET_TYPES:
                years = self._list_years(dtype)
                if not years:
                    continue
                logger.info(f"  {dtype}: years {years[0]}..{years[-1]}")
                for year in years:
                    dockets = self._list_dockets(dtype, year)
                    for dk in dockets:
                        orders = self._orders_for_docket(dtype, year, dk)
                        for o in orders:
                            rec = self.normalize(o)
                            if rec:
                                break
                        if rec:
                            break
                    if rec:
                        break
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, "
                    f"docket {rec['docket']}, date={rec.get('date')})"
                )
                logger.info("API test PASSED")
                return True
            logger.error("  Full-text extraction failed or no Orders found")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw Order metadata dicts across the four docket types,
        newest year first. The framework calls normalize() on each raw dict,
        which downloads the PDF and extracts the full text.
        """
        seen = set()
        for dtype in DOCKET_TYPES:
            years = self._list_years(dtype)
            for year in years:
                dockets = self._list_dockets(dtype, year)
                for dk in dockets:
                    for o in self._orders_for_docket(dtype, year, dk):
                        if o["pdf_url"] in seen:
                            continue
                        seen.add(o["pdf_url"])
                        yield o

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/SD-PUC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SDPUCScraper()

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
