#!/usr/bin/env python3
"""
US/WY-SBOE -- Wyoming State Board of Equalization (SBOE)
Board Decisions & Orders (case_law)

Fetches the full text of the Wyoming State Board of Equalization's published
adjudicative decisions. The SBOE is the constitutional full-time board that
hears and decides all Wyoming state/local tax appeals -- Department of Revenue
determinations on sales/use, mineral/severance and ad valorem valuation, plus
appeals from County Boards of Equalization on property-tax valuation -- so
every published document is an adjudication of a specific case -> case_law,
jurisdiction US-WY.

Access (no JavaScript, no CAPTCHA, no auth):
  The "Recent Board Decisions" listing
    http://taxappeals.state.wy.us/18_opinions.html
  is a server-rendered HTML table. Each <tr> carries three cells --
  Petitioner name, decision Date ("Month D, YYYY"), and Docket No. (an
  <a href="images/....PDF"> to the decision PDF). The dominant filename
  pattern is images/docket_no_{YYYYNN}.PDF (year + sequence, e.g.
  docket_no_202526.PDF = docket 2025-26); a handful are descriptively named
  certification-decision / appendix PDFs tied to the same docket.

  NOTE on the host: the HTTPS certificate is name-invalid, so this scraper
  talks plain HTTP (http://taxappeals.state.wy.us/). The index and the PDFs
  serve HTTP 200 with no gate.

  CAVEAT -- OCR REQUIRED: the decision PDFs are SCANNED IMAGES with no text
  layer (pdfplumber/pypdf return 0 chars). Full text is obtained via the OCR
  fallback in common.pdf_extract (PyMuPDF -> pytesseract), which is gated on the
  `tesseract` binary being installed (export PATH to include it if off-PATH).

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py bootstrap --full     # Fetch all decisions
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WY-SBOE")

BASE_URL = "http://taxappeals.state.wy.us/"
INDEX_URLS = [
    "http://taxappeals.state.wy.us/18_opinions.html",     # Recent Board Decisions
    "http://taxappeals.state.wy.us/18_proc_orders.html",  # Procedural Orders
]

MIN_TEXT_CHARS = 200
TAG_RE = re.compile(r"<[^>]+>")
# A table row: <tr> ... <td>Petitioner</td><td>Date</td><td><a href=PDF>Docket</a></td> </tr>
ROW_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.S | re.I)
TD_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.S | re.I)
HREF_RE = re.compile(r'href="([^"]+\.pdf)"', re.I)
DATE_RE = re.compile(
    r"\b(January|February|March|April|May|June|July|August|September|October|"
    r"November|December)\s+(\d{1,2}),\s+(\d{4})\b",
    re.I,
)
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _cell_text(td_html: str) -> str:
    txt = TAG_RE.sub(" ", td_html)
    txt = html_module.unescape(txt)
    return re.sub(r"\s+", " ", txt).strip()


def _slug(pdf_url: str) -> str:
    base = unquote(pdf_url).rsplit("/", 1)[-1].rsplit(".", 1)[0]
    base = re.sub(r"[^A-Za-z0-9._,-]+", "-", base).strip("-")[:100]
    return base or "decision"


def _iso_date(cell: str, docket: str | None) -> str | None:
    """Decision date from the row's Date cell, falling back to the docket year."""
    m = DATE_RE.search(cell or "")
    if m:
        mm = MONTHS.get(m.group(1).lower())
        try:
            return datetime(int(m.group(3)), mm, int(m.group(2))).date().isoformat()
        except (ValueError, TypeError):
            pass
    if docket:
        ym = re.match(r"(\d{4})", docket)
        if ym:
            return f"{ym.group(1)}-01-01"
    return None


def _docket_from(cell_text: str, filename: str) -> str | None:
    """Docket like '2025-26' from the link text, else from docket_no_YYYYNN."""
    m = re.search(r"\b(19|20)\d{2}\s*-\s*\d{1,3}\b", cell_text or "")
    if m:
        return re.sub(r"\s+", "", m.group(0))
    m = re.search(r"docket_no_(\d{4})(\d{2,3})", filename, re.I)
    if m:
        return f"{m.group(1)}-{int(m.group(2))}"
    return None


class WYSboeScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=120,
            verify=False,  # HTTPS cert is name-invalid; we use plain HTTP anyway
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_text(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF content ({ctype}) for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def discover_documents(self, sample: bool = False) -> list[dict]:
        docs: list[dict] = []
        seen: set[str] = set()
        for index_url in INDEX_URLS:
            page = self._get_text(index_url)
            if not page:
                logger.warning(f"Failed to fetch index page {index_url}")
                continue
            is_orders = "proc_orders" in index_url
            new = 0
            for row_html in ROW_RE.findall(page):
                href_m = HREF_RE.search(row_html)
                if not href_m:
                    continue
                pdf_url = urljoin(BASE_URL, html_module.unescape(href_m.group(1).strip()))
                if pdf_url in seen:
                    continue
                seen.add(pdf_url)
                cells = [_cell_text(td) for td in TD_RE.findall(row_html)]
                # Row layout: [Petitioner, Date, Docket]. Be tolerant of order.
                petitioner = cells[0] if cells else ""
                date_cell = next((c for c in cells if DATE_RE.search(c)), "")
                filename = pdf_url.rsplit("/", 1)[-1]
                docket = _docket_from(" ".join(cells), unquote(filename))
                # Skip index/nav artefacts that slipped through (no petitioner + no docket)
                if not petitioner and not docket and "docket_no" not in filename.lower():
                    continue
                doc_kind = "Procedural Order" if is_orders else "Board Decision"
                title = f"Wyoming State Board of Equalization — {doc_kind}"
                if docket:
                    title += f" (Docket {docket})"
                if petitioner and petitioner.lower() not in ("petitioner", "docket no."):
                    title += f" — {petitioner}"
                docs.append({
                    "slug": _slug(pdf_url),
                    "docket_number": docket,
                    "petitioner": petitioner or None,
                    "doc_kind": doc_kind,
                    "title": title[:300],
                    "date": _iso_date(date_cell, docket),
                    "pdf_url": pdf_url,
                })
                new += 1
            logger.info(f"Discovered {new} PDFs on {index_url}")
            if sample and len(docs) >= 30:
                break
        logger.info(f"Discovered {len(docs)} total WY SBOE documents")
        return docs

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["pdf_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/WY-SBOE",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars) — scanned PDF, "
                           f"OCR (tesseract) required: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Wyoming State Board of Equalization decisions...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} documents")
            for doc in docs[:5]:
                raw = self._build_raw(doc)
                if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                    logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                                f"{raw.get('docket_number')}")
                    logger.info("API test PASSED")
                    return True
            logger.error("  Text extraction failed on the first 5 — the PDFs are "
                         "scanned; OCR (tesseract) must be available on this host")
            return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard schema."""
        docket = raw.get("docket_number")
        title = raw.get("title") or "Wyoming State Board of Equalization decision"
        return {
            "_id": f"US/WY-SBOE/{raw['slug']}",
            "_source": "US/WY-SBOE",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "petitioner": raw.get("petitioner"),
            "court": "Wyoming State Board of Equalization",
            "title": title[:300],
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-WY",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 30:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WY-SBOE bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = WYSboeScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
