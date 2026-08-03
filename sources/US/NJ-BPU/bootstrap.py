#!/usr/bin/env python3
"""
US/NJ-BPU -- New Jersey Board of Public Utilities Orders

Fetches the full text of Board Orders issued by the New Jersey Board of Public
Utilities (NJBPU) adjudicating utility dockets (electric, gas, water,
telecommunications, cable, clean energy). Each Order is an administrative
adjudication / final decision of a specific docket by the Board = case_law.
Public domain (US state government edict).

Strategy (official public document search: publicaccess.bpu.state.nj.us,
ASP.NET WebForms behind Imperva/Incapsula):
  1. GET /Search.aspx to establish the session (Incapsula + ASP.NET_SessionId
     cookies) and capture the hidden __VIEWSTATE / __EVENTVALIDATION tokens.
  2. POST /Search.aspx an Advanced Search with searchType=Advanced,
     AdvanceDocumentTitle="ORDER" (matches the "ORDERS" document folder),
     ListType=Document and an OpenDate window (M/D/YYYY). The server responds
     at /SearchDocResults.aspx with a server-rendered GridView: one row per
     document carrying the Docket #, Document Title, Folder, Uploaded By,
     Description, Posted Date and a DocumentHandler.ashx?document_id={id} link.
  3. The GridView pages 30 rows at a time; follow the pager (a __doPostBack on
     lbtnNext) reposting the results-page viewstate until the window is
     exhausted.
  4. For each row in the ORDERS folder the born-digital Order PDF is downloaded
     from /DocumentHandler.ashx?document_id={id} (same session) and the full
     text is extracted (fitz/PyMuPDF; Tesseract OCR fallback for the rare scan).

The search endpoint intermittently throws a server-side 500 (redirect to
Error.aspx); every search re-GETs fresh tokens and retries.

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
import calendar
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
logger = logging.getLogger("legal-data-hunter.US.NJ-BPU")

BASE_URL = "https://publicaccess.bpu.state.nj.us"
SEARCH_URL = f"{BASE_URL}/Search.aspx"
RESULTS_URL = f"{BASE_URL}/SearchDocResults.aspx"
DOC_URL = f"{BASE_URL}/DocumentHandler.ashx?document_id="

P = "ctl00$ContentPlaceHolder1$searchFilter$"

# Corpus floor: the NJBPU public-access Orders folder thins out before ~2000.
FIRST_YEAR = 2000

DOCID_RE = re.compile(r"document_id=(\d+)", re.I)
NEXT_RE = re.compile(r"__doPostBack\('([^']*lbtnNext)'")
# NJ docket ids look like CE24010011, EO20020128, WR21030243, GR22101143, etc.
DOCKET_RE = re.compile(r"\b([A-Z]{2}\d{6,10})\b")


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


class NJBPUScraper(BaseScraper):

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
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.5

    # ---- low-level helpers --------------------------------------------------

    def _hidden(self, html: str) -> dict:
        fields = {}
        for name in ("__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION"):
            m = re.search(r'id="' + name + r'" value="([^"]*)"', html)
            fields[name] = m.group(1) if m else ""
        return fields

    def _search(self, start: str, end: str, retries: int = 5) -> str:
        """Run one Advanced Search window; return the page-1 results HTML.

        Re-GETs fresh tokens and retries on the intermittent Error.aspx 500.
        """
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                r = self.http.get(SEARCH_URL)
                if r.status_code != 200:
                    logger.warning(f"GET Search.aspx HTTP {r.status_code}")
                    time.sleep(2 ** attempt)
                    continue
                hidden = self._hidden(r.text)
                if not hidden.get("__VIEWSTATE"):
                    time.sleep(2 ** attempt)
                    continue
                data = {
                    "__EVENTTARGET": "",
                    "__EVENTARGUMENT": "",
                    "__VIEWSTATE": hidden["__VIEWSTATE"],
                    "__VIEWSTATEGENERATOR": hidden["__VIEWSTATEGENERATOR"],
                    "__VIEWSTATEENCRYPTED": "",
                    "__EVENTVALIDATION": hidden["__EVENTVALIDATION"],
                    P + "searchType": "Advanced",
                    P + "SearchText": "",
                    P + "AdvanceCaseNumber": "",
                    P + "AdvanceDocumentTitle": "ORDER",
                    P + "AdvancePartyName": "",
                    P + "AdvanceKeyword": "",
                    P + "OpenDateFrom": start,
                    P + "OpenDateTo": end,
                    P + "ListType": "Document",
                    P + "btnAdvanceSearch": "Search",
                }
                time.sleep(self.delay)
                resp = self.http.post(
                    SEARCH_URL, data=data,
                    headers={"Referer": SEARCH_URL,
                             "Content-Type": "application/x-www-form-urlencoded"},
                )
                if resp.status_code == 200 and "Error.aspx" not in resp.url \
                        and "cannot process this request" not in resp.text:
                    return resp.text
                logger.warning(
                    f"Search {start}..{end} attempt {attempt+1}: "
                    f"status={resp.status_code} url={resp.url}"
                )
            except Exception as e:
                logger.warning(f"Search {start}..{end} attempt {attempt+1} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return ""

    def _next_page(self, html: str, retries: int = 4) -> str:
        """Follow the GridView pager (lbtnNext) reposting results-page state."""
        m = NEXT_RE.search(html)
        if not m:
            return ""
        target = m.group(1)
        hidden = self._hidden(html)
        if not hidden.get("__VIEWSTATE"):
            return ""
        data = {
            "__EVENTTARGET": target,
            "__EVENTARGUMENT": "",
            "__VIEWSTATE": hidden["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": hidden["__VIEWSTATEGENERATOR"],
            "__VIEWSTATEENCRYPTED": "",
            "__EVENTVALIDATION": hidden["__EVENTVALIDATION"],
        }
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.post(
                    RESULTS_URL, data=data,
                    headers={"Referer": RESULTS_URL,
                             "Content-Type": "application/x-www-form-urlencoded"},
                )
                if resp.status_code == 200 and "Error.aspx" not in resp.url \
                        and "cannot process this request" not in resp.text:
                    return resp.text
            except Exception as e:
                logger.warning(f"Next-page attempt {attempt+1} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return ""

    # ---- results parsing ----------------------------------------------------

    def parse_results(self, html: str) -> list:
        """Parse a SearchDocResults GridView into raw metadata dicts."""
        items = []
        soup = BeautifulSoup(html or "", "html.parser")
        table = soup.find("table", id=lambda v: v and "gvSearchRs" in v)
        if table is None:
            return items
        rows = table.find_all("tr")
        if not rows:
            return items
        headers = [c.get_text(" ", strip=True).lower()
                   for c in rows[0].find_all(["th", "td"])]

        def col(frag, default):
            for i, h in enumerate(headers):
                if frag in h:
                    return i
            return default

        i_docket = col("docket", 0)
        i_title = col("document title", 1)
        i_folder = col("folder", 2)
        i_desc = col("description", 4)
        i_date = col("posted date", 5)

        for tr in rows[1:]:
            tds = tr.find_all("td")
            if not tds:
                continue
            link = tr.find("a", href=DOCID_RE)
            if not link:
                continue
            m = DOCID_RE.search(link.get("href", ""))
            if not m:
                continue
            docid = m.group(1)

            def cell(i):
                return tds[i].get_text(" ", strip=True) if i < len(tds) else ""

            folder = cell(i_folder)
            if "order" not in folder.lower():
                continue  # only the ORDERS folder = Board Orders
            docket = cell(i_docket).rstrip("- ").strip()
            title = cell(i_title)
            desc = cell(i_desc)
            date = parse_date(cell(i_date))
            items.append({
                "docid": docid,
                "docket": docket or None,
                "doc_title": title or None,
                "description": desc or None,
                "folder": folder,
                "date": date,
                "pdf_url": f"{DOC_URL}{docid}",
                "url": f"{DOC_URL}{docid}",
            })
        return items

    def _fetch_window(self, start: str, end: str) -> list:
        html = self._search(start, end)
        if not html:
            return []
        out = []
        seen_pages = 0
        while html and seen_pages < 400:
            page_rows = self.parse_results(html)
            out.extend(page_rows)
            if not NEXT_RE.search(html):
                break
            html = self._next_page(html)
            seen_pages += 1
        return out

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

    def extract_pdf_text(self, pdf_bytes: bytes) -> str:
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

    def _download(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url, headers={"Referer": RESULTS_URL})
                if resp.status_code == 200 and resp.content[:5] == b"%PDF-":
                    return resp.content
                logger.debug(f"download {url} status={resp.status_code}")
            except Exception as e:
                logger.warning(f"download {url} attempt {attempt+1} error: {e}")
            time.sleep(1.5 * (attempt + 1))
        return None

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        pdf_bytes = self._download(raw["pdf_url"])
        if not pdf_bytes:
            return None
        text = self.extract_pdf_text(pdf_bytes)
        if not text or len(text) < 200:
            logger.debug(f"Short/empty text for docid {raw['docid']}")
            return None

        docid = raw["docid"]
        docket = raw.get("docket")
        if not docket:
            dm = DOCKET_RE.search(text[:3000])
            if dm:
                docket = dm.group(1)

        title = "NJ BPU Order"
        if docket:
            title += f" — Docket {docket}"
        if raw.get("date"):
            title += f" ({raw['date']})"

        return {
            "_id": f"US/NJ-BPU/{docid}",
            "_source": "US/NJ-BPU",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "document_id": docid,
            "docket_number": docket,
            "doc_type": "Order",
            "title": title,
            "description": raw.get("description") or raw.get("doc_title"),
            "text": text,
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
            "date": raw.get("date"),
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing NJ BPU public document search (Orders)...")
        try:
            rows = []
            for start, end in _month_windows():
                rows = self._fetch_window(start, end)
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
                    f"docid {rec['document_id']}, docket={rec.get('docket_number')}, "
                    f"date={rec.get('date')})"
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
            rows = self._fetch_window(start, end)
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

    parser = argparse.ArgumentParser(description="US/NJ-BPU bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NJBPUScraper()

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
