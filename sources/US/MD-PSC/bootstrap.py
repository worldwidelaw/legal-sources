#!/usr/bin/env python3
"""
US/MD-PSC -- Maryland Public Service Commission Orders

Fetches the full text of Orders issued by the Maryland Public Service
Commission (PSC) adjudicating utility cases — electric, gas, water/sewer,
telecommunications and transportation matters: rate cases, certificates of
public convenience and necessity (CPCN), merger approvals and merger-
condition enforcement, tariff filings, fuel/purchased-gas adjustments and
regulatory dockets. Each Commission Order is an administrative adjudication /
edict of a specific case = case_law. Public domain (US state government edict).

Strategy (official DMS "Commission orders" portal):

  The MD PSC publishes every Commission Order through its Document Management
  System (DMS) portal at ``webpscxb.pscmaryland.com/DMS``. The
  ``/DMS/commissionorders`` page is an ASP.NET WebForms search with a
  "Find by date range" query (btnFind2) that returns the list of orders
  issued in a date window. The date-range search silently returns nothing for
  spans wider than ~one month, so fetch_all() enumerates the corpus one
  calendar month at a time (newest month first), collecting for each order its
  DMS "mail log" id, order number and issue date.

  Retrieving the full text is a two-hop chain per order:
    1. ``/DMS/maillogpdfview/MailLog/0/0/{maillog_id}/0`` returns a small HTML
       PDF-viewer page that embeds the real document path in a
       ``data-pdf='/DMS/pdfview/...'`` attribute (backtick-encoded server path,
       filename carries the case number in parentheses).
    2. ``/DMS/pdfview/{path}`` streams the born-digital ``application/pdf``.
  normalize() follows that chain, extracts full text with fitz/PyMuPDF and
  reads the case number from the order body ("CASE NO. 9449"), falling back to
  the parenthesised case number in the pdfview filename.

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
import html as htmllib
from pathlib import Path
from datetime import datetime, timezone, date
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
logger = logging.getLogger("legal-data-hunter.US.MD-PSC")

BASE = "https://webpscxb.pscmaryland.com"
ORDERS_URL = f"{BASE}/DMS/commissionorders"

# Earliest year with orders in the DMS (coverage is patchy before ~2005 but
# real back to 2000); a few empty early months cost one request each.
START_YEAR = 1999

# One order row in the results HTML: the "open pdf" button carries the mail-log
# view path plus the order number as its label.
BUTTON_RE = re.compile(
    r"data-pdf='/DMS/maillogpdfview/MailLog/\d+/\d+/(\d+)/\d+'>\s*Order&nbsp;([\w.\-]+)",
    re.I,
)
# "Issue Date:&nbsp;January 28, 2025"
ISSUE_DATE_RE = re.compile(r"Issue Date:(?:&nbsp;|\s)*([A-Za-z]+ \d{1,2}, \d{4})")
# The real document path embedded in the mail-log viewer page.
PDFVIEW_RE = re.compile(r"data-pdf='(/DMS/pdfview/[^']+)'", re.I)

# Case number inside the order body / filename.
BODY_CASE_RE = re.compile(r"CASE\s+NO\.?\s*([0-9]{3,5}[A-Za-z]?)", re.I)
FILE_CASE_RE = re.compile(r"\((\d{3,5})\)~pdf", re.I)
BODY_ORDER_RE = re.compile(r"ORDER\s+NO\.?\s*([0-9]{4,6})", re.I)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
VIEWSTATE_RE = re.compile(r'id="__VIEWSTATE"[^>]*value="([^"]*)"')
VSGEN_RE = re.compile(r'id="__VIEWSTATEGENERATOR"[^>]*value="([^"]*)"')

# The most recent N month windows the Commission is guaranteed to have issued
# orders in. If every one of them comes back empty, the search is broken for
# this vantage rather than the months being genuinely quiet.
PROBE_WINDOWS = 6


class MDPSCUnreachable(RuntimeError):
    """Raised when the DMS date-range search yields nothing for this vantage.

    The btnFind2 search answers HTTP 200 with an order-less form whenever it
    refuses a request, so a datacenter block and a quiet month look identical
    in the markup. Failing loud keeps that from being reported as an empty
    corpus (issue #1255).
    """


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def issue_date_to_iso(s: str) -> str | None:
    m = re.match(r"([A-Za-z]+) (\d{1,2}), (\d{4})", s or "")
    if not m:
        return None
    mon = MONTHS.get(m.group(1).lower())
    if not mon:
        return None
    day, year = int(m.group(2)), int(m.group(3))
    if not (1 <= day <= 31 and 1980 <= year <= 2100):
        return None
    return f"{year:04d}-{mon:02d}-{day:02d}"


def month_windows(start_year: int) -> list[tuple[str, str]]:
    """(first, last) day strings for every month from now back to start_year."""
    today = datetime.now(timezone.utc).date()
    windows = []
    y, m = today.year, today.month
    while (y, m) >= (start_year, 1):
        first = date(y, m, 1)
        if m == 12:
            last = date(y, 12, 31)
        else:
            last = date(y, m + 1, 1).toordinal() - 1
            last = date.fromordinal(last)
        windows.append((first.isoformat(), last.isoformat()))
        m -= 1
        if m == 0:
            y, m = y - 1, 12
    return windows


class MDPSCScraper(BaseScraper):

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
        self.delay = 0.5
        self.checkpoint_path = Path(self.source_dir) / "data" / "months_done.json"
        self._done_months = self._load_checkpoint()
        # Sample runs stop mid-corpus and never write records.jsonl, so they
        # must not mark months as done for the next full run.
        self.use_checkpoint = True

    # ---- checkpoint ---------------------------------------------------------

    def _load_checkpoint(self) -> set:
        try:
            with open(self.checkpoint_path) as fh:
                return set(json.load(fh).get("months_done") or [])
        except Exception:
            return set()

    def _save_checkpoint(self) -> None:
        self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.checkpoint_path.with_suffix(".tmp")
        with open(tmp, "w") as fh:
            json.dump({"months_done": sorted(self._done_months)}, fh)
        tmp.replace(self.checkpoint_path)

    # ---- low-level fetch ----------------------------------------------------

    def _get_text(self, url: str, retries: int = 4) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.session.get(url, timeout=90)
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 404:
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 4) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.session.get(url, timeout=120)
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

    # ---- date-range search --------------------------------------------------

    def _search_month(self, start: str, end: str) -> list[dict]:
        """POST the date-range search for [start, end]; return order rows."""
        page = self._get_text(ORDERS_URL)
        if not page:
            raise MDPSCUnreachable(
                f"{ORDERS_URL} did not answer for {start}..{end} after retries "
                f"— the DMS host is refusing this vantage; needs a US "
                f"residential vantage/proxy"
            )
        mvs = VIEWSTATE_RE.search(page)
        mvg = VSGEN_RE.search(page)
        if not mvs:
            raise MDPSCUnreachable(
                f"No __VIEWSTATE in the {ORDERS_URL} response for "
                f"{start}..{end} ({len(page)} bytes) — the response is not the "
                f"WebForms search page (block page or markup change)"
            )
        data = {
            "__VIEWSTATE": mvs.group(1),
            "__VIEWSTATEGENERATOR": mvg.group(1) if mvg else "",
            "ctl00$ContentPlaceHolder1$ddlTypeOfOrder": "All",
            "ctl00$ContentPlaceHolder1$txtStartDate": start,
            "ctl00$ContentPlaceHolder1$txtEndDate": end,
            "ctl00$ContentPlaceHolder1$btnFind2": "Find by date range",
        }
        body = None
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                resp = self.session.post(ORDERS_URL, data=data, timeout=180)
                if resp.status_code == 200:
                    body = resp.text
                    break
                logger.warning(f"Search HTTP {resp.status_code} for {start}..{end}")
            except Exception as e:
                logger.warning(f"Search error {start}..{end} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        if not body:
            raise MDPSCUnreachable(
                f"btnFind2 date-range search did not answer for {start}..{end} "
                f"after 3 attempts — the DMS host is refusing this vantage; "
                f"needs a US residential vantage/proxy"
            )
        return self._parse_rows(body)

    @staticmethod
    def _parse_rows(body: str) -> list[dict]:
        rows = []
        for m in BUTTON_RE.finditer(body):
            maillog_id = m.group(1)
            order_no = htmllib.unescape(m.group(2)).strip()
            # nearest preceding "Issue Date:" is this order's issue date
            preceding = body[: m.start()]
            dm = None
            for dm in ISSUE_DATE_RE.finditer(preceding):
                pass
            issue_date = issue_date_to_iso(dm.group(1)) if dm else None
            rows.append(
                {
                    "maillog_id": maillog_id,
                    "order_number": order_no,
                    "date": issue_date,
                }
            )
        return rows

    def _enumerate_orders(self, max_months: int | None = None,
                          checkpoint: bool = False) -> Generator[dict, None, None]:
        """Yield deduplicated order rows, newest month first.

        The full walk is ~320 monthly searches at ~17s each before a single PDF
        is downloaded, so completed months are checkpointed and skipped with no
        network calls on the next run (issue #1255).
        """
        seen: set[str] = set()
        windows = month_windows(START_YEAR)
        if max_months:
            windows = windows[:max_months]
        if checkpoint and self._done_months:
            logger.info(
                f"Resuming: {len(self._done_months)} months already searched, "
                f"skipping them with no network calls"
            )
        total = 0
        searched = 0
        for idx, (start, end) in enumerate(windows):
            month = start[:7]
            # The newest few windows are never skipped: they are this run's
            # canary that the search still works, and they pick up orders
            # issued since the last run. Everything older obeys the checkpoint.
            if checkpoint and idx >= PROBE_WINDOWS and month in self._done_months:
                continue
            rows = self._search_month(start, end)
            searched += 1
            for r in rows:
                if r["maillog_id"] in seen:
                    continue
                seen.add(r["maillog_id"])
                total += 1
                yield r
            if rows:
                logger.info(f"{month}: {len(rows)} orders (running total {total})")
            if checkpoint:
                self._done_months.add(month)
                self._save_checkpoint()
            # The search answers 200 with an order-less form when it refuses a
            # request, so an empty run of the most recent months means the
            # search is broken here — not that the Commission issued nothing.
            if total == 0 and idx + 1 >= PROBE_WINDOWS:
                raise MDPSCUnreachable(
                    f"btnFind2 returned 0 order rows for the {searched} most "
                    f"recent month windows (through {month}) — the DMS "
                    f"date-range search is silently empty for this vantage; "
                    f"needs a US residential vantage/proxy"
                )

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

    def _resolve_pdf(self, maillog_id: str) -> tuple[str | None, str | None]:
        """Return (pdfview_url, pdfview_path) for a mail-log id, or (None, None)."""
        viewer = self._get_text(
            f"{BASE}/DMS/maillogpdfview/MailLog/0/0/{maillog_id}/0"
        )
        if not viewer:
            return None, None
        m = PDFVIEW_RE.search(viewer)
        if not m:
            return None, None
        path = m.group(1)
        return BASE + path, path

    # ---- normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> dict | None:
        maillog_id = raw["maillog_id"]
        order_no = raw.get("order_number") or ""
        pdf_url, pdf_path = self._resolve_pdf(maillog_id)
        if not pdf_url:
            logger.debug(f"No pdfview path for maillog {maillog_id}")
            return None
        pdf = self._get_bytes(pdf_url)
        if not pdf or pdf[:5] != b"%PDF-":
            logger.debug(f"No PDF for maillog {maillog_id}")
            return None
        text = self._extract_pdf(pdf)
        if not text or len(text) < 200:
            logger.debug(f"Short/empty text for maillog {maillog_id} ({len(text) if text else 0})")
            return None

        # Case number: prefer the pdfview filename's parenthesised code, then body.
        case_number = None
        fm = FILE_CASE_RE.search(pdf_path or "")
        if fm:
            case_number = fm.group(1)
        if not case_number:
            bm = BODY_CASE_RE.search(text)
            if bm:
                case_number = bm.group(1)

        if not order_no:
            om = BODY_ORDER_RE.search(text)
            if om:
                order_no = om.group(1)
        if not order_no:
            return None

        date_iso = raw.get("date")
        title = f"Maryland PSC Order No. {order_no}"
        if case_number:
            title += f" (Case No. {case_number})"

        return {
            "_id": f"US/MD-PSC/order-{order_no}",
            "_source": "US/MD-PSC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": case_number,
            "order_number": order_no,
            "title": title,
            "text": text,
            "url": f"{BASE}/DMS/maillogpdfview/MailLog/0/0/{maillog_id}/0",
            "date": date_iso,
        }

    # ---- api test -----------------------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Maryland PSC commission-orders enumeration...")
        try:
            rows = list(self._enumerate_orders(max_months=6))
            if len(rows) < 5:
                logger.error(f"  Too few orders enumerated in recent months: {len(rows)}")
                return False
            logger.info(f"  Enumerated {len(rows)} recent orders")
            rec = None
            for r in rows:
                rec = self.normalize(r)
                if rec:
                    break
            if rec and len(rec["text"]) > 200:
                logger.info(
                    f"  Full text OK ({len(rec['text'])} chars, order={rec['order_number']}, "
                    f"case={rec.get('case_number')}, date={rec.get('date')})"
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
        yield from self._enumerate_orders(checkpoint=self.use_checkpoint)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        since = as_date_str(since)  # update() passes a datetime; #1512
        for raw in self._enumerate_orders():
            if not since or (raw.get("date") or "") >= since:
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/MD-PSC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MDPSCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"bootstrap-fast complete: {json.dumps(stats, default=str)}")
        return

    scraper.use_checkpoint = not args.sample
    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
