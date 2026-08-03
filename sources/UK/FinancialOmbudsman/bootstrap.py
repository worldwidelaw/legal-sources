#!/usr/bin/env python3
"""
UK/FinancialOmbudsman -- Financial Ombudsman Service (FOS) -- Ombudsman
Decisions.

The Financial Ombudsman Service is the UK's statutory dispute-resolution scheme
for financial services, established under Part XVI of the Financial Services and
Markets Act 2000 (FSMA). When a consumer complaint against a financial business
cannot be settled, an Ombudsman issues a final, binding decision determining the
complaint (upheld / not upheld) and, where upheld, directing redress. Since
1 April 2013 the FCA's DISP rules require FOS to publish every final decision;
the Service publishes them, anonymised, on its website as born-digital PDFs.

Each decision is an adjudication of a specific dispute by a statutory
office-holder = case_law. The corpus is one of the largest single-tribunal
bodies of published decisions anywhere: 404,000+ final decisions covering
banking, insurance, mortgages, investments, pensions, consumer credit, PPI,
payment protection, fraud/scams and more.

Strategy:
  - The public "Ombudsman decisions" search
    (/businesses/resolving-complaint/ombudsman-decisions/search) is a
    server-rendered listing: 10 results per page, paged with ?Start=N (N steps
    by 10), Sort=date. Each result <li> carries the Decision Reference
    (DRN-NNNNNNN), decision date, business name, outcome (Upheld / Not upheld),
    industry-sector tag and a link to the decision PDF at /decision/{DRN}.pdf.
  - Page the listing to enumerate every DRN + its listing metadata.
  - Download each born-digital decision PDF and extract full text with PyMuPDF
    (pdfplumber/pypdf fallback). No OCR needed.
  - One record per decision.

Data:
  - 404,000+ final decisions, 2013-present
  - Language: English
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html
import calendar
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.FinancialOmbudsman")

BASE_URL = "https://www.financial-ombudsman.org.uk"
SEARCH_PATH = "/businesses/resolving-complaint/ombudsman-decisions/search"
DECISION_PDF = "/decision/{drn}.pdf"

PAGE_SIZE = 10
# The listing search backend caps deep pagination: an unfiltered Sort=date sweep
# only returns results reliably up to ~Start=160,000 (goes erratic/empty past
# that), so a single linear sweep silently truncates the corpus at ~140-160K of
# the ~404,560 total (issue #1170). Fix: partition the sweep into calendar-month
# windows via the DateFrom/DateTo filters. Each month holds ~2,000-3,000
# decisions (max year ~37K), far under the deep-paging ceiling, so every window
# paginates to completion and the union covers the full corpus.
WINDOW_MAX_START = 80000    # per-window safety ceiling (>> largest month)
START_YEAR = 2013           # FCA DISP publish rule began 1 Apr 2013
START_MONTH = 1
TAG_RE = re.compile(r"<[^>]+>")

# The results list: <ul class="search-results"> ... </ul>
RESULTS_UL_RE = re.compile(
    r'<ul class="search-results"[^>]*>(.*?)</ul>', re.S | re.I)
ITEM_RE = re.compile(r"<li>(.*?)</li>", re.S | re.I)
# Two URL formats coexist: newer decisions use a hyphen ("decision/DRN-6400983.pdf"),
# older ones (pre-~2017) drop it ("decision/DRN3474862.pdf"). Capture the DRN
# reference exactly as it appears so the PDF URL round-trips for both.
DRN_HREF_RE = re.compile(r'href="[^"]*decision/(DRN-?\d+)\.pdf"', re.I)
DATE_EM_RE = re.compile(r"<em>\s*([^<]+?)\s*</em>", re.I)
TAG_TYPE_RE = re.compile(
    r'<span class="search-result__tag[^"]*">\s*([^<]+?)\s*</span>', re.I)
INFO_MAIN_RE = re.compile(
    r'<div class="search-result__info-main">(.*?)</div>', re.S | re.I)
RESULT_COUNT_RE = re.compile(r"returned\s+([\d,]+)\s+results", re.I)

_MONTHS = {m: i for i, m in enumerate(
    ("jan feb mar apr may jun jul aug sep oct nov dec".split()), start=1)}


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub(" ", s or "")).strip()


def _parse_listing_date(text: str) -> Optional[str]:
    """Listing date is like '2 Jun 2026' -> '2026-06-02'."""
    if not text:
        return None
    m = re.match(r"(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})", text.strip())
    if not m:
        return None
    day, mon, year = m.group(1), m.group(2)[:3].lower(), m.group(3)
    month = _MONTHS.get(mon)
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a born-digital decision PDF via PyMuPDF, with a shared
    pdfplumber/pypdf fallback."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 120:
                return text
        except Exception as e:
            logger.debug(f"fitz extract failed: {e}")
    try:
        from common import pdf_extract as _pe
        for fn in ("_extract_with_pdfplumber", "_extract_with_pypdf"):
            f = getattr(_pe, fn, None)
            if f:
                try:
                    t = f(pdf_bytes)
                    if t and len(t) >= 120:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return ""


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in (text or "").replace("\r", "").split("\n")]
    out, blanks = [], 0
    for ln in lines:
        if ln.strip():
            blanks = 0
            out.append(ln.strip())
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


def _month_windows() -> List[Tuple[str, str]]:
    """Yield (DateFrom, DateTo) 'YYYY-MM-DD' pairs, one per calendar month,
    newest month first, from the current month back to START_YEAR/START_MONTH.
    Windowing the search by month keeps each result set (~2-3K) well under the
    listing's deep-pagination ceiling so the whole corpus is reachable."""
    now = datetime.now(timezone.utc)
    y, mo = now.year, now.month
    windows: List[Tuple[str, str]] = []
    while (y > START_YEAR) or (y == START_YEAR and mo >= START_MONTH):
        last = calendar.monthrange(y, mo)[1]
        windows.append((f"{y:04d}-{mo:02d}-01", f"{y:04d}-{mo:02d}-{last:02d}"))
        mo -= 1
        if mo == 0:
            mo = 12
            y -= 1
    return windows


class FinancialOmbudsmanScraper(BaseScraper):
    """Scraper for the UK Financial Ombudsman Service published decisions
    (server-rendered listing + born-digital decision PDFs)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=90,
        )

    # -- HTTP helpers ----------------------------------------------------
    def _search_url(self, start: int, date_from: Optional[str] = None,
                    date_to: Optional[str] = None) -> str:
        params = ["Sort=date", f"Start={start}"]
        if date_from:
            params.append(f"DateFrom={date_from}")
        if date_to:
            params.append(f"DateTo={date_to}")
        return f"{BASE_URL}{SEARCH_PATH}?" + "&".join(params)

    def _get_html(self, url: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"GET {url}: HTTP {resp.status_code}")
            return None
        return resp.content.decode("utf-8", "replace")

    def _fetch_pdf(self, drn: str) -> Optional[bytes]:
        url = BASE_URL + DECISION_PDF.format(drn=drn)
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"pdf {url}: {e}")
            return None
        if resp.status_code != 200:
            logger.warning(f"pdf {url}: HTTP {resp.status_code}")
            return None
        data = resp.content
        if not data[:5].startswith(b"%PDF"):
            logger.debug(f"pdf {url}: not a PDF")
            return None
        return data

    # -- listing parsing -------------------------------------------------
    def _parse_page(self, page_html: str) -> List[Dict[str, Any]]:
        m = RESULTS_UL_RE.search(page_html)
        if not m:
            return []
        rows: List[Dict[str, Any]] = []
        for im in ITEM_RE.finditer(m.group(1)):
            item = im.group(1)
            drn_m = DRN_HREF_RE.search(item)
            if not drn_m:
                continue
            drn = drn_m.group(1).upper()
            date_iso = None
            business = None
            outcome = None
            info_m = INFO_MAIN_RE.search(item)
            if info_m:
                info = info_m.group(1)
                dm = DATE_EM_RE.search(info)
                if dm:
                    date_iso = _parse_listing_date(_strip(dm.group(1)))
                # info-main is: <em>DATE</em> <sep> BUSINESS <sep> OUTCOME
                parts = [p.strip() for p in re.split(
                    r'<span class="search-result__separator"[^>]*>\s*</span>',
                    info) if p.strip()]
                cleaned = [_strip(p) for p in parts]
                cleaned = [c for c in cleaned if c]
                if len(cleaned) >= 2:
                    business = cleaned[1]
                if len(cleaned) >= 3:
                    outcome = cleaned[2]
            tag_m = TAG_TYPE_RE.search(item)
            category = _strip(tag_m.group(1)) if tag_m else None
            rows.append({
                "drn": drn,
                "date": date_iso,
                "business": business,
                "outcome": outcome,
                "category": category,
            })
        return rows

    def _build_raw(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pdf = self._fetch_pdf(row["drn"])
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.debug(f"extract {row['drn']}: {e}")
            text = ""
        if not text:
            return None
        raw = dict(row)
        raw["text"] = text
        return raw

    # -- core ------------------------------------------------------------
    def _iter_window(self, date_from: str, date_to: str, seen: set,
                     since_date=None,
                     ) -> Generator[Dict[str, Any], None, None]:
        """Paginate a single DateFrom..DateTo window (newest-first). If
        `since_date` is given, return as soon as a decision older than it is
        reached (the rest of the window is older still)."""
        start = 0
        empty_streak = 0
        while start < WINDOW_MAX_START:
            page_html = self._get_html(
                self._search_url(start, date_from, date_to))
            if page_html is None:
                empty_streak += 1
                if empty_streak >= 3:
                    break
                start += PAGE_SIZE
                continue
            rows = self._parse_page(page_html)
            if not rows:
                # End of this window (or a transient empty page).
                empty_streak += 1
                if empty_streak >= 2:
                    break
                start += PAGE_SIZE
                continue
            empty_streak = 0
            for row in rows:
                if since_date and row.get("date"):
                    try:
                        if datetime.strptime(
                                row["date"], "%Y-%m-%d").date() < since_date:
                            return
                    except ValueError:
                        pass
                if row["drn"] in seen:
                    continue
                seen.add(row["drn"])
                raw = self._build_raw(row)
                if raw:
                    yield raw
            start += PAGE_SIZE

    def _iter_listing(self) -> Generator[Dict[str, Any], None, None]:
        """Full sweep across every calendar-month window (newest-first),
        deduping DRNs across windows."""
        seen = set()
        for date_from, date_to in _month_windows():
            for raw in self._iter_window(date_from, date_to, seen):
                yield raw

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for raw in self._iter_listing():
            produced += 1
            yield raw
        if produced == 0:
            raise RuntimeError(
                "FOS ombudsman-decisions listing returned 0 decisions — "
                "site blocked, layout changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: walk month windows newest-first, stopping once windows
        fall entirely before `since`."""
        since_date = since.date()
        since_iso = since_date.isoformat()
        seen = set()
        for date_from, date_to in _month_windows():
            if date_to < since_iso:
                # Windows are newest-first; this one and all later ones are older.
                break
            for raw in self._iter_window(date_from, date_to, seen,
                                         since_date=since_date):
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        drn = raw.get("drn", "")
        business = raw.get("business")
        title = f"Ombudsman decision {drn}"
        if business:
            title += f" — {business}"
        return {
            "_id": f"UK-FOS-{drn}",
            "_source": "UK/FinancialOmbudsman",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": BASE_URL + DECISION_PDF.format(drn=drn),
            "decision_reference": drn,
            "business": business,
            "outcome": raw.get("outcome"),
            "category": raw.get("category"),
            "court": "Financial Ombudsman Service",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing FOS ombudsman-decisions listing...")
        page_html = self._get_html(
            f"{BASE_URL}{SEARCH_PATH}?Sort=date&Start=0")
        print(f"  listing page 0: {'OK' if page_html else 'FAILED'}")
        if not page_html:
            return
        cm = RESULT_COUNT_RE.search(page_html)
        if cm:
            print(f"  total results reported: {cm.group(1)}")
        rows = self._parse_page(page_html)
        print(f"  parsed {len(rows)} results on page 0")
        if rows:
            print(f"  first: {rows[0]['drn']} | {rows[0]['date']} | "
                  f"{rows[0]['business']} | {rows[0]['outcome']} | "
                  f"{rows[0]['category']}")
            raw = self._build_raw(rows[0])
            if raw:
                print(f"  full text extracted: {len(raw['text'])} chars - OK")


def main():
    scraper = FinancialOmbudsmanScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            logger.info("Running bootstrap in sample mode")
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            logger.info("Running full bootstrap")
            stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
