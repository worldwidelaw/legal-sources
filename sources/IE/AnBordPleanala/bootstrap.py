#!/usr/bin/env python3
"""
IE/AnBordPleanala -- An Bord Pleanála / An Coimisiún Pleanála (Irish Planning Appeals Board) — Decisions

An Bord Pleanála (rebranded An Coimisiún Pleanála in 2024) is Ireland's national
independent statutory body that decides appeals and applications on planning,
infrastructure and related consents under the Planning and Development Act 2000.
For every determined case the Board publishes the reasoned **Inspector's Report**
(a born-digital PDF with a text layer) alongside the formal Board Order and Board
Direction (short scanned orders). The Inspector's Report is the substantive
adjudicative reasoning = case_law (public government-edict works).

Strategy:
  - Enumerate determined cases via the public case listing, windowed by decision
    date (monthly). The listing caps at 500 results, so windows that hit the cap
    are recursively split into shorter sub-windows.
      GET /en-ie/cases?decisionFrom=YYYY-MM-DD&decisionTo=YYYY-MM-DD
      → anchors /en-ie/case/{N}
  - Per case: GET /en-ie/case/{N} → parse metadata (case reference, planning
    authority reference, address, case type, decision, date signed) and the
    Inspector's Report PDF link.
  - Fetch the Inspector's Report PDF and extract full text with PyMuPDF (fitz).
    Born-digital, no OCR needed. Cases whose report is missing or scanned
    (no text layer) are skipped (metadata-only records are not stored).

Data:
  - ~20,000+ determined cases with Inspector's Reports, 2016-present
  - Language: English
  - Auth: None (free public access)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent months)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html
import logging
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from typing import Generator, Optional, Dict, Any, List, Set, Tuple

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
logger = logging.getLogger("legal-data-hunter.IE.AnBordPleanala")

BASE_URL = "https://www.pleanala.ie"
FIRST_YEAR = 2016
LISTING_CAP = 500

CASE_LINK_RE = re.compile(r'/en-ie/case/(\d+)', re.IGNORECASE)
REPORT_LINK_RE = re.compile(
    r'href="(/anbordpleanala/media/abp/cases/reports/\d+/r\d+[^"]*\.pdf)"',
    re.IGNORECASE,
)
CASE_REF_RE = re.compile(r'Case reference:\s*([A-Z0-9.\-/]+)', re.IGNORECASE)
PA_REF_RE = re.compile(r'Planning Authority Case Reference:\s*([^<\n]+)', re.IGNORECASE)
ADDRESS_RE = re.compile(r'<p class="address">\s*(.*?)\s*</p>', re.IGNORECASE | re.S)
FIELD_RE = re.compile(
    r'<p class="case-sub">\s*([^<]+?)\s*</p>.*?<p class="case-summary">\s*(.*?)\s*</p>',
    re.IGNORECASE | re.S,
)


def _pdf_text(pdf_bytes: bytes) -> str:
    if not fitz:
        raise RuntimeError("PyMuPDF (fitz) is required to extract An Bord Pleanála PDFs")
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return "\n".join(page.get_text() for page in doc)
    finally:
        doc.close()


def _clean(text: str) -> str:
    lines = [ln.rstrip() for ln in text.split("\n")]
    out, blanks = [], 0
    for ln in lines:
        s = ln.strip()
        if s:
            blanks = 0
            out.append(s)
        else:
            blanks += 1
            if blanks <= 1:
                out.append("")
    return "\n".join(out).strip()


def _strip_tags(s: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", s)).strip()


def _last_day(year: int, month: int) -> date:
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


class AnBordPleanalaScraper(BaseScraper):
    """Scraper for the An Bord Pleanála determined-case database."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=90,
        )

    # ---- enumeration -------------------------------------------------------

    def _listing_case_numbers(self, d_from: date, d_to: date) -> Optional[Set[str]]:
        url = (f"/en-ie/cases?decisionFrom={d_from.isoformat()}"
               f"&decisionTo={d_to.isoformat()}")
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code != 200:
                logger.warning(f"listing {d_from}..{d_to}: HTTP {resp.status_code}")
                return None
            body = resp.content.decode("utf-8", errors="replace")
            return set(CASE_LINK_RE.findall(body))
        except Exception as e:
            logger.warning(f"Error listing {d_from}..{d_to}: {e}")
            return None

    def _cases_in_window(self, d_from: date, d_to: date) -> Set[str]:
        """Collect unique case numbers in a window, splitting on the 500 cap."""
        nums = self._listing_case_numbers(d_from, d_to)
        if nums is None:
            return set()
        if len(nums) < LISTING_CAP or d_from >= d_to:
            return nums
        # Hit the cap — split the window in half by day and recurse.
        mid = d_from + (d_to - d_from) / 2
        logger.info(f"window {d_from}..{d_to} hit cap ({len(nums)}), splitting at {mid}")
        left = self._cases_in_window(d_from, mid)
        right = self._cases_in_window(mid + timedelta(days=1), d_to)
        return left | right

    def _iter_months(self, start_year: int) -> Generator[Tuple[date, date], None, None]:
        today = datetime.now(timezone.utc).date()
        y, m = start_year, 1
        while (y, m) <= (today.year, today.month):
            yield date(y, m, 1), _last_day(y, m)
            m += 1
            if m > 12:
                m, y = 1, y + 1

    # ---- per-case ----------------------------------------------------------

    def _case_page(self, num: str) -> Optional[str]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/en-ie/case/{num}")
            if resp.status_code != 200:
                return None
            return resp.content.decode("utf-8", errors="replace")
        except Exception as e:
            logger.warning(f"case {num}: {e}")
            return None

    def _fetch_report(self, path: str) -> Optional[bytes]:
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            if resp.status_code != 200:
                return None
            data = resp.content
            if not data[:5].startswith(b"%PDF"):
                return None
            return data
        except Exception as e:
            logger.warning(f"report {path}: {e}")
            return None

    @staticmethod
    def _parse_meta(page: str) -> Dict[str, Any]:
        meta: Dict[str, Any] = {}
        m = CASE_REF_RE.search(page)
        if m:
            meta["case_ref"] = m.group(1).strip()
        m = PA_REF_RE.search(page)
        if m:
            meta["pa_ref"] = m.group(1).strip()
        m = ADDRESS_RE.search(page)
        if m:
            meta["address"] = _strip_tags(m.group(1))
        # case-sub / case-summary field pairs
        for label, value in FIELD_RE.findall(page):
            key = label.strip().lower()
            val = _strip_tags(value)
            if key == "case type":
                meta["case_type"] = val
            elif key == "decision":
                meta["decision"] = val
            elif key == "date signed":
                meta["date_signed"] = val
            elif key == "development description" or key == "description":
                meta["description"] = val
        return meta

    def _build_raw(self, num: str) -> Optional[Dict[str, Any]]:
        page = self._case_page(num)
        if not page:
            return None
        rm = REPORT_LINK_RE.search(page)
        if not rm:
            return None  # no inspector's report → no full text to store
        report_path = html.unescape(rm.group(1))
        pdf = self._fetch_report(report_path)
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.warning(f"extract case {num} failed: {e}")
            return None
        if len(text.strip()) < 400:
            return None  # scanned/empty report — skip metadata-only record
        meta = self._parse_meta(page)
        meta.update({"num": num, "text": text, "report_path": report_path})
        return meta

    # ---- framework hooks ---------------------------------------------------

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yielded = False
        seen: Set[str] = set()
        for d_from, d_to in self._iter_months(FIRST_YEAR):
            nums = self._cases_in_window(d_from, d_to)
            fresh = sorted(n for n in nums if n not in seen)
            if fresh:
                logger.info(f"{d_from:%Y-%m}: {len(fresh)} new cases")
            for num in fresh:
                seen.add(num)
                raw = self._build_raw(num)
                if raw:
                    yielded = True
                    yield raw
        if not yielded:
            raise RuntimeError(
                "An Bord Pleanála listing returned 0 usable cases — site blocked or markup changed"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        start_year = max(FIRST_YEAR, since.year)
        seen: Set[str] = set()
        for d_from, d_to in self._iter_months(start_year):
            if d_to < since.date():
                continue
            for num in sorted(self._cases_in_window(d_from, d_to)):
                if num in seen:
                    continue
                seen.add(num)
                raw = self._build_raw(num)
                if raw:
                    yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        num = raw.get("num", "")
        text = _clean(raw.get("text", "") or "")
        if len(text) < 400:
            return None

        case_ref = raw.get("case_ref") or f"ABP-{num}"
        decision = raw.get("decision", "")
        address = raw.get("address", "")
        case_type = raw.get("case_type", "")

        iso_date = None
        ds = raw.get("date_signed", "")
        m = re.search(r"(\d{2})/(\d{2})/(\d{4})", ds)
        if m:
            iso_date = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

        title_bits = [f"An Bord Pleanála {case_ref}"]
        if address:
            title_bits.append(address)
        if decision:
            title_bits.append(decision)
        title = " — ".join(title_bits)[:400]

        return {
            "_id": f"IE/AnBordPleanala/{num}",
            "_source": "IE/AnBordPleanala",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": iso_date,
            "url": f"{BASE_URL}/en-ie/case/{num}",
            "case_ref": case_ref,
            "pa_ref": raw.get("pa_ref", ""),
            "case_type": case_type,
            "decision": decision,
            "address": address,
            "description": raw.get("description", ""),
            "report_url": f"{BASE_URL}{raw.get('report_path', '')}",
            "court": "An Bord Pleanála (An Coimisiún Pleanála)",
            "jurisdiction": "IE",
            "language": "en",
        }

    def test_connection(self):
        print("Testing An Bord Pleanála case database...")
        d_from, d_to = date(2024, 6, 1), date(2024, 6, 30)
        nums = self._cases_in_window(d_from, d_to)
        print(f"  2024-06: {len(nums)} cases")
        if nums:
            num = sorted(nums)[0]
            raw = self._build_raw(num)
            if raw:
                print(f"  case {num}: {len(raw['text'])} chars, ref={raw.get('case_ref')} - OK")
            else:
                print(f"  case {num}: no usable inspector report")


def main():
    scraper = AnBordPleanalaScraper()
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
