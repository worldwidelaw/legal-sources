#!/usr/bin/env python3
"""
UK/GOC -- General Optical Council -- Fitness to Practise hearing determinations.

The General Optical Council (GOC) is the UK statutory regulator for
optometrists, dispensing opticians, student registrants and optical businesses
(~30,000 registrants). Fitness to practise concerns are heard by the independent
GOC Fitness to Practise Committee under the Opticians Act 1989 and the General
Optical Council (Fitness to Practise) Rules 2013. Each concluded hearing
publishes a reasoned DETERMINATION setting out the allegation, the facts found
proved, whether the registrant's fitness to practise is impaired, and the
sanction / order imposed (erasure, suspension, conditions, warning) or the
interim order made. These are binding professional-regulator adjudications =
case law, distinct from UK/GMC (doctors), UK/GDC (dentists), UK/SDT
(solicitors), UK/BTAS (barristers), UK/HCPTS (health & care professions),
UK/NMC (nurses/midwives), UK/GPhC (pharmacists) and UK/SocialWorkEngland.

Access & structure (all public, no auth):
  - optical.org serves the "Past hearings and outcomes" page as a single static
    HTML document:
      https://optical.org/raising-concerns/hearings/past-hearings.html
    It groups every published hearing under year -> month collapsible panels,
    each holding a <table class="table"> whose data rows are:
        Hearing date | Registrant name | Outcome | Decision (<a href> link)
    The month header (e.g. "July 2026") supplies the year for date cells that
    omit it (e.g. "01 July").
  - Each Decision link points at a born-digital PDF served from
        https://optical.org/asset/{GUID}/
    (content-type application/pdf, real text layer, no OCR needed): a structured
    header (F(NN)NN case reference / registrant name + registration number /
    hearing type / committee members / legal & clinical advisers) followed by
    the reasoned determination.
  - The page exposes a rolling window (~12 months for warnings / suspension /
    conditions, up to 5 years for erasure under the GOC disclosure policy), so
    one run captures the current window (~80 determinations spanning ~2.5 years)
    and re-runs accumulate the record (the pipeline dedups on _id = the stable
    asset GUID).

Strategy:
  - Fetch the past-hearings page; walk it in document order tracking the current
    year -> month heading; for each table row with a Decision asset link,
    download the PDF, extract the text layer (PyMuPDF, with a shared
    pdfplumber/pypdf fallback) and yield full text with the row metadata.

Data:
  - ~80 full-text fitness-to-practise determinations in the live window.
    Language: English. Auth: none.

Usage:
  python bootstrap.py bootstrap          # Full pull (current published window)
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent hearings first)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    import fitz  # PyMuPDF
except Exception:  # pragma: no cover
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.GOC")

SITE_BASE = "https://optical.org"
PAST_HEARINGS_URL = SITE_BASE + "/raising-concerns/hearings/past-hearings.html"

_WS_RE = re.compile(r"[ \t]+")
_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
# collapsible month/year header, e.g. "July 2026"
_HEADER_RE = re.compile(
    r'class="collapsible-header-link"[^>]*>(.*?)</a>', re.S | re.I)
_TABLE_RE = re.compile(r"<table[^>]*>(.*?)</table>", re.S | re.I)
_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.S | re.I)
_ASSET_RE = re.compile(
    r'href="(https?://optical\.org/asset/[^"]+)"', re.I)


def _pdf_text(pdf_bytes: bytes) -> str:
    """Full text of a born-digital determination PDF via PyMuPDF, with a shared
    pdfplumber/pypdf fallback."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 80:
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
                    if t and len(t) >= 80:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    return ""


def _strip_tags(fragment: str) -> str:
    return html.unescape(re.sub(r"<[^>]+>", " ", fragment or ""))


def _clean(text: str) -> str:
    text = html.unescape(text or "").replace("\r", "").replace("\xa0", " ")
    lines = [_WS_RE.sub(" ", ln).rstrip() for ln in text.split("\n")]
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


def _cell_text(fragment: str) -> str:
    return _WS_RE.sub(" ", _strip_tags(fragment)).strip()


def _parse_date(cell: str, fallback_year: Optional[int],
                fallback_month: Optional[int]) -> Optional[str]:
    """Parse a hearing-date cell like '01 July', '12 June 2024',
    '29 May - 2 June' -> ISO 'YYYY-MM-DD'. Uses the last date in a range and
    falls back to the month/year of the collapsible header."""
    if cell:
        cell = cell.strip()
        # collect all "DD Month [YYYY]" occurrences; prefer the last (range end)
        matches = re.findall(
            r"(\d{1,2})\s*[-–]?\s*(?:\d{1,2}\s+)?"  # optional leading range part
            r"([A-Za-z]+)\s*(\d{4})?", cell)
        best = None
        for d, mon, y in matches:
            mi = _MONTHS.get(mon.lower())
            if not mi:
                continue
            year = int(y) if y else fallback_year
            if not year:
                continue
            try:
                best = f"{year:04d}-{mi:02d}-{int(d):02d}"
            except Exception:
                continue
        if best:
            return best
    if fallback_year and fallback_month:
        return f"{fallback_year:04d}-{fallback_month:02d}-01"
    return None


def _asset_guid(url: str) -> str:
    m = re.search(r"/asset/([^/]+)/?", url)
    guid = unquote(m.group(1)) if m else url
    return guid.strip().upper()


class GOCScraper(BaseScraper):
    """Scraper for General Optical Council fitness-to-practise determinations."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=SITE_BASE,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-GB,en;q=0.9",
                "Referer": SITE_BASE + "/",
            },
            timeout=60,
            respect_robots=False,
        )

    # -- HTTP ------------------------------------------------------------
    def _get(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"{url}: HTTP {resp.status_code}")
            return None
        return resp.content

    def _get_pdf(self, url: str) -> Optional[bytes]:
        body = self._get(url)
        if not body:
            return None
        if not body[:5].startswith(b"%PDF"):
            logger.debug(f"asset {url}: not a PDF")
            return None
        return body

    # -- parsing ---------------------------------------------------------
    def _iter_rows(self, page_html: str) -> Generator[Dict[str, Any], None, None]:
        """Walk the page in document order, tracking the current month/year
        collapsible header, and yield one dict per data row with an asset link."""
        # Split on collapsible headers so each table inherits its header's month.
        parts = _HEADER_RE.split(page_html)
        # parts[0] = preamble; then alternating (header_text, chunk)
        cur_year: Optional[int] = None
        cur_month: Optional[int] = None
        for idx in range(1, len(parts), 2):
            header_txt = _cell_text(parts[idx])
            chunk = parts[idx + 1] if idx + 1 < len(parts) else ""
            hm = re.search(r"([A-Za-z]+)\s+(\d{4})", header_txt)
            hy = re.search(r"\b(20\d{2})\b", header_txt)
            if hm:
                cur_month = _MONTHS.get(hm.group(1).lower(), cur_month)
                cur_year = int(hm.group(2))
            elif hy:
                cur_year = int(hy.group(1))
            # only tables inside this chunk (up to the next header, guaranteed by
            # split) belong to this month
            for tbl in _TABLE_RE.findall(chunk):
                for row in _ROW_RE.findall(tbl):
                    if "/asset/" not in row:
                        continue
                    am = _ASSET_RE.search(row)
                    if not am:
                        continue
                    cells = [_cell_text(c) for c in _CELL_RE.findall(row)]
                    cells = cells + ["", "", "", ""]
                    date_cell, name, outcome, decision = cells[0], cells[1], cells[2], cells[3]
                    if not name and not decision:
                        continue
                    yield {
                        "asset_url": am.group(1),
                        "date_cell": date_cell,
                        "name": name,
                        "outcome": outcome,
                        "decision_label": decision,
                        "year": cur_year,
                        "month": cur_month,
                    }

    def _list_rows(self) -> List[Dict[str, Any]]:
        body = self._get(PAST_HEARINGS_URL)
        if not body:
            raise RuntimeError(
                "GOC past-hearings page unreachable — optical.org blocked or "
                "the URL changed")
        page_html = body.decode("utf-8", errors="replace")
        rows = list(self._iter_rows(page_html))
        if not rows:
            raise RuntimeError(
                "GOC past-hearings page returned no decision rows — the page "
                "layout (collapsible month tables / asset links) changed")
        # de-duplicate on the asset GUID, keep first (newest, page is newest-first)
        seen = set()
        uniq = []
        for r in rows:
            g = _asset_guid(r["asset_url"])
            if g in seen:
                continue
            seen.add(g)
            uniq.append(r)
        logger.info(f"GOC past hearings: {len(uniq)} decision documents")
        return uniq

    def _build_raw(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        pdf = self._get_pdf(row["asset_url"])
        if not pdf:
            return None
        text = _clean(_pdf_text(pdf))
        if len(text) < 150:
            return None
        date = _parse_date(row.get("date_cell"), row.get("year"),
                           row.get("month"))
        return {
            "guid": _asset_guid(row["asset_url"]),
            "asset_url": row["asset_url"],
            "text": text,
            "date": date,
            "name": (row.get("name") or "").strip() or None,
            "outcome": (row.get("outcome") or "").strip() or None,
            "decision_label": (row.get("decision_label") or "").strip() or None,
        }

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for row in self._list_rows():
            raw = self._build_raw(row)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "GOC listed hearings but extracted 0 determinations — the asset "
                "PDF scheme changed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for row in self._list_rows():
            raw = self._build_raw(row)
            if not raw:
                continue
            d = raw.get("date")
            if d:
                try:
                    if datetime.strptime(d, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 150:
            return None
        # registration number appears in the determination header alongside the
        # registrant name, e.g. "(01-44561)" (optometrist/DO) or "(D-11927)".
        reg = None
        mreg = re.search(r"\(([A-Z]?-?\d[\d-]{3,})\)", text[:1500])
        if mreg:
            reg = mreg.group(1).strip("-")
        name = raw.get("name") or "GOC registrant"
        label = raw.get("decision_label") or "Fitness to Practise determination"
        title = f"{name} — {label}"
        if raw.get("date"):
            title += f" ({raw['date']})"
        return {
            "_id": f"UK-GOC-{raw['guid']}",
            "_source": "UK/GOC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("asset_url"),
            "registrant": name,
            "registration_number": reg,
            "hearing_type": label,
            "outcome": raw.get("outcome"),
            "court": "General Optical Council — Fitness to Practise Committee",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing GOC past-hearings page...")
        rows = self._list_rows()
        print(f"  Listed {len(rows)} decision documents")
        got = 0
        for row in rows:
            raw = self._build_raw(row)
            if raw:
                got += 1
                print(f"  {raw.get('name')} [{raw.get('decision_label')}] "
                      f"{raw.get('date')}: {len(raw['text'])} chars - OK")
            if got >= 3:
                break
        if got == 0:
            print("  No determinations extracted — check asset PDF access")


def main():
    scraper = GOCScraper()
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
