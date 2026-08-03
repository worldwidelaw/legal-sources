#!/usr/bin/env python3
"""
UK/GPhC -- General Pharmaceutical Council -- Fitness to Practise determinations.

The General Pharmaceutical Council (GPhC) is the UK statutory regulator for
~60,000 pharmacists and pharmacy technicians and ~14,000 registered pharmacies
in Great Britain. Its Fitness to Practise Committee sits under the Pharmacy
Order 2010; concluded hearings publish a reasoned "determination" setting out
the allegation/charges, the facts found proved, whether the registrant's fitness
to practise is impaired and the sanction/order imposed (removal, suspension,
conditions, warning) or the interim order made. These are binding
professional-regulator adjudications = case law, distinct from UK/GMC (doctors),
UK/SDT (solicitors), UK/BTAS (barristers), UK/HCPTS (health & care professions),
UK/NMC (nurses/midwives), UK/GDC (dentists), UK/SocialWorkEngland.

Access & structure (all public, no auth):
  - The GPhC Hearings page lists forthcoming and past hearings in a Drupal Views
    table. Each concluded past-hearing row carries the hearing date, registrant
    name, category (pharmacist / pharmacy technician), registration number, type
    of hearing and outcome, plus a "Read determination" link to a born-digital
    PDF at https://files.pharmacyregulation.org/determinations/D{NNNNNN}/
    D{NNNNNN}.pdf . These PDFs have a real text layer (no OCR): a structured
    header (committee / hearing type / dates / registrant name / registration
    number / part of the register / type of case / committee members /
    representation) followed by the numbered reasoned determination.
  - GPhC publishes determinations only where fitness to practise is impaired and
    keeps each on the website for ~12 months, so a single run captures the
    current rolling window (~240 determinations) and re-runs accumulate the
    record (the pipeline dedups on _id = the stable determination number).
  - www.pharmacyregulation.org UA-gates plain clients (HTTP 403) but serves a
    browser User-Agent (HTTP 200); the whole past-hearings window renders on the
    single Hearings page (no pagination).

Strategy:
  - GET the Hearings page with a browser UA; parse every past-hearing row;
    download each determination PDF; extract the text layer (PyMuPDF, with a
    shared pdfplumber/pypdf fallback) and yield full text with the row metadata.

Data:
  - ~240 full-text fitness-to-practise determinations in the live window.
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
from typing import Generator, Optional, Dict, Any, List

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
logger = logging.getLogger("legal-data-hunter.UK.GPhC")

BASE_URL = "https://www.pharmacyregulation.org"
HEARINGS_URL = ("/patients-and-public/reporting-concerns/investigating-concerns/"
                "hearings")

_WS_RE = re.compile(r"[ \t]+")
_TAG_RE = re.compile(r"<[^>]+>")
_ROW_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.I | re.S)
_TD_RE = re.compile(r"<td\b[^>]*>(.*?)</td>", re.I | re.S)
_DET_RE = re.compile(
    r'href="(https://files\.pharmacyregulation\.org/determinations/'
    r'D\d+/D\d+\.pdf)"', re.I)
_DNUM_RE = re.compile(r"/determinations/(D\d+)/", re.I)
_TIME_RE = re.compile(r'datetime="([^"]+)"', re.I)


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


def _cell_text(cell_html: str) -> str:
    """Plain text of a table cell, dropping the download-link boilerplate."""
    # cut the <ul>...</ul> download list from the outcome cell
    cell_html = re.split(r"<ul\b", cell_html, maxsplit=1, flags=re.I)[0]
    txt = _TAG_RE.sub(" ", cell_html)
    return _WS_RE.sub(" ", html.unescape(txt)).strip()


class GPhCScraper(BaseScraper):
    """Scraper for GPhC fitness-to-practise determination PDFs."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
            timeout=60,
            respect_robots=False,
        )

    # -- HTTP ------------------------------------------------------------
    def _get_html(self, url: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.debug(f"GET {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"{url}: HTTP {resp.status_code}")
            return None
        return resp.content.decode("utf-8", "replace")

    def _get_pdf(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
        except Exception as e:
            logger.warning(f"GET pdf {url} failed: {e}")
            return None
        if resp.status_code != 200:
            logger.debug(f"pdf {url}: HTTP {resp.status_code}")
            return None
        body = resp.content
        if not body[:5].startswith(b"%PDF"):
            logger.debug(f"pdf {url}: not a PDF")
            return None
        return body

    # -- enumeration -----------------------------------------------------
    def _rows(self) -> List[Dict[str, Any]]:
        page = self._get_html(BASE_URL + HEARINGS_URL)
        if not page:
            raise RuntimeError(
                "GPhC hearings page unreachable — pharmacyregulation.org blocked "
                "the request (needs a browser User-Agent)")
        rows, seen = [], set()
        for rm in _ROW_RE.finditer(page):
            row_html = rm.group(1)
            dm = _DET_RE.search(row_html)
            if not dm:
                continue  # forthcoming hearing / no determination published
            det_urls = []
            for u in _DET_RE.findall(row_html):
                if u not in det_urls:
                    det_urls.append(u)
            dnum = _DNUM_RE.search(det_urls[0]).group(1)
            if dnum in seen:
                continue
            seen.add(dnum)
            tds = _TD_RE.findall(row_html)
            cells = [_cell_text(td) for td in tds]
            # column order: date, name, category, registration number, type, outcome
            tm = _TIME_RE.search(row_html)
            iso_date = None
            if tm:
                iso_date = tm.group(1)[:10]
            rows.append({
                "dnum": dnum,
                "det_urls": det_urls,
                "date": iso_date,
                "name": cells[1] if len(cells) > 1 else None,
                "profession": cells[2] if len(cells) > 2 else None,
                "registration_number": cells[3] if len(cells) > 3 else None,
                "hearing_type": cells[4] if len(cells) > 4 else None,
                "outcome": cells[5] if len(cells) > 5 else None,
            })
        if not rows:
            raise RuntimeError(
                "GPhC hearings page returned 0 determination rows — the Views "
                "table layout changed or determinations were withdrawn")
        return rows

    def _build_raw(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        parts = []
        for url in row["det_urls"]:
            pdf = self._get_pdf(url)
            if not pdf:
                continue
            t = _clean(_pdf_text(pdf))
            if len(t) >= 120:
                parts.append(t)
        if not parts:
            return None
        text = "\n\n".join(parts)
        if len(text) < 150:
            return None
        raw = dict(row)
        raw["text"] = text
        raw["url"] = row["det_urls"][0]
        return raw

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for row in self._rows():
            raw = self._build_raw(row)
            if raw:
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "GPhC found determination rows but extracted 0 texts — the PDF "
                "host or layout changed")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        since_date = since.date()
        for row in self._rows():
            if row.get("date"):
                try:
                    if datetime.strptime(row["date"], "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            raw = self._build_raw(row)
            if raw:
                yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 150:
            return None
        name = (raw.get("name") or "").strip() or "GPhC registrant"
        htype = (raw.get("hearing_type") or "").strip() or "Fitness to Practise determination"
        title = f"{name} — {htype}"
        if raw.get("date"):
            title += f" ({raw['date']})"
        return {
            "_id": f"UK-GPhC-{raw['dnum']}",
            "_source": "UK/GPhC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "registrant": name,
            "registration_number": (raw.get("registration_number") or "").strip() or None,
            "profession": (raw.get("profession") or "").strip() or None,
            "hearing_type": htype,
            "outcome": (raw.get("outcome") or "").strip() or None,
            "determination_number": raw["dnum"],
            "court": "General Pharmaceutical Council — Fitness to Practise Committee",
            "jurisdiction": "GB",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing GPhC hearings-page enumeration...")
        rows = self._rows()
        print(f"  {len(rows)} determination rows in the live window")
        got = 0
        for row in rows:
            raw = self._build_raw(row)
            if raw:
                got += 1
                print(f"  {raw.get('name')} [{raw.get('hearing_type')}] "
                      f"{raw.get('date')}: {len(raw['text'])} chars - OK")
            if got >= 3:
                break
        if got == 0:
            print("  No determinations extracted — check PDF host access")


def main():
    scraper = GPhCScraper()
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
