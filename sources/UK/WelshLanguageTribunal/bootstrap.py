#!/usr/bin/env python3
"""
UK/WelshLanguageTribunal -- Welsh Language Tribunal (Tribiwnlys y Gymraeg) --
Decisions.

The Welsh Language Tribunal (WLT, Tribiwnlys y Gymraeg) is the independent
statutory tribunal for Wales established under the Welsh Language (Wales) Measure
2011. It determines appeals and applications against decisions of the Welsh
Language Commissioner concerning Welsh-language standards imposed on public
bodies and other organisations. Its written, reasoned decisions are binding and
appealable on a point of law to the Upper Tribunal -- i.e. adjudicative case law
for the GB-WLS (Wales) jurisdiction. WLT decisions are NOT on the National
Archives Find Case Law service (England & Wales superior courts + reserved UK
tribunals only), so they are not covered by UK/CaseLaw. Decisions may be issued
in Welsh, English, or both (bilingual tribunal).

Site: https://welshlanguagetribunal.gov.wales (Drupal). Decisions are browsed by
April-April "tribunal year" window:

    /decisions                                    -> landing (current year)
    /previous-decisions                           -> lists the year windows
    /decisions/4/{YYYY-04--YYYY-04}               -> lists per-decision slug pages
    /{slug}                                        -> decision detail page: title
        (case ref TYG/WLT/YY/NN) + one or more born-digital decision PDFs under
        /sites/welshlanguage/files/.

Decision detail slugs take the form /tygwlt{digits}-{party}.

Strategy:
  - Read /previous-decisions to enumerate the tribunal-year windows, and also
    include the current-year window (constructed from the newest published one).
  - Page each window, collect the /tygwlt... detail-page slugs.
  - On each detail page, read the title (case ref) and the decision PDF href(s).
  - Download each born-digital PDF and extract full text with PyMuPDF (shared
    pdfplumber/pypdf fallback). No OCR needed.
  - One record per decision. Decision date: a YYMMDD prefix in the PDF filename
    if present, else the start of the tribunal-year window (YYYY-04-01). The
    "last date in the body" heuristic is deliberately NOT used -- decisions cite
    many historical/statutory dates, so it grabs the wrong date.

Data:
  - Full-text decisions, April 2017-present (~25 and growing)
  - Language: Welsh and/or English (bilingual tribunal)
  - Auth: None (free public access)
  - Licence: Open Government Licence v3.0 (Crown copyright)

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
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

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
logger = logging.getLogger("legal-data-hunter.UK.WelshLanguageTribunal")

BASE_URL = "https://welshlanguagetribunal.gov.wales"

# Case-type URL segment for the year-window listings (WLT decisions collection).
CASE_TYPE = "4"

MAX_PAGES = 40
MAX_PDFS = 6

# Year-window links (/decisions/4/2022-04--2023-04) on a listing page.
YEARWIN_RE = re.compile(r'href="(/decisions/\d+/(\d{4})-\d{2}--\d{4}-\d{2})"', re.I)
# Per-decision detail-page slug links (/tygwlt165-pembrokeshire-county-council).
SLUG_RE = re.compile(r'href="(/tygwlt[a-z0-9-]+)"', re.I)
# Decision PDF href on a detail page.
PDF_RE = re.compile(
    r'href="(/sites/welshlanguage/files/[^"]+?\.pdf)"', re.I)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
# Case reference TYG/WLT/YY/NN (separators vary: / or spaces; label case varies).
REF_RE = re.compile(r"(TYG[\s/]*WLT[\s/]*\d{1,2}[\s/]*\d{1,3})", re.I)
# YYMMDD prefix in a PDF filename, e.g. 170426-decision-...pdf.
FILE_DATE_RE = re.compile(r"/(\d{2})(\d{2})(\d{2})-[a-z]", re.I)


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


def _slug_from_path(path: str) -> str:
    return unquote(path.rstrip("/").rsplit("/", 1)[-1])


def _date_from_filename(pdf_path: str) -> Optional[str]:
    m = FILE_DATE_RE.search(pdf_path or "")
    if not m:
        return None
    yy, mm, dd = m.groups()
    try:
        year = 2000 + int(yy)
        month, day = int(mm), int(dd)
        if 1 <= month <= 12 and 1 <= day <= 31 and year <= datetime.now().year + 1:
            return f"{year:04d}-{month:02d}-{day:02d}"
    except (ValueError, TypeError):
        pass
    return None


def _pdf_text(pdf_bytes: bytes) -> str:
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


def _detect_language(text: str) -> str:
    """Crude Welsh/English detector for the bilingual tribunal. Returns 'cy',
    'en', or 'cy,en' when both appear substantially."""
    t = (text or "").lower()
    welsh = sum(t.count(w) for w in (" yr ", " a'r ", " wedi ", " sydd ",
                                     " penderfyniad", " tribiwnlys", " gymraeg",
                                     " yn ", " ei "))
    english = sum(t.count(w) for w in (" the ", " and ", " of ", " decision ",
                                       " tribunal ", " that ", " is "))
    if welsh > english * 1.5:
        return "cy"
    if english > welsh * 1.5:
        return "en"
    return "cy,en"


class WelshLanguageTribunalScraper(BaseScraper):
    """Scraper for Welsh Language Tribunal decisions (server-rendered Drupal
    tribunal-year listings + born-digital PDFs)."""

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
                "Accept-Language": "en-GB,en;q=0.9,cy;q=0.8",
            },
            timeout=90,
        )
        self._seen: set = set()

    # -- HTTP helpers ----------------------------------------------------
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

    def _fetch_pdf(self, url: str) -> Optional[bytes]:
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

    # -- discovery -------------------------------------------------------
    def _year_windows(self) -> List[str]:
        wins: List[str] = []
        start_years: List[int] = []
        for page in ("/previous-decisions", "/decisions"):
            index = self._get_html(f"{BASE_URL}{page}")
            if not index:
                continue
            for path, start in YEARWIN_RE.findall(index):
                if path not in wins:
                    wins.append(path)
                    start_years.append(int(start))
        if start_years:
            nxt = max(start_years) + 1
            cur = f"/decisions/{CASE_TYPE}/{nxt}-04--{nxt + 1}-04"
            if cur not in wins:
                wins.append(cur)
        return sorted(set(wins), reverse=True)

    def _window_start_date(self, window_path: str) -> Optional[str]:
        m = re.search(r"/(\d{4})-\d{2}--\d{4}-\d{2}$", window_path)
        if m:
            return f"{int(m.group(1)):04d}-04-01"
        return None

    def _decision_slugs(self, window_path: str) -> List[str]:
        slugs: List[str] = []
        page = 0
        while page < MAX_PAGES:
            sep = "&" if "?" in window_path else "?"
            url = f"{BASE_URL}{window_path}{sep}page={page}"
            page_html = self._get_html(url)
            if not page_html:
                break
            found = []
            for s in SLUG_RE.findall(page_html):
                if s not in slugs and s not in found:
                    found.append(s)
            if not found:
                break
            slugs.extend(found)
            if 'rel="next"' not in page_html and "pager__item--next" not in page_html:
                break
            page += 1
        return slugs

    # -- detail parsing --------------------------------------------------
    def _build_raw(self, slug_path: str, window_path: str) -> Optional[Dict[str, Any]]:
        detail = self._get_html(urljoin(BASE_URL, slug_path))
        if not detail:
            return None
        title = ""
        hm = H1_RE.search(detail)
        if hm:
            title = _strip(hm.group(1))
        if not title:
            tm = TITLE_RE.search(detail)
            if tm:
                title = _strip(tm.group(1)).split("|", 1)[0].strip()
        ref_m = REF_RE.search(title) or REF_RE.search(detail)
        reference = ""
        if ref_m:
            reference = re.sub(r"[\s/]+", "/", ref_m.group(1).upper()).strip("/")
            reference = reference.replace("TYG/WLT", "TYG/WLT")
        pdfs: List[str] = []
        for p in PDF_RE.findall(detail):
            if p not in pdfs:
                pdfs.append(p)
            if len(pdfs) >= MAX_PDFS:
                break
        if not pdfs:
            return None
        texts: List[str] = []
        file_date: Optional[str] = None
        for rel in pdfs:
            if not file_date:
                file_date = _date_from_filename(rel)
            pdf = self._fetch_pdf(urljoin(BASE_URL, rel))
            if not pdf:
                continue
            try:
                t = _pdf_text(pdf)
            except Exception as e:
                logger.debug(f"extract {rel}: {e}")
                t = ""
            if t:
                texts.append(t)
        text = "\n\n----\n\n".join(texts).strip()
        if not text:
            return None
        # Decisions cite many historical dates, so the "last date in the body"
        # heuristic is unreliable here (it grabs cited/statutory dates). Prefer a
        # YYMMDD prefix in the PDF filename, else the tribunal-year window start.
        date = file_date or self._window_start_date(window_path)
        return {
            "slug": _slug_from_path(slug_path),
            "detail_path": slug_path,
            "title": title,
            "reference": reference,
            "date": date,
            "pdf_url": urljoin(BASE_URL, pdfs[0]),
            "text": text,
        }

    # -- core ------------------------------------------------------------
    def _iter_all(self, limit: Optional[int] = None
                  ) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for window in self._year_windows():
            for slug in self._decision_slugs(window):
                if slug in self._seen:
                    continue
                self._seen.add(slug)
                raw = self._build_raw(slug, window)
                if raw:
                    produced += 1
                    yield raw
                    if limit and produced >= limit:
                        return

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for raw in self._iter_all():
            produced += 1
            yield raw
        if produced == 0:
            raise RuntimeError(
                "Welsh Language Tribunal listings returned 0 decisions — site "
                "blocked, layout changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        windows = self._year_windows()  # newest first
        for window in windows[:2]:
            for slug in self._decision_slugs(window):
                if slug in self._seen:
                    continue
                self._seen.add(slug)
                raw = self._build_raw(slug, window)
                if raw:
                    yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        slug = raw.get("slug", "")
        title = raw.get("title", "") or slug.replace("-", " ").title()
        reference = raw.get("reference", "") or slug.upper()
        return {
            "_id": f"UK-WelshLanguageTribunal-{slug}",
            "_source": "UK/WelshLanguageTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": reference,
            "pdf_url": raw.get("pdf_url", ""),
            "court": "Welsh Language Tribunal",
            "jurisdiction": "GB-WLS",
            "language": _detect_language(text),
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Welsh Language Tribunal listings...")
        windows = self._year_windows()
        print(f"  {len(windows)} tribunal-year windows: {windows}")
        for window in windows:
            slugs = self._decision_slugs(window)
            if not slugs:
                continue
            print(f"  {window}: {len(slugs)} decisions")
            raw = self._build_raw(slugs[0], window)
            if raw:
                print(f"    {raw['reference'] or raw['title'] or slugs[0]}: "
                      f"{len(raw['text'])} chars extracted - OK")
                return


def main():
    scraper = WelshLanguageTribunalScraper()
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
