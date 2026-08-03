#!/usr/bin/env python3
"""
UK/EducationTribunalWales -- Education Tribunal for Wales (Tribiwnlys Addysg
Cymru) -- Decisions.

The Education Tribunal for Wales (ETW), formerly the Special Educational Needs
Tribunal for Wales (SENTW), is the independent statutory tribunal for Wales that
determines:

  * Additional Learning Needs (ALN) appeals under the Additional Learning Needs
    and Education Tribunal (Wales) Act 2018 (and, historically, Special
    Educational Needs (SEN) appeals under the Education Act 1996), and
  * disability discrimination claims in the field of education under the
    Equality Act 2010.

Its written, reasoned decisions are binding on the parties (a local authority,
governing body and the child/parent) and appealable on a point of law to the
Upper Tribunal -- i.e. adjudicative case law for the GB-WLS (Wales)
jurisdiction. ETW decisions are NOT on the National Archives Find Case Law
service (which indexes England & Wales superior courts + reserved UK tribunals),
so they are not covered by UK/CaseLaw. They are published anonymised.

Site: https://educationtribunal.gov.wales (Drupal). Decisions are browsed by
September-September "school year" window:

    /decisions                                    -> landing (current year)
    /previous-decisions                           -> lists the school-year windows
    /decisions/3/{YYYY-09--YYYY-09}               -> lists per-decision slug pages
    /{slug}                                        -> decision detail page: title
        + one born-digital decision PDF under /sites/educationtribunal/files/.

Three decision series appear, distinguished by the detail-page slug prefix:
    additional-learning-needs-appeal-decision-*   (ALN appeals, 2018 Act)
    special-educational-needs-appeal-decision-*    (legacy SEN appeals)
    disability-discrimination-claim-decision-*     (Equality Act 2010)

Strategy:
  - Read /previous-decisions to enumerate the school-year windows, and also
    include the current-year window (constructed from the newest published one).
  - Page each window, collect the per-decision detail-page slugs.
  - On each detail page, read the title and the decision PDF href.
  - Download each born-digital PDF and extract full text with PyMuPDF (shared
    pdfplumber/pypdf fallback). No OCR needed.
  - One record per decision. Decisions are anonymised (only years survive in the
    text), so the date is the start of the school-year window (YYYY-09-01).

Data:
  - Full-text anonymised decisions, September 2020-present (~50 and growing)
  - Language: English (bilingual site; decisions are in English)
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
logger = logging.getLogger("legal-data-hunter.UK.EducationTribunalWales")

BASE_URL = "https://educationtribunal.gov.wales"

# The "case type" URL segment for the year-window listings. On this site all
# three decision series share segment 3 (the ETW decisions collection).
CASE_TYPE = "3"

MAX_PAGES = 40             # safety ceiling per year window
MAX_PDFS = 4

# Decision-series slug prefix -> human label.
SERIES_LABELS = {
    "additional-learning-needs-appeal": "Additional Learning Needs appeal",
    "special-educational-needs-appeal": "Special Educational Needs appeal",
    "disability-discrimination-claim": "Disability discrimination claim",
}

# Year-window links (/decisions/3/2023-09--2024-09) on a listing page.
YEARWIN_RE = re.compile(r'href="(/decisions/\d+/(\d{4})-\d{2}--\d{4}-\d{2})"', re.I)
# Per-decision detail-page slug links (…-decision-NN / …-claim-decision-NN).
SLUG_RE = re.compile(
    r'href="(/[a-z0-9-]*(?:decision|claim)[a-z0-9-]*)"', re.I)
# Decision PDF href on a detail page.
PDF_RE = re.compile(
    r'href="(/sites/educationtribunal/files/[^"]+?\.pdf)"', re.I)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
# School year encoded in a PDF filename, e.g. …decision07-23-24.pdf.
FILE_YEAR_RE = re.compile(r"-(\d{2})-(\d{2})\.pdf$", re.I)


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


def _slug_from_path(path: str) -> str:
    return unquote(path.rstrip("/").rsplit("/", 1)[-1])


def _series_from_slug(slug: str) -> str:
    for prefix, label in SERIES_LABELS.items():
        if slug.startswith(prefix):
            return label
    return "Education Tribunal for Wales decision"


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


class EducationTribunalWalesScraper(BaseScraper):
    """Scraper for Education Tribunal for Wales decisions (server-rendered Drupal
    school-year listings + born-digital PDFs)."""

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
        """Enumerate school-year window paths, newest first. The published set is
        on /previous-decisions; the current school year (window after the newest
        published one) is added so newly-published decisions are picked up."""
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
        # Add the current school-year window (one year after the newest known),
        # in case decisions have been published since the index last refreshed.
        if start_years:
            nxt = max(start_years) + 1
            cur = f"/decisions/{CASE_TYPE}/{nxt}-09--{nxt + 1}-09"
            if cur not in wins:
                wins.append(cur)
        # Newest first.
        return sorted(set(wins), reverse=True)

    def _window_start_date(self, window_path: str) -> Optional[str]:
        m = re.search(r"/(\d{4})-\d{2}--\d{4}-\d{2}$", window_path)
        if m:
            return f"{int(m.group(1)):04d}-09-01"
        return None

    def _decision_slugs(self, window_path: str) -> List[str]:
        """All per-decision detail slugs in one school-year window (paged, though
        a single page normally holds the whole window)."""
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
                slug = s
                if "decisions" in slug or "previous" in slug:
                    continue
                if slug not in slugs and slug not in found:
                    found.append(slug)
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
        pdfs: List[str] = []
        for p in PDF_RE.findall(detail):
            if p not in pdfs:
                pdfs.append(p)
            if len(pdfs) >= MAX_PDFS:
                break
        if not pdfs:
            return None
        texts: List[str] = []
        school_year = ""
        for rel in pdfs:
            if not school_year:
                fm = FILE_YEAR_RE.search(rel)
                if fm:
                    school_year = f"20{fm.group(1)}/{fm.group(2)}"
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
        return {
            "slug": _slug_from_path(slug_path),
            "detail_path": slug_path,
            "title": title,
            "series": _series_from_slug(_slug_from_path(slug_path)),
            "school_year": school_year,
            "date": self._window_start_date(window_path),
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
                "Education Tribunal for Wales listings returned 0 decisions — "
                "site blocked, layout changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: walk only the two most recent school-year windows
        (windows are chronological; the current + previous year cover any
        realistic `since`)."""
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
        school_year = raw.get("school_year", "")
        if school_year and school_year not in title:
            title = f"{title} ({school_year})"
        return {
            "_id": f"UK-EducationTribunalWales-{slug}",
            "_source": "UK/EducationTribunalWales",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": slug,
            "series": raw.get("series", ""),
            "school_year": school_year,
            "pdf_url": raw.get("pdf_url", ""),
            "court": "Education Tribunal for Wales",
            "jurisdiction": "GB-WLS",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Education Tribunal for Wales listings...")
        windows = self._year_windows()
        print(f"  {len(windows)} school-year windows: {windows}")
        for window in windows:
            slugs = self._decision_slugs(window)
            if not slugs:
                continue
            print(f"  {window}: {len(slugs)} decisions")
            raw = self._build_raw(slugs[0], window)
            if raw:
                print(f"    {raw['title'] or slugs[0]}: "
                      f"{len(raw['text'])} chars extracted - OK")
                return


def main():
    scraper = EducationTribunalWalesScraper()
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
