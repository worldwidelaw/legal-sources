#!/usr/bin/env python3
"""
UK/AgriculturalLandTribunalWales -- Agricultural Land Tribunal for Wales
(Tribiwnlys Tir Amaethyddol Cymru) -- Decisions.

The Agricultural Land Tribunal for Wales (ALT Wales) is the independent
statutory tribunal for Wales (GB-WLS) that determines agricultural-land
disputes: applications for succession to agricultural tenancies, consents to
the operation of notices to quit, certificates of bad husbandry, and land
drainage / ditch and watercourse disputes (Agricultural Holdings Act 1986,
Agriculture (Miscellaneous Provisions) Act 1976, Land Drainage Act 1991).
Its written, reasoned decisions are adjudicative case law for Wales. ALT Wales
decisions are NOT on the National Archives Find Case Law service (England &
Wales superior courts + reserved UK tribunals only), so they are not covered by
UK/CaseLaw. The tribunal is bilingual (Welsh and/or English).

Site: https://agriculturallandtribunal.gov.wales (Drupal). Decisions are browsed
by case type, each a paginated set of April-April "tribunal year" windows:

    /decisions                                    -> landing
    /land-drainage-applications                   -> case type, window-id 3
    /tenancy-applications                         -> case type, window-id 4
    /agricultural-applications-decisions          -> case type, window-id 5
    /decisions/{3|4|5}/{YYYY-04--YYYY-04}         -> lists per-decision slug pages
    /alt-{NNNN}-{party-holding}                    -> decision detail page: title
        (case ref "ALT NNNN") + one or more decision PDFs under
        /sites/agriculturalland/files/.

Strategy:
  - Read the three case-type pages to enumerate the tribunal-year windows
    (window-ids 3/4/5), plus the constructed current-year window.
  - Page each window, collect the /alt-... detail-page slugs.
  - On each detail page, read the title (case ref) and the decision PDF href(s).
  - Extract full text: newer decisions are born-digital PDFs (PyMuPDF). The
    older 2002-2019 decisions were bulk-scanned in 2020 (image-only, no text
    layer) -> fall back to OCR (tesseract, eng+cym) rendering each page to PNG.
  - One record per decision. Decision date: the start of the tribunal-year
    window (YYYY-04-01) -- decisions cite many historical/statutory dates so the
    "last date in the body" heuristic grabs the wrong date, and the PDF upload
    folder (/files/YYYY-MM/) is the upload month, not the decision date.

Data:
  - Full-text decisions, 2002-present (~22 and growing)
  - Language: Welsh and/or English (bilingual tribunal)
  - Auth: None (free public access)
  - Licence: Open Government Licence v3.0 (Crown copyright)

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html
import shutil
import logging
import subprocess
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
logger = logging.getLogger("legal-data-hunter.UK.AgriculturalLandTribunalWales")

BASE_URL = "https://agriculturallandtribunal.gov.wales"

# The three case-type landing pages and the window-id used in their
# /decisions/{id}/{window} listing links.
CASE_TYPE_PAGES = {
    "3": "/land-drainage-applications",
    "4": "/tenancy-applications",
    "5": "/agricultural-applications-decisions",
}

MAX_PAGES = 40
MAX_PDFS = 4
OCR_MAX_PAGES = 40      # cap OCR work per decision
OCR_PAGE_TIMEOUT = 120  # seconds per page

TESSERACT = shutil.which("tesseract") or "/opt/homebrew/bin/tesseract"

# Year-window links (/decisions/5/2022-04--2023-04) on a case-type page.
YEARWIN_RE = re.compile(r'href="(/decisions/(\d+)/(\d{4})-\d{2}--\d{4}-\d{2})"', re.I)
# Per-decision detail-page slug links (/alt-6155-llertai-llangadfan-welshpool).
SLUG_RE = re.compile(r'href="(/alt-[a-z0-9-]+)"', re.I)
# Decision PDF href on a detail page.
PDF_RE = re.compile(r'href="(/sites/agriculturalland/files/[^"]+?\.pdf)"', re.I)
TITLE_RE = re.compile(r"<title>(.*?)</title>", re.I | re.S)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.I | re.S)
TAG_RE = re.compile(r"<[^>]+>")
# Case reference "ALT NNNN" (label case / separators vary).
REF_RE = re.compile(r"ALT[\s/-]*?(\d{3,5})", re.I)


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


def _slug_from_path(path: str) -> str:
    return unquote(path.rstrip("/").rsplit("/", 1)[-1])


def _ocr_langs() -> str:
    try:
        out = subprocess.run(
            [TESSERACT, "--list-langs"], capture_output=True, text=True, timeout=30
        ).stdout.split()
        have = set(out)
        return "eng+cym" if {"eng", "cym"} <= have else "eng"
    except Exception:
        return "eng"


def _ocr_pdf(pdf_bytes: bytes, langs: str) -> str:
    """OCR a scanned/image-only PDF via tesseract (stdin PNG pipe). The Homebrew
    tesseract cannot open input files directly but reads PNG bytes from stdin."""
    if fitz is None or not shutil.which(TESSERACT) and not Path(TESSERACT).exists():
        return ""
    texts: List[str] = []
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return ""
    try:
        for page in doc[:OCR_MAX_PAGES]:
            try:
                png = page.get_pixmap(dpi=200).tobytes("png")
                r = subprocess.run(
                    [TESSERACT, "stdin", "stdout", "-l", langs],
                    input=png, capture_output=True, timeout=OCR_PAGE_TIMEOUT,
                )
                texts.append(r.stdout.decode("utf-8", "replace"))
            except Exception as e:
                logger.debug(f"ocr page failed: {e}")
    finally:
        doc.close()
    return "\n".join(texts).strip()


def _pdf_text(pdf_bytes: bytes, langs: str) -> str:
    """Born-digital extraction first (PyMuPDF, pdfplumber/pypdf fallback); OCR
    fallback for image-only scans."""
    if fitz is not None:
        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            try:
                text = "\n".join(page.get_text() for page in doc).strip()
            finally:
                doc.close()
            if len(text) >= 200:
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
                    if t and len(t) >= 200:
                        return t
                except Exception:
                    continue
    except Exception:
        pass
    # Image-only scan -> OCR.
    return _ocr_pdf(pdf_bytes, langs)


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
    t = (text or "").lower()
    welsh = sum(t.count(w) for w in (" yr ", " a'r ", " wedi ", " sydd ",
                                     " penderfyniad", " tribiwnlys", " gymraeg",
                                     " yn ", " ei ", " tir "))
    english = sum(t.count(w) for w in (" the ", " and ", " of ", " decision ",
                                       " tribunal ", " that ", " is "))
    if welsh > english * 1.5:
        return "cy"
    if english > welsh * 1.5:
        return "en"
    return "cy,en"


class AgriculturalLandTribunalWalesScraper(BaseScraper):
    """Scraper for Agricultural Land Tribunal for Wales decisions (server-rendered
    Drupal tribunal-year listings + born-digital PDFs, OCR fallback for scans)."""

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
            timeout=120,
        )
        self._seen: set = set()
        self._langs = _ocr_langs()

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
        latest_by_id: Dict[str, int] = {}
        for wid, page in CASE_TYPE_PAGES.items():
            index = self._get_html(f"{BASE_URL}{page}")
            if not index:
                continue
            for path, found_id, start in YEARWIN_RE.findall(index):
                if path not in wins:
                    wins.append(path)
                y = int(start)
                if y > latest_by_id.get(found_id, 0):
                    latest_by_id[found_id] = y
        # Construct the current-year window per case type (new decisions land in
        # the latest window, which may post-date the last one on the index page).
        for wid, y in latest_by_id.items():
            cur = f"/decisions/{wid}/{y + 1}-04--{y + 2}-04"
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
        reference = f"ALT {ref_m.group(1)}" if ref_m else ""
        pdfs: List[str] = []
        for p in PDF_RE.findall(detail):
            if p not in pdfs:
                pdfs.append(p)
            if len(pdfs) >= MAX_PDFS:
                break
        if not pdfs:
            return None
        texts: List[str] = []
        for rel in pdfs:
            pdf = self._fetch_pdf(urljoin(BASE_URL, rel))
            if not pdf:
                continue
            try:
                t = _pdf_text(pdf, self._langs)
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
            "reference": reference,
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
                "Agricultural Land Tribunal for Wales listings returned 0 "
                "decisions — site blocked, layout changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        windows = self._year_windows()  # newest first
        for window in windows[:6]:
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
            "_id": f"UK-AgriculturalLandTribunalWales-{slug}",
            "_source": "UK/AgriculturalLandTribunalWales",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": reference,
            "pdf_url": raw.get("pdf_url", ""),
            "court": "Agricultural Land Tribunal for Wales",
            "jurisdiction": "GB-WLS",
            "language": _detect_language(text),
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Agricultural Land Tribunal for Wales listings...")
        print(f"  OCR languages: {self._langs}")
        windows = self._year_windows()
        print(f"  {len(windows)} tribunal-year windows")
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
    scraper = AgriculturalLandTribunalWalesScraper()
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
