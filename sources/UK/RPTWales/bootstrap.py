#!/usr/bin/env python3
"""
UK/RPTWales -- Residential Property Tribunal Wales (Tribiwnlys Eiddo Preswyl) --
Decisions.

The Residential Property Tribunal Wales (RPTW) is the independent statutory
tribunal for Wales that resolves disputes about private rented and leasehold
residential property under, among others, the Housing Act 2004, the Landlord and
Tenant Act 1985/1987, the Commonhold and Leasehold Reform Act 2002, the Rent Act
1977 and the Leasehold Reform, Housing and Urban Development Act 1993. It sits in
three panels, each publishing its own decision series:

    Panel (case-type id)                          slug   ~coverage
    ------------------------------------------------------------------
    2  Leasehold Valuation Tribunals              lvt    2012-present
    1  Rent Assessment Committees                 rac    2012-present
    4  Residential Property Tribunals             rpt    2012-present
    ------------------------------------------------------------------
    (case-type id 3 exists in the URL scheme but publishes no decisions.)

Its written decisions (with statement of reasons) are binding, appealable to the
Upper Tribunal (Lands Chamber) -- i.e. adjudicative case law for the GB-WLS
(Wales) jurisdiction, NOT covered by UK/CaseLaw (which is England & Wales
superior courts + reserved UK tribunals, indexed via the National Archives Find
Case Law service; RPTW decisions are not on that service).

Site: https://residentialpropertytribunal.gov.wales (Drupal 11). Decisions are
browsed by panel and by April-March "tribunal year" window:

    /decisions/{case_type}/%2A            -> lists the year-window links
    /decisions/{case_type}/{YYYY-04--YYYY-04}  -> lists per-decision slug pages
    /{slug}                               -> decision detail page: reference
        number, Act, case type, property, and one or more born-digital decision
        PDFs under /sites/residentialproperty/files/.

Strategy:
  - For each panel, read the %2A index to enumerate its year windows.
  - Page each year window, collect the per-decision detail-page slugs.
  - On each detail page, parse the reference number / Act / case type / property
    metadata and the decision PDF href(s).
  - Download each born-digital PDF and extract full text with PyMuPDF (shared
    pdfplumber/pypdf fallback). No OCR needed.
  - One record per decision. Decision date is taken from the signed date at the
    foot of the decision text, falling back to the month/year encoded in the
    reference number (NNNN/MM/YY).

Data:
  - Several thousand full-text decisions, April 2012-present
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
logger = logging.getLogger("legal-data-hunter.UK.RPTWales")

BASE_URL = "https://residentialpropertytribunal.gov.wales"

# Panel (case-type id) -> human label. Ids confirmed live; id 3 publishes nothing.
PANELS: Dict[str, str] = {
    "2": "Leasehold Valuation Tribunal",
    "1": "Rent Assessment Committee",
    "4": "Residential Property Tribunal",
}

MAX_PAGES = 60             # safety ceiling per year window
MAX_PDFS = 8

# Year-window links on a /decisions/{type}/%2A index (e.g. 2023-04--2024-04).
YEARWIN_RE = re.compile(r'href="(/decisions/\d+/\d{4}-\d{2}--\d{4}-\d{2})"', re.I)
# Per-decision detail-page slug links (prefix rac/lvt/rpt + digits + text).
SLUG_RE = re.compile(r'href="(/[a-z]{2,5}\d{4,}[a-z0-9-]*)"', re.I)
# Decision PDF href on a detail page.
PDF_RE = re.compile(r'href="(/sites/residentialproperty/files/[^"]+?\.pdf)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")

# Detail-page metadata: all fields are packed into the <meta name="description">
# content as "Label: value" segments, e.g.
#   "Reference number: LVT/0002/05/23 Act: ... Case type: ... Property: ..."
# The whitespace between a value and the next label is inconsistent (sometimes
# absent) and "Case type" is capitalised variously, so we split on the labels
# rather than fixed separators.
META_DESC_RE = re.compile(
    r'<meta\s+name="description"\s+content="([^"]*)"', re.I)
META_LABEL_RE = re.compile(
    r"(Reference number|Act|Case type|Property|Applicant|Respondent|"
    r"Representative|Landlord|Tenant)\s*:", re.I)


def _parse_desc(desc: str) -> Dict[str, str]:
    """Split the packed 'Label: value Label: value ...' description into fields
    keyed by lower_snake label."""
    out: Dict[str, str] = {}
    matches = list(META_LABEL_RE.finditer(desc))
    for i, m in enumerate(matches):
        key = m.group(1).lower().replace(" ", "_")
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(desc)
        val = desc[start:end].strip()
        if val and key not in out:
            out[key] = val
    return out

_MONTHS = ("January|February|March|April|May|June|July|August|September|"
           "October|November|December")
_MONTH_NUM = {m: i for i, m in enumerate(
    ("january february march april may june july august september october "
     "november december").split(), start=1)}
TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(" + _MONTHS + r")\s+(\d{4})\b", re.I)
# Reference number NNNN/MM/YY -> month + 2-digit year (e.g. LVT/0002/05/23).
REF_DATE_RE = re.compile(r"/(\d{4})/(\d{2})/(\d{2})\b")


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


def _date_from_text(text: str) -> Optional[str]:
    """Decision date: the last '<day> <Month> <year>' in the body (RPTW
    decisions are signed and dated at the foot)."""
    matches = TEXT_DATE_RE.findall(text or "")
    if not matches:
        return None
    day, mon, year = matches[-1]
    month = _MONTH_NUM.get(mon.lower())
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def _date_from_ref(ref: str) -> Optional[str]:
    """Fallback date from the reference number NNNN/MM/YY -> YYYY-MM-01."""
    m = REF_DATE_RE.search(ref or "")
    if not m:
        return None
    _, mm, yy = m.groups()
    try:
        month = int(mm)
        year = 2000 + int(yy)
        if 1 <= month <= 12:
            return f"{year:04d}-{month:02d}-01"
    except (ValueError, TypeError):
        pass
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


def _slug_from_path(path: str) -> str:
    return unquote(path.rstrip("/").rsplit("/", 1)[-1])


class RPTWalesScraper(BaseScraper):
    """Scraper for Residential Property Tribunal Wales decisions (server-rendered
    Drupal year-window listings + born-digital PDFs)."""

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
    def _year_windows(self, case_type: str) -> List[str]:
        index = self._get_html(f"{BASE_URL}/decisions/{case_type}/%2A")
        if not index:
            return []
        wins = []
        for w in YEARWIN_RE.findall(index):
            if w not in wins:
                wins.append(w)
        return wins

    def _decision_slugs(self, window_path: str) -> List[str]:
        """All per-decision detail slugs in one year window (paged, though a
        single page normally holds the whole window)."""
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
                if slug not in slugs and slug not in found:
                    found.append(slug)
            if not found:
                break
            slugs.extend(found)
            # Year windows are not paginated in practice; stop after page 0
            # unless a "next" pager link is present.
            if 'rel="next"' not in page_html and "pager__item--next" not in page_html:
                break
            page += 1
        return slugs

    # -- detail parsing --------------------------------------------------
    def _build_raw(self, slug_path: str, panel_id: str) -> Optional[Dict[str, Any]]:
        detail = self._get_html(urljoin(BASE_URL, slug_path))
        if not detail:
            return None
        meta: Dict[str, str] = {}
        dm = META_DESC_RE.search(detail)
        if dm:
            desc = html.unescape(dm.group(1)).strip()
            fields = _parse_desc(desc)
            meta["reference"] = fields.get("reference_number", "")
            meta["act"] = fields.get("act", "")
            meta["case_type"] = fields.get("case_type", "")
            meta["property"] = fields.get("property") or fields.get("applicant", "")
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
            "panel_id": panel_id,
            "reference": meta.get("reference", ""),
            "act": meta.get("act", ""),
            "case_type": meta.get("case_type", ""),
            "property": meta.get("property", ""),
            "text": text,
        }

    # -- core ------------------------------------------------------------
    def _iter_panel(self, case_type: str,
                    limit: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for window in self._year_windows(case_type):
            for slug in self._decision_slugs(window):
                if slug in self._seen:
                    continue
                self._seen.add(slug)
                raw = self._build_raw(slug, case_type)
                if raw:
                    produced += 1
                    yield raw
                    if limit and produced >= limit:
                        return

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        for case_type in PANELS:
            for raw in self._iter_panel(case_type):
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "RPT Wales listings returned 0 decisions — site blocked, layout "
                "changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: walk only the most recent tribunal-year window per
        panel (windows are chronological; the current + previous year cover any
        realistic `since`)."""
        for case_type in PANELS:
            windows = self._year_windows(case_type)
            for window in windows[-2:]:
                for slug in self._decision_slugs(window):
                    if slug in self._seen:
                        continue
                    self._seen.add(slug)
                    raw = self._build_raw(slug, case_type)
                    if raw:
                        yield raw

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        slug = raw.get("slug", "")
        reference = raw.get("reference") or slug.upper()
        prop = raw.get("property", "")
        panel_id = raw.get("panel_id", "")
        panel = PANELS.get(panel_id, "Residential Property Tribunal Wales")
        title = f"{reference}: {prop}".strip().rstrip(":").strip() or reference
        # The reference number encodes the registration month/year (NNNN/MM/YY)
        # and is reliable; the last date in the body text is often a cited or
        # future compliance date, so we prefer the reference-derived date.
        date = _date_from_ref(reference) or _date_from_text(text)
        return {
            "_id": f"UK-RPTWales-{slug}",
            "_source": "UK/RPTWales",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": reference,
            "property": prop,
            "act": raw.get("act", ""),
            "case_type": raw.get("case_type", ""),
            "panel": panel,
            "court": "Residential Property Tribunal Wales",
            "jurisdiction": "GB-WLS",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Residential Property Tribunal Wales listings...")
        for case_type, label in PANELS.items():
            windows = self._year_windows(case_type)
            print(f"  panel {case_type} ({label}): {len(windows)} year windows")
            if not windows:
                continue
            slugs = self._decision_slugs(windows[-2] if len(windows) > 1 else windows[-1])
            print(f"    latest window: {len(slugs)} decisions")
            if slugs:
                raw = self._build_raw(slugs[0], case_type)
                if raw:
                    print(f"    {raw['reference'] or slugs[0]}: "
                          f"{len(raw['text'])} chars extracted - OK")
                    return


def main():
    scraper = RPTWalesScraper()
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
