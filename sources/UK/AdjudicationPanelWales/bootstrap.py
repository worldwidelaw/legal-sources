#!/usr/bin/env python3
"""
UK/AdjudicationPanelWales -- The Adjudication Panel for Wales (Panel Dyfarnu
Cymru) -- Decision Reports.

The Adjudication Panel for Wales (APW) is the independent statutory tribunal
established under Part III of the Local Government Act 2000. It determines
allegations that elected and co-opted members of Welsh county/community councils,
national park authorities and fire authorities have breached their authority's
statutory code of conduct. Cases are referred by the Public Services Ombudsman
for Wales to a Case Tribunal (initial determination) or reach an Appeal Tribunal
(appeal against a local standards committee's finding). Its published "Decision
Reports" set out the findings and any sanction (e.g. disqualification/suspension)
-- binding, appealable adjudicative case law for the Wales (GB-WLS) jurisdiction,
NOT covered by UK/CaseLaw (Find Case Law = E&W superior courts + reserved UK
tribunals only).

Site: https://adjudicationpanel.gov.wales (Drupal 11, same platform as RPT Wales).
Decisions are browsed by tribunal type and by April-March "tribunal year" window:

    /decisions/{case_type}/%2A            -> lists the year-window links
    /decisions/{case_type}/{YYYY-04--YYYY-04}  -> lists per-decision slug pages
    /{slug}                               -> decision detail page: name, reference
        number, relevant authority, nature of allegation, hearing date, tribunal
        decision, and the decision-report PDF under
        /sites/adjudicationpanel/files/.

    case_type 1 -> Case Tribunal  (reference suffix /CT)
    case_type 2 -> Appeal Tribunal (reference suffix /AT)

Strategy (identical discovery to UK/RPTWales; APW-specific metadata + dating):
  - For each tribunal type, read the %2A index to enumerate its year windows.
  - Page each window, collect the per-decision detail-page slugs.
  - On each detail page, parse the packed <meta name="description"> metadata and
    the decision-report PDF href(s).
  - Download each born-digital PDF and extract full text with PyMuPDF (shared
    pdfplumber/pypdf fallback). No OCR needed.
  - Decision date is the "Hearing:" date from the metadata, falling back to the
    year encoded in the reference number, then the last date in the body text.

Data:
  - A few hundred full-text decision reports, 2003-present
  - Language: English (bilingual site; reports are in English)
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
logger = logging.getLogger("legal-data-hunter.UK.AdjudicationPanelWales")

BASE_URL = "https://adjudicationpanel.gov.wales"

# Tribunal type (case-type id in the URL) -> label.
PANELS: Dict[str, str] = {
    "1": "Case Tribunal",
    "2": "Appeal Tribunal",
}

MAX_PAGES = 60
MAX_PDFS = 8

YEARWIN_RE = re.compile(r'href="(/decisions/\d+/\d{4}-\d{2}--\d{4}-\d{2})"', re.I)
# Per-decision detail slugs: prefix "apw" + digits + text.
SLUG_RE = re.compile(r'href="(/apw\d{3,}[a-z0-9-]*)"', re.I)
PDF_RE = re.compile(r'href="(/sites/adjudicationpanel/files/[^"]+?\.pdf)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")

META_DESC_RE = re.compile(
    r'<meta\s+name="description"\s+content="([^"]*)"', re.I)
META_LABEL_RE = re.compile(
    r"(Name|Reference number|Relevant authority|Nature of allegation|"
    r"Hearing|Tribunal Decision)\s*:", re.I)

_MONTHS = ("January|February|March|April|May|June|July|August|September|"
           "October|November|December")
_MONTH_NUM = {m: i for i, m in enumerate(
    ("january february march april may june july august september october "
     "november december").split(), start=1)}
TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(" + _MONTHS + r")\s+(\d{4})\b", re.I)
# Reference APW/0002/2023-024/CT -> the 4-digit year after the case number.
REF_YEAR_RE = re.compile(r"/(\d{4})\b")


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


def _iso_from_textdate(s: str) -> Optional[str]:
    m = TEXT_DATE_RE.search(s or "")
    if not m:
        return None
    day, mon, year = m.group(1), m.group(2), m.group(3)
    month = _MONTH_NUM.get(mon.lower())
    if not month:
        return None
    try:
        return f"{int(year):04d}-{month:02d}-{int(day):02d}"
    except (ValueError, TypeError):
        return None


def _date_from_text_last(text: str) -> Optional[str]:
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


def _year_from_ref(ref: str) -> Optional[str]:
    """Fallback date from the 4-digit year in the reference -> YYYY-01-01."""
    for y in REF_YEAR_RE.findall(ref or ""):
        try:
            yi = int(y)
            if 1990 <= yi <= 2100:
                return f"{yi:04d}-01-01"
        except (ValueError, TypeError):
            continue
    return None


def _parse_desc(desc: str) -> Dict[str, str]:
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


def _slug_from_path(path: str) -> str:
    return unquote(path.rstrip("/").rsplit("/", 1)[-1])


class AdjudicationPanelWalesScraper(BaseScraper):
    """Scraper for Adjudication Panel for Wales decision reports (server-rendered
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
    def _build_raw(self, slug_path: str, panel_id: str) -> Optional[Dict[str, Any]]:
        detail = self._get_html(urljoin(BASE_URL, slug_path))
        if not detail:
            return None
        meta: Dict[str, str] = {}
        dm = META_DESC_RE.search(detail)
        if dm:
            fields = _parse_desc(html.unescape(dm.group(1)).strip())
            meta = fields
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
            "name": meta.get("name", ""),
            "reference": meta.get("reference_number", ""),
            "authority": meta.get("relevant_authority", ""),
            "allegation": meta.get("nature_of_allegation", ""),
            "hearing": meta.get("hearing", ""),
            "decision": meta.get("tribunal_decision", ""),
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
                "Adjudication Panel for Wales listings returned 0 decisions — "
                "site blocked, layout changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        for case_type in PANELS:
            windows = self._year_windows(case_type)
            for window in windows[:2]:  # %2A index lists newest windows first
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
        name = raw.get("name", "")
        panel_id = raw.get("panel_id", "")
        panel = PANELS.get(panel_id, "Adjudication Panel for Wales")
        title = f"{reference}: {name}".strip().rstrip(":").strip() or reference
        date = (_iso_from_textdate(raw.get("hearing", ""))
                or _year_from_ref(reference)
                or _date_from_text_last(text))
        return {
            "_id": f"UK-APW-{slug}",
            "_source": "UK/AdjudicationPanelWales",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": reference,
            "respondent": name,
            "authority": raw.get("authority", ""),
            "allegation": raw.get("allegation", ""),
            "hearing_date": raw.get("hearing", ""),
            "outcome": raw.get("decision", ""),
            "panel": panel,
            "court": "Adjudication Panel for Wales",
            "jurisdiction": "GB-WLS",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Adjudication Panel for Wales listings...")
        for case_type, label in PANELS.items():
            windows = self._year_windows(case_type)
            print(f"  panel {case_type} ({label}): {len(windows)} year windows")
            if not windows:
                continue
            for window in windows:
                slugs = self._decision_slugs(window)
                if not slugs:
                    continue
                raw = self._build_raw(slugs[0], case_type)
                if raw:
                    n = self.normalize(raw)
                    print(f"    {raw['reference'] or slugs[0]}: "
                          f"{len(n['text']) if n else 0} chars, date={n['date'] if n else None} - OK")
                    return


def main():
    scraper = AdjudicationPanelWalesScraper()
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
