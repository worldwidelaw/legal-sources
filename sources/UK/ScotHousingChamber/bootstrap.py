#!/usr/bin/env python3
"""
UK/ScotHousingChamber -- First-tier Tribunal for Scotland (Housing and Property
Chamber) -- Written Decisions.

The Housing and Property Chamber of the First-tier Tribunal for Scotland is the
statutory tribunal that determines disputes between landlords, tenants,
homeowners, property factors and letting agents under Scottish housing law
(Private Housing (Tenancies) (Scotland) Act 2016, Housing (Scotland) Act 1988,
Property Factors (Scotland) Act 2011, Letting Agent Code of Practice, etc.). Its
"Written Decisions (with Statement of Reasons)" are binding, appealable to the
Upper Tribunal for Scotland -- i.e. adjudicative case law for the GB-SCT
jurisdiction (NOT covered by UK/CaseLaw, which is England & Wales + reserved UK
tribunals only).

The Chamber publishes every written decision on the Scottish Courts and Tribunals
Service (SCTS) subsite housingandpropertychamber.scot. Decisions are organised
into six server-rendered Drupal "Views" listings, one per application category,
each a paginated HTML table whose rows carry the Chamber reference, hearing date,
parties and one or more born-digital decision PDFs under /sites/default/files/.

    Category (Drupal listing)                       approx. decisions
    ------------------------------------------------------------------
    Evictions & civil proceedings                   ~16,300
    Other private-tenancy applications              ~2,300
    Property factors                                ~1,400
    Rent (terms / prescribed property costs)          ~640
    Right of entry                                    ~960
    Letting agents                                    ~340
    ------------------------------------------------------------------
    Total                                           ~21,000+ (2017-present)

Strategy:
  - Discover the six category listing URLs from the /previous-tribunal-decisions
    hub (fall back to the hard-coded canonical paths).
  - Page each listing (?page=N, Drupal 0-indexed) and parse each table row for
    the Chamber ref, detail-page slug, hearing date (<time datetime>) and the
    decision PDF href(s).
  - Download each born-digital PDF and extract its full text with PyMuPDF (no
    OCR needed; the shared pdfplumber/pypdf helper is used as a fallback).
  - One record per decision row; multiple attachment PDFs on a row are extracted
    and concatenated.

Data:
  - ~21,000+ written decisions, 2017-present
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
logger = logging.getLogger("legal-data-hunter.UK.ScotHousingChamber")

BASE_URL = "https://www.housingandpropertychamber.scot"
HUB = "/previous-tribunal-decisions"

# Canonical category listing paths (each a Drupal View; paths confirmed live).
# Some publisher links redirect to these; the HttpClient follows redirects.
CATEGORIES: Dict[str, str] = {
    "eviction-and-civil-proceedings":
        "/apply-tribunal/evictions-and-civil-proceedings/eviction-and-civil-proceedings-decisions",
    "letting-agents":
        "/apply-tribunal/letting-agents/letting-agents-decisions",
    "property-factors":
        "/apply-tribunal/property-factors/property-factors-decisions",
    "rent":
        "/apply-tribunal/rent-terms-prescribed-property-costs/rent-decisions",
    "right-of-entry":
        "/apply-tribunal/right-entry/right-entry-decisions",
    "other-private-tenancy":
        "/apply-tribunal/other-private-tenancy-applications/other-private-tenancy-applications-decisions",
}

CATEGORY_LABELS = {
    "eviction-and-civil-proceedings": "Eviction and civil proceedings",
    "letting-agents": "Letting agent (Code of Practice)",
    "property-factors": "Property factors",
    "rent": "Rent (terms / prescribed property costs)",
    "right-of-entry": "Right of entry",
    "other-private-tenancy": "Other private tenancy applications",
}

MAX_PAGES = 4000            # safety ceiling per category
MAX_PDFS_PER_ROW = 8

# One <tr> ... </tr> table row.
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
# The Chamber-reference anchor: first link into a "...-decisions/{slug}" detail
# page (the listing path is e.g. ".../eviction-and-civil-proceedings-decisions").
REF_RE = re.compile(
    r'<a\s+href="([^"]*decisions/[^"/]+)"[^>]*>(.*?)</a>', re.S | re.I)
TIME_RE = re.compile(r'<time[^>]*datetime="([^"]+)"', re.I)
PDF_RE = re.compile(r'href="(/sites/default/files/[^"]+?\.pdf)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")

_MONTHS = ("January|February|March|April|May|June|July|August|September|"
           "October|November|December")
_MONTH_NUM = {m: i for i, m in enumerate(
    ("january february march april may june july august september october "
     "november december").split(), start=1)}
TEXT_DATE_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+(" + _MONTHS + r")\s+(\d{4})\b", re.I)


def _date_from_text(text: str) -> Optional[str]:
    """Fallback decision date: the last '<day> <Month> <year>' in the text
    (Housing & Property Chamber decisions are signed/dated at the foot)."""
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


def _strip(s: str) -> str:
    return html.unescape(TAG_RE.sub("", s or "")).strip()


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
    # Fallback: shared extractor (opendataloader / pdfplumber / pypdf).
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


class ScotHousingChamberScraper(BaseScraper):
    """Scraper for the First-tier Tribunal for Scotland (Housing and Property
    Chamber) written decisions (server-rendered Drupal listings + PDFs)."""

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
    def _discover_categories(self) -> Dict[str, str]:
        """Prefer live-discovered listing paths from the hub; fall back to the
        hard-coded canonical set."""
        cats = dict(CATEGORIES)
        hub = self._get_html(BASE_URL + HUB)
        if hub:
            for href in re.findall(r'href="([^"]+?-decisions)"', hub, re.I):
                if "/decisions/" in href:
                    continue  # individual decision, not a listing
                path = href if href.startswith("/") else urljoin(BASE_URL + HUB, href)
                if path.startswith("http"):
                    path = "/" + path.split("/", 3)[-1]
                key = _slug_from_path(path).replace("-decisions", "")
                cats.setdefault(key, path)
        return cats

    # -- listing parsing -------------------------------------------------
    def _parse_rows(self, page_html: str, category: str) -> List[Dict[str, Any]]:
        body = page_html
        m = re.search(r"<tbody.*?</tbody>", page_html, re.S | re.I)
        if m:
            body = m.group(0)
        rows: List[Dict[str, Any]] = []
        for rm in ROW_RE.finditer(body):
            row = rm.group(1)
            ref_m = REF_RE.search(row)
            if not ref_m:
                continue
            detail_path = ref_m.group(1)
            case_ref = _strip(ref_m.group(2)) or _slug_from_path(detail_path)
            pdfs = []
            for p in PDF_RE.findall(row):
                if p not in pdfs:
                    pdfs.append(p)
                if len(pdfs) >= MAX_PDFS_PER_ROW:
                    break
            if not pdfs:
                continue
            tm = TIME_RE.search(row)
            hearing_date = None
            if tm:
                hearing_date = tm.group(1)[:10]  # YYYY-MM-DD
            rows.append({
                "category": category,
                "detail_path": detail_path,
                "slug": _slug_from_path(detail_path),
                "case_ref": case_ref,
                "hearing_date": hearing_date,
                "pdf_urls": pdfs,
            })
        return rows

    def _build_raw(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        texts: List[str] = []
        for rel in row["pdf_urls"]:
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
        raw = dict(row)
        raw["text"] = text
        return raw

    # -- core ------------------------------------------------------------
    def _iter_category(self, category: str, path: str,
                       limit: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        page = 0
        empty_streak = 0
        while page < MAX_PAGES:
            url = f"{BASE_URL}{path}?page={page}"
            page_html = self._get_html(url)
            if page_html is None:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                page += 1
                continue
            rows = self._parse_rows(page_html, category)
            if not rows:
                break
            empty_streak = 0
            for row in rows:
                raw = self._build_raw(row)
                if raw:
                    produced += 1
                    yield raw
                    if limit and produced >= limit:
                        return
            page += 1

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        cats = self._discover_categories()
        produced = 0
        for category, path in cats.items():
            for raw in self._iter_category(category, path):
                produced += 1
                yield raw
        if produced == 0:
            raise RuntimeError(
                "Housing & Property Chamber listings returned 0 decisions — "
                "site blocked, layout changed, or all PDFs unreadable"
            )

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: walk each listing newest-first (default sort) and stop a
        category once we pass rows older than `since`."""
        since_date = since.date()
        cats = self._discover_categories()
        for category, path in cats.items():
            page = 0
            stop = False
            while page < MAX_PAGES and not stop:
                page_html = self._get_html(f"{BASE_URL}{path}?page={page}")
                if not page_html:
                    break
                rows = self._parse_rows(page_html, category)
                if not rows:
                    break
                for row in rows:
                    hd = row.get("hearing_date")
                    if hd:
                        try:
                            if datetime.strptime(hd, "%Y-%m-%d").date() < since_date:
                                stop = True
                                continue
                        except ValueError:
                            pass
                    raw = self._build_raw(row)
                    if raw:
                        yield raw
                page += 1

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        category = raw.get("category", "")
        slug = raw.get("slug", "")
        case_ref = raw.get("case_ref") or slug
        title = f"{case_ref} — First-tier Tribunal for Scotland (Housing and Property Chamber)"
        date = raw.get("hearing_date") or _date_from_text(text)
        return {
            "_id": f"UK-ScotHPC-{slug}",
            "_source": "UK/ScotHousingChamber",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": urljoin(BASE_URL, raw.get("detail_path", "")),
            "case_ref": case_ref,
            "category": category,
            "category_label": CATEGORY_LABELS.get(category, category),
            "court": "First-tier Tribunal for Scotland (Housing and Property Chamber)",
            "jurisdiction": "GB-SCT",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing Housing & Property Chamber listings...")
        cats = self._discover_categories()
        print(f"  discovered {len(cats)} category listings")
        cat, path = next(iter(cats.items()))
        page_html = self._get_html(f"{BASE_URL}{path}?page=0")
        print(f"  {cat}: {'OK' if page_html else 'FAILED'}")
        if page_html:
            rows = self._parse_rows(page_html, cat)
            print(f"  parsed {len(rows)} rows on page 0")
            if rows:
                raw = self._build_raw(rows[0])
                if raw:
                    print(f"  first decision {rows[0]['case_ref']}: "
                          f"{len(raw['text'])} chars extracted - OK")


def main():
    scraper = ScotHousingChamberScraper()
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
