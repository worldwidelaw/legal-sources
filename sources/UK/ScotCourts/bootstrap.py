#!/usr/bin/env python3
"""
UK/ScotCourts -- Scottish Courts and Tribunals Service judgments (scotcourts.gov.uk).

The Scottish Courts and Tribunals Service (SCTS) publishes the full text of
judgments and opinions from Scotland's superior and appellate courts and the
Upper Tribunal for Scotland. This is the authoritative Scottish superior-court
case-law corpus for the GB-SCT jurisdiction, which is NOT covered by UK/CaseLaw
(the National Archives "Find Case Law" service indexes England & Wales superior
courts + reserved UK tribunals only; Scottish court judgments are not on it).

Courts covered (a single "Judgments" index):
  - Court of Session (Inner House / Outer House) -- supreme civil court  [CSOH/CSIH]
  - High Court of Justiciary -- supreme criminal court                   [HCJAC/HCJ]
  - Sheriff Appeal Court (Civil & Criminal)                              [SAC ...]
  - Sheriff Courts (Civil & Criminal), across all sheriffdoms            [SC ...]
  - National Personal Injury Court
  - Upper Tribunal for Scotland (all chambers: Social Security, Housing &
    Property, Local Taxation, General Regulatory)                        [UT ...]

Site: https://www.scotcourts.gov.uk/judgments/ -- a client-side (Vue) search app
backed by a public JSON API (Azure-hosted). The app reads its API base from a
`data-base-url` attribute on the judgments page:

    https://api.pa.web.scotcourts.gov.uk/web

and calls two operations (autorest/@azure client baked into the JS bundle):

    GET  /web/definition/{contentId}   -> index configuration (indexType/limit)
    POST /web/search                   -> paginated judgment results

The search POST body is:

    {"query": "", "filters": [], "page": N,
     "indexType": "Judgments", "category": "", "limit": 50}

and each result carries structured metadata plus a relative documentLink to a
born-digital decision PDF hosted on the main site:

    {"title": "...", "documentLink": "/media/<id>/<neutral-citation>-....pdf",
     "date": "2026-07-17T00:00:00Z", "court": ["Court of Session"],
     "sheriffdom": [...], "judges": [...], "additionalDate": "...", "tags": [...]}

Strategy:
  - POST /web/search paging 1..pagination.page.total (limit 50), newest first.
  - For each result, download https://www.scotcourts.gov.uk{documentLink} and
    extract full text with PyMuPDF (pdfplumber/pypdf fallback). Born-digital, no
    OCR needed.
  - Metadata (title, court, judges, date, neutral citation) comes from the API +
    the PDF filename; text comes from the PDF.
  - One record per judgment PDF.

Data:
  - ~13,120 full-text judgments (growing), earliest coverage varies by court.
  - Language: English
  - Auth: None (free public access)
  - Licence: SCTS terms (personal / in-house use only) -- commercial-restricted.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py bootstrap-fast     # Full pull (runner alias)
  python bootstrap.py update             # Incremental (recent judgments)
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UK.ScotCourts")

SITE = "https://www.scotcourts.gov.uk"
API_BASE = "https://api.pa.web.scotcourts.gov.uk/web"
CONTENT_ID = 1414  # judgments search page content id
INDEX_TYPE = "Judgments"
PAGE_LIMIT = 50

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

# Neutral citation as it appears in the PDF filename stem, e.g.
#   2026scedi90 / 2026csoh52 / 2026sacciv46 / 2026hcjac12 / 2026ut60
FILE_CITE_RE = re.compile(r"(20\d{2}[a-z]{2,8}\d+[a-z]?)", re.I)
# Neutral citation as printed in the judgment body, e.g. "[2026] CSOH 52",
# "[2026] SC EDI 90", "[2026] HCJAC 12", "[2026] UT 60".
BODY_CITE_RE = re.compile(r"\[\s*(20\d{2})\s*\]\s*([A-Z][A-Z ]{1,12}?\d+)")


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


def _iso_date(s: Optional[str]) -> Optional[str]:
    """API dates look like '2026-07-17T00:00:00Z'; keep the date part."""
    if not s:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", s)
    return m.group(1) if m else None


def _neutral_citation(document_link: str, text: str) -> str:
    """The PDF filename stem carries the judgment's OWN neutral citation (e.g.
    2026csoh68, 2026sacciv46); prefer it. The first bracketed citation in the
    body is often a *cited* authority, not the decision itself, so only use it
    as a fallback when the filename yields nothing."""
    stem = Path(document_link or "").stem
    fm = FILE_CITE_RE.search(stem)
    if fm:
        return fm.group(1).upper()
    m = BODY_CITE_RE.search(text or "")
    if m:
        year, rest = m.group(1), re.sub(r"\s+", " ", m.group(2)).strip()
        return f"[{year}] {rest}"
    return ""


class ScotCourtsScraper(BaseScraper):
    """Scraper for Scottish Courts and Tribunals Service judgments (JSON search
    API + born-digital PDFs)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": UA,
            "Accept": "application/json",
            "Accept-Language": "en-GB,en;q=0.9",
            "Origin": SITE,
            "Referer": f"{SITE}/judgments/",
        })

    # -- discovery -------------------------------------------------------
    def _search_page(self, page: int) -> Optional[Dict[str, Any]]:
        body = {
            "query": "",
            "filters": [],
            "page": page,
            "indexType": INDEX_TYPE,
            "category": "",
            "limit": PAGE_LIMIT,
        }
        for attempt in range(4):
            self.rate_limiter.wait()
            try:
                resp = self.session.post(
                    f"{API_BASE}/search", json=body, timeout=60)
            except Exception as e:
                logger.warning(f"search page {page} attempt {attempt}: {e}")
                time.sleep(2 * (attempt + 1))
                continue
            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception as e:
                    logger.warning(f"search page {page}: bad JSON: {e}")
                    return None
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(3 * (attempt + 1))
                continue
            logger.warning(f"search page {page}: HTTP {resp.status_code}")
            return None
        return None

    def _fetch_pdf(self, document_link: str) -> Optional[bytes]:
        url = document_link if document_link.startswith("http") else SITE + document_link
        for attempt in range(3):
            self.rate_limiter.wait()
            try:
                resp = self.session.get(
                    url, timeout=90,
                    headers={"Accept": "application/pdf,*/*"})
            except Exception as e:
                logger.warning(f"pdf {url} attempt {attempt}: {e}")
                time.sleep(2 * (attempt + 1))
                continue
            if resp.status_code == 200 and resp.content[:5].startswith(b"%PDF"):
                return resp.content
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(3 * (attempt + 1))
                continue
            logger.debug(f"pdf {url}: HTTP {resp.status_code}")
            return None
        return None

    def _hydrate(self, meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        link = meta.get("documentLink") or ""
        if not link:
            return None
        pdf = self._fetch_pdf(link)
        if not pdf:
            return None
        try:
            text = _pdf_text(pdf)
        except Exception as e:
            logger.debug(f"extract {link}: {e}")
            text = ""
        if not text:
            return None
        out = dict(meta)
        out["text"] = text
        out["url"] = SITE + link if not link.startswith("http") else link
        return out

    def _iter_meta(self, max_pages: Optional[int] = None):
        first = self._search_page(1)
        if not first:
            raise RuntimeError(
                "scotcourts judgments API returned nothing for page 1 — "
                "endpoint drift or vantage block")
        total_pages = ((first.get("pagination") or {}).get("page") or {}).get("total") or 1
        if max_pages:
            total_pages = min(total_pages, max_pages)
        logger.info(f"judgments index: {total_pages} pages "
                    f"(~{((first.get('pagination') or {}).get('count') or {}).get('total')} judgments)")
        for r in (first.get("results") or []):
            yield r
        for page in range(2, total_pages + 1):
            data = self._search_page(page)
            if not data:
                logger.warning(f"page {page} empty — skipping")
                continue
            for r in (data.get("results") or []):
                yield r

    # -- core ------------------------------------------------------------
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        produced = 0
        seen = set()
        for meta in self._iter_meta():
            link = meta.get("documentLink") or ""
            if not link or link in seen:
                continue
            seen.add(link)
            hydrated = self._hydrate(meta)
            if hydrated:
                produced += 1
                yield hydrated
        if produced == 0:
            raise RuntimeError(
                "scotcourts judgments search returned 0 usable PDFs — "
                "API blocked, endpoint drift, or all PDFs unreadable")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Incremental: results are newest-first; stop once we pass `since`."""
        cutoff = since.strftime("%Y-%m-%d") if since else None
        for meta in self._iter_meta():
            date = _iso_date(meta.get("date"))
            if cutoff and date and date < cutoff:
                break
            link = meta.get("documentLink") or ""
            if not link:
                continue
            hydrated = self._hydrate(meta)
            if hydrated:
                yield hydrated

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        text = _clean(raw.get("text", "") or "")
        if len(text) < 200:
            return None
        link = raw.get("documentLink") or ""
        title = (raw.get("title") or "").strip()
        date = _iso_date(raw.get("date")) or _iso_date(raw.get("additionalDate"))

        courts = [c for c in (raw.get("court") or []) if c]
        court = "; ".join(courts) if courts else "Scottish Courts and Tribunals"
        judges = [j for j in (raw.get("judges") or []) if j]
        sheriffdoms = [s for s in (raw.get("sheriffdom") or []) if s]
        tags = [t for t in (raw.get("tags") or []) if t]

        citation = _neutral_citation(link, text)

        ident = citation or Path(link).stem or title
        slug = re.sub(r"[^A-Za-z0-9]+", "-", ident).strip("-")[:120]
        if not slug:
            return None

        if not title:
            title = citation or Path(link).stem

        return {
            "_id": f"UK-ScotCourts-{slug}",
            "_source": "UK/ScotCourts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("url", ""),
            "neutral_citation": citation,
            "court": court,
            "judges": judges,
            "sheriffdom": sheriffdoms,
            "tags": tags,
            "jurisdiction": "GB-SCT",
            "language": "en",
        }

    # -- diagnostics -----------------------------------------------------
    def test_connection(self):
        print("Testing scotcourts judgments search API...")
        data = self._search_page(1)
        if not data:
            print("  page 1 returned nothing")
            return
        pag = data.get("pagination") or {}
        print(f"  pagination: {pag}")
        results = data.get("results") or []
        print(f"  page 1 results: {len(results)}")
        if not results:
            return
        raw = self._hydrate(results[0])
        if raw:
            rec = self.normalize(raw)
            print(f"  {rec['neutral_citation']} ({rec['date']}) "
                  f"[{rec['court']}]: {len(rec['text'])} chars - OK")


def main():
    scraper = ScotCourtsScraper()
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
