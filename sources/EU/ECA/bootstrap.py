#!/usr/bin/env python3
"""
EU/ECA — European Court of Auditors: audit reports, opinions and reviews.

The European Court of Auditors (ECA) is the EU's independent external auditor.
It publishes, in the Official Journal and on its own site, a large full-text
corpus of audit output that constitutes official EU *doctrine*:

  * **Special reports** (SR-YYYY-NN) — performance/compliance audits of a
    specific EU policy or programme;
  * **Annual reports** and **Annual reports on EU agencies** (AR/SAR/AGENCIES) —
    the statement of assurance on the EU budget and the agencies' accounts;
  * **Opinions** (OP) — the ECA's formal opinions on draft EU legislation with
    a financial impact (required under the Treaties);
  * **Reviews / Landscape reviews / Rapid case reviews** (RW / INSR / RCR) —
    analytical overviews of a policy area;
  * **Audit previews / Journals / other publications**.

Why this is additive
---------------------
EU/OJC-Acts (CELLAR sector-3 ``Y``) captures only the short OJ **C-series
summaries** of a handful of ECA special reports. This source captures the
*full* ECA report / opinion / review bodies (tens of thousands of words each),
which are not otherwise in the corpus. No CELEX ``_id`` collision — the ``_id``
here is the ECA document code (e.g. ``SR-2026-14``).

Data flow
---------
1. Enumerate every published PDF via the ECA site's public SharePoint Search
   REST endpoint (``/_api/search/query``). Two document libraries hold the
   corpus:
     * the modern set under ``/ECAPublications/*`` and
     * the historical archive under ``/Lists/ECADocuments/*``.
   Each publication exists in ~24 language variants; we keep only the English
   manifestation (path suffix ``_EN.pdf``). Path-constrained queries page deep
   with ``startrow`` (the unconstrained site-wide query does not, hence the two
   explicit library paths).
2. Download each English PDF and extract its full text with PyMuPDF (the reports
   are born-digital with a clean text layer). A minimum-length guard skips the
   rare pre-2000 scanned document.
3. Normalize to the standard schema (doctrine).

Reachability: ``www.eca.europa.eu`` and its SharePoint search API answer plain
HTTPS requests (HTTP 200) — no WAF/JS challenge, no auth. The PDFs are served
directly from the same host.
"""

import re
import sys
import time
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urlencode

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None

# Shared chained extractor (opendataloader -> pdfplumber -> pypdf -> OCR). Used
# as a fallback wherever PyMuPDF is unavailable, so a fleet image without
# PyMuPDF still yields full text instead of silently skipping every document.
try:
    from common.pdf_extract import _extract as _shared_pdf_extract
except ImportError:  # pragma: no cover
    _shared_pdf_extract = None

logger = logging.getLogger("legal-data-hunter")

SEARCH_API = "https://www.eca.europa.eu/_api/search/query"

# The two document libraries that hold the published corpus. Path-constrained
# queries (unlike the unconstrained site-wide FileExtension:pdf query) page deep
# via startrow, so we enumerate each library explicitly.
LIBRARY_PATHS = (
    "https://www.eca.europa.eu/ECAPublications*",
    "https://www.eca.europa.eu/Lists/ECADocuments*",
)

PAGE_SIZE = 500
_MIN_TEXT = 400

# If this many consecutive documents yield no usable text before a single record
# is produced, the run is broken (no PDF backend / host refusing downloads) —
# raise instead of finishing quietly with an empty corpus (issue #1211).
_FAILFAST_AFTER = 20

# 2-digit-year publication codes: AAR21 -> 2021, INSR14_10 -> 2014.
_YEAR4_RE = re.compile(r"(19[5-9][0-9]|20[0-4][0-9])")
_YEAR2_RE = re.compile(r"(?:AAR|AR|SAR|INSR|SR|OP|RW|RCR|ISR)[-_]?([0-9]{2})(?:[-_]|$)")


def _clean_pdf_text(text: str) -> str:
    """Tidy PyMuPDF output: collapse runs of whitespace."""
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _doc_code(path: str) -> str:
    """The ECA document code = the English PDF filename stem without ``_EN``.

    e.g. ``.../SR-2026-14/SR-2026-14_EN.pdf`` -> ``SR-2026-14``
         ``.../Lists/ECADocuments/AAR21/AAR21_EN.PDF`` -> ``AAR21``
    """
    stem = path.rsplit("/", 1)[-1]
    stem = re.sub(r"\.pdf$", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"_EN$", "", stem, flags=re.IGNORECASE)
    return stem


def _derive_year(code: str, title: str) -> Optional[int]:
    """Best-effort publication year from the document code, then the title."""
    m = _YEAR4_RE.search(code)
    if m:
        return int(m.group(1))
    m = _YEAR2_RE.search(code.upper())
    if m:
        yy = int(m.group(1))
        return 2000 + yy if yy < 50 else 1900 + yy
    m = _YEAR4_RE.search(title or "")
    if m:
        return int(m.group(1))
    return None


class ECAScraper(BaseScraper):
    """Scraper for European Court of Auditors publications (SharePoint search)."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/ZachLaik) legal-open-data",
        })
        self._download_failures = 0
        self._extract_failures = 0

    # ---- HTTP helper ------------------------------------------------------

    def _get(self, url, *, headers=None, max_retries=4, timeout=60, stream=False):
        last = None
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, headers=headers, timeout=timeout,
                                     allow_redirects=True, stream=stream)
                if r.status_code == 200:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 ** attempt, 30))
                    continue
                return r
            except requests.RequestException as e:
                last = e
                time.sleep(min(2 ** attempt, 30))
        if last:
            logger.debug("GET %s failed: %s", url, last)
        return None

    # ---- Enumeration ------------------------------------------------------

    def _search_page(self, path_glob: str, startrow: int) -> tuple:
        """One SharePoint search page. Returns (rows, total_rows)."""
        query = "FileExtension:pdf Path:%s" % path_glob
        params = {
            "querytext": "'%s'" % query,
            "rowlimit": str(PAGE_SIZE),
            "startrow": str(startrow),
            "selectproperties": "'Title,Path,Write'",
            "clienttype": "'ContentSearchRegular'",
        }
        url = SEARCH_API + "?" + urlencode(params, safe="':*")
        r = self._get(url, headers={"Accept": "application/json;odata=nometadata"}, timeout=90)
        if r is None or r.status_code != 200:
            logger.warning("search page %s@%s failed (%s)", path_glob, startrow,
                           getattr(r, "status_code", "?"))
            return [], 0
        try:
            rel = r.json()["PrimaryQueryResult"]["RelevantResults"]
        except (ValueError, KeyError):
            logger.warning("search returned unexpected JSON at %s", startrow)
            return [], 0
        total = rel.get("TotalRows", 0)
        out = []
        for row in rel.get("Table", {}).get("Rows", []):
            cells = {c["Key"]: c.get("Value") for c in row.get("Cells", [])}
            path = cells.get("Path") or ""
            if path:
                out.append({"path": path, "title": cells.get("Title"), "write": cells.get("Write")})
        return out, total

    def _failure_diagnosis(self, seen: int) -> str:
        """Explain *why* a run produced no records, for the fleet log."""
        if self._download_failures >= self._extract_failures:
            return (
                f"EU/ECA: {self._download_failures} of the first {seen} report PDFs "
                "could not be downloaded from www.eca.europa.eu — the host is "
                "refusing this vantage (datacenter-IP block?). Needs a residential/EU proxy."
            )
        backends = "PyMuPDF" if fitz is not None else "(PyMuPDF missing)"
        if _shared_pdf_extract is None:
            backends += ", common.pdf_extract unavailable"
        return (
            f"EU/ECA: downloaded {seen} report PDFs but extracted no text from any "
            f"of them — no working PDF backend in this environment [{backends}]. "
            "Install PyMuPDF (or pdfplumber/pypdf for the shared fallback)."
        )

    def _enumerate_english(self) -> Generator[dict, None, None]:
        """Yield {path,title,write} for every English publication PDF, deduped."""
        seen = set()
        for glob in LIBRARY_PATHS:
            startrow = 0
            while True:
                rows, total = self._search_page(glob, startrow)
                if not rows:
                    break
                for meta in rows:
                    path = meta["path"]
                    if not path.upper().endswith("_EN.PDF"):
                        continue
                    code = _doc_code(path)
                    if code in seen:
                        continue
                    seen.add(code)
                    yield meta
                startrow += len(rows)
                if total and startrow >= total:
                    break
                time.sleep(0.5)
        if not seen:
            raise RuntimeError(
                "EU/ECA: the SharePoint search API returned no English publication "
                "PDFs for either document library — the query shape changed or "
                "www.eca.europa.eu/_api/search/query is refusing this vantage."
            )

    # ---- Full-text retrieval ---------------------------------------------

    def _pdf_text(self, url: str) -> str:
        """Download a report PDF and return its text layer.

        Distinguishes *download* failure from *extraction* failure so the caller
        can tell "the host refused us" from "we have no PDF backend" — both used
        to surface identically as an empty string and a silently skipped record.
        """
        r = self._get(url, timeout=120)
        if r is None or r.status_code != 200:
            self._download_failures += 1
            return ""
        if not r.content[:5] == b"%PDF-":
            self._download_failures += 1
            return ""

        text = ""
        if fitz is not None:
            try:
                doc = fitz.open(stream=r.content, filetype="pdf")
                try:
                    text = _clean_pdf_text("\n".join(page.get_text() for page in doc))
                finally:
                    doc.close()
            except Exception as e:
                logger.debug("fitz open failed for %s: %s", url, e)

        # PyMuPDF absent (fleet image gap) or it produced nothing — fall back to
        # the shared chained extractor rather than dropping the document.
        if len(text) < _MIN_TEXT and _shared_pdf_extract is not None:
            try:
                fallback = _shared_pdf_extract(r.content)
            except Exception as e:
                logger.debug("fallback extract failed for %s: %s", url, e)
                fallback = None
            if fallback and len(fallback) > len(text):
                text = _clean_pdf_text(fallback)

        if len(text) < _MIN_TEXT:
            self._extract_failures += 1
        return text

    # ---- BaseScraper contract --------------------------------------------

    def _iter(self) -> Generator[dict, None, None]:
        seen = 0
        yielded = 0
        for meta in self._enumerate_english():
            path = meta["path"]
            seen += 1
            text = self._pdf_text(path)
            if len(text) < _MIN_TEXT:
                logger.debug("Skip %s: text too short (%d)", path, len(text))
                # A handful of pre-2000 scans legitimately have no text layer,
                # but a *run* of failures means the environment is broken (no
                # PDF backend) or the host is refusing us. Fail loud rather than
                # walking the whole corpus and reporting "No records written".
                if seen >= _FAILFAST_AFTER and yielded == 0:
                    raise RuntimeError(self._failure_diagnosis(seen))
                continue
            time.sleep(1)  # be polite to the ECA host
            yielded += 1
            yield {
                "path": path,
                "code": _doc_code(path),
                "title": meta.get("title"),
                "text": text,
            }

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # SharePoint 'Write' is the (bulk) upload date, not publication date, and
        # is unreliable for incremental filtering; re-enumerate and let the
        # idempotent upsert skip unchanged records.
        yield from self._iter()

    def normalize(self, raw: dict) -> dict:
        code = raw["code"]
        title = (raw.get("title") or "").strip() or f"European Court of Auditors — {code}"
        year = _derive_year(code, title)
        date = f"{year}-01-01" if year else None
        return {
            "_id": f"EU/ECA/{code}",
            "_source": "EU/ECA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_code": code,
            "title": title,
            "text": raw["text"],
            "date": date,
            "year": year,
            "language": "en",
            "url": raw["path"],
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = ECAScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        rows, total = scraper._search_page(LIBRARY_PATHS[0], 0)
        print(f"search page 0: {len(rows)} rows, TotalRows={total}")
        for meta in scraper._enumerate_english():
            txt = scraper._pdf_text(meta["path"])
            print(f"  {_doc_code(meta['path'])}: {len(txt)} chars — {(meta.get('title') or '')[:60]}")
            break
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
