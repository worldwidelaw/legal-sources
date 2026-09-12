#!/usr/bin/env python3
"""
EU/FrameworkDecisions — Framework decisions, joint actions and common positions
(CELEX **sector 3**, document class ``F``).

The ``F`` descriptor gathers the EU's former **third-pillar** binding acts: the
Justice-and-Home-Affairs **framework decisions** (Art. 34 TEU, binding on Member
States as to the result to be achieved — e.g. the Framework Decision on the
European Arrest Warrant 2002/584/JHA, on combating terrorism 2002/475/JHA, and on
combating racism and xenophobia 2008/913/JHA) together with the **joint actions**
and **common positions** adopted under Title VI TEU / early CFSP. They are
published in the Official Journal and catalogued by the EU Publications Office
(CELLAR / EUR-Lex) under the sector-3 descriptor ``F`` (CELEX ``3{YYYY}F{NNNN}``).

This is a distinct, additive corpus. EU/EUR-Lex enumerates sector 3 by the
ordinary binding resource-types only (REG / DIR / DEC and their
implementing/delegated variants) and therefore never pulls the ``F`` framework
decisions and joint actions — they carry the dedicated FRAMEWORK_DEC / JOINT_ACT
resource types, not DEC. No ``_id`` collision with any other descriptor.

Data flow
---------
1. Enumerate every act via the public CELLAR SPARQL endpoint (CELEX matching
   ``^3[0-9]{4}F[0-9]``). The corpus is ~175 rows, far under the SPARQL OFFSET
   ceiling, so a single paged LIMIT/OFFSET sweep suffices.
2. Fetch the full text of each act from CELLAR via HTTP content negotiation:
   OJ/Formex **xHTML** for modern acts, an **OJ HTML** manifestation via the
   language-suffixed CELEX (``.ENG``) for older ones, and a born-digital **PDF**
   stream (PyMuPDF) for the remainder. All are served by CELLAR, which bypasses
   the eur-lex.europa.eu AWS-WAF that 202-challenges datacenter IPs, so it is
   fleet-safe.
3. Normalize to the standard schema (legislation — binding acts).

Same CELLAR recipe as EU/OJC-Acts and EU/CouncilResolutions, narrowed to the
sector-3 ``F`` descriptor. Corrigenda (``…R(01)``) and metadata-only stubs
resolve to no usable manifestation and are skipped by the minimum-length guard.
"""

import sys
import html
import re
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import fitz  # PyMuPDF
except ImportError:  # pragma: no cover
    fitz = None

logger = logging.getLogger("legal-data-hunter")

SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"
CELLAR_CELEX = "http://publications.europa.eu/resource/celex/{celex}"

# CELEX pattern for sector-3 framework decisions / joint actions: 3{YYYY}F{NNNN}[(NN)].
CELEX_RE = r"^3[0-9]{4}F[0-9]"

SPARQL_QUERY = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT ?celex ?date ?title WHERE {{
  ?work cdm:resource_legal_id_celex ?celex .
  FILTER(REGEX(STR(?celex), "{pattern}"))
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{
    ?exp cdm:expression_belongs_to_work ?work ;
         cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> ;
         cdm:expression_title ?title .
  }}
}}
ORDER BY DESC(?date) ?celex
LIMIT {limit} OFFSET {offset}
"""

# Leading token in the Formex/xHTML export is the source filename — strip it.
_FILENAME_PREFIX_RE = re.compile(r"^\s*\S+\.(?:docx|xml|fmx\.xml|xhtml[^\s]*)\s+", re.IGNORECASE)

# Boilerplate the OJ HTML manifestation prepends before the document body:
#   "EUR-Lex - 31996Y0718 - EN | Avis juridique important | 31996Y0718 ..."
_OJ_HTML_PREFIX_RE = re.compile(
    r"^\s*EUR-Lex\s*-\s*\S+\s*-\s*\S+\s+Avis juridique important\s+\|\s+\S+\s+",
    re.IGNORECASE,
)

# Streams inside a CELLAR "300 Multiple-Choice" listing.
_STREAM_RE = re.compile(
    r'href="([^"]+/DOC_\d+)"[\s\S]*?<li title="stream_name">([^<]+)</li>',
    re.IGNORECASE,
)

_MIN_TEXT = 300


def _strip_html(raw_html: str) -> str:
    """Strip tags/entities from an (x)HTML document, returning clean text."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = _FILENAME_PREFIX_RE.sub("", text)
    text = re.sub(r"[ \t  ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = _OJ_HTML_PREFIX_RE.sub("", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_pdf_text(text: str) -> str:
    """Tidy PyMuPDF output: collapse whitespace."""
    text = re.sub(r"[ \t  ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class FrameworkDecisionsScraper(BaseScraper):
    """Scraper for EU framework decisions / joint actions (CELEX sector-3 ``F``) via CELLAR."""

    PAGE_SIZE = 200

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (+https://github.com/ZachLaik) legal-open-data",
        })

    # ---- HTTP helper ------------------------------------------------------

    def _get(self, url, *, headers=None, params=None, max_retries=4, timeout=60):
        last = None
        for attempt in range(max_retries):
            try:
                r = self.session.get(url, headers=headers, params=params,
                                     timeout=timeout, allow_redirects=True)
                if r.status_code == 200:
                    return r
                if r.status_code in (429, 500, 502, 503, 504):
                    time.sleep(min(2 ** attempt, 30))
                    continue
                return r  # 300/404 etc. — let caller decide
            except requests.RequestException as e:
                last = e
                time.sleep(min(2 ** attempt, 30))
        if last:
            raise last
        return None

    # ---- Enumeration ------------------------------------------------------

    def _sparql_page(self, offset: int) -> list:
        query = SPARQL_QUERY.format(pattern=CELEX_RE, limit=self.PAGE_SIZE, offset=offset)
        r = self._get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "application/sparql-results+json"},
            timeout=180,
        )
        if r is None or r.status_code != 200:
            logger.warning("SPARQL page offset=%s failed (%s)", offset, getattr(r, "status_code", "?"))
            return []
        try:
            bindings = r.json().get("results", {}).get("bindings", [])
        except ValueError:
            logger.warning("SPARQL returned non-JSON for offset=%s", offset)
            return []
        out = []
        for b in bindings:
            celex = b.get("celex", {}).get("value")
            if not celex:
                continue
            out.append({
                "celex": celex,
                "date": b.get("date", {}).get("value"),
                "title": b.get("title", {}).get("value"),
            })
        return out

    # ---- Full-text retrieval ---------------------------------------------

    def _fetch_text(self, celex: str) -> str:
        """Return clean full text for a CELEX.

        Modern acts carry an xHTML (OJ/Formex) manifestation; older ones an OJ
        HTML manifestation addressed by the language-suffixed CELEX; a minority
        only a born-digital PDF. Try each in turn.
        """
        enc = quote(celex, safe="")

        # 1) xHTML (OJ/Formex bodies) — the usual case for modern acts.
        r = self._get(
            CELLAR_CELEX.format(celex=enc),
            headers={"Accept": "application/xhtml+xml", "Accept-Language": "en"},
            timeout=90,
        )
        if r is not None and r.status_code == 200 and "xml" in (r.headers.get("content-type", "").lower()):
            text = _strip_html(r.text)
            if len(text) >= _MIN_TEXT:
                return text

        # 2) OJ HTML via language-suffixed CELEX (older acts).
        r = self._get(
            CELLAR_CELEX.format(celex=enc) + ".ENG",
            headers={"Accept": "text/html"},
            timeout=90,
        )
        if r is not None and r.status_code == 200 and "html" in (r.headers.get("content-type", "").lower()):
            text = _strip_html(r.text)
            if len(text) >= _MIN_TEXT:
                return text

        # 3) PDF streams.
        if fitz is None:
            return ""
        r = self._get(
            CELLAR_CELEX.format(celex=enc),
            headers={"Accept": "application/pdf", "Accept-Language": "en"},
            timeout=90,
        )
        if r is None:
            return ""
        ctype = r.headers.get("content-type", "").lower()
        if r.status_code == 200 and "pdf" in ctype:
            return self._pdf_bytes_to_text(r.content)
        if r.status_code == 300:
            url = self._pick_pdf_stream(r.text)
            if not url:
                return ""
            pr = self._get(url, timeout=120)
            if pr is None or pr.status_code != 200:
                return ""
            return self._pdf_bytes_to_text(pr.content)
        return ""

    @staticmethod
    def _pick_pdf_stream(listing_html: str) -> Optional[str]:
        """Choose the best PDF stream from a CELLAR 300 listing (prefer EN)."""
        streams = _STREAM_RE.findall(listing_html)
        if not streams:
            return None
        for url, name in streams:
            if re.search(r"_EN_.*part1.*\.pdf$", name, re.I) or re.search(r"_EN_ACT", name, re.I):
                return url
        for url, name in streams:
            if "_EN_" in name.upper() and name.lower().endswith(".pdf"):
                return url
        return streams[0][0]

    @staticmethod
    def _pdf_bytes_to_text(data: bytes) -> str:
        if fitz is None or not data:
            return ""
        try:
            doc = fitz.open(stream=data, filetype="pdf")
        except Exception as e:
            logger.debug("fitz open failed: %s", e)
            return ""
        try:
            parts = [page.get_text() for page in doc]
            return _clean_pdf_text("\n".join(parts))
        finally:
            doc.close()

    # ---- BaseScraper contract --------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW Council-resolution dicts (with full text), newest-first."""
        offset = 0
        seen = set()
        while True:
            page = self._sparql_page(offset)
            if not page:
                break
            for meta in page:
                celex = meta["celex"]
                if celex in seen:
                    continue
                seen.add(celex)
                text = self._fetch_text(celex)
                if len(text) < _MIN_TEXT:
                    logger.debug("Skip %s: text too short (%d)", celex, len(text))
                    continue
                time.sleep(1)  # be polite to CELLAR
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield acts with a document date on/after ``since`` (newest-first)."""
        cutoff = since.date().isoformat() if isinstance(since, datetime) else str(since)
        offset = 0
        seen = set()
        while True:
            page = self._sparql_page(offset)
            if not page:
                break
            stop = False
            for meta in page:
                d = meta.get("date")
                if d and d < cutoff:
                    stop = True
                    break
                celex = meta["celex"]
                if celex in seen:
                    continue
                seen.add(celex)
                text = self._fetch_text(celex)
                if len(text) < _MIN_TEXT:
                    continue
                time.sleep(1)
                yield {
                    "celex": celex,
                    "date": meta.get("date"),
                    "title": meta.get("title"),
                    "text": text,
                }
            if stop or len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        celex = raw["celex"]
        title = (raw.get("title") or "").strip()
        if not title:
            title = f"Framework Decision / Joint Action {celex}"
        date = raw.get("date")
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": celex,
            "_source": "EU/FrameworkDecisions",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "celex": celex,
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}",
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = FrameworkDecisionsScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        page = scraper._sparql_page(0)
        print(f"SPARQL returned {len(page)} framework decisions / joint actions on page 1")
        if page:
            c = page[0]["celex"]
            txt = scraper._fetch_text(c)
            print(f"  {c}: {(page[0].get('title') or '')[:70]}")
            print(f"  full text length: {len(txt)} chars")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
