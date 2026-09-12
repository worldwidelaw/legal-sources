#!/usr/bin/env python3
"""
EU/ParliamentaryAssemblies — Acts and resolutions of the EU's joint / regional
parliamentary assemblies established under EU international agreements.

Sector 2 of EUR-Lex covers *international agreements and the acts of bodies
created by them*. Its ``P`` descriptor is the **parliamentary-assembly act
class**: resolutions and other acts adopted by the interparliamentary bodies the
EU sets up with partner regions. The corpus (~418 CELEX, 1997 onward) is
dominated by the **ACP-EU Joint Parliamentary Assembly** (JPA) and its
predecessor the *ACP-EU Joint Assembly* — the parliamentary institution of the
ACP-EU partnership (Lomé → Cotonou → the post-Cotonou "Samoa Agreement" OACPS-EU
framework), bringing together MEPs and parliamentarians of the African,
Caribbean and Pacific (ACP / OACPS) group of states (~85% of the series) — and
also includes the **Euronest Parliamentary Assembly** (EU-Eastern Partnership),
the **EuroLat** Euro-Latin American Assembly and the Union-for-the-Mediterranean
assembly. They adopt resolutions on development cooperation, human rights, trade,
peace and security, and other political questions, published in the Official
Journal (C series).

Each act is registered in the EU Publications Office repository (CELLAR) with a
CELEX number of the form ``2{YYYY}P{NNNN}``. This is a distinct, additive corpus:
EU/EUR-Lex enumerates sector-3 legislation only, and the sector-2 siblings
EU/InternationalAgreements (``A``) and EU/JointBodyDecisions (``D``) cover
agreements and joint-committee decisions, never the ``P`` parliamentary acts. The
whole ``P`` series is well under the SPARQL 10 000-row OFFSET ceiling, so a
single paged enumeration suffices.

Data flow
---------
1. Enumerate every assembly act via the public CELLAR SPARQL endpoint (CELEX
   matching ``^2[0-9]{4}P[0-9]``). Each row yields CELEX + document date +
   English expression title.
2. Fetch the full text from CELLAR via HTTP content negotiation. Modern acts
   (~2010+) are served as OJ/Formex **xHTML** on the bare CELEX; older ones as an
   **OJ HTML** manifestation (bare CELEX ``Accept: text/html``, or the
   language-suffixed ``/resource/celex/{CELEX}.ENG``); a minority carry a
   born-digital **PDF** stream (PyMuPDF). Try each in turn; skip the CELEX that
   are metadata-only (no manifestation → 404 everywhere).
3. Normalize to the standard schema (doctrine).

Why CELLAR? It is the authoritative, anonymous, datacenter-friendly full-text
source for this corpus and bypasses the eur-lex.europa.eu AWS-WAF that
202-challenges datacenter IPs, so it is fleet-safe. Same CELLAR recipe as
EU/ESC-Opinions, EU/InternationalAgreements and EU/JointBodyDecisions, narrowed
to the sector-2 ``P`` descriptor.
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

# The whole sector-2 "P" (JPA) series — a single paged pattern stays well under
# the SPARQL 10K OFFSET ceiling (~418 CELEX total, 1997 onward).
CELEX_PATTERN = "^2[0-9]{4}P[0-9]"

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
ORDER BY DESC(?celex)
LIMIT {limit} OFFSET {offset}
"""

# Leading token in the Formex/xHTML export is the source filename — strip it.
_FILENAME_PREFIX_RE = re.compile(r"^\s*\S+\.(?:docx|xml|fmx\.xml|xhtml[^\s]*)\s+", re.IGNORECASE)

# Boilerplate the OJ HTML manifestation prepends before the document body:
#   "EUR-Lex - 21999P0924(12) - EN Avis juridique important | 21999P0924(12) ..."
_OJ_HTML_PREFIX_RE = re.compile(
    r"^\s*EUR-Lex\s*-\s*\S+\s*-\s*\S+\s+Avis juridique important\s+\|\s+\S+\s+",
    re.IGNORECASE,
)

# Streams inside a CELLAR "300 Multiple-Choice" listing.
_STREAM_RE = re.compile(
    r'href="([^"]+/DOC_\d+)"[\s\S]*?<li title="stream_name">([^<]+)</li>',
    re.IGNORECASE,
)


def _strip_html(raw_html: str) -> str:
    """Strip tags/entities from an (x)HTML document, returning clean text."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = _FILENAME_PREFIX_RE.sub("", text)
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = _OJ_HTML_PREFIX_RE.sub("", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_pdf_text(text: str) -> str:
    """Tidy PyMuPDF output: collapse whitespace."""
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class ParliamentaryAssembliesScraper(BaseScraper):
    """Scraper for ACP-EU Joint Parliamentary Assembly acts via CELLAR."""

    PAGE_SIZE = 500

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

    def _sparql_page(self, pattern: str, offset: int) -> list:
        query = SPARQL_QUERY.format(pattern=pattern, limit=self.PAGE_SIZE, offset=offset)
        r = self._get(
            SPARQL_ENDPOINT,
            params={"query": query, "format": "application/sparql-results+json"},
            timeout=180,
        )
        if r is None or r.status_code != 200:
            logger.warning("SPARQL page pattern=%s offset=%s failed (%s)",
                           pattern, offset, getattr(r, "status_code", "?"))
            return []
        try:
            bindings = r.json().get("results", {}).get("bindings", [])
        except ValueError:
            logger.warning("SPARQL returned non-JSON for pattern=%s offset=%s", pattern, offset)
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

    def _enumerate(self) -> Generator[dict, None, None]:
        """Yield JPA-act metadata for the whole P series (newest CELEX first)."""
        offset = 0
        seen = set()
        while True:
            page = self._sparql_page(CELEX_PATTERN, offset)
            if not page:
                break
            new = 0
            for meta in page:
                if meta["celex"] in seen:
                    continue
                seen.add(meta["celex"])
                new += 1
                yield meta
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE
            if new == 0:
                break

    # ---- Full-text retrieval ---------------------------------------------

    def _fetch_text(self, celex: str) -> str:
        """Return clean full text for a CELEX.

        Modern JPA acts are served as OJ/Formex xHTML on the bare CELEX; older
        ones as an OJ HTML manifestation (bare CELEX text/html, or the
        language-suffixed CELEX); a minority carry a born-digital PDF. Try each in
        turn and return the first that yields real text.
        """
        enc = quote(celex, safe="")

        attempts = [
            # (url, accept, content-type-substring)
            (CELLAR_CELEX.format(celex=enc), "application/xhtml+xml", "xml"),
            (CELLAR_CELEX.format(celex=enc), "text/html", "html"),
            (CELLAR_CELEX.format(celex=enc) + ".ENG", "text/html", "html"),
        ]
        for url, accept, ct_needle in attempts:
            r = self._get(url, headers={"Accept": accept, "Accept-Language": "en"}, timeout=90)
            if r is not None and r.status_code == 200 and \
                    ct_needle in (r.headers.get("content-type", "").lower()):
                text = _strip_html(r.text)
                if len(text) >= 200:
                    return text

        # PDF streams.
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
        """Yield RAW JPA-act dicts (with full text), newest CELEX first."""
        for meta in self._enumerate():
            celex = meta["celex"]
            text = self._fetch_text(celex)
            if len(text) < 200:
                logger.debug("Skip %s: text too short (%d)", celex, len(text))
                continue
            time.sleep(1)  # be polite to CELLAR
            yield {
                "celex": celex,
                "date": meta.get("date"),
                "title": meta.get("title"),
                "text": text,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-sweep the whole series so late CELLAR digitisations / new sessions
        are caught (the corpus is small enough to re-enumerate fully)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        celex = raw["celex"]
        title = (raw.get("title") or "").strip()
        if not title:
            title = f"EU Parliamentary Assembly act {celex}"
        date = raw.get("date")
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": celex,
            "_source": "EU/ParliamentaryAssemblies",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "celex": celex,
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}",
        }


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    scraper = ParliamentaryAssembliesScraper()

    command = sys.argv[1] if len(sys.argv) > 1 else "bootstrap"
    sample_mode = "--sample" in sys.argv

    if command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")
    elif command == "test":
        page = scraper._sparql_page(CELEX_PATTERN, 0)
        print(f"SPARQL returned {len(page)} JPA acts on page 1")
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
