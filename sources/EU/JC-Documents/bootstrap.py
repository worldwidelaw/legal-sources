#!/usr/bin/env python3
"""
EU/JC-Documents — EU Joint Communications & Joint Reports (sector-5 CELEX ``JC`` class).

These are the joint documents issued by the **High Representative of the Union
for Foreign Affairs and Security Policy together with the European Commission**
(e.g. "JOINT COMMUNICATION TO THE EUROPEAN PARLIAMENT AND THE COUNCIL …",
"JOINT REPORT …"). They set out the Union's external-action strategy — foreign,
security, defence, neighbourhood, cyber and trade-defence policy — and each
carries a CELEX number of the form ``5{YYYY}JC{NNNN}`` (sector 5 = preparatory
acts, descriptor ``JC``). The series runs from ~2011 (creation of the EEAS under
the Lisbon Treaty) to the present.

Strategy (identical recipe to the sibling EU/COM-Documents, which flags ``JC``
as a future sibling in its notes):

1. Enumerate every JC work via the public CELLAR SPARQL endpoint (the whole
   ``^5[0-9]{4}JC`` descriptor is only a few hundred works, comfortably under the
   ~10 000-row SPARQL OFFSET ceiling, so no year-scoping is needed).
2. Retrieve full text via CELLAR content negotiation on the bare CELEX:
     - ``Accept: application/xhtml+xml`` → OJ/Formex xHTML body (older docs);
     - fall back to ``Accept: application/pdf`` → either a direct born-digital
       PDF or a ``300 Multiple-Choice`` listing of PDF streams (recent docs),
       extracted with PyMuPDF.
3. Normalize to the standard schema (doctrine).

Why CELLAR and not the EUR-Lex portal? EUR-Lex HTML is behind an AWS-WAF that
challenges datacenter IPs; CELLAR (publications.europa.eu) is the authoritative,
anonymous, datacenter-friendly full-text source for the same corpus.

Distinct from EU/COM-Documents (sector-5 PC/DC), EU/EUR-Lex (enacted sector-3
REG/DIR/DEC) and EU/EuroParl (EP adopted texts). No descriptor overlap.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py update               # Incremental (re-scan, loader dedups)
  python bootstrap.py test                 # Quick connectivity check
"""

import sys
import html
import re
import time
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

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

# CELEX pattern for EU Joint Communications: 5{YYYY}JC{NNNN}
CELEX_RE = r"^5[0-9]{4}JC"

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

# Leading token in a Formex/xHTML export is the source filename — strip it.
_FILENAME_PREFIX_RE = re.compile(r"^\s*\S+\.(?:docx|xml|fmx\.xml|xhtml[^\s]*)\s+", re.IGNORECASE)

# Streams inside a CELLAR "300 Multiple-Choice" listing.
_STREAM_RE = re.compile(
    r'href="([^"]+/DOC_\d+)"[\s\S]*?<li title="stream_name">([^<]+)</li>',
    re.IGNORECASE,
)


def _strip_html(raw_html: str) -> str:
    """Strip tags/entities from an xHTML document, returning clean text."""
    text = re.sub(r"<script[\s\S]*?</script>", " ", raw_html)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = _FILENAME_PREFIX_RE.sub("", text)
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _clean_pdf_text(text: str) -> str:
    """Tidy PyMuPDF output: collapse the EN/EN gutter markers and whitespace."""
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class JCDocumentsScraper(BaseScraper):
    """Scraper for EU Joint Communications (JC) via CELLAR."""

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
            logger.warning("SPARQL page offset=%s failed (%s)",
                           offset, getattr(r, "status_code", "?"))
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
        """Return clean full text for a CELEX, trying xHTML then PDF streams."""
        # 1) xHTML (OJ Formex bodies — mostly older joint communications).
        r = self._get(
            CELLAR_CELEX.format(celex=celex),
            headers={"Accept": "application/xhtml+xml", "Accept-Language": "en"},
            timeout=90,
        )
        if r is not None and r.status_code == 200 and "xml" in (r.headers.get("content-type", "")):
            text = _strip_html(r.text)
            if len(text) >= 200:
                return text

        # 2) PDF streams (recent docs have no xHTML manifestation).
        if fitz is None:
            return ""
        r = self._get(
            CELLAR_CELEX.format(celex=celex),
            headers={"Accept": "application/pdf", "Accept-Language": "en"},
            timeout=90,
        )
        if r is None:
            return ""
        ctype = r.headers.get("content-type", "")
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
        """Choose the best PDF stream from a CELLAR 300 listing.

        Prefer the English "ACT part1" body; fall back to the first English PDF,
        then to the first stream of any language.
        """
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
        """Yield RAW joint-communication dicts (with full text), newest first."""
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
            if len(page) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield joint communications with a document date on/after ``since``."""
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
                if len(text) < 200:
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
            title = f"EU Joint Communication {celex}"
        date = raw.get("date")
        if date and not re.match(r"^\d{4}-\d{2}-\d{2}", str(date)):
            date = None
        return {
            "_id": celex,
            "_source": "EU/JC-Documents",
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
    scraper = JCDocumentsScraper()

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
        print(f"SPARQL returned {len(page)} JC docs on page 1")
        for meta in page[:3]:
            c = meta["celex"]
            txt = scraper._fetch_text(c)
            print(f"  {c}: {(meta.get('title') or '')[:60]} -> {len(txt)} chars")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
