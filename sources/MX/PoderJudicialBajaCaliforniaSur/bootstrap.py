#!/usr/bin/env python3
"""
MX/PoderJudicialBajaCaliforniaSur -- Baja California Sur State Court Decisions

Fetches full-text court decisions (sentencias en versión pública) from the
Poder Judicial del Estado de Baja California Sur public "Sentencias Públicas"
access-libre portal.

Portal: https://e-tribunalbcs.mx/AccesoLibre/SentenciasPublicasBusqueda.aspx

Strategy:
  - The portal is an ASP.NET WebForms search ("Acceso Libre") that posts back to
    SentenciasPublicasBusqueda.aspx and renders the FULL result set for a given
    publication year in a single HTML table (no server-side pagination).
  - We GET the form once per year to harvest __VIEWSTATE/__EVENTVALIDATION, then
    POST btnBuscar with the "año" filter enabled (txtAño=YYYY) and every other
    selector left at "VER TODOS". The materia radio does not restrict results —
    one search returns the full corpus for that year across all materias.
  - Each result row exposes an encrypted document token via
    AbrirSentenciaPublica('<token>'). Resolving Documento.aspx?cadena=<token>
    returns a viewer page whose <iframe> points at SentenciasPublicasPDF.aspx,
    which streams the actual full-text PDF (application/pdf). We extract its
    text layer (decisions carry a real text layer, ~10K-200K chars).

Usage:
  python bootstrap.py test                 # quick connectivity test
  python bootstrap.py bootstrap --sample   # fetch a small sample
  python bootstrap.py bootstrap --full     # fetch everything
  python bootstrap.py bootstrap-fast --full
"""

import re
import sys
import html
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote, urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as extract_pdf_text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialBajaCaliforniaSur")

BASE = "https://e-tribunalbcs.mx/AccesoLibre"
SEARCH_URL = f"{BASE}/SentenciasPublicasBusqueda.aspx"
DOC_URL = f"{BASE}/Documento.aspx"

# Portal's earliest available publication year (txtAño min="2013").
FIRST_YEAR = 2013

_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S | re.I)
# href quotes are HTML-encoded as &#39; in the raw markup, so accept either.
_TOKEN_RE = re.compile(
    r"AbrirSentenciaPublica\(\s*(?:&#39;|&quot;|['\"])([^'\"&]+)(?:&#39;|&quot;|['\"])")
_HIDDEN_RE = re.compile(
    r'<input[^>]*type="hidden"[^>]*name="([^"]+)"[^>]*value="([^"]*)"', re.I | re.S
)
_IFRAME_RE = re.compile(r"""<iframe[^>]*src=['"]([^'"]+)['"]""", re.I)
_TAG_RE = re.compile(r"<[^>]+>")


def _clean(text: str) -> str:
    text = _TAG_RE.sub("", text or "")
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


class BajaCaliforniaSurCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialBajaCaliforniaSur full-text sentencias."""

    def __init__(self, source_dir=None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Legal-Data-Hunter/1.0",
        })

    # ── HTTP helpers ──────────────────────────────────────────────────
    def _get_form_state(self) -> Optional[Dict[str, str]]:
        """GET the search page and return all hidden form fields (VIEWSTATE etc)."""
        for attempt in range(3):
            try:
                time.sleep(1.0)
                resp = self.session.get(SEARCH_URL, timeout=45)
                resp.raise_for_status()
                fields = {name: html.unescape(val)
                          for name, val in _HIDDEN_RE.findall(resp.text)}
                if "__VIEWSTATE" in fields:
                    return fields
            except requests.exceptions.RequestException as e:
                logger.warning(f"Form GET attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    def _search_year(self, year: int) -> Optional[str]:
        """POST the search for a single publication year; return result HTML."""
        state = self._get_form_state()
        if state is None:
            return None
        data = dict(state)
        data.update({
            "__EVENTTARGET": "ctl00$ContentPlaceHolder1$btnBuscar",
            "__EVENTARGUMENT": "",
            "ctl00$ContentPlaceHolder1$gMateria": "rbCivil",
            "ctl00$ContentPlaceHolder1$ddlMunicipio": "0",
            "ctl00$ContentPlaceHolder1$ddlInstancia": "-1",
            "ctl00$ContentPlaceHolder1$ddlJuzgado": "0",
            "ctl00$ContentPlaceHolder1$ddlTipoJuicio": "VER TODOS",
            "ctl00$ContentPlaceHolder1$ddlTipoResultado": "VER TODOS",
            "ctl00$ContentPlaceHolder1$ckAño": "on",
            "ctl00$ContentPlaceHolder1$txtAño": str(year),
        })
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.post(SEARCH_URL, data=data, timeout=90,
                                         headers={"Referer": SEARCH_URL})
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"Year {year} search HTTP {resp.status_code} "
                               f"(attempt {attempt+1})")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Year {year} search attempt {attempt+1} failed: {e}")
            time.sleep(4)
            # refresh state for the retry
            state = self._get_form_state()
            if state:
                data.update(state)
        return None

    def _fetch_pdf(self, token: str) -> Optional[bytes]:
        """Resolve a result token to its full-text PDF bytes."""
        doc_url = f"{DOC_URL}?cadena={quote(token, safe='')}"
        try:
            time.sleep(1.0)
            rd = self.session.get(doc_url, timeout=60, headers={"Referer": SEARCH_URL})
            rd.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.warning(f"Documento.aspx failed for token: {e}")
            return None
        m = _IFRAME_RE.search(rd.text)
        if not m:
            return None
        iframe_url = urljoin(doc_url, html.unescape(m.group(1)))
        for attempt in range(3):
            try:
                time.sleep(1.0)
                rp = self.session.get(iframe_url, timeout=90,
                                      headers={"Referer": doc_url})
                rp.raise_for_status()
                ctype = rp.headers.get("Content-Type", "").lower()
                if "pdf" in ctype or rp.content[:4] == b"%PDF":
                    return rp.content
                return None
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF fetch attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    # ── Parsing ───────────────────────────────────────────────────────
    @staticmethod
    def _parse_rows(html_text: str) -> List[Dict[str, Any]]:
        """Parse the result table rows into raw dicts.

        Columns: # | FECHA PUBLICACIÓN | NÚMERO | ÓRGANO JURISDICCIONAL |
                 JUICIO | RESULTADO | VER SENTENCIA(token)
        """
        rows: List[Dict[str, Any]] = []
        # isolate the results table to avoid matching unrelated rows
        tbl = re.search(
            r'<table[^>]*id="[^"]*tblResultados[^"]*"[^>]*>(.*?)</table>',
            html_text, re.S | re.I)
        scope = tbl.group(1) if tbl else html_text
        for tr in _TR_RE.findall(scope):
            tm = _TOKEN_RE.search(tr)
            if not tm:
                continue
            token = html.unescape(tm.group(1))
            cells = [_clean(td) for td in _TD_RE.findall(tr)]
            # cells: [num, fecha, numero, organo, juicio, resultado, (verb)]
            def g(i):
                return cells[i] if len(cells) > i else ""
            fecha = g(1)
            iso = None
            md = re.match(r"(\d{2})/(\d{2})/(\d{4})", fecha)
            if md:
                iso = f"{md.group(3)}-{md.group(2)}-{md.group(1)}"
            rows.append({
                "token": token,
                "fecha": fecha,
                "date": iso,
                "numero": g(2),
                "organo": g(3),
                "juicio": g(4),
                "resultado": g(5),
            })
        return rows

    # ── Core iteration ────────────────────────────────────────────────
    def _iter_listing(self) -> Generator[Dict[str, Any], None, None]:
        current_year = datetime.now(timezone.utc).year
        seen = set()
        for year in range(current_year, FIRST_YEAR - 1, -1):
            page = self._search_year(year)
            if not page:
                continue
            rows = self._parse_rows(page)
            logger.info(f"Year {year}: {len(rows)} decisions")
            for row in rows:
                if row["token"] in seen:
                    continue
                seen.add(row["token"])
                pdf_bytes = self._fetch_pdf(row["token"])
                if not pdf_bytes:
                    continue
                text = extract_pdf_text(pdf_bytes)
                if not text or len(text) < 100:
                    continue
                row["_text"] = text
                row["_year"] = year
                yield row

    # ── BaseScraper interface ─────────────────────────────────────────
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_listing()

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        year = datetime.now(timezone.utc).year
        page = self._search_year(year)
        if not page:
            return
        seen = set()
        for row in self._parse_rows(page):
            if row["token"] in seen:
                continue
            seen.add(row["token"])
            if since and row.get("date") and row["date"] < since:
                continue
            pdf_bytes = self._fetch_pdf(row["token"])
            if not pdf_bytes:
                continue
            text = extract_pdf_text(pdf_bytes)
            if not text or len(text) < 100:
                continue
            row["_text"] = text
            row["_year"] = year
            yield row

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        token = raw.get("token", "")
        numero = raw.get("numero", "")
        slug_src = numero or token
        slug = re.sub(r"[^A-Za-z0-9]+", "-", slug_src).strip("-")[:80]

        juicio = raw.get("juicio", "")
        organo = raw.get("organo", "")
        resultado = raw.get("resultado", "")

        title_parts = [p for p in [juicio, numero] if p]
        title = " — ".join(title_parts) if title_parts else f"Sentencia {numero or slug}"

        return {
            "_id": f"MX-BCS-{slug}",
            "_source": "MX/PoderJudicialBajaCaliforniaSur",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": raw.get("date"),
            "url": SEARCH_URL,
            "court": organo,
            "subject_area": juicio,
            "case_type": juicio,
            "case_number": numero,
            "result": resultado,
            "jurisdiction": "MX-BCS",
        }

    # ── Connectivity test ─────────────────────────────────────────────
    def test(self) -> bool:
        year = datetime.now(timezone.utc).year
        page = self._search_year(year)
        if not page:
            # current year may be sparse; fall back to a prior year
            page = self._search_year(year - 1)
        if not page:
            logger.error("Cannot reach BCS sentencias portal")
            return False
        rows = self._parse_rows(page)
        logger.info(f"Listing OK: {len(rows)} rows parsed")
        if not rows:
            logger.error("No rows parsed")
            return False
        pdf = self._fetch_pdf(rows[0]["token"])
        if not pdf:
            logger.error("PDF download failed")
            return False
        text = extract_pdf_text(pdf)
        logger.info(f"Sample PDF text length: {len(text or '')} chars")
        return bool(text and len(text) > 100)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="MX/PoderJudicialBajaCaliforniaSur data fetcher")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BajaCaliforniaSurCourtScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {stats}")
        if stats.get("records_new", 0) == 0 and stats.get("records_fetched", 0) == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
