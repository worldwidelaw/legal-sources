#!/usr/bin/env python3
"""
ES/TACPC-Canarias -- Tribunal Administrativo de Contratos Publicos de la
Comunidad Autonoma de Canarias (TACP Canarias)

Regional public-procurement appeals tribunal for the Canary Islands, created by
Decreto 10/2015. It resolves the "recurso especial en materia de contratacion"
(special appeals in public procurement) and issues binding resoluciones. Sibling
of the built ES/TARCJA-Andalucia, ES/OARC-Euskadi, ES/TACP-Madrid and
ES/TACPA-Aragon, and of the captcha-blocked central tribunal ES/TACRC.

Strategy:
  - The tribunal publishes its resoluciones through a Solr-backed search visor
    (OpenCms portal) at gobiernodecanarias.org. The listing is a server-rendered
    HTML POST to the visor index.jsp:
        POST .../tacp/visor_resoluciones/index.jsp
            param_accion=buscar
            chpae_resolucion_fecharesolucion_es_dt_d=DD/MM/YYYY   (fecha desde)
            chpae_resolucion_fecharesolucion_es_dt=DD/MM/YYYY      (fecha hasta)
    The response embeds a <ul class="list"> of result <li> items, each with the
    resolution number (num/anio), fecha de resolucion, expediente, resumen and a
    link to the born-digital PDF (/cmsgob1/export/sites/.../RES-*.pdf).
    NOTE: the visor caps the rendered result set at 200 items regardless of the
    reported "Se han encontrado N resultado/s" total, and server-side param_page
    pagination is a no-op. To retrieve the full corpus we window the search by
    date: one window per year, subdivided into months whenever a year exceeds the
    200-item cap. The date filter is one of the fields that works for ALL years
    (full-field search only works 2023-present, but fecha desde/hasta works back
    to 2015).
  - Each row's full text is the born-digital PDF, extracted with PyMuPDF (fitz).
    No OCR needed (text layer present).

Data:
  - ~2,200+ resoluciones, 2015-present
  - Type: case_law  (jurisdiction ES-CN)
  - License: Gobierno de Canarias open data (PSI reuse, Ley 37/2007)
  - Language: Spanish (es)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records for validation
  python bootstrap.py bootstrap-fast     # Concurrent full pull (VPS entrypoint)
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmllib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.tacpc-canarias")

HOST = "https://www.gobiernodecanarias.org"
VISOR = HOST + "/hacienda/contratacion/tacp/visor_resoluciones/index.jsp"
FIRST_YEAR = 2015
DISPLAY_CAP = 200          # visor renders at most 200 items per response
MIN_TEXT_CHARS = 200

RE_ITEM = re.compile(
    r'<h4 class="resultado_titulo">\s*'
    r'<a[^>]*href="([^"]+\.pdf)"[^>]*title="([^"]*)"[^>]*>(.*?)</a>',
    re.I | re.S,
)
RE_TOTAL = re.compile(r"Se han encontrado\s+(\d+)\s+resultado", re.I)
RE_DATE = re.compile(r'doc-date">\s*([0-9]{2})-([0-9]{2})-([0-9]{4})', re.I)
RE_EXPEDIENTE = re.compile(r"Expediente:\s*</strong>\s*([^<]+)", re.I)

MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}
MONTH_LAST_DAY = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


class TACPCCanariasScraper(BaseScraper):
    """
    Scraper for ES/TACPC-Canarias -- Canary Islands procurement appeals tribunal.
    Country: ES  (jurisdiction ES-CN)
    Data types: case_law
    Auth: none (Gobierno de Canarias open data)
    """

    def __init__(self, source_dir: Optional[str] = None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    # -- listing -------------------------------------------------------

    def _search(self, date_from: str, date_to: str) -> str:
        """POST a date-windowed search. Dates are DD/MM/YYYY strings."""
        self.rate_limiter.wait()
        resp = self.session.post(
            VISOR,
            data={
                "param_accion": "buscar",
                "param_page": "1",
                "chpae_resolucion_fecharesolucion_es_dt_d": date_from,
                "chpae_resolucion_fecharesolucion_es_dt": date_to,
            },
            timeout=90,
        )
        resp.raise_for_status()
        resp.encoding = "utf-8"
        return resp.text

    @staticmethod
    def _parse_total(html: str) -> int:
        m = RE_TOTAL.search(html)
        return int(m.group(1)) if m else 0

    @staticmethod
    def _parse_items(html: str) -> list:
        """Extract result rows: each <li> holds the PDF link + inline metadata."""
        rows = []
        i = html.find('<ul class="list">')
        if i < 0:
            return rows
        block = html[i:]
        # split into individual <li> items so metadata stays with its link
        items = re.split(r"<li>", block)
        for item in items:
            m = RE_ITEM.search(item)
            if not m:
                continue
            href, title_attr, anchor = m.group(1), m.group(2), _clean(m.group(3))
            number = _clean(title_attr) or anchor
            dm = RE_DATE.search(item)
            date = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}" if dm else ""
            exp = RE_EXPEDIENTE.search(item)
            expediente = _clean(exp.group(1)) if exp else ""
            # first doc-summay <p> after expediente carries the resumen
            resumen = ""
            sm = re.search(
                r'doc-summay">(?:<p[^>]*>)?\s*(?!<strong>Expediente)(.*?)</p>',
                item, re.I | re.S,
            )
            for cand in re.finditer(r'<p class="doc-summay">(.*?)</p>', item, re.I | re.S):
                txt = _clean(cand.group(1))
                if txt and not txt.startswith("Expediente") and not txt.startswith("Acto") \
                        and not txt.startswith("Tipo"):
                    resumen = txt
                    break
            rows.append({
                "url": href if href.startswith("http") else HOST + href,
                "number": number,
                "date": date,
                "expediente": expediente,
                "resumen": resumen,
            })
        return rows

    def _window(self, date_from: str, date_to: str) -> list:
        html = self._search(date_from, date_to)
        total = self._parse_total(html)
        rows = self._parse_items(html)
        return rows, total

    # -- full text -----------------------------------------------------

    def _extract_pdf_text(self, url: str) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for full-text extraction")
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()
        if not resp.content or resp.content[:4] != b"%PDF":
            return ""
        doc = fitz.open(stream=resp.content, filetype="pdf")
        try:
            parts = [page.get_text() for page in doc]
        finally:
            doc.close()
        text = "\n".join(parts)
        text = re.sub(r"[ \t]+\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # -- data acquisition ----------------------------------------------

    def _iter_windows(self) -> Generator[dict, None, None]:
        """Yield unique rows, windowing by year and subdividing to months when a
        year exceeds the 200-item display cap."""
        seen = set()
        this_year = datetime.now(timezone.utc).year
        for year in range(FIRST_YEAR, this_year + 1):
            rows, total = self._window(f"01/01/{year}", f"31/12/{year}")
            if total > DISPLAY_CAP or len(rows) >= DISPLAY_CAP:
                logger.info(
                    f"{year}: {total} reported > cap {DISPLAY_CAP} -- "
                    f"subdividing into months"
                )
                rows = []
                for mo in range(1, 13):
                    last = MONTH_LAST_DAY[mo - 1]
                    if mo == 2 and year % 4 != 0:
                        last = 28
                    mrows, mtotal = self._window(
                        f"01/{mo:02d}/{year}", f"{last:02d}/{mo:02d}/{year}"
                    )
                    if mtotal > DISPLAY_CAP or len(mrows) >= DISPLAY_CAP:
                        logger.warning(
                            f"{year}-{mo:02d}: {mtotal} still > cap -- "
                            f"truncated at {len(mrows)} rows"
                        )
                    rows.extend(mrows)
            else:
                logger.info(f"{year}: {total} resoluciones ({len(rows)} rows parsed)")
            for row in rows:
                key = row["url"]
                if key in seen:
                    continue
                seen.add(key)
                yield row

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_windows()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_date = since.date() if isinstance(since, datetime) else since
        for row in self.fetch_all():
            iso = row.get("date", "")
            if iso:
                try:
                    if datetime.strptime(iso, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield row

    # -- normalization --------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw.get("url", "")
        if not url:
            return None
        try:
            text = self._extract_pdf_text(url)
        except Exception as e:
            logger.debug(f"PDF extract failed for {url}: {e}")
            return None
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text for {url}: {len(text)} chars")
            return None

        number = raw.get("number", "")
        # unique id from the PDF filename stem
        stem = url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
        title = f"Resolución {number} del TACP Canarias" if number \
            else f"Resolución TACP Canarias {stem}"

        return {
            # Required base fields
            "_id": f"ES-CN-TACPC-{stem}",
            "_source": "ES/TACPC-Canarias",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": raw.get("date", ""),
            "url": url,
            # Additional metadata
            "resolution_number": number,
            "case_number": raw.get("expediente", ""),
            "summary": raw.get("resumen", ""),
            "tribunal": "Tribunal Administrativo de Contratos Públicos "
                        "de la Comunidad Autónoma de Canarias",
            "jurisdiction": "ES-CN",
            "language": "es",
            "country": "ES",
        }

    # -- connectivity test ---------------------------------------------

    def test_connection(self):
        print("Testing TACPC-Canarias (Canary Islands procurement tribunal) visor...")
        try:
            rows, total = self._window("01/01/2016", "31/12/2016")
            print(f"\n1. 2016 reported total: {total}")
            print(f"2. Page rows parsed: {len(rows)}")
            if rows:
                r = rows[0]
                print(f"   First: Resolución {r['number']} ({r['date']}) "
                      f"exp={r['expediente']}")
                print(f"   Resumen: {r['resumen'][:90]!r}")
                print(f"   URL: {r['url']}")
                rec = self.normalize(r)
                if rec:
                    print(f"   Extracted text: {len(rec['text'])} chars  date={rec['date']}")
                    print(f"   Sample: {rec['text'][:200]!r}")
                else:
                    print("   normalize returned None")
        except Exception as e:
            print(f"   error: {e}")
        print("\nTest complete!")


def main():
    scraper = TACPCCanariasScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
              "[--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} "
                  f"records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        print(f"\nbootstrap-fast complete: {stats['records_fetched']} fetched, "
              f"{stats['records_new']} new, {stats['errors']} errors")
        print(json.dumps(stats, indent=2, default=str))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
