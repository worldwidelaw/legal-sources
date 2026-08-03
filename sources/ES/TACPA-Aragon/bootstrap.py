#!/usr/bin/env python3
"""
ES/TACPA-Aragon -- Tribunal Administrativo de Contratos Publicos de Aragon (TACPA)

Regional public-procurement appeals tribunal for Aragon. It resolves the
"recurso especial en materia de contratacion" (special appeals in public
procurement) plus "cuestiones de nulidad" and issues binding acuerdos. Sibling
of the built ES/TARCJA-Andalucia, ES/OARC-Euskadi and ES/TACP-Madrid, and of the
captcha-blocked central tribunal ES/TACRC.

Strategy:
  - The tribunal publishes its acuerdos through a BASIS/BRSCGI full-text search
    engine at gd.aragon.es. The listing command returns rows of inline metadata
    (numero, resumen tematico, tipo de contrato, sentido, fecha) plus a link to
    each acuerdo's full document:
        GET .../BRSCGI?CMD=VERLST&BASE=ACTA&DOCS=1-100&SEC=ACTA_PUBL&SORT=@FEPU
            &SEPARADOR=&TEMA-C=&NUME-C=&TIPO-C=&SENA-C=
            &@FERE-GE=20110101&@FERE-LE=20261231
    NOTE: BRSCGI rejects a query with no indexed field ("Introduzca un termino
    de busqueda") UNLESS the *full* set of (empty) form fields is supplied
    alongside the @FERE date range -- that combination is what makes the range
    itself the query. Paginate with DOCS=101-200, 201-300, ...
  - Each row's full text is the born-digital PDF served by
        .../BRSCGI?CMD=VEROBJ&MLKOB={id}
    extracted with PyMuPDF (fitz). No OCR needed (born-digital text layer).

Data:
  - ~1,900+ acuerdos, 2011-present
  - Type: case_law  (jurisdiction ES-AR)
  - License: Gobierno de Aragon open data (PSI reuse, Ley 37/2007)
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
logger = logging.getLogger("legal-data-hunter.ES.tacpa-aragon")

CGI = "https://gd.aragon.es/cgi-bin/ACTA/BRSCGI"
PAGE_SIZE = 100
DATE_FROM = "20110101"
DATE_TO = "20261231"
MIN_TEXT_CHARS = 200
MAX_EMPTY_PAGES = 2

RE_MLKOB = re.compile(r"Abrirobjeto\('MLKOB=([0-9A-Za-z+/=_-]+)'\)")
RE_TOTAL = re.compile(r"Documentos?\s+\d+\s+a\s+\d+\s+de\s+(\d+)", re.I)

MONTHS = {
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
    "julio": 7, "agosto": 8, "septiembre": 9, "setiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
}


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", htmllib.unescape(re.sub(r"<[^>]+>", " ", s))).strip()


class TACPAAragonScraper(BaseScraper):
    """
    Scraper for ES/TACPA-Aragon -- Aragon procurement appeals tribunal.
    Country: ES  (jurisdiction ES-AR)
    Data types: case_law
    Auth: none (Gobierno de Aragon open data)
    """

    def __init__(self, source_dir: Optional[str] = None):
        if source_dir is None:
            source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "Legal-Data-Hunter/1.0 (Open Data Research)",
            "Accept": "*/*",
        })

    # -- listing -------------------------------------------------------

    def _list_params(self, start: int) -> dict:
        """Full BRSCGI form field set (empties included) + date range window."""
        return {
            "CMD": "VERLST",
            "BASE": "ACTA",
            "DOCS": f"{start}-{start + PAGE_SIZE - 1}",
            "SEC": "ACTA_PUBL",
            "SORT": "@FEPU",
            "SEPARADOR": "",
            "TEMA-C": "",
            "NUME-C": "",
            "TIPO-C": "",
            "SENA-C": "",
            "@FERE-GE": DATE_FROM,
            "@FERE-LE": DATE_TO,
        }

    def _fetch_list(self, start: int) -> str:
        self.rate_limiter.wait()
        resp = self.session.get(CGI, params=self._list_params(start), timeout=90)
        resp.raise_for_status()
        html = resp.content.decode("latin-1", errors="replace")
        if "Introduzca un t" in html and "MLKOB=" not in html:
            raise RuntimeError(
                "TACPA-Aragon BRSCGI rejected the query "
                "('Introduzca un termino de busqueda') -- form-field set changed"
            )
        return html

    @staticmethod
    def _parse_rows(html: str) -> list:
        rows = []
        matches = list(RE_MLKOB.finditer(html))
        prev = 0
        for m in matches:
            block = _clean(html[prev:m.start()])
            prev = m.end()
            mlkob = m.group(1)

            num = re.search(r"N[uú]mero:\s*([0-9]+/[0-9]{4})", block)
            fecha = re.search(
                r"Fecha de adopci[oó]n del acuerdo:\s*([0-9]{2}/[0-9]{2}/[0-9]{2,4})",
                block,
            )
            tipo = re.search(r"Tipo de contrato:\s*([A-Za-zÁÉÍÓÚáéíóúñÑ]+)", block)
            sent = re.search(r"Sentido del acuerdo:\s*(.*?)\s*Fecha de adopci", block)
            resu = re.search(r"Resumen tem[aá]tico:\s*(.*?)\s*N[uú]mero:", block)

            rows.append({
                "mlkob": mlkob,
                "number": num.group(1) if num else "",
                "fecha": fecha.group(1) if fecha else "",
                "tipo_contrato": tipo.group(1) if tipo else "",
                "sentido": sent.group(1).strip() if sent else "",
                "resumen": resu.group(1).strip() if resu else "",
            })
        return rows

    # -- full text -----------------------------------------------------

    def _extract_pdf_text(self, mlkob: str) -> str:
        if fitz is None:
            raise RuntimeError("PyMuPDF (fitz) is required for full-text extraction")
        self.rate_limiter.wait()
        resp = self.session.get(
            CGI, params={"CMD": "VEROBJ", "MLKOB": mlkob}, timeout=120
        )
        resp.raise_for_status()
        if not resp.content or not resp.content[:4] == b"%PDF":
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

    @staticmethod
    def _iso_from_listing(fecha: str) -> str:
        """Convert dd/mm/yy(yy) to ISO. Two-digit years: 11-26 -> 20xx."""
        if not fecha:
            return ""
        try:
            d, m, y = fecha.split("/")
            if len(y) == 2:
                y = "20" + y
            return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"
        except (ValueError, IndexError):
            return ""

    @staticmethod
    def _iso_from_body(text: str) -> str:
        """Parse a Spanish long-form date from the acuerdo body, e.g.
        'de 25 de marzo de 2014' / 'de 25 de marzo 2014'."""
        m = re.search(
            r"(\d{1,2})\s+de\s+([a-záéíóú]+)\s+(?:de\s+)?(\d{4})",
            text[:1500], re.I,
        )
        if not m:
            return ""
        day = int(m.group(1))
        month = MONTHS.get(m.group(2).lower())
        year = int(m.group(3))
        if not month:
            return ""
        return f"{year:04d}-{month:02d}-{day:02d}"

    # -- data acquisition ----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        start = 1
        empties = 0
        total = None
        while True:
            html = self._fetch_list(start)
            if total is None:
                mt = RE_TOTAL.search(_clean(html))
                if mt:
                    total = int(mt.group(1))
                    logger.info(f"TACPA-Aragon reports {total} acuerdos total")
            rows = self._parse_rows(html)
            if not rows:
                empties += 1
                logger.info(f"DOCS {start}-: no rows (empty {empties}/{MAX_EMPTY_PAGES})")
                if empties >= MAX_EMPTY_PAGES:
                    break
                start += PAGE_SIZE
                continue
            empties = 0
            logger.info(f"DOCS {start}-{start + len(rows) - 1}: {len(rows)} acuerdos")
            for row in rows:
                yield row
            if total is not None and start + PAGE_SIZE > total:
                break
            start += PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_date = since.date() if isinstance(since, datetime) else since
        for row in self.fetch_all():
            iso = self._iso_from_listing(row.get("fecha", ""))
            if iso:
                try:
                    if datetime.strptime(iso, "%Y-%m-%d").date() < since_date:
                        continue
                except ValueError:
                    pass
            yield row

    # -- normalization --------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        mlkob = raw.get("mlkob", "")
        if not mlkob:
            return None
        try:
            text = self._extract_pdf_text(mlkob)
        except Exception as e:
            logger.debug(f"PDF extract failed for MLKOB={mlkob}: {e}")
            return None
        if not text or len(text) < MIN_TEXT_CHARS:
            logger.debug(f"Insufficient text for MLKOB={mlkob}: {len(text)} chars")
            return None

        number = raw.get("number", "")
        date = self._iso_from_listing(raw.get("fecha", "")) or self._iso_from_body(text)

        title = f"Acuerdo {number} del TACPA" if number else f"Acuerdo TACPA {mlkob}"

        return {
            # Required base fields
            "_id": f"ES-AR-TACPA-{mlkob}",
            "_source": "ES/TACPA-Aragon",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,  # MANDATORY FULL TEXT
            "date": date,
            "url": f"{CGI}?CMD=VEROBJ&MLKOB={mlkob}",
            # Additional metadata
            "resolution_number": number,
            "summary": raw.get("resumen", ""),
            "type_contract": raw.get("tipo_contrato", ""),
            "resolution_sense": raw.get("sentido", ""),
            "tribunal": "Tribunal Administrativo de Contratos Públicos de Aragón",
            "jurisdiction": "ES-AR",
            "language": "es",
            "country": "ES",
        }

    # -- connectivity test ---------------------------------------------

    def test_connection(self):
        print("Testing TACPA-Aragon (Aragon procurement tribunal) BRSCGI...")
        try:
            html = self._fetch_list(1)
            mt = RE_TOTAL.search(_clean(html))
            print(f"\n1. Total reported: {mt.group(1) if mt else '?'}")
            rows = self._parse_rows(html)
            print(f"2. Page rows: {len(rows)}")
            if rows:
                r = rows[0]
                print(f"   First: Acuerdo {r['number']} ({r['fecha']}) "
                      f"tipo={r['tipo_contrato']} sentido={r['sentido']}")
                print(f"   Resumen: {r['resumen'][:90]!r}")
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
    scraper = TACPAAragonScraper()

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
