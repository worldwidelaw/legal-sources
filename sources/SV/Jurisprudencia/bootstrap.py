#!/usr/bin/env python3
"""
SV/Jurisprudencia -- El Salvador Judicial Documentation Center

Fetches court decisions from jurisprudencia.gob.sv via PHP AJAX search
and extracts full text from linked PDFs.

Strategy:
  - POST to result.php with date ranges to enumerate decisions
  - Parse HTML response for case metadata and PDF paths
  - Download PDFs from DocumentosBoveda and extract text via pdfplumber
  - Iterate year-by-year to stay under the 300 result cap per query

Courts covered: Sala Constitucional, Sala Civil, Sala Penal,
Sala Contencioso Administrativo, appellate chambers, tribunals, courts.

License: Open (government public records)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import hashlib
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SV.Jurisprudencia")

SEARCH_URL = "https://www.jurisprudencia.gob.sv/busqueda/result.php"
BASE_URL = "https://www.jurisprudencia.gob.sv/busqueda/"
SESSION_URL = "https://www.jurisprudencia.gob.sv/busqueda/busquedalibre.php?id=1"


def _parse_date_sv(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except (ValueError, AttributeError):
        return None


def _parse_results_html(html: str) -> list:
    """Parse the HTML table of search results into structured records."""
    results = []
    # Each result row pattern: showFile.php?bd=...&data=...&number=...&fecha=...&numero=...
    row_pattern = re.compile(
        r"showFile\.php\?bd=(\d+)&data=([^&'\"]+)&number=(\d+)&fecha=([^&'\"]+)&numero=([^&'\"]+)",
        re.DOTALL,
    )
    # Metadata patterns
    origen_pat = re.compile(r"Origen</b></i>:\s*([^<]+)", re.I)
    tribunal_pat = re.compile(r"Nombre de tribunal</b></i>:\s*([^<]+)", re.I)
    tipo_res_pat = re.compile(r"Tipo de Resolución</b></i>:\s*([^<]+)", re.I)
    materia_pat = re.compile(r"Materia</b></i>:\s*([^<]+)", re.I)
    fallo_pat = re.compile(r"Fallo</b></i>:\s*([^<]+)", re.I)

    # Split by table rows
    rows = re.split(r"<tr>", html)
    for row in rows:
        match = row_pattern.search(row)
        if not match:
            continue

        bd = match.group(1)
        pdf_path = unquote(match.group(2))
        number = match.group(3)
        fecha = unquote(match.group(4))
        numero = unquote(match.group(5))

        # Extract metadata from the row
        origen = ""
        m = origen_pat.search(row)
        if m:
            origen = m.group(1).strip()

        tribunal = ""
        m = tribunal_pat.search(row)
        if m:
            tribunal = m.group(1).strip()

        tipo_resolucion = ""
        m = tipo_res_pat.search(row)
        if m:
            tipo_resolucion = m.group(1).strip()

        materia = ""
        m = materia_pat.search(row)
        if m:
            materia = m.group(1).strip()

        fallo = ""
        m = fallo_pat.search(row)
        if m:
            fallo = m.group(1).strip()

        results.append({
            "bd": bd,
            "pdf_path": pdf_path,
            "number": number,
            "fecha": fecha,
            "numero": numero,
            "origen": origen,
            "tribunal": tribunal,
            "tipo_resolucion": tipo_resolucion,
            "materia": materia,
            "fallo": fallo,
        })

    return results


class SVJurisprudenciaScraper(BaseScraper):
    """
    Scraper for SV/Jurisprudencia -- El Salvador court decisions.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        """Get a requests session with cookies from the search page."""
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            })
            # Hit the search page to get session cookie
            self.session.get(SESSION_URL, timeout=15)
        return self.session

    def _search(self, year: int, max_results: int = 300) -> list:
        """Search for decisions in a given year using advanced search."""
        session = self._get_session()
        data = {
            "avanzada": "true",
            "baseDatos": "1",
            "nivel1": "-1",
            "nivel2": "-1",
            "nivel3": "-1",
            "nivel4": "-1",
            "maximo": str(max_results),
            "inicio": f"{year}-01-01",
            "fin": f"{year}-12-31",
            "numeroReferencia": "-1",
            "nombreDocumento": "-1",
            "propiedades": "",
        }
        resp = session.post(SEARCH_URL, data=data, timeout=30)
        resp.raise_for_status()
        return _parse_results_html(resp.text)

    def _search_by_court(self, nivel1: int, nivel2: int, year: int,
                         max_results: int = 300) -> list:
        """Search for decisions by court and year."""
        session = self._get_session()
        data = {
            "libre": "true",
            "txtBusquedaLibre": "*",
            "baseDatos": "1",
            "nivel1": str(nivel1),
            "nivel2": str(nivel2),
            "nivel3": "0",
            "nivel4": "0",
            "maximo": str(max_results),
            "inicio": f"{year}-01-01",
            "fin": f"{year}-12-31",
            "tipoBusquedaFrasePalabra": "2",
        }
        resp = session.post(SEARCH_URL, data=data, timeout=30)
        resp.raise_for_status()
        return _parse_results_html(resp.text)

    def _download_pdf_text(self, pdf_path: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        import pdfplumber

        url = f"https://www.jurisprudencia.gob.sv/{pdf_path}"
        session = self._get_session()
        try:
            resp = session.get(url, timeout=60)
            if resp.status_code != 200:
                logger.debug(f"PDF download failed ({resp.status_code}): {url}")
                return None
            if len(resp.content) < 200:
                return None

            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
            pdf.close()

            full_text = "\n\n".join(pages_text)
            full_text = re.sub(r"\n{3,}", "\n\n", full_text)
            full_text = re.sub(r" {2,}", " ", full_text)
            return full_text.strip() if len(full_text) > 50 else None
        except Exception as e:
            logger.debug(f"PDF extraction failed for {pdf_path}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions, iterating year by year."""
        current_year = datetime.now().year
        seen = set()

        for year in range(current_year, 1989, -1):
            logger.info(f"Searching year {year}...")
            time.sleep(2)

            results = self._search(year)
            logger.info(f"  Found {len(results)} results for {year}")

            for rec in results:
                key = rec["numero"]
                if key in seen:
                    continue
                seen.add(key)

                time.sleep(2)
                text = self._download_pdf_text(rec["pdf_path"])
                if not text:
                    continue

                rec["_full_text"] = text
                yield rec

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions since a given date."""
        current_year = datetime.now().year
        for year in range(current_year, since.year - 1, -1):
            time.sleep(2)
            results = self._search(year)
            for rec in results:
                date_iso = _parse_date_sv(rec["fecha"])
                if date_iso and date_iso >= since.strftime("%Y-%m-%d"):
                    time.sleep(2)
                    text = self._download_pdf_text(rec["pdf_path"])
                    if text:
                        rec["_full_text"] = text
                        yield rec

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standardized schema."""
        text = raw.get("_full_text", "").strip()
        if not text:
            return None

        numero = raw.get("numero", "").strip()
        fecha = raw.get("fecha", "").strip()

        doc_id = f"SV-{numero}" if numero else f"SV-{hashlib.sha256(text.encode()).hexdigest()[:16]}"

        tribunal = raw.get("tribunal", "").strip()
        materia = raw.get("materia", "").strip()
        tipo = raw.get("tipo_resolucion", "").strip()

        title_parts = []
        if numero:
            title_parts.append(numero)
        if tribunal:
            title_parts.append(tribunal)
        if tipo:
            title_parts.append(tipo)
        title = " - ".join(title_parts) if title_parts else "El Salvador Court Decision"

        return {
            "_id": doc_id,
            "_source": "SV/Jurisprudencia",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": _parse_date_sv(fecha),
            "url": f"https://www.jurisprudencia.gob.sv/{raw.get('pdf_path', '')}",
            "case_number": numero,
            "court": tribunal,
            "court_group": raw.get("origen", "").strip(),
            "resolution_type": tipo,
            "subject_area": materia,
            "fallo": raw.get("fallo", "").strip(),
        }


if __name__ == "__main__":
    scraper = SVJurisprudenciaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    sample = "--sample" in sys.argv

    if cmd == "test":
        print("Testing jurisprudencia.gob.sv connectivity...")
        try:
            results = scraper._search(2024, max_results=5)
            print(f"OK: Found {len(results)} results for 2024")
            if results:
                r = results[0]
                print(f"  Case: {r['numero']}, Date: {r['fecha']}")
                print(f"  Court: {r['tribunal']}, Subject: {r['materia']}")
                print(f"  PDF: {r['pdf_path']}")
                text = scraper._download_pdf_text(r["pdf_path"])
                if text:
                    print(f"  Text: {len(text)} chars")
                    print(f"  Preview: {text[:200]}")
                else:
                    print("  WARNING: PDF text extraction failed")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif cmd == "bootstrap":
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample else None

        for raw in scraper.fetch_all():
            normalized = scraper.normalize(raw)
            if normalized is None:
                continue

            count += 1
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)

            if count % 50 == 0:
                logger.info(f"Processed {count} records")

            if limit and count >= limit:
                break

        print(f"Saved {count} records to {sample_dir}/")

    elif cmd == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        count = 0
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)
        for raw in scraper.fetch_updates(since):
            normalized = scraper.normalize(raw)
            if normalized:
                count += 1
        print(f"Updated {count} records")

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
