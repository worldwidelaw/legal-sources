#!/usr/bin/env python3
"""
AR/CSJNDatosAbiertos -- Argentina Supreme Court Decisions

Fetches court decisions (fallos) from Argentina's Corte Suprema de Justicia
de la Nación via the sjconsulta portal.

Strategy:
  - Enumerate analysis IDs (1 to ~824,000+)
  - Fetch metadata JSON via getAllDocumentos.html?idAnalisis={id}
  - Download PDF via verDocumentoById.html?idDocumento={codigo}
  - Extract full text from PDF using pdfplumber

Data: ~800K+ analysis IDs, decisions since 1863, full PDFs since ~1994.
License: Open data (public court decisions).
Rate limit: 1 req/sec (self-imposed).

Usage:
  python bootstrap.py bootstrap            # Full pull (scans all IDs)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py update --since DATE  # Fetch decisions after DATE
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.CSJNDatosAbiertos")

BASE_URL = "https://sjconsulta.csjn.gov.ar/sjconsulta"
METADATA_URL = f"{BASE_URL}/documentos/getAllDocumentos.html"
PDF_URL = f"{BASE_URL}/documentos/verDocumentoById.html"
VIEW_URL = f"{BASE_URL}/documentos/verDocumentoByIdLinks498.html"

# Known populated ID ranges (to speed up scanning)
# Based on testing: IDs 1-~50000 and ~550000-824000+ are populated
# IDs ~50000-550000 are sparse
DENSE_RANGES = [(1, 50000), (550000, 830000)]
SPARSE_RANGES = [(50001, 549999)]

# Sample IDs known to have data and PDFs (recent decisions)
SAMPLE_IDS = [
    824000, 823900, 823800, 823700, 823600,
    823500, 823400, 823300, 823200, 823100,
    823000, 822900, 822800, 822700, 822600,
    822500, 822400, 822300, 822200, 822100,
]


class CSJNScraper(BaseScraper):
    """
    Scraper for AR/CSJNDatosAbiertos -- Argentina Supreme Court.
    Country: AR
    URL: https://sjconsulta.csjn.gov.ar/sjconsulta/

    Data types: case_law
    Auth: none (public data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, application/pdf, */*",
                "Referer": f"{BASE_URL}/",
            },
            timeout=60,
        )

    # -- Metadata fetching ---------------------------------------------------

    def _fetch_metadata(self, id_analisis: int) -> Optional[dict]:
        """Fetch metadata for a given analysis ID."""
        try:
            resp = self.client.get(
                METADATA_URL,
                params={"idAnalisis": id_analisis},
                timeout=15,
            )
            if resp is None or resp.status_code != 200:
                return None
            data = resp.json()
            if not data or len(data) == 0:
                return None
            return data[0]
        except Exception as e:
            logger.debug(f"Metadata fetch failed for ID {id_analisis}: {e}")
            return None

    # -- PDF text extraction -------------------------------------------------

    def _extract_text_from_pdf(self, id_documento: int) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="AR/CSJNDatosAbiertos",
            source_id="",
            pdf_bytes=id_documento,
            table="case_law",
        ) or ""

    # -- Date parsing --------------------------------------------------------

    @staticmethod
    def _parse_date(doc: dict) -> Optional[str]:
        """Extract decision date from document metadata."""
        # Try fechaString first (format: DD/MM/YYYY)
        fecha_str = doc.get("fechaString")
        if fecha_str:
            try:
                dt = datetime.strptime(fecha_str, "%d/%m/%Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try fechaLong (format: YYYYMMDD as integer)
        fecha_long = doc.get("fechaLong")
        if fecha_long:
            s = str(fecha_long)
            if len(s) == 8:
                try:
                    dt = datetime.strptime(s, "%Y%m%d")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Try anioFallo (just the year)
        anio = doc.get("anioFallo")
        if anio:
            return f"{anio}-01-01"

        return None

    # -- Build raw record from metadata + PDF --------------------------------

    def _build_record(self, doc: dict) -> Optional[dict]:
        """Build a raw record from API metadata and PDF text."""
        codigo = doc.get("codigo")
        ad = doc.get("analisisDocumental", {})
        id_analisis = ad.get("id")

        if not codigo or not id_analisis:
            return None

        # Extract text from PDF
        self.rate_limiter.wait()
        text = self._extract_text_from_pdf(codigo)
        if not text:
            logger.debug(f"No text extracted for ID {id_analisis} (doc {codigo})")
            return None

        # Build raw record
        caratula = ad.get("caratula") or ad.get("caratulaFallo") or ""
        titulo = doc.get("titulo") or ""

        competencia = None
        comp_obj = ad.get("competencia")
        if comp_obj and isinstance(comp_obj, dict):
            competencia = comp_obj.get("valor")

        tipo_doc = None
        tipo_obj = doc.get("tipoDocumento")
        if tipo_obj and isinstance(tipo_obj, dict):
            tipo_doc = tipo_obj.get("valor")

        materia = None
        mat_obj = ad.get("materiaSecretaria")
        if mat_obj and isinstance(mat_obj, dict):
            materia = mat_obj.get("valor")

        sentido = None
        sent_obj = ad.get("sentidoPronunciamiento")
        if sent_obj and isinstance(sent_obj, dict):
            sentido = sent_obj.get("valor", "").strip()

        # Expediente info
        expediente_str = None
        recibo = ad.get("reciboEntrada")
        if recibo and isinstance(recibo, dict):
            exps = recibo.get("reciboEntradaExpedientes", [])
            if exps and len(exps) > 0:
                exp = exps[0].get("expediente", {})
                if exp:
                    num = exp.get("numeroExpediente", "")
                    anio = exp.get("anioExpediente", "")
                    cam = exp.get("camara", {})
                    abrev = cam.get("abreviatura", "") if isinstance(cam, dict) else ""
                    if num and anio:
                        expediente_str = f"{abrev} {num}/{anio}".strip()

        return {
            "id_analisis": id_analisis,
            "codigo": codigo,
            "titulo": titulo,
            "caratula": caratula,
            "text": text,
            "date": self._parse_date(doc),
            "anio_fallo": doc.get("anioFallo"),
            "competencia": competencia,
            "tipo_documento": tipo_doc,
            "materia_secretaria": materia,
            "sentido_pronunciamiento": sentido,
            "expediente": expediente_str,
            "tomo": doc.get("tomoDocAsoc"),
            "pagina": doc.get("paginaDocAsoc"),
        }

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all court decisions by scanning ID ranges."""
        total_found = 0
        total_scanned = 0

        # Scan dense ranges first (most decisions are here)
        for start, end in DENSE_RANGES:
            logger.info(f"Scanning dense range {start}-{end}")
            for id_analisis in range(start, end + 1):
                total_scanned += 1
                if total_scanned % 500 == 0:
                    logger.info(
                        f"Progress: scanned {total_scanned}, found {total_found}"
                    )

                self.rate_limiter.wait()
                doc = self._fetch_metadata(id_analisis)
                if not doc:
                    continue

                record = self._build_record(doc)
                if record:
                    total_found += 1
                    yield record

        # Then sparse ranges (less populated)
        for start, end in SPARSE_RANGES:
            logger.info(f"Scanning sparse range {start}-{end} (step 10)")
            for id_analisis in range(start, end + 1, 10):
                total_scanned += 1
                self.rate_limiter.wait()
                doc = self._fetch_metadata(id_analisis)
                if not doc:
                    continue

                record = self._build_record(doc)
                if record:
                    total_found += 1
                    yield record

        logger.info(f"Scan complete: {total_found} records from {total_scanned} IDs")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions by scanning from high IDs downward."""
        since_str = since.strftime("%Y%m%d")
        logger.info(f"Fetching updates since {since.date()}")

        # Start from the highest known ID and work backward
        max_id = 830000
        consecutive_empty = 0
        found = 0

        for id_analisis in range(max_id, 0, -1):
            self.rate_limiter.wait()
            doc = self._fetch_metadata(id_analisis)

            if not doc:
                consecutive_empty += 1
                if consecutive_empty > 200:
                    logger.info(f"200 consecutive empty IDs, stopping scan")
                    break
                continue

            consecutive_empty = 0

            # Check if this decision is after 'since'
            fecha_long = doc.get("fechaLong")
            if fecha_long and int(str(fecha_long)) < int(since_str):
                logger.info(f"Reached decisions before {since.date()}, stopping")
                break

            record = self._build_record(doc)
            if record:
                found += 1
                yield record

        logger.info(f"Update complete: {found} new records")

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch a sample of recent decisions for validation."""
        found = 0
        tried = 0

        for id_analisis in SAMPLE_IDS:
            if found >= count:
                break

            tried += 1
            self.rate_limiter.wait()
            doc = self._fetch_metadata(id_analisis)
            if not doc:
                logger.debug(f"Sample ID {id_analisis}: no data")
                continue

            record = self._build_record(doc)
            if record:
                found += 1
                logger.info(
                    f"Sample {found}/{count}: ID {id_analisis} - "
                    f"{record['caratula'][:60]} ({record['date']})"
                )
                yield record
            else:
                logger.debug(f"Sample ID {id_analisis}: no text extracted")

        logger.info(f"Sample complete: {found} records from {tried} IDs")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to standard schema."""
        title = raw.get("caratula") or raw.get("titulo") or "Unknown"
        # Clean up title
        title = re.sub(r"\s+", " ", title).strip()

        return {
            "_id": f"AR-CSJN-{raw['id_analisis']}",
            "_source": "AR/CSJNDatosAbiertos",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("date"),
            "url": f"{VIEW_URL}?idAnalisis={raw['id_analisis']}",
            "id_analisis": raw["id_analisis"],
            "expediente": raw.get("expediente"),
            "competencia": raw.get("competencia"),
            "tipo_documento": raw.get("tipo_documento"),
            "materia_secretaria": raw.get("materia_secretaria"),
            "sentido": raw.get("sentido_pronunciamiento"),
            "tomo": raw.get("tomo"),
            "pagina": raw.get("pagina"),
        }

    def test_api(self) -> bool:
        """Test API connectivity."""
        logger.info("Testing CSJN sjconsulta API...")

        # Test metadata endpoint
        doc = self._fetch_metadata(824000)
        if not doc:
            logger.error("Metadata endpoint failed")
            return False
        logger.info(f"Metadata OK: {doc.get('analisisDocumental', {}).get('caratula', 'N/A')[:60]}")

        # Test PDF endpoint
        codigo = doc.get("codigo")
        if codigo:
            text = self._extract_text_from_pdf(codigo)
            if text:
                logger.info(f"PDF extraction OK: {len(text)} chars")
            else:
                logger.warning("PDF extraction returned no text")
                return False
        else:
            logger.warning("No document code in metadata")
            return False

        logger.info("All API tests passed")
        return True


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = CSJNScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N] [--since DATE]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == "--since" and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]

        if not since_str:
            print("Usage: python bootstrap.py update --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        gen = scraper.fetch_updates(since)

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
