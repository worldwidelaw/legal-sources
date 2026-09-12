#!/usr/bin/env python3
"""
NI/BCN-Regulations — Banco Central de Nicaragua Regulatory Documents

Fetches regulatory documents (normas, resoluciones, reglamentos, leyes financieras)
from the BCN website. HTML pages are behind Radware bot protection, but static
PDF files serve directly. Discovery uses the Wayback CDX API plus known URLs.

Strategy:
  1. Query Wayback CDX API for PDF URLs in normas_disposiciones/
  2. Merge with known marco_juridico_financiero/ URLs
  3. Download PDFs directly from bcn.gob.ni (no bot protection on static files)
  4. Extract text with PyMuPDF / pdfplumber
  5. Normalize into standard schema

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NI.BCN-Regulations")

BASE_URL = "https://www.bcn.gob.ni"
SOURCE_ID = "NI/BCN-Regulations"
CDX_API = "https://web.archive.org/cdx/search/cdx"
NORMAS_DIR = "/sites/default/files/normas_disposiciones/"
MJF_DIR = "/sites/default/files/marco_juridico_financiero/"

# Known marco_juridico_financiero PDFs (not reliably in CDX)
KNOWN_MJF_FILES = [
    "02_Ley_No_732_Ley_Organica_BCN.pdf",
    "03_Ley_No_1235_Ley_del_Comite_de_Estabilidad_Financiera.pdf",
    "04_Ley_No_316_Ley_Superintendencia_de_Bancos_y_Otras_Instituciones_Financieras.pdf",
    "05_Decreto_No_1824_Ley_General_de_Titulos_Valores_(Digesto_Juridico_Banca_y_Finanzas).pdf",
    "06_Ley_No_176_Reguladora_de_Prestamos_Particulares.pdf",
    "07_Ley_No_515_Ley_de_Promocion_y_Uso_de_Tarjeta_de_Credito.pdf",
    "09_Ley_No_587_Ley_de_Mercado_de_Capiltales.pdf",
    "10_Ley_No_976_Ley_de_la_Unidad_de_Analisis_Financiero.pdf",
    "12_Ley_No_977_Ley_contra_el_Lavado_de_Activos.pdf",
    "13_Ley_No_734_Ley_de_Almacenes.pdf",
    "14_Ley_No_733_General_de_Seguros.pdf",
    "Ley_1232_Administraci%C3%B3n_del_Sistema_Monetario_Financiero.pdf",
]

# Known normas_disposiciones PDFs (CDX fallback — CDX can timeout)
KNOWN_NORMAS_FILES = [
    "CD_BCN_xl_1_17.pdf",
    "CD_Norma_Proveedores_PSP_PSAV.pdf",
    "Certificacion%20de%20Resolucion%20CD-BCN-XV-1-16.pdf",
    "CertificacionPoliticadeRecuperaciondeCartera.pdf",
    "Liberacion_Encaje_Legal_Moneda_Nacional_Financiamiento_Actividad_Economica.pdf",
    "Norma_%20apertura_manejo_cuentasBCN.pdf",
    "norma_administradores_sp.pdf",
    "Norma_de_los_Proveedores_de_Servicios_de_Pago_de_Remesas_y_de_Compraventa_o_Cambio_de_Monedas.pdf",
    "norma_mercado_cambio_operaciones_con_divisas.pdf",
    "Norma_para_el_Canje_de_Billetes_y_Monedas.pdf",
    "Norma_Procedimientos_Contratacion_Impresion_Billetes_Acunacion_Monedas.pdf",
    "Norma_procedimientos_Seleccion_Contratacion_Firmas_Contadores_Publicos_Independientes.pdf",
    "Norma_sobre_el_Encaje_Legal.pdf",
    "Norma_sobre_emision_valores_y_otras_operaciones_monetarias.pdf",
    "Norma_Tramitacion_Recursos_Administrativos.pdf",
    "NormaContratacionInstitucionesFinancierasyEmpresasGestionAdministracionRIB.pdf",
    "normas_financieras_BCN_con_reformas.pdf",
    "Normativa%20para%20Aplicaci%C3%B3n%20de%20Encaje%20Legal.pdf",
    "Normativa%20para%20la%20Apertura%20y%20Manejo%20de%20Cuentas%20en%20el%20BCN.pdf",
    "Politica_Admon_RIBvfinal_Web_BCN.pdf",
    "RA_GG-08-MAYO-2025-LASMF-DO-PSP_PVSA.pdf",
    "Reglamento%20para%20el%20Est%C3%A1ndar%20de%20Cuentas%20Bancarias.pdf",
    "reglamento_administradores_sp.pdf",
    "Reglamento_Aplicaci%C3%B3n_Norma_Emision_Valores_Otras_Operaciones_Monetarias.pdf",
    "Reglamento_atencion_consumidores_y_usuarios.pdf",
    "Reglamento_norma_administradores_SPE.pdf",
    "Reglamento_Proveedores_cambio_Monedas.pdf",
    "Reglamento_Proveedores_Servicios_pago_Remesas.pdf",
    "Reglamento_Proveedores_Tecnologia_Financiera_Servicios_Pago.pdf",
    "Reglamento_subagentes_proveedores_servicios_pago_remesas.pdf",
    "reglamento_truncamiento_cheques.pdf",
    "Reglamento_vigilancia_supervision_sistemas_pagos.pdf",
    "Resoluci%C3%B3n_Administrativa_GG-08-MAYO-2025-LASMF-DO%20-PSP_PVSA.pdf",
    "Resolucion_administrativa_GG-21-NOVIEMBRE-2025-LASMF-DO.pdf",
    "Resolucion_administrativa_GG-PSPR_PSCM.pdf",
    "resolucionCD-BCN-XVI-1-19_venta_inmuebles_residuales.pdf",
]


def _extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using PyMuPDF, falling back to pdfplumber."""
    try:
        import fitz
    except ImportError:
        try:
            import pdfplumber
            import io
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p.strip())
                return text if text.strip() else None
        except ImportError:
            logger.error("No PDF library available (need PyMuPDF or pdfplumber)")
            return None

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                pages.append(text)
        doc.close()
        return "\n\n".join(pages) if pages else None
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return None


def _title_from_filename(filename: str) -> str:
    """Derive a human-readable title from a PDF filename."""
    name = unquote(filename)
    if name.lower().endswith(".pdf"):
        name = name[:-4]
    name = name.replace("_", " ").replace("-", " — ", 1).replace("-", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _infer_doc_type(title: str) -> str:
    """Infer document type from title."""
    t = title.upper()
    if "RESOLUCI" in t or "RESOLUCION" in t:
        return "resolucion"
    if "NORMA" in t:
        return "norma"
    if "REGLAMENTO" in t:
        return "reglamento"
    if "LEY " in t or "LEY_" in t:
        return "ley"
    if "DECRETO" in t:
        return "decreto"
    if "CIRCULAR" in t:
        return "circular"
    if "CERTIFICACI" in t:
        return "certificacion"
    if "POLITICA" in t:
        return "politica"
    if "NORMATIVA" in t:
        return "normativa"
    return "otro"


def _infer_category(url: str) -> str:
    """Infer document category from URL path."""
    if "marco_juridico_financiero" in url:
        return "marco_juridico"
    if "normas_disposiciones" in url:
        return "normas_disposiciones"
    return "otro"


class BCNScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/pdf,*/*",
            },
            timeout=120,
        )

    def _discover_cdx_urls(self, path_pattern: str) -> list[str]:
        """Discover PDF URLs via Wayback CDX API."""
        logger.info("Querying CDX for: %s", path_pattern)
        try:
            import requests
            r = requests.get(CDX_API, params={
                "url": f"www.bcn.gob.ni{path_pattern}*",
                "output": "json",
                "fl": "original,mimetype,statuscode",
                "filter": "mimetype:application/pdf",
                "collapse": "urlkey",
                "limit": 500,
            }, timeout=60)
            if r.status_code != 200:
                logger.warning("CDX returned %d", r.status_code)
                return []
            data = r.json()
            urls = []
            for row in data[1:]:  # skip header
                url = row[0]
                # Normalize to https://www.bcn.gob.ni/...
                if url.startswith("http://"):
                    url = "https://" + url[7:]
                if not url.startswith("https://www."):
                    url = url.replace("https://bcn.", "https://www.bcn.")
                urls.append(url)
            logger.info("CDX returned %d unique PDF URLs for %s", len(urls), path_pattern)
            return urls
        except Exception as e:
            logger.warning("CDX query failed: %s", e)
            return []

    def _get_all_pdf_urls(self) -> list[str]:
        """Get deduplicated list of all PDF URLs to fetch."""
        urls = set()

        # 1. CDX discovery for normas_disposiciones
        for url in self._discover_cdx_urls(NORMAS_DIR):
            urls.add(url)

        # 2. CDX discovery for marco_juridico_financiero
        for url in self._discover_cdx_urls(MJF_DIR):
            urls.add(url)

        # 3. Known marco_juridico_financiero files
        for f in KNOWN_MJF_FILES:
            urls.add(f"{BASE_URL}{MJF_DIR}{f}")

        # 4. Known normas_disposiciones files (CDX fallback)
        for f in KNOWN_NORMAS_FILES:
            urls.add(f"{BASE_URL}{NORMAS_DIR}{f}")

        # Deduplicate by filename (some URLs differ only in www vs non-www)
        seen_filenames = {}
        for url in urls:
            filename = unquote(url.rstrip("/").split("/")[-1]).lower()
            if filename not in seen_filenames:
                seen_filenames[filename] = url

        result = sorted(seen_filenames.values())
        logger.info("Total unique PDF URLs: %d", len(result))
        return result

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        logger.info("Downloading PDF: %s", pdf_url)
        try:
            resp = self.http.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning("PDF download failed: HTTP %d for %s", resp.status_code, pdf_url)
                return None

            content = resp.content
            if len(content) < 500:
                logger.warning("PDF too small (%d bytes): %s", len(content), pdf_url)
                return None

            text = _extract_text_from_pdf(content)
            if text:
                logger.info("Extracted %d chars from %s", len(text), pdf_url.split("/")[-1][:60])
            else:
                logger.warning("No text extracted from %s", pdf_url.split("/")[-1][:60])
            return text
        except Exception as e:
            logger.warning("Error downloading %s: %s", pdf_url, e)
            return None

    def _make_id(self, pdf_url: str) -> str:
        """Generate a stable document ID from the URL."""
        filename = unquote(pdf_url.rstrip("/").split("/")[-1])
        if filename.lower().endswith(".pdf"):
            filename = filename[:-4]
        # Clean up for ID purposes
        doc_id = re.sub(r"[^\w\-]", "_", filename)
        doc_id = re.sub(r"_+", "_", doc_id).strip("_")
        return doc_id

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": raw["_fetched_at"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "doc_type": raw.get("doc_type", "otro"),
            "category": raw.get("category", "otro"),
            "language": "es",
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        pdf_urls = self._get_all_pdf_urls()
        fetched_at = datetime.now(timezone.utc).isoformat()
        doc_count = 0
        sample_limit = 15 if sample else 999999

        for pdf_url in pdf_urls:
            if doc_count >= sample_limit:
                break

            time.sleep(2)  # Rate limit
            text = self._download_and_extract(pdf_url)
            if not text:
                continue

            filename = unquote(pdf_url.rstrip("/").split("/")[-1])
            title = _title_from_filename(filename)
            doc_id = self._make_id(pdf_url)

            record = {
                "_id": doc_id,
                "_source": SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": fetched_at,
                "title": title,
                "text": text,
                "date": None,
                "pdf_url": pdf_url,
                "doc_type": _infer_doc_type(title),
                "category": _infer_category(pdf_url),
            }

            yield self.normalize(record)
            doc_count += 1
            logger.info("Yielded doc %d: %s (%d chars)", doc_count, title[:50], len(text))

        logger.info("Total documents fetched: %d", doc_count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def test_api(self) -> dict:
        """Quick connectivity test."""
        test_url = f"{BASE_URL}{NORMAS_DIR}normas_financieras_BCN_con_reformas.pdf"
        text = self._download_and_extract(test_url)
        return {
            "status": "ok" if text else "error",
            "test_url": test_url,
            "text_length": len(text) if text else 0,
            "has_text": text is not None and len(text) > 0,
        }


def main():
    scraper = BCNScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        result = scraper.test_api()
        print(json.dumps(result, indent=2, ensure_ascii=False))

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=sample):
            count += 1
            if sample:
                out = sample_dir / f"{count:03d}.json"
                out.write_text(json.dumps(record, indent=2, ensure_ascii=False), encoding="utf-8")
                print(f"  [{count}] {record['title'][:60]} ({len(record.get('text', ''))} chars)")

        print(f"\nTotal: {count} records")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
