#!/usr/bin/env python3
"""
PE/JNE -- Peru Jurado Nacional de Elecciones (National Elections Jury)

Fetches resolutions and jurisprudence from the JNE via the internal JSON API
at jurisprudencia.jne.gob.pe. Full text is available in the strParteResolutiva
field of each resolution record.

Strategy:
  - Get list of case types from /Home/InicioJurisprudencia
  - For each case type, search with a common term to retrieve all matching
    resolutions via /Resoluciones/ConsultarResoluciones
  - Each resolution includes full text (strParteResolutiva), metadata, and
    optional PDF link

Source: https://jurisprudencia.jne.gob.pe/
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull (all types)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PE.JNE")

BASE_URL = "https://jurisprudencia.jne.gob.pe"
INIT_URL = f"{BASE_URL}/Home/InicioJurisprudencia"
SEARCH_INIT_URL = f"{BASE_URL}/Generales/GeneraConsultaRapida"
SEARCH_RESULTS_URL = f"{BASE_URL}/Resoluciones/ConsultarResoluciones"
PDF_BASE_URL = f"{BASE_URL}/Tmp/Proyectos"

# Common Spanish words to use as search terms for broad retrieval
SEARCH_TERMS = ["de", "la", "el", "en", "que", "por", "se", "con", "del", "los"]


class JNEScraper(BaseScraper):
    """
    Scraper for PE/JNE -- Peru Jurado Nacional de Elecciones.
    Country: PE
    URL: https://jurisprudencia.jne.gob.pe/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest",
                "Content-Type": "application/json",
            },
            timeout=120,
        )
        self._session_cookies = {}

    def _init_session(self):
        """Initialize session by fetching the main page to get cookies."""
        resp = self.client.get(BASE_URL, timeout=30)
        if hasattr(resp, 'cookies'):
            self._session_cookies.update(resp.cookies)

    def _get_case_types(self) -> List[dict]:
        """Fetch list of available case types from the API."""
        self.rate_limiter.wait()
        resp = self.client.get(INIT_URL, timeout=30)
        data = resp.json()
        if not data.get("success"):
            logger.error("Failed to fetch case types")
            return []
        return data["data"]["lTipoExpediente"]

    def _search_resolutions(self, search_term: str, type_ids: str, include_jee: str = "0") -> List[dict]:
        """Search for resolutions matching the given term and type IDs."""
        # First, initialize the search session
        self.rate_limiter.wait()
        init_params = f"?strBusqueda={search_term}|{type_ids}|{include_jee}"
        self.client.post(
            SEARCH_INIT_URL + init_params,
            data="",
            headers={"Content-Length": "0"},
            timeout=30,
        )

        # Then fetch results
        self.rate_limiter.wait()
        result_params = f"?strFrase={search_term}&tipos={type_ids}&incluye={include_jee}"
        resp = self.client.post(
            SEARCH_RESULTS_URL + result_params,
            data="",
            headers={"Content-Length": "0"},
            timeout=120,
        )
        data = resp.json()
        results = data.get("data", {})
        if results is None:
            return []
        return results.get("lConsultaResolucion", []) or []

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse JNE date format (DD/MM/YYYY HH:MM:SS) to ISO 8601."""
        if not date_str:
            return None
        try:
            # Format: "26/08/2019 17:05:22"
            dt = datetime.strptime(date_str.strip(), "%d/%m/%Y %H:%M:%S")
            return dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            try:
                dt = datetime.strptime(date_str.strip()[:10], "%d/%m/%Y")
                return dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                return None

    def normalize(self, raw: dict) -> dict:
        """Transform a raw JNE resolution into the standard schema."""
        id_proyecto = raw.get("idProyecto", 0)
        pronunciamiento = raw.get("strPronunciamiento", "") or ""
        case_number = raw.get("strCodExpedienteExt", "") or ""
        text = raw.get("strParteResolutiva", "") or ""
        date_str = raw.get("strFechaPublicacion", "") or ""
        case_type = raw.get("strTipoExpediente", "") or ""
        subject = raw.get("strMateria", "") or ""
        ruling = raw.get("strFallo", "") or ""
        jurisdiction = raw.get("strProcedencia", "") or ""
        electoral_process = raw.get("strProcesoElectoral", "") or ""
        court = raw.get("strJuradoElectoral", "") or ""
        parties = raw.get("strPartesProcesales", "") or ""
        causales = raw.get("strCausales", "") or ""
        has_pdf = raw.get("fgExisteArchivoFisico", False)

        title = f"{pronunciamiento}"
        if case_number:
            title += f" - {case_number}"
        if case_type:
            title += f" ({case_type})"

        # Build URL to the PDF if available, otherwise to the search page
        url = f"{PDF_BASE_URL}/{id_proyecto}.pdf" if has_pdf else f"{BASE_URL}/"

        # Add contextual metadata to the text
        full_text_parts = []
        if pronunciamiento:
            full_text_parts.append(f"Pronunciamiento: {pronunciamiento}")
        if case_number:
            full_text_parts.append(f"Expediente: {case_number}")
        if case_type:
            full_text_parts.append(f"Tipo: {case_type}")
        if subject:
            full_text_parts.append(f"Materia: {subject}")
        if ruling:
            full_text_parts.append(f"Fallo: {ruling}")
        if jurisdiction:
            full_text_parts.append(f"Procedencia: {jurisdiction}")
        if parties:
            full_text_parts.append(f"Partes procesales: {parties}")
        if causales:
            full_text_parts.append(f"Causales: {causales}")
        if electoral_process and electoral_process != "SIN PROCESO ELECTORAL":
            full_text_parts.append(f"Proceso electoral: {electoral_process}")
        full_text_parts.append("")
        full_text_parts.append(text)

        combined_text = "\n".join(full_text_parts)

        return {
            "_id": f"PE-JNE-{id_proyecto}",
            "_source": "PE/JNE",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": combined_text.strip(),
            "date": self._parse_date(date_str),
            "url": url,
            "case_number": case_number,
            "case_type": case_type,
            "subject_matter": subject,
            "ruling": ruling,
            "jurisdiction": jurisdiction,
            "electoral_process": electoral_process,
        }

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch updates since a given date (not supported, yields nothing)."""
        logger.info("fetch_updates not supported for JNE; use fetch_all")
        return
        yield  # make it a generator

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all resolutions from the JNE jurisprudencia system."""
        logger.info("Fetching case types...")
        case_types = self._get_case_types()
        if not case_types:
            logger.error("No case types found")
            return

        logger.info(f"Found {len(case_types)} case types")
        seen_ids = set()
        total_yielded = 0

        for ct in case_types:
            type_id = ct["idTipoExpediente"]
            type_name = ct["strTipoExpediente"]
            logger.info(f"Searching type {type_id}: {type_name}")

            best_results = []
            # Try multiple search terms and keep the one returning most results
            for term in SEARCH_TERMS[:3]:
                try:
                    results = self._search_resolutions(term, str(type_id))
                    if len(results) > len(best_results):
                        best_results = results
                    if len(results) > 0:
                        logger.info(f"  Term '{term}': {len(results)} results")
                    time.sleep(1)
                except Exception as e:
                    logger.warning(f"  Term '{term}' failed for type {type_id}: {e}")
                    time.sleep(2)
                    continue

            logger.info(f"  Best result for {type_name}: {len(best_results)} resolutions")

            for raw in best_results:
                id_proyecto = raw.get("idProyecto")
                if id_proyecto in seen_ids:
                    continue
                seen_ids.add(id_proyecto)

                text = raw.get("strParteResolutiva", "") or ""
                if len(text.strip()) < 20:
                    continue

                record = self.normalize(raw)
                yield record
                total_yielded += 1

            logger.info(f"  Total unique so far: {total_yielded}")

        logger.info(f"Completed: {total_yielded} unique resolutions")

    def fetch_sample(self, n: int = 15) -> list[dict]:
        """Fetch a sample of resolutions for testing."""
        logger.info(f"Fetching {n} sample resolutions...")
        samples = []
        seen_ids = set()

        # Use vacancia (25) as it has rich content
        sample_types = [25, 24, 13, 15, 20]

        for type_id in sample_types:
            if len(samples) >= n:
                break

            try:
                results = self._search_resolutions("de", str(type_id))
                logger.info(f"Type {type_id}: {len(results)} results")
            except Exception as e:
                logger.warning(f"Type {type_id} failed: {e}")
                time.sleep(2)
                continue

            for raw in results:
                if len(samples) >= n:
                    break

                id_proyecto = raw.get("idProyecto")
                if id_proyecto in seen_ids:
                    continue
                seen_ids.add(id_proyecto)

                text = raw.get("strParteResolutiva", "") or ""
                if len(text.strip()) < 50:
                    continue

                record = self.normalize(raw)
                samples.append(record)

            time.sleep(1)

        logger.info(f"Collected {len(samples)} samples")
        return samples

    def test_api(self):
        """Test API connectivity and print summary."""
        logger.info("Testing JNE API connectivity...")

        # Test init endpoint
        case_types = self._get_case_types()
        logger.info(f"Case types: {len(case_types)}")
        for ct in case_types[:5]:
            logger.info(f"  - {ct['idTipoExpediente']}: {ct['strTipoExpediente']}")

        # Test search
        results = self._search_resolutions("vacancia", "25")
        logger.info(f"Search test (vacancia, type 25): {len(results)} results")
        if results:
            r = results[0]
            logger.info(f"  First: {r.get('strPronunciamiento')}")
            text = r.get("strParteResolutiva", "")
            logger.info(f"  Text length: {len(text)} chars")

    # --- CLI entry point ---
    @staticmethod
    def cli():
        import argparse

        parser = argparse.ArgumentParser(description="PE/JNE bootstrap")
        parser.add_argument("command", choices=["bootstrap", "test-api"])
        parser.add_argument("--sample", action="store_true", help="Sample mode")
        parser.add_argument("--full", action="store_true", help="Full mode (ignored, default)")
        args = parser.parse_args()

        scraper = JNEScraper()

        if args.command == "test-api":
            scraper.test_api()
            return

        if args.command == "bootstrap":
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            if args.sample:
                records = scraper.fetch_sample(15)
            else:
                records = list(scraper.fetch_all())

            for i, record in enumerate(records):
                out_path = sample_dir / f"{i:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"Wrote {len(records)} records to {sample_dir}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    JNEScraper.cli()
