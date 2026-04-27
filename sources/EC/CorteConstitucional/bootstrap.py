#!/usr/bin/env python3
"""
EC/CorteConstitucional -- Ecuador Constitutional Court Data Fetcher

Fetches case law (sentencias, dictámenes) from Ecuador's Corte
Constitucional via the SACC REST API.

Strategy:
  - Bootstrap: Paginates through all decisions using the search endpoint.
    The API returns the "motivo" field with full decision reasoning text.
    PDF documents are also available via HDFS storage for additional text.
  - Update: Searches with date range to find recent records.
  - Sample: Fetches records across different pages for validation.

API: https://buscador.corteconstitucional.gob.ec/buscador-externo/rest/api/
Website: https://buscador.corteconstitucional.gob.ec

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import time
import base64
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EC.corteconstitucional")

API_BASE = "https://buscador.corteconstitucional.gob.ec/buscador-externo/rest/api"
STORAGE_BASE = "https://esacc.corteconstitucional.gob.ec/storage/api/v1/10_DWL_FL/"

# Default search criteria (from the Angular app's JS bundle)
DEFAULT_CRITERIA = {
    "numSentencia": "",
    "numeroCausa": "",
    "textoSentencia": "",
    "motivo": "",
    "metadata": "",
    "subBusqueda": "",
    "tipoLegitimado": 0,
    "legitimados": "",
    "tipoAcciones": [],
    "materias": [],
    "intereses": [],
    "decisiones": [],
    "jueces": [],
    "derechoDemandado": [],
    "derechosTratado": [],
    "derechosVulnerado": [],
    "temaEspecificos": [],
    "conceptos": [],
    "fechaNotificacion": "",
    "fechaDecision": "",
    "sort": "",
    "precedenteAprobado": "",
    "precedentePropuesto": "",
    "tipoNormas": [],
    "asuntos": [],
    "analisisMerito": "",
    "novedad": "",
    "merito": "",
    "paginacion": {"page": 1, "pageSize": 50, "total": 0, "contar": True},
}


def encode_params(params: dict) -> str:
    """Encode parameters using the SACC API encoding scheme.

    The API expects: JSON.stringify({dato: btoa(encodeURIComponent(JSON.stringify(params)))})
    """
    json_str = json.dumps(params)
    url_encoded = urllib.parse.quote(json_str)
    b64_encoded = base64.b64encode(url_encoded.encode()).decode()
    return json.dumps({"dato": b64_encoded})


def build_pdf_url(doc: dict) -> str:
    """Build the HDFS storage URL for a document."""
    if not doc:
        return ""
    carpeta = doc.get("carpeta", "tramite")
    uuid = doc.get("uuid", "")
    if not uuid:
        return ""
    param = base64.b64encode(
        json.dumps({"carpeta": carpeta, "uuid": uuid}).encode()
    ).decode()
    return f"{STORAGE_BASE}{param}"


class CorteConstitucionalScraper(BaseScraper):
    """
    Scraper for EC/CorteConstitucional -- Ecuador Constitutional Court.
    Country: EC
    URL: https://buscador.corteconstitucional.gob.ec

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://buscador.corteconstitucional.gob.ec",
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        }

    def _search(self, criteria: dict) -> dict:
        """Execute a search against the sentencia endpoint."""
        import requests

        url = f"{API_BASE}/sentencia/100_BUSCR_SNTNCIA"
        body = encode_params(criteria)

        for attempt in range(3):
            try:
                resp = requests.post(
                    url, data=body, headers=self.headers, timeout=60
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("tipoMensaje") == "exito":
                        return data
                    logger.warning(f"API warning: {data.get('mensaje')}")
                    return data
                logger.warning(
                    f"API returned {resp.status_code} (attempt {attempt + 1})"
                )
            except Exception as e:
                logger.warning(f"Request error (attempt {attempt + 1}): {e}")
            time.sleep(2 * (attempt + 1))

        return {"dato": [], "totalFilas": 0}

    def _get_catalog(self) -> dict:
        """Get catalog data (materias, acciones, etc.)."""
        import requests

        url = f"{API_BASE}/catalogoSentencia/100_OBT_RSMN_CTLG"
        body = encode_params({})
        resp = requests.post(url, data=body, headers=self.headers, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("dato", {})
        return {}

    # -- Normalization --------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """Transform raw API record into standard schema."""
        resolucion = raw.get("resolucion", {})
        record_id = resolucion.get("id", "")
        numero = resolucion.get("numero", "")

        # Parse date from epoch milliseconds
        fecha_ms = resolucion.get("fechadecision")
        date = None
        if fecha_ms:
            try:
                dt = datetime.fromtimestamp(fecha_ms / 1000, tz=timezone.utc)
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, OSError, TypeError):
                date = None

        # Full text from motivo field
        text = (resolucion.get("motivo") or "").strip()

        # Build PDF URL from first document
        docs = resolucion.get("documento", [])
        pdf_url = ""
        if docs:
            pdf_url = build_pdf_url(docs[0])

        # Extract metadata
        causas = resolucion.get("causa", [])
        tipo_accion = ""
        numero_causa = ""
        if causas:
            tipo_accion = causas[0].get("tipoaccion", "")
            numero_causa = causas[0].get("numero", "")

        materias = resolucion.get("materia", [])
        materia = materias[0].get("nombre", "") if materias else ""

        jueces = resolucion.get("juez", [])
        juez = jueces[0].get("nombrecompleto", "") if jueces else ""

        decisiones = resolucion.get("decision", [])
        decision = decisiones[0].get("resumendecision", "") if decisiones else ""

        title = f"Sentencia {numero}" if numero else f"Sentencia CC-{record_id}"

        return {
            "_id": f"EC-CC-{record_id}",
            "_source": "EC/CorteConstitucional",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "numero": numero,
            "numero_causa": numero_causa,
            "tipo_accion": tipo_accion,
            "materia": materia,
            "juez": juez,
            "decision": decision,
            "metadata_sentencia": (
                resolucion.get("metadatasentencia") or ""
            ).strip(),
            "es_novedad": resolucion.get("esnovedad", False),
            "es_relevante": resolucion.get("esrelevante", False),
        }

    # -- Fetchers --------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw API records for all decisions.

        Yields raw API records (not normalized) so BaseScraper.bootstrap()
        can call normalize() exactly once per record.
        """
        total_fetched = 0
        page = 1

        # Use wildcard search to get all results
        criteria = dict(DEFAULT_CRITERIA)
        criteria["textoSentencia"] = "*"
        criteria["subBusqueda"] = "*"
        criteria["sort"] = "desc"
        criteria["paginacion"] = {
            "page": page,
            "pageSize": 20,
            "total": 0,
            "contar": True,
        }

        while True:
            criteria["paginacion"]["page"] = page

            data = self._search(criteria)
            records = data.get("dato") or []
            if not records:
                break

            total = data.get("totalFilas", 0)
            if page == 1:
                logger.info(f"Total records available: {total}")
                criteria["paginacion"]["total"] = total

            for record in records:
                # Quick check that the raw record has motivo text
                resolucion = record.get("resolucion", {})
                if resolucion.get("motivo"):
                    yield record
                    total_fetched += 1

            logger.info(
                f"Page {page}, fetched {len(records)} records (total: {total_fetched})"
            )

            # API always returns 20 records per page regardless of pageSize
            if len(records) < 20:
                break

            page += 1
            time.sleep(2)

        logger.info(f"Total fetched: {total_fetched} decisions")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recently added decisions."""
        page = 1
        max_pages = 20

        criteria = dict(DEFAULT_CRITERIA)
        criteria["textoSentencia"] = "*"
        criteria["subBusqueda"] = "*"
        criteria["sort"] = "desc"
        criteria["paginacion"] = {
            "page": page,
            "pageSize": 50,
            "total": 0,
            "contar": True,
        }

        while page <= max_pages:
            criteria["paginacion"]["page"] = page

            data = self._search(criteria)
            records = data.get("dato") or []
            if not records:
                break

            found_old = False
            for record in records:
                normalized = self.normalize(record)
                if normalized.get("date") and normalized["date"] < since:
                    found_old = True
                    continue
                if normalized.get("text"):
                    yield normalized

            if found_old or len(records) < 50:
                break
            page += 1
            time.sleep(2)

    def fetch_sample(self) -> list:
        """Fetch sample records across different pages for validation."""
        samples = []
        seen_ids = set()

        # Get total count first
        criteria = dict(DEFAULT_CRITERIA)
        criteria["textoSentencia"] = "*"
        criteria["subBusqueda"] = "*"
        criteria["sort"] = "relevancia"
        criteria["paginacion"] = {
            "page": 1,
            "pageSize": 3,
            "total": 0,
            "contar": True,
        }

        data = self._search(criteria)
        total = data.get("totalFilas", 0)
        logger.info(f"Total records in database: {total}")

        # Sample from different pages
        pages_to_sample = [1, 2, 10, 50, 100]

        for page in pages_to_sample:
            if len(samples) >= 15:
                break

            logger.info(f"Sampling page {page}...")
            criteria["paginacion"]["page"] = page
            criteria["paginacion"]["pageSize"] = 3

            data = self._search(criteria)
            records = data.get("dato") or []

            for record in records:
                resolucion = record.get("resolucion", {})
                record_id = resolucion.get("id")
                if record_id in seen_ids:
                    continue
                seen_ids.add(record_id)

                normalized = self.normalize(record)
                text_len = len(normalized.get("text", ""))
                logger.info(
                    f"  {normalized['_id']}: {normalized['title'][:60]} | "
                    f"text: {text_len} chars"
                )
                samples.append(normalized)

            time.sleep(2)

        return samples

    # -- CLI --------------------------------------------------------

    def test_api(self):
        """Quick API connectivity test."""
        import requests

        print("Testing EC/CorteConstitucional API...")

        # Test catalog endpoint
        catalog = self._get_catalog()
        materias = catalog.get("materias", [])
        print(f"  Materias: {len(materias)} subject areas")
        for m in materias[:5]:
            print(f"    - {m['nombre']}")

        # Test search
        criteria = dict(DEFAULT_CRITERIA)
        criteria["textoSentencia"] = "*"
        criteria["subBusqueda"] = "*"
        criteria["paginacion"]["pageSize"] = 1
        criteria["paginacion"]["contar"] = True

        data = self._search(criteria)
        total = data.get("totalFilas", 0)
        print(f"\n  Total decisions: {total}")

        records = data.get("dato") or []
        if records:
            rec = records[0].get("resolucion", {})
            print(f"  Sample: {rec.get('numero', 'N/A')}")
            fecha_ms = rec.get("fechadecision")
            if fecha_ms:
                dt = datetime.fromtimestamp(fecha_ms / 1000, tz=timezone.utc)
                print(f"  Date: {dt.strftime('%Y-%m-%d')}")
            motivo = rec.get("motivo", "")
            print(f"  Motivo length: {len(motivo)} chars")
            print(f"  First 200 chars: {motivo[:200]}")

            docs = rec.get("documento", [])
            if docs:
                url = build_pdf_url(docs[0])
                print(f"\n  PDF URL: {url[:100]}...")
                resp = requests.get(
                    url, timeout=20, headers={"User-Agent": "LegalDataHunter/1.0"}
                )
                print(f"  PDF Status: {resp.status_code}")
                print(f"  PDF Size: {len(resp.content)} bytes")

        print("\nAPI test complete.")


def main():
    scraper = CorteConstitucionalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 else "2026-01-01"
        logger.info(f"Fetching updates since {since}...")
        count = 0
        output_dir = Path(__file__).parent / "data"
        output_dir.mkdir(exist_ok=True)

        for record in scraper.fetch_updates(since):
            path = output_dir / f"{record['_id']}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Update complete: {count} new/updated records")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
