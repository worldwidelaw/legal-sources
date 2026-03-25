#!/usr/bin/env python3
"""
MX/SCJNDatosAbiertosAPI -- Mexico Supreme Court Open Data JSON API

Fetches court decisions from the SCJN (Suprema Corte de Justicia de la Nación)
open data platform via its public REST API.

Strategy:
  - Search via POST /api/v1/bj/busqueda with index and pagination
  - Fetch full text via GET /api/v1/bj/documento/{index}/{id}
  - Covers: sentencias_pub (103K), tesis (311K), ejecutorias (22K)

API:
  - Base: https://bj.scjn.gob.mx/api/v1/bj
  - Search: POST /busqueda {q, indice, page, size, filtros}
  - Detail: GET /documento/{index}/{id}
  - No auth required, no rate limits detected

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.scjn-api")

BASE_URL = "https://bj.scjn.gob.mx"
API_BASE = "/api/v1/bj"

# Indices to harvest (public, with full text available)
INDICES = [
    {"name": "sentencias_pub", "id_field": "idEngrose", "label": "Sentencias"},
    {"name": "tesis", "id_field": "registroDigital", "label": "Tesis"},
    {"name": "ejecutorias", "id_field": "registroDigital", "label": "Ejecutorias"},
]

# For sample mode, fetch from each index
SAMPLE_PER_INDEX = 4


class SCJNScraper(BaseScraper):
    """
    Scraper for MX/SCJNDatosAbiertosAPI -- Mexico Supreme Court JSON API.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=60,
        )

    def _search(self, index: str, page: int = 1, size: int = 50, q: str = "*") -> Optional[Dict]:
        """Search an index with pagination."""
        self.rate_limiter.wait()
        try:
            body = {
                "q": q,
                "indice": index,
                "page": page,
                "size": size,
                "filtros": {},
                "semantica": 0,
                "sortField": "",
                "sortDireccion": "",
            }
            resp = self.client.post(
                f"{API_BASE}/busqueda",
                json_data=body,
            )

            if resp.status_code != 200:
                logger.warning(f"Search {index} page {page}: HTTP {resp.status_code}")
                return None

            return resp.json()

        except Exception as e:
            logger.warning(f"Search error {index} page {page}: {e}")
            return None

    def _fetch_document(self, index: str, doc_id: str) -> Optional[Dict]:
        """Fetch full document detail by index and ID."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"{API_BASE}/documento/{index}/{doc_id}")

            if resp.status_code != 200:
                logger.debug(f"Document {index}/{doc_id}: HTTP {resp.status_code}")
                return None

            return resp.json()

        except Exception as e:
            logger.warning(f"Error fetching {index}/{doc_id}: {e}")
            return None

    def _extract_sentencia_text(self, doc: Dict) -> str:
        """Extract full text from a sentencia document."""
        parts = []
        for section in ["preambulo", "resultando", "considerando", "resuelve", "firman", "puntosResolutivos"]:
            content = doc.get(section, [])
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict):
                        text = item.get("parrafo", "")
                        if text:
                            parts.append(text)
                    elif isinstance(item, str):
                        parts.append(item)
            elif isinstance(content, str) and content:
                parts.append(content)
        return "\n\n".join(parts)

    def _extract_tesis_text(self, doc: Dict) -> str:
        """Extract full text from a tesis document."""
        texto = doc.get("texto", {})
        if isinstance(texto, dict):
            return texto.get("contenido", "")
        elif isinstance(texto, str):
            return texto
        return ""

    def _extract_ejecutoria_text(self, doc: Dict) -> str:
        """Extract full text from an ejecutoria document."""
        texto = doc.get("texto", "")
        if isinstance(texto, dict):
            return texto.get("contenido", texto.get("texto", ""))
        return str(texto) if texto else ""

    def _extract_text(self, index: str, doc: Dict) -> str:
        """Extract full text based on index type."""
        if index == "sentencias_pub":
            return self._extract_sentencia_text(doc)
        elif index == "tesis":
            return self._extract_tesis_text(doc)
        elif index == "ejecutorias":
            return self._extract_ejecutoria_text(doc)
        return ""

    def _extract_title(self, index: str, result: Dict, doc: Dict) -> str:
        """Extract document title from search result or detail."""
        if index == "sentencias_pub":
            return (
                result.get("asunto", "")
                or doc.get("asunto", "")
                or f"Sentencia {result.get('idEngrose', '')}"
            )
        elif index == "tesis":
            return (
                result.get("rubro", "")
                or doc.get("rubro", "")
                or f"Tesis {result.get('registroDigital', '')}"
            )
        elif index == "ejecutorias":
            return (
                result.get("rubro", "")
                or doc.get("rubro", "")
                or f"Ejecutoria {result.get('registroDigital', '')}"
            )
        return "Unknown"

    def _extract_date(self, result: Dict) -> Optional[str]:
        """Extract and format date from search result."""
        for field in ["fechaPublicacion", "fechaResolucion", "fecha", "fechaSentencia"]:
            val = result.get(field)
            if val:
                # Try common formats
                for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"]:
                    try:
                        dt = datetime.strptime(val[:19], fmt)
                        return dt.strftime("%Y-%m-%d")
                    except (ValueError, TypeError):
                        continue
                # Try epoch ms
                if isinstance(val, (int, float)):
                    try:
                        dt = datetime.fromtimestamp(val / 1000, tz=timezone.utc)
                        return dt.strftime("%Y-%m-%d")
                    except (ValueError, OSError):
                        pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all indices."""
        for idx_info in INDICES:
            index = idx_info["name"]
            id_field = idx_info["id_field"]
            label = idx_info["label"]

            # First search to get total
            first_page = self._search(index, page=1, size=50)
            if not first_page:
                logger.warning(f"Cannot access index {index}")
                continue

            total = first_page.get("total", 0)
            total_pages = first_page.get("totalPaginas", 0)
            logger.info(f"Index {index} ({label}): {total} documents, {total_pages} pages")

            page = 1
            fetched = 0

            while page <= total_pages:
                if page == 1:
                    results = first_page.get("resultados", [])
                else:
                    search_result = self._search(index, page=page, size=50)
                    if not search_result:
                        page += 1
                        continue
                    results = search_result.get("resultados", [])

                if not results:
                    break

                for result in results:
                    doc_id = str(result.get(id_field, ""))
                    if not doc_id:
                        continue

                    doc = self._fetch_document(index, doc_id)
                    if not doc:
                        continue

                    full_text = self._extract_text(index, doc)
                    if not full_text or len(full_text) < 50:
                        continue

                    title = self._extract_title(index, result, doc)
                    date_str = self._extract_date(result)

                    yield {
                        "index": index,
                        "doc_id": doc_id,
                        "title": title,
                        "full_text": full_text,
                        "date": date_str,
                        "result_meta": result,
                        "doc_meta": {k: v for k, v in doc.items()
                                     if k not in ("preambulo", "resultando", "considerando",
                                                   "resuelve", "firman", "puntosResolutivos",
                                                   "texto")},
                    }

                    fetched += 1
                    if fetched % 100 == 0:
                        logger.info(f"Index {index}: fetched {fetched}/{total}")

                page += 1

            logger.info(f"Index {index}: completed {fetched} documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent documents by filtering on year."""
        current_year = datetime.now().year
        for idx_info in INDICES:
            index = idx_info["name"]
            id_field = idx_info["id_field"]

            for year in [current_year, current_year - 1]:
                page = 1
                while True:
                    body = {
                        "q": "*",
                        "indice": index,
                        "page": page,
                        "size": 50,
                        "filtros": {"anio": [str(year)]},
                        "semantica": 0,
                        "sortField": "",
                        "sortDireccion": "",
                    }
                    self.rate_limiter.wait()
                    try:
                        resp = self.client.post(f"{API_BASE}/busqueda", json_data=body)
                        if resp.status_code != 200:
                            break
                        data = resp.json()
                        results = data.get("resultados", [])
                        if not results:
                            break
                    except Exception:
                        break

                    for result in results:
                        doc_id = str(result.get(id_field, ""))
                        if not doc_id:
                            continue
                        doc = self._fetch_document(index, doc_id)
                        if not doc:
                            continue
                        full_text = self._extract_text(index, doc)
                        if not full_text or len(full_text) < 50:
                            continue
                        title = self._extract_title(index, result, doc)
                        date_str = self._extract_date(result)
                        yield {
                            "index": index,
                            "doc_id": doc_id,
                            "title": title,
                            "full_text": full_text,
                            "date": date_str,
                            "result_meta": result,
                            "doc_meta": {},
                        }

                    if page >= data.get("totalPaginas", 0):
                        break
                    page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        index = raw.get("index", "")
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", f"Document {doc_id}")

        return {
            "_id": f"MX-SCJN-{index}-{doc_id}",
            "_source": "MX/SCJNDatosAbiertosAPI",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": raw.get("date"),
            "url": f"{BASE_URL}/datos-abiertos/documento/{index}/{doc_id}",
            "index_type": index,
            "court": "Suprema Corte de Justicia de la Nación",
            "jurisdiction": "MX",
            "language": "es",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Mexico SCJN Open Data API...")

        for idx_info in INDICES:
            index = idx_info["name"]
            id_field = idx_info["id_field"]
            label = idx_info["label"]

            print(f"\n--- {label} ({index}) ---")
            result = self._search(index, page=1, size=3)
            if not result:
                print(f"  FAILED: No response")
                continue

            total = result.get("total", 0)
            print(f"  Total: {total:,} documents")

            results = result.get("resultados", [])
            if results:
                first = results[0]
                doc_id = str(first.get(id_field, ""))
                print(f"  First doc ID: {doc_id}")

                doc = self._fetch_document(index, doc_id)
                if doc:
                    text = self._extract_text(index, doc)
                    print(f"  Full text: {len(text)} chars")
                    if text:
                        print(f"  Sample: {text[:200]}...")
                else:
                    print(f"  FAILED: Could not fetch document detail")

        print("\nTest complete!")


def main():
    scraper = SCJNScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
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
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
