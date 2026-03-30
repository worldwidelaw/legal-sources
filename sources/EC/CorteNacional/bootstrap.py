#!/usr/bin/env python3
"""
EC/CorteNacional -- Ecuador National Court of Justice Data Fetcher

Fetches case law (cassation and revision sentences) from Ecuador's
Corte Nacional de Justicia via the Función Judicial REST API.

Strategy:
  - Bootstrap: Paginates through all 8 salas (chambers), downloading
    each sentence's PDF for full text extraction via pdfminer.six.
  - Update: Filters by sala and sorts by date to find recent records.
  - Sample: Fetches 12+ records across multiple salas for validation.

API: https://api.funcionjudicial.gob.ec/BUSCADOR-SENTENCIAS-SERVICES/api/buscador-sentencias
Website: https://busquedasentencias.cortenacional.gob.ec

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Incremental update
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import io
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EC.cortenacional")

API_BASE = "https://api.funcionjudicial.gob.ec/BUSCADOR-SENTENCIAS-SERVICES/api/buscador-sentencias"
DOC_BASE = "https://api.funcionjudicial.gob.ec/CJ-DOCUMENTO-SERVICE/api/document/query/hba"

# Try multiple PDF backends (pypdf is in requirements.txt)
PDF_BACKEND = None
try:
    import pypdf
    PDF_BACKEND = "pypdf"
except ImportError:
    try:
        import PyPDF2
        PDF_BACKEND = "PyPDF2"
    except ImportError:
        try:
            from pdfminer.high_level import extract_text as pdfminer_extract
            PDF_BACKEND = "pdfminer"
        except ImportError:
            PDF_BACKEND = None
            logger.warning("No PDF backend available (need pypdf, PyPDF2, or pdfminer.six)")


def extract_pdf_text(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using available backend."""
    if not PDF_BACKEND:
        return None
    try:
        if PDF_BACKEND == "pypdf":
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
            parts = [page.extract_text() for page in reader.pages if page.extract_text()]
            text = "\n\n".join(parts)
        elif PDF_BACKEND == "PyPDF2":
            reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
            parts = [page.extract_text() for page in reader.pages if page.extract_text()]
            text = "\n\n".join(parts)
        elif PDF_BACKEND == "pdfminer":
            text = pdfminer_extract(io.BytesIO(pdf_bytes))
        else:
            return None
        if text:
            text = re.sub(r'\x00', '', text)
            text = re.sub(r'[\r\n]{3,}', '\n\n', text)
            text = text.strip()
        return text if text and len(text) > 100 else None
    except Exception as e:
        logger.warning(f"PDF extraction failed ({PDF_BACKEND}): {e}")
        return None


class CorteNacionalScraper(BaseScraper):
    """
    Scraper for EC/CorteNacional -- Ecuador National Court of Justice.
    Country: EC
    URL: https://busquedasentencias.cortenacional.gob.ec

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "curl/8.7.1",
                "Content-Type": "application/json",
                "X-reCAPTCHA-Token": "open-data-research",
            },
            timeout=60,
        )
        self.doc_client = HttpClient(
            base_url="",
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,
        )
        self.salas = []

    # -- API helpers --------------------------------------------------------

    def _load_salas(self):
        """Load the list of specialized chambers."""
        if self.salas:
            return
        import requests as req
        resp = req.get(
            f"{API_BASE}/query/sala/lista",
            headers={"User-Agent": "curl/8.7.1"},
            timeout=30,
        )
        resp.raise_for_status()
        self.salas = resp.json()
        logger.info(f"Loaded {len(self.salas)} salas")

    def _search_by_sala(self, sala_id: int, page: int = 0, page_size: int = 50) -> dict:
        """Search sentences by chamber (sala) with pagination.

        Uses requests directly (not HttpClient) because the API's reCAPTCHA
        validation intermittently returns 500, which HttpClient's retry logic
        treats as retryable, leading to max retry errors.
        """
        import requests as req
        body = json.dumps({
            "salaId": sala_id,
            "orden": "SCORE",
            "pageNumber": page,
            "pageSize": page_size,
        })
        url = f"{API_BASE}/query/sentencia/busqueda/busquedaPorFiltros"
        for attempt in range(3):
            resp = req.post(
                url,
                data=body,
                headers={
                    "Content-Type": "application/json",
                    "X-reCAPTCHA-Token": "open-data-research",
                    "User-Agent": "curl/8.7.1",
                },
                timeout=60,
            )
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"API returned {resp.status_code} for sala {sala_id} page {page} (attempt {attempt+1})")
            time.sleep(2 * (attempt + 1))
        resp.raise_for_status()
        return resp.json()

    def _download_pdf_text(self, url: str) -> Optional[str]:
        """Download PDF and extract text."""
        if not url:
            return None
        try:
            resp = self.doc_client.get(url)
            if resp.status_code == 200 and len(resp.content) > 500:
                return extract_pdf_text(resp.content)
        except Exception as e:
            logger.warning(f"PDF download failed for {url}: {e}")
        return None

    # -- Normalization --------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """Transform raw API record into standard schema."""
        record_id = raw.get("id", "")
        resolution = (raw.get("numeroResolucion") or "").strip()
        providencia = (raw.get("nombreProvidencia") or "").strip()
        title = f"{resolution} - {providencia}" if resolution else providencia or f"Sentencia {record_id}"

        # Parse date
        date_str = raw.get("fechaProvidencia")
        date = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("+00:00", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                date = None

        # Clean text from resumen (search excerpts) - strip HTML tags
        resumen = raw.get("resumen")
        resumen_text = ""
        if resumen and isinstance(resumen, list):
            parts = []
            for r in resumen:
                clean = re.sub(r'<[^>]+>', '', r)
                clean = unescape(clean)
                clean = re.sub(r'\xa0', ' ', clean)
                clean = re.sub(r'\s+', ' ', clean).strip()
                parts.append(clean)
            resumen_text = "\n\n".join(parts)

        # Full text from PDF (set by caller) or resumen as fallback
        text = raw.get("_full_text") or resumen_text

        pdf_url = raw.get("urlPdf") or ""

        return {
            "_id": f"EC-CNJ-{record_id}",
            "_source": "EC/CorteNacional",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "sala": (raw.get("nombreSala") or "").strip(),
            "juez_ponente": (raw.get("juezPonente") or "").strip(),
            "materia": (raw.get("nombreMateria") or "").strip(),
            "numero_proceso": (raw.get("numeroProceso") or "").strip(),
            "numero_resolucion": resolution,
            "estado_proceso": (raw.get("nombreEstadoProceso") or "").strip(),
            "fecha_concluye": raw.get("fechaConcluye"),
        }

    # -- Fetchers --------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all sentences with full text from PDFs."""
        total_fetched = 0
        page = 0

        while True:
            try:
                data = self._search_by_sala(1, page=page, page_size=50)
            except Exception as e:
                logger.error(f"API error page {page}: {e}")
                break

            content = data.get("content", [])
            if not content:
                break

            for record in content:
                pdf_url = record.get("urlPdf")
                full_text = self._download_pdf_text(pdf_url)
                if full_text:
                    record["_full_text"] = full_text

                normalized = self.normalize(record)
                if normalized.get("text"):
                    yield normalized
                    total_fetched += 1

                time.sleep(1)  # Rate limiting for PDF downloads

            total_pages = data.get("totalPages", 0)
            logger.info(f"Page {page+1}/{total_pages}, fetched {len(content)} records (total: {total_fetched})")

            if data.get("last", True):
                break

            page += 1
            time.sleep(0.5)

        logger.info(f"Total fetched: {total_fetched} sentences")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recently added sentences."""
        page = 0
        max_pages = 20  # Check up to 20 pages for recent records

        while page < max_pages:
            try:
                data = self._search_by_sala(1, page=page, page_size=50)
            except Exception as e:
                logger.error(f"API error page {page}: {e}")
                break

            content = data.get("content", [])
            if not content:
                break

            found_old = False
            for record in content:
                date_str = record.get("fechaProvidencia", "")
                if date_str and date_str[:10] < since:
                    found_old = True
                    continue

                pdf_url = record.get("urlPdf")
                full_text = self._download_pdf_text(pdf_url)
                if full_text:
                    record["_full_text"] = full_text

                normalized = self.normalize(record)
                if normalized.get("text"):
                    yield normalized

                time.sleep(1)

            if found_old or data.get("last", True):
                break
            page += 1
            time.sleep(0.5)

    def fetch_sample(self) -> list:
        """Fetch sample records across different pages for validation."""
        samples = []
        seen_ids = set()

        # Sample from different pages to get variety across salas
        pages_to_sample = [0, 1, 5, 50, 100, 200]

        for page in pages_to_sample:
            if len(samples) >= 12:
                break

            logger.info(f"Sampling page {page}...")
            try:
                data = self._search_by_sala(1, page=page, page_size=3)
            except Exception as e:
                logger.error(f"API error for page {page}: {e}")
                continue

            content = data.get("content", [])
            total = data.get("totalElements", 0)
            if page == 0 or not hasattr(self, '_total_logged'):
                logger.info(f"  Total records in database: {total}")
                self._total_logged = True

            for record in content:
                record_id = record.get("id")
                if record_id in seen_ids:
                    continue
                seen_ids.add(record_id)

                pdf_url = record.get("urlPdf")
                logger.info(f"  Downloading PDF for record {record_id}: {record.get('numeroResolucion')}")
                full_text = self._download_pdf_text(pdf_url)
                if full_text:
                    record["_full_text"] = full_text
                    logger.info(f"  Extracted {len(full_text)} chars of text")
                else:
                    logger.warning(f"  No text extracted from PDF")

                normalized = self.normalize(record)
                samples.append(normalized)
                time.sleep(1.5)  # Rate limiting

        return samples

    # -- CLI --------------------------------------------------------

    def test_api(self):
        """Quick API connectivity test."""
        print("Testing EC/CorteNacional API...")

        # Test salas endpoint
        resp = self.client.get("/query/sala/lista")
        salas = resp.json()
        print(f"  Salas: {len(salas)} chambers found")
        for s in salas:
            print(f"    - [{s['id']}] {s['nombre'][:80]}")

        # Test search endpoint
        data = self._search_by_sala(1, page=0, page_size=1)
        total = data.get("totalElements", 0)
        print(f"\n  Sala 1 total records: {total}")

        content = data.get("content", [])
        if content:
            rec = content[0]
            print(f"  Sample: {rec.get('numeroResolucion')} - {rec.get('nombreProvidencia', '')[:50]}")
            print(f"  Date: {rec.get('fechaProvidencia', '')[:10]}")
            print(f"  PDF URL: {rec.get('urlPdf', '')[:80]}...")

        # Test PDF download
        if content and content[0].get("urlPdf"):
            print(f"\n  Testing PDF download...")
            text = self._download_pdf_text(content[0]["urlPdf"])
            if text:
                print(f"  PDF text extracted: {len(text)} chars")
                print(f"  First 200 chars: {text[:200]}")
            else:
                print("  PDF text extraction FAILED")

        print("\nAPI test complete.")


def main():
    scraper = CorteNacionalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample:
            logger.info("Fetching sample records...")
            samples = scraper.fetch_sample()

            # Save samples
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            for i, record in enumerate(samples):
                path = sample_dir / f"sample_{i:03d}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"Saved {len(samples)} sample records to {sample_dir}")

            # Validation
            texts = [r for r in samples if r.get("text") and len(r["text"]) > 100]
            print(f"\nValidation: {len(texts)}/{len(samples)} records have full text")
            for r in samples:
                text_len = len(r.get("text", ""))
                print(f"  {r['_id']}: {r['title'][:60]} | text: {text_len} chars")
        else:
            logger.info("Starting full bootstrap...")
            count = 0
            output_dir = Path(__file__).parent / "data"
            output_dir.mkdir(exist_ok=True)

            for record in scraper.fetch_all():
                path = output_dir / f"{record['_id']}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 100 == 0:
                    logger.info(f"  Saved {count} records...")

            logger.info(f"Bootstrap complete: {count} records saved")

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
