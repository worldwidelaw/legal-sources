#!/usr/bin/env python3
"""
MX/PoderJudicialJalisco -- Jalisco State Court Decisions

Fetches court decisions from the Supremo Tribunal de Justicia del Estado de
Jalisco via their public REST API. Uses AI-generated plain-text summaries
as the full text content.

API: https://publica-sentencias-backend.stjjalisco.gob.mx/
Frontend: https://publicacionsentencias.stjjalisco.gob.mx/

Strategy:
  - Paginate through /tocas?page={N} (15 records/page, ~5,500 pages)
  - For each decision, fetch the AI summary from /toca/{id}/file_resumen
  - Normalize into standard schema with full text

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialJalisco")

API_BASE = "https://publica-sentencias-backend.stjjalisco.gob.mx"


class JaliscoCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialJalisco court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _api_get(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """Make a GET request with retries."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _fetch_summary(self, toca_id: int) -> str:
        """Fetch the AI-generated plain-text summary for a decision."""
        url = f"{API_BASE}/toca/{toca_id}/file_resumen"
        resp = self._api_get(url, timeout=30)
        if resp is None:
            return ""
        text = resp.text.strip()
        return text if len(text) >= 50 else ""

    def _paginate_tocas(self, max_pages: int = 6000) -> Generator[Dict[str, Any], None, None]:
        """Paginate through all court decisions."""
        for page in range(1, max_pages + 1):
            url = f"{API_BASE}/tocas?page={page}"
            resp = self._api_get(url)
            if resp is None:
                logger.warning(f"Failed to fetch page {page}")
                break

            try:
                body = resp.json()
            except ValueError:
                logger.warning(f"Invalid JSON on page {page}")
                break

            tocas_data = body.get("data", {}).get("tocas", {})
            items = tocas_data.get("data", [])
            if not items:
                break

            if page == 1:
                total = tocas_data.get("total", 0)
                last_page = tocas_data.get("last_page", 0)
                logger.info(f"Total decisions: {total}, pages: {last_page}")

            for item in items:
                yield item

            last_page = tocas_data.get("last_page", 0)
            if page >= last_page:
                break

            if page % 100 == 0:
                logger.info(f"Progress: page {page}/{last_page}")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        toca_id = raw.get("id", 0)
        numero = raw.get("numero", "")
        periodo = raw.get("periodo", "")

        sala = raw.get("salas_data", {}) or {}
        sala_name = sala.get("nombre", "")

        magistrado = raw.get("magistrado_data", {}) or {}
        judge_parts = [
            magistrado.get("nombre", ""),
            magistrado.get("primer_apellido", ""),
            magistrado.get("segundo_apellido", ""),
        ]
        judge = " ".join(p for p in judge_parts if p).strip()

        materia = raw.get("materia_data", {}) or {}
        subject = materia.get("nombre", "")

        tipo_juicio = raw.get("tipo_juicio_data", {}) or {}
        case_type = tipo_juicio.get("nombre", "")

        sentido = raw.get("sentido_data", {}) or {}
        ruling = sentido.get("nombre", "")

        delito = raw.get("delito_data", {}) or {}
        crime = delito.get("nombre", "")

        fecha_emision = raw.get("fecha_emision", "")
        fecha_pub = raw.get("fecha_publicacion", "")
        date_str = fecha_emision[:10] if fecha_emision else ""
        pub_date = fecha_pub[:10] if fecha_pub else ""

        title = f"Toca {numero}/{periodo} — {sala_name}" if sala_name else f"Toca {numero}/{periodo}"
        case_number = f"{numero}/{periodo}" if numero and periodo else str(numero)

        pdf_url = f"{API_BASE}/toca/{toca_id}/file"

        return {
            "_id": f"MX-JAL-{toca_id}",
            "_source": "MX/PoderJudicialJalisco",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": date_str,
            "url": pdf_url,
            "court": sala_name,
            "judge": judge,
            "subject_area": subject,
            "case_type": case_type,
            "ruling": ruling,
            "case_number": case_number,
            "publication_date": pub_date,
            "crime_or_action": crime,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all court decisions with AI summaries."""
        count = 0
        skipped = 0

        for item in self._paginate_tocas():
            toca_id = item.get("id")
            if not toca_id:
                continue

            text = self._fetch_summary(toca_id)
            if not text:
                skipped += 1
                logger.debug(f"No summary for toca {toca_id}, skipping")
                continue

            item["_text"] = text
            count += 1
            yield item

            if count % 100 == 0:
                logger.info(f"Fetched {count} decisions ({skipped} skipped)")

        logger.info(f"Completed: {count} decisions fetched, {skipped} skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (first 10 pages)."""
        count = 0
        for page in range(1, 11):
            url = f"{API_BASE}/tocas?page={page}"
            resp = self._api_get(url)
            if resp is None:
                break

            try:
                body = resp.json()
            except ValueError:
                break

            items = body.get("data", {}).get("tocas", {}).get("data", [])
            if not items:
                break

            for item in items:
                toca_id = item.get("id")
                if not toca_id:
                    continue
                text = self._fetch_summary(toca_id)
                if not text:
                    continue
                item["_text"] = text
                count += 1
                yield item

        logger.info(f"Updates: {count} decisions fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        url = f"{API_BASE}/tocas?page=1"
        resp = self._api_get(url)
        if resp is None:
            logger.error("Cannot reach STJ Jalisco API")
            return False

        try:
            body = resp.json()
        except ValueError:
            logger.error("Invalid JSON response")
            return False

        tocas = body.get("data", {}).get("tocas", {})
        total = tocas.get("total", 0)
        items = tocas.get("data", [])
        logger.info(f"API OK: {total} total decisions, {len(items)} on page 1")

        if items:
            item = items[0]
            toca_id = item.get("id")
            logger.info(f"Sample: Toca {item.get('numero')}/{item.get('periodo')} (id={toca_id})")
            text = self._fetch_summary(toca_id)
            logger.info(f"Summary length: {len(text)} chars")
            return len(text) > 0

        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialJalisco data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = JaliscoCourtScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
