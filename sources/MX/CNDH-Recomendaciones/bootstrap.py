#!/usr/bin/env python3
"""
MX/CNDH-Recomendaciones -- Mexico CNDH Human Rights Recommendations

Fetches recommendations from the CNDH REST API with PDF full text extraction.

Strategy:
  - Query the pagination API for each category (Resoluciones, ResolucionesVG,
    ResolucionesGenerales, ResolucionesMNPT, AccionesInconstitucionalidad)
  - For each item, normalize the PDF URL based on category-specific rules
  - Download the PDF and extract text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Fetch all recommendations
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.CNDH-Recomendaciones")

API_BASE = "https://apiportal.cndh.org.mx/api"
SITE_BASE = "https://www.cndh.org.mx"

# Categories with their total counts and PDF URL normalization rules
CATEGORIES = [
    {"tipo": "Resoluciones", "label": "Recommendations"},
    {"tipo": "ResolucionesVG", "label": "Serious Violations"},
    {"tipo": "ResolucionesGenerales", "label": "General Recommendations"},
    {"tipo": "ResolucionesMNPT", "label": "Torture Prevention"},
    {"tipo": "AccionesInconstitucionalidad", "label": "Unconstitutionality Actions"},
]

PAGE_SIZE = 10


def _normalize_pdf_url(url_pdf: str, tipo: str) -> str:
    """Normalize PDF URL based on category-specific path rules."""
    if not url_pdf:
        return ""
    # AccionesInconstitucionalidad already has full URLs
    if url_pdf.startswith("http"):
        return url_pdf
    # Strip category-specific prefixes
    if tipo == "ResolucionesVG" and url_pdf.startswith("/Documentos"):
        url_pdf = url_pdf[len("/Documentos"):]
    elif tipo in ("ResolucionesGenerales", "ResolucionesMNPT") and url_pdf.startswith("/assets"):
        url_pdf = url_pdf[len("/assets"):]
    # Ensure leading slash
    if not url_pdf.startswith("/"):
        url_pdf = "/" + url_pdf
    return f"{SITE_BASE}{url_pdf}"


class CNDHScraper(BaseScraper):
    """Scraper for MX/CNDH-Recomendaciones."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
            "Origin": "https://www.cndh.org.mx",
            "Referer": "https://www.cndh.org.mx/",
        })

    def _api_request(self, url: str, timeout: int = 30) -> Optional[dict]:
        """Make an API request with retries."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _fetch_category(self, tipo: str, max_pages: int = 500) -> Generator[Dict[str, Any], None, None]:
        """Fetch all items from a category via pagination."""
        for page in range(1, max_pages + 1):
            url = f"{API_BASE}/Seccion/paginacion?Tipo={tipo}&Autoridad=&Buscar=&anio=&idPagina={page}&numPagina={PAGE_SIZE}"
            data = self._api_request(url)
            if not data:
                logger.warning(f"Failed to fetch {tipo} page {page}")
                break

            items = data.get("data", [])
            if not items:
                break

            total_pages = data.get("tamanoPagina", 0)
            if page == 1:
                total = data.get("totalRegistros", 0)
                logger.info(f"{tipo}: {total} items across {total_pages} pages")

            for item in items:
                item["_category"] = tipo
                yield item

            if page >= total_pages:
                break

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        tipo = raw.get("_category", "Resoluciones")
        title = raw.get("titulo", "") or raw.get("titulo4", "")
        number = raw.get("numeroConsecutivo", "")
        year = raw.get("fecha", "")

        doc_id = f"MX-CNDH-{tipo}-{year}-{number}" if number else f"MX-CNDH-{tipo}-{title[:50]}"
        pdf_url = _normalize_pdf_url(raw.get("urlPDF", ""), tipo)

        return {
            "_id": doc_id,
            "_source": "MX/CNDH-Recomendaciones",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": f"{year}-01-01" if year and year.isdigit() else "",
            "url": pdf_url,
            "category": tipo,
            "authority": raw.get("autoridad", ""),
            "description": raw.get("descripcion", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all recommendations across all categories."""
        count = 0
        for cat in CATEGORIES:
            tipo = cat["tipo"]
            label = cat["label"]
            logger.info(f"Fetching category: {label} ({tipo})")

            for item in self._fetch_category(tipo):
                title = item.get("titulo", "") or item.get("titulo4", "")
                number = item.get("numeroConsecutivo", "")
                year = item.get("fecha", "")
                doc_id = f"MX-CNDH-{tipo}-{year}-{number}" if number else f"MX-CNDH-{tipo}-{title[:50]}"

                pdf_url = _normalize_pdf_url(item.get("urlPDF", ""), tipo)
                text = ""
                if pdf_url:
                    try:
                        text = extract_pdf_markdown(
                            source="MX/CNDH-Recomendaciones",
                            source_id=doc_id,
                            pdf_url=pdf_url,
                            table="doctrine",
                        ) or ""
                    except Exception as e:
                        logger.warning(f"PDF extraction failed for {doc_id}: {e}")

                if not text or len(text) < 50:
                    logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
                    continue

                item["text"] = text
                count += 1
                yield item

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent recommendations (first 3 pages per category)."""
        count = 0
        for cat in CATEGORIES:
            tipo = cat["tipo"]
            for page in range(1, 4):
                url = f"{API_BASE}/Seccion/paginacion?Tipo={tipo}&Autoridad=&Buscar=&anio=&idPagina={page}&numPagina={PAGE_SIZE}"
                data = self._api_request(url)
                if not data:
                    break
                items = data.get("data", [])
                if not items:
                    break
                for item in items:
                    item["_category"] = tipo
                    number = item.get("numeroConsecutivo", "")
                    year = item.get("fecha", "")
                    title = item.get("titulo", "") or item.get("titulo4", "")
                    doc_id = f"MX-CNDH-{tipo}-{year}-{number}" if number else f"MX-CNDH-{tipo}-{title[:50]}"

                    pdf_url = _normalize_pdf_url(item.get("urlPDF", ""), tipo)
                    text = ""
                    if pdf_url:
                        try:
                            text = extract_pdf_markdown(
                                source="MX/CNDH-Recomendaciones",
                                source_id=doc_id,
                                pdf_url=pdf_url,
                                table="doctrine",
                            ) or ""
                        except Exception as e:
                            logger.warning(f"PDF extraction failed: {e}")

                    if not text or len(text) < 50:
                        continue

                    item["text"] = text
                    count += 1
                    yield item

        logger.info(f"Updates: {count} documents fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        url = f"{API_BASE}/Seccion/paginacion?Tipo=Resoluciones&Autoridad=&Buscar=&anio=&idPagina=1&numPagina=10"
        data = self._api_request(url)
        if not data:
            logger.error("Cannot reach CNDH API")
            return False

        items = data.get("data", [])
        total = data.get("totalRegistros", 0)
        logger.info(f"API OK: {total} total Resoluciones, {len(items)} on page 1")

        if items:
            item = items[0]
            pdf_url = _normalize_pdf_url(item.get("urlPDF", ""), "Resoluciones")
            logger.info(f"Sample: {item.get('titulo', '')} → {pdf_url}")
            return True
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/CNDH-Recomendaciones data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CNDHScraper()

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
