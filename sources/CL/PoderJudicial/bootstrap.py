#!/usr/bin/env python3
"""
CL/PoderJudicial -- Chilean Judiciary Case Law Fetcher (juris.pjud.cl)

Fetches Chilean case law with full text from the Poder Judicial search API.

Strategy:
  - Obtain CSRF token from search page
  - POST to /busqueda/buscar_sentencias with court ID and pagination
  - Full text in texto_sentencia field (HTML cleaned to plain text)
  - 2-second delay between requests

Usage:
  python bootstrap.py bootstrap          # Fetch all case law
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.PoderJudicial")

BASE_URL = "https://juris.pjud.cl"

# Court configurations: (name, id_buscador, search_page_path)
COURTS = [
    ("Corte Suprema", 528, "Corte_Suprema"),
    ("Corte de Apelaciones", 168, "Corte_de_Apelaciones"),
    ("Civiles", 328, "Civiles"),
    ("Penales", 268, "Penales"),
    ("Familia", 270, "Familia"),
    ("Laborales", 271, "Laborales"),
    ("Cobranza", 269, "Cobranza"),
]


class PoderJudicialScraper(BaseScraper):
    """Scraper for CL/PoderJudicial -- Chilean case law via juris.pjud.cl."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "es-CL,es;q=0.9,en;q=0.5",
            "X-Requested-With": "XMLHttpRequest",
        })
        self._csrf_token = None

    def _get_csrf_token(self, court_page: str = "Corte_Suprema") -> Optional[str]:
        """Fetch CSRF token from a search page."""
        url = f"{BASE_URL}/busqueda?{court_page}"
        try:
            time.sleep(2)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            # Extract CSRF token from meta tag
            m = re.search(r'<meta\s+name="csrf-token"\s+content="([^"]+)"', resp.text)
            if m:
                token = m.group(1)
                self._csrf_token = token
                self.session.headers["X-CSRF-TOKEN"] = token
                logger.info(f"CSRF token obtained from {court_page}")
                return token
            logger.error("CSRF token not found in page")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get CSRF token: {e}")
            return None

    def _search(self, court_id: int, offset: int = 0, rows: int = 20,
                filtros: dict = None) -> Optional[Dict]:
        """Search for sentencias via the JSON API."""
        if not self._csrf_token:
            if not self._get_csrf_token():
                return None

        url = f"{BASE_URL}/busqueda/buscar_sentencias"
        data = {
            "_token": self._csrf_token,
            "id_buscador": court_id,
            "filtros": json.dumps(filtros or {}),
            "numero_filas_paginacion": rows,
            "offset_paginacion": offset,
            "orden": "relevancia",
            "personalizacion": "",
        }

        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.post(url, data=data, timeout=60)
                if resp.status_code == 419:
                    # CSRF token expired, refresh
                    logger.warning("CSRF token expired, refreshing")
                    self._get_csrf_token()
                    data["_token"] = self._csrf_token
                    continue
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"Search attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _clean_html(self, html_text: str) -> str:
        """Clean HTML tags from text content."""
        if not html_text:
            return ""
        soup = BeautifulSoup(html_text, "html.parser")
        text = soup.get_text(separator="\n", strip=True)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()

    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO format."""
        if not date_str:
            return ""
        # Try ISO format first
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        if m:
            return m.group(1)
        # Try DD/MM/YYYY
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_str)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        # Try YYYY-MM-DDThh:mm:ss
        m = re.match(r"(\d{4}-\d{2}-\d{2})T", date_str)
        if m:
            return m.group(1)
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = raw.get("id", "")
        if not doc_id:
            doc_id = f"CL-{raw.get('rol', 'unknown')}-{raw.get('court', 'unknown')}"

        # Build title from ROL and court
        rol = raw.get("rol", "")
        court = raw.get("court", "")
        title = f"{rol} - {court}" if rol else raw.get("title", "")

        return {
            "_id": f"CL-{doc_id}",
            "_source": "CL/PoderJudicial",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "court": court,
            "url": raw.get("url", ""),
        }

    def _extract_record(self, doc: Dict, court_name: str) -> Optional[Dict]:
        """Extract a normalized record from a Solr document."""
        # Get full text from texto_sentencia
        text_html = doc.get("texto_sentencia", "")
        text = self._clean_html(text_html)
        if not text or len(text) < 100:
            return None

        # Extract metadata
        doc_id = doc.get("id", "")
        # ROL number varies by court type
        rol = (doc.get("rol_era_sup_s") or doc.get("rol_era_ape_s")
               or doc.get("rol_corte_i", ""))
        caratulado = doc.get("caratulado_s", "")
        # Date field varies by court
        fecha = (doc.get("fec_sentencia_sup_dt") or doc.get("fec_sentencia_dt")
                 or doc.get("fec_sentencia_ape_dt", ""))
        recurso = doc.get("gls_tip_recurso_sup_s", "")
        url = doc.get("url_corta_acceso_sentencia") or doc.get("url_acceso_sentencia", "")

        title_parts = []
        if rol:
            title_parts.append(f"Rol {rol}")
        if caratulado:
            title_parts.append(caratulado[:120])
        if recurso:
            title_parts.append(recurso)
        title = " - ".join(title_parts) if title_parts else f"{court_name} #{doc_id}"

        raw = {
            "id": doc_id,
            "rol": rol,
            "title": title,
            "text": text,
            "date": self._parse_date(str(fecha)) if fecha else "",
            "court": court_name,
            "url": url or f"{BASE_URL}/busqueda?{court_name.replace(' ', '_')}",
        }
        return self.normalize(raw)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch case law from all courts."""
        for court_name, court_id, court_page in COURTS:
            logger.info(f"Starting court: {court_name} (id={court_id})")
            # Refresh CSRF for each court
            self._get_csrf_token(court_page)

            offset = 0
            rows = 20
            total = None

            while True:
                result = self._search(court_id, offset=offset, rows=rows)
                if not result or "response" not in result:
                    logger.warning(f"No response for {court_name} at offset {offset}")
                    break

                response = result["response"]
                if total is None:
                    total = response.get("numFound", 0)
                    logger.info(f"{court_name}: {total:,} total decisions")

                docs = response.get("docs", [])
                if not docs:
                    break

                for doc in docs:
                    record = self._extract_record(doc, court_name)
                    if record:
                        yield record

                offset += rows
                if offset >= total:
                    break

            logger.info(f"Completed {court_name}")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (first page of each court)."""
        for court_name, court_id, court_page in COURTS:
            self._get_csrf_token(court_page)
            result = self._search(court_id, offset=0, rows=20)
            if not result or "response" not in result:
                continue
            for doc in result["response"].get("docs", []):
                record = self._extract_record(doc, court_name)
                if record:
                    yield record

    def test(self) -> bool:
        """Quick connectivity test."""
        token = self._get_csrf_token()
        if not token:
            logger.error("Cannot obtain CSRF token")
            return False

        result = self._search(528, offset=0, rows=5)  # Corte Suprema
        if not result or "response" not in result:
            logger.error("Search API failed")
            return False

        total = result["response"].get("numFound", 0)
        docs = result["response"].get("docs", [])
        logger.info(f"Corte Suprema: {total:,} total, got {len(docs)} docs")

        if docs:
            doc = docs[0]
            text = self._clean_html(doc.get("texto_sentencia", ""))
            logger.info(f"Sample doc: {doc.get('rol', '?')} ({len(text)} chars text)")
            if text and len(text) > 100:
                logger.info("Full text extraction OK")
                return True

        logger.warning("Could not verify full text extraction")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/PoderJudicial data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = PoderJudicialScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all():
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
