#!/usr/bin/env python3
"""
CL/TribunalConstitucional -- Chilean Constitutional Court Case Law Fetcher

Fetches constitutional decisions with full text from the TC REST API at
buscador-backend.tcchile.cl.

Strategy:
  1. Enumerate all fichas (case metadata) via /buscadorexterno/ficha
  2. For each ficha, fetch full text via /extended/sentenciaByID
  3. Fallback: search by common terms to find OCR text

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import urllib.parse
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
logger = logging.getLogger("legal-data-hunter.CL.TribunalConstitucional")

API_BASE = "https://buscador-backend.tcchile.cl/api"


class TribunalConstitucionalScraper(BaseScraper):
    """Scraper for CL/TribunalConstitucional -- Chilean constitutional case law."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _api_get(self, endpoint: str, filter_params: dict) -> Optional[Dict]:
        """Make a GET request to the TC API with filter params."""
        filter_json = json.dumps(filter_params, separators=(",", ":"))
        url = f"{API_BASE}/{endpoint}?filter={urllib.parse.quote(filter_json)}"

        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 500:
                    logger.warning(f"Server error for {endpoint}, attempt {attempt+1}")
                    time.sleep(5)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"API request failed (attempt {attempt+1}): {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _fetch_fichas(self, page: int = 1, limit: int = 50) -> Optional[Dict]:
        """Fetch case metadata (fichas) listing."""
        return self._api_get("buscadorexterno/ficha", {"page": page, "limit": limit})

    def _fetch_sentencia_by_id(self, sentencia_id: int, search: str = "de") -> Optional[Dict]:
        """Fetch full text of a single sentencia by ID."""
        return self._api_get("extended/sentenciaByID", {"id": sentencia_id, "search": search})

    def _search_sentencias(self, search: str, page: int = 1, limit: int = 50) -> Optional[Dict]:
        """Search sentencias by keyword (returns full text)."""
        return self._api_get("extended/sentencias", {"search": search, "page": page, "limit": limit})

    def _extract_ficha_metadata(self, ficha: Dict) -> Dict:
        """Extract metadata from a ficha record."""
        detalle = ficha.get("detalle", [])
        meta = {}
        for d in detalle:
            param_id = d.get("param_id")
            valor = d.get("valor", "")
            if param_id == 38:  # caratula
                meta["caratula"] = valor
            elif param_id == 46:  # norma
                meta["norma"] = valor
            elif param_id == 50:  # extracto
                meta["extracto"] = valor
        return meta

    def _parse_date(self, date_str: str) -> str:
        """Parse date to ISO format."""
        if not date_str:
            return ""
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        if m:
            return m.group(1)
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_str)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        m = re.match(r"(\d{2})-(\d{2})-(\d{4})", date_str)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return ""

    def _clean_text(self, text: str) -> str:
        """Clean OCR text content."""
        if not text:
            return ""
        # Remove excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": f"CL-TC-{raw.get('id', '')}",
            "_source": "CL/TribunalConstitucional",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "court": "Tribunal Constitucional de Chile",
            "url": raw.get("url", ""),
            "rol": raw.get("rol", ""),
            "competencia": raw.get("competencia", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions using search-based approach for full text."""
        # Use common legal terms to cover the corpus
        search_terms = [
            "inaplicabilidad", "inconstitucionalidad", "requerimiento",
            "constitucional", "recurso", "derecho", "ley", "tribunal",
            "sentencia", "articulo",
        ]

        seen_ids = set()

        for term in search_terms:
            logger.info(f"Searching for term: '{term}'")
            page = 1
            total = None

            while True:
                result = self._search_sentencias(term, page=page, limit=50)
                if not result:
                    break

                data = result.get("data", {})
                if total is None:
                    meta = result.get("meta", {})
                    total = meta.get("total", 0)
                    logger.info(f"Term '{term}': {total:,} results")

                results_list = data.get("results", [])
                if not results_list:
                    break

                for doc in results_list:
                    doc_id = doc.get("sentence_id") or doc.get("id", "")
                    if doc_id in seen_ids:
                        continue
                    seen_ids.add(doc_id)

                    text = self._clean_text(doc.get("content", ""))
                    if not text or len(text) < 100:
                        continue

                    rol = doc.get("rol", "")
                    competencia = doc.get("competencia", "") or doc.get("competenciaShortName", "")
                    title_parts = []
                    if rol:
                        title_parts.append(f"Rol {rol}")
                    if competencia:
                        title_parts.append(competencia)
                    title = " - ".join(title_parts) if title_parts else f"TC #{doc_id}"

                    raw = {
                        "id": doc_id,
                        "title": title,
                        "text": text,
                        "date": "",  # Not in search results
                        "url": f"https://buscador.tcchile.cl/#/sentencia/{doc_id}",
                        "rol": rol,
                        "competencia": competencia,
                    }
                    yield self.normalize(raw)

                page += 1
                if total and (page - 1) * 50 >= total:
                    break

        logger.info(f"Fetch complete: {len(seen_ids)} unique decisions")

    def fetch_with_fichas(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch using ficha enumeration + sentenciaByID for full text."""
        page = 1
        total = None
        count = 0

        while True:
            result = self._fetch_fichas(page=page, limit=50)
            if not result:
                break

            data = result.get("data", [])
            if not data:
                break

            if total is None:
                total = result.get("total", len(data))
                logger.info(f"Total fichas: {total:,}")

            for ficha in data:
                folio = ficha.get("folio", "")
                ficha_id = ficha.get("id", "")
                fecha = ficha.get("fecha_sentencia", "")
                nombre = ficha.get("nombre", "")
                meta = self._extract_ficha_metadata(ficha)

                # Try to get full text
                sentencia = self._fetch_sentencia_by_id(ficha_id)
                text = ""
                if sentencia:
                    s_data = sentencia.get("data", {})
                    if isinstance(s_data, dict):
                        text = self._clean_text(s_data.get("content", ""))
                    elif isinstance(s_data, list) and s_data:
                        text = self._clean_text(s_data[0].get("content", ""))

                if not text or len(text) < 100:
                    continue

                title_parts = []
                if folio:
                    title_parts.append(f"Rol {folio}")
                if nombre:
                    title_parts.append(nombre)
                if meta.get("caratula"):
                    title_parts.append(meta["caratula"][:80])
                title = " - ".join(title_parts) if title_parts else f"TC #{ficha_id}"

                raw = {
                    "id": ficha_id,
                    "title": title,
                    "text": text,
                    "date": self._parse_date(str(fecha)) if fecha else "",
                    "url": f"https://buscador.tcchile.cl/#/sentencia/{ficha_id}",
                    "rol": folio,
                    "competencia": nombre,
                }
                yield self.normalize(raw)
                count += 1

            page += 1
            if total and (page - 1) * 50 >= total:
                break

        logger.info(f"Ficha fetch complete: {count} records with full text")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions."""
        result = self._search_sentencias("de", page=1, limit=20)
        if not result:
            return
        for doc in result.get("data", {}).get("results", []):
            text = self._clean_text(doc.get("content", ""))
            if not text or len(text) < 100:
                continue
            doc_id = doc.get("sentence_id") or doc.get("id", "")
            rol = doc.get("rol", "")
            raw = {
                "id": doc_id,
                "title": f"Rol {rol}" if rol else f"TC #{doc_id}",
                "text": text,
                "date": "",
                "url": f"https://buscador.tcchile.cl/#/sentencia/{doc_id}",
                "rol": rol,
                "competencia": doc.get("competencia", ""),
            }
            yield self.normalize(raw)

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test ficha endpoint
        result = self._fetch_fichas(page=1, limit=5)
        if not result:
            logger.error("Ficha endpoint failed")
            return False
        total = result.get("total", 0)
        logger.info(f"Ficha endpoint OK: {total:,} total records")

        # Test sentencia search
        search = self._search_sentencias("constitucional", page=1, limit=5)
        if not search:
            logger.error("Search endpoint failed")
            return False
        s_total = search.get("meta", {}).get("total", 0)
        results = search.get("data", {}).get("results", [])
        logger.info(f"Search endpoint OK: {s_total:,} results for 'constitucional'")

        if results:
            text = self._clean_text(results[0].get("content", ""))
            logger.info(f"Sample text length: {len(text)} chars")
            if text and len(text) > 100:
                logger.info("Full text extraction OK")
                return True

        logger.warning("Could not verify full text extraction")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/TribunalConstitucional data fetcher")
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
    args = parser.parse_args()

    scraper = TribunalConstitucionalScraper()

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
