#!/usr/bin/env python3
"""
CL/CorteSuprema -- Chilean Supreme Court Jurisprudence via juris.pjud.cl

Fetches Chilean Supreme Court decisions with full text from the official
Poder Judicial jurisprudence search system (Solr-backed JSON API).

Strategy:
  - GET search page to obtain session cookie + CSRF token
  - POST to buscar_sentencias with wildcard search, paginate by offset
  - Full text in texto_sentencia field (HTML, cleaned to plain text)
  - ~281,981 public decisions available

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
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CL.CorteSuprema")

BASE_URL = "https://juris.pjud.cl"
SEARCH_PAGE_URL = f"{BASE_URL}/busqueda"
SEARCH_API_URL = f"{BASE_URL}/busqueda/buscar_sentencias"
CORTE_SUPREMA_ID = "528"
PAGE_SIZE = 10


class CorteSupremaScraper(BaseScraper):
    """Scraper for CL/CorteSuprema -- Chilean Supreme Court via juris.pjud.cl."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json, text/html, */*",
            "Accept-Language": "es-CL,es;q=0.9,en-US;q=0.5,en;q=0.3",
        })
        self._csrf_token = None

    def _get_csrf_token(self) -> Optional[str]:
        """Fetch the search page and extract the CSRF token."""
        if self._csrf_token:
            return self._csrf_token

        try:
            # Load the Corte Suprema search page
            resp = self.session.get(
                f"{SEARCH_PAGE_URL}?Corte_Suprema=",
                timeout=30,
            )
            resp.raise_for_status()

            # Extract _token from hidden input
            soup = BeautifulSoup(resp.text, "html.parser")
            token_input = soup.find("input", {"name": "_token"})
            if token_input:
                self._csrf_token = token_input.get("value")
                logger.info(f"Got CSRF token: {self._csrf_token[:20]}...")
                return self._csrf_token

            # Try meta tag
            meta = soup.find("meta", {"name": "csrf-token"})
            if meta:
                self._csrf_token = meta.get("content")
                logger.info(f"Got CSRF token from meta: {self._csrf_token[:20]}...")
                return self._csrf_token

            logger.error("Could not find CSRF token on page")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to get CSRF token: {e}")
            return None

    def _search(self, offset: int = 0, order: str = "recientes") -> Optional[Dict]:
        """Search for decisions via the API."""
        token = self._get_csrf_token()
        if not token:
            return None

        filtros = json.dumps({
            "todas": "*",
            "algunas": "",
            "literal": "",
            "excluir": "",
            "proximidad": "",
            "filtros_omnibox": [],
        })

        data = {
            "_token": token,
            "id_buscador": CORTE_SUPREMA_ID,
            "filtros": filtros,
            "numero_filas_paginacion": str(PAGE_SIZE),
            "offset_paginacion": str(offset),
            "orden": order,
            "personalizacion": "false",
        }

        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRF-TOKEN": token,
        }

        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.post(
                    SEARCH_API_URL,
                    data=data,
                    headers=headers,
                    timeout=60,
                )
                if resp.status_code == 419:
                    # CSRF token expired, refresh
                    logger.warning("CSRF token expired, refreshing...")
                    self._csrf_token = None
                    token = self._get_csrf_token()
                    if token:
                        data["_token"] = token
                        headers["X-CSRF-TOKEN"] = token
                    continue
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"Search attempt {attempt+1} failed (offset={offset}): {e}")
                if attempt < 2:
                    time.sleep(10)
                    # Refresh CSRF on retry
                    self._csrf_token = None

        return None

    def _clean_html_text(self, html_text: str) -> str:
        """Clean HTML from decision text to plain text."""
        if not html_text:
            return ""
        # Replace <br>, <br/>, <p> with newlines
        text = re.sub(r"<br\s*/?>", "\n", html_text, flags=re.IGNORECASE)
        text = re.sub(r"</?p[^>]*>", "\n", text, flags=re.IGNORECASE)
        # Strip remaining HTML tags
        text = re.sub(r"<[^>]+>", "", text)
        # Decode HTML entities
        import html
        text = html.unescape(text)
        # Normalize whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    def _parse_date(self, date_str: str) -> str:
        """Parse date from Solr format to ISO 8601."""
        if not date_str:
            return ""
        # Solr dates: "2024-12-05T00:00:00Z"
        m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
        if m:
            return m.group(1)
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = raw.get("id", "")
        text = self._clean_html_text(
            raw.get("texto_sentencia", "") or raw.get("texto_sentencia_anon", "")
        )
        title = raw.get("caratulado_s", "") or f"ROL {raw.get('rol_era_sup_s', 'unknown')}"
        date = self._parse_date(raw.get("fec_sentencia_sup_dt", ""))
        case_number = raw.get("rol_era_sup_s", "")
        chamber = raw.get("gls_sala_sup_s", "")
        appeal_type = raw.get("gls_tip_recurso_sup_s", "")
        result = raw.get("resultado_recurso_sup_s", "")
        judges = raw.get("gls_ministro_ss", [])
        if isinstance(judges, list):
            judges = "; ".join(judges)
        descriptors = raw.get("gls_descriptor_ss", [])
        if isinstance(descriptors, list):
            descriptors = "; ".join(descriptors)

        url = raw.get("url_acceso_sentencia", "")
        if not url and doc_id:
            url = f"{BASE_URL}/busqueda/pagina_detalle_sentencia?id_sentencia={doc_id}"

        return {
            "_id": str(doc_id),
            "_source": "CL/CorteSuprema",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "case_number": case_number,
            "chamber": chamber,
            "appeal_type": appeal_type,
            "result": result,
            "judges": judges,
            "descriptors": descriptors,
            "url": url,
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Corte Suprema decisions."""
        offset = 0
        count = 0
        total = None

        while True:
            if max_records and count >= max_records:
                return

            result = self._search(offset=offset)
            if result is None:
                logger.error(f"Search failed at offset {offset}")
                break

            # Extract documents from response
            response = result.get("response", result)
            if isinstance(response, dict):
                docs = response.get("docs", [])
                if total is None:
                    total = response.get("numFound", "?")
                    logger.info(f"Total decisions available: {total}")
            else:
                docs = []

            if not docs:
                logger.info(f"No more documents at offset {offset}")
                break

            logger.info(f"Offset {offset}: {len(docs)} documents")

            for doc in docs:
                if max_records and count >= max_records:
                    return

                normalized = self.normalize(doc)
                if not normalized["text"] or len(normalized["text"]) < 50:
                    logger.warning(
                        f"Insufficient text for {normalized['_id']}: "
                        f"{len(normalized.get('text', ''))} chars"
                    )
                    continue

                count += 1
                yield normalized

            offset += PAGE_SIZE

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent decisions (most recent 50)."""
        yield from self.fetch_all(max_records=50)

    def test(self) -> bool:
        """Quick connectivity test."""
        token = self._get_csrf_token()
        if not token:
            logger.error("Cannot get CSRF token")
            return False

        result = self._search(offset=0)
        if result is None:
            logger.error("Search API failed")
            return False

        response = result.get("response", result)
        if isinstance(response, dict):
            docs = response.get("docs", [])
            total = response.get("numFound", 0)
        else:
            docs = []
            total = 0

        if not docs:
            logger.error("No documents returned")
            return False

        logger.info(f"API OK: {total} total decisions, got {len(docs)} on first page")

        # Check first doc has full text
        first = docs[0]
        text = first.get("texto_sentencia", "") or first.get("texto_sentencia_anon", "")
        clean = self._clean_html_text(text)
        logger.info(
            f"Sample: ROL {first.get('rol_era_sup_s', '?')} | "
            f"{first.get('caratulado_s', '?')[:50]} | "
            f"{len(clean)} chars"
        )

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CL/CorteSuprema data fetcher")
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

    scraper = CorteSupremaScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] ROL {record.get('case_number', '?')} | "
                f"{record.get('title', '?')[:60]} ({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

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
