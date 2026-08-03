#!/usr/bin/env python3
"""
MX/PoderJudicialNayarit -- Nayarit State Court Decisions (Sentencias Dictadas)

Fetches full-text court decisions from the Poder Judicial del Estado de Nayarit
"Tribunal Virtual — Sentencias Dictadas" public portal.

Portal: https://tribunalvirtual-tsjnay.gob.mx/publico/sentencias.public.php

Strategy:
  - The public search form posts to app/sentencias.buscar.public.php with a
    static Authorization header. Submitting only the `anio` (year) field (juzgado
    blank) returns ALL decisions for that year as JSON:
        {"resultados": [{"expediente","juzgado","materia",
                         "fecha_resolucion","tipo_registro","archivo"}, ...]}
  - `archivo` is a relative path to the full-text PDF (documentos/YYYY/<file>.pdf),
    resolved against the /publico/ base.
  - We iterate year by year (2008..current), download each PDF, and extract text.

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py bootstrap-fast --full
  python bootstrap.py test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import _extract as extract_pdf_text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialNayarit")

BASE = "https://tribunalvirtual-tsjnay.gob.mx/publico/"
LISTING_PAGE = f"{BASE}sentencias.public.php"
SEARCH_URL = f"{BASE}app/sentencias.buscar.public.php"
# Static token embedded in the public page's AJAX call.
AUTH_TOKEN = "Z9xKb3DfM18QrTyLo7PaVwXeJu6NHcGS"

START_YEAR = 2008


class NayaritCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialNayarit full-text sentencias dictadas."""

    def __init__(self, source_dir=None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Legal-Data-Hunter/1.0",
            "Referer": LISTING_PAGE,
        })
        self._primed = False

    def _ensure_session(self) -> bool:
        if self._primed:
            return True
        for attempt in range(3):
            try:
                self.session.get(LISTING_PAGE, timeout=30)
                self._primed = True
                return True
            except requests.exceptions.RequestException as e:
                logger.warning(f"Session init attempt {attempt+1} failed: {e}")
                time.sleep(3)
        logger.error("Could not prime session")
        return False

    def _current_year(self) -> int:
        return datetime.now(timezone.utc).year

    def _fetch_year(self, year: int) -> List[Dict[str, Any]]:
        data = {"fecha": "", "juzgado": "", "exp": "", "anio": str(year), "interes": ""}
        headers = {"Authorization": AUTH_TOKEN, "X-Requested-With": "XMLHttpRequest"}
        for attempt in range(3):
            try:
                time.sleep(1.0)
                resp = self.session.post(SEARCH_URL, data=data, headers=headers, timeout=60)
                resp.raise_for_status()
                payload = resp.json()
                return payload.get("resultados", []) or []
            except (requests.exceptions.RequestException, ValueError) as e:
                logger.warning(f"Year {year} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return []

    def _fetch_pdf(self, archivo: str) -> Optional[bytes]:
        url = archivo if archivo.startswith("http") else BASE + archivo.lstrip("/")
        for attempt in range(3):
            try:
                time.sleep(0.8)
                resp = self.session.get(url, timeout=60)
                # A 404 means the listed PDF simply isn't published — many rows
                # reference files that don't exist on the server. Don't waste
                # retries (and trip the server's connection-drop) on 4xx.
                if 400 <= resp.status_code < 500:
                    logger.debug(f"PDF {archivo} -> HTTP {resp.status_code}, skipping")
                    return None
                resp.raise_for_status()
                if resp.content[:4] != b"%PDF":
                    logger.debug(f"Non-PDF for {archivo}")
                    return None
                return resp.content
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF fetch {archivo} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    def _iter_rows(self) -> Generator[Dict[str, Any], None, None]:
        if not self._ensure_session():
            return
        seen = set()
        for year in range(self._current_year(), START_YEAR - 1, -1):
            rows = self._fetch_year(year)
            if not rows:
                continue
            logger.info(f"Year {year}: {len(rows)} decisions")
            for row in rows:
                archivo = (row.get("archivo") or "").strip()
                if not archivo or archivo in seen:
                    continue
                seen.add(archivo)
                pdf = self._fetch_pdf(archivo)
                if not pdf:
                    continue
                text = extract_pdf_text(pdf)
                if not text or len(text) < 100:
                    continue
                yield {
                    "archivo": archivo,
                    "expediente": row.get("expediente", ""),
                    "juzgado": row.get("juzgado", ""),
                    "materia": row.get("materia", ""),
                    "fecha": row.get("fecha_resolucion", ""),
                    "tipo": row.get("tipo_registro", ""),
                    "_text": text,
                }

    # ── BaseScraper interface ─────────────────────────────────────────
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_rows()

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        for raw in self._iter_rows():
            if since and raw.get("fecha") and raw["fecha"] < since:
                continue
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        archivo = raw.get("archivo", "")
        # Stable id from the archive path (unique per document).
        slug = re.sub(r"[^A-Za-z0-9]+", "-", archivo.rsplit("/", 1)[-1].rsplit(".", 1)[0]).strip("-")
        doc_id = f"MX-NAY-{slug}" if slug else f"MX-NAY-{abs(hash(archivo))}"

        fecha = raw.get("fecha", "")
        date_str = None
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", fecha or "")
        if m and 1950 <= int(m.group(1)) <= 2100:
            date_str = fecha

        expediente = raw.get("expediente", "")
        materia = raw.get("materia", "")
        juzgado = raw.get("juzgado", "")
        tipo = raw.get("tipo", "")

        title_parts = [p for p in [materia, tipo, expediente] if p]
        title = " — ".join(title_parts) if title_parts else f"Sentencia {expediente or doc_id}"

        url = archivo if archivo.startswith("http") else BASE + archivo.lstrip("/")

        return {
            "_id": doc_id,
            "_source": "MX/PoderJudicialNayarit",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": date_str,
            "url": url,
            "court": juzgado,
            "subject_area": materia,
            "ruling": tipo,
            "case_number": expediente,
        }

    # ── Connectivity test ─────────────────────────────────────────────
    def test(self) -> bool:
        if not self._ensure_session():
            return False
        rows = self._fetch_year(self._current_year() - 1)
        if not rows:
            rows = self._fetch_year(self._current_year() - 2)
        if not rows:
            logger.error("Search returned no rows")
            return False
        logger.info(f"Search OK: {len(rows)} rows for sample year")
        pdf = self._fetch_pdf(rows[0].get("archivo", ""))
        if not pdf:
            logger.error("PDF download failed")
            return False
        text = extract_pdf_text(pdf)
        logger.info(f"Sample PDF text length: {len(text or '')} chars")
        return bool(text and len(text) > 100)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialNayarit data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NayaritCourtScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        logger.info(f"Bootstrap-fast complete: {stats}")
        if stats.get("records_new", 0) == 0 and stats.get("records_fetched", 0) == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
