#!/usr/bin/env python3
"""
MX/PoderJudicialCampeche -- Campeche State Court Decisions (Versiones Públicas)

Fetches full-text court decisions from the Poder Judicial del Estado de Campeche
"Sistema de Versiones Públicas de Resoluciones Jurisdiccionales y Precedentes".

Portal: https://tribunalvirtual.poderjudicialcampeche.gob.mx/precedentes/precedentes/publico/

Strategy:
  - The portal is a Django + DataTables app. A server-side DataTables handler
    (tabla-sentencias-handler-publico-renewed) returns paginated rows as JSON.
    Each row's last cell is the id_resolucion.
  - The full-text PDF for a decision is served at
    /precedentes/precedentes/sentencias/sentenciapublica/<id>/
  - We paginate through all ~28,000 rows, download each PDF, and extract text.
  - CSRF: Django sets a csrftoken cookie on the listing page; we send it back as
    both the csrfmiddlewaretoken POST field and the X-CSRFToken header.

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
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialCampeche")

BASE = "https://tribunalvirtual.poderjudicialcampeche.gob.mx"
LISTING_PAGE = f"{BASE}/precedentes/precedentes/publico/"
HANDLER_URL = f"{BASE}/precedentes/precedentes/tabla-sentencias-handler-publico-renewed/"
DETAIL_URL = f"{BASE}/precedentes/precedentes/obtener-datos-version-publica/"
PDF_URL_TMPL = f"{BASE}/precedentes/precedentes/sentencias/sentenciapublica/{{id}}/"

PAGE_SIZE = 100


class CampecheCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialCampeche full-text versiones públicas."""

    def __init__(self, source_dir=None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Legal-Data-Hunter/1.0",
            "Referer": LISTING_PAGE,
        })
        self._csrf = None

    # ── Session / CSRF ────────────────────────────────────────────────
    def _ensure_session(self) -> bool:
        if self._csrf:
            return True
        for attempt in range(3):
            try:
                self.session.get(LISTING_PAGE, timeout=30)
                tok = self.session.cookies.get("csrftoken")
                if tok:
                    self._csrf = tok
                    return True
            except requests.exceptions.RequestException as e:
                logger.warning(f"Session init attempt {attempt+1} failed: {e}")
                time.sleep(3)
        logger.error("Could not obtain CSRF token")
        return False

    # ── Listing (DataTables handler) ──────────────────────────────────
    def _fetch_page(self, start: int, length: int) -> Optional[Dict[str, Any]]:
        data = {
            "draw": "1",
            "start": str(start),
            "length": str(length),
            "csrfmiddlewaretoken": self._csrf,
            "filtros": json.dumps({}),
        }
        headers = {
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRFToken": self._csrf,
        }
        for attempt in range(3):
            try:
                time.sleep(1.0)
                resp = self.session.post(HANDLER_URL, data=data, headers=headers, timeout=45)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                logger.warning(f"Listing start={start} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    def _fetch_pdf(self, res_id: str) -> Optional[bytes]:
        url = PDF_URL_TMPL.format(id=res_id)
        for attempt in range(3):
            try:
                time.sleep(1.0)
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                if resp.content[:4] != b"%PDF":
                    logger.debug(f"Non-PDF for id {res_id}")
                    return None
                return resp.content
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF fetch {res_id} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    # ── Core iteration ────────────────────────────────────────────────
    def _iter_rows(self) -> Generator[Dict[str, Any], None, None]:
        if not self._ensure_session():
            return
        first = self._fetch_page(0, PAGE_SIZE)
        if not first:
            return
        total = first.get("recordsTotal", 0)
        logger.info(f"Total decisions: {total}")
        seen = set()
        start = 0
        page = first
        while True:
            rows: List[List[Any]] = page.get("data", [])
            if not rows:
                break
            for row in rows:
                if not row:
                    continue
                res_id = str(row[-1])
                if res_id in seen:
                    continue
                seen.add(res_id)
                pdf = self._fetch_pdf(res_id)
                if not pdf:
                    continue
                text = extract_pdf_text(pdf)
                if not text or len(text) < 100:
                    continue
                yield {
                    "id": res_id,
                    "fecha": row[0] if len(row) > 0 else "",
                    "materia": row[1] if len(row) > 1 else "",
                    "expediente": row[2] if len(row) > 2 else "",
                    "asunto": row[3] if len(row) > 3 else "",
                    "tipo": row[4] if len(row) > 4 else "",
                    "anio_judicial": row[5] if len(row) > 5 else "",
                    "_text": text,
                }
            start += PAGE_SIZE
            if start >= total:
                break
            page = self._fetch_page(start, PAGE_SIZE)
            if not page:
                break
            if start % 1000 == 0:
                logger.info(f"Progress: {start}/{total}")

    # ── BaseScraper interface ─────────────────────────────────────────
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_rows()

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        for raw in self._iter_rows():
            if since and raw.get("fecha") and raw["fecha"] < since:
                continue
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        res_id = raw.get("id", "")
        fecha = raw.get("fecha", "")
        import re
        date_str = None
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", fecha or "")
        if m:
            # Some source rows carry scrambled years (e.g. "7202-10-30");
            # only keep dates whose year is plausible.
            year = int(m.group(1))
            if 1950 <= year <= 2100:
                date_str = fecha

        materia = raw.get("materia", "")
        asunto = raw.get("asunto", "")
        expediente = raw.get("expediente", "")
        tipo = raw.get("tipo", "")

        title_parts = [p for p in [materia, asunto, expediente] if p]
        title = " — ".join(title_parts) if title_parts else f"Resolución {res_id}"

        return {
            "_id": f"MX-CAM-{res_id}",
            "_source": "MX/PoderJudicialCampeche",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": date_str,
            "url": PDF_URL_TMPL.format(id=res_id),
            "subject_area": materia,
            "case_type": asunto,
            "ruling": tipo,
            "case_number": expediente,
            "period": raw.get("anio_judicial", ""),
        }

    # ── Connectivity test ─────────────────────────────────────────────
    def test(self) -> bool:
        if not self._ensure_session():
            return False
        page = self._fetch_page(0, 10)
        if not page:
            logger.error("Listing handler failed")
            return False
        total = page.get("recordsTotal", 0)
        rows = page.get("data", [])
        logger.info(f"Listing OK: {total} total, {len(rows)} rows on page 1")
        if not rows:
            return False
        res_id = str(rows[0][-1])
        pdf = self._fetch_pdf(res_id)
        if not pdf:
            logger.error("PDF download failed")
            return False
        text = extract_pdf_text(pdf)
        logger.info(f"Sample PDF text length: {len(text or '')} chars")
        return bool(text and len(text) > 100)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialCampeche data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CampecheCourtScraper()

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
