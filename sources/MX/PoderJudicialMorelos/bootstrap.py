#!/usr/bin/env python3
"""
MX/PoderJudicialMorelos -- Morelos State Court Decisions (Versiones Públicas)

Fetches full-text court decisions (sentencias, versiones públicas) from the
Poder Judicial del Estado de Morelos "Buscador Temático de Versiones Públicas
de Sentencias" (SISEMOR).

Front-end: https://tsjmorelos.gob.mx/sentencias/  (Angular SPA at
           http://sisemor.tsjmorelos2.gob.mx:8000/sentencias)
API base:  http://sisemor.tsjmorelos2.gob.mx:8080/

Strategy:
  - GET buscador/listarEstadistica  -> JSON list of ALL public sentencias
    (id_sentencia, numero_expediente, fecha_sentencia, id_materia, sede, ...).
  - GET sentencia/verDocumento?idSentencia=<id>  -> JSON with nombre_documento.
  - GET archivos/<nombre_documento>  -> the full-text PDF; extract text.
  - Catalog endpoints (catalogos/materias, catalogos/tipojuzgado,
    catalogos/distritos) map numeric ids to human-readable labels.

Usage:
  python bootstrap.py test
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap-fast --full
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
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialMorelos")

API = "http://sisemor.tsjmorelos2.gob.mx:8080/"
FRONTEND = "https://tsjmorelos.gob.mx/sentencias/"
LIST_URL = API + "buscador/listarEstadistica"


class MorelosCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialMorelos full-text sentencias (versiones públicas)."""

    def __init__(self, source_dir=None):
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Legal-Data-Hunter/1.0",
            "Referer": "http://sisemor.tsjmorelos2.gob.mx:8000/",
        })
        self._catalogs: Dict[str, Dict[str, str]] = {}

    # ── helpers ───────────────────────────────────────────────────────
    def _get_json(self, url: str, timeout: int = 90) -> Any:
        for attempt in range(3):
            try:
                time.sleep(0.8)
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.RequestException, ValueError) as e:
                logger.warning(f"GET {url} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    def _load_catalogs(self) -> None:
        if self._catalogs:
            return
        specs = {
            "materia": ("catalogos/materias", "id_materia", "nombre_materia"),
            "juzgado": ("catalogos/tipojuzgado", "id_juzgado", "nombre_juzgado"),
            "distrito": ("catalogos/distritos", "id_distrito", "nombre_distrito"),
            "resolucion": ("catalogos/resolucion", "id_resolucion", "nombre_resolucion"),
        }
        for key, (path, idf, namef) in specs.items():
            data = self._get_json(API + path, timeout=30) or []
            self._catalogs[key] = {
                str(r.get(idf)): (r.get(namef) or "").strip()
                for r in data if isinstance(r, dict)
            }
        logger.info(
            "Catalogs loaded: "
            + ", ".join(f"{k}={len(v)}" for k, v in self._catalogs.items())
        )

    def _label(self, cat: str, value: Any) -> str:
        return self._catalogs.get(cat, {}).get(str(value), "")

    def _list_all(self) -> List[Dict[str, Any]]:
        data = self._get_json(LIST_URL, timeout=180)
        if not isinstance(data, list):
            logger.error("listarEstadistica did not return a list")
            return []
        logger.info(f"listarEstadistica: {len(data)} sentencias")
        return data

    def _fetch_pdf_for(self, id_sentencia: str) -> Optional[bytes]:
        meta = self._get_json(
            API + f"sentencia/verDocumento?idSentencia={id_sentencia}", timeout=60
        )
        if not isinstance(meta, dict):
            return None
        nombre = (meta.get("nombre_documento") or "").strip()
        if not nombre or (meta.get("extension") or "").lower() != "pdf":
            return None
        for attempt in range(3):
            try:
                time.sleep(0.6)
                resp = self.session.get(API + "archivos/" + nombre, timeout=90)
                if 400 <= resp.status_code < 500:
                    return None
                resp.raise_for_status()
                if resp.content[:4] != b"%PDF":
                    return None
                return resp.content
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF {nombre} attempt {attempt+1} failed: {e}")
                time.sleep(4)
        return None

    def _iter_rows(self) -> Generator[Dict[str, Any], None, None]:
        self._load_catalogs()
        seen = set()
        # Newest first: sort by id_sentencia descending where possible.
        rows = self._list_all()
        try:
            rows.sort(key=lambda r: int(r.get("id_sentencia") or 0), reverse=True)
        except Exception:
            pass
        for row in rows:
            sid = str(row.get("id_sentencia") or "").strip()
            if not sid or sid in seen:
                continue
            seen.add(sid)
            pdf = self._fetch_pdf_for(sid)
            if not pdf:
                continue
            text = extract_pdf_text(pdf)
            if not text or len(text) < 100:
                continue
            row["_text"] = text
            yield row

    # ── BaseScraper interface ─────────────────────────────────────────
    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_rows()

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        for raw in self._iter_rows():
            f = raw.get("fecha_sentencia") or ""
            if since and f and f < since:
                continue
            yield raw

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        sid = str(raw.get("id_sentencia") or "").strip()
        doc_id = f"MX-MOR-{sid}" if sid else f"MX-MOR-{abs(hash(json.dumps(raw, sort_keys=True)))}"

        fecha = (raw.get("fecha_sentencia") or "").strip()
        date_str = None
        m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", fecha)
        if m and 1950 <= int(m.group(1)) <= 2100:
            date_str = m.group(0)

        expediente = (raw.get("numero_expediente") or "").strip()
        materia = self._label("materia", raw.get("id_materia"))
        juzgado = self._label("juzgado", raw.get("id_juzgado"))
        distrito = self._label("distrito", raw.get("id_distrito"))
        resolucion = self._label("resolucion", raw.get("id_resolucion")) or (
            raw.get("tipo_resolucion") or "").strip()
        sede = (raw.get("sede") or "").strip()

        title_parts = [p for p in [materia, resolucion, expediente] if p]
        title = " — ".join(title_parts) if title_parts else f"Sentencia {expediente or doc_id}"

        return {
            "_id": doc_id,
            "_source": "MX/PoderJudicialMorelos",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("_text", ""),
            "date": date_str,
            "url": FRONTEND,
            "court": juzgado,
            "district": distrito,
            "venue": sede,
            "subject_area": materia,
            "ruling": resolucion,
            "case_number": expediente,
        }

    # ── Connectivity test ─────────────────────────────────────────────
    def test(self) -> bool:
        rows = self._list_all()
        if not rows:
            logger.error("listarEstadistica returned no rows")
            return False
        self._load_catalogs()
        # try the first id that yields a real PDF
        for row in rows[:10]:
            sid = str(row.get("id_sentencia") or "")
            pdf = self._fetch_pdf_for(sid)
            if not pdf:
                continue
            text = extract_pdf_text(pdf)
            logger.info(f"Sample sentencia {sid}: {len(text or '')} chars")
            if text and len(text) > 100:
                return True
        logger.error("No sample PDF with extractable text found")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialMorelos data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MorelosCourtScraper()

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
