#!/usr/bin/env python3
"""
CL/TTA — Tribunales Tributarios y Aduaneros (Tax & Customs Courts)

Fetches tax and customs tribunal decisions from all 18 regional TTA courts.
~11,500 decisions from 2012-present with full text via ElasticSearch.

Data source: https://ojv.tta.cl/#/instituciones/jurisprudencia
API: ElasticSearch endpoint at /buscador/obtienedocumentosfiltroexpandidoes
Full text: attachment.content field (pre-extracted by ES ingest-attachment)
License: Public domain (official Chilean tribunal decisions)
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("legal-data-hunter.CL.TTA")

SOURCE_ID = "CL/TTA"
SAMPLE_DIR = Path(__file__).parent / "sample"

ES_URL = "https://ojv.tta.cl/buscador/obtienedocumentosfiltroexpandidoes"
API_URL = "https://ojv.tta.cl/api"

FIELDS_WITH_TEXT = [
    "Ruc", "Rit", "FechaPresentacion", "Tribunal", "TribunalId",
    "Procedimiento", "Servicio", "Materia", "Caratula",
    "FechaSentencia", "SiglaServicio", "ExpedienteId", "Extracto",
    "attachment",
]
FIELDS_META_ONLY = [
    "Ruc", "Rit", "FechaPresentacion", "Tribunal", "TribunalId",
    "Procedimiento", "Materia", "Caratula", "FechaSentencia",
    "SiglaServicio", "ExpedienteId",
]


class TTAFetcher:
    """Fetcher for Chilean Tax & Customs Tribunal decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _es_search(self, page: int, per_page: int = 100,
                   include_text: bool = True) -> Dict[str, Any]:
        """Query the ElasticSearch endpoint for decisions."""
        params = {
            "sistema": "SACTA",
            "numeroPagina": str(page),
            "registrosPorPagina": str(per_page),
            "camposOrdenamiento": "[]",
            "camposIncluidos": ",".join(FIELDS_WITH_TEXT if include_text else FIELDS_META_ONLY),
            "camposExcluidos": "Contenido" if include_text else "attachment,Contenido",
            "filtros": "[]",
        }
        resp = self.session.get(ES_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _parse_date(self, date_str: Optional[str]) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        for fmt in ("%Y-%m-%d", "%d-%m-%Y %H:%M:%S", "%d-%m-%Y", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue
        return date_str[:10] if len(date_str) >= 10 else None

    def _clean_text(self, text: str) -> str:
        """Clean extracted text: remove excessive whitespace, fix encoding."""
        if not text:
            return ""
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\r\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]{2,}", " ", text)
        return text.strip()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a raw ES record into the standard schema."""
        attachment = raw.get("attachment", {}) or {}
        full_text = self._clean_text(attachment.get("content", ""))
        ruc = raw.get("ruc", "")
        rit = raw.get("rit", "")
        doc_id = ruc or rit or raw.get("expedienteid", "unknown")
        fecha = self._parse_date(raw.get("fechasentencia"))
        tribunal = raw.get("tribunal", "")
        caratula = raw.get("caratula", "")
        title = f"{caratula}" if caratula else f"TTA Decision {rit or ruc}"
        return {
            "_id": f"CL-TTA-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": fecha,
            "url": f"https://ojv.tta.cl/#/instituciones/fallos",
            "ruc": ruc,
            "rit": rit,
            "tribunal": tribunal,
            "procedure": raw.get("procedimiento", ""),
            "subject": raw.get("materia", ""),
            "service": raw.get("siglaservicio", ""),
            "extracto": raw.get("extracto", ""),
            "expediente_id": raw.get("expedienteid", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions from ElasticSearch with full text."""
        page = 1
        per_page = 100
        total_yielded = 0
        max_records = 15 if sample else 999999

        while total_yielded < max_records:
            try:
                data = self._es_search(page, per_page, include_text=True)
            except requests.RequestException as e:
                logger.error(f"ES request failed on page {page}: {e}")
                break

            records = data.get("registrosEncontrados", [])
            if not records:
                logger.info(f"No more records at page {page}")
                break

            for rec_str in records:
                try:
                    raw = json.loads(rec_str) if isinstance(rec_str, str) else rec_str
                except (json.JSONDecodeError, TypeError) as e:
                    logger.warning(f"Failed to parse record: {e}")
                    continue

                doc = self.normalize(raw)
                if not doc["text"]:
                    logger.warning(f"Empty text for {doc['_id']}, skipping")
                    continue

                yield doc
                total_yielded += 1
                if total_yielded >= max_records:
                    break

            logger.info(f"Page {page}: fetched {len(records)} records (total: {total_yielded})")
            page += 1
            time.sleep(2)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions modified since a given date."""
        page = 1
        per_page = 100

        while True:
            params = {
                "sistema": "SACTA",
                "numeroPagina": str(page),
                "registrosPorPagina": str(per_page),
                "camposOrdenamiento": "[]",
                "camposIncluidos": ",".join(FIELDS_WITH_TEXT),
                "camposExcluidos": "Contenido",
                "filtros": json.dumps([
                    {"campo": "FechaSentencia", "valor": since, "operador": ">="}
                ]),
            }
            try:
                resp = self.session.get(ES_URL, params=params, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except requests.RequestException as e:
                logger.error(f"Update request failed on page {page}: {e}")
                break

            records = data.get("registrosEncontrados", [])
            if not records:
                break

            for rec_str in records:
                try:
                    raw = json.loads(rec_str) if isinstance(rec_str, str) else rec_str
                except (json.JSONDecodeError, TypeError):
                    continue
                doc = self.normalize(raw)
                if doc["text"]:
                    yield doc

            logger.info(f"Update page {page}: {len(records)} records")
            page += 1
            time.sleep(2)


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = TTAFetcher()
    count = 0
    for doc in fetcher.fetch_all(sample=sample):
        out_path = SAMPLE_DIR / f"{count:04d}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        text_len = len(doc.get("text", ""))
        logger.info(f"[{count+1}] {doc['_id']} — {doc['title'][:60]}… ({text_len} chars)")
        count += 1
    logger.info(f"Bootstrap complete: {count} records saved to {SAMPLE_DIR}")
    return count


def test():
    """Quick connectivity test."""
    fetcher = TTAFetcher()
    data = fetcher._es_search(1, 1, include_text=False)
    total = data.get("totalRegistrosEncontrados", 0)
    records = data.get("registrosEncontrados", [])
    logger.info(f"Test OK: {total} total records, got {len(records)} sample")
    if records:
        raw = json.loads(records[0]) if isinstance(records[0], str) else records[0]
        logger.info(f"  Sample: RUC={raw.get('ruc')}, RIT={raw.get('rit')}")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CL/TTA — Tax & Customs Tribunal decisions")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only 15 sample records")
    parser.add_argument("--since", type=str, default=None,
                        help="ISO date for updates (e.g., 2025-01-01)")
    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap(sample=args.sample)
    elif args.command == "update":
        since = args.since or "2025-01-01"
        fetcher = TTAFetcher()
        count = 0
        for doc in fetcher.fetch_updates(since):
            count += 1
        logger.info(f"Update complete: {count} records since {since}")
    elif args.command == "test":
        test()
