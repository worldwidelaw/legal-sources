#!/usr/bin/env python3
"""
VE/TSJDecisiones -- Venezuela Supreme Court Decisions (TSJ)

Fetches full text court decisions from all chambers of the Tribunal Supremo
de Justicia via the Liferay JSON API + historico.tsj.gob.ve full text files.

Strategy:
  1. /listSala → get all chambers
  2. /listAnoBySala → get years per chamber
  3. /listDayByAnoSala → get dates with decision counts
  4. /listDecisionByFechaSala → get decision metadata
  5. Fetch full text HTML from historico.tsj.gob.ve/decisiones/{sala}/{month}/{file}

Data: Public (TSJ Venezuela, free access).
Rate limit: 2 sec between requests.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample decisions
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VE.TSJDecisiones")

TSJ_BASE = "https://www.tsj.gob.ve/decisiones"
HISTORICO_BASE = "https://historico.tsj.gob.ve/decisiones"
DELAY = 2

# Liferay portlet IDs for each API method
PORTLET_IDS = {
    "listSala": "senderSentencias_WAR_NoticiasTsjPorlet612",
    "listAnoBySala": "receiverSentencia_WAR_NoticiasTsjPorlet612",
    "listDayByAnoSala": "displaySentencias_WAR_NoticiasTsjPorlet612",
    "listDecisionByFechaSala": "displayListaDecision_WAR_NoticiasTsjPorlet612",
}

# Column config per portlet
PORTLET_COL = {
    "listSala": {"col_id": "_118_INSTANCE_C808K7b2myu1__column-1", "col_count": "1"},
    "listAnoBySala": {"col_id": "_118_INSTANCE_C808K7b2myu1__column-2", "col_count": "2"},
    "listDayByAnoSala": {"col_id": "_118_INSTANCE_C808K7b2myu1__column-2", "col_count": "2", "col_pos": "1"},
    "listDecisionByFechaSala": {"col_id": "column-1", "col_count": "2", "col_pos": "1"},
}

# Main salas to scrape (skip juzgados de sustanciación and accidentales for efficiency)
MAIN_SALAS = {"001", "005", "007", "006", "002", "003", "004"}


def html_to_text(html_content: str) -> str:
    """Extract clean text from TSJ decision HTML."""
    if not html_content:
        return ""
    content = html_content
    content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)
    content = re.sub(r'<p[^>]*>', '\n\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<br\s*/?>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<div[^>]*>', '\n', content, flags=re.IGNORECASE)
    content = re.sub(r'<[^>]+>', ' ', content)
    content = html_module.unescape(content)
    content = re.sub(r'[ \t]+', ' ', content)
    content = re.sub(r'\n[ \t]+', '\n', content)
    content = re.sub(r'\n{3,}', '\n\n', content)
    return content.strip()


class TSJDecisionesScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = None

    def _init_http(self):
        """Initialize HTTP client."""
        import requests
        self.http = requests.Session()
        self.http.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
            "Accept": "application/json, text/html, */*",
        })

    def _api_call(self, method: str, extra_params: dict = None) -> Optional[dict]:
        """Call TSJ Liferay JSON API."""
        portlet_id = PORTLET_IDS[method]
        col_cfg = PORTLET_COL[method]

        params = {
            "p_p_id": portlet_id,
            "p_p_lifecycle": "2",
            "p_p_state": "normal",
            "p_p_mode": "view",
            "p_p_cacheability": "cacheLevelPage",
            "p_p_col_id": col_cfg["col_id"],
            "p_p_col_count": col_cfg["col_count"],
            "server[endpoint]": "/services/WSDecision.HTTPEndpoint",
            "server[method]": f"/{method}",
        }
        if "col_pos" in col_cfg:
            params["p_p_col_pos"] = col_cfg["col_pos"]
        if extra_params:
            params.update(extra_params)

        try:
            resp = self.http.get(TSJ_BASE, params=params, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"API {method}: HTTP {resp.status_code}")
                return None
            data = resp.json()
            return data
        except Exception as e:
            logger.warning(f"API {method} error: {e}")
            return None

    @staticmethod
    def _ensure_list_of_dicts(value) -> list:
        """Coerce API response value to a list of dicts, skipping non-dict items."""
        if isinstance(value, dict):
            return [value]
        if isinstance(value, list):
            return [v for v in value if isinstance(v, dict)]
        return []

    def _list_salas(self) -> list:
        """Get all salas (chambers)."""
        data = self._api_call("listSala")
        if not data:
            return []
        try:
            return self._ensure_list_of_dicts(data["coleccion"]["SALA"])
        except (KeyError, TypeError):
            return []

    def _list_years(self, sala_id: str) -> list:
        """Get years with decisions for a sala."""
        data = self._api_call("listAnoBySala", {"SSALAID": sala_id})
        if not data:
            return []
        try:
            years = self._ensure_list_of_dicts(data["coleccion"]["array"])
            return [y["id"] for y in years if y.get("TIENE") == "1"]
        except (KeyError, TypeError):
            return []

    def _list_days(self, sala_id: str, year: str) -> list:
        """Get dates with decisions for a sala/year."""
        data = self._api_call("listDayByAnoSala", {"SALA": sala_id, "ANO": year})
        if not data:
            return []
        try:
            return self._ensure_list_of_dicts(data["coleccion"]["DIA"])
        except (KeyError, TypeError):
            return []

    def _list_decisions(self, sala_id: str, fecha: str) -> list:
        """Get decisions for a specific date/sala."""
        data = self._api_call("listDecisionByFechaSala", {"FECHA": fecha, "SALA": sala_id})
        if not data:
            return []
        try:
            return self._ensure_list_of_dicts(data["coleccion"]["SENTENCIA"])
        except (KeyError, TypeError):
            return []

    def _fetch_full_text(self, sala_dir: str, nombre_mes: str, filename: str) -> str:
        """Fetch full text HTML from historico.tsj.gob.ve."""
        month = nombre_mes.strip().lower()
        url = f"{HISTORICO_BASE}/{sala_dir}/{month}/{quote(filename)}"
        try:
            resp = self.http.get(url, timeout=30)
            if resp.status_code != 200:
                logger.debug(f"Full text 404: {url}")
                return ""
            # Handle windows-1252 encoding
            resp.encoding = "windows-1252"
            return html_to_text(resp.text)
        except Exception as e:
            logger.debug(f"Full text fetch error: {e}")
            return ""

    def test_api(self):
        """Test connectivity to TSJ API."""
        logger.info("Testing TSJ API connectivity...")
        try:
            self._init_http()
            salas = self._list_salas()
            if salas:
                logger.info(f"Found {len(salas)} salas")
                for s in salas[:3]:
                    logger.info(f"  - {s.get('SSALAID')}: {s.get('SSALADESCRIPCION', '').strip()}")

                # Test year listing
                time.sleep(DELAY)
                years = self._list_years(salas[0]["SSALAID"])
                logger.info(f"Years for first sala: {years[:5]}...")

                logger.info("Connectivity test PASSED")
                return True
            logger.error("Connectivity test FAILED: no salas returned")
            return False
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw decision data into standard schema."""
        if not raw or not raw.get("text") or len(raw["text"]) < 50:
            return None

        sent_id = raw.get("SSENTID", "")
        numero = raw.get("SSENTNUMERO", "unknown")
        expediente = raw.get("SSENTEXPEDIENTE", "")
        fecha = raw.get("DSENTFECHA", "")
        partes = raw.get("SSENTPARTES", "").strip()
        decision_type = raw.get("SSENTDECISION", "").strip()
        ponente = raw.get("SPONENOMBRE", "").strip()
        procedimiento = raw.get("SPROCDESCRIPCION", "").strip()
        sala_dir = raw.get("SSALADIR", "")
        sala_name = raw.get("sala_name", sala_dir)

        _id = f"VE-TSJ-{sent_id}" if sent_id else f"VE-TSJ-{numero}-{expediente}".replace("/", "-").replace(" ", "_")

        # Parse date (dd/mm/yyyy -> ISO)
        iso_date = None
        if fecha:
            try:
                parts = fecha.split("/")
                if len(parts) == 3:
                    iso_date = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except Exception:
                pass

        title_parts = [f"Sentencia N° {numero}"]
        if sala_name:
            title_parts.append(sala_name.strip())
        if partes:
            title_parts.append(partes[:100])
        title = " — ".join(title_parts)

        # Build full text URL
        nombre_mes = raw.get("NOMBREMES", "").strip().lower()
        filename = raw.get("SSENTNOMBREDOC", "")
        url = f"{HISTORICO_BASE}/{sala_dir}/{nombre_mes}/{filename}" if filename else f"https://www.tsj.gob.ve/decisiones"

        return {
            "_id": _id,
            "_source": "VE/TSJDecisiones",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": iso_date,
            "url": url,
            "decision_number": numero,
            "case_number": expediente,
            "sala": sala_name,
            "sala_dir": sala_dir,
            "parties": partes,
            "decision_type": decision_type,
            "reporting_judge": ponente,
            "procedure": procedimiento,
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch court decisions with full text."""
        sample_limit = 15 if sample else None
        count = 0
        errors = 0

        self._init_http()
        salas = self._list_salas()
        if not salas:
            logger.error("Failed to fetch salas list")
            return

        logger.info(f"Found {len(salas)} salas")

        # Filter to main salas only
        salas = [s for s in salas if s.get("SSALAID") in MAIN_SALAS]
        logger.info(f"Processing {len(salas)} main salas")

        for sala in salas:
            sala_id = sala["SSALAID"]
            sala_desc = sala.get("SSALADESCRIPCION", "").strip()
            sala_dir = sala.get("SSALADIR", "")
            logger.info(f"Processing sala: {sala_desc} ({sala_id}/{sala_dir})")

            time.sleep(DELAY)
            years = self._list_years(sala_id)
            if not years:
                continue

            # For sample: just latest year; for full: all years
            if sample:
                years = years[:1]

            for year in years:
                time.sleep(DELAY)
                days = self._list_days(sala_id, year)
                if not days:
                    continue
                logger.info(f"  Year {year}: {len(days)} dates with decisions")

                # For sample: just first 2 dates per sala
                if sample:
                    days = days[:2]

                for day_info in days:
                    fecha = day_info.get("FECHA", "")
                    nombre_mes = day_info.get("NOMBREMES", "")
                    if not fecha:
                        continue

                    time.sleep(DELAY)
                    decisions = self._list_decisions(sala_id, fecha)
                    if not decisions:
                        continue

                    for dec in decisions:
                        filename = dec.get("SSENTNOMBREDOC", "")
                        if not filename:
                            continue

                        time.sleep(DELAY)
                        text = self._fetch_full_text(sala_dir, nombre_mes, filename)
                        if not text or len(text) < 50:
                            errors += 1
                            continue

                        dec["text"] = text
                        dec["sala_name"] = sala_desc
                        dec["SSALADIR"] = sala_dir
                        dec["NOMBREMES"] = nombre_mes

                        count += 1
                        logger.info(f"  [{count}] {dec.get('SSENTNUMERO', '?')} ({fecha}) — {len(text)} chars")
                        yield dec

                        if sample_limit and count >= sample_limit:
                            logger.info(f"Sample limit ({sample_limit}) reached")
                            return

        logger.info(f"Total decisions fetched: {count} (errors: {errors})")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch recent decisions."""
        yield from self.fetch_all(sample=False)

def main():
    import argparse
    parser = argparse.ArgumentParser(description="VE/TSJDecisiones bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 decisions)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--workers", type=int, default=5, help="Concurrent workers for bootstrap-fast")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for bootstrap-fast")
    args = parser.parse_args()

    scraper = TSJDecisionesScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
        if stats.get("records_fetched", 0) == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        sys.exit(0)
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast(
            max_workers=args.workers,
            batch_size=args.batch_size,
        )
        logger.info(f"Bootstrap-fast complete: {json.dumps(stats, indent=2)}")
        if stats.get("records_fetched", 0) == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()
