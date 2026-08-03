#!/usr/bin/env python3
"""
MX/PoderJudicialTabasco -- Tabasco State Court Decisions (Versiones Públicas de Sentencias)

Fetches public-version court decisions (sentencias / fallos) from the Tribunal
Superior de Justicia del Estado de Tabasco via the public "Consulta de Versiones
Públicas de Sentencias" portal.

Portal: https://tsj-tabasco.gob.mx/sentencias-version-publica/
AJAX endpoint: https://tsj-tabasco.gob.mx/resources/php/ajax.php

Two corpora are covered:

  * Segunda instancia (appellate "fallos") — full appellate judgments with legal
    reasoning. Discovered via funcion=ObtenListaSentenciasPublicas2daInsta over the
    4 combinations of TipoBusqueda (T=Tocas, A=Amparos) x Materia (C=Civil, P=Penal).
    An empty Toca/Fecha filter returns the entire catalogue per combination.

  * Primera instancia (trial-court sentencias) — discovered via
    funcion=ObtenListaSentenciasPublicas1raInsta per (Municipio, Juzgado). The
    Juzgado list for each Municipio is loaded from funciones.php?funcion=juzgado2.

Each result row carries a direct PDF link to the public-version decision; the PDF
body is downloaded and its full text extracted via common.pdf_extract.

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast --full
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html
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
logger = logging.getLogger("legal-data-hunter.MX.PoderJudicialTabasco")

BASE = "https://tsj-tabasco.gob.mx"
AJAX = f"{BASE}/resources/php/ajax.php"
FUNCIONES = f"{BASE}/resources/php/funciones.php"

# Segunda instancia search combinations (empty filter returns full catalogue).
SEGUNDA_COMBOS = [
    ("T", "C", "Tocas", "Civil"),
    ("T", "P", "Tocas", "Penal"),
    ("A", "C", "Amparos", "Civil"),
    ("A", "P", "Amparos", "Penal"),
]

# Municipio ids in the Primera-instancia selector (Balancán..Tenosique).
MUNICIPIOS = list(range(1, 18))


def _clean(text: str) -> str:
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _abs_pdf(url: str) -> str:
    url = url.strip()
    if url.startswith("http://"):
        url = "https://" + url[len("http://"):]
    return url


class TabascoCourtScraper(BaseScraper):
    """Scraper for MX/PoderJudicialTabasco public-version court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; Legal-Data-Hunter/1.0; "
                          "+https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": f"{BASE}/sentencias-version-publica/",
        })

    # ---- HTTP helpers -------------------------------------------------

    def _post(self, url: str, data: str, timeout: int = 90) -> Optional[str]:
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.post(
                    url, data=data, timeout=timeout,
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                )
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                resp.raise_for_status()
                return resp.text
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    # ---- Discovery ----------------------------------------------------

    def _parse_rows(self, html_text: str) -> List[Dict[str, str]]:
        """Parse a results table into a list of cell-dicts + pdf link."""
        rows = []
        for tr in re.findall(r"<tr>(.*?)</tr>", html_text, re.S | re.I):
            link_m = re.search(r'href="([^"]+\.pdf)"', tr, re.I)
            if not link_m:
                continue
            cells = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S | re.I)
            rows.append({
                "cells": [_clean(c) for c in cells],
                "pdf": _abs_pdf(link_m.group(1)),
            })
        return rows

    def _segunda(self, tipo: str, materia: str) -> List[Dict[str, Any]]:
        data = (f"funcion=ObtenListaSentenciasPublicas2daInsta&TipoBusqueda={tipo}"
                f"&Materia={materia}&Toca=&Fecha=&JuicioDelito=&Resolucion=")
        text = self._post(AJAX, data)
        out = []
        if not text:
            return out
        for r in self._parse_rows(text):
            c = r["cells"]
            out.append({
                "instancia": "segunda",
                "tipo": tipo, "materia": materia,
                "toca": c[0] if len(c) > 0 else "",
                "sala": c[1] if len(c) > 1 else "",
                "fecha": c[2] if len(c) > 2 else "",
                "pdf": r["pdf"],
            })
        return out

    def _juzgados(self, municipio: int) -> List[str]:
        text = self._post(f"{FUNCIONES}?funcion=juzgado2&Id={municipio}", data="")
        if not text:
            return []
        return re.findall(r"<option\s+value='([^']+)'", text)

    def _primera(self, municipio: int, juzgado: str) -> List[Dict[str, Any]]:
        data = (f"funcion=ObtenListaSentenciasPublicas1raInsta&Municipio={municipio}"
                f"&Juzgado={juzgado}&Fecha=&Expediente=&JuicioDelito=&Resolucion=")
        text = self._post(AJAX, data)
        out = []
        if not text:
            return out
        for r in self._parse_rows(text):
            c = r["cells"]
            out.append({
                "instancia": "primera",
                "municipio": str(municipio), "juzgado": juzgado,
                "juzgado_nombre": c[0] if len(c) > 0 else "",
                "expediente": c[1] if len(c) > 1 else "",
                "fecha": c[2] if len(c) > 2 else "",
                "resolucion": c[3] if len(c) > 3 else "",
                "actos": c[4] if len(c) > 4 else "",
                "pdf": r["pdf"],
            })
        return out

    def _discover(self) -> Generator[Dict[str, Any], None, None]:
        seen = set()
        for tipo, materia, _, _ in SEGUNDA_COMBOS:
            rows = self._segunda(tipo, materia)
            logger.info(f"2da instancia {tipo}/{materia}: {len(rows)} rows")
            for r in rows:
                if r["pdf"] in seen:
                    continue
                seen.add(r["pdf"])
                yield r
        for municipio in MUNICIPIOS:
            juzgados = self._juzgados(municipio)
            for juz in juzgados:
                rows = self._primera(municipio, juz)
                if rows:
                    logger.info(f"1ra instancia muni={municipio} juz={juz}: {len(rows)} rows")
                for r in rows:
                    if r["pdf"] in seen:
                        continue
                    seen.add(r["pdf"])
                    yield r

    # ---- Schema -------------------------------------------------------

    @staticmethod
    def _pdf_id(pdf_url: str) -> str:
        stem = pdf_url.rsplit("/", 1)[-1]
        return re.sub(r"\.pdf$", "", stem, flags=re.I)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = self._pdf_id(raw["pdf"])
        date_str = (raw.get("fecha") or "")[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
            date_str = None

        if raw.get("instancia") == "segunda":
            tipo_label = "Toca" if raw.get("tipo") == "T" else "Amparo"
            materia_label = "Civil" if raw.get("materia") == "C" else "Penal"
            ref = raw.get("toca", "")
            title = f"{tipo_label} {ref} — {raw.get('sala','')} (Materia {materia_label})".strip()
            court = raw.get("sala", "")
            case_number = ref
            subject = materia_label
            case_type = tipo_label
            ruling = ""
        else:
            ref = raw.get("expediente", "")
            court = raw.get("juzgado_nombre", "")
            title = f"Expediente {ref} — {court}".strip()
            case_number = ref
            subject = ""
            case_type = raw.get("actos", "")
            ruling = raw.get("resolucion", "")

        return {
            "_id": f"MX-TAB-{doc_id}",
            "_source": "MX/PoderJudicialTabasco",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title or f"Sentencia {doc_id}",
            "text": raw.get("_text", ""),
            "date": date_str,
            "url": raw["pdf"],
            "court": court,
            "instance": "2da" if raw.get("instancia") == "segunda" else "1ra",
            "subject_area": subject,
            "case_type": case_type,
            "ruling": ruling,
            "case_number": case_number,
        }

    # ---- Fetch --------------------------------------------------------

    def _extract_text(self, pdf_url: str, doc_id: str) -> str:
        try:
            text = extract_pdf_markdown(
                "MX/PoderJudicialTabasco", f"MX-TAB-{doc_id}", pdf_url=pdf_url,
            )
            return text or ""
        except Exception as e:
            logger.warning(f"PDF extract failed for {pdf_url}: {e}")
            return ""

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        skipped = 0
        for raw in self._discover():
            doc_id = self._pdf_id(raw["pdf"])
            text = self._extract_text(raw["pdf"], doc_id)
            if not text or len(text) < 50:
                skipped += 1
                continue
            raw["_text"] = text
            count += 1
            yield raw
            if count % 50 == 0:
                logger.info(f"Fetched {count} decisions ({skipped} skipped)")
        logger.info(f"Completed: {count} decisions fetched, {skipped} skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        # Segunda-instancia tables are date-desc ordered; re-scan and let the
        # loader dedup on _id.
        count = 0
        for tipo, materia, _, _ in SEGUNDA_COMBOS:
            for raw in self._segunda(tipo, materia)[:50]:
                doc_id = self._pdf_id(raw["pdf"])
                text = self._extract_text(raw["pdf"], doc_id)
                if not text or len(text) < 50:
                    continue
                raw["_text"] = text
                count += 1
                yield raw
        logger.info(f"Updates: {count} decisions fetched")

    def test(self) -> bool:
        text = self._post(
            AJAX,
            "funcion=ObtenListaSentenciasPublicas2daInsta&TipoBusqueda=T&Materia=C"
            "&Toca=&Fecha=&JuicioDelito=&Resolucion=",
        )
        if not text:
            logger.error("Cannot reach TSJ Tabasco AJAX endpoint")
            return False
        rows = self._parse_rows(text)
        logger.info(f"AJAX OK: {len(rows)} 2da-instancia rows for Tocas/Civil")
        if not rows:
            return False
        sample = rows[0]
        txt = self._extract_text(sample["pdf"], self._pdf_id(sample["pdf"]))
        logger.info(f"Sample PDF {sample['pdf']} -> {len(txt)} chars")
        return len(txt) > 50


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MX/PoderJudicialTabasco data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TabascoCourtScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
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
