#!/usr/bin/env python3
"""
BR/TRF4 -- Federal Regional Court 4th Region (Tribunal Regional Federal da 4ª Região)

Fetches court decisions from TRF4's eproc2 jurisprudence portal.
TRF4 covers Rio Grande do Sul (RS), Paraná (PR) and Santa Catarina (SC).

Endpoints:
  - Search: POST externo_controlador.php?acao=jurisprudencia@jurisprudencia/listar_resultados
  - Paginate: POST externo_controlador.php?acao=jurisprudencia@jurisprudencia/ajax_paginar_resultado
  - Full text: GET externo_controlador.php?acao=jurisprudencia@jurisprudencia/download_inteiro_teor&id_jurisprudencia=<ID>

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from base64 import b64encode

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.TRF4")

SOURCE_ID = "BR/TRF4"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://jurisprudencia.trf4.jus.br/eproc2trf4"
CONTROLLER = f"{BASE_URL}/externo_controlador.php"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.5",
}

DELAY = 2.0
PAGE_SIZE = 10

# Regex patterns
RE_PROCESS_NUM = re.compile(r'(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})')
RE_PROCESS_NUM_RAW = re.compile(r'txtValor=(\d{20})')
RE_TOTAL = re.compile(r'name="hdnTotalResultado"[^>]*value="(\d+)"', re.IGNORECASE)
RE_RESULTADO_ID = re.compile(r'id="resultado(\d{20,})"')
RE_DATA_CITACAO = re.compile(r'data-citacao="([^"]*)"')
RE_DATE_BR = re.compile(r'(\d{2})/(\d{2})/(\d{4})')
RE_PAGES = re.compile(r'name="hdnTotalPaginas"[^>]*value="(\d+)"', re.IGNORECASE)

# Origins: 1=TRF4, 2=TRU4, 3=Turmas Recursais
ORIGINS = ["1", "2"]
# Doc types: 1=Acordao, 2=Decisao monocratica
DOC_TYPES = ["1", "2"]


def clean_html(text: str) -> str:
    """Strip HTML tags and clean text."""
    if not text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class TRF4Scraper(BaseScraper):
    """Scraper for BR/TRF4 -- Federal Regional Court 4th Region decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _build_search_form(self, page: int = 1) -> dict:
        """Build the POST form data for search/pagination."""
        return {
            "txtPesquisa": "*",
            "selOrigem[]": ORIGINS,
            "rdoCampo": "T",
            "selTipoDocumento[]": DOC_TYPES,
            "hdnPaginaAtual": str(page),
            "chkAgruparResultados": "",
        }

    def _search_page(self, page: int = 1) -> Optional[str]:
        """Fetch a page of search results."""
        if page == 1:
            acao = "jurisprudencia@jurisprudencia/listar_resultados"
        else:
            acao = "jurisprudencia@jurisprudencia/ajax_paginar_resultado"

        params = {"acao": acao}
        form_data = self._build_search_form(page=page)

        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.post(
                    CONTROLLER, params=params, data=form_data, timeout=60
                )
                resp.raise_for_status()
                return resp.text
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Search attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("Search failed: %s", e)
                return None
        return None

    def _fetch_full_text(self, jur_id: str) -> Optional[str]:
        """Fetch the full decision text via download_inteiro_teor."""
        params = {
            "acao": "jurisprudencia@jurisprudencia/download_inteiro_teor",
            "id_jurisprudencia": jur_id,
            "termosPesquisados": b64encode(b"*").decode(),
        }
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(
                    CONTROLLER, params=params, timeout=60
                )
                resp.raise_for_status()
                return clean_html(resp.text)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Doc fetch attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("Failed to fetch doc %s: %s", jur_id, e)
                return None
        return None

    def _parse_results(self, page_html: str) -> list:
        """Parse search results page into a list of result dicts."""
        results = []

        # Split by resultadoItem divs (class may have prefix classes)
        cards = re.split(r'(?=resultadoItem" id="resultado\d)', page_html)

        for card in cards:
            # Extract resultado ID from id="resultado<ID>"
            id_match = RE_RESULTADO_ID.search(card)
            if not id_match:
                continue

            jur_id = id_match.group(1)

            # Extract process number from txtValor URL parameter (20-digit raw)
            proc = ""
            raw_match = RE_PROCESS_NUM_RAW.search(card)
            if raw_match:
                raw = raw_match.group(1)
                # Format: NNNNNNN-DD.YYYY.J.TT.OOOO
                proc = f"{raw[0:7]}-{raw[7:9]}.{raw[9:13]}.{raw[13]}.{raw[14:16]}.{raw[16:20]}"

            # Also try standard CNJ format
            if not proc:
                proc_match = RE_PROCESS_NUM.search(card)
                proc = proc_match.group(1) if proc_match else ""

            # Extract ementa from data-citacao or result text
            citacao_match = RE_DATA_CITACAO.search(card)
            ementa = html_mod.unescape(citacao_match.group(1)) if citacao_match else ""

            # Extract decision date
            date_match = RE_DATE_BR.search(card)
            date_str = None
            if date_match:
                d, m, y = date_match.groups()
                try:
                    datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
                    date_str = f"{y}-{m}-{d}"
                except ValueError:
                    pass

            results.append({
                "jur_id": jur_id,
                "process_number": proc,
                "ementa": ementa,
                "date": date_str,
            })

        return results

    def _get_total(self, page_html: str) -> int:
        """Extract total result count from hidden field."""
        m = RE_TOTAL.search(page_html)
        if m:
            return int(m.group(1))
        return 0

    def _get_total_pages(self, page_html: str) -> int:
        """Extract total pages from hidden field."""
        m = RE_PAGES.search(page_html)
        if m:
            return int(m.group(1))
        return 0

    def normalize(self, doc: dict) -> dict:
        """Transform a parsed record into the standard schema."""
        proc = doc.get("process_number", "")
        text = doc.get("text", "")
        ementa = doc.get("ementa", "")

        # Extract case class from ementa
        classe = ""
        if ementa:
            first_line = ementa.split("\n")[0].strip()
            class_match = re.match(r'^(.+?)\s+N[ºo°]\s', first_line)
            if class_match:
                classe = class_match.group(1).strip()

        title = f"{classe} - {proc}" if classe and proc else f"TRF4 {proc}"

        # Use date from card, or try to extract from text
        date = doc.get("date")
        if not date and text:
            date_match = RE_DATE_BR.search(text[-500:] if len(text) > 500 else text)
            if date_match:
                d, m, y = date_match.groups()
                try:
                    datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
                    date = f"{y}-{m}-{d}"
                except ValueError:
                    pass

        safe_proc = re.sub(r'[^0-9]', '', proc)
        jur_id = doc.get("jur_id", "")
        doc_id = f"BR-TRF4-{safe_proc}" if safe_proc else f"BR-TRF4-{jur_id}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{CONTROLLER}?acao=jurisprudencia@jurisprudencia/download_inteiro_teor&id_jurisprudencia={jur_id}",
            "language": "pt",
            "process_number": proc,
            "classe": classe,
            "court": "TRF4",
            "ementa": ementa[:500] if ementa else "",
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch TRF4 decisions with pagination."""
        sample_limit = 15 if sample else 999999
        count = 0
        seen = set()

        # First page
        page_html = self._search_page(page=1)
        if not page_html:
            logger.error("Failed to fetch first page")
            return

        total = self._get_total(page_html)
        total_pages = self._get_total_pages(page_html)
        logger.info("Total results: %d (%d pages)", total, total_pages)

        current_page = 1
        max_pages = min(total_pages, 100) if not sample else total_pages

        while count < sample_limit:
            if current_page > 1:
                page_html = self._search_page(page=current_page)
                if not page_html:
                    break

            results = self._parse_results(page_html)
            if not results:
                logger.info("No results on page %d", current_page)
                break

            for result in results:
                if count >= sample_limit:
                    break

                proc = result["process_number"]
                jur_id = result["jur_id"]
                dedup_key = proc if proc else jur_id
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                # Fetch full text
                full_text = self._fetch_full_text(jur_id)
                if not full_text or len(full_text) < 50:
                    logger.warning("No/short full text for %s (id=%s)", proc, jur_id)
                    full_text = result.get("ementa", "")
                    if len(full_text) < 50:
                        continue

                result["text"] = full_text
                record = self.normalize(result)
                yield record
                count += 1

                if count % 10 == 0:
                    logger.info("Fetched %d records...", count)

            current_page += 1
            if current_page > max_pages:
                break

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recent decisions."""
        logger.info("Fetching recent TRF4 decisions")
        yield from self.fetch_all()


def main():
    scraper = TRF4Scraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to TRF4 eproc2 portal...")
        page_html = scraper._search_page(page=1)
        if not page_html:
            logger.error("Search test FAILED")
            sys.exit(1)

        total = scraper._get_total(page_html)
        results = scraper._parse_results(page_html)
        logger.info("TRF4 OK — %d total results, %d on first page", total, len(results))

        if results:
            jur_id = results[0]["jur_id"]
            full_text = scraper._fetch_full_text(jur_id)
            if full_text:
                logger.info("Full text OK — %d chars for id=%s", len(full_text), jur_id)
                logger.info("Preview: %.200s", full_text[:200])
            else:
                logger.error("Full text fetch FAILED for id=%s", jur_id)
                sys.exit(1)
        return

    if command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in scraper.fetch_all(sample=sample):
            count += 1
            safe_id = record["_id"].replace("/", "_")[:100]
            fname = SAMPLE_DIR / f"{safe_id}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            if count % 50 == 0:
                logger.info("Saved %d records...", count)
        logger.info("Bootstrap complete: %d records saved to %s", count, SAMPLE_DIR)

    elif command == "update":
        since = (sys.argv[2] if len(sys.argv) > 2
                 and not sys.argv[2].startswith("-") else "2025-01-01")
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d records since %s", count, since)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
