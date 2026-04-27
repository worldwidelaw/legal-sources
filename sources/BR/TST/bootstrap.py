#!/usr/bin/env python3
"""
BR/TST -- Tribunal Superior do Trabalho (Brazilian Labor Supreme Court)

Fetches case law from the TST jurisprudence backend API. The API exposes
a POST search endpoint at /rest/pesquisa-textual/{start}/{size} that
returns paginated results with full-text HTML of court decisions.

Each record includes:
  - inteiroTeorHtml: full text of the decision (HTML)
  - ementa: legal headnote/summary (plain text)
  - dispositivo: holding/ruling text
  - nomRelator: reporting justice
  - dtaJulgamento/dtaPublicacao: judgment and publication dates
  - orgaoJudicante: judging body (Turma, Seção, etc.)

The API contains 8M+ records. Full bootstrap paginates through all of them.
Sample mode fetches a small batch for validation.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import hashlib
import logging
import re
import time
import requests
from html.parser import HTMLParser
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.TST")

BASE_URL = "https://jurisprudencia-backend2.tst.jus.br"
SEARCH_ENDPOINT = "/rest/pesquisa-textual"
PAGE_SIZE = 50
SAMPLE_SIZE = 15


def strip_html(html_str: str) -> str:
    """Convert HTML to clean plain text, removing style/script blocks and comments."""
    if not html_str:
        return ""
    # Remove style and script blocks
    text = re.sub(r"<style[^>]*>.*?</style>", " ", html_str, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML comments
    text = re.sub(r"<!--.*?-->", " ", text, flags=re.DOTALL)
    # Remove all HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Decode common HTML entities
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'").replace("&nbsp;", " ")
    text = re.sub(r"&#?\w+;", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse TST date formats to ISO 8601 (YYYY-MM-DD)."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Format: "2026-03-24T23:59:59-03" or "2026-04-06"
    m = re.match(r"(\d{4}-\d{2}-\d{2})", date_str)
    if m:
        return m.group(1)
    return None


class TSTScraper(BaseScraper):
    SOURCE_ID = "BR/TST"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "LegalDataHunter/1.0 (open-data research)",
        })

    def _search(self, start: int, size: int, body: Optional[Dict] = None) -> Dict[str, Any]:
        """Call the TST search API."""
        url = f"{BASE_URL}{SEARCH_ENDPOINT}/{start}/{size}"
        payload = body or {}
        resp = self.session.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a TST search result record into standard schema."""
        reg = raw.get("registro", raw)

        # Extract full text from HTML
        full_text = strip_html(reg.get("inteiroTeorHtml", ""))
        # Fallback to highlighted version if main is empty
        if not full_text:
            full_text = strip_html(reg.get("inteiroTeorHTMLHighlight", ""))
        # Fallback to ementa if no full text at all
        if not full_text:
            full_text = reg.get("ementa", "")

        ementa = reg.get("ementa", "")
        dispositivo = strip_html(reg.get("dispositivo", "")) if reg.get("dispositivo") else ""
        process_number = reg.get("numFormatado", "")
        doc_id = reg.get("id", "")

        # Build unique ID
        _id = doc_id or hashlib.md5(
            f"{process_number}:{reg.get('dtaJulgamento', '')}".encode()
        ).hexdigest()

        # Dates
        judgment_date = _parse_date(reg.get("dtaJulgamento"))
        publication_date = _parse_date(reg.get("dtaPublicacao"))

        # Decision type
        tipo = reg.get("tipo", {})
        decision_type = tipo.get("nome", "") if isinstance(tipo, dict) else str(tipo)

        # Judging body
        orgao_judicante = ""
        oj = reg.get("orgaoJudicante", {})
        if isinstance(oj, dict):
            orgao_judicante = oj.get("descricao", "")

        # Relator
        relator = reg.get("nomRelator", "")
        if relator:
            relator = relator.strip().title()

        # Title from process number and type
        title = f"{decision_type} - {process_number}".strip(" -")
        if not title:
            title = ementa[:200] if ementa else f"TST Decision {_id[:12]}"

        return {
            "_id": _id,
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "ementa": ementa,
            "dispositivo": dispositivo,
            "date": judgment_date or publication_date,
            "publication_date": publication_date,
            "judgment_date": judgment_date,
            "url": f"https://jurisprudencia.tst.jus.br/#/pesquisa/processo/{doc_id}" if doc_id else "",
            "process_number": process_number,
            "judge_relator": relator,
            "orgao_judicante": orgao_judicante,
            "decision_type": decision_type,
            "orgao": reg.get("orgao", {}).get("sigla", "TST") if isinstance(reg.get("orgao"), dict) else "TST",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield raw TST records via paginated search.

        Yields raw API records (not normalized) so BaseScraper.bootstrap()
        can call normalize() exactly once per record.
        """
        start = 1
        total = None
        fetched = 0

        while True:
            try:
                data = self._search(start, PAGE_SIZE)
            except Exception as e:
                logger.error("Search failed at start=%d: %s", start, e)
                break

            if total is None:
                total = data.get("totalRegistros", 0)
                logger.info("Total records available: %d", total)

            records = data.get("registros", [])
            if not records:
                logger.info("No more records at start=%d", start)
                break

            for rec in records:
                # Quick check that the raw record has some text content
                reg = rec.get("registro", rec)
                has_text = (reg.get("inteiroTeorHtml") or
                            reg.get("inteiroTeorHTMLHighlight") or
                            reg.get("ementa"))
                if has_text:
                    yield rec
                    fetched += 1

            start += PAGE_SIZE
            if total and start > total:
                break

            time.sleep(1.5)

            if fetched % 500 == 0 and fetched > 0:
                logger.info("Progress: %d records fetched (start=%d/%d)", fetched, start, total or 0)

        logger.info("Fetch complete: %d records", fetched)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch raw decisions published since a given date."""
        body = {"publicacaoInicial": since}
        start = 1
        total = None
        fetched = 0

        while True:
            try:
                data = self._search(start, PAGE_SIZE, body)
            except Exception as e:
                logger.error("Search failed at start=%d: %s", start, e)
                break

            if total is None:
                total = data.get("totalRegistros", 0)
                logger.info("Update records since %s: %d", since, total)

            records = data.get("registros", [])
            if not records:
                break

            for rec in records:
                reg = rec.get("registro", rec)
                has_text = (reg.get("inteiroTeorHtml") or
                            reg.get("inteiroTeorHTMLHighlight") or
                            reg.get("ementa"))
                if has_text:
                    yield rec
                    fetched += 1

            start += PAGE_SIZE
            if total and start > total:
                break

            time.sleep(1.5)

        logger.info("Update fetch complete: %d records", fetched)

    def test(self) -> bool:
        """Quick connectivity and data test."""
        try:
            data = self._search(1, 1)
            total = data.get("totalRegistros", 0)
            records = data.get("registros", [])
            if not records:
                logger.error("No records returned")
                return False
            rec = self.normalize(records[0])
            logger.info("API OK — %d total records, sample has %d chars of text",
                         total, len(rec.get("text", "")))
            return bool(rec.get("text"))
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    scraper = TSTScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=SAMPLE_SIZE)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info("Bootstrap complete: %d records — %s", fetched, stats)
        if fetched == 0:
            sys.exit(1)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
