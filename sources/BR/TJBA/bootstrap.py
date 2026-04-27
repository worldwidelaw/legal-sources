#!/usr/bin/env python3
"""
BR/TJBA -- Bahia State Court (Tribunal de Justiça da Bahia)

Fetches court decisions from TJBA's GraphQL jurisprudence API.
~3.9M decisions (2nd degree + Turmas Recursais, acordãos + monocráticas)
with full text HTML in the conteudo field.

API: POST https://jurisprudenciaws.tjba.jus.br/graphql
  - GraphQL endpoint, no authentication required
  - Returns full text HTML in conteudo field
  - Pagination: pageNumber (0-indexed), itemsPerPage (10)
  - Segments: segundoGrau × turmasRecursais × tipoAcordaos × tipoDecisoesMonocraticas

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
import html
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.TJBA")

SOURCE_ID = "BR/TJBA"
SAMPLE_DIR = Path(__file__).parent / "sample"

GRAPHQL_URL = "https://jurisprudenciaws.tjba.jus.br/graphql"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

DELAY = 1.5
PAGE_SIZE = 10

# GraphQL query for fetching decisions
GRAPHQL_QUERY = """
query filter($decisaoFilter: DecisaoFilter!, $pageNumber: Int!, $itemsPerPage: Int!) {
  filter(decisaoFilter: $decisaoFilter, pageNumber: $pageNumber, itemsPerPage: $itemsPerPage) {
    itemCount
    pageCount
    decisoes {
      id
      numeroProcesso
      ementa
      tipoDecisao
      dataPublicacao
      dataJulgamento
      hash
      conteudo
      contentType
      relator { id nome }
      orgaoJulgador { id nome }
      classe { id descricao }
    }
  }
}
"""

# Segments: (segundoGrau, turmasRecursais, tipoAcordaos, tipoDecisoesMonocraticas, label)
SEGMENTS = [
    (True, False, True, False, "2G-Acordaos"),
    (True, False, False, True, "2G-Monocraticas"),
    (False, True, True, False, "TR-Acordaos"),
    (False, True, False, True, "TR-Monocraticas"),
]


def strip_html(text: str) -> str:
    """Strip HTML tags and decode entities from text."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class TJBAScraper(BaseScraper):
    """Scraper for BR/TJBA -- Bahia State Court decisions via GraphQL API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _graphql(self, variables: dict) -> Optional[dict]:
        """Execute a GraphQL query against the TJBA API."""
        payload = {
            "query": GRAPHQL_QUERY,
            "variables": variables,
        }

        for attempt in range(4):
            try:
                time.sleep(DELAY)
                resp = self.session.post(GRAPHQL_URL, json=payload, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                if "errors" in data:
                    logger.error("GraphQL errors: %s", data["errors"])
                    return None
                return data.get("data", {}).get("filter", {})
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = 3 * (attempt + 1)
                logger.warning("Attempt %d failed: %s. Retrying in %ds...", attempt + 1, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("GraphQL request failed: %s", e)
                return None
        logger.error("All retries exhausted")
        return None

    def _build_filter(self, sg: bool, tr: bool, ta: bool, tdm: bool,
                      date_from: Optional[str] = None, date_to: Optional[str] = None) -> dict:
        """Build a DecisaoFilter for the GraphQL query."""
        f = {
            "segundoGrau": sg,
            "turmasRecursais": tr,
            "tipoAcordaos": ta,
            "tipoDecisoesMonocraticas": tdm,
            "orgaos": [],
            "relatores": [],
            "classes": [],
            "ordenadoPor": "dataPublicacao",
        }
        if date_from:
            f["dataInicial"] = date_from
        if date_to:
            f["dataFinal"] = date_to
        return f

    def normalize(self, doc: dict) -> Optional[dict]:
        """Transform a GraphQL decision record into the standard schema."""
        doc_id = doc.get("id", "")
        processo = doc.get("numeroProcesso", "")
        tipo = doc.get("tipoDecisao", "")

        # Full text from conteudo (HTML)
        raw_html = doc.get("conteudo", "") or ""
        text = strip_html(raw_html)

        # Fallback to ementa if full text is too short
        if len(text) < 50:
            ementa = doc.get("ementa", "") or ""
            if ementa:
                text = ementa

        if len(text) < 20:
            return None

        # Dates
        pub_date = doc.get("dataPublicacao", "")
        julg_date = doc.get("dataJulgamento", "")
        date = (julg_date or pub_date or "")[:10] if (julg_date or pub_date) else None

        # Title
        relator = doc.get("relator", {}) or {}
        orgao = doc.get("orgaoJulgador", {}) or {}
        classe = doc.get("classe", {}) or {}

        tipo_label = "Acórdão" if tipo == "ACORDAO" else "Decisão Monocrática"
        title = f"{tipo_label} - {processo}" if processo else tipo_label
        if classe.get("descricao"):
            title += f" ({classe['descricao']})"

        return {
            "_id": f"BR-TJBA-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://jurisprudencia.tjba.jus.br/",
            "language": "pt",
            "process_number": processo,
            "decision_type": tipo,
            "rapporteur": relator.get("nome", ""),
            "court_body": orgao.get("nome", ""),
            "case_class": classe.get("descricao", ""),
            "publication_date": (pub_date or "")[:10] or None,
            "judgment_date": (julg_date or "")[:10] or None,
            "hash": doc.get("hash", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all TJBA decisions across all segments."""
        count = 0
        sample_limit = 15 if sample else 999_999_999

        for sg, tr, ta, tdm, label in SEGMENTS:
            if count >= sample_limit:
                break

            logger.info("=== Fetching segment: %s ===", label)
            filt = self._build_filter(sg, tr, ta, tdm)

            # First page to get total count
            result = self._graphql({
                "decisaoFilter": filt,
                "pageNumber": 0,
                "itemsPerPage": PAGE_SIZE,
            })
            if not result:
                logger.error("Failed to fetch segment %s", label)
                continue

            total = result.get("itemCount", 0)
            total_pages = result.get("pageCount", 0)
            logger.info("Segment %s: %d total records, %d pages", label, total, total_pages)

            decisoes = result.get("decisoes", []) or []
            for doc in decisoes:
                if count >= sample_limit:
                    break
                yield doc
                count += 1

            if count >= sample_limit or sample:
                continue

            # Paginate remaining pages
            for page in range(1, total_pages):
                if count >= sample_limit:
                    break

                result = self._graphql({
                    "decisaoFilter": filt,
                    "pageNumber": page,
                    "itemsPerPage": PAGE_SIZE,
                })
                if not result:
                    break

                decisoes = result.get("decisoes", []) or []
                if not decisoes:
                    break

                for doc in decisoes:
                    if count >= sample_limit:
                        break
                    yield doc
                    count += 1

                if page % 100 == 0:
                    logger.info("Segment %s: page %d/%d, %d records so far",
                                label, page, total_pages, count)

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch decisions published since a date."""
        since_str = since if isinstance(since, str) else since.isoformat()[:10]
        since_iso = f"{since_str}T00:00:00Z"

        for sg, tr, ta, tdm, label in SEGMENTS:
            logger.info("Fetching updates for segment %s since %s", label, since_str)
            filt = self._build_filter(sg, tr, ta, tdm, date_from=since_iso)

            page = 0
            while True:
                result = self._graphql({
                    "decisaoFilter": filt,
                    "pageNumber": page,
                    "itemsPerPage": PAGE_SIZE,
                })
                if not result:
                    break

                decisoes = result.get("decisoes", []) or []
                if not decisoes:
                    break

                for doc in decisoes:
                    yield doc

                page += 1


def main():
    scraper = TJBAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity...")
        filt = scraper._build_filter(True, False, True, False)
        result = scraper._graphql({
            "decisaoFilter": filt,
            "pageNumber": 0,
            "itemsPerPage": 1,
        })
        if result:
            total = result.get("itemCount", 0)
            logger.info("GraphQL API OK — %d 2G acordãos", total)
        else:
            logger.error("API test failed")
            sys.exit(1)
        return

    if command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample)
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2))

    elif command == "update":
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "2025-01-01"
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d records since %s", count, since)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
