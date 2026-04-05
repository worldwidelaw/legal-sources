#!/usr/bin/env python3
"""
BR/TJDFT -- Federal District Court (Tribunal de Justiça do Distrito Federal e Territórios)

Fetches court decisions from TJDFT's JurisDF API.
3.3M+ decisions (acordãos + monocratic decisions) with full text via HTML.

API: POST https://jurisdf.tjdft.jus.br/api/v1/pesquisa
  - JSON body with query, pagina (0-indexed), tamanho (max 40)
  - retornaInteiroTeor: true returns full text HTML in inteiroTeorHtml field
  - No authentication required

Bases: acordaos (~1.7M), decisoes (~1.6M), informativo-jurisprudencia,
       jurisprudencia-foco, sumulas

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
logger = logging.getLogger("legal-data-hunter.BR.TJDFT")

SOURCE_ID = "BR/TJDFT"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"

API_URL = "https://jurisdf.tjdft.jus.br/api/v1/pesquisa"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

DELAY = 1.5
PAGE_SIZE = 40
BASES = ["acordaos", "decisoes"]


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


class TJDFTScraper(BaseScraper):
    """Scraper for BR/TJDFT -- Federal District Court decisions via JurisDF API."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _search(self, query: str = "*", page: int = 0, size: int = PAGE_SIZE,
                base: Optional[str] = None, full_text: bool = True) -> Optional[dict]:
        """Execute a search against the JurisDF API."""
        body = {
            "query": query,
            "pagina": page,
            "tamanho": size,
            "retornaInteiroTeor": full_text,
            "retornaTotalizacao": page == 0,
        }
        if base:
            body["termosAcessorios"] = [{"campo": "base", "valor": base}]

        for attempt in range(4):
            try:
                time.sleep(DELAY)
                resp = self.session.post(API_URL, json=body, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                wait = 3 * (attempt + 1)
                logger.warning("Attempt %d failed: %s. Retrying in %ds...", attempt + 1, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("API request failed: %s", e)
                return None
        logger.error("All retries exhausted")
        return None

    def normalize(self, doc: dict) -> dict:
        """Transform an API record into the standard schema."""
        uuid = doc.get("uuid", "")
        processo = doc.get("processo", "")
        base = doc.get("base", "")

        # Full text from inteiroTeorHtml
        raw_html = doc.get("inteiroTeorHtml", "")
        text = strip_html(raw_html)

        # Fallback to ementa + decisao if full text unavailable
        if len(text) < 50:
            ementa = doc.get("ementa", "")
            decisao = doc.get("decisao", "")
            parts = []
            if ementa:
                parts.append(f"EMENTA: {ementa}")
            if decisao:
                parts.append(f"DECISÃO: {decisao}")
            text = "\n\n".join(parts)

        # Date
        raw_date = doc.get("dataJulgamento", "")
        date = raw_date[:10] if raw_date else None

        # Title
        identificador = doc.get("identificador", "")
        orgao = doc.get("descricaoOrgaoJulgador", "")
        title = f"Acórdão {identificador}" if base == "acordaos" else f"Decisão {identificador}"
        if orgao:
            title += f" - {orgao}"

        return {
            "_id": f"BR-TJDFT-{uuid}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"https://jurisdf.tjdft.jus.br/",
            "language": "pt",
            "uuid": uuid,
            "process_number": processo,
            "base": base,
            "rapporteur": doc.get("nomeRelator", ""),
            "court_body": orgao,
            "publication_date": (doc.get("dataPublicacao", "") or "")[:10] or None,
            "cnj_class": doc.get("descricaoClasseCnj", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch all TJDFT decisions."""
        count = 0
        sample_limit = 15 if sample else 999999

        for base in BASES:
            if count >= sample_limit:
                break

            logger.info("=== Fetching base: %s ===", base)
            # First request to get total
            result = self._search(query="*", page=0, base=base, full_text=True)
            if not result:
                logger.error("Failed to fetch base %s", base)
                continue

            total = result.get("hits", {}).get("value", 0)
            logger.info("Base %s: %d total records", base, total)

            records = result.get("registros", [])
            for doc in records:
                if count >= sample_limit:
                    break
                record = self.normalize(doc)
                if len(record["text"]) < 20:
                    continue
                yield record
                count += 1

            if count >= sample_limit:
                break

            # Paginate through remaining pages
            max_pages = min((total + PAGE_SIZE - 1) // PAGE_SIZE, sample_limit // PAGE_SIZE + 1)
            if sample:
                max_pages = 1  # Already fetched page 0

            for page in range(1, max_pages):
                if count >= sample_limit:
                    break
                result = self._search(query="*", page=page, base=base, full_text=True)
                if not result:
                    break
                records = result.get("registros", [])
                if not records:
                    break
                for doc in records:
                    if count >= sample_limit:
                        break
                    record = self.normalize(doc)
                    if len(record["text"]) < 20:
                        continue
                    yield record
                    count += 1

                if page % 10 == 0:
                    logger.info("Base %s: page %d/%d, %d records so far", base, page, max_pages, count)

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch decisions published since a date."""
        for base in BASES:
            logger.info("Fetching updates for base %s since %s", base, since)
            page = 0
            while True:
                body = {
                    "query": "*",
                    "pagina": page,
                    "tamanho": PAGE_SIZE,
                    "retornaInteiroTeor": True,
                    "termosAcessorios": [
                        {"campo": "base", "valor": base},
                    ],
                }
                result = self._search(query="*", page=page, base=base, full_text=True)
                if not result:
                    break
                records = result.get("registros", [])
                if not records:
                    break

                yielded = 0
                for doc in records:
                    pub_date = (doc.get("dataPublicacao", "") or "")[:10]
                    if pub_date and pub_date >= since:
                        record = self.normalize(doc)
                        if len(record["text"]) >= 20:
                            yield record
                            yielded += 1
                    elif pub_date and pub_date < since:
                        return  # Results are sorted by date desc, so we can stop

                if yielded == 0:
                    break
                page += 1
                time.sleep(DELAY)


def main():
    scraper = TJDFTScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity...")
        result = scraper._search(query="*", page=0, size=1, full_text=False)
        if result:
            total = result.get("hits", {}).get("value", 0)
            logger.info("API OK — %d total records", total)
        else:
            logger.error("API test failed")
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
        since = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith("-") else "2025-01-01"
        count = sum(1 for _ in scraper.fetch_updates(since))
        logger.info("Update complete: %d records since %s", count, since)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
