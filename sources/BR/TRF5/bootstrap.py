#!/usr/bin/env python3
"""
BR/TRF5 -- Federal Regional Court 5th Region (Tribunal Regional Federal da 5ª Região)

Fetches court decisions from TRF5's Julia Pesquisa REST API.
TRF5 covers Northeastern states: AL, CE, PB, PE, RN, SE.

Endpoints:
  - Search: GET /julia-pesquisa/api/v1/documento:dt/{origem}?draw=1&start=0&length=100&...
    Returns JSON with full text of decisions.

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

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.TRF5")

SOURCE_ID = "BR/TRF5"
SAMPLE_DIR = Path(__file__).parent / "sample"
BASE_URL = "https://juliapesquisa.trf5.jus.br/julia-pesquisa"
SEARCH_URL = f"{BASE_URL}/api/v1/documento:dt"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json",
}

DELAY = 2.0
PAGE_SIZE = 100

# Origins to fetch from (G2 = appellate, G1 = first instance, TR_XX = turmas recursais)
ORIGINS = ["G2", "G1", "TR_AL", "TR_CE", "TR_PB", "TR_PE", "TR_RN", "TR_SE"]

RE_PROCESS_NUM = re.compile(r'(\d{7}-\d{2}\.\d{4}\.\d\.\d{2}\.\d{4})')


def clean_text(text: str) -> str:
    """Clean text of HTML tags and excessive whitespace."""
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


def format_process_number(raw: str) -> str:
    """Format a 20-digit raw process number into CNJ standard format."""
    raw = re.sub(r'[^0-9]', '', raw)
    if len(raw) == 20:
        return f"{raw[0:7]}-{raw[7:9]}.{raw[9:13]}.{raw[13]}.{raw[14:16]}.{raw[16:20]}"
    return raw


class TRF5Scraper(BaseScraper):
    """Scraper for BR/TRF5 -- Federal Regional Court 5th Region decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _search_page(self, origin: str, start: int = 0,
                     date_ini: str = "", date_fim: str = "",
                     query: str = "") -> Optional[dict]:
        """Fetch a page of search results from Julia Pesquisa API."""
        url = f"{SEARCH_URL}/{origin}"
        params = {
            "draw": "1",
            "start": str(start),
            "length": str(PAGE_SIZE),
            "pesquisaLivre": query,
            "numeroProcesso": "",
            "orgaoJulgador": "",
            "relator": "",
            "dataIni": date_ini,
            "dataFim": date_fim,
        }
        for attempt in range(3):
            try:
                time.sleep(DELAY)
                resp = self.session.get(url, params=params, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 5 * (attempt + 1)
                logger.warning("Search attempt %d failed: %s. Retry in %ds",
                               attempt + 1, e, wait)
                time.sleep(wait)
            except Exception as e:
                logger.error("Search failed for %s start=%d: %s", origin, start, e)
                return None
        return None

    def normalize(self, doc: dict) -> dict:
        """Transform a Julia Pesquisa record into the standard schema."""
        raw_proc = doc.get("numeroProcesso", "")
        proc = format_process_number(raw_proc)
        text = clean_text(doc.get("texto", ""))
        classe = doc.get("classeJudicial", "")
        relator = doc.get("relator", "")
        orgao = doc.get("orgaoJulgador", "")

        title = f"{classe} - {proc}" if classe and proc else f"TRF5 {proc}"

        date = doc.get("dataJulgamento") or doc.get("dataAssinatura")

        safe_proc = re.sub(r'[^0-9]', '', proc)
        code = doc.get("codigoDocumento", "")
        doc_id = f"BR-TRF5-{safe_proc}" if safe_proc else f"BR-TRF5-{code[:30]}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": f"{BASE_URL}",
            "language": "pt",
            "process_number": proc,
            "classe": classe,
            "court": "TRF5",
            "relator": relator,
            "orgao_julgador": orgao,
            "ementa": text[:500] if text else "",
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch TRF5 decisions across all origins."""
        sample_limit = 15 if sample else 999999
        count = 0
        seen = set()

        origins = ["G2"] if sample else ORIGINS

        for origin in origins:
            if count >= sample_limit:
                break

            logger.info("Fetching from origin: %s", origin)
            result = self._search_page(origin, start=0)
            if not result:
                logger.warning("No results from origin %s", origin)
                continue

            total = result.get("recordsTotal", 0)
            logger.info("Origin %s: %d total records", origin, total)

            data = result.get("data", [])
            start = 0

            while data and count < sample_limit:
                for doc in data:
                    if count >= sample_limit:
                        break

                    proc = doc.get("numeroProcesso", "")
                    if proc in seen:
                        continue
                    seen.add(proc)

                    text = doc.get("texto", "")
                    if not text or len(clean_text(text)) < 50:
                        continue

                    record = self.normalize(doc)
                    yield record
                    count += 1

                    if count % 50 == 0:
                        logger.info("Fetched %d records...", count)

                start += PAGE_SIZE
                if start >= min(total, 10000):
                    break

                result = self._search_page(origin, start=start)
                if not result:
                    break
                data = result.get("data", [])

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recent decisions."""
        logger.info("Fetching recent TRF5 decisions since %s", since)
        yield from self.fetch_all()


def main():
    scraper = TRF5Scraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to TRF5 Julia Pesquisa API...")
        result = scraper._search_page("G2", start=0)
        if not result:
            logger.error("Search test FAILED")
            sys.exit(1)

        total = result.get("recordsTotal", 0)
        data = result.get("data", [])
        logger.info("TRF5 OK — %d total records, %d on first page", total, len(data))

        if data:
            doc = data[0]
            text = clean_text(doc.get("texto", ""))
            logger.info("Full text OK — %d chars for proc=%s",
                        len(text), doc.get("numeroProcesso", "?"))
            logger.info("Preview: %.200s", text[:200])
        return

    if command in ("bootstrap", "bootstrap-fast"):
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        try:
            for record in scraper.fetch_all(sample=sample):
                count += 1
                safe_id = record["_id"].replace("/", "_")[:100]
                fname = SAMPLE_DIR / f"{safe_id}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                if count % 50 == 0:
                    logger.info("Saved %d records...", count)
        except (KeyboardInterrupt, SystemExit):
            logger.warning("Interrupted after %d records", count)
        except Exception as e:
            logger.error("Crashed after %d records: %s", count, e)
        logger.info("Bootstrap complete: %d records saved to %s", count, SAMPLE_DIR)
        sys.exit(0)  # partial data is still valid

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
