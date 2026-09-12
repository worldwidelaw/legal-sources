#!/usr/bin/env python3
"""
BR/TJDFT -- Tribunal de Justiça do Distrito Federal e dos Territórios

Fetches decisions from the public Elasticsearch-backed REST API.
No authentication required. Supports full pagination (no result cap).

API: POST https://jurisdf.tjdft.jus.br/api/v1/pesquisa
Response: {hits: {value: N}, registros: [...], paginacao: {...}}

Usage:
  python bootstrap.py bootstrap          # Full collection
  python bootstrap.py bootstrap --sample # 15 sample records
  python bootstrap.py test               # Connectivity test
  python bootstrap.py update             # Incremental (since last run)
"""

import sys
import json
import re
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.TJDFT")

API_URL = "https://jurisdf.tjdft.jus.br/api/v1/pesquisa"
PAGE_SIZE = 20

# Default keyword set — broad enough for full-text discovery.
# Override in config.yaml fetch.queries if you want domain-specific collection.
DEFAULT_QUERIES = [
    # Core jurisdiction keywords covering the full TJDFT docket
    "direito",
    "recurso",
    "apelacao",
    "mandado",
]

HEADERS = {
    "User-Agent": "legal-sources-bot/1.0 (research; github.com/worldwidelaw/legal-sources)",
    "Accept": "application/json",
    "Content-Type": "application/json",
    "Origin": "https://jurisdf.tjdft.jus.br",
    "Referer": "https://jurisdf.tjdft.jus.br/",
}


def _clean_html(text: str) -> str:
    """Remove <mark> tags and normalise whitespace from API response text."""
    if not text:
        return ""
    text = re.sub(r"</?mark>", "", text)
    text = text.replace("\xa0", " ")
    return " ".join(text.split())


def _parse_date(dt_str: str) -> Optional[str]:
    """Parse ISO 8601 datetime to YYYY-MM-DD, or return None."""
    if not dt_str:
        return None
    m = re.match(r"(\d{4}-\d{2}-\d{2})", str(dt_str))
    return m.group(1) if m else None


class TJDFTScraper(BaseScraper):
    """
    Scraper for the TJDFT public Elasticsearch REST API.

    The API supports arbitrary keyword queries with full pagination.
    Each page returns up to PAGE_SIZE (20) records with total hit count,
    allowing complete collection without any result-count ceiling.
    """

    def _post(self, query: str, page: int = 0) -> dict:
        """POST a single search request; returns parsed JSON response."""
        import urllib.request
        body = json.dumps({
            "query": query,
            "pagina": page,
            "tamanho": PAGE_SIZE,
            "espelho": False,
            "sinonimos": False,
        }, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(API_URL, data=body, headers=HEADERS, method="POST")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all TJDFT decisions across all configured queries.

        Queries are taken from config.yaml fetch.queries (if set) or DEFAULT_QUERIES.
        Pagination is automatic — no result-count cap.
        """
        queries = self.config.get("fetch", {}).get("queries", DEFAULT_QUERIES)
        seen_uuids: set = set()
        sleep_s = self.config.get("fetch", {}).get("rate_limit_seconds", 0.3)

        for query in queries:
            # First request to get total count
            try:
                r0 = self._post(query, page=0)
            except Exception as e:
                logger.warning("Query %r failed: %s", query, e)
                continue

            total = r0.get("hits", {}).get("value", 0)
            pages = (total + PAGE_SIZE - 1) // PAGE_SIZE
            logger.info("Query %r: %d results, %d pages", query, total, pages)

            # Yield first page
            for record in r0.get("registros", []):
                uid = record.get("uuid") or record.get("processo", "")
                if uid and uid not in seen_uuids:
                    seen_uuids.add(uid)
                    yield record

            # Subsequent pages
            for page in range(1, pages):
                time.sleep(sleep_s)
                try:
                    resp = self._post(query, page=page)
                    for record in resp.get("registros", []):
                        uid = record.get("uuid") or record.get("processo", "")
                        if uid and uid not in seen_uuids:
                            seen_uuids.add(uid)
                            yield record
                except Exception as e:
                    logger.warning("Page %d of query %r failed: %s", page, query, e)
                    time.sleep(2)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """
        Yield decisions published/judged after `since` (ISO 8601 date string).

        The TJDFT API has no native date-filter parameter, so we fetch all
        results for each query and filter client-side. For frequent incremental
        runs this is efficient because the API is fast (0.3s/page).
        """
        if since is None:
            since = self.status.get("last_run", "")

        since_dt = None
        if since:
            try:
                since_dt = datetime.fromisoformat(since.replace("Z", "+00:00")).date()
            except ValueError:
                pass

        for raw in self.fetch_all():
            date_str = _parse_date(raw.get("dataJulgamento") or raw.get("dataPublicacao", ""))
            if since_dt and date_str:
                try:
                    record_date = datetime.fromisoformat(date_str).date()
                    if record_date <= since_dt:
                        continue
                except ValueError:
                    pass
            yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform a raw TJDFT API record into the worldwidelaw standard schema.

        Returns None if the record lacks both ementa and decisao text
        (insufficient content for indexing).
        """
        uuid = raw.get("uuid", "")
        processo = raw.get("processo", "")

        marcadores = raw.get("marcadores", {})
        ementa_list = marcadores.get("ementa", [])
        ementa_raw = ementa_list[0] if ementa_list else raw.get("ementa", "")
        ementa = _clean_html(ementa_raw)
        decisao = _clean_html(raw.get("decisao", ""))

        text = ementa or decisao
        if not text:
            return None

        date_julgamento = _parse_date(raw.get("dataJulgamento", ""))
        date_publicacao = _parse_date(raw.get("dataPublicacao", ""))
        date = date_julgamento or date_publicacao

        orgao = raw.get("descricaoOrgaoJulgador", "")
        relator = raw.get("nomeRelator", "")
        base = raw.get("base", "")  # ACORDAOS, DECISOES, etc.
        identificador = raw.get("identificador", "")

        # Stable document ID: prefer UUID, fall back to process number hash
        doc_id = uuid or hashlib.sha256(processo.encode()).hexdigest()[:16]

        title = processo
        if orgao:
            title = f"{processo} – {orgao}"

        return {
            "_id": doc_id,
            "title": title,
            "text": text,
            "date": date,
            "process_number": processo,
            "judge_relator": relator,
            "orgao_julgador": orgao,
            "base": base,
            "identificador": identificador,
            "ementa": ementa,
            "decisao": decisao[:2000] if decisao else "",
            "url": f"https://jurisdf.tjdft.jus.br/#{identificador}" if identificador else "",
            "source": "BR/TJDFT",
            "country": "BR",
            "language": "pt",
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="BR/TJDFT scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")
    args = parser.parse_args()

    source_dir = Path(__file__).parent
    scraper = TJDFTScraper(str(source_dir))

    if args.command == "test":
        print("Testing TJDFT API connectivity...")
        try:
            r = scraper._post("agua", page=0)
            total = r.get("hits", {}).get("value", 0)
            print(f"OK — {total:,} results for query \'agua\'")
        except Exception as e:
            print(f"FAIL: {e}")
            sys.exit(1)

    elif args.command in ("bootstrap", "update"):
        sample_limit = 15 if args.sample else None
        generator = scraper.fetch_all() if args.command == "bootstrap" else scraper.fetch_updates()
        count = 0
        sample_dir = source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        for raw in generator:
            normalized = scraper.normalize(raw)
            if normalized is None:
                continue
            count += 1

            # Save first 15 as samples
            if count <= 15:
                safe_id = re.sub(r"[^\w-]", "_", normalized["_id"])[:40]
                with open(sample_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

            if sample_limit and count >= sample_limit:
                print(f"Sample mode: collected {count} records")
                break

            if count % 500 == 0:
                logger.info("Collected %d records so far...", count)

        print(f"Done: {count} records collected")
