#!/usr/bin/env python3
"""
MX/SCJNDatosAbiertosAPI -- Mexico Supreme Court Open Data JSON API

Fetches court decisions from the SCJN (Suprema Corte de Justicia de la Nación)
open data platform via its public REST API.

Strategy:
  - Search via POST /api/v1/bj/busqueda with index and pagination
  - Fetch full text via GET /api/v1/bj/documento/{index}/{id}
  - Covers: sentencias_pub (103K), tesis (311K), ejecutorias (22K)

API:
  - Base: https://bj.scjn.gob.mx/api/v1/bj
  - Search: POST /busqueda {q, indice, page, size, filtros}
  - Detail: GET /documento/{index}/{id}
  - No auth required, no rate limits detected

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MX.scjn-api")

BASE_URL = "https://bj.scjn.gob.mx"
API_BASE = "/api/v1/bj"

# Indices to harvest (public, with full text available).
#
# `date_field` is the field the index sorts newest-first on, and is what the
# incremental refresh walks. It differs per index and is NOT interchangeable:
# `tesis` carries no resolution date at all, only the weekly-gazette publication
# stamp, while the other two carry `fechaResolucion` in two different formats
# (`sentencias_pub` DD/MM/YYYY, `ejecutorias` ISO-8601).
INDICES = [
    {"name": "sentencias_pub", "id_field": "idEngrose", "label": "Sentencias",
     "date_field": "fechaResolucion"},
    {"name": "tesis", "id_field": "registroDigital", "label": "Tesis",
     "date_field": "fechaPublicacionSemanario"},
    {"name": "ejecutorias", "id_field": "registroDigital", "label": "Ejecutorias",
     "date_field": "fechaResolucion"},
]

# Date fields seen across the three indices, most specific first. `tesis` has
# none of the "fecha*" names the other indices use — omitting
# `fechaPublicacionSemanario` left every tesis record with a null date, and left
# any date-cutoff refresh with nothing to compare against.
DATE_FIELDS = [
    "fechaPublicacion",
    "fechaResolucion",
    "fechaPublicacionSemanario",
    "fecha",
    "fechaSentencia",
]

# The API's "no date" sentinel, returned as a real-looking timestamp. Treated as
# a null date rather than year 1, which would otherwise sort ahead of everything
# and read as "older than any cutoff".
NULL_DATE_PREFIX = "01/01/0001"

# How many consecutive listing entries older than the cutoff to tolerate before
# concluding the newest-first walk has passed the boundary. Sorting is by date
# only, so same-day ties can interleave; a whole page of margin is cheap
# (listing pages cost one request, document details cost one each).
STALE_RUN_LIMIT = 100

# For sample mode, fetch from each index
SAMPLE_PER_INDEX = 4


class SCJNScraper(BaseScraper):
    """
    Scraper for MX/SCJNDatosAbiertosAPI -- Mexico Supreme Court JSON API.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=60,
        )

        # Checkpoint: last fully-processed page per index. A full corpus pull
        # (~440K docs, one detail fetch each) exceeds the 100h fleet wall clock
        # (issue #1099 exit 124), so we persist progress and skip completed
        # pages with no network calls on restart, letting reruns advance
        # monotonically to completion.
        self._checkpoint_path = self.source_dir / "data" / "scjn_checkpoint.json"
        self._done_pages: Dict[str, int] = self._load_checkpoint()

    def _load_checkpoint(self) -> Dict[str, int]:
        """Load the highest fully-processed page number per index."""
        try:
            with open(self._checkpoint_path) as f:
                data = json.load(f)
            pages = {k: int(v) for k, v in data.get("done_pages", {}).items()}
            if pages:
                logger.info(f"Resuming from checkpoint: {pages}")
            return pages
        except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
            return {}

    def _save_checkpoint(self) -> None:
        """Persist the highest fully-processed page number per index."""
        try:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._checkpoint_path, "w") as f:
                json.dump({"done_pages": self._done_pages}, f)
        except OSError as e:
            logger.warning(f"Could not write checkpoint: {e}")

    def _sort_field(self, index: str) -> str:
        """The index's unique id field, used as a deterministic sort key."""
        for info in INDICES:
            if info["name"] == index:
                return info["id_field"]
        return ""

    def _search(self, index: str, page: int = 1, size: int = 50, q: str = "*",
                sort_field: Optional[str] = None,
                sort_direction: str = "asc") -> Optional[Dict]:
        """Search an index with pagination.

        Sorts ascending by the index's unique id field so pagination is a
        deterministic total order. Without an explicit sort the API falls back
        to relevance/internal order, which is unstable across the multi-day,
        multi-run crawl (the live index gains documents) → the same records
        reappear on different pages while others are never seen, which is the
        root of the #1099 under-write (337,636 fetched, 349 unique).

        `sort_field`/`sort_direction` override that for the incremental refresh,
        which walks the index's date field newest-first instead.
        """
        self.rate_limiter.wait()
        try:
            body = {
                "q": q,
                "indice": index,
                "page": page,
                "size": size,
                "filtros": {},
                "semantica": 0,
                "sortField": sort_field or self._sort_field(index),
                "sortDireccion": sort_direction,
            }
            resp = self.client.post(
                f"{API_BASE}/busqueda",
                json_data=body,
            )

            if resp.status_code != 200:
                logger.warning(f"Search {index} page {page}: HTTP {resp.status_code}")
                return None

            return resp.json()

        except Exception as e:
            logger.warning(f"Search error {index} page {page}: {e}")
            return None

    def _fetch_document(self, index: str, doc_id: str) -> Optional[Dict]:
        """Fetch full document detail by index and ID."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(f"{API_BASE}/documento/{index}/{doc_id}")

            if resp.status_code != 200:
                logger.debug(f"Document {index}/{doc_id}: HTTP {resp.status_code}")
                return None

            return resp.json()

        except Exception as e:
            logger.warning(f"Error fetching {index}/{doc_id}: {e}")
            return None

    def _extract_sentencia_text(self, doc: Dict) -> str:
        """Extract full text from a sentencia document."""
        parts = []
        for section in ["preambulo", "resultando", "considerando", "resuelve", "firman", "puntosResolutivos"]:
            content = doc.get(section, [])
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict):
                        text = item.get("parrafo", "")
                        if text:
                            parts.append(text)
                    elif isinstance(item, str):
                        parts.append(item)
            elif isinstance(content, str) and content:
                parts.append(content)
        return "\n\n".join(parts)

    def _extract_tesis_text(self, doc: Dict) -> str:
        """Extract full text from a tesis document."""
        texto = doc.get("texto", {})
        if isinstance(texto, dict):
            return texto.get("contenido", "")
        elif isinstance(texto, str):
            return texto
        return ""

    def _extract_ejecutoria_text(self, doc: Dict) -> str:
        """Extract full text from an ejecutoria document."""
        texto = doc.get("texto", "")
        if isinstance(texto, dict):
            return texto.get("contenido", texto.get("texto", ""))
        return str(texto) if texto else ""

    def _extract_text(self, index: str, doc: Dict) -> str:
        """Extract full text based on index type."""
        if index == "sentencias_pub":
            return self._extract_sentencia_text(doc)
        elif index == "tesis":
            return self._extract_tesis_text(doc)
        elif index == "ejecutorias":
            return self._extract_ejecutoria_text(doc)
        return ""

    def _extract_title(self, index: str, result: Dict, doc: Dict) -> str:
        """Extract document title from search result or detail."""
        if index == "sentencias_pub":
            return (
                result.get("asunto", "")
                or doc.get("asunto", "")
                or f"Sentencia {result.get('idEngrose', '')}"
            )
        elif index == "tesis":
            return (
                result.get("rubro", "")
                or doc.get("rubro", "")
                or f"Tesis {result.get('registroDigital', '')}"
            )
        elif index == "ejecutorias":
            return (
                result.get("rubro", "")
                or doc.get("rubro", "")
                or f"Ejecutoria {result.get('registroDigital', '')}"
            )
        return "Unknown"

    def _parse_date(self, val: Any) -> Optional[str]:
        """Normalise one raw date value to YYYY-MM-DD, or None."""
        if not val:
            return None
        if isinstance(val, str) and val.startswith(NULL_DATE_PREFIX):
            return None
        if isinstance(val, str):
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d",
                        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(val[:19], fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
            return None
        # Epoch milliseconds
        if isinstance(val, (int, float)):
            try:
                return datetime.fromtimestamp(val / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            except (ValueError, OSError, OverflowError):
                return None
        return None

    def _extract_date(self, result: Dict) -> Optional[str]:
        """Extract and format date from a search result."""
        for field in DATE_FIELDS:
            parsed = self._parse_date(result.get(field))
            if parsed:
                return parsed
        return None

    def _build_record(self, index: str, id_field: str, result: Dict) -> Optional[dict]:
        """Fetch a listing entry's detail and build the raw record, or None.

        Shared by `fetch_all` and `fetch_updates` so the two paths can never
        drift into emitting differently-shaped records.
        """
        doc_id = str(result.get(id_field, ""))
        if not doc_id:
            return None

        doc = self._fetch_document(index, doc_id)
        if not doc:
            return None

        full_text = self._extract_text(index, doc)
        if not full_text or len(full_text) < 50:
            return None

        return {
            "index": index,
            "doc_id": doc_id,
            "title": self._extract_title(index, result, doc),
            "full_text": full_text,
            "date": self._extract_date(result),
            "result_meta": result,
            "doc_meta": {k: v for k, v in doc.items()
                         if k not in ("preambulo", "resultando", "considerando",
                                      "resuelve", "firman", "puntosResolutivos",
                                      "texto")},
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all indices."""
        for idx_info in INDICES:
            index = idx_info["name"]
            id_field = idx_info["id_field"]
            label = idx_info["label"]

            # First search to get total
            first_page = self._search(index, page=1, size=50)
            if not first_page:
                logger.warning(f"Cannot access index {index}")
                continue

            total = first_page.get("total", 0)
            total_pages = first_page.get("totalPaginas", 0)
            logger.info(f"Index {index} ({label}): {total} documents, {total_pages} pages")

            resume_from = self._done_pages.get(index, 0)
            if resume_from:
                logger.info(f"Index {index}: skipping pages 1-{resume_from} (checkpoint)")

            page = 1
            fetched = 0

            while page <= total_pages:
                # Skip pages already fully processed in a previous run — no
                # network calls, so restarts advance monotonically.
                if page <= resume_from:
                    page += 1
                    continue

                if page == 1:
                    results = first_page.get("resultados", [])
                else:
                    search_result = self._search(index, page=page, size=50)
                    if not search_result:
                        page += 1
                        continue
                    results = search_result.get("resultados", [])

                if not results:
                    break

                for result in results:
                    record = self._build_record(index, id_field, result)
                    if record is None:
                        continue

                    yield record

                    fetched += 1
                    if fetched % 100 == 0:
                        logger.info(f"Index {index}: fetched {fetched}/{total}")

                # Page fully processed — record it and flush every 25 pages to
                # bound disk writes while keeping restart granularity tight.
                self._done_pages[index] = page
                if page % 25 == 0:
                    self._save_checkpoint()

                page += 1

            self._done_pages[index] = max(self._done_pages.get(index, 0), total_pages)
            self._save_checkpoint()
            logger.info(f"Index {index}: completed {fetched} documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield only documents dated on or after `since`.

        Each index is walked newest-first on its own date field and stopped once
        the listing has run past the cutoff, so a refresh costs a handful of
        listing pages instead of the whole corpus.

        This replaces a year-filter walk that never looked at `since` (#1502).
        Two things were wrong with it beyond ignoring the cutoff:

        * `filtros: {"anio": [...]}` is only honoured by `sentencias_pub`. On
          `tesis` and `ejecutorias` it matches nothing, so 334K of the 440K
          documents were silently excluded from every refresh.
        * it fetched the full detail of every document in a two-year window
          before it could tell whether any of them were new — one request per
          document, for documents already in the index.

        The cutoff is read off the *listing* entry, so a document older than
        `since` costs nothing but its share of a listing page.
        """
        cutoff = as_date_str(since)
        if not cutoff:
            logger.warning("No usable `since` cutoff — falling back to fetch_all")
            yield from self.fetch_all()
            return

        logger.info(f"Incremental refresh: documents dated >= {cutoff}")

        for idx_info in INDICES:
            index = idx_info["name"]
            id_field = idx_info["id_field"]
            date_field = idx_info["date_field"]

            page = 1
            emitted = 0
            scanned = 0
            stale_run = 0
            total_pages = None

            while total_pages is None or page <= total_pages:
                data = self._search(index, page=page, size=50,
                                    sort_field=date_field, sort_direction="desc")
                if not data:
                    logger.warning(f"Index {index}: search failed at page {page}, stopping")
                    break

                if total_pages is None:
                    total_pages = data.get("totalPaginas", 0)

                results = data.get("resultados", [])
                if not results:
                    break

                for result in results:
                    scanned += 1
                    doc_date = self._parse_date(result.get(date_field))

                    # A missing date can't be compared. Treat it as possibly-new
                    # rather than a boundary marker: skipping it would drop the
                    # document, and counting it as stale would end the walk on
                    # one unstamped entry.
                    if doc_date is not None and doc_date < cutoff:
                        stale_run += 1
                        continue

                    stale_run = 0
                    record = self._build_record(index, id_field, result)
                    if record is not None:
                        emitted += 1
                        yield record

                if stale_run >= STALE_RUN_LIMIT:
                    logger.info(
                        f"Index {index}: {stale_run} consecutive documents older "
                        f"than {cutoff} — reached the cutoff boundary"
                    )
                    break

                page += 1

            logger.info(
                f"Index {index}: {emitted} document(s) at or after {cutoff} "
                f"from {scanned} listing entries scanned"
            )

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        index = raw.get("index", "")
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", f"Document {doc_id}")

        return {
            "_id": f"MX-SCJN-{index}-{doc_id}",
            "_source": "MX/SCJNDatosAbiertosAPI",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": raw.get("date"),
            "url": f"{BASE_URL}/datos-abiertos/documento/{index}/{doc_id}",
            "index_type": index,
            "court": "Suprema Corte de Justicia de la Nación",
            "jurisdiction": "MX",
            "language": "es",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Mexico SCJN Open Data API...")

        for idx_info in INDICES:
            index = idx_info["name"]
            id_field = idx_info["id_field"]
            label = idx_info["label"]

            print(f"\n--- {label} ({index}) ---")
            result = self._search(index, page=1, size=3)
            if not result:
                print(f"  FAILED: No response")
                continue

            total = result.get("total", 0)
            print(f"  Total: {total:,} documents")

            results = result.get("resultados", [])
            if results:
                first = results[0]
                doc_id = str(first.get(id_field, ""))
                print(f"  First doc ID: {doc_id}")

                doc = self._fetch_document(index, doc_id)
                if doc:
                    text = self._extract_text(index, doc)
                    print(f"  Full text: {len(text)} chars")
                    if text:
                        print(f"  Sample: {text[:200]}...")
                else:
                    print(f"  FAILED: Could not fetch document detail")

        print("\nTest complete!")


def main():
    scraper = SCJNScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
