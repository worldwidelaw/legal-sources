#!/usr/bin/env python3
"""
INTL/SUMMA - SUMMA Inter-American Case Law Fetcher (CEJIL)

Fetches Inter-American Court and Commission case law from SUMMA database.
Platform: Uwazi (HURIDOCS). No authentication required.

Data source: https://summa.cejil.org
API: Uwazi REST API (search, entities, per-page text extraction)
License: Public domain (inter-American human rights decisions)

Templates:
  - Sentencia de la CorteIDH (Court judgments): ~506 docs
  - Resolución de la CorteIDH (Court resolutions): ~1,329 docs
  - Resolución de Presidencia (Presidential resolutions): ~585 docs
  - Voto Separado (Separate opinions): ~588 docs

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py bootstrap-fast        # Full bootstrap (VPS-compatible)
  python bootstrap.py test                  # Test API connectivity
"""

import argparse
import json
import sys
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("INTL/SUMMA")

BASE_URL = "https://summa.cejil.org"
SOURCE_ID = "INTL/SUMMA"

# Uwazi template IDs for case law document types
TEMPLATES = {
    "sentencia": "58b2f3a35d59f31e1345b4ac",
    "resolucion": "58b2f3a35d59f31e1345b471",
    "resolucion_presidencia": "58b2f3a35d59f31e1345b482",
    "voto_separado": "58b2f3a35d59f31e1345b49f",
}

PAGE_SIZE = 50
RATE_LIMIT_DELAY = 1.0


class SUMMAScraper(BaseScraper):
    """Scraper for SUMMA Inter-American Case Law database."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
                "Accept": "application/json",
            },
            timeout=120,
            max_retries=3,
            backoff_factor=2.0,
        )

    def _api_get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Make a GET request to the Uwazi API."""
        try:
            return self.http.get_json(endpoint, params=params)
        except Exception as e:
            logger.error(f"API error for {endpoint}: {e}")
            return None

    def _fetch_page_text(self, doc_id: str, page: int) -> Optional[str]:
        """Fetch extracted text of a single page from a document."""
        data = self._api_get("/api/documents/page", {"_id": doc_id, "page": page})
        if data:
            return data.get("data", "")
        return None

    def _fetch_full_text(self, entity: dict) -> str:
        """Extract full text from an entity using concurrent page fetching."""
        documents = entity.get("documents", [])
        if not documents:
            return ""

        doc = documents[0]
        doc_id = doc.get("_id", "")
        total_pages = doc.get("totalPages", 0)

        if not doc_id or not total_pages:
            return ""

        # Fetch all pages concurrently (up to 5 at a time)
        text_parts = [None] * total_pages
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            for page_num in range(1, total_pages + 1):
                future = executor.submit(self._fetch_page_text, doc_id, page_num)
                futures[future] = page_num - 1  # 0-indexed position

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    page_text = future.result()
                    if page_text:
                        text_parts[idx] = page_text.strip()
                except Exception as e:
                    logger.debug(f"Page fetch error: {e}")

        return "\n\n".join(p for p in text_parts if p)

    def _get_metadata_value(self, metadata: dict, key: str) -> Optional[str]:
        """Extract a simple string value from Uwazi metadata."""
        val = metadata.get(key)
        if not val or not isinstance(val, list) or len(val) == 0:
            return None
        first = val[0]
        if isinstance(first, dict):
            return first.get("label") or first.get("value")
        return str(first)

    def _get_metadata_date(self, metadata: dict, key: str) -> Optional[str]:
        """Extract a date from Uwazi metadata (stored as epoch seconds)."""
        val = metadata.get(key)
        if not val or not isinstance(val, list) or len(val) == 0:
            return None
        first = val[0]
        if isinstance(first, dict):
            epoch = first.get("value")
        else:
            epoch = first
        if epoch and isinstance(epoch, (int, float)):
            return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%d")
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all case law entities from all templates."""
        for template_name, template_id in TEMPLATES.items():
            logger.info(f"Fetching template: {template_name}")
            offset = 0

            while True:
                params = {
                    "types": json.dumps([template_id]),
                    "limit": PAGE_SIZE,
                    "from": offset,
                }
                data = self._api_get("/api/search", params)
                if not data:
                    break

                rows = data.get("rows", [])
                total = data.get("totalRows", 0)
                if not rows:
                    break

                logger.info(f"  {template_name} offset={offset}, got {len(rows)}/{total}")

                for entity in rows:
                    entity["_template_name"] = template_name
                    yield entity
                    time.sleep(RATE_LIMIT_DELAY)

                offset += len(rows)
                if offset >= total:
                    break

            logger.info(f"  Done with {template_name}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield entities modified since the given date."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform a SUMMA entity into standard schema, including full text fetch."""
        metadata = raw.get("metadata", {})
        shared_id = raw.get("sharedId", "")
        template_name = raw.get("_template_name", "unknown")

        title = raw.get("title", "")
        logger.info(f"  Normalizing: {title[:60]}...")

        full_text = self._fetch_full_text(raw)
        if not full_text:
            logger.warning(f"  No full text for: {title}")

        case_number = self._get_metadata_value(metadata, "n_mero")
        country = self._get_metadata_value(metadata, "pa_s")
        court = self._get_metadata_value(metadata, "mecanismo")
        date = self._get_metadata_date(metadata, "fecha")
        judgment_type = self._get_metadata_value(metadata, "tipo")

        url = f"{BASE_URL}/en/entity/{shared_id}"

        return {
            "_id": f"SUMMA-{shared_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date,
            "url": url,
            "case_number": case_number,
            "country": country,
            "court": court,
            "document_type": template_name,
            "judgment_type": judgment_type,
            "language": raw.get("language", "es"),
        }


def main():
    parser = argparse.ArgumentParser(description="INTL/SUMMA data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    parser.add_argument("--workers", type=int, default=5, help="Parallel workers (bootstrap-fast)")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size (bootstrap-fast)")
    args = parser.parse_args()

    scraper = SUMMAScraper()

    if args.command == "test":
        logger.info("Testing SUMMA (Uwazi) API connectivity...")
        data = scraper._api_get("/api/search", {"limit": 0})
        if data:
            logger.info(f"Search API: OK (total entities: {data.get('totalRows', '?')})")
            for name, tid in TEMPLATES.items():
                params = {"types": json.dumps([tid]), "limit": 0}
                tdata = scraper._api_get("/api/search", params)
                if tdata:
                    logger.info(f"  Template '{name}': {tdata.get('totalRows', '?')} entities")
        else:
            logger.error("Search API: FAILED")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        logger.info(f"Bootstrap complete: {stats}")

    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast(
            max_workers=args.workers,
            batch_size=args.batch_size,
        )
        logger.info(f"Bootstrap-fast complete: {stats}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
