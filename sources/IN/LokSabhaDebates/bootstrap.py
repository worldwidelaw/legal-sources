#!/usr/bin/env python3
"""
IN/LokSabhaDebates -- Lok Sabha Debate Transcripts

Fetches debate transcripts from the Indian Parliament Digital Library
(elibrary.sansad.in) via the DSpace 7 REST API. Full text is obtained
from DSpace's pre-extracted TEXT bundles (Tika filter output).

Coverage: 1st–18th Lok Sabha (1952–present), ~6,650 debate sessions.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch debates from last 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.LokSabhaDebates")

API_BASE = "https://elibrary.sansad.in/server/api"
COLLECTION_UUID = "b2770639-c57b-4ba3-a9f1-57761c60d195"  # Lok Sabha Debates (Text)
PAGE_SIZE = 100
MIN_TEXT_CHARS = 200
REQUEST_TIMEOUT = 60
DELAY_BETWEEN_REQUESTS = 1.5


class LokSabhaDebatesScraper(BaseScraper):
    SOURCE_ID = "IN/LokSabhaDebates"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "LegalDataHunter/1.0 (research project)",
        })

    def _api_get(self, url: str, params: dict = None) -> dict:
        """Make a GET request to the DSpace 7 API with retry logic."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                if attempt < 2:
                    wait = (attempt + 1) * 5
                    logger.warning("API request failed (attempt %d/3): %s — retrying in %ds", attempt + 1, e, wait)
                    time.sleep(wait)
                else:
                    raise

    def _get_text_content(self, item_uuid: str) -> Optional[str]:
        """Fetch the pre-extracted text from the TEXT bundle of an item."""
        bundles_url = f"{API_BASE}/core/items/{item_uuid}/bundles"
        try:
            bundles_data = self._api_get(bundles_url)
        except Exception as e:
            logger.warning("Failed to fetch bundles for %s: %s", item_uuid, e)
            return None

        text_bundle_uuid = None
        for bundle in bundles_data.get("_embedded", {}).get("bundles", []):
            if bundle.get("name") == "TEXT":
                text_bundle_uuid = bundle.get("uuid")
                break

        if not text_bundle_uuid:
            logger.debug("No TEXT bundle for item %s", item_uuid)
            return None

        time.sleep(DELAY_BETWEEN_REQUESTS)

        bitstreams_url = f"{API_BASE}/core/bundles/{text_bundle_uuid}/bitstreams"
        try:
            bs_data = self._api_get(bitstreams_url)
        except Exception as e:
            logger.warning("Failed to fetch bitstreams for TEXT bundle %s: %s", text_bundle_uuid, e)
            return None

        bitstreams = bs_data.get("_embedded", {}).get("bitstreams", [])
        if not bitstreams:
            logger.debug("No bitstreams in TEXT bundle for item %s", item_uuid)
            return None

        text_bs_uuid = bitstreams[0].get("uuid")
        if not text_bs_uuid:
            return None

        time.sleep(DELAY_BETWEEN_REQUESTS)

        content_url = f"{API_BASE}/core/bitstreams/{text_bs_uuid}/content"
        try:
            resp = self.session.get(content_url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            text = resp.text.strip()
            if len(text) >= MIN_TEXT_CHARS:
                return text
            else:
                logger.debug("Text too short (%d chars) for item %s", len(text), item_uuid)
                return None
        except Exception as e:
            logger.warning("Failed to download text for item %s: %s", item_uuid, e)
            return None

    def _extract_metadata(self, item: dict) -> Dict[str, Any]:
        """Extract metadata fields from a DSpace item."""
        meta = item.get("metadata", {})

        def first_val(field: str) -> Optional[str]:
            values = meta.get(field, [])
            return values[0].get("value") if values else None

        date_str = first_val("dc.date.issued")
        handle = first_val("dc.identifier.uri") or (
            f"https://elibrary.sansad.in/handle/{item.get('handle', '')}"
            if item.get("handle") else None
        )

        return {
            "uuid": item.get("uuid"),
            "title": item.get("name", "Lok Sabha Debates"),
            "date": date_str,
            "lok_sabha_number": first_val("dc.identifier.loksabhanumber"),
            "session_number": first_val("dc.identifier.sessionnumber"),
            "handle": handle,
            "language": first_val("dc.language.iso") or "en",
        }

    def _build_title(self, meta: Dict[str, Any]) -> str:
        """Build a descriptive title from metadata."""
        parts = ["Lok Sabha Debates"]
        if meta.get("lok_sabha_number"):
            parts.append(f"({meta['lok_sabha_number']}th Lok Sabha")
            if meta.get("session_number"):
                parts[-1] += f", Session {meta['session_number']}"
            parts[-1] += ")"
        if meta.get("date"):
            parts.append(f"— {meta['date']}")
        return " ".join(parts)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into the standard schema."""
        return {
            "_id": raw["uuid"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("descriptive_title", raw.get("title", "Lok Sabha Debates")),
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("handle"),
            "lok_sabha_number": raw.get("lok_sabha_number"),
            "session_number": raw.get("session_number"),
            "language": raw.get("language", "en"),
        }

    def _search_page(self, page: int) -> dict:
        """Fetch one page of search results from the collection."""
        url = f"{API_BASE}/discover/search/objects"
        params = {
            "scope": COLLECTION_UUID,
            "size": PAGE_SIZE,
            "page": page,
            "sort": "dc.date.issued,DESC",
        }
        return self._api_get(url, params=params)

    def _items_from_search(self, data: dict):
        """Extract item objects from a search response."""
        objects = (
            data.get("_embedded", {})
            .get("searchResult", {})
            .get("_embedded", {})
            .get("objects", [])
        )
        for obj in objects:
            item = obj.get("_embedded", {}).get("indexableObject")
            if item:
                yield item

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Yield all debate records with full text."""
        max_records = 15 if sample else None
        count = 0
        page = 0

        # Get total count from first page
        first_page = self._search_page(0)
        total = (
            first_page.get("_embedded", {})
            .get("searchResult", {})
            .get("page", {})
            .get("totalElements", 0)
        )
        total_pages = (
            first_page.get("_embedded", {})
            .get("searchResult", {})
            .get("page", {})
            .get("totalPages", 0)
        )
        logger.info("Collection has %d items across %d pages", total, total_pages)

        # Process first page
        for item in self._items_from_search(first_page):
            if max_records and count >= max_records:
                return

            meta = self._extract_metadata(item)
            time.sleep(DELAY_BETWEEN_REQUESTS)

            text = self._get_text_content(meta["uuid"])
            if not text:
                logger.info("Skipping %s (date=%s) — no extractable text", meta["uuid"], meta.get("date"))
                continue

            meta["text"] = text
            meta["descriptive_title"] = self._build_title(meta)
            yield self.normalize(meta)
            count += 1
            logger.info("Fetched %d/%s: %s", count, max_records or total, meta.get("date", "unknown"))

        # Process remaining pages
        for page in range(1, total_pages):
            if max_records and count >= max_records:
                return

            time.sleep(DELAY_BETWEEN_REQUESTS)
            try:
                page_data = self._search_page(page)
            except Exception as e:
                logger.error("Failed to fetch page %d: %s", page, e)
                continue

            for item in self._items_from_search(page_data):
                if max_records and count >= max_records:
                    return

                meta = self._extract_metadata(item)
                time.sleep(DELAY_BETWEEN_REQUESTS)

                text = self._get_text_content(meta["uuid"])
                if not text:
                    logger.info("Skipping %s (date=%s) — no extractable text", meta["uuid"], meta.get("date"))
                    continue

                meta["text"] = text
                meta["descriptive_title"] = self._build_title(meta)
                yield self.normalize(meta)
                count += 1
                logger.info("Fetched %d/%s: %s", count, max_records or total, meta.get("date", "unknown"))

        logger.info("Completed: %d records fetched", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch debates issued since the given date."""
        since = as_date_str(since)  # update() passes a datetime; #1512
        since_date = datetime.fromisoformat(since).date() if isinstance(since, str) else since
        logger.info("Fetching debates since %s", since_date)

        for record in self.fetch_all():
            if record.get("date"):
                try:
                    rec_date = datetime.fromisoformat(record["date"]).date()
                    if rec_date < since_date:
                        logger.info("Reached records before %s, stopping", since_date)
                        return
                except ValueError:
                    pass
            yield record

    def test(self) -> bool:
        """Quick connectivity check."""
        try:
            data = self._search_page(0)
            total = (
                data.get("_embedded", {})
                .get("searchResult", {})
                .get("page", {})
                .get("totalElements", 0)
            )
            logger.info("Connection OK. Collection has %d items.", total)
            return total > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IN/LokSabhaDebates bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    parser.add_argument("--since", type=str, help="ISO date for incremental update")
    args = parser.parse_args()

    scraper = LokSabhaDebatesScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    if args.command == "update":
        since = args.since or (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
        records = scraper.fetch_updates(since)
    else:
        records = scraper.fetch_all(sample=args.sample)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)
    count = 0

    for record in records:
        count += 1
        out_file = sample_dir / f"{record['_id'][:12]}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info("Done. Saved %d records to %s", count, sample_dir)
    if count == 0:
        logger.error("No records fetched!")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
