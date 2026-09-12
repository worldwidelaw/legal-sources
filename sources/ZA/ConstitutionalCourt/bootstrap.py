#!/usr/bin/env python3
"""
ZA/ConstitutionalCourt -- South Africa Constitutional Court Judgments

Fetches full-text judgments from the official Constitutional Court DSpace
repository at collections.concourt.org.za via REST API.

Data access:
  - DSpace REST API at /rest/collections and /rest/items
  - Yearly collections (1994-2026) containing ~1,900 judgment items
  - Each item has metadata (title, case number, citation, judges, synopsis)
  - Pre-extracted text available as .pdf.txt bitstreams (no PDF parsing needed)
  - Full text retrieved via /rest/bitstreams/{id}/retrieve

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10-15 sample records
  python bootstrap.py update             # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZA.ConstitutionalCourt")

BASE_URL = "https://collections.concourt.org.za"
API_URL = BASE_URL + "/server/api"
DELAY = 1.5


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
        "Accept": "application/json",
    })
    return session


class ConCourtFetcher:
    SOURCE_ID = "ZA/ConstitutionalCourt"

    def __init__(self):
        self.session = get_session()

    def _get_json(self, url: str) -> Any:
        """GET a URL and return parsed JSON, with retries."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 429:
                    wait = 10 * (attempt + 1)
                    logger.warning("Rate limited, waiting %ds...", wait)
                    time.sleep(wait)
                    continue
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return None
            except requests.RequestException as e:
                logger.warning("Request error (attempt %d): %s", attempt + 1, e)
                time.sleep(5)
        return None

    def _get_text(self, url: str) -> Optional[str]:
        """GET a URL and return text content."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60, headers={"Accept": "text/plain"})
                if resp.status_code == 200:
                    return resp.text
                if resp.status_code == 429:
                    time.sleep(10 * (attempt + 1))
                    continue
                logger.warning("HTTP %d for text at %s", resp.status_code, url)
                return None
            except requests.RequestException as e:
                logger.warning("Text request error (attempt %d): %s", attempt + 1, e)
                time.sleep(5)
        return None

    def get_yearly_collections(self) -> List[Dict[str, Any]]:
        """Every yearly judgment collection (1994-present).

        The repository was upgraded to DSpace 9, whose REST API lives at
        /server/api and is paged; the DSpace 6 /rest/collections endpoint this
        used to call now 404s (issue #1315).
        """
        yearly, page = [], 0
        while True:
            data = self._get_json(
                f"{API_URL}/core/collections?size=100&page={page}"
            )
            if not data:
                break
            collections = (data.get("_embedded") or {}).get("collections") or []
            for collection in collections:
                if re.match(r"^\d{4}$", collection.get("name", "")):
                    yearly.append({
                        "id": collection["uuid"],
                        "name": collection["name"],
                        "numberItems": collection.get("archivedItemsCount", 0),
                    })
            page_info = data.get("page") or {}
            page += 1
            if page >= page_info.get("totalPages", 0):
                break
            time.sleep(DELAY)
        yearly.sort(key=lambda c: c["name"], reverse=True)
        return yearly

    @staticmethod
    def _flatten_metadata(metadata: Dict[str, Any]) -> List[Dict[str, str]]:
        """DSpace 9's `{key: [{value}, ...]}` as DSpace 6's `[{key, value}]`.

        Everything downstream of here (extract_metadata, normalize) already
        speaks the flat list, so the API change stops at this function.
        """
        flat = []
        for key, values in (metadata or {}).items():
            for entry in values or []:
                flat.append({"key": key, "value": entry.get("value", "")})
        return flat

    def get_collection_items(self, coll_id: str, offset: int = 0,
                             limit: int = 50) -> List[Dict[str, Any]]:
        """A page of items in a collection, in the legacy item shape."""
        page = offset // limit if limit else 0
        data = self._get_json(
            f"{API_URL}/discover/search/objects?dsoType=item"
            f"&scope={coll_id}&size={limit}&page={page}"
        )
        time.sleep(DELAY)
        if not data:
            return []
        search_result = (data.get("_embedded") or {}).get("searchResult") or {}
        objects = (search_result.get("_embedded") or {}).get("objects") or []
        items = []
        for wrapper in objects:
            item = (wrapper.get("_embedded") or {}).get("indexableObject")
            if not item:
                continue
            items.append({
                "id": item.get("uuid"),
                "uuid": item.get("uuid"),
                "name": item.get("name", ""),
                "handle": item.get("handle", ""),
                "lastModified": item.get("lastModified"),
                "metadata": self._flatten_metadata(item.get("metadata")),
            })
        return items

    def _text_bitstreams(self, item_uuid: str) -> List[Dict[str, Any]]:
        """The item's TEXT-bundle bitstreams (the .pdf.txt extractions).

        Only the TEXT bundle is walked: ORIGINAL holds the multi-megabyte PDFs
        and THUMBNAIL the page images, and neither is worth a request when the
        repository has already extracted the text alongside them.
        """
        bundles = self._get_json(f"{API_URL}/core/items/{item_uuid}/bundles")
        if not bundles:
            return []
        text_bundle = next(
            (
                bundle
                for bundle in (bundles.get("_embedded") or {}).get("bundles") or []
                if bundle.get("name", "").upper() == "TEXT"
            ),
            None,
        )
        if not text_bundle:
            return []
        time.sleep(DELAY)
        listing = self._get_json(
            f"{API_URL}/core/bundles/{text_bundle['uuid']}/bitstreams?size=200"
        )
        if not listing:
            return []
        bitstreams = []
        for bitstream in (listing.get("_embedded") or {}).get("bitstreams") or []:
            name = bitstream.get("name", "")
            bitstreams.append({
                "id": bitstream.get("uuid"),
                "name": name,
                # DSpace 9 leaves dc.format.mimetype off these records, and
                # everything in the TEXT bundle is a plain-text extraction.
                "mimeType": "text/plain",
                "sizeBytes": bitstream.get("sizeBytes", 0),
                "_content": ((bitstream.get("_links") or {}).get("content") or {}).get("href"),
            })
        return bitstreams

    def find_judgment_text(self, item: Dict[str, Any]) -> Optional[str]:
        """The judgment's full text, from the repository's own extraction.

        Strategy:
        1. Prefer a .pdf.txt bitstream named for the judgment or the order
        2. Fall back to the largest non-trivial .pdf.txt
        3. Download it from the bitstream's `content` link
        """
        bitstreams = item.get("bitstreams")
        if bitstreams is None:
            bitstreams = self._text_bitstreams(item.get("uuid") or item.get("id"))
            time.sleep(DELAY)
        txt_bitstreams = [
            b for b in bitstreams
            if b.get("name", "").endswith(".txt")
            and b.get("mimeType") == "text/plain"
            and b.get("name", "").lower() != "license.txt"
        ]
        if not txt_bitstreams:
            return None

        # Prefer bitstreams with "Judgment" or "Final" in the name
        judgment_bs = [
            b for b in txt_bitstreams
            if any(kw in b.get("name", "").lower()
                   for kw in ["judgment", "final judgment", "order"])
            and "media summary" not in b.get("name", "").lower()
        ]

        # Pick the best match, or largest .pdf.txt
        if judgment_bs:
            chosen = max(judgment_bs, key=lambda b: b.get("sizeBytes", 0))
        else:
            # Skip license.txt and very small files
            non_trivial = [b for b in txt_bitstreams if b.get("sizeBytes", 0) > 500]
            if not non_trivial:
                return None
            chosen = max(non_trivial, key=lambda b: b.get("sizeBytes", 0))

        content_url = chosen.get("_content")
        if not content_url and chosen.get("id"):
            content_url = f"{API_URL}/core/bitstreams/{chosen['id']}/content"
        if not content_url:
            return None

        text = self._get_text(content_url)
        time.sleep(DELAY)
        if text and len(text.strip()) > 100:
            return text.strip()
        return None

    def extract_metadata(self, item: Dict[str, Any]) -> Dict[str, str]:
        """Extract structured metadata from DSpace item metadata array."""
        meta = {}
        for m in item.get("metadata", []):
            key = m.get("key", "")
            val = m.get("value", "")
            if key == "dc.title":
                meta["title"] = val
            elif key == "dc.identifier.citation":
                meta["citation"] = val
            elif key == "dc.identifier.casenumber":
                meta["case_number"] = val
            elif key == "dc.date.issued":
                meta["date_issued"] = val
            elif key == "dc.date.judgment":
                meta["date_judgment"] = val
            elif key == "dc.contributor.judge":
                meta.setdefault("judges", [])
                meta["judges"].append(val)
            elif key == "dc.concourt.synopsis":
                meta["synopsis"] = val
            elif key == "dc.concourt.casehistory":
                meta["case_history"] = val
            elif key == "dc.identifier.uri":
                meta["uri"] = val
        return meta

    def normalize(self, item: Dict[str, Any], text: Optional[str] = None) -> Dict[str, Any]:
        """Normalize a DSpace item into the standard schema.

        Accepts either (item, text) or a single dict with `_text` key embedded
        (the yield-raw pattern for BaseScraper compatibility).
        """
        if text is None:
            text = item.get("_text", "") or ""
        meta = self.extract_metadata(item)
        title = meta.get("title", item.get("name", "Unknown"))
        case_number = meta.get("case_number", "")
        citation = meta.get("citation", "")

        # Build stable ID from citation or case number
        if citation:
            _id = re.sub(r"[^\w-]", "_", citation.strip("[]"))[:100]
        elif case_number:
            _id = re.sub(r"[^\w-]", "_", case_number)[:100]
        else:
            _id = re.sub(r"[^\w-]", "_", title.lower())[:100]

        # Parse date - prefer judgment date, fall back to issued date
        date_str = meta.get("date_judgment") or meta.get("date_issued", "")
        date = self._parse_date(date_str)

        uri = meta.get("uri", "")
        handle = item.get("handle", "")
        url = uri or (f"{BASE_URL}/handle/{handle}" if handle else "")

        judges_raw = meta.get("judges", [])
        judges = "; ".join(judges_raw) if isinstance(judges_raw, list) else str(judges_raw)

        return {
            "_id": _id,
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "language": "en",
            "case_number": case_number,
            "citation": citation,
            "judges": judges,
            "synopsis": meta.get("synopsis", ""),
            "case_history": meta.get("case_history", ""),
        }

    @staticmethod
    def _parse_date(s: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not s:
            return None
        # Try ISO format first (2025-02-13)
        m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
        if m:
            return m.group(1)
        # Try "13 February 2025" style
        try:
            for fmt in ("%d %B %Y", "%B %d, %Y", "%Y-%m-%dT%H:%M:%SZ"):
                try:
                    dt = datetime.strptime(s.strip().split("(")[0].strip(), fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue
        except Exception:
            pass
        return None

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Constitutional Court judgments with full text."""
        collections = self.get_yearly_collections()
        if not collections:
            logger.error("Failed to retrieve collections")
            return

        logger.info("Found %d yearly collections", len(collections))
        total = 0
        sample_limit = 15 if sample else None

        for coll in collections:
            coll_name = coll["name"]
            coll_id = coll["id"]
            coll_count = coll.get("numberItems", 0)
            logger.info("Processing collection %s (%d items)...", coll_name, coll_count)

            offset = 0
            while True:
                if sample_limit and total >= sample_limit:
                    break

                items = self.get_collection_items(coll_id, offset=offset, limit=50)
                if not items:
                    break

                for item in items:
                    if sample_limit and total >= sample_limit:
                        break

                    name = item.get("name", "?")
                    text = self.find_judgment_text(item)
                    if not text:
                        logger.debug("  No text found for: %s", name[:60])
                        continue

                    raw = dict(item)
                    raw["_text"] = text
                    total += 1
                    yield raw

                    if total % 25 == 0:
                        logger.info("  Progress: %d documents fetched", total)

                offset += len(items)
                if len(items) < 50:
                    break

            if sample_limit and total >= sample_limit:
                break

        logger.info("Fetch complete. Total documents: %d", total)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch judgments added since a given date (newest collections first)."""
        collections = self.get_yearly_collections()
        for coll in collections:
            # Skip collections from years before the since date
            try:
                coll_year = int(coll["name"])
                since_year = int(since[:4])
                if coll_year < since_year:
                    break
            except ValueError:
                continue

            offset = 0
            while True:
                items = self.get_collection_items(coll["id"], offset=offset, limit=50)
                if not items:
                    break
                for item in items:
                    meta = self.extract_metadata(item)
                    date_str = meta.get("date_judgment") or meta.get("date_issued", "")
                    date = self._parse_date(date_str)
                    if date and date < since:
                        continue
                    text = self.find_judgment_text(item)
                    if text:
                        raw = dict(item)
                        raw["_text"] = text
                        yield raw
                offset += len(items)
                if len(items) < 50:
                    break

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            yearly = self.get_yearly_collections()
            if not yearly:
                logger.error("Test failed: no collections returned")
                return False
            logger.info("Test passed: %d yearly collections found", len(yearly))

            # Test fetching one item with text
            if yearly:
                items = self.get_collection_items(yearly[0]["id"], limit=1)
                if items:
                    text = self.find_judgment_text(items[0])
                    logger.info("Text retrieval test: %s (%d chars)",
                                "OK" if text else "FAILED",
                                len(text) if text else 0)
            return True
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# === CLI entry point ===

def main():
    import argparse

    parser = argparse.ArgumentParser(description="ZA/ConstitutionalCourt bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "update", "test"]
    )
    parser.add_argument("--sample", action="store_true", help="Fetch only 10-15 sample records")
    parser.add_argument("--since", type=str, help="Date for incremental update (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    fetcher = ConCourtFetcher()

    if args.command == "test":
        success = fetcher.test()
        sys.exit(0 if success else 1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        # A sample run writes the curated sample/ files; a full run streams to
        # data/records.jsonl, which is what the pipeline ingests. The full path
        # used to write into sample/ too, so a completed crawl looked like a
        # 15-record sample fallback (the #798 class).
        sample_dir = Path(__file__).parent / "sample"
        data_dir = Path(__file__).parent / "data"
        records_path = data_dir / "records.jsonl"
        if args.sample:
            sample_dir.mkdir(exist_ok=True)
            stream = None
        else:
            data_dir.mkdir(exist_ok=True)
            stream = records_path.open("w", encoding="utf-8")

        count = 0
        try:
            for raw in fetcher.fetch_all(sample=args.sample):
                record = fetcher.normalize(raw)
                if stream is None:
                    safe_name = re.sub(r"[^\w\-.]", "_", str(record["_id"]))[:100]
                    (sample_dir / f"{safe_name}.json").write_text(
                        json.dumps(record, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                else:
                    stream.write(json.dumps(record, ensure_ascii=False) + "\n")
                    stream.flush()
                count += 1
                text_len = len(record.get("text", ""))
                logger.info(
                    "  [%d] %s | %s | text=%d chars",
                    count, record.get("date", "?"), record["title"][:60], text_len,
                )
        finally:
            if stream is not None:
                stream.close()

        destination = "sample/" if args.sample else str(records_path)
        logger.info("%s complete: %d records written to %s",
                    args.command, count, destination)
        sys.exit(0 if count >= (10 if args.sample else 1) else 1)

    if args.command == "update":
        since = args.since or "2026-01-01"
        count = 0
        for raw in fetcher.fetch_updates(since):
            record = fetcher.normalize(raw)
            count += 1
            logger.info("  [%d] %s: %s", count, record.get("date", "?"),
                        record["title"][:60])
        logger.info("Update complete: %d new records since %s", count, since)


if __name__ == "__main__":
    main()
