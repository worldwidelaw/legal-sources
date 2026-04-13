#!/usr/bin/env python3
"""
PA/CorteSuprema -- Panama Supreme Court (Corte Suprema de Justicia)

Fetches Panamanian Supreme Court decisions from the DSpace digital repository
at repositoriodigital.organojudicial.gob.pa via REST API.

Collections covered:
  - FA. Pleno (Plenary decisions)
  - FB. Sala (Chamber decisions)
  - EA. Fallos sobre Inconstitucionalidad (Unconstitutionality rulings)
  - Compendio de Fallos 2021-2025 (Annual compilations)

Full text is pre-extracted by DSpace as TEXT bundle bitstreams (no PDF parsing needed).

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "PA/CorteSuprema"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PA.CorteSuprema")

BASE_URL = "https://repositoriodigital.organojudicial.gob.pa/rest"

# Case law collections (uuid -> name)
COLLECTIONS = {
    "3b62618c-4933-4f17-986e-5208db9e56de": "FA. Pleno",
    "6c086157-80d1-4841-9d15-a466cc95a681": "FB. Sala",
    "f8992a6b-881c-4b26-a40b-bd63ffe35031": "EA. Fallos sobre Inconstitucionalidad",
    "de99376f-b275-45f8-94b5-e4debb3c6f1e": "Compendio de Fallos 2021",
    "76d0ccb0-b136-4df9-a23a-e931445b6166": "Compendio de Fallos 2022",
    "c1600f9f-a68c-4def-bd00-a4614026f29f": "Compendio de Fallos 2023",
    "49762cf9-4b94-4384-88e2-4d9cbd1ab068": "Compendio de Fallos 2024",
    "94ec2637-5caa-4703-b165-233bd0371a11": "Compendio de Fallos 2025",
}

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research project)",
    "Accept": "application/json",
}

PAGE_SIZE = 50
REQUEST_DELAY = 1.0  # seconds between requests


def get_collection_items(collection_uuid: str, limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Paginate through all items in a DSpace collection."""
    offset = 0
    count = 0
    while True:
        url = f"{BASE_URL}/collections/{collection_uuid}/items"
        params = {
            "limit": PAGE_SIZE,
            "offset": offset,
            "expand": "bitstreams,metadata",
        }
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=60)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch items at offset {offset}: {e}")
            break

        items = resp.json()
        if not items:
            break

        for item in items:
            yield item
            count += 1
            if limit and count >= limit:
                return

        offset += PAGE_SIZE
        if len(items) < PAGE_SIZE:
            break
        time.sleep(REQUEST_DELAY)


def get_text_from_bitstreams(bitstreams: list) -> str:
    """Extract full text from TEXT bundle bitstream."""
    for bs in bitstreams:
        if bs.get("bundleName") == "TEXT" and bs.get("name", "").endswith(".txt"):
            bs_uuid = bs["uuid"]
            try:
                resp = requests.get(
                    f"{BASE_URL}/bitstreams/{bs_uuid}/retrieve",
                    headers={"User-Agent": HEADERS["User-Agent"]},
                    timeout=60,
                )
                resp.raise_for_status()
                return resp.content.decode("utf-8", errors="replace").strip()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Failed to retrieve text bitstream {bs_uuid}: {e}")
                return ""
    return ""


def get_metadata_value(metadata: list, key: str) -> Optional[str]:
    """Get first value for a metadata key."""
    for m in metadata:
        if m.get("key") == key:
            return m.get("value")
    return None


def get_metadata_values(metadata: list, key: str) -> list:
    """Get all values for a metadata key."""
    return [m.get("value") for m in metadata if m.get("key") == key]


def normalize(item: dict, collection_name: str, text: str) -> dict:
    """Transform DSpace item to standard schema."""
    metadata = item.get("metadata", [])
    handle = item.get("handle", "")
    title = get_metadata_value(metadata, "dc.title") or item.get("name", "")
    date_issued = get_metadata_value(metadata, "dc.date.issued")
    contributors = get_metadata_values(metadata, "dc.contributor.corporatename")
    magistrado = ""
    for c in contributors:
        if c and "corte suprema" not in c.lower() and "panamá" not in c.lower() and "órgano" not in c.lower():
            magistrado = c

    uri = get_metadata_value(metadata, "dc.identifier.uri") or ""
    if not uri and handle:
        uri = f"https://repositoriodigital.organojudicial.gob.pa/handle/{handle}"

    return {
        "_id": f"PA-CSJ-{handle.replace('/', '-')}" if handle else f"PA-CSJ-{item.get('uuid', '')}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_issued,
        "url": uri,
        "handle": handle,
        "collection": collection_name,
        "magistrado": magistrado,
        "language": get_metadata_value(metadata, "dc.language.iso") or "spa",
        "publisher": get_metadata_value(metadata, "dc.publisher") or "Órgano Judicial",
        "extent": get_metadata_value(metadata, "dc.format.extent") or "",
        "license": get_metadata_value(metadata, "dc.rights.license") or "",
    }


def fetch_all(limit: Optional[int] = None) -> Generator[dict, None, None]:
    """Fetch all case law items from all collections."""
    total = 0
    for col_uuid, col_name in COLLECTIONS.items():
        logger.info(f"Fetching collection: {col_name} ({col_uuid})")
        col_count = 0
        for item in get_collection_items(col_uuid, limit=limit):
            text = get_text_from_bitstreams(item.get("bitstreams", []))
            if not text:
                logger.debug(f"No text for item {item.get('handle', 'unknown')}, skipping")
                continue
            record = normalize(item, col_name, text)
            yield record
            col_count += 1
            total += 1
            time.sleep(REQUEST_DELAY)

            if limit and total >= limit:
                logger.info(f"Reached limit of {limit} records")
                return

        logger.info(f"  {col_name}: {col_count} records with text")


def test_api():
    """Test API connectivity and show collection stats."""
    logger.info("Testing DSpace REST API...")
    try:
        resp = requests.get(f"{BASE_URL}/collections", headers=HEADERS, timeout=30)
        resp.raise_for_status()
        collections = resp.json()
        logger.info(f"Found {len(collections)} total collections")

        total_items = 0
        for col_uuid, col_name in COLLECTIONS.items():
            for c in collections:
                if c.get("uuid") == col_uuid:
                    count = c.get("numberItems", 0)
                    total_items += count
                    logger.info(f"  {col_name}: {count} items")
                    break

        logger.info(f"Total case law items: {total_items}")

        # Test item fetch
        logger.info("\nTesting item fetch from FA. Pleno...")
        items_resp = requests.get(
            f"{BASE_URL}/collections/3b62618c-4933-4f17-986e-5208db9e56de/items",
            params={"limit": 1, "offset": 0, "expand": "bitstreams,metadata"},
            headers=HEADERS,
            timeout=30,
        )
        items_resp.raise_for_status()
        items = items_resp.json()
        if items:
            item = items[0]
            logger.info(f"  Sample item: {item.get('name')}")
            text = get_text_from_bitstreams(item.get("bitstreams", []))
            logger.info(f"  Text length: {len(text)} chars")
            if text:
                logger.info(f"  Text preview: {text[:200]}...")

    except Exception as e:
        logger.error(f"API test failed: {e}")
        sys.exit(1)


def bootstrap(sample: bool = False, full: bool = False):
    """Run bootstrap: fetch and save records."""
    limit = 15 if sample else None
    out_dir = SAMPLE_DIR if sample else SOURCE_DIR / "data"
    out_dir.mkdir(parents=True, exist_ok=True)

    records = []
    text_lengths = []

    for record in fetch_all(limit=limit):
        records.append(record)
        text_lengths.append(len(record.get("text", "")))

        # Save individual record
        safe_id = record["_id"].replace("/", "_").replace(" ", "_")
        filepath = out_dir / f"{safe_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        logger.info(f"Saved {record['_id']} ({len(record.get('text', '')):,} chars)")

    # Summary
    if records:
        avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0
        logger.info(f"\n{'='*60}")
        logger.info(f"Total records: {len(records)}")
        logger.info(f"Avg text length: {avg_text:,.0f} chars")
        logger.info(f"Min text length: {min(text_lengths):,} chars")
        logger.info(f"Max text length: {max(text_lengths):,} chars")
        logger.info(f"Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
        logger.info(f"Output directory: {out_dir}")
    else:
        logger.warning("No records fetched!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PA/CorteSuprema data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, full=args.full)
