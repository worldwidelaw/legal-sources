#!/usr/bin/env python3
"""
INTL/SUMMA - SUMMA Inter-American Case Law Fetcher (CEJIL)

Fetches Inter-American Court and Commission case law from SUMMA database.
Platform: Uwazi (HURIDOCS). No authentication required.

Data source: https://summa.cejil.org
API: Uwazi REST API (search, entities, per-page text extraction)
License: Public domain (inter-American human rights decisions)

Templates:
  - Sentencia de la CorteIDH (Court judgments): 506 docs
  - Resolución de la CorteIDH (Court resolutions): 1,329 docs
  - Resolución de Presidencia (Presidential resolutions): 585 docs
  - Voto Separado (Separate opinions): 588 docs

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test API connectivity
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

BASE_URL = "https://summa.cejil.org"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "INTL/SUMMA"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json",
}

# Uwazi template IDs for case law document types
TEMPLATES = {
    "sentencia": "58b2f3a35d59f31e1345b4ac",
    "resolucion": "58b2f3a35d59f31e1345b471",
    "resolucion_presidencia": "58b2f3a35d59f31e1345b482",
    "voto_separado": "58b2f3a35d59f31e1345b49f",
}

# For sample mode, fetch from these templates
SAMPLE_TEMPLATES = ["sentencia", "resolucion"]
SAMPLE_PER_TEMPLATE = 6

PAGE_SIZE = 50
RATE_LIMIT_DELAY = 1.5


def api_get(endpoint: str, params: dict = None) -> Optional[dict]:
    """Make a GET request to the Uwazi API."""
    url = f"{BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=120)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}")
        return None
    except ValueError as e:
        print(f"  Error parsing JSON from {url}: {e}")
        return None


def fetch_page_text(doc_id: str, page: int) -> Optional[str]:
    """Fetch the extracted text of a single page from a document."""
    url = f"{BASE_URL}/api/documents/page"
    params = {"_id": doc_id, "page": page}
    try:
        resp = requests.get(url, headers=HEADERS, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", "")
    except (requests.RequestException, ValueError) as e:
        print(f"    Error fetching page {page} of {doc_id}: {e}")
        return None


def fetch_full_text(entity: dict) -> str:
    """Extract full text from an entity by fetching all pages of its first document."""
    documents = entity.get("documents", [])
    if not documents:
        return ""

    doc = documents[0]
    doc_id = doc.get("_id", "")
    total_pages = doc.get("totalPages", 0)

    if not doc_id or not total_pages:
        return ""

    text_parts = []
    for page_num in range(1, total_pages + 1):
        page_text = fetch_page_text(doc_id, page_num)
        if page_text:
            text_parts.append(page_text.strip())
        time.sleep(0.3)  # Be gentle with per-page requests

    return "\n\n".join(text_parts)


def get_metadata_value(metadata: dict, key: str) -> Optional[str]:
    """Extract a simple string value from Uwazi metadata."""
    val = metadata.get(key)
    if not val or not isinstance(val, list) or len(val) == 0:
        return None
    first = val[0]
    if isinstance(first, dict):
        return first.get("label") or first.get("value")
    return str(first)


def get_metadata_date(metadata: dict, key: str) -> Optional[str]:
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


def normalize(entity: dict, full_text: str, doc_type: str) -> dict:
    """Normalize a SUMMA entity into standard schema."""
    metadata = entity.get("metadata", {})
    shared_id = entity.get("sharedId", "")

    case_number = get_metadata_value(metadata, "n_mero")
    country = get_metadata_value(metadata, "pa_s")
    court = get_metadata_value(metadata, "mecanismo")
    date = get_metadata_date(metadata, "fecha")
    judgment_type = get_metadata_value(metadata, "tipo")

    title = entity.get("title", "")
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
        "document_type": doc_type,
        "judgment_type": judgment_type,
        "language": entity.get("language", "es"),
    }


def fetch_entities(template_name: str, template_id: str, limit: int = 0) -> Generator[dict, None, None]:
    """Fetch all entities of a given template type with full text."""
    offset = 0
    fetched = 0
    page_size = min(PAGE_SIZE, limit) if limit > 0 else PAGE_SIZE

    while True:
        print(f"  Fetching {template_name} offset={offset}...")
        params = {
            "types": json.dumps([template_id]),
            "limit": page_size,
            "from": offset,
        }
        data = api_get("/api/search", params)
        if not data:
            break

        rows = data.get("rows", [])
        total = data.get("totalRows", 0)
        if not rows:
            break

        for entity in rows:
            title = entity.get("title", "unknown")
            print(f"    [{fetched+1}] {title[:60]}...")
            full_text = fetch_full_text(entity)
            if not full_text:
                print(f"      WARNING: No full text for {title}")
            record = normalize(entity, full_text, template_name)
            yield record
            fetched += 1
            if limit > 0 and fetched >= limit:
                return
            time.sleep(RATE_LIMIT_DELAY)

        offset += len(rows)
        if offset >= total:
            break

    print(f"  Done: {fetched} {template_name} records fetched.")


def bootstrap_sample():
    """Fetch sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    total = 0

    for template_name in SAMPLE_TEMPLATES:
        template_id = TEMPLATES[template_name]
        for record in fetch_entities(template_name, template_id, limit=SAMPLE_PER_TEMPLATE):
            fname = SAMPLE_DIR / f"{record['_id']}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            total += 1
            text_len = len(record.get("text", ""))
            print(f"      Saved {fname.name} ({text_len} chars text)")

    print(f"\nSample complete: {total} records saved to {SAMPLE_DIR}")
    validate_sample()


def bootstrap_full():
    """Fetch all records from all templates."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    total = 0

    for template_name, template_id in TEMPLATES.items():
        print(f"\n=== Fetching {template_name} ===")
        for record in fetch_entities(template_name, template_id):
            fname = SAMPLE_DIR / f"{record['_id']}.json"
            with open(fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            total += 1

    print(f"\nFull bootstrap complete: {total} records saved.")


def validate_sample():
    """Validate sample records meet quality requirements."""
    files = list(SAMPLE_DIR.glob("*.json"))
    if not files:
        print("FAIL: No sample files found")
        return False

    print(f"\nValidating {len(files)} sample records...")
    issues = []
    for f in files:
        with open(f, "r", encoding="utf-8") as fh:
            rec = json.load(fh)
        name = f.name
        if not rec.get("text"):
            issues.append(f"{name}: missing or empty 'text' field")
        elif len(rec["text"]) < 100:
            issues.append(f"{name}: text too short ({len(rec['text'])} chars)")
        if not rec.get("title"):
            issues.append(f"{name}: missing title")
        if not rec.get("date"):
            issues.append(f"{name}: missing date")
        if not rec.get("_id"):
            issues.append(f"{name}: missing _id")

    if issues:
        print("ISSUES FOUND:")
        for i in issues:
            print(f"  - {i}")
        return False
    else:
        print("ALL CHECKS PASSED")
        # Print summary
        for f in files[:3]:
            with open(f, "r", encoding="utf-8") as fh:
                rec = json.load(fh)
            print(f"  {rec['_id']}: {rec['title'][:50]}... ({len(rec.get('text',''))} chars)")
        return True


def test_connectivity():
    """Test API connectivity and report available data."""
    print("Testing SUMMA (Uwazi) API connectivity...\n")

    # Test search endpoint
    data = api_get("/api/search", {"limit": 0})
    if data:
        print(f"  Search API: OK (total entities: {data.get('totalRows', '?')})")
    else:
        print("  Search API: FAILED")
        return False

    # Test each template
    for name, tid in TEMPLATES.items():
        params = {"types": json.dumps([tid]), "limit": 0}
        data = api_get("/api/search", params)
        if data:
            print(f"  Template '{name}': {data.get('totalRows', '?')} entities")

    # Test page text extraction
    params = {"types": json.dumps([TEMPLATES["sentencia"]]), "limit": 1}
    data = api_get("/api/search", params)
    if data and data.get("rows"):
        entity = data["rows"][0]
        docs = entity.get("documents", [])
        if docs:
            doc_id = docs[0].get("_id", "")
            page_text = fetch_page_text(doc_id, 1)
            if page_text:
                print(f"\n  Full text extraction: OK ({len(page_text)} chars from page 1)")
            else:
                print("\n  Full text extraction: FAILED")
        else:
            print("\n  No documents array found on entity")
    print("\nConnectivity test complete.")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="INTL/SUMMA data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        success = test_connectivity()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        if args.sample:
            bootstrap_sample()
        else:
            bootstrap_full()
