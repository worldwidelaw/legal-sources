#!/usr/bin/env python3
"""
Chilean Constitutional Court (Tribunal Constitucional)

Fetches constitutional decisions from the TC Chile backend API.
Full text available as OCR content via the sentenciaByID endpoint.

Data source: https://buscador.tcchile.cl
License: Public Domain
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional

SOURCE_ID = "CL/TribunalConstitucional"
BASE_URL = "https://buscador-backend.tcchile.cl/api"
RATE_LIMIT = 1.0

# Common Spanish word to retrieve all decisions (API requires non-empty search)
ENUMERATE_TERM = "que"


def api_get(endpoint: str, params: dict = None, timeout: int = 30) -> Optional[dict]:
    """Make a GET request to the TC Chile API."""
    url = f"{BASE_URL}{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        'Accept': 'application/json',
        'User-Agent': 'Legal Data Hunter/1.0 (Legal Research)',
    })
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        print(f"  API error {e.code} for {endpoint}")
        return None
    except Exception as e:
        print(f"  Request error: {e}")
        return None


def get_all_ids() -> List[int]:
    """Get all sentencia IDs by searching for a common word."""
    filter_json = json.dumps({'search': ENUMERATE_TERM, 'page': 1, 'per_page': 1})
    result = api_get('/extended/sentencias', {'filter': filter_json})
    if not result:
        return []
    all_ids = result.get('data', {}).get('all', [])
    total = result.get('meta', {}).get('total', 0)
    print(f"  Total sentencias: {total}, IDs retrieved: {len(all_ids)}")
    return all_ids


def fetch_by_id(sentencia_id: int) -> Optional[dict]:
    """Fetch a single sentencia by its internal ID."""
    filter_json = json.dumps({'id': sentencia_id, 'search': ENUMERATE_TERM})
    result = api_get('/extended/sentenciaByID', {'filter': filter_json}, timeout=60)
    if not result:
        return None
    results = result.get('data', {}).get('results', [])
    if not results:
        return None
    return results[0]


def clean_ocr_text(text: str) -> str:
    """Clean OCR artifacts from extracted text."""
    if not text:
        return ""
    # Remove page number artifacts (e.g., "0000033\nTREINTA Y TRES\n")
    text = re.sub(r'\d{7}\n[A-ZÁÉÍÓÚÑ\s]+\n+', '', text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def normalize(raw: dict) -> dict:
    """Transform raw sentencia data into standard schema."""
    rol = str(raw.get('rol', '') or raw.get('title', ''))
    sentence_id = raw.get('sentence_id') or raw.get('id', '')
    doc_id = f"CL_TC_{rol}"

    content = raw.get('content', '')
    text = clean_ocr_text(content)

    # Extract date from 'created' field
    created = raw.get('created') or raw.get('created_date', '')
    date_str = created[:10] if created else None

    # Build title
    competencia = raw.get('competencia', '') or raw.get('competenciaShortName', '')
    title = f"Rol {rol}"
    if competencia:
        title += f" — {competencia}"

    url = f"https://buscador.tcchile.cl/sentencia/{rol}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_str,
        "url": url,
        "rol": rol,
        "competencia": competencia,
        "page_count": raw.get('page_count'),
    }


def fetch_all() -> Iterator[dict]:
    """Fetch all available sentencias with full text."""
    print("Getting all sentencia IDs...")
    all_ids = get_all_ids()
    if not all_ids:
        print("ERROR: No IDs retrieved")
        return

    total = len(all_ids)
    fetched = 0
    skipped = 0

    for i, sid in enumerate(all_ids):
        if i % 50 == 0:
            print(f"  Progress: {i}/{total} (fetched={fetched}, skipped={skipped})")

        raw = fetch_by_id(sid)
        if not raw:
            skipped += 1
            time.sleep(RATE_LIMIT)
            continue

        record = normalize(raw)
        if len(record.get('text', '')) >= 100:
            yield record
            fetched += 1
        else:
            skipped += 1

        time.sleep(RATE_LIMIT)

    print(f"\nDone: {fetched} fetched, {skipped} skipped out of {total}")


def fetch_updates(since: datetime) -> Iterator[dict]:
    """Fetch sentencias modified since a given date."""
    all_ids = get_all_ids()
    since_str = since.isoformat()[:10]

    for sid in all_ids:
        raw = fetch_by_id(sid)
        if not raw:
            time.sleep(RATE_LIMIT)
            continue

        modified = raw.get('modified', '') or ''
        created = raw.get('created', '') or ''
        if modified[:10] >= since_str or created[:10] >= since_str:
            record = normalize(raw)
            if len(record.get('text', '')) >= 100:
                yield record

        time.sleep(RATE_LIMIT)


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    print("Getting all sentencia IDs...")
    all_ids = get_all_ids()
    if not all_ids:
        print("ERROR: No IDs retrieved")
        return

    # Sample evenly from the list (IDs are sorted by recency)
    total = len(all_ids)
    step = max(1, total // count)
    sample_ids = [all_ids[i * step] for i in range(min(count, total))]

    records_saved = 0
    total_text_chars = 0

    for sid in sample_ids:
        if records_saved >= count:
            break

        print(f"\n  Fetching sentencia ID {sid}...")
        raw = fetch_by_id(sid)
        if not raw:
            print(f"    Not found, skipping")
            time.sleep(RATE_LIMIT)
            continue

        record = normalize(raw)
        text_len = len(record.get('text', ''))

        if text_len < 100:
            print(f"    Text too short ({text_len} chars), skipping")
            time.sleep(RATE_LIMIT)
            continue

        total_text_chars += text_len

        filename = f"record_{records_saved:04d}.json"
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    Saved: {filename}")
        print(f"    Rol: {record.get('rol', '')}")
        print(f"    Competencia: {record.get('competencia', '')[:60]}")
        print(f"    Date: {record.get('date', '')}")
        print(f"    Text: {text_len:,} chars")
        records_saved += 1

        time.sleep(RATE_LIMIT)

    # Print summary
    print("\n" + "=" * 60)
    print("SAMPLE SUMMARY")
    print("=" * 60)
    print(f"Records saved: {records_saved}")
    if records_saved > 0:
        avg_chars = total_text_chars // records_saved
        print(f"Total text chars: {total_text_chars:,}")
        print(f"Average text length: {avg_chars:,} chars/doc")
    print(f"Sample directory: {sample_dir}")

    if records_saved >= 10:
        print("\n✓ SUCCESS: 10+ sample records with full text")
    else:
        print(f"\n✗ WARNING: Only {records_saved} records saved (need 10+)")


def main():
    parser = argparse.ArgumentParser(
        description="Chilean Constitutional Court Fetcher"
    )
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Running full bootstrap...")
            records_saved = 0
            for record in fetch_all():
                safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', record['_id'])[:100]
                filepath = sample_dir / f"{safe_id}.json"
                filepath.parent.mkdir(parents=True, exist_ok=True)
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                records_saved += 1
                if records_saved % 100 == 0:
                    print(f"  Saved {records_saved} records...")
            print(f"\nFull bootstrap complete: {records_saved} records saved")

    elif args.command == "fetch":
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("ERROR: --since required for updates command")
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
