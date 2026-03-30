#!/usr/bin/env python3
"""
Chilean Constitutional Court (Tribunal Constitucional)

Fetches constitutional decisions from the TC Chile backend API.
Full text available as OCR content via the paginated sentencias endpoint.

Data source: https://buscador.tcchile.cl
License: Public Domain
"""

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

SOURCE_ID = "CL/TribunalConstitucional"
BASE_URL = "https://buscador-backend.tcchile.cl/api"
RATE_LIMIT = 2.0
PAGE_SIZE = 10

# Common Spanish word to retrieve all decisions (API requires non-empty search)
ENUMERATE_TERM = "que"


def api_get(endpoint: str, params: dict = None, timeout: int = 60) -> Optional[dict]:
    """Make a GET request to the TC Chile API with retry on 429."""
    url = f"{BASE_URL}{endpoint}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={
        'Accept': 'application/json',
        'User-Agent': 'Legal Data Hunter/1.0 (Legal Research)',
    })
    for attempt in range(4):
        try:
            resp = urllib.request.urlopen(req, timeout=timeout)
            return json.loads(resp.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 3:
                backoff = (attempt + 1) * 5
                print(f"  429 rate limited, backing off {backoff}s (attempt {attempt+1}/4)")
                time.sleep(backoff)
                continue
            print(f"  API error {e.code} for {endpoint}")
            return None
        except Exception as e:
            if attempt < 3:
                print(f"  Request error (attempt {attempt+1}/4): {e}")
                time.sleep(2)
                continue
            print(f"  Request error: {e}")
            return None
    return None


def fetch_page(page: int, per_page: int = PAGE_SIZE) -> Optional[dict]:
    """Fetch a page of sentencias with full content."""
    filter_json = json.dumps({
        'search': ENUMERATE_TERM,
        'page': page,
        'per_page': per_page
    })
    return api_get('/extended/sentencias', {'filter': filter_json})


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


def extract_date_from_content(text: str) -> Optional[str]:
    """Try to extract a date from the OCR content."""
    # Look for patterns like "Santiago, veintiuno de marzo de dos mil veinticinco"
    # or "Santiago, 21 de marzo de 2025"
    m = re.search(
        r'(\d{1,2})\s+de\s+'
        r'(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)'
        r'\s+de\s+(\d{4})',
        text[:3000], re.IGNORECASE
    )
    if m:
        day = int(m.group(1))
        months = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
            'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
            'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        month = months.get(m.group(2).lower())
        year = int(m.group(3))
        if month and 1900 < year < 2100 and 1 <= day <= 31:
            return f"{year}-{month:02d}-{day:02d}"
    return None


def normalize(raw: dict) -> dict:
    """Transform raw sentencia data into standard schema."""
    rol = str(raw.get('rol', '') or raw.get('id', ''))
    sentence_id = raw.get('sentence_id', '')
    doc_id = f"CL_TC_{rol}"

    content = raw.get('content', '')
    text = clean_ocr_text(content)

    # Try to extract date from content
    date_str = extract_date_from_content(content)

    # Build title
    competencia = raw.get('competencia', '') or ''
    short_name = raw.get('competenciaShortName', '') or ''
    comp_display = competencia or (short_name if short_name != 'None' else '')
    title = f"Rol {rol}"
    if comp_display:
        title += f" — {comp_display}"

    url = f"https://buscador.tcchile.cl/sentencia/{urllib.parse.quote(rol)}"

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
        "sentence_id": sentence_id,
        "competencia": comp_display,
    }


def fetch_all(max_docs: int = None) -> Iterator[dict]:
    """Fetch all available sentencias with full text via pagination."""
    print("Fetching sentencias via paginated API...")

    # Get total count first
    result = fetch_page(1, per_page=1)
    if not result:
        print("ERROR: Cannot reach API")
        return
    meta = result.get('meta', {})
    total = meta.get('total', 0)
    last_page = meta.get('last_page', 1)
    print(f"  Total sentencias: {total}, pages: {last_page}")

    fetched = 0
    skipped = 0
    seen_ids = set()

    for page in range(1, last_page + 1):
        if max_docs and fetched >= max_docs:
            break

        result = fetch_page(page, per_page=PAGE_SIZE)
        if not result:
            print(f"  Page {page} failed, skipping")
            time.sleep(RATE_LIMIT)
            continue

        results = result.get('data', {}).get('results', [])
        if not results:
            break

        for raw in results:
            if max_docs and fetched >= max_docs:
                break

            sid = raw.get('sentence_id', '')
            if sid in seen_ids:
                continue
            seen_ids.add(sid)

            record = normalize(raw)
            if len(record.get('text', '')) >= 100:
                yield record
                fetched += 1
            else:
                skipped += 1

        if page % 10 == 0:
            print(f"  Page {page}/{last_page} (fetched={fetched}, skipped={skipped})")

        time.sleep(RATE_LIMIT)

    print(f"\nDone: {fetched} fetched, {skipped} skipped out of {total}")


def fetch_updates(since: datetime) -> Iterator[dict]:
    """Fetch all sentencias and filter by extracted date."""
    since_str = since.isoformat()[:10]
    for record in fetch_all():
        if record.get('date') and record['date'] >= since_str:
            yield record


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    records_saved = 0
    total_text_chars = 0

    for record in fetch_all(max_docs=count):
        total_text_chars += len(record.get('text', ''))

        filename = f"record_{records_saved:04d}.json"
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"    Saved: {filename}")
        print(f"    Rol: {record.get('rol', '')}")
        print(f"    Date: {record.get('date', 'N/A')}")
        print(f"    Text: {len(record.get('text', '')):,} chars")
        records_saved += 1

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
        print("\nSUCCESS: 10+ sample records with full text")
    else:
        print(f"\nWARNING: Only {records_saved} records saved (need 10+)")


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
            sample_dir.mkdir(parents=True, exist_ok=True)
            records_saved = 0
            for record in fetch_all():
                safe_id = re.sub(r'[^a-zA-Z0-9_-]', '_', record['_id'])[:100]
                filepath = sample_dir / f"{safe_id}.json"
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
