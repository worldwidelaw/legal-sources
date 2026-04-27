#!/usr/bin/env python3
"""
Hungarian Anonymized Court Decisions (Anonimizált Határozatok Tára)

Fetches court decisions from ALL Hungarian courts via the eakta.birosag.hu
JSON search API. Full text downloaded as RTF and converted to plain text.

Data source: https://eakta.birosag.hu/anonimizalt-hatarozatok
License: Public Domain
"""

import argparse
import io
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Iterator, Optional

try:
    from striprtf.striprtf import rtf_to_text
except ImportError:
    print("ERROR: striprtf not installed. Run: pip3 install striprtf")
    sys.exit(1)

SOURCE_ID = "HU/AnonHatarozatok"
BASE_URL = "https://eakta.birosag.hu"
SEARCH_URL = f"{BASE_URL}/AnonimizaltHatarozat/Search?Area="
DOWNLOAD_URL = f"{BASE_URL}/AnonimizaltHatarozat/DownloadHatarozatMobile?Area="
SESSION_URL = f"{BASE_URL}/anonimizalt-hatarozatok"

PAGE_SIZE = 100
RATE_LIMIT = 1.5

# Courts to iterate — covers all Hungarian courts
COURTS = [
    "Kúria",
    # Appellate courts (Ítélőtábla)
    "Debreceni Ítélőtábla",
    "Fővárosi Ítélőtábla",
    "Győri Ítélőtábla",
    "Pécsi Ítélőtábla",
    "Szegedi Ítélőtábla",
    # Regional courts (Törvényszék)
    "Balassagyarmati Törvényszék",
    "Budapest Környéki Törvényszék",
    "Debreceni Törvényszék",
    "Egri Törvényszék",
    "Fővárosi Törvényszék",
    "Győri Törvényszék",
    "Gyulai Törvényszék",
    "Kaposvári Törvényszék",
    "Kecskeméti Törvényszék",
    "Miskolci Törvényszék",
    "Nyíregyházi Törvényszék",
    "Pécsi Törvényszék",
    "Szegedi Törvényszék",
    "Székesfehérvári Törvényszék",
    "Szekszárdi Törvényszék",
    "Szolnoki Törvényszék",
    "Szombathelyi Törvényszék",
    "Tatabányai Törvényszék",
    "Veszprémi Törvényszék",
    "Zalaegerszegi Törvényszék",
]

# Legal areas for sub-partitioning when results exceed 10k
LEGAL_AREAS = [
    "büntetőjog",
    "polgári jog",
    "közigazgatási jog",
    "munkaügy",
    "gazdasági jog",
]


class Session:
    """Manages HTTP session with cookies for eakta.birosag.hu."""

    def __init__(self):
        self.cookie_jar = urllib.request.HTTPCookieProcessor()
        self.opener = urllib.request.build_opener(self.cookie_jar)
        self.opener.addheaders = [
            ('User-Agent', 'Legal Data Hunter/1.0 (EU Legal Research)'),
        ]
        self._init_session()

    def _init_session(self):
        """Get session cookies by visiting the main page."""
        try:
            req = urllib.request.Request(SESSION_URL)
            self.opener.open(req, timeout=30)
        except Exception as e:
            print(f"Warning: session init failed: {e}")

    def search(self, params: dict) -> Optional[dict]:
        """Execute search API call and return JSON response."""
        data = urllib.parse.urlencode(params).encode('utf-8')
        req = urllib.request.Request(
            SEARCH_URL,
            data=data,
            headers={
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'Accept': 'application/json',
                'X-Requested-With': 'XMLHttpRequest',
                'Referer': SESSION_URL,
            },
        )
        try:
            resp = self.opener.open(req, timeout=30)
            return json.loads(resp.read().decode('utf-8'))
        except Exception as e:
            print(f"  Search error: {e}")
            return None

    def download_document(self, court: str, case_number: str, index_id: str) -> Optional[str]:
        """Download document (RTF or DOCX) and convert to plain text."""
        params = urllib.parse.urlencode({
            'birosagName': court,
            'ugyszam': case_number,
            'azonosito': index_id,
        })
        url = f"{DOWNLOAD_URL}&{params}"
        req = urllib.request.Request(url, headers={
            'Referer': SESSION_URL,
        })
        try:
            resp = self.opener.open(req, timeout=60)
            doc_bytes = resp.read()

            # Detect format: DOCX starts with PK (ZIP magic), RTF starts with {\rtf
            if doc_bytes[:2] == b'PK':
                return self._extract_docx_text(doc_bytes)
            else:
                rtf_text = doc_bytes.decode('utf-8', errors='ignore')
                return rtf_to_text(rtf_text)
        except Exception as e:
            print(f"  Download error: {e}")
            return None

    @staticmethod
    def _extract_docx_text(docx_bytes: bytes) -> Optional[str]:
        """Extract text from DOCX (Office Open XML) bytes."""
        try:
            with zipfile.ZipFile(io.BytesIO(docx_bytes)) as zf:
                if 'word/document.xml' not in zf.namelist():
                    return None
                xml_content = zf.read('word/document.xml')
                root = ET.fromstring(xml_content)
                # Extract all text from w:t elements
                ns = {'w': 'http://schemas.openxmlformats.org/wordprocessingml/2006/main'}
                paragraphs = []
                for para in root.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'):
                    texts = []
                    for t in para.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t'):
                        if t.text:
                            texts.append(t.text)
                    if texts:
                        paragraphs.append(''.join(texts))
                return '\n\n'.join(paragraphs)
        except Exception as e:
            print(f"  DOCX parse error: {e}")
            return None


def clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    return text.strip()


def clean_text(text: str) -> str:
    """Normalize whitespace in extracted text."""
    if not text:
        return ""
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    return text.strip()


def normalize(raw: dict) -> dict:
    """Transform raw decision data into standard schema."""
    egyedi = raw.get('EgyediAzonosito', '')
    azonosito = raw.get('Azonosito', '')
    doc_id = egyedi or re.sub(r'[^a-zA-Z0-9]', '_', azonosito) or raw.get('IndexId', 'unknown')
    doc_id = f"HU_ANON_{doc_id}"

    # Build title from case number and court
    court = raw.get('MeghozoBirosag', '')
    title = f"{azonosito} — {court}" if court else azonosito

    # Clean legal provisions (HTML formatted)
    provisions = clean_html(raw.get('Jogszabalyhelyek', ''))

    # Date from decision year
    year = raw.get('HatarozatEve')
    date_str = f"{year}-01-01" if year else None

    # Build URL for direct access
    encoded_case = urllib.parse.quote(azonosito, safe='')
    encoded_court = urllib.parse.quote(court, safe='')
    url = f"{BASE_URL}/anonimizalt-hatarozatok?azonosito={encoded_case}&birosag={encoded_court}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get('_full_text', ''),
        "date": date_str,
        "url": url,
        "case_number": azonosito,
        "court": court,
        "collegium": raw.get('Kollegium', ''),
        "legal_area": raw.get('JogTerulet', ''),
        "legal_provisions": provisions,
        "abstract": raw.get('Rezume', ''),
        "egyedi_azonosito": egyedi,
    }


def search_paginated(session: Session, court: str = "", legal_area: str = "",
                     year_from: int = 0, year_to: int = 0,
                     max_results: int = 0) -> Iterator[dict]:
    """Search with pagination, yielding raw result dicts."""
    params = {
        'ResultStartIndex': 0,
        'ResultCount': PAGE_SIZE,
    }
    if court:
        params['MeghozoBirosag'] = court
    if legal_area:
        params['JogTerulet'] = legal_area
    if year_from:
        params['MeghozatalIdejeTol'] = f"{year_from}.01.01."
    if year_to:
        params['MeghozatalIdejeIg'] = f"{year_to}.12.31."

    offset = 0
    total = None
    yielded = 0

    while True:
        params['ResultStartIndex'] = offset
        result = session.search(params)
        if not result or not result.get('Success'):
            break

        if total is None:
            total = result.get('Count', 0)
            if total == 0:
                break

        items = result.get('List', [])
        if not items:
            break

        for item in items:
            yield item
            yielded += 1
            if max_results and yielded >= max_results:
                return

        offset += len(items)
        if offset >= min(total, 10000):
            break

        time.sleep(RATE_LIMIT)


def fetch_with_text(session: Session, items: Iterator[dict]) -> Iterator[dict]:
    """For each search result, download full text RTF and yield normalized record."""
    for item in items:
        court = item.get('MeghozoBirosag', '')
        case_num = item.get('Azonosito', '')
        index_id = item.get('IndexId', '')

        if not index_id:
            continue

        text = session.download_document(court, case_num, index_id)
        if text and len(clean_text(text)) >= 100:
            item['_full_text'] = clean_text(text)
            yield normalize(item)
        else:
            print(f"  No text for {case_num} (IndexId={index_id})")

        time.sleep(RATE_LIMIT)


def fetch_all() -> Iterator[dict]:
    """Fetch all available decisions with full text."""
    session = Session()

    for court in COURTS:
        print(f"\n=== Court: {court} ===")

        # First, check total count for this court
        test_params = {'ResultStartIndex': 0, 'ResultCount': 1, 'MeghozoBirosag': court}
        test_result = session.search(test_params)
        if not test_result or not test_result.get('Success'):
            print(f"  Skipping (search failed)")
            continue

        total = test_result.get('Count', 0)
        print(f"  Total results: {total}")

        if total == 0:
            continue

        if total <= 10000:
            # Can paginate directly
            items = search_paginated(session, court=court)
            yield from fetch_with_text(session, items)
        else:
            # Need to partition by legal area
            for area in LEGAL_AREAS:
                print(f"  --- Legal area: {area} ---")
                items = search_paginated(session, court=court, legal_area=area)
                yield from fetch_with_text(session, items)

            # Also fetch without legal area filter for uncategorized
            print(f"  --- Legal area: (other) ---")
            items = search_paginated(session, court=court)
            yield from fetch_with_text(session, items)


def fetch_updates(since: datetime) -> Iterator[dict]:
    """Fetch decisions modified since a given date."""
    session = Session()
    year = since.year
    current_year = datetime.now().year

    for court in COURTS:
        items = search_paginated(session, court=court, year_from=year, year_to=current_year)
        yield from fetch_with_text(session, items)


def bootstrap_sample(sample_dir: Path, count: int = 15):
    """Fetch sample records for validation."""
    session = Session()
    sample_dir.mkdir(parents=True, exist_ok=True)

    records_saved = 0
    total_text_chars = 0
    courts_sampled = set()

    # Sample from diverse courts: Kuria + some appellate + some regional
    sample_courts = [
        "Kúria",
        "Fővárosi Ítélőtábla",
        "Debreceni Ítélőtábla",
        "Fővárosi Törvényszék",
        "Szegedi Törvényszék",
    ]

    for court in sample_courts:
        if records_saved >= count:
            break

        print(f"\n=== Sampling from: {court} ===")

        # Get a few items from this court
        items_needed = min(3, count - records_saved)
        items = search_paginated(session, court=court, max_results=items_needed)

        for item in items:
            if records_saved >= count:
                break

            case_num = item.get('Azonosito', '')
            index_id = item.get('IndexId', '')
            print(f"  Fetching RTF for {case_num}...")

            text = session.download_document(court, case_num, index_id)
            if not text or len(clean_text(text)) < 100:
                print(f"  No text, skipping")
                continue

            item['_full_text'] = clean_text(text)
            record = normalize(item)

            text_len = len(record.get('text', ''))
            total_text_chars += text_len

            filename = f"record_{records_saved:04d}.json"
            filepath = sample_dir / filename
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  Saved: {filename}")
            print(f"  Court: {record.get('court', '')}")
            print(f"  Case: {record.get('case_number', '')}")
            print(f"  Text: {text_len:,} chars")
            courts_sampled.add(court)
            records_saved += 1

            time.sleep(RATE_LIMIT)

    # Print summary
    print("\n" + "=" * 60)
    print("SAMPLE SUMMARY")
    print("=" * 60)
    print(f"Records saved: {records_saved}")
    print(f"Courts sampled: {len(courts_sampled)} ({', '.join(courts_sampled)})")
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
        description="Hungarian Anonymized Court Decisions Fetcher"
    )
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records for validation")
    parser.add_argument("--count", type=int, default=15,
                        help="Number of sample records to fetch")
    parser.add_argument("--since", type=str,
                        help="Fetch updates since date (ISO format)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

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
                safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', record['_id'])[:100]
                filepath = sample_dir / f"{safe_name}.json"
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
        session = Session()
        year = since.year
        current_year = datetime.now().year
        for court in COURTS:
            items = search_paginated(session, court=court,
                                     year_from=year, year_to=current_year)
            for record in fetch_with_text(session, items):
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
