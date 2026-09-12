#!/usr/bin/env python3
"""
TR/Mevzuat - Turkish Legislation Database (Mevzuat Bilgi Sistemi)

Fetches legislation from the official Turkish government database at mevzuat.gov.tr.
Covers laws, decrees, regulations, directives, and presidential decisions.

API Endpoints:
  - DataTable API: POST /Anasayfa/MevzuatDatatable (JSON)
  - Full text iframe: GET /anasayfa/MevzuatFihristDetayIframe?MevzuatTur=X&MevzuatNo=Y&MevzuatTertip=5

Legislation types (MevzuatTur), verified against the API's own mevzuatTurEnumString:
  1  = Kanunlar (Laws)                          916 docs
  2  = Tüzükler (Statutory regulations)         107 docs
  3  = Yönetmelik (Directives)                8,851 docs
  4  = Kanun Hükmünde Kararnameler (KHK)        63 docs
  5  = Mülga Kanun (Repealed laws)             185 docs
  6  = AGGREGATE — not a type. Enumerates the WHOLE corpus (19,069 docs),
       each record carrying its real mevzuatTur (including 8, 9, 20, ...
       Cumhurbaşkanlığı Kararnamesi / Kararı / Tebliğ, which no single-type
       query reaches). This is the discovery lane used by fetch_all().
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import requests
from bs4 import BeautifulSoup

# Base URL
BASE_URL = "https://www.mevzuat.gov.tr"

# MevzuatTur used as the DISCOVERY lane: 6 is the aggregate over every type.
DISCOVERY_TUR = 6

# _id slug map. FROZEN — do not "correct" these labels.
#
# The original mapping mislabelled turs 2-5 (2 is Tüzük not KHK, 3 is Yönetmelik
# not Tüzük, 4 is KHK not Yönetmelik, 5 is Mülga Kanun not CB Kararnamesi), but
# ~9,200 rows are already in Neon keyed on these slugs. The slug only has to be
# unique and stable, and it is; re-keying would duplicate every one of those rows.
# The human-readable type is carried by `mevzuat_tur_name`, which reads the API's
# own enum string and IS correct.
#
# Turs outside this map (8, 9, 20, ... surfaced only by the aggregate query) key
# on the numeric tur, so two documents sharing a mevzuatNo across types cannot
# collapse onto one _id.
MEVZUAT_ID_SLUGS = {
    1: "kanun",
    2: "khk",
    3: "tuzuk",
    4: "yonetmelik",
    5: "cb_kararname",
}


class MevzuatFetcher:
    """Fetcher for Turkish Legislation Database."""

    def __init__(self, sample_dir: Optional[Path] = None):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept-Language': 'tr-TR,tr;q=0.9,en;q=0.8',
        })
        self.sample_dir = sample_dir or Path(__file__).parent / "sample"
        self._init_session()

    def _init_session(self):
        """Initialize session by visiting main page to get cookies."""
        response = self.session.get(BASE_URL)
        response.raise_for_status()

    def _get_datatable_headers(self) -> Dict[str, str]:
        """Get headers for DataTable API requests."""
        return {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Content-Type': 'application/json; charset=utf-8',
            'X-Requested-With': 'XMLHttpRequest',
            'Origin': BASE_URL,
            'Referer': f'{BASE_URL}/',
        }

    def fetch_legislation_list(
        self,
        mevzuat_tur: int,
        start: int = 0,
        length: int = 100,
        mevzuat_tertip: int = 5,
    ) -> Dict[str, Any]:
        """
        Fetch a page of legislation metadata from the DataTable API.

        Args:
            mevzuat_tur: Legislation type (1-6)
            start: Offset for pagination
            length: Number of records per page
            mevzuat_tertip: Constitution order (5 = current)

        Returns:
            API response with recordsTotal, recordsFiltered, and data array
        """
        url = f"{BASE_URL}/Anasayfa/MevzuatDatatable"
        data = {
            'draw': 1,
            'start': start,
            'length': length,
            'parameters': {
                'MevzuatTur': mevzuat_tur,
                'MevzuatTertip': mevzuat_tertip,
            }
        }

        response = self.session.post(
            url,
            headers=self._get_datatable_headers(),
            json=data,
        )
        response.raise_for_status()
        return response.json()

    def fetch_legislation_text(
        self,
        mevzuat_no: str,
        mevzuat_tur: int,
        mevzuat_tertip: int = 5,
    ) -> str:
        """
        Fetch the full text of a legislation document.

        Args:
            mevzuat_no: Legislation number
            mevzuat_tur: Legislation type
            mevzuat_tertip: Constitution order

        Returns:
            Plain text content of the legislation
        """
        url = f"{BASE_URL}/anasayfa/MevzuatFihristDetayIframe"
        params = {
            'MevzuatTur': mevzuat_tur,
            'MevzuatNo': mevzuat_no,
            'MevzuatTertip': mevzuat_tertip,
        }

        response = self.session.get(url, params=params)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        # Main content is in WordSection1 div
        word_section = soup.find('div', class_='WordSection1')
        if word_section:
            # Clean up the text
            text = word_section.get_text(separator='\n', strip=True)
            # Remove excessive whitespace
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            return text.strip()

        # Fallback: get all text from body
        body = soup.find('body')
        if body:
            return body.get_text(separator='\n', strip=True)

        return ""

    def normalize(self, raw: Dict[str, Any], text: str) -> Dict[str, Any]:
        """
        Normalize a raw record into standard schema.

        Args:
            raw: Raw metadata from DataTable API
            text: Full text content

        Returns:
            Normalized record with standard fields
        """
        mevzuat_no = raw.get('mevzuatNo', '')
        mevzuat_tur = raw.get('mevzuatTur', raw.get('tur', 1))
        mevzuat_tertip = raw.get('mevzuatTertip', '5')

        # Parse dates
        accept_date = self._parse_date(raw.get('kabulTarih', ''))
        gazette_date = self._parse_date(raw.get('resmiGazeteTarihi', ''))

        # Build unique ID. Unknown turs key on the number so they cannot collide
        # with each other (see MEVZUAT_ID_SLUGS).
        slug = MEVZUAT_ID_SLUGS.get(mevzuat_tur) or f"t{mevzuat_tur}"
        doc_id = f"TR-{slug}-{mevzuat_no}"

        # Build source URL
        url = raw.get('url', '')
        if url and not url.startswith('http'):
            url = f"{BASE_URL}/{url}"

        return {
            '_id': doc_id,
            '_source': 'TR/Mevzuat',
            '_type': 'legislation',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': raw.get('mevAdi', '').strip(),
            'text': text,
            'date': gazette_date or accept_date,
            'url': url,
            # Additional metadata
            'mevzuat_no': mevzuat_no,
            'mevzuat_tur': mevzuat_tur,
            'mevzuat_tur_name': (raw.get('mevzuatTurEnumString') or '').strip(),
            'mevzuat_tertip': mevzuat_tertip,
            'accept_date': accept_date,
            'gazette_date': gazette_date,
            'gazette_number': raw.get('resmiGazeteSayisi', ''),
            'mukerrer': raw.get('mukerrer', ''),
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse Turkish date format (DD.MM.YYYY) to ISO format."""
        if not date_str:
            return None
        try:
            # Turkish format: DD.MM.YYYY
            dt = datetime.strptime(date_str.strip(), '%d.%m.%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            return None

    def fetch_all(
        self,
        mevzuat_types: Optional[list] = None,
        limit: Optional[int] = None,
        skip_ids: Optional[set] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch all legislation with full text.

        Discovery goes through the aggregate query (MevzuatTur=6), which lists the
        entire corpus — including the Cumhurbaşkanlığı Kararnamesi / Kararı / Tebliğ
        types (turs 8, 9, 20, ...) that no single-type query returns. Each record
        carries its real `mevzuatTur`, so the full text fetch uses that, not the
        query type.

        Args:
            mevzuat_types: Restrict to these types (sampling only). Default: the
                aggregate lane, i.e. everything.
            limit: Maximum number of records to fetch (for testing)
            skip_ids: _id values already written, for resuming a partial crawl

        Yields:
            Normalized records with full text
        """
        lanes = mevzuat_types if mevzuat_types is not None else [DISCOVERY_TUR]
        skip_ids = skip_ids or set()
        seen = set()
        count = 0

        for lane in lanes:
            result = self.fetch_legislation_list(lane, start=0, length=1)
            total = result.get('recordsTotal', 0)
            print(f"\nLane MevzuatTur={lane}: {total} records", flush=True)

            if total == 0:
                continue

            page_size = 100
            for start in range(0, total, page_size):
                if limit and count >= limit:
                    return

                try:
                    result = self.fetch_legislation_list(
                        lane, start=start, length=page_size,
                    )
                except Exception as e:
                    print(f"  ERROR listing at offset {start}: {e}", flush=True)
                    continue

                rows = result.get('data', [])
                if not rows:
                    print(f"  Empty page at offset {start}, stopping lane", flush=True)
                    break

                for raw in rows:
                    if limit and count >= limit:
                        return

                    mevzuat_no = raw.get('mevzuatNo', '')
                    # The record's OWN type — the aggregate lane mixes them.
                    mevzuat_tur = raw.get('mevzuatTur', lane)
                    key = (mevzuat_tur, mevzuat_no)
                    if key in seen:
                        continue
                    seen.add(key)

                    record_id = self.normalize(raw, '')['_id']
                    if record_id in skip_ids:
                        continue

                    title = raw.get('mevAdi', '')[:50]
                    print(f"  [{count + 1}/{total}] {mevzuat_no}: {title}...", flush=True)

                    try:
                        text = self.fetch_legislation_text(
                            mevzuat_no,
                            mevzuat_tur,
                            int(raw.get('mevzuatTertip', 5)),
                        )

                        if not text:
                            print(f"    WARNING: No text found for {mevzuat_no}", flush=True)
                            continue

                        yield self.normalize(raw, text)
                        count += 1

                        # Rate limiting
                        time.sleep(1.5)

                    except Exception as e:
                        print(f"    ERROR fetching {mevzuat_no}: {e}", flush=True)
                        continue

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch legislation updated since a given date.

        Args:
            since: Date to fetch updates from

        Yields:
            Normalized records with full text
        """
        # The API has no date filter, but the aggregate lane is ordered
        # newest-gazette-first, so walk it until a whole page predates `since`.
        if isinstance(since, str):
            since_str = since[:10]
        else:
            since_str = since.strftime('%Y-%m-%d')

        page_size = 100
        for start in range(0, 5000, page_size):
            result = self.fetch_legislation_list(
                DISCOVERY_TUR, start=start, length=page_size,
            )
            rows = result.get('data', [])
            if not rows:
                return

            fresh = 0
            for raw in rows:
                gazette_date = self._parse_date(raw.get('resmiGazeteTarihi', ''))
                if not gazette_date or gazette_date < since_str:
                    continue
                fresh += 1
                mevzuat_no = raw.get('mevzuatNo', '')

                try:
                    text = self.fetch_legislation_text(
                        mevzuat_no,
                        raw.get('mevzuatTur', DISCOVERY_TUR),
                        int(raw.get('mevzuatTertip', 5)),
                    )

                    if text:
                        yield self.normalize(raw, text)
                        time.sleep(1.5)

                except Exception as e:
                    print(f"Error fetching {mevzuat_no}: {e}", flush=True)

            # A page with nothing newer than `since` means we have walked past it.
            if fresh == 0:
                return

    def bootstrap_sample(self, count: int = 15) -> None:
        """
        Fetch sample records for testing.

        Args:
            count: Number of sample records to fetch
        """
        self.sample_dir.mkdir(parents=True, exist_ok=True)

        # Fetch a mix of legislation types
        records = []

        # Fetch some laws (type 1)
        print("Fetching sample Laws (Kanunlar)...")
        for record in self.fetch_all(mevzuat_types=[1], limit=5):
            records.append(record)
            self._save_sample(record)

        # Fetch some presidential decrees (type 5)
        print("\nFetching sample Presidential Decrees...")
        for record in self.fetch_all(mevzuat_types=[5], limit=3):
            records.append(record)
            self._save_sample(record)

        # Fetch some KHK (type 2)
        print("\nFetching sample KHK (Decrees with Force of Law)...")
        for record in self.fetch_all(mevzuat_types=[2], limit=3):
            records.append(record)
            self._save_sample(record)

        # Fetch some regulations (type 3)
        print("\nFetching sample Regulations (Tüzükler)...")
        for record in self.fetch_all(mevzuat_types=[3], limit=4):
            records.append(record)
            self._save_sample(record)

        print(f"\n{'='*60}")
        print(f"Sample collection complete!")
        print(f"Total records: {len(records)}")
        print(f"Sample directory: {self.sample_dir}")

        # Print statistics
        if records:
            text_lengths = [len(r.get('text', '')) for r in records]
            print(f"\nText statistics:")
            print(f"  Min length: {min(text_lengths):,} chars")
            print(f"  Max length: {max(text_lengths):,} chars")
            print(f"  Avg length: {sum(text_lengths) // len(text_lengths):,} chars")

    def _save_sample(self, record: Dict[str, Any]) -> None:
        """Save a sample record to the sample directory."""
        filename = f"{record['_id']}.json"
        filepath = self.sample_dir / filename

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"  Saved: {filename} ({len(record.get('text', '')):,} chars)")


def main():
    parser = argparse.ArgumentParser(
        description='TR/Mevzuat - Turkish Legislation Database Fetcher'
    )
    parser.add_argument(
        'command',
        # bootstrap-fast is the fleet's full-crawl entry point. It was missing
        # from this list, so every fleet run died on an argparse error and the
        # wrapper fell back to ingesting the ~15 bundled sample/*.json (#1538).
        choices=['bootstrap', 'bootstrap-fast', 'fetch', 'updates'],
        help='Command to run',
    )
    parser.add_argument(
        '--sample',
        action='store_true',
        help='Fetch sample records only (for bootstrap)',
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Limit number of records to fetch',
    )
    parser.add_argument(
        '--since',
        type=str,
        default=None,
        help='Date to fetch updates from (YYYY-MM-DD)',
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    fetcher = MevzuatFetcher()

    if args.command in ('bootstrap', 'bootstrap-fast'):
        if args.sample:
            fetcher.bootstrap_sample(count=args.limit or 15)
            return

        # Full crawl streams to data/records.jsonl — the file the pipeline
        # ingests. It used to write one JSON per record into sample/, so even a
        # successful crawl left the pipeline nothing to read.
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        out_path = data_dir / "records.jsonl"

        # Resume: skip anything already written by a previous partial run.
        skip_ids = set()
        if out_path.exists():
            with open(out_path, encoding='utf-8') as fh:
                for line in fh:
                    try:
                        skip_ids.add(json.loads(line)['_id'])
                    except (ValueError, KeyError):
                        continue
            print(f"Resuming: {len(skip_ids)} records already in {out_path}")

        count = 0
        with open(out_path, 'a', encoding='utf-8') as fh:
            for record in fetcher.fetch_all(limit=args.limit, skip_ids=skip_ids):
                fh.write(json.dumps(record, ensure_ascii=False) + '\n')
                fh.flush()
                count += 1

        total = count + len(skip_ids)
        print(f"\nBootstrap complete: {count} new records ({total} total) -> {out_path}")
        if total == 0:
            print("ERROR: no records written — failing loudly instead of "
                  "letting the wrapper report a sample-only completion")
            sys.exit(1)

    elif args.command == 'fetch':
        for record in fetcher.fetch_all(limit=args.limit):
            print(json.dumps(record, ensure_ascii=False))

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since is required for updates command")
            sys.exit(1)
        since = datetime.strptime(args.since, '%Y-%m-%d')
        for record in fetcher.fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == '__main__':
    main()
