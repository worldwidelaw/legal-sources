#!/usr/bin/env python3
"""
TR/Mevzuat - Turkish Legislation Database (Mevzuat Bilgi Sistemi)

Fetches legislation from the official Turkish government database at mevzuat.gov.tr.
Covers laws, decrees, regulations, directives, and presidential decisions.

API Endpoints:
  - DataTable API: POST /Anasayfa/MevzuatDatatable (JSON)
  - Full text iframe: GET /anasayfa/MevzuatFihristDetayIframe?MevzuatTur=X&MevzuatNo=Y&MevzuatTertip=5

Legislation types (MevzuatTur):
  1 = Kanunlar (Laws)
  2 = KHK (Decree with Force of Law)
  3 = Tüzükler (Regulations)
  4 = Yönetmelikler (Directives)
  5 = Cumhurbaşkanlığı Kararnameleri (Presidential Decrees)
  6 = Cumhurbaşkanı Kararları (Presidential Decisions)
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

# Legislation type mapping
MEVZUAT_TYPES = {
    1: "kanun",           # Laws
    2: "khk",             # Decree with Force of Law
    3: "tuzuk",           # Regulations
    4: "yonetmelik",      # Directives
    5: "cb_kararname",    # Presidential Decrees
    6: "cb_karar",        # Presidential Decisions
}

MEVZUAT_TYPE_NAMES = {
    1: "Kanunlar (Laws)",
    2: "KHK (Decree with Force of Law)",
    3: "Tüzükler (Regulations)",
    4: "Yönetmelikler (Directives)",
    5: "Cumhurbaşkanlığı Kararnameleri (Presidential Decrees)",
    6: "Cumhurbaşkanı Kararları (Presidential Decisions)",
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

        # Build unique ID
        doc_id = f"TR-{MEVZUAT_TYPES.get(mevzuat_tur, 'other')}-{mevzuat_no}"

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
            'mevzuat_tur_name': MEVZUAT_TYPE_NAMES.get(mevzuat_tur, ''),
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
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch all legislation with full text.

        Args:
            mevzuat_types: List of legislation types to fetch (default: all)
            limit: Maximum number of records to fetch (for testing)

        Yields:
            Normalized records with full text
        """
        if mevzuat_types is None:
            mevzuat_types = list(MEVZUAT_TYPES.keys())

        count = 0
        for mevzuat_tur in mevzuat_types:
            type_name = MEVZUAT_TYPE_NAMES.get(mevzuat_tur, f"Type {mevzuat_tur}")
            print(f"\nFetching {type_name}...")

            # Get total count
            result = self.fetch_legislation_list(mevzuat_tur, start=0, length=1)
            total = result.get('recordsTotal', 0)
            print(f"  Total records: {total}")

            if total == 0:
                continue

            # Paginate through all records
            page_size = 100
            for start in range(0, total, page_size):
                if limit and count >= limit:
                    return

                result = self.fetch_legislation_list(
                    mevzuat_tur,
                    start=start,
                    length=page_size,
                )

                for raw in result.get('data', []):
                    if limit and count >= limit:
                        return

                    mevzuat_no = raw.get('mevzuatNo', '')
                    title = raw.get('mevAdi', '')[:50]
                    print(f"  [{count + 1}] Fetching {mevzuat_no}: {title}...")

                    try:
                        # Fetch full text
                        text = self.fetch_legislation_text(
                            mevzuat_no,
                            mevzuat_tur,
                            int(raw.get('mevzuatTertip', 5)),
                        )

                        if not text:
                            print(f"    WARNING: No text found for {mevzuat_no}")
                            continue

                        # Normalize and yield
                        record = self.normalize(raw, text)
                        yield record
                        count += 1

                        # Rate limiting
                        time.sleep(1.5)

                    except Exception as e:
                        print(f"    ERROR fetching {mevzuat_no}: {e}")
                        continue

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch legislation updated since a given date.

        Args:
            since: Date to fetch updates from

        Yields:
            Normalized records with full text
        """
        # The API doesn't have a direct date filter, so we fetch recent records
        # and filter by gazette date
        since_str = since.strftime('%Y-%m-%d')

        for mevzuat_tur in MEVZUAT_TYPES.keys():
            result = self.fetch_legislation_list(mevzuat_tur, start=0, length=100)

            for raw in result.get('data', []):
                gazette_date = self._parse_date(raw.get('resmiGazeteTarihi', ''))
                if gazette_date and gazette_date >= since_str:
                    mevzuat_no = raw.get('mevzuatNo', '')

                    try:
                        text = self.fetch_legislation_text(
                            mevzuat_no,
                            mevzuat_tur,
                        )

                        if text:
                            yield self.normalize(raw, text)
                            time.sleep(1.5)

                    except Exception as e:
                        print(f"Error fetching {mevzuat_no}: {e}")

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
        choices=['bootstrap', 'fetch', 'updates'],
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

    args = parser.parse_args()

    fetcher = MevzuatFetcher()

    if args.command == 'bootstrap':
        if args.sample:
            fetcher.bootstrap_sample(count=args.limit or 15)
        else:
            count = 0
            for record in fetcher.fetch_all(limit=args.limit):
                fetcher._save_sample(record)
                count += 1
            print(f"\nBootstrap complete: {count} records")

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
