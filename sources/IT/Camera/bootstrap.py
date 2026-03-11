#!/usr/bin/env python3
"""
IT/Camera - Italian Chamber of Deputies (Camera dei Deputati)
Fetches parliamentary bills (proposte di legge / progetti di legge) with full text.

Data source: https://www.camera.it
Bill pages: https://www.camera.it/leg{N}/126?tab=1&leg={N}&idDocumento={ID}
Full text: https://documenti.camera.it/apps/commonServices/getDocumento.ashx

Legislatures:
- XIX (current): 2022-present
- XVIII: 2018-2022
- XVII: 2013-2018
- XVI: 2008-2013
- etc.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup
import yaml


class CameraFetcher:
    """Fetcher for Italian Chamber of Deputies parliamentary bills."""

    BASE_URL = "https://www.camera.it"
    DOCS_URL = "https://documenti.camera.it"

    def __init__(self, legislature: int = 19, rate_limit: float = 2.0):
        self.legislature = legislature
        self.rate_limit = rate_limit
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.8",
        })
        self._last_request_time = 0

    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request_time = time.time()

    def _get(self, url: str, **kwargs) -> requests.Response:
        """Make a rate-limited GET request."""
        self._rate_limit()
        response = self.session.get(url, timeout=30, **kwargs)
        return response

    def get_bill_page(self, bill_id: int) -> Optional[Dict[str, Any]]:
        """
        Fetch bill metadata from the bill info page.
        Returns None if the bill doesn't exist.
        """
        url = f"{self.BASE_URL}/leg{self.legislature}/126?tab=1&leg={self.legislature}&idDocumento={bill_id}"

        try:
            response = self._get(url)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, 'html.parser')

            # Check if it's an error page
            title = soup.find('title')
            if title and 'errore' in title.string.lower():
                return None

            # Check for "documento non disponibile"
            if 'non disponibile' in response.text.lower():
                return None

            # Extract document code from text link
            doc_code = None
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                if 'getDocumento.ashx' in href and 'testo_pdl' in href and 'pdf' not in href.lower():
                    match = re.search(r'codice=([^&]+)', href)
                    if match:
                        doc_code = match.group(1)
                        break

            if not doc_code:
                # No text document available
                return None

            # Extract metadata
            metadata = {
                'bill_id': bill_id,
                'legislature': self.legislature,
                'doc_code': doc_code,
                'url': url,
            }

            # Extract title from page content
            page_text = soup.get_text()

            # Look for bill title pattern (e.g., "PROPOSTA DI LEGGE" or "DISEGNO DI LEGGE")
            bill_type_match = re.search(
                r'(PROPOSTA DI LEGGE|DISEGNO DI LEGGE|PROGETTO DI LEGGE)[^\n]*\n([^\n]+)',
                page_text, re.IGNORECASE
            )
            if bill_type_match:
                metadata['bill_type'] = bill_type_match.group(1).strip()
                metadata['title'] = bill_type_match.group(2).strip()[:500]

            # Extract presentation date
            date_match = re.search(r'Presentat[ao]\s+(?:il\s+)?(\d{1,2}\s+\w+\s+\d{4})', page_text)
            if date_match:
                metadata['presentation_date_raw'] = date_match.group(1)
                # Try to parse Italian date
                metadata['presentation_date'] = self._parse_italian_date(date_match.group(1))

            # Extract initiative type (government, popular, etc.)
            if "D'INIZIATIVA POPOLARE" in page_text.upper():
                metadata['initiative_type'] = 'popular'
            elif "D'INIZIATIVA DEL GOVERNO" in page_text.upper():
                metadata['initiative_type'] = 'government'
            elif "D'INIZIATIVA DEI DEPUTATI" in page_text.upper():
                metadata['initiative_type'] = 'deputies'

            # Look for assignment info
            assignment_match = re.search(r'Assegnat[ao]\s+(?:alla?\s+)?([^\n]+)', page_text)
            if assignment_match:
                metadata['assigned_to'] = assignment_match.group(1).strip()[:200]

            return metadata

        except Exception as e:
            print(f"Error fetching bill {bill_id}: {e}", file=sys.stderr)
            return None

    def _parse_italian_date(self, date_str: str) -> Optional[str]:
        """Parse Italian date string to ISO format."""
        months = {
            'gennaio': '01', 'febbraio': '02', 'marzo': '03', 'aprile': '04',
            'maggio': '05', 'giugno': '06', 'luglio': '07', 'agosto': '08',
            'settembre': '09', 'ottobre': '10', 'novembre': '11', 'dicembre': '12'
        }
        try:
            parts = date_str.lower().split()
            if len(parts) >= 3:
                day = parts[0].zfill(2)
                month = months.get(parts[1])
                year = parts[2]
                if month and year.isdigit():
                    return f"{year}-{month}-{day}"
        except:
            pass
        return None

    def get_bill_text(self, doc_code: str) -> Optional[str]:
        """Fetch full text of a bill from the HTML endpoint."""
        url = (f"{self.DOCS_URL}/apps/commonServices/getDocumento.ashx"
               f"?sezione=lavori&tipoDoc=testo_pdl&idlegislatura={self.legislature}&codice={doc_code}")

        try:
            response = self._get(url)
            if response.status_code != 200:
                return None

            soup = BeautifulSoup(response.text, 'html.parser')

            # Remove script and style elements
            for element in soup(['script', 'style', 'nav', 'footer', 'header']):
                element.decompose()

            # Get text content
            text = soup.get_text(separator='\n', strip=True)

            # Clean up the text
            # Remove navigation elements at the end
            text = re.sub(r'Per tornare alla pagina di provenienza.*$', '', text, flags=re.DOTALL)
            text = re.sub(r'Camera dei deputati\s*©\s*Tutti i diritti riservati.*$', '', text, flags=re.DOTALL)

            # Remove excessive whitespace
            text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
            text = text.strip()

            if len(text) < 100:
                return None

            return text

        except Exception as e:
            print(f"Error fetching text for {doc_code}: {e}", file=sys.stderr)
            return None

    def fetch_bill(self, bill_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a complete bill with metadata and full text."""
        metadata = self.get_bill_page(bill_id)
        if not metadata:
            return None

        doc_code = metadata.get('doc_code')
        if not doc_code:
            return None

        text = self.get_bill_text(doc_code)
        if not text:
            return None

        metadata['text'] = text
        return metadata

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize raw bill data to standard schema."""
        bill_id = raw.get('bill_id')
        legislature = raw.get('legislature', self.legislature)

        # Generate a unique ID
        _id = f"IT/Camera/leg{legislature}/AC{bill_id}"

        # Build title from available info
        title_parts = []
        if raw.get('bill_type'):
            title_parts.append(raw['bill_type'])
        if raw.get('title'):
            title_parts.append(raw['title'])
        title = ' - '.join(title_parts) if title_parts else f"Atto Camera {bill_id}"

        return {
            '_id': _id,
            '_source': 'IT/Camera',
            '_type': 'legislation',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': raw.get('text', ''),
            'date': raw.get('presentation_date'),
            'url': raw.get('url', f"https://www.camera.it/leg{legislature}/126?tab=1&leg={legislature}&idDocumento={bill_id}"),
            # Additional metadata
            'bill_number': f"AC{bill_id}",
            'legislature': legislature,
            'doc_code': raw.get('doc_code'),
            'bill_type': raw.get('bill_type'),
            'initiative_type': raw.get('initiative_type'),
            'assigned_to': raw.get('assigned_to'),
            'presentation_date_raw': raw.get('presentation_date_raw'),
        }

    def fetch_all(self, start_id: int = 1, max_bills: int = 1000) -> Iterator[Dict[str, Any]]:
        """
        Fetch all bills starting from a given ID.
        Yields normalized bill records.
        """
        consecutive_failures = 0
        bill_id = start_id
        fetched = 0

        while fetched < max_bills and consecutive_failures < 50:
            raw = self.fetch_bill(bill_id)

            if raw:
                consecutive_failures = 0
                fetched += 1
                yield self.normalize(raw)
                print(f"Fetched bill {bill_id} ({fetched}/{max_bills})", file=sys.stderr)
            else:
                consecutive_failures += 1
                # Some bill IDs might be skipped, continue trying
                if consecutive_failures >= 50:
                    print(f"Too many consecutive failures at bill {bill_id}, stopping.", file=sys.stderr)
                    break

            bill_id += 1

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """
        Fetch bills updated since a given date.
        Note: Camera doesn't provide an update feed, so this fetches recent bills.
        """
        # For updates, we fetch from the beginning of the current legislature
        # In practice, you'd want to track last fetched ID
        for record in self.fetch_all(start_id=1, max_bills=100):
            if record.get('date') and record['date'] >= since:
                yield record


def bootstrap(sample_count: int = 12, output_dir: str = 'sample') -> None:
    """Bootstrap the data source by fetching sample records."""
    fetcher = CameraFetcher(legislature=19, rate_limit=2.0)

    output_path = Path(__file__).parent / output_dir
    output_path.mkdir(exist_ok=True)

    records = []
    total_text_length = 0

    print(f"Fetching {sample_count} sample bills from Camera dei Deputati (Legislature XIX)...")

    # Fetch sample bills - try various IDs to get a good sample
    bill_ids_to_try = list(range(1, 200))  # Try first 200 bill IDs

    for bill_id in bill_ids_to_try:
        if len(records) >= sample_count:
            break

        raw = fetcher.fetch_bill(bill_id)
        if raw and raw.get('text'):
            record = fetcher.normalize(raw)
            records.append(record)
            total_text_length += len(record.get('text', ''))

            # Save individual record
            filename = f"AC{bill_id}.json"
            with open(output_path / filename, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"  [{len(records)}/{sample_count}] AC{bill_id}: {len(record.get('text', ''))} chars - {record.get('title', '')[:60]}...")

    # Summary
    print(f"\n=== Bootstrap Summary ===")
    print(f"Total records: {len(records)}")
    print(f"Total text length: {total_text_length:,} chars")
    if records:
        avg_length = total_text_length / len(records)
        print(f"Average text length: {avg_length:,.0f} chars/doc")

    # Verify all records have text
    records_with_text = sum(1 for r in records if r.get('text') and len(r['text']) > 100)
    print(f"Records with substantial text: {records_with_text}/{len(records)}")

    # Save manifest
    manifest = {
        'source': 'IT/Camera',
        'legislature': 19,
        'sample_count': len(records),
        'fetched_at': datetime.utcnow().isoformat() + 'Z',
        'avg_text_length': avg_length if records else 0,
        'records_with_text': records_with_text,
    }
    with open(output_path / 'manifest.json', 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)


def main():
    parser = argparse.ArgumentParser(description='IT/Camera - Italian Chamber of Deputies fetcher')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser('bootstrap', help='Fetch sample records')
    bootstrap_parser.add_argument('--sample', action='store_true', help='Run in sample mode')
    bootstrap_parser.add_argument('--count', type=int, default=12, help='Number of samples to fetch')

    # Fetch command
    fetch_parser = subparsers.add_parser('fetch', help='Fetch all records')
    fetch_parser.add_argument('--start', type=int, default=1, help='Starting bill ID')
    fetch_parser.add_argument('--max', type=int, default=1000, help='Maximum bills to fetch')
    fetch_parser.add_argument('--legislature', type=int, default=19, help='Legislature number')

    args = parser.parse_args()

    if args.command == 'bootstrap' or (hasattr(args, 'sample') and args.sample):
        bootstrap(sample_count=getattr(args, 'count', 12))
    elif args.command == 'fetch':
        fetcher = CameraFetcher(legislature=args.legislature)
        for record in fetcher.fetch_all(start_id=args.start, max_bills=args.max):
            print(json.dumps(record, ensure_ascii=False))
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
