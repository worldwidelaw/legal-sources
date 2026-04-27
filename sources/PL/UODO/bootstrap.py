#!/usr/bin/env python3
"""
PL/UODO -- Polish Data Protection Authority (UODO) Decisions

Fetches GDPR and data protection decisions from the UODO decisions portal:
  https://orzeczenia.uodo.gov.pl

Data source: Portal Orzeczeń UODO (NeuroLex/Neurosoft platform)
Strategy:
  1. CSV export endpoint returns all decisions (~562) in one request
  2. Full text fetched per-document via /document/{urn}/content
License: Public domain (official government decisions)

Usage:
  python bootstrap.py bootstrap --sample     # Fetch sample records
  python bootstrap.py bootstrap              # Full fetch
  python bootstrap.py test-api               # Connectivity test
"""

import argparse
import csv
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlparse

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://orzeczenia.uodo.gov.pl"
SOURCE_ID = "PL/UODO"
SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"


class HTMLTextExtractor(HTMLParser):
    """Extract plain text from HTML, preserving paragraph breaks."""

    def __init__(self):
        super().__init__()
        self.result = []
        self._skip = False
        self._block_tags = {'p', 'div', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6',
                            'li', 'tr', 'dd', 'dt', 'blockquote', 'br'}

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style'):
            self._skip = True
        if tag in self._block_tags:
            self.result.append('\n')
        if tag == 'br':
            self.result.append('\n')

    def handle_endtag(self, tag):
        if tag in ('script', 'style'):
            self._skip = False
        if tag in self._block_tags:
            self.result.append('\n')

    def handle_data(self, data):
        if not self._skip:
            self.result.append(data)

    def get_text(self) -> str:
        text = ''.join(self.result)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()


def html_to_text(html: str) -> str:
    """Convert HTML to plain text."""
    parser = HTMLTextExtractor()
    parser.feed(html)
    return parser.get_text()


class UODOFetcher:
    """Fetcher for UODO data protection decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'pl-PL,pl;q=0.9,en;q=0.5',
        })

    def fetch_csv_index(self) -> List[Dict[str, str]]:
        """Fetch the CSV export with all decision metadata."""
        url = f"{BASE_URL}/search/export/csv"
        params = {
            'targetUrl': BASE_URL,
            'download': 'true'
        }
        logger.info("Fetching CSV index of all decisions...")
        resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()

        content = resp.text
        # Handle BOM if present
        if content.startswith('\ufeff'):
            content = content[1:]

        # Filter out comment lines (start with #) and blank lines
        lines = content.split('\n')
        data_lines = [lines[0]]  # Keep header
        for line in lines[1:]:
            stripped = line.strip()
            if stripped and not stripped.startswith('#'):
                data_lines.append(line)

        reader = csv.DictReader(io.StringIO('\n'.join(data_lines)))
        rows = list(reader)
        logger.info(f"CSV index contains {len(rows)} decisions")
        return rows

    def extract_urn_from_url(self, url: str) -> Optional[str]:
        """Extract URN from a document URL like /document/urn:ndoc:gov:pl:uodo:2025:dkn_5131_4"""
        if not url:
            return None
        path = urlparse(url).path
        match = re.search(r'/document/(urn:.+?)(?:\?|$)', path)
        if match:
            return match.group(1)
        # Try full URL pattern
        match = re.search(r'/document/(urn:.+?)(?:\?|$)', url)
        if match:
            return match.group(1)
        return None

    def fetch_document_content(self, urn: str) -> Optional[str]:
        """Fetch full text content of a decision by URN."""
        url = f"{BASE_URL}/document/{urn}/content"
        params = {'query': ''}
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            html = resp.text
            text = html_to_text(html)
            return text if text else None
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch content for {urn}: {e}")
            return None

    def fetch_document_meta(self, urn: str) -> Optional[Dict[str, Any]]:
        """Fetch metadata for a decision by URN."""
        url = f"{BASE_URL}/document/{urn}/meta"
        params = {'query': ''}
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            html = resp.text
            # Extract metadata from HTML meta page
            meta = {}
            # Try to extract key-value pairs from dt/dd elements
            dt_pattern = re.compile(r'<dt[^>]*>(.*?)</dt>\s*<dd[^>]*>(.*?)</dd>', re.DOTALL)
            for dt_match in dt_pattern.finditer(html):
                key = html_to_text(dt_match.group(1)).strip()
                value = html_to_text(dt_match.group(2)).strip()
                if key and value:
                    meta[key] = value
            return meta if meta else None
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch meta for {urn}: {e}")
            return None

    def normalize(self, csv_row: Dict[str, str], full_text: str,
                  meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Normalize a decision record to standard schema."""
        # CSV columns: Sygnatura, Status, Opis statusu, Tytul, Data wydania, Data publikacji, Adres
        case_number = csv_row.get('Sygnatura', '').strip()
        title = csv_row.get('Tytuł', '').strip() or csv_row.get('Tytul', '').strip()
        status = csv_row.get('Opis statusu', '').strip() or csv_row.get('Status', '').strip()
        date_issued = csv_row.get('Data wydania', '').strip()
        date_published = csv_row.get('Data publikacji', '').strip()
        doc_url = csv_row.get('Adres', '').strip()

        # Parse date
        date_iso = None
        for date_str in [date_issued, date_published]:
            if date_str:
                try:
                    dt = datetime.strptime(date_str, '%Y-%m-%d')
                    date_iso = dt.strftime('%Y-%m-%d')
                    break
                except ValueError:
                    try:
                        dt = datetime.strptime(date_str, '%d.%m.%Y')
                        date_iso = dt.strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue

        # Build ID from case number
        doc_id = case_number.replace('.', '_').replace('/', '_') if case_number else None
        if not doc_id:
            urn = self.extract_urn_from_url(doc_url)
            doc_id = urn.split(':')[-1] if urn else f"uodo_{hash(doc_url) % 10**8}"

        record = {
            '_id': f"PL_UODO_{doc_id}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title or f"Decyzja {case_number}",
            'text': full_text,
            'date': date_iso,
            'url': doc_url,
            'case_number': case_number,
            'status': status,
            'publication_date': date_published if date_published else None,
        }

        # Add metadata if available
        if meta:
            if 'Słowa kluczowe' in meta:
                record['keywords'] = meta['Słowa kluczowe']
            if 'Komórka organizacyjna' in meta:
                record['department'] = meta['Komórka organizacyjna']

        return record

    def fetch_all(self, sample: bool = False) -> Iterator[Dict[str, Any]]:
        """Fetch all UODO decisions with full text."""
        rows = self.fetch_csv_index()

        if sample:
            rows = rows[:15]
            logger.info(f"Sample mode: processing {len(rows)} decisions")

        for i, row in enumerate(rows):
            doc_url = row.get('Adres', '').strip()
            case_number = row.get('Sygnatura', '').strip()
            logger.info(f"[{i+1}/{len(rows)}] Fetching {case_number}...")

            urn = self.extract_urn_from_url(doc_url)
            if not urn:
                logger.warning(f"  Could not extract URN from {doc_url}, skipping")
                continue

            # Fetch full text
            full_text = self.fetch_document_content(urn)
            if not full_text:
                logger.warning(f"  No content for {case_number}, skipping")
                continue

            # Fetch metadata
            meta = self.fetch_document_meta(urn)
            time.sleep(1.0)  # Rate limit

            record = self.normalize(row, full_text, meta)
            yield record

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch decisions published since a given date."""
        rows = self.fetch_csv_index()
        since_dt = datetime.strptime(since, '%Y-%m-%d')

        for i, row in enumerate(rows):
            pub_date = row.get('Data publikacji', '').strip()
            if not pub_date:
                continue
            try:
                row_dt = datetime.strptime(pub_date, '%Y-%m-%d')
            except ValueError:
                continue
            if row_dt < since_dt:
                continue

            doc_url = row.get('Adres', '').strip()
            case_number = row.get('Sygnatura', '').strip()
            urn = self.extract_urn_from_url(doc_url)
            if not urn:
                continue

            full_text = self.fetch_document_content(urn)
            if not full_text:
                continue

            meta = self.fetch_document_meta(urn)
            time.sleep(1.0)

            record = self.normalize(row, full_text, meta)
            yield record

    def test_api(self) -> bool:
        """Test connectivity to the UODO decisions portal."""
        logger.info("Testing UODO API connectivity...")

        # Test CSV endpoint
        try:
            url = f"{BASE_URL}/search/export/csv"
            resp = self.session.get(url, params={'targetUrl': BASE_URL, 'download': 'true'}, timeout=30)
            resp.raise_for_status()
            lines = resp.text.strip().split('\n')
            logger.info(f"  CSV export: OK ({len(lines)-1} decisions)")
        except Exception as e:
            logger.error(f"  CSV export: FAILED - {e}")
            return False

        # Test document content endpoint with first decision
        try:
            # Re-use fetch_csv_index to get properly parsed rows
            rows = self.fetch_csv_index()
            first = rows[0]
            doc_url = first.get('Adres', '').strip()
            urn = self.extract_urn_from_url(doc_url)
            if urn:
                content = self.fetch_document_content(urn)
                if content and len(content) > 100:
                    logger.info(f"  Document content: OK ({len(content)} chars)")
                else:
                    logger.error(f"  Document content: Too short or empty")
                    return False
            else:
                logger.error(f"  Could not extract URN from {doc_url}")
                return False
        except Exception as e:
            logger.error(f"  Document content: FAILED - {e}")
            return False

        logger.info("All API tests passed!")
        return True


def bootstrap_sample(fetcher: UODOFetcher):
    """Fetch sample records and save to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetcher.fetch_all(sample=True):
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        text_len = len(record.get('text', ''))
        logger.info(f"  Saved {record['_id']} ({text_len} chars of text)")
        count += 1
    logger.info(f"Sample complete: {count} records saved to {SAMPLE_DIR}")
    return count


def bootstrap_full(fetcher: UODOFetcher):
    """Fetch all records and save to data/ directory."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetcher.fetch_all(sample=False):
        fname = DATA_DIR / f"{record['_id']}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        if count % 50 == 0:
            logger.info(f"  Progress: {count} records saved")
    logger.info(f"Full bootstrap complete: {count} records saved to {DATA_DIR}")
    return count


def main():
    parser = argparse.ArgumentParser(description='PL/UODO Data Protection Decisions Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'update', 'test-api'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Only fetch sample records (15 decisions)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (YYYY-MM-DD)')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    fetcher = UODOFetcher()

    if args.command == 'test-api':
        success = fetcher.test_api()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        if args.sample:
            count = bootstrap_sample(fetcher)
        else:
            count = bootstrap_full(fetcher)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)

    elif args.command == 'update':
        since = args.since or '2024-01-01'
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetcher.fetch_updates(since):
            fname = DATA_DIR / f"{record['_id']}.json"
            with open(fname, 'w', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} new/updated records since {since}")


if __name__ == '__main__':
    main()
