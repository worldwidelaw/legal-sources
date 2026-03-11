#!/usr/bin/env python3
"""
German Federal Council (Bundesrat) Data Fetcher

Uses the DIP (Dokumentations- und Informationssystem) API to fetch
Bundesrat parliamentary documents with full text.

API Documentation: https://dip.bundestag.de/über-dip/hilfe/api
Public API key valid until May 2026.

Data includes:
- Drucksachen (printed materials: motions, reports, opinions)
- Full text of parliamentary documents
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_BASE = "https://search.dip.bundestag.de/api/v1"
# Public demo API key valid until May 2026
DEFAULT_API_KEY = "OSOegLs.PR2lwJ1dwCeje9vTj7FPOt3hvpYKtwKkhw"


class BundesratFetcher:
    """Fetcher for German Bundesrat documents from DIP API"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('DIP_API_KEY') or DEFAULT_API_KEY
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Authorization': f'ApiKey {self.api_key}',
            'Accept': 'application/json'
        })

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict]:
        """Make a request to the DIP API"""
        url = f"{API_BASE}/{endpoint}"
        params = params or {}

        try:
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"API request failed: {e}")
            return None

    def fetch_drucksache_list(self, rows: int = 100, cursor: str = None) -> Optional[Dict]:
        """Fetch list of Bundesrat Drucksachen"""
        params = {
            'f.zuordnung': 'BR',  # Filter for Bundesrat only
            'rows': rows
        }
        if cursor:
            params['cursor'] = cursor

        return self._make_request('drucksache', params)

    def fetch_drucksache_with_text(self, rows: int = 10, cursor: str = None) -> Optional[Dict]:
        """Fetch Bundesrat Drucksachen with full text"""
        params = {
            'f.zuordnung': 'BR',  # Filter for Bundesrat only
            'rows': rows
        }
        if cursor:
            params['cursor'] = cursor

        return self._make_request('drucksache-text', params)

    def fetch_single_document(self, doc_id: str) -> Optional[Dict]:
        """Fetch a single document by ID"""
        return self._make_request(f'drucksache/{doc_id}')

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all Bundesrat documents with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        cursor = None
        count = 0
        batch_size = 10  # API limit for text endpoint

        while True:
            logger.info(f"Fetching batch starting at document {count}...")

            result = self.fetch_drucksache_with_text(rows=batch_size, cursor=cursor)

            if not result or 'documents' not in result:
                logger.error("Failed to fetch documents or no documents returned")
                break

            documents = result.get('documents', [])
            if not documents:
                logger.info("No more documents to fetch")
                break

            for doc in documents:
                text = doc.get('text', '')
                if text and len(text) > 100:
                    yield doc
                    count += 1

                    if limit and count >= limit:
                        logger.info(f"Reached limit of {limit} documents")
                        return

            # Get cursor for next page
            cursor = result.get('cursor')
            if not cursor:
                logger.info("No more pages available")
                break

            # Rate limiting
            time.sleep(0.5)

        logger.info(f"Fetched {count} documents with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """
        Fetch documents updated since a given date.

        Args:
            since: Fetch documents updated after this date

        Yields:
            Raw document dictionaries
        """
        cursor = None
        count = 0
        batch_size = 10
        since_str = since.strftime('%Y-%m-%d')

        while True:
            params = {
                'f.zuordnung': 'BR',
                'f.aktualisiert.start': since_str,
                'rows': batch_size
            }
            if cursor:
                params['cursor'] = cursor

            result = self._make_request('drucksache-text', params)

            if not result or 'documents' not in result:
                break

            documents = result.get('documents', [])
            if not documents:
                break

            for doc in documents:
                text = doc.get('text', '')
                if text and len(text) > 100:
                    yield doc
                    count += 1

            cursor = result.get('cursor')
            if not cursor:
                break

            time.sleep(0.5)

        logger.info(f"Fetched {count} updated documents since {since_str}")

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        doc_id = raw_doc.get('id', '')
        dokumentnummer = raw_doc.get('dokumentnummer', '')
        titel = raw_doc.get('titel', '')
        datum = raw_doc.get('datum', '')
        text = raw_doc.get('text', '')
        drucksachetyp = raw_doc.get('drucksachetyp', '')
        wahlperiode = raw_doc.get('wahlperiode', '')

        # Get PDF URL from fundstelle if available
        fundstelle = raw_doc.get('fundstelle', {})
        pdf_url = fundstelle.get('pdf_url', '')

        # Build URL to DIP portal
        url = f"https://dip.bundestag.de/drucksache/{dokumentnummer.replace('/', '-')}/{doc_id}" if doc_id else ''

        # Get origin/author info
        urheber = raw_doc.get('urheber', [])
        authors = [u.get('titel', '') for u in urheber if u.get('titel')]

        # Get related proceedings
        vorgangsbezug = raw_doc.get('vorgangsbezug', [])
        proceedings = [v.get('titel', '') for v in vorgangsbezug if v.get('titel')]

        return {
            '_id': doc_id,
            '_source': 'DE/Bundesrat',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': titel,
            'text': text,
            'date': datum,
            'url': url,
            'pdf_url': pdf_url,
            'language': 'de',
            # Additional metadata
            'document_number': dokumentnummer,
            'document_type': drucksachetyp,
            'electoral_period': wahlperiode,
            'authors': authors,
            'related_proceedings': proceedings
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BundesratFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 12 if '--sample' in sys.argv else 100

        for raw_doc in fetcher.fetch_all(limit=target_count + 10):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                continue

            # Save to sample directory
            doc_id = normalized['_id']
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['document_number']} - {normalized['title'][:50]}... ({text_len} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        # Test mode
        fetcher = BundesratFetcher()
        print("Testing Bundesrat fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Document Number: {normalized['document_number']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Type: {normalized['document_type']}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
