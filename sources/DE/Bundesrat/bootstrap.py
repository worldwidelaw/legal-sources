#!/usr/bin/env python3
"""
German Federal Council (Bundesrat) Data Fetcher

Uses the DIP (Dokumentations- und Informationssystem) API to fetch
Bundesrat parliamentary documents with full text.

API Documentation: https://dip.bundestag.de/über-dip/hilfe/api
Public API key valid until end of May 2027. Override with DIP_API_KEY.

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
# Public demo API key published by the Bundestag, valid until end of May 2027.
# The Bundestag rotates this roughly yearly; when it expires the API answers 401
# and the current key is republished at https://dip.bundestag.de/über-dip/hilfe/api
DEFAULT_API_KEY = "R2BZaee.DjdCyihKZMf8AOjtScubP2EVydegzjmBIQ"


class DipAuthError(RuntimeError):
    """Raised when the DIP API rejects our key — the corpus is unreachable, not empty."""


class BundesratFetcher:
    """Fetcher for German Bundesrat documents from DIP API"""

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv('DIP_API_KEY') or DEFAULT_API_KEY
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Authorization': f'ApiKey {self.api_key}',
            'Accept': 'application/json'
        })

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Optional[Dict]:
        """Make a request to the DIP API.

        Auth failures raise DipAuthError rather than returning None: an expired
        public key otherwise looks exactly like "no more documents" and the crawl
        exits 0 having written nothing (issue #1450).
        """
        url = f"{API_BASE}/{endpoint}"
        params = params or {}

        last_error = None
        for attempt in range(5):
            try:
                response = self.session.get(url, params=params, timeout=60)
                if response.status_code in (401, 403):
                    raise DipAuthError(
                        f"DIP API rejected the API key (HTTP {response.status_code}). "
                        "The public key has most likely expired — fetch the current one from "
                        "https://dip.bundestag.de/über-dip/hilfe/api or set DIP_API_KEY."
                    )
                if response.status_code == 429 or response.status_code >= 500:
                    retry_after = response.headers.get('Retry-After')
                    delay = float(retry_after) if retry_after and retry_after.isdigit() else min(60, 2 ** attempt)
                    logger.warning(f"HTTP {response.status_code} from {endpoint}, retrying in {delay}s")
                    time.sleep(delay)
                    last_error = f"HTTP {response.status_code}"
                    continue
                response.raise_for_status()
                return response.json()
            except DipAuthError:
                raise
            except (requests.RequestException, ValueError) as e:
                last_error = e
                logger.warning(f"API request failed ({e}), attempt {attempt + 1}/5")
                time.sleep(min(60, 2 ** attempt))

        logger.error(f"API request to {endpoint} failed after 5 attempts: {last_error}")
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

    def fetch_all(self, limit: int = None, checkpoint_path: Path = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all Bundesrat documents with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)
            checkpoint_path: If given, the DIP cursor is persisted here after every
                page so a re-run resumes where the previous one stopped instead of
                re-walking the ~100K document corpus from the start.

        Yields:
            Raw document dictionaries with full text
        """
        cursor = self._load_checkpoint(checkpoint_path)
        count = 0
        pages = 0
        batch_size = 10  # API limit for the drucksache-text endpoint

        while True:
            logger.info(f"Fetching batch starting at document {count}...")

            result = self.fetch_drucksache_with_text(rows=batch_size, cursor=cursor)

            if not result or 'documents' not in result:
                if pages == 0:
                    raise RuntimeError(
                        "DIP drucksache-text returned no usable response on the first page — "
                        "the corpus is unreachable, not empty. Refusing to report success."
                    )
                logger.error("Failed to fetch a page; stopping with partial results")
                break

            documents = result.get('documents', [])
            if not documents:
                logger.info("No more documents to fetch")
                break
            pages += 1

            for doc in documents:
                text = doc.get('text', '')
                if text and len(text) > 100:
                    yield doc
                    count += 1

                    if limit and count >= limit:
                        logger.info(f"Reached limit of {limit} documents")
                        return

            # DIP signals exhaustion by echoing back the cursor it was given.
            next_cursor = result.get('cursor')
            if not next_cursor or next_cursor == cursor:
                logger.info("No more pages available")
                self._save_checkpoint(checkpoint_path, None)
                break
            cursor = next_cursor
            self._save_checkpoint(checkpoint_path, cursor)

            # Rate limiting
            time.sleep(0.5)

        if pages > 0 and count == 0:
            raise RuntimeError(
                f"Walked {pages} DIP pages but every document had empty text — "
                "the drucksache-text payload shape has changed."
            )
        logger.info(f"Fetched {count} documents with full text")

    @staticmethod
    def _load_checkpoint(path: Path) -> Optional[str]:
        if not path or not path.exists():
            return None
        try:
            cursor = json.loads(path.read_text(encoding='utf-8')).get('cursor')
        except (ValueError, OSError) as e:
            logger.warning(f"Ignoring unreadable checkpoint {path}: {e}")
            return None
        if cursor:
            logger.info(f"Resuming from checkpointed cursor {cursor}")
        return cursor

    @staticmethod
    def _save_checkpoint(path: Path, cursor: Optional[str]) -> None:
        if not path:
            return
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({'cursor': cursor}), encoding='utf-8')
        except OSError as e:
            logger.warning(f"Could not write checkpoint {path}: {e}")

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


def run_full(fetcher: 'BundesratFetcher') -> int:
    """Stream the whole corpus to data/records.jsonl (the path the fleet ingests)."""
    data_dir = Path(__file__).parent / 'data'
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / 'records.jsonl'
    checkpoint = data_dir / 'bundesrat_checkpoint.json'

    written = 0
    with open(out_path, 'w', encoding='utf-8') as out:
        for raw_doc in fetcher.fetch_all(checkpoint_path=checkpoint):
            normalized = fetcher.normalize(raw_doc)
            if len(normalized.get('text', '')) < 100:
                continue
            out.write(json.dumps(normalized, ensure_ascii=False) + '\n')
            written += 1
            if written % 500 == 0:
                out.flush()
                logger.info(f"Wrote {written} records to {out_path}")

    logger.info(f"bootstrap_fast complete: {written} fetched -> {out_path}")
    if written == 0:
        logger.error("No records written — treating as failure")
        return 1
    return 0


def main():
    """Main entry point for testing and bootstrap"""

    command = sys.argv[1] if len(sys.argv) > 1 else None

    # The fleet wrapper invokes `bootstrap-fast`; without this alias argparse-less
    # scrapers fall through to sample-only mode (issue #1450).
    if command == 'bootstrap-fast' or (command == 'bootstrap' and '--full' in sys.argv):
        sys.exit(run_full(BundesratFetcher()))

    if command == 'bootstrap':
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
