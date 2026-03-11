#!/usr/bin/env python3
"""
Swedish Legislation (SFS) Data Fetcher

Official open data from the Riksdag (Swedish Parliament)
https://data.riksdagen.se

This fetcher uses the Riksdag's open data API to download Swedish legislation
(Svenska Författningssamlingen - SFS) with full text content.

Data structure:
- List API: /dokumentlista/?doktyp=sfs&utformat=json
- Full text: /dokument/{dok_id}.text
- Full JSON: /dokument/{dok_id}.json

No authentication required. Data is public domain.
"""

import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://data.riksdagen.se"
LIST_URL = f"{BASE_URL}/dokumentlista/"
DOC_URL = f"{BASE_URL}/dokument"


class SwedishLegislationFetcher:
    """Fetcher for Swedish legislation (SFS) from Riksdagen"""

    def __init__(self, slow_mode: bool = False):
        """
        Initialize the fetcher.

        Args:
            slow_mode: If True, use longer delays between requests (for VPS/datacenter IPs)
        """
        self.session = requests.Session()
        self.slow_mode = slow_mode

        # Delays (in seconds) - VPS mode uses much longer delays to avoid rate limiting
        # The Riksdag API is aggressive about blocking VPS/datacenter IPs
        self.doc_delay = 8.0 if slow_mode else 3.0      # Between document fetches
        self.page_delay = 30.0 if slow_mode else 5.0    # Between page requests
        self.list_delay = 15.0 if slow_mode else 3.0    # Before fetching document list

        # Configure retry strategy - avoid retrying on 500 errors (causes issues with this API)
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,  # 2, 4, 8, 16, 32 second backoffs
            status_forcelist=[429, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/plain, application/json, */*',
            'Accept-Language': 'en-US,en;q=0.9,sv;q=0.8',
        })

        if slow_mode:
            logger.info("Running in SLOW MODE - using longer delays for VPS/datacenter IPs")

    def _fetch_document_list(self, page: int = 1, from_date: str = None, to_date: str = None) -> Dict:
        """Fetch a page of SFS documents using curl"""
        params = f"doktyp=sfs&utformat=json&p={page}&sort=datum&sortorder=desc"
        if from_date:
            params += f"&from={from_date}"
        if to_date:
            params += f"&tom={to_date}"

        url = f"{LIST_URL}?{params}"
        max_attempts = 8 if self.slow_mode else 5

        # Add delay before fetching document list (especially important for VPS)
        if self.slow_mode and page > 1:
            logger.info(f"Waiting {self.page_delay}s before fetching page {page}...")
            time.sleep(self.page_delay)

        for attempt in range(max_attempts):
            try:
                # Small delay before list request to avoid hammering the API
                if self.slow_mode:
                    time.sleep(self.list_delay)

                result = subprocess.run(
                    ['curl', '-s', '--max-time', '90', '-L', url],
                    capture_output=True,
                    text=True,
                    timeout=100
                )
                if result.returncode == 0 and result.stdout:
                    return json.loads(result.stdout)
                else:
                    # Exponential backoff
                    delay = min(10 * (2 ** attempt), 300)  # Max 5 min
                    logger.warning(f"Curl list failed on attempt {attempt+1}, waiting {delay}s...")
                    time.sleep(delay)
            except subprocess.TimeoutExpired:
                delay = min(10 * (2 ** attempt), 300)
                logger.warning(f"List timeout on attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except json.JSONDecodeError as e:
                delay = min(10 * (2 ** attempt), 300)
                logger.warning(f"JSON decode error on attempt {attempt+1}: {e}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"Connection error on attempt {attempt+1}: {e}")
                if attempt < max_attempts - 1:
                    delay = min(10 * (2 ** attempt), 300)
                    time.sleep(delay)
                else:
                    raise
        raise Exception(f"Failed to fetch document list after {max_attempts} attempts")

    def _fetch_full_text(self, dok_id: str) -> Optional[str]:
        """Fetch the full text of a document using curl (more reliable on macOS)"""
        url = f"{DOC_URL}/{dok_id}.text"
        max_attempts = 5 if self.slow_mode else 3

        for attempt in range(max_attempts):
            try:
                # Use curl via subprocess - more reliable on macOS with LibreSSL
                result = subprocess.run(
                    ['curl', '-s', '--max-time', '60', '-L', url],
                    capture_output=True,
                    text=True,
                    timeout=70
                )
                if result.returncode == 0 and result.stdout and len(result.stdout) > 50:
                    return result.stdout
                else:
                    delay = min(5 * (2 ** attempt), 120)  # Max 2 min
                    logger.warning(f"Curl failed for {dok_id}, attempt {attempt+1}, waiting {delay}s...")
                    time.sleep(delay)
            except subprocess.TimeoutExpired:
                delay = min(5 * (2 ** attempt), 120)
                logger.warning(f"Timeout for {dok_id}, attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"Error fetching {dok_id}: {e}")
                if attempt < max_attempts - 1:
                    delay = min(5 * (2 ** attempt), 120)
                    time.sleep(delay)
                else:
                    return None
        return None

    def _fetch_document_json(self, dok_id: str) -> Optional[Dict]:
        """Fetch full JSON metadata for a document"""
        url = f"{DOC_URL}/{dok_id}.json"
        try:
            response = self.session.get(url, timeout=60)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.warning(f"Failed to fetch JSON for {dok_id}: {e}")
            return None

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text"""
        if not text:
            return ""
        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        # Remove any HTML tags that might have slipped through
        text = re.sub(r'<[^>]+>', '', text)
        return text.strip()

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all Swedish SFS legislation with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        page = 1
        count = 0
        total_docs = None
        consecutive_failures = 0
        max_consecutive_failures = 5

        while True:
            logger.info(f"Fetching page {page}...")
            try:
                data = self._fetch_document_list(page=page)
                consecutive_failures = 0  # Reset on success
            except Exception as e:
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.error(f"Too many consecutive failures ({consecutive_failures}), stopping")
                    raise
                # Exponential backoff before retry
                backoff = min(60 * (2 ** consecutive_failures), 600)  # Max 10 min
                logger.warning(f"List fetch failed ({e}), waiting {backoff}s before retry...")
                time.sleep(backoff)
                continue

            doc_list = data.get('dokumentlista', {})
            documents = doc_list.get('dokument', [])

            if total_docs is None:
                total_docs = int(doc_list.get('@traffar', 0))
                logger.info(f"Total documents available: {total_docs}")

            if not documents:
                break

            doc_failures = 0
            for doc in documents:
                dok_id = doc.get('dok_id', '')
                if not dok_id:
                    continue

                logger.info(f"[{count+1}] Fetching: {doc.get('titel', '')[:60]}...")

                # Fetch full text
                full_text = self._fetch_full_text(dok_id)

                if full_text and len(full_text) > 100:
                    doc['full_text'] = self._clean_text(full_text)
                    yield doc
                    count += 1
                    doc_failures = 0  # Reset on success

                    if limit and count >= limit:
                        return
                else:
                    doc_failures += 1
                    logger.warning(f"Skipping {dok_id}: no text or text too short")

                    # If we're getting too many failures, the API might be blocking us
                    if doc_failures >= 3 and self.slow_mode:
                        backoff = 120  # 2 minutes
                        logger.warning(f"Multiple document failures, cooling down for {backoff}s...")
                        time.sleep(backoff)
                        doc_failures = 0

                # Rate limiting - use configured delay
                time.sleep(self.doc_delay)

            # Check for next page
            next_page = doc_list.get('@nasta_sida')
            if not next_page:
                break
            page += 1

        logger.info(f"Fetched {count} documents with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents modified since a given date"""
        from_date = since.strftime('%Y-%m-%d')
        
        page = 1
        count = 0
        
        while True:
            logger.info(f"Fetching updates page {page} (from {from_date})...")
            data = self._fetch_document_list(page=page, from_date=from_date)
            
            doc_list = data.get('dokumentlista', {})
            documents = doc_list.get('dokument', [])
            
            if not documents:
                break
                
            for doc in documents:
                dok_id = doc.get('dok_id', '')
                if not dok_id:
                    continue
                
                full_text = self._fetch_full_text(dok_id)
                
                if full_text and len(full_text) > 100:
                    doc['full_text'] = self._clean_text(full_text)
                    yield doc
                    count += 1
                
                time.sleep(1.5)
            
            next_page = doc_list.get('@nasta_sida')
            if not next_page:
                break
            page += 1
            
        logger.info(f"Fetched {count} updated documents")

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        dok_id = raw_doc.get('dok_id', '')
        beteckning = raw_doc.get('beteckning', '')
        
        # Build URL to the law on riksdagen.se
        url = raw_doc.get('url', '')
        if not url and dok_id:
            url = f"https://www.riksdagen.se/sv/dokument-och-lagar/dokument/svensk-forfattningssamling/{dok_id}/"
        
        # Parse date
        date_str = raw_doc.get('datum', '')
        
        # Get document type from sokdata if available
        sokdata = raw_doc.get('sokdata', {})
        doc_type = sokdata.get('soktyp', 'legislation')
        
        return {
            '_id': dok_id,
            '_source': 'SE/SvenskaForfattningssamlingen',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('titel', ''),
            'subtitle': raw_doc.get('undertitel', ''),
            'sfs_number': beteckning,
            'text': raw_doc.get('full_text', ''),
            'date': date_str if date_str else None,
            'published': raw_doc.get('publicerad', ''),
            'url': url,
            'language': 'sv',
            'summary': raw_doc.get('summary', ''),
            'organ': raw_doc.get('organ', ''),
            'document_name': raw_doc.get('dokumentnamn', '')
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        # Use slow mode if --slow flag is passed or VPS_MODE env var is set
        # Default to slow mode for VPS safety (opt-out with --fast)
        is_fast = '--fast' in sys.argv
        slow_mode = not is_fast and ('--slow' in sys.argv or os.environ.get('VPS_MODE') == '1' or True)
        fetcher = SwedishLegislationFetcher(slow_mode=slow_mode)

        if slow_mode:
            logger.info("VPS/SLOW MODE: Using conservative rate limiting (8s doc, 30s page delays)")
        else:
            logger.info("FAST MODE: Using standard rate limiting (3s doc, 5s page delays)")
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 10 if '--sample' in sys.argv else 100

        for raw_doc in fetcher.fetch_all(limit=target_count + 5):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_').replace(':', '-')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['sfs_number']} - {normalized['title'][:50]} ({text_len} chars)")
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
        fetcher = SwedishLegislationFetcher()
        print("Testing Swedish Legislation (SFS) fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"SFS Number: {normalized['sfs_number']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
