#!/usr/bin/env python3
"""
Kazakhstan Legislation (Adilet) Data Fetcher

Official open data from the Ministry of Justice of Kazakhstan
https://zan.gov.kz/

This fetcher uses the zan.gov.kz REST API to download Kazakh legislation
with full structured text content.

Data structure:
- Search API: POST /api/documents/search  (paginated, JSON body)
- Full doc:   GET  /api/documents/{id}/rus (structured content array)

206,000+ documents. No authentication required.
SSL uses Kazakhstan national PKI (not globally trusted) — requires verify=False.
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
from typing import Dict, Any, Iterator, Optional, List

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_BASE = "https://zan.gov.kz/api"
SEARCH_URL = f"{API_BASE}/documents/search"
DOC_URL = f"{API_BASE}/documents"
PAGE_SIZE = 10


class AdiletFetcher:
    """Fetcher for Kazakhstan legislation from zan.gov.kz API"""

    def __init__(self, slow_mode: bool = False):
        self.slow_mode = slow_mode
        self.doc_delay = 3.0 if slow_mode else 1.5
        self.page_delay = 5.0 if slow_mode else 2.0

        if slow_mode:
            logger.info("Running in SLOW MODE — longer delays between requests")

    def _curl_post(self, url: str, body: dict, max_attempts: int = 3) -> Optional[dict]:
        """POST JSON via curl (bypasses Kazakhstan PKI cert issues)"""
        body_json = json.dumps(body)
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    ['curl', '-s', '-k', '--max-time', '60',
                     '-X', 'POST', url,
                     '-H', 'Content-Type: application/json',
                     '-H', 'Accept: application/json',
                     '-d', body_json],
                    capture_output=True, text=True, timeout=70
                )
                if result.returncode == 0 and result.stdout:
                    return json.loads(result.stdout)
                else:
                    delay = min(5 * (2 ** attempt), 60)
                    logger.warning(f"POST failed attempt {attempt+1}, waiting {delay}s...")
                    time.sleep(delay)
            except (subprocess.TimeoutExpired, json.JSONDecodeError) as e:
                delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"POST error attempt {attempt+1}: {e}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"POST unexpected error: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(5)
                else:
                    return None
        return None

    def _curl_get(self, url: str, max_attempts: int = 3) -> Optional[dict]:
        """GET JSON via curl"""
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    ['curl', '-s', '-k', '--max-time', '60',
                     '-H', 'Accept: application/json',
                     url],
                    capture_output=True, text=True, timeout=70
                )
                if result.returncode == 0 and result.stdout:
                    return json.loads(result.stdout)
                else:
                    delay = min(5 * (2 ** attempt), 60)
                    logger.warning(f"GET failed attempt {attempt+1}, waiting {delay}s...")
                    time.sleep(delay)
            except (subprocess.TimeoutExpired, json.JSONDecodeError) as e:
                delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"GET error attempt {attempt+1}: {e}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"GET unexpected error: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(5)
                else:
                    return None
        return None

    def _search_documents(self, page: int = 1, page_size: int = PAGE_SIZE) -> Optional[dict]:
        """Search for documents via the zan.gov.kz API"""
        body = {"page": page, "pageSize": page_size}
        return self._curl_post(SEARCH_URL, body)

    def _fetch_document(self, doc_id: int, lang: str = "rus") -> Optional[dict]:
        """Fetch full document with structured content"""
        url = f"{DOC_URL}/{doc_id}/{lang}"
        return self._curl_get(url)

    def _extract_text(self, content_blocks: List[dict]) -> str:
        """Extract plain text from the structured content[] array"""
        if not content_blocks:
            return ""
        parts = []
        for block in content_blocks:
            text = block.get('text', '')
            if not text:
                continue
            # Clean HTML tags if any
            text = re.sub(r'<[^>]+>', '', text)
            text = text.strip()
            if text:
                block_type = block.get('type', '')
                # Add spacing for headings/titles
                if block_type in ('title', 'heading'):
                    parts.append(f"\n{text}\n")
                else:
                    parts.append(text)
        full_text = '\n'.join(parts)
        # Normalize whitespace
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        return full_text.strip()

    def _build_title(self, doc_data: dict) -> str:
        """Build a title from document metadata"""
        # Try Russian title first, then Kazakh
        metadata = doc_data.get('metadata', {})
        title_rus = metadata.get('titleRus') or metadata.get('title', {}).get('rus', '')
        title_kaz = metadata.get('titleKaz') or metadata.get('title', {}).get('kaz', '')
        return title_rus or title_kaz or ''

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all Kazakhstan legislation with full text"""
        page = 1
        count = 0
        total_docs = None
        consecutive_failures = 0

        while True:
            logger.info(f"Fetching search page {page}...")
            data = self._search_documents(page=page)

            if not data:
                consecutive_failures += 1
                if consecutive_failures >= 5:
                    logger.error("Too many consecutive search failures, stopping")
                    break
                time.sleep(10)
                continue

            consecutive_failures = 0

            if total_docs is None:
                total_docs = data.get('documentsFound', 0)
                total_pages = data.get('pageCount', 0)
                logger.info(f"Total documents: {total_docs}, pages: {total_pages}")

            doc_list = data.get('list', [])
            if not doc_list:
                logger.info("No more documents in search results")
                break

            for item in doc_list:
                doc_id = item.get('id')
                if not doc_id:
                    continue

                logger.info(f"[{count+1}] Fetching document {doc_id}...")
                full_doc = self._fetch_document(doc_id)

                if full_doc:
                    # Merge search metadata with full document
                    full_doc['_search_meta'] = item
                    yield full_doc
                    count += 1

                    if limit and count >= limit:
                        return
                else:
                    logger.warning(f"Failed to fetch document {doc_id}")

                time.sleep(self.doc_delay)

            page += 1
            time.sleep(self.page_delay)

        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents modified since a given date (uses same search, filters by date)"""
        # The search endpoint returns most recent first by default
        # We paginate until we find documents older than 'since'
        page = 1
        count = 0
        since_str = since.strftime('%Y-%m-%d')

        while True:
            data = self._search_documents(page=page)
            if not data:
                break

            doc_list = data.get('list', [])
            if not doc_list:
                break

            found_old = False
            for item in doc_list:
                # Check date
                approval_date = item.get('stateAgencyApprovalDate', '')
                if approval_date and approval_date < since_str:
                    found_old = True
                    break

                doc_id = item.get('id')
                if not doc_id:
                    continue

                full_doc = self._fetch_document(doc_id)
                if full_doc:
                    full_doc['_search_meta'] = item
                    yield full_doc
                    count += 1

                time.sleep(self.doc_delay)

            if found_old:
                break
            page += 1
            time.sleep(self.page_delay)

        logger.info(f"Fetched {count} updated documents since {since_str}")

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        search_meta = raw_doc.get('_search_meta', {})
        metadata = raw_doc.get('metadata', {})
        content_blocks = raw_doc.get('content', [])

        # Extract text from structured content
        text = self._extract_text(content_blocks)

        # Build title
        title = self._build_title(raw_doc)
        if not title:
            # Fallback to search result summary
            summary = search_meta.get('summary', {})
            title = summary.get('rus', '') or summary.get('kaz', '')

        # Document ID and code
        doc_id = search_meta.get('id') or raw_doc.get('id', '')
        code = search_meta.get('code', '')

        # Date
        date = search_meta.get('stateAgencyApprovalDate', '')
        if not date:
            date = search_meta.get('initialPublicationDate', '')

        # Requisites (document number/reference)
        requisites = search_meta.get('requisites', {})
        requisites_rus = requisites.get('rus', '') if isinstance(requisites, dict) else str(requisites)

        # Status
        status = search_meta.get('status', '')

        # Act types
        act_types = search_meta.get('actTypes', [])
        act_type_str = ', '.join(act_types) if act_types else ''

        # URL
        url = f"https://adilet.zan.kz/rus/docs/{code}" if code else f"https://zan.gov.kz/client/#!/doc/{doc_id}/rus"

        return {
            '_id': str(doc_id),
            '_source': 'KZ/Adilet',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': text,
            'date': date if date else None,
            'url': url,
            'language': 'ru',
            'code': code,
            'requisites': requisites_rus,
            'status': status,
            'act_types': act_type_str,
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        is_fast = '--fast' in sys.argv
        slow_mode = not is_fast and ('--slow' in sys.argv or os.environ.get('VPS_MODE') == '1')
        fetcher = AdiletFetcher(slow_mode=slow_mode)

        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 15 if '--sample' in sys.argv else 100

        for raw_doc in fetcher.fetch_all(limit=target_count + 10):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 50:
                logger.warning(f"Skipping doc {normalized['_id']}: text too short ({text_len} chars)")
                continue

            # Save to sample directory
            doc_id = str(normalized['_id']).replace('/', '_').replace(':', '-')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized.get('code', '')} - {normalized['title'][:60]} ({text_len} chars)")
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

    elif len(sys.argv) > 1 and sys.argv[1] == 'updates':
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == '--since' and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]
        if not since_str:
            print("Usage: bootstrap.py updates --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, '%Y-%m-%d')
        fetcher = AdiletFetcher()
        for raw_doc in fetcher.fetch_updates(since):
            normalized = fetcher.normalize(raw_doc)
            print(f"{normalized['_id']}: {normalized['title'][:60]} ({len(normalized.get('text', ''))} chars)")

    elif len(sys.argv) > 1 and sys.argv[1] == 'validate':
        sample_dir = Path(__file__).parent / 'sample'
        files = list(sample_dir.glob('*.json'))
        if not files:
            print("No sample files found. Run bootstrap --sample first.")
            sys.exit(1)

        print(f"Validating {len(files)} sample files...")
        issues = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
            text = data.get('text', '')
            title = data.get('title', '')
            if not text or len(text) < 50:
                print(f"  FAIL: {f.name} — text too short ({len(text)} chars)")
                issues += 1
            if not title:
                print(f"  WARN: {f.name} — no title")
            if '<' in text and '>' in text:
                print(f"  WARN: {f.name} — possible HTML in text")
                issues += 1

        print(f"\nValidation: {len(files)} files, {issues} issues")
        sys.exit(1 if issues > 0 else 0)

    else:
        print("Usage:")
        print("  bootstrap.py bootstrap --sample   Fetch 15 sample documents")
        print("  bootstrap.py bootstrap             Fetch 100 documents")
        print("  bootstrap.py updates --since DATE  Fetch updates since DATE")
        print("  bootstrap.py validate              Validate sample data")


if __name__ == '__main__':
    main()
