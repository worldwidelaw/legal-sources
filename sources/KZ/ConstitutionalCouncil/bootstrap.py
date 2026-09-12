#!/usr/bin/env python3
"""
Kazakhstan Constitutional Court & Council Decisions Fetcher

Fetches normative resolutions from the Constitutional Court (2023+) and
Constitutional Council (1996-2022) of Kazakhstan via the zan.gov.kz REST API.

Data source: https://zan.gov.kz/api/documents/search (actTypes=НПОС)
Filtered by requisites containing "Конституционн" + "Суда" or "Совета".

~94 decisions with full text. No authentication required.
SSL uses Kazakhstan national PKI — requires curl -k.
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_BASE = "https://zan.gov.kz/api"
SEARCH_URL = f"{API_BASE}/documents/search"
DOC_URL = f"{API_BASE}/documents"
PAGE_SIZE = 10


class ConstitutionalCouncilFetcher:
    """Fetcher for Kazakhstan Constitutional Court/Council decisions"""

    def __init__(self, slow_mode: bool = False):
        self.slow_mode = slow_mode
        self.doc_delay = 3.0 if slow_mode else 1.5
        self.page_delay = 5.0 if slow_mode else 2.0

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

    def _is_constitutional_decision(self, doc: dict) -> bool:
        """Check if a document is a Constitutional Court or Council decision"""
        req_rus = doc.get('requisites', {}).get('rus', '')
        return 'Конституционн' in req_rus and ('Суда' in req_rus or 'Совета' in req_rus)

    def _get_body_type(self, doc: dict) -> str:
        """Determine if it's from the Court (2023+) or Council (pre-2023)"""
        req_rus = doc.get('requisites', {}).get('rus', '')
        if 'Суда' in req_rus:
            return 'Constitutional Court'
        return 'Constitutional Council'

    def _extract_text(self, content_blocks: List[dict]) -> str:
        """Extract plain text from the structured content[] array"""
        if not content_blocks:
            return ""
        parts = []
        for block in content_blocks:
            text = block.get('text', '')
            if not text:
                continue
            text = re.sub(r'<[^>]+>', '', text)
            text = text.strip()
            if text:
                block_type = block.get('type', '')
                if block_type in ('title', 'heading'):
                    parts.append(f"\n{text}\n")
                else:
                    parts.append(text)
        full_text = '\n'.join(parts)
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        return full_text.strip()

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all Constitutional Court/Council decisions with full text"""
        page = 1
        count = 0
        total_pages = None
        consecutive_failures = 0

        while True:
            logger.info(f"Fetching search page {page}...")
            data = self._curl_post(SEARCH_URL, {
                'page': page,
                'pageSize': PAGE_SIZE,
                'actTypes': ['НПОС']
            })

            if not data:
                consecutive_failures += 1
                if consecutive_failures >= 5:
                    logger.error("Too many consecutive search failures, stopping")
                    break
                time.sleep(10)
                continue

            consecutive_failures = 0

            if total_pages is None:
                total_pages = data.get('pageCount', 0)
                total_docs = data.get('documentsFound', 0)
                logger.info(f"НПОС total: {total_docs} docs, {total_pages} pages (filtering for Constitutional)")

            doc_list = data.get('list', [])
            if not doc_list:
                break

            for item in doc_list:
                if not self._is_constitutional_decision(item):
                    continue

                doc_id = item.get('id')
                if not doc_id:
                    continue

                body_type = self._get_body_type(item)
                logger.info(f"[{count+1}] Fetching {body_type} decision {doc_id}...")

                full_doc = self._curl_get(f"{DOC_URL}/{doc_id}/rus")
                if full_doc:
                    full_doc['_search_meta'] = item
                    full_doc['_body_type'] = body_type
                    yield full_doc
                    count += 1

                    if limit and count >= limit:
                        return
                else:
                    logger.warning(f"Failed to fetch document {doc_id}")

                time.sleep(self.doc_delay)

            if page >= total_pages:
                break
            page += 1
            time.sleep(self.page_delay)

        logger.info(f"Fetched {count} Constitutional Court/Council decisions total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions modified since a given date"""
        since_str = since.strftime('%Y-%m-%d')
        for doc in self.fetch_all():
            date = doc.get('_search_meta', {}).get('stateAgencyApprovalDate', '')
            if date and date >= since_str:
                yield doc
            elif date and date < since_str:
                break

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        search_meta = raw_doc.get('_search_meta', {})
        content_blocks = raw_doc.get('content', [])
        body_type = raw_doc.get('_body_type', 'Constitutional Court')

        text = self._extract_text(content_blocks)

        summary = search_meta.get('summary', {})
        title = summary.get('rus', '') or summary.get('kaz', '')

        doc_id = search_meta.get('id') or raw_doc.get('id', '')
        code = search_meta.get('code', '')
        date = search_meta.get('stateAgencyApprovalDate', '')
        if not date:
            date = search_meta.get('initialPublicationDate', '')

        requisites = search_meta.get('requisites', {})
        requisites_rus = requisites.get('rus', '') if isinstance(requisites, dict) else str(requisites)

        url = f"https://adilet.zan.kz/rus/docs/{code}" if code else f"https://zan.gov.kz/client/#!/doc/{doc_id}/rus"

        return {
            '_id': str(doc_id),
            '_source': 'KZ/ConstitutionalCouncil',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': text,
            'date': date if date else None,
            'url': url,
            'language': 'ru',
            'code': code,
            'requisites': requisites_rus,
            'body': body_type,
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        slow_mode = '--slow' in sys.argv or os.environ.get('VPS_MODE') == '1'
        fetcher = ConstitutionalCouncilFetcher(slow_mode=slow_mode)

        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 15 if '--sample' in sys.argv else 200

        for raw_doc in fetcher.fetch_all(limit=target_count + 10):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 50:
                logger.warning(f"Skipping doc {normalized['_id']}: text too short ({text_len} chars)")
                continue

            doc_id = str(normalized['_id']).replace('/', '_').replace(':', '-')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized.get('body', '')} | {normalized['title'][:60]} ({text_len} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

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
        print("  bootstrap.py bootstrap             Fetch all documents")
        print("  bootstrap.py validate              Validate sample data")


if __name__ == '__main__':
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
