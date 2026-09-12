#!/usr/bin/env python3
"""
Kazakhstan National Bank Regulations Fetcher

Fetches regulatory acts of the National Bank of Kazakhstan from adilet.zan.kz.
Uses the Adilet search filtered by organ code kv=1_117 (National Bank).

~3000+ registered regulatory acts. No authentication required.
SSL uses Kazakhstan national PKI — requires curl -k.
"""

import html
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

ADILET_BASE = "https://adilet.zan.kz"
SEARCH_URL = f"{ADILET_BASE}/rus/search/docs/"
PAGE_SIZE = 10  # Adilet returns ~10-15 docs per page

MONTHS_RU = {
    'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
    'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
    'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12',
}


class NationalBankFetcher:
    """Fetcher for Kazakhstan National Bank regulations from Adilet"""

    def __init__(self, slow_mode: bool = False):
        self.slow_mode = slow_mode
        self.doc_delay = 3.0 if slow_mode else 1.5
        self.page_delay = 5.0 if slow_mode else 2.0

    def _curl_get(self, url: str, max_attempts: int = 3) -> Optional[str]:
        """GET HTML via curl (bypasses Kazakhstan PKI cert issues)"""
        for attempt in range(max_attempts):
            try:
                result = subprocess.run(
                    ['curl', '-s', '-k', '--max-time', '60', url],
                    capture_output=True, text=True, timeout=70
                )
                if result.returncode == 0 and result.stdout:
                    return result.stdout
                delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"GET failed attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except subprocess.TimeoutExpired:
                delay = min(5 * (2 ** attempt), 60)
                logger.warning(f"GET timeout attempt {attempt+1}, waiting {delay}s...")
                time.sleep(delay)
            except Exception as e:
                logger.warning(f"GET error: {e}")
                if attempt < max_attempts - 1:
                    time.sleep(5)
                else:
                    return None
        return None

    def _list_doc_codes(self, page: int = 1) -> List[str]:
        """Get document codes from Adilet NB search page"""
        url = f"{SEARCH_URL}?kv=1_117&page={page}"
        content = self._curl_get(url)
        if not content:
            return []

        # Find V-prefix doc codes (Ministry of Justice registered acts)
        codes = re.findall(r'href="/rus/docs/(V[A-Z0-9_]+)"', content)
        return sorted(set(codes))

    def _get_max_page(self) -> int:
        """Get the maximum page number from pagination"""
        content = self._curl_get(f"{SEARCH_URL}?kv=1_117")
        if not content:
            return 0
        pages = re.findall(r'page=(\d+)', content)
        return max(int(p) for p in pages) if pages else 0

    def _extract_doc(self, code: str) -> Optional[Dict[str, Any]]:
        """Fetch and extract a single document from Adilet"""
        url = f"{ADILET_BASE}/rus/docs/{code}"
        content = self._curl_get(url)
        if not content:
            return None

        # Extract title
        title_match = re.search(r'<title>([^<]+)</title>', content)
        title = ''
        if title_match:
            title = title_match.group(1)
            title = title.replace(' - ИПС "Әділет"', '').strip()
            title = html.unescape(title)

        if not title or title == 'ИПС "Әділет"':
            return None

        # Extract text from content div
        text_match = re.search(
            r'<div[^>]*class="container_gamma text text_new"[^>]*>(.*?)</div>\s*<div[^>]*class="container_omega',
            content, re.DOTALL
        )
        if not text_match:
            return None

        raw_text = text_match.group(1)
        raw_text = re.sub(r'<[^>]+>', '\n', raw_text)
        raw_text = raw_text.replace('&nbsp;', ' ')
        raw_text = html.unescape(raw_text)
        raw_text = re.sub(r'\n\s*\n', '\n', raw_text).strip()
        lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
        text = '\n'.join(lines)

        if len(text) < 50:
            return None

        # Extract date from requisites
        date = None
        date_match = re.search(r'от\s+(\d{1,2})\s+(\w+)\s+(\d{4})\s+года', content)
        if date_match:
            day = date_match.group(1).zfill(2)
            month_str = date_match.group(2).lower()
            year = date_match.group(3)
            month = MONTHS_RU.get(month_str, '')
            if month:
                date = f"{year}-{month}-{day}"

        return {
            'code': code,
            'title': title,
            'text': text,
            'date': date,
            'url': url,
        }

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all National Bank regulations"""
        max_page = self._get_max_page()
        logger.info(f"Adilet NB search: {max_page} pages")

        count = 0
        for page in range(1, max_page + 1):
            logger.info(f"Listing page {page}/{max_page}...")
            codes = self._list_doc_codes(page)
            if not codes:
                logger.warning(f"No codes on page {page}, skipping")
                continue

            logger.info(f"  Found {len(codes)} NB docs on page {page}")

            for code in codes:
                doc = self._extract_doc(code)
                if doc:
                    yield doc
                    count += 1

                    if limit and count >= limit:
                        logger.info(f"Reached limit of {limit}")
                        return
                else:
                    logger.warning(f"  Failed to extract {code}")

                time.sleep(self.doc_delay)

            time.sleep(self.page_delay)

        logger.info(f"Fetched {count} National Bank regulations total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch regulations from recent pages (newest first)"""
        since_str = since.strftime('%Y-%m-%d')
        for doc in self.fetch_all():
            if doc.get('date') and doc['date'] < since_str:
                break
            yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        return {
            '_id': raw_doc['code'],
            '_source': 'KZ/NationalBank',
            '_type': 'legislation',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc['title'],
            'text': raw_doc['text'],
            'date': raw_doc.get('date'),
            'url': raw_doc['url'],
            'language': 'ru',
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        slow_mode = '--slow' in sys.argv or os.environ.get('VPS_MODE') == '1'
        fetcher = NationalBankFetcher(slow_mode=slow_mode)

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
                logger.warning(f"Skipping {normalized['_id']}: text too short ({text_len} chars)")
                continue

            doc_id = normalized['_id'].replace('/', '_').replace(':', '-')
            filepath = sample_dir / f"{doc_id}.json"

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['title'][:60]} ({text_len} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        files = list(sample_dir.glob('*.json'))
        total_chars = sum(len(json.load(open(f))['text']) for f in files)

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    elif len(sys.argv) > 1 and sys.argv[1] == 'validate':
        sample_dir = Path(__file__).parent / 'sample'
        files = list(sample_dir.glob('*.json'))
        if not files:
            print("No sample files. Run bootstrap --sample first.")
            sys.exit(1)

        print(f"Validating {len(files)} sample files...")
        issues = 0
        for f in files:
            data = json.load(open(f))
            text = data.get('text', '')
            if not text or len(text) < 50:
                print(f"  FAIL: {f.name} — text too short ({len(text)} chars)")
                issues += 1
            if not data.get('title'):
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
