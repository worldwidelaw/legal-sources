#!/usr/bin/env python3
"""
PT/AdC -- Portuguese Competition Authority Fetcher

Fetches competition decisions, communications, working papers, and other doctrine
from the Autoridade da Concorrência (AdC) via their Drupal JSON:API.

Strategy:
  - Discovery: Drupal JSON:API at www.concorrencia.pt/jsonapi
    Content types: node/news (~783), node/communication (~523),
    node/working_paper (~88), node/legislation (~50), node/public_consultation (~41)
  - Full text: body.value field in JSON:API response (HTML, cleaned to plain text)
  - ~1,400+ documents with full text

Data types:
  - Merger notifications and decisions (CCENT)
  - Antitrust decisions (PRC)
  - Press releases and communications
  - Working papers and studies
  - Legislation references
  - Public consultations

License: Public (Portuguese government open data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import hashlib
import html
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, Optional, Set

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.concorrencia.pt"
JSONAPI_BASE = f"{BASE_URL}/jsonapi"
PAGE_SIZE = 50

# Content types to fetch with their JSON:API endpoints
# Only types with body text: working_paper, legislation, consultation have no body
CONTENT_TYPES = [
    ("node/news", "news"),                  # ~783 items (merger/antitrust decisions)
    ("node/communication", "communication"),  # ~523 items (press releases)
    ("node/intervention", "intervention"),    # ~140 items (speeches, presentations)
]


class AdCFetcher:
    """Fetcher for Portuguese Competition Authority doctrine documents"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'application/vnd.api+json',
        })

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML to plain text"""
        if not html_content:
            return ""
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = html.unescape(text)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</li>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\n\s*\n\s*\n+', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def _fetch_with_retry(self, url: str, params: dict = None, retries: int = 2, timeout: int = 60) -> Optional[requests.Response]:
        """Fetch URL with retries"""
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < retries:
                    wait = 3 * (attempt + 1)
                    logger.warning(f"Attempt {attempt+1} failed for {url}: {e}. Retry in {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to fetch {url} after {retries+1} attempts: {e}")
                    return None

    def _make_id(self, uuid: str, content_type: str) -> str:
        """Generate a stable document ID"""
        return f"PT-AdC-{content_type}-{uuid[:12]}"

    def _extract_date(self, attributes: dict) -> str:
        """Extract date from node attributes"""
        # Try field_begin_date first
        date_str = attributes.get('field_begin_date')
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                pass

        # Try created timestamp
        created = attributes.get('created')
        if created:
            try:
                dt = datetime.fromisoformat(created.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                pass

        return ""

    def _classify_document(self, title: str, content_type: str) -> str:
        """Classify document type"""
        title_lower = title.lower()
        if content_type == "legislation":
            return "legislation"
        if 'ccent' in title_lower or 'concentra' in title_lower or 'fusão' in title_lower or 'merger' in title_lower:
            return "merger_decision"
        if 'prc' in title_lower or 'sancion' in title_lower or 'coima' in title_lower:
            return "antitrust_decision"
        if content_type == "working_paper":
            return "working_paper"
        if content_type == "public_consultation":
            return "public_consultation"
        if content_type == "communication":
            return "communication"
        return "decision"

    def discover_documents(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Discover all documents via Drupal JSON:API.

        Yields raw document dicts with: uuid, title, text, date, url, content_type
        """
        seen_uuids: Set[str] = set()
        total_yielded = 0

        for endpoint, content_type in CONTENT_TYPES:
            if limit and total_yielded >= limit:
                return

            logger.info(f"Fetching content type: {endpoint}")
            offset = 0

            while True:
                if limit and total_yielded >= limit:
                    return

                url = f"{JSONAPI_BASE}/{endpoint}"
                params = {
                    'page[limit]': PAGE_SIZE,
                    'page[offset]': offset,
                }

                response = self._fetch_with_retry(url, params=params)
                if not response:
                    break

                try:
                    data = response.json()
                except Exception as e:
                    logger.error(f"JSON parse error for {endpoint} offset={offset}: {e}")
                    break

                items = data.get('data', [])
                if not items:
                    break

                for item in items:
                    if limit and total_yielded >= limit:
                        return

                    uuid = item.get('id', '')
                    if not uuid or uuid in seen_uuids:
                        continue
                    seen_uuids.add(uuid)

                    attributes = item.get('attributes', {})
                    title = attributes.get('title', '')
                    body = attributes.get('body', {}) or {}
                    body_html = body.get('value', '') or body.get('processed', '') or ''

                    text = self._clean_html(body_html)
                    if not text or len(text) < 50:
                        continue

                    # Build URL from path alias
                    path_info = attributes.get('path', {}) or {}
                    alias = path_info.get('alias', '')
                    doc_url = f"{BASE_URL}{alias}" if alias else f"{BASE_URL}/node/{uuid}"

                    date = self._extract_date(attributes)

                    total_yielded += 1
                    yield {
                        'uuid': uuid,
                        'title': title,
                        'text': text,
                        'date': date,
                        'url': doc_url,
                        'content_type': content_type,
                    }

                # Check for next page
                next_link = data.get('links', {}).get('next', {})
                if isinstance(next_link, dict):
                    next_href = next_link.get('href')
                elif isinstance(next_link, str):
                    next_href = next_link
                else:
                    next_href = None

                if not next_href:
                    break

                offset += PAGE_SIZE
                time.sleep(1)  # Rate limiting

            logger.info(f"Content type '{endpoint}': {total_yielded} total documents so far")

        logger.info(f"Total documents discovered: {total_yielded}")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        uuid = raw.get('uuid', '')
        content_type = raw.get('content_type', '')
        title = raw.get('title', '')
        text = raw.get('text', '')

        doc_id = self._make_id(uuid, content_type)
        doc_type = self._classify_document(title, content_type)

        return {
            '_id': doc_id,
            '_source': 'PT/AdC',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': text,
            'date': raw.get('date', ''),
            'url': raw.get('url', ''),
            'language': 'pt',
            'doc_type': doc_type,
        }


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = AdCFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None
        limit = target_count + 5 if target_count else None

        logger.info(f"Starting bootstrap (sample={is_sample})...")

        saved = 0
        for raw in fetcher.discover_documents(limit=limit):
            if target_count and saved >= target_count:
                break

            normalized = fetcher.normalize(raw)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            filename = f"{normalized['_id']}.json"
            filename = re.sub(r'[^\w\-.]', '_', filename)
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{saved+1}]: {normalized['title'][:50]}... ({text_len:,} chars)")
            saved += 1

        logger.info(f"Bootstrap complete. Saved {saved} documents to {sample_dir}")

        # Summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for fp in files:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        fetcher = AdCFetcher()
        print("Testing PT/AdC fetcher...")
        count = 0
        for raw in fetcher.discover_documents(limit=3):
            normalized = fetcher.normalize(raw)
            print(f"\n--- Document {count+1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Type: {normalized['doc_type']}")
            print(f"Date: {normalized['date']}")
            print(f"URL: {normalized['url']}")
            print(f"Text length: {len(normalized['text'])} chars")
            print(f"Text preview: {normalized['text'][:300]}...")
            count += 1


if __name__ == '__main__':
    main()
