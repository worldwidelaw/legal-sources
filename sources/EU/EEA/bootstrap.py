#!/usr/bin/env python3
"""
EEA (European Environment Agency) Data Fetcher
Fetches briefings and reports with full text from the EEA Plone API.

Approach:
1. Use the ++api++ JSON endpoint to search for publications
2. Filter by content types: briefing, web_report
3. Fetch individual publications and extract text from 'blocks'
4. Normalize to standard schema

Content types covered:
- briefing: Short online assessments
- web_report: Longer web-based reports
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.eea.europa.eu"
API_BASE = f"{BASE_URL}/++api++/en/analysis/publications"
CONTENT_TYPES = ['briefing', 'web_report']
ITEMS_PER_PAGE = 50


class EEAFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'application/json',
        })

    def _make_request(self, url: str, timeout: int = 30, max_retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    def _extract_text_from_blocks(self, blocks: Dict[str, Any]) -> str:
        """Extract all text content from Plone blocks structure"""
        text_parts = []

        for block_id, block in blocks.items():
            if not isinstance(block, dict):
                continue

            block_type = block.get('@type', '')

            # Slate blocks contain plaintext
            if block_type == 'slate' and block.get('plaintext'):
                text_parts.append(block['plaintext'])

            # Text blocks may have value with nested text
            elif block_type == 'text':
                if block.get('text'):
                    # Rich text - try to extract plain text
                    text = block['text']
                    if isinstance(text, dict) and text.get('data'):
                        text_parts.append(text['data'])
                    elif isinstance(text, str):
                        # Strip HTML tags
                        clean = re.sub(r'<[^>]+>', ' ', text)
                        clean = re.sub(r'\s+', ' ', clean).strip()
                        if clean:
                            text_parts.append(clean)

            # Tables may have data
            elif block_type == 'slateTable':
                if block.get('table'):
                    table = block['table']
                    if isinstance(table, dict):
                        rows = table.get('rows', [])
                        for row in rows:
                            if isinstance(row, dict):
                                cells = row.get('cells', [])
                                for cell in cells:
                                    if isinstance(cell, dict) and cell.get('value'):
                                        for item in cell['value']:
                                            if isinstance(item, dict):
                                                for child in item.get('children', []):
                                                    if isinstance(child, dict) and child.get('text'):
                                                        text_parts.append(child['text'])

            # Recursively extract from nested blocks
            if block.get('blocks'):
                nested_text = self._extract_text_from_blocks(block['blocks'])
                if nested_text:
                    text_parts.append(nested_text)

        # Join and clean text
        full_text = '\n\n'.join(text_parts)
        # Normalize whitespace
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r' {2,}', ' ', full_text)

        return full_text.strip()

    def _fetch_publication_detail(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch full publication data from API"""
        # Convert regular URL to API URL
        if '++api++' not in url:
            api_url = url.replace(BASE_URL, f"{BASE_URL}/++api++")
        else:
            api_url = url

        response = self._make_request(api_url)
        if response is None:
            return None

        try:
            return response.json()
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON from {api_url}")
            return None

    def _search_publications(self, start: int = 0, size: int = ITEMS_PER_PAGE,
                           content_types: List[str] = None) -> Optional[Dict[str, Any]]:
        """Search publications with pagination"""
        params = [
            f"b_start={start}",
            f"b_size={size}",
            "sort_on=effective",
            "sort_order=descending",
        ]

        # Add content type filters
        for ct in (content_types or CONTENT_TYPES):
            params.append(f"portal_type={ct}")

        url = f"{API_BASE}/@search?{'&'.join(params)}"

        response = self._make_request(url)
        if response is None:
            return None

        try:
            return response.json()
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON from search")
            return None

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all EEA publications with full text.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
        """
        fetched = 0
        start = 0

        while True:
            if max_docs is not None and fetched >= max_docs:
                logger.info(f"Reached max_docs limit ({max_docs})")
                return

            logger.info(f"Fetching publications starting at {start}...")
            search_result = self._search_publications(start=start)

            if search_result is None:
                logger.warning("Search request failed")
                break

            items = search_result.get('items', [])
            if not items:
                logger.info("No more publications found")
                break

            for item in items:
                if max_docs is not None and fetched >= max_docs:
                    return

                item_url = item.get('@id', '')
                item_type = item.get('@type', '')

                if not item_url:
                    continue

                logger.info(f"Fetching [{item_type}]: {item_url.split('/')[-1][:50]}...")

                # Fetch full publication data
                pub_data = self._fetch_publication_detail(item_url)
                time.sleep(1.5)  # Rate limiting

                if pub_data is None:
                    continue

                # Extract text from blocks
                blocks = pub_data.get('blocks', {})
                text = self._extract_text_from_blocks(blocks)

                # Also add description if substantial
                description = pub_data.get('description', '')
                if description and len(description) > 50:
                    text = f"{description}\n\n{text}"

                if not text or len(text) < 200:
                    logger.warning(f"Insufficient text for {pub_data.get('UID', 'unknown')}")
                    continue

                # Build raw document
                raw_doc = {
                    'uid': pub_data.get('UID', ''),
                    'title': pub_data.get('title', ''),
                    'description': description,
                    'text': text,
                    'effective': pub_data.get('effective', ''),
                    'modified': pub_data.get('modified', ''),
                    'created': pub_data.get('created', ''),
                    'url': item_url,
                    'content_type': item_type,
                    'topics': pub_data.get('topics', []),
                    'geo_coverage': pub_data.get('geo_coverage', []),
                    'language': pub_data.get('language', {}).get('token', 'en'),
                }

                yield raw_doc
                fetched += 1

                logger.info(f"Fetched: {raw_doc['title'][:60]}... ({len(text):,} chars)")

            # Check if more pages exist
            batching = search_result.get('batching', {})
            if 'next' not in batching:
                logger.info("No more pages")
                break

            start += ITEMS_PER_PAGE
            time.sleep(1.0)  # Rate limiting between pages

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date"""
        for doc in self.fetch_all():
            if doc.get('effective'):
                try:
                    # Parse ISO date
                    effective_str = doc['effective']
                    if 'T' in effective_str:
                        effective = datetime.fromisoformat(effective_str.replace('Z', '+00:00'))
                    else:
                        effective = datetime.strptime(effective_str[:10], '%Y-%m-%d')

                    # Make naive for comparison
                    effective_naive = effective.replace(tzinfo=None)

                    if effective_naive >= since:
                        yield doc
                except Exception:
                    # If date parsing fails, include document
                    yield doc
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        # Parse date
        parsed_date = None
        effective = raw_doc.get('effective', '')
        if effective:
            try:
                if 'T' in effective:
                    dt = datetime.fromisoformat(effective.replace('Z', '+00:00'))
                    parsed_date = dt.strftime('%Y-%m-%d')
                else:
                    parsed_date = effective[:10]
            except Exception:
                pass

        # Build unique ID
        uid = raw_doc.get('uid', '')
        doc_id = f"EEA-{uid}" if uid else f"EEA-{hash(raw_doc.get('url', ''))}"

        # Extract topics as list
        topics = raw_doc.get('topics', [])
        if isinstance(topics, list):
            topic_list = [t.get('title', t) if isinstance(t, dict) else str(t) for t in topics]
        else:
            topic_list = []

        # Extract geo coverage
        geo = raw_doc.get('geo_coverage', [])
        if isinstance(geo, list):
            geo_list = [g.get('title', g) if isinstance(g, dict) else str(g) for g in geo]
        else:
            geo_list = []

        return {
            '_id': doc_id,
            '_source': 'EU/EEA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'uid': uid,
            'title': raw_doc.get('title', ''),
            'description': raw_doc.get('description', ''),
            'text': raw_doc.get('text', ''),
            'date': parsed_date,
            'url': raw_doc.get('url', ''),
            'content_type': raw_doc.get('content_type', ''),
            'topics': topic_list,
            'geo_coverage': geo_list,
            'language': raw_doc.get('language', 'en'),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = EEAFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv

        if is_sample:
            target_count = 15
            logger.info("Fetching sample documents (15 records)...")
        else:
            target_count = 50
            logger.info("Fetching 50 documents (use --sample for quick test)...")

        sample_count = 0

        for raw_doc in fetcher.fetch_all(max_docs=target_count):
            normalized = fetcher.normalize(raw_doc)

            # Validate text content
            if len(normalized.get('text', '')) < 200:
                logger.warning(f"Skipping {normalized['_id']} - insufficient text")
                continue

            # Save to sample directory
            uid = normalized.get('uid', str(sample_count))
            filename = f"{uid}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['title'][:60]}... ({len(normalized['text']):,} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        if sample_count > 0:
            files = list(sample_dir.glob('*.json'))
            total_chars = 0
            for f in files:
                with open(f, 'r') as fp:
                    doc = json.load(fp)
                    total_chars += len(doc.get('text', ''))
            avg_chars = total_chars // len(files) if files else 0
            logger.info(f"Average text length: {avg_chars:,} characters per document")

    else:
        # Test mode
        fetcher = EEAFetcher()

        print("Testing EEA fetcher...")
        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"Content Type: {normalized['content_type']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"URL: {normalized['url']}")
            count += 1

        print(f"\nSuccessfully fetched {count} documents")


if __name__ == '__main__':
    main()
