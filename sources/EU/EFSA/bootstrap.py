#!/usr/bin/env python3
"""
EFSA (European Food Safety Authority) Data Fetcher

Fetches scientific opinions, assessments, and guidance documents from EFSA.

Approach:
1. Use CrossRef API to fetch metadata for EFSA Journal publications (DOI prefix: 10.2903)
2. Extract abstracts which contain the scientific conclusions and recommendations
3. Normalize to standard schema

The EFSA Journal is an open access publication that contains official EU doctrine
on food safety, nutrition, animal health, plant protection, and environmental
risk assessment.

Total available: ~11,500+ publications
"""

import json
import logging
import re
import time
import html
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
CROSSREF_API = "https://api.crossref.org/works"
DOI_PREFIX = "10.2903"  # EFSA Journal DOI prefix
ITEMS_PER_PAGE = 100


class EFSAFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources; mailto:contact@example.com)',
            'Accept': 'application/json',
        })

    def _make_request(self, url: str, params: dict = None, max_retries: int = 3) -> Optional[dict]:
        """Make API request with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    def _clean_abstract(self, abstract: str) -> str:
        """Clean JATS XML and HTML from abstract text"""
        if not abstract:
            return ""

        # Remove JATS XML tags like <jats:p>, <jats:sec>, etc.
        text = re.sub(r'<jats:[^>]+>', '', abstract)
        text = re.sub(r'</jats:[^>]+>', '', text)

        # Remove any remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        text = text.strip()

        return text

    def _parse_date(self, item: dict) -> Optional[str]:
        """Extract and format date from CrossRef item"""
        # Try various date fields in order of preference
        for date_field in ['published-print', 'published-online', 'created', 'issued']:
            if date_field in item and 'date-parts' in item[date_field]:
                parts = item[date_field]['date-parts'][0]
                if len(parts) >= 1:
                    year = parts[0]
                    month = parts[1] if len(parts) > 1 else 1
                    day = parts[2] if len(parts) > 2 else 1
                    try:
                        return f"{year:04d}-{month:02d}-{day:02d}"
                    except (ValueError, TypeError):
                        pass
        return None

    def _extract_authors(self, item: dict) -> List[str]:
        """Extract author names from CrossRef item"""
        authors = []
        for author in item.get('author', []):
            if 'name' in author:
                # Organizational author
                authors.append(author['name'])
            elif 'given' in author and 'family' in author:
                authors.append(f"{author['given']} {author['family']}")
            elif 'family' in author:
                authors.append(author['family'])
        return authors

    def _get_document_type(self, item: dict) -> str:
        """Determine document type from CrossRef metadata"""
        title_lower = item.get('title', [''])[0].lower() if item.get('title') else ''

        # Common EFSA document type patterns
        if 'scientific opinion' in title_lower or 'opinion on' in title_lower:
            return 'scientific_opinion'
        elif 'guidance' in title_lower or 'guidance on' in title_lower:
            return 'guidance'
        elif 'technical report' in title_lower:
            return 'technical_report'
        elif 'scientific report' in title_lower:
            return 'scientific_report'
        elif 'statement' in title_lower:
            return 'statement'
        elif 'conclusion' in title_lower or 'peer review' in title_lower:
            return 'peer_review'
        elif 'assessment' in title_lower or 'risk assessment' in title_lower:
            return 'risk_assessment'
        elif 'safety evaluation' in title_lower:
            return 'safety_evaluation'
        else:
            return 'scientific_output'

    def fetch_all(self, max_docs: int = None, cursor: str = "*") -> Iterator[Dict[str, Any]]:
        """
        Fetch all EFSA publications from CrossRef API.

        Uses deep paging with cursor for efficient iteration through all results.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
            cursor: Starting cursor for pagination (* = beginning)
        """
        fetched = 0
        current_cursor = cursor

        while True:
            if max_docs is not None and fetched >= max_docs:
                logger.info(f"Reached max_docs limit ({max_docs})")
                return

            params = {
                'filter': f'prefix:{DOI_PREFIX}',
                'rows': ITEMS_PER_PAGE,
                'cursor': current_cursor,
                'sort': 'published',
                'order': 'desc',
                'select': 'DOI,title,abstract,author,issued,published-print,published-online,created,container-title,volume,issue,page,URL,type,subject',
            }

            logger.info(f"Fetching page (cursor: {current_cursor[:20] if current_cursor != '*' else '*'})...")

            data = self._make_request(CROSSREF_API, params=params)

            if not data or 'message' not in data:
                logger.warning("Failed to fetch data from CrossRef API")
                break

            message = data['message']
            items = message.get('items', [])

            if not items:
                logger.info("No more items available")
                break

            for item in items:
                if max_docs is not None and fetched >= max_docs:
                    return

                # Skip items without abstract
                abstract = item.get('abstract', '')
                if not abstract:
                    logger.debug(f"Skipping {item.get('DOI')} - no abstract")
                    continue

                cleaned_abstract = self._clean_abstract(abstract)
                if len(cleaned_abstract) < 100:
                    logger.debug(f"Skipping {item.get('DOI')} - abstract too short")
                    continue

                doc = {
                    'doi': item.get('DOI'),
                    'title': item.get('title', [''])[0] if item.get('title') else '',
                    'abstract': cleaned_abstract,
                    'authors': self._extract_authors(item),
                    'date': self._parse_date(item),
                    'journal': item.get('container-title', ['EFSA Journal'])[0] if item.get('container-title') else 'EFSA Journal',
                    'volume': item.get('volume'),
                    'issue': item.get('issue'),
                    'page': item.get('page'),
                    'url': item.get('URL') or f"https://doi.org/{item.get('DOI')}",
                    'type': item.get('type', 'journal-article'),
                    'subjects': item.get('subject', []),
                    'document_type': self._get_document_type(item),
                }

                yield doc
                fetched += 1

                if fetched % 100 == 0:
                    logger.info(f"Fetched {fetched} documents...")

            # Get next cursor
            next_cursor = message.get('next-cursor')
            if not next_cursor or next_cursor == current_cursor:
                logger.info(f"Pagination complete. Total fetched: {fetched}")
                break

            current_cursor = next_cursor
            time.sleep(1.0)  # Rate limiting

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """
        Fetch documents updated since a given date.

        Uses CrossRef's from-pub-date filter for efficient date-based queries.
        """
        since_str = since.strftime('%Y-%m-%d')

        params = {
            'filter': f'prefix:{DOI_PREFIX},from-pub-date:{since_str}',
            'rows': ITEMS_PER_PAGE,
            'cursor': '*',
            'select': 'DOI,title,abstract,author,issued,published-print,published-online,created,container-title,volume,issue,page,URL,type,subject',
        }

        current_cursor = '*'

        while True:
            params['cursor'] = current_cursor

            logger.info(f"Fetching updates since {since_str}...")

            data = self._make_request(CROSSREF_API, params=params)

            if not data or 'message' not in data:
                break

            message = data['message']
            items = message.get('items', [])

            if not items:
                break

            for item in items:
                abstract = item.get('abstract', '')
                if not abstract:
                    continue

                cleaned_abstract = self._clean_abstract(abstract)
                if len(cleaned_abstract) < 100:
                    continue

                doc = {
                    'doi': item.get('DOI'),
                    'title': item.get('title', [''])[0] if item.get('title') else '',
                    'abstract': cleaned_abstract,
                    'authors': self._extract_authors(item),
                    'date': self._parse_date(item),
                    'journal': item.get('container-title', ['EFSA Journal'])[0] if item.get('container-title') else 'EFSA Journal',
                    'volume': item.get('volume'),
                    'issue': item.get('issue'),
                    'page': item.get('page'),
                    'url': item.get('URL') or f"https://doi.org/{item.get('DOI')}",
                    'type': item.get('type', 'journal-article'),
                    'subjects': item.get('subject', []),
                    'document_type': self._get_document_type(item),
                }

                yield doc

            next_cursor = message.get('next-cursor')
            if not next_cursor or next_cursor == current_cursor:
                break

            current_cursor = next_cursor
            time.sleep(1.0)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        doi = raw_doc.get('doi', '')

        # Create a unique ID from DOI
        doc_id = doi.replace('/', '_').replace('.', '-') if doi else f"EFSA-{hash(raw_doc.get('title', ''))}"

        return {
            '_id': doc_id,
            '_source': 'EU/EFSA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'doi': doi,
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('abstract', ''),
            'date': raw_doc.get('date'),
            'url': raw_doc.get('url', ''),
            'authors': raw_doc.get('authors', []),
            'journal': raw_doc.get('journal', 'EFSA Journal'),
            'volume': raw_doc.get('volume'),
            'issue': raw_doc.get('issue'),
            'document_type': raw_doc.get('document_type', 'scientific_output'),
            'subjects': raw_doc.get('subjects', []),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = EFSAFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv

        if is_sample:
            target_count = 20  # Request more to ensure 15+ valid samples
            logger.info("Fetching sample documents (15+ records)...")
        else:
            target_count = 50
            logger.info("Fetching 50 documents (use --sample for quick test)...")

        sample_count = 0

        for raw_doc in fetcher.fetch_all(max_docs=target_count):
            normalized = fetcher.normalize(raw_doc)

            # Validate text content
            if len(normalized.get('text', '')) < 100:
                logger.warning(f"Skipping {normalized['_id']} - insufficient text")
                continue

            # Save to sample directory
            filename = f"{normalized['_id']}.json"
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
        fetcher = EFSAFetcher()

        print("Testing EFSA fetcher...")
        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=5):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"DOI: {normalized['doi']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"Document type: {normalized['document_type']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"Text preview: {normalized.get('text', '')[:200]}...")
            print(f"URL: {normalized['url']}")
            count += 1

        print(f"\nSuccessfully fetched {count} documents")


if __name__ == '__main__':
    main()
