#!/usr/bin/env python3
"""
European Parliament Adopted Texts Data Fetcher

This fetcher retrieves adopted texts from the European Parliament including:
- Legislative resolutions
- Non-legislative resolutions
- Legislative acts
- Opinions, declarations, decisions
- Recommendations

Approach:
1. Use EP Open Data Portal API (data.europarl.europa.eu) to list documents
2. Extract document metadata from JSON-LD response
3. Fetch full text HTML from doceo pages (europarl.europa.eu/doceo/document/)
4. Parse HTML to extract clean text content
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from html.parser import HTMLParser

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_BASE = "https://data.europarl.europa.eu/api/v2/adopted-texts"
DOCEO_BASE = "https://www.europarl.europa.eu/doceo/document/"
RSS_URL = "https://www.europarl.europa.eu/rss/doc/texts-adopted/en.xml"

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


class TextContentExtractor(HTMLParser):
    """Extract text content from EP adopted texts HTML pages."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.in_content_div = False
        self.content_depth = 0
        self.current_tag = None
        self.skip_tags = {'script', 'style', 'nav', 'header', 'footer', 'aside'}
        self.block_tags = {'p', 'div', 'li', 'td', 'th', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'br'}

    def handle_starttag(self, tag, attrs):
        attrs_dict = dict(attrs)
        self.current_tag = tag

        # Start capturing content in main document area
        if attrs_dict.get('id') == 'TexteAdopte' or attrs_dict.get('class', '').find('doc-ti') >= 0:
            self.in_content_div = True
            self.content_depth = 1
        elif self.in_content_div:
            self.content_depth += 1

        # Handle line breaks
        if tag in ('br', 'p', 'div', 'li'):
            if self.text_parts and not self.text_parts[-1].endswith('\n'):
                self.text_parts.append('\n')

    def handle_endtag(self, tag):
        if self.in_content_div:
            self.content_depth -= 1
            if self.content_depth <= 0:
                self.in_content_div = False

        if tag in self.block_tags:
            if self.text_parts and not self.text_parts[-1].endswith('\n'):
                self.text_parts.append('\n')

    def handle_data(self, data):
        if self.current_tag in self.skip_tags:
            return
        stripped = data.strip()
        if stripped:
            self.text_parts.append(stripped + ' ')

    def get_text(self) -> str:
        text = ''.join(self.text_parts)
        # Clean up excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)
        return text.strip()


class EuroParlFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/ld+json'
        })

    def _make_request(self, url: str, params: Optional[Dict] = None,
                      headers: Optional[Dict] = None, silent: bool = False) -> requests.Response:
        """Make HTTP request with retry logic."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, headers=headers, timeout=60)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if not silent:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def _fetch_document_list(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """Fetch list of adopted texts from EP API."""
        params = {
            'limit': limit,
            'offset': offset
        }

        try:
            response = self._make_request(API_BASE, params=params)
            data = response.json()
            return data.get('data', [])
        except Exception as e:
            logger.error(f"Failed to fetch document list: {e}")
            return []

    def _extract_doceo_id(self, eli_id: str) -> Optional[str]:
        """Extract DOCEO document ID from ELI ID.

        Example: eli/dl/doc/TA-9-2020-0335 -> TA-9-2020-0335
        """
        match = re.search(r'(TA-\d+-\d+-\d+)', eli_id)
        if match:
            return match.group(1)
        return None

    def _fetch_full_text(self, doceo_id: str, lang: str = 'EN') -> Optional[str]:
        """Fetch full text HTML from DOCEO page."""
        url = f"{DOCEO_BASE}{doceo_id}_{lang}.html"

        try:
            # Need different headers for HTML
            headers = {
                'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
                'Accept': 'text/html'
            }
            response = self._make_request(url, headers=headers, silent=True)

            # Parse and extract text
            parser = TextContentExtractor()
            parser.feed(response.text)
            text = parser.get_text()

            if not text or len(text) < 100:
                # Fallback: extract all paragraph text
                paragraphs = re.findall(r'<p[^>]*>(.*?)</p>', response.text, re.DOTALL | re.IGNORECASE)
                text = '\n'.join(
                    re.sub(r'<[^>]+>', '', p).strip()
                    for p in paragraphs
                    if len(re.sub(r'<[^>]+>', '', p).strip()) > 10
                )

            return text if text else None

        except Exception as e:
            logger.debug(f"Failed to fetch full text for {doceo_id}: {e}")
            return None

    def _parse_document(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse API document response into normalized record."""
        eli_id = doc.get('id', '')
        doceo_id = self._extract_doceo_id(eli_id)

        if not doceo_id:
            logger.debug(f"Could not extract DOCEO ID from: {eli_id}")
            return None

        # Find English expression for title
        title = None
        expressions = doc.get('is_realized_by', [])
        for expr in expressions:
            if expr.get('id', '').endswith('/en'):
                title = expr.get('title', {}).get('en')
                break

        if not title:
            # Try any available title
            for expr in expressions:
                titles = expr.get('title', {})
                if isinstance(titles, dict):
                    for lang, t in titles.items():
                        if t:
                            title = t
                            break
                elif isinstance(titles, str):
                    title = titles
                if title:
                    break

        if not title:
            title = doceo_id

        return {
            '_id': doceo_id,
            '_source': 'EU/EuroParl',
            '_type': 'legislation',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'eli_id': eli_id,
            'doceo_id': doceo_id,
            'title': title,
            'date': doc.get('document_date'),
            'parliamentary_term': doc.get('parliamentary_term', ''),
            'eurovoc_concepts': doc.get('is_about', []),
            'url': f"{DOCEO_BASE}{doceo_id}_EN.html",
            'text': None  # Will be populated later
        }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw API data into standard schema."""
        return raw  # Already normalized in _parse_document

    def fetch_all(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Yield all adopted text documents with full text."""
        offset = 0
        page_size = 100
        total_fetched = 0

        while True:
            if limit and total_fetched >= limit:
                break

            logger.info(f"Fetching documents with offset {offset}...")
            docs = self._fetch_document_list(limit=page_size, offset=offset)

            if not docs:
                logger.info(f"No more documents at offset {offset}")
                break

            for doc in docs:
                if limit and total_fetched >= limit:
                    break

                record = self._parse_document(doc)
                if not record:
                    continue

                # Fetch full text
                full_text = self._fetch_full_text(record['doceo_id'])
                if full_text and len(full_text) > 100:
                    record['text'] = full_text
                    total_fetched += 1
                    yield record
                else:
                    logger.debug(f"No full text for {record['doceo_id']}")

                time.sleep(0.5)  # Rate limiting

            offset += page_size
            time.sleep(1)  # Delay between pages

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Yield documents modified since the given date."""
        for record in self.fetch_all():
            if record.get('date'):
                try:
                    doc_date = datetime.fromisoformat(record['date'].replace('Z', '+00:00'))
                    if doc_date.replace(tzinfo=None) >= since.replace(tzinfo=None):
                        yield record
                except ValueError:
                    yield record


def bootstrap_sample(count: int = 12) -> List[Dict[str, Any]]:
    """Fetch sample records for testing."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    fetcher = EuroParlFetcher()
    samples = []

    logger.info(f"Fetching {count} sample records...")

    for i, record in enumerate(fetcher.fetch_all(limit=count)):
        samples.append(record)

        # Save individual record
        filename = SAMPLE_DIR / f"{record['_id']}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved {i+1}/{count}: {record['_id']} ({len(record.get('text', ''))} chars)")

    # Save combined file
    combined_file = SAMPLE_DIR / "_all_samples.json"
    with open(combined_file, 'w', encoding='utf-8') as f:
        json.dump(samples, f, indent=2, ensure_ascii=False)

    return samples


def validate_samples() -> bool:
    """Validate sample records meet quality requirements."""
    combined_file = SAMPLE_DIR / "_all_samples.json"

    if not combined_file.exists():
        logger.error("No sample file found")
        return False

    with open(combined_file, 'r', encoding='utf-8') as f:
        samples = json.load(f)

    if len(samples) < 10:
        logger.error(f"Only {len(samples)} samples, need at least 10")
        return False

    # Check required fields
    required_fields = ['_id', '_source', '_type', '_fetched_at', 'title', 'text', 'date', 'url']
    empty_text_count = 0
    total_chars = 0

    for i, record in enumerate(samples):
        missing = [f for f in required_fields if not record.get(f)]
        if missing:
            logger.warning(f"Record {i} missing fields: {missing}")

        text = record.get('text', '')
        if not text or len(text) < 100:
            empty_text_count += 1
            logger.warning(f"Record {i} ({record.get('_id')}) has insufficient text: {len(text)} chars")
        else:
            total_chars += len(text)

    if empty_text_count > len(samples) * 0.3:
        logger.error(f"Too many records without full text: {empty_text_count}/{len(samples)}")
        return False

    avg_chars = total_chars // max(1, len(samples) - empty_text_count)
    logger.info(f"Validation passed: {len(samples)} records, avg {avg_chars} chars/doc")
    return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description='EU/EuroParl Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'validate', 'fetch'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Run in sample mode (12 records)')
    parser.add_argument('--limit', type=int, default=None,
                       help='Limit number of records to fetch')
    parser.add_argument('--output', type=str, default=None,
                       help='Output file path')
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == 'bootstrap':
        limit = 12 if args.sample else (args.limit or 12)
        samples = bootstrap_sample(count=limit)
        print(f"\nBootstrap complete: {len(samples)} records")

        # Quick stats
        total_chars = sum(len(r.get('text', '')) for r in samples)
        avg_chars = total_chars // max(1, len(samples))
        print(f"Total text: {total_chars:,} chars")
        print(f"Average: {avg_chars:,} chars/doc")

    elif args.command == 'validate':
        if validate_samples():
            print("Validation PASSED")
        else:
            print("Validation FAILED")
            exit(1)

    elif args.command == 'fetch':
        fetcher = EuroParlFetcher()
        records = list(fetcher.fetch_all(limit=args.limit))

        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                json.dump(records, f, indent=2, ensure_ascii=False)
        else:
            print(json.dumps(records, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
