#!/usr/bin/env python3
"""
EU/ESRB — European Systemic Risk Board Data Fetcher

Fetches recommendations, warnings, opinions, and decisions from the ESRB
using CELLAR SPARQL for discovery + content negotiation for full text.

Approach:
1. SPARQL query to Publications Office CELLAR for all ESRB-authored documents
2. CELLAR content negotiation (HTML) to retrieve full text per CELEX ID
3. HTML→text extraction using html2text
"""

import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

import requests
from bs4 import BeautifulSoup
from urllib.parse import quote
import html2text

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SOURCE_ID = "EU/ESRB"
SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"
CELLAR_CELEX_BASE = "http://publications.europa.eu/resource/celex/"
EURLEX_HTML_BASE = "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:"

# HTML to text converter
h2t = html2text.HTML2Text()
h2t.ignore_links = True
h2t.ignore_images = True
h2t.body_width = 0


class ESRBFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
        })

    def _make_request(self, url: str, headers: Optional[Dict] = None,
                     params: Optional[Dict] = None, timeout: int = 60,
                     max_retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retry logic."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, headers=headers, params=params, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None

    def discover_documents(self, limit: int = 1000) -> List[Dict[str, Any]]:
        """Discover all ESRB documents via SPARQL."""
        query = """
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>

SELECT DISTINCT ?celex ?date ?title WHERE {
  ?work cdm:work_created_by_agent <http://publications.europa.eu/resource/authority/corporate-body/ESRB> .
  ?work cdm:resource_legal_id_celex ?celex .
  OPTIONAL { ?work cdm:work_date_document ?date . }
  ?expr cdm:expression_belongs_to_work ?work .
  ?expr cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> .
  ?expr cdm:expression_title ?title .
} ORDER BY DESC(?date)
"""
        all_docs = []
        offset = 0
        page_size = 500

        while True:
            paged_query = query + f"\nLIMIT {page_size}\nOFFSET {offset}"
            params = {
                'query': paged_query,
                'format': 'application/sparql-results+json'
            }

            response = self._make_request(SPARQL_ENDPOINT, params=params)
            if not response:
                logger.error("SPARQL query failed")
                break

            data = response.json()
            bindings = data.get('results', {}).get('bindings', [])

            if not bindings:
                break

            for b in bindings:
                celex = b.get('celex', {}).get('value', '')
                date_val = b.get('date', {}).get('value', '')
                title = b.get('title', {}).get('value', '')

                if celex:
                    all_docs.append({
                        'celex': celex,
                        'date': date_val,
                        'title': title,
                    })

            logger.info(f"SPARQL: fetched {len(bindings)} documents (offset={offset})")

            if len(bindings) < page_size:
                break
            offset += page_size
            time.sleep(1)

        logger.info(f"Total ESRB documents discovered: {len(all_docs)}")
        return all_docs

    def fetch_full_text(self, celex: str) -> Optional[Dict[str, str]]:
        """Fetch full text for a document via CELLAR content negotiation."""
        encoded_celex = quote(celex, safe='')
        url = f"{CELLAR_CELEX_BASE}{encoded_celex}"
        headers = {
            'Accept': 'text/html, application/xhtml+xml',
            'Accept-Language': 'en, fr;q=0.5',
        }

        response = self._make_request(url, headers=headers)
        if not response:
            return None

        html_content = response.text
        if len(html_content) < 200:
            return None

        text = self._extract_text(html_content)
        if not text or len(text) < 100:
            return None

        return {
            'text': text,
            'url': response.url,
        }

    def _extract_text(self, html: str) -> str:
        """Extract clean text from CELLAR HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        # Remove non-content elements
        for tag in soup(['script', 'style', 'nav', 'header', 'footer', 'meta', 'link']):
            tag.decompose()

        # Try to find main content container
        content = soup.find('body')
        if not content:
            return ""

        text = h2t.handle(str(content))
        # Clean excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()

    def _classify_document(self, title: str) -> str:
        """Classify ESRB document type from title."""
        title_lower = title.lower()
        if 'recommendation' in title_lower:
            return 'recommendation'
        elif 'warning' in title_lower:
            return 'warning'
        elif 'opinion' in title_lower:
            return 'opinion'
        elif 'decision' in title_lower:
            return 'decision'
        elif 'call for' in title_lower or 'vacancy' in title_lower:
            return 'administrative'
        else:
            return 'other'

    def normalize(self, doc: Dict[str, Any], full_text_data: Dict[str, str]) -> Dict[str, Any]:
        """Normalize a document into the standard schema."""
        celex = doc['celex']
        title = doc['title']
        date_str = doc.get('date', '')

        # Parse date
        date_iso = None
        if date_str:
            try:
                date_iso = datetime.fromisoformat(date_str.replace('Z', '+00:00')).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                date_iso = date_str[:10] if len(date_str) >= 10 else None

        doc_type = self._classify_document(title)
        url = full_text_data.get('url', f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}")

        return {
            '_id': celex,
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': full_text_data['text'],
            'date': date_iso,
            'url': url,
            'celex_id': celex,
            'document_type': doc_type,
            'author': 'European Systemic Risk Board',
        }

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all ESRB documents with full text."""
        documents = self.discover_documents()

        if max_docs:
            documents = documents[:max_docs]

        fetched = 0
        for doc in documents:
            celex = doc['celex']

            full_text_data = self.fetch_full_text(celex)
            if not full_text_data:
                logger.warning(f"No full text for {celex}, skipping")
                continue

            record = self.normalize(doc, full_text_data)
            yield record
            fetched += 1

            if fetched % 10 == 0:
                logger.info(f"Progress: {fetched}/{len(documents)} documents fetched")

            time.sleep(1.5)  # Rate limiting

        logger.info(f"Completed: {fetched}/{len(documents)} documents with full text")


def bootstrap(sample_dir: Path, max_samples: int = 15):
    """Bootstrap: fetch sample documents and save to sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    fetcher = ESRBFetcher()
    count = 0

    for record in fetcher.fetch_all(max_docs=max_samples):
        filename = re.sub(r'[^a-zA-Z0-9_()-]', '_', record['_id']) + '.json'
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved sample {count}: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} samples saved to {sample_dir}")
    return count


if __name__ == '__main__':
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description='EU/ESRB Data Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'fetch_all'],
                       help='Command to run')
    parser.add_argument('--sample', action='store_true',
                       help='Run in sample mode (15 documents)')
    parser.add_argument('--max-docs', type=int, default=None,
                       help='Maximum documents to fetch')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        sample_dir = Path(__file__).parent / 'sample'
        max_samples = 15 if args.sample else (args.max_docs or 15)
        count = bootstrap(sample_dir, max_samples=max_samples)
        if count < 10:
            logger.error(f"Only {count} samples fetched, expected at least 10")
            sys.exit(1)
    elif args.command == 'fetch_all':
        fetcher = ESRBFetcher()
        count = 0
        for record in fetcher.fetch_all(max_docs=args.max_docs):
            print(json.dumps(record, ensure_ascii=False))
            count += 1
        logger.info(f"Total records: {count}")
