#!/usr/bin/env python3
"""
ECB (European Central Bank) Data Fetcher
Fetches ECB legal acts (decisions, regulations, guidelines, opinions) with full text.

Approach:
1. Query EUR-Lex CELLAR SPARQL endpoint to get ECB document metadata (CELEX IDs)
2. Download full text HTML from EUR-Lex using CELEX identifiers
3. Parse HTML and extract clean text
4. Normalize to standard schema

Document types covered:
- Decisions (CELEX prefix 32xxxD)
- Regulations (CELEX prefix 32xxxR)
- Guidelines (CELEX prefix 32xxxO)
- Opinions (CELEX prefix 52xxxAB)
- Recommendations (CELEX prefix 32xxxH, 52xxxXB)
"""

import json
import logging
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
EURLEX_BASE = "https://eur-lex.europa.eu"
ECB_AUTHOR_URI = "http://publications.europa.eu/resource/authority/corporate-body/ECB"


class ECBFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def _make_request(self, url: str, timeout: int = 60, max_retries: int = 3,
                      headers: Optional[Dict] = None) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        req_headers = self.session.headers.copy()
        if headers:
            req_headers.update(headers)

        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout, headers=req_headers)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    def _sparql_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute SPARQL query against CELLAR endpoint"""
        try:
            response = self.session.post(
                SPARQL_ENDPOINT,
                data={'query': query},
                headers={'Accept': 'application/sparql-results+json'},
                timeout=120
            )
            response.raise_for_status()

            results = response.json()
            return results.get('results', {}).get('bindings', [])
        except Exception as e:
            logger.error(f"SPARQL query failed: {e}")
            return []

    def _get_ecb_documents(self, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Query CELLAR for ECB documents.
        Returns list of documents with CELEX ID, title, date, etc.
        """
        query = f"""
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT DISTINCT ?celex ?title ?date ?docType
WHERE {{
  ?work cdm:work_created_by_agent <{ECB_AUTHOR_URI}> .
  ?work cdm:resource_legal_id_celex ?celex .

  OPTIONAL {{
    ?work cdm:work_title ?title .
    FILTER(LANG(?title) = 'en')
  }}

  OPTIONAL {{
    ?work cdm:work_date_document ?date
  }}

  OPTIONAL {{
    ?work cdm:work_resource_type ?rt .
    ?rt skos:prefLabel ?docType .
    FILTER(LANG(?docType) = 'en')
  }}
}}
ORDER BY DESC(?date)
LIMIT {limit}
OFFSET {offset}
"""
        results = self._sparql_query(query)

        documents = []
        seen_celex = set()

        for binding in results:
            celex = binding.get('celex', {}).get('value', '')
            if not celex or celex in seen_celex:
                continue
            seen_celex.add(celex)

            doc = {
                'celex': celex,
                'title': binding.get('title', {}).get('value', ''),
                'date': binding.get('date', {}).get('value', ''),
                'document_type': binding.get('docType', {}).get('value', 'unknown'),
            }
            documents.append(doc)

        return documents

    def _extract_text_from_html(self, html: str) -> str:
        """Extract clean text from EUR-Lex HTML document"""
        soup = BeautifulSoup(html, 'html.parser')

        # Remove script and style elements
        for element in soup.find_all(['script', 'style', 'nav', 'header', 'footer']):
            element.decompose()

        # Try to find the main document content
        # EUR-Lex uses different containers for different document types
        content_selectors = [
            'div.eli-container',
            'div#TexteOnly',
            'div.texte',
            'div.docHtml',
            'div#document1',
            'body'
        ]

        text_parts = []

        for selector in content_selectors:
            container = soup.select_one(selector)
            if container:
                # Get text from paragraphs and other text elements
                for elem in container.find_all(['p', 'div', 'span', 'td', 'li', 'h1', 'h2', 'h3', 'h4', 'h5']):
                    text = elem.get_text(separator=' ', strip=True)
                    if text and len(text) > 10:
                        text_parts.append(text)

                if text_parts:
                    break

        if not text_parts:
            # Fallback: get all text from body
            text = soup.get_text(separator='\n', strip=True)
        else:
            text = '\n\n'.join(text_parts)

        # Clean up text
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = re.sub(r'\t+', ' ', text)

        # Remove common navigation/footer text
        noise_patterns = [
            r'Official Journal of the European Union.*?(?=\n)',
            r'EUR-Lex - \d+ - EN - EUR-Lex',
            r'Help\s*Print\s*Share',
            r'© European Union.*$',
        ]
        for pattern in noise_patterns:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)

        return text.strip()

    def _fetch_document_text(self, celex: str) -> str:
        """Fetch full text of document from EUR-Lex"""
        # EUR-Lex HTML URL format
        url = f"{EURLEX_BASE}/legal-content/EN/TXT/HTML/?uri=CELEX:{celex}"

        response = self._make_request(url)
        if response is None:
            logger.warning(f"Failed to fetch HTML for {celex}")
            return ""

        text = self._extract_text_from_html(response.text)

        if len(text) < 100:
            # Try alternate format
            url2 = f"{EURLEX_BASE}/legal-content/EN/ALL/?uri=CELEX:{celex}"
            response2 = self._make_request(url2)
            if response2:
                text2 = self._extract_text_from_html(response2.text)
                if len(text2) > len(text):
                    text = text2

        return text

    def _classify_document_type(self, celex: str, doc_type_str: str) -> str:
        """Classify document type based on CELEX number and metadata"""
        celex_upper = celex.upper()

        # CELEX number structure: XYYYYTNNNN
        # X = sector (3 = secondary legislation, 5 = preparatory acts)
        # YYYY = year
        # T = type letter
        # NNNN = number

        if 'D' in celex_upper and celex_upper.startswith('3'):
            return 'decision'
        elif 'R' in celex_upper and celex_upper.startswith('3'):
            return 'regulation'
        elif 'O' in celex_upper and celex_upper.startswith('3'):
            return 'guideline'
        elif 'AB' in celex_upper:
            return 'opinion'
        elif 'XB' in celex_upper or 'H' in celex_upper:
            return 'recommendation'
        elif doc_type_str:
            doc_type_lower = doc_type_str.lower()
            if 'decision' in doc_type_lower:
                return 'decision'
            elif 'regulation' in doc_type_lower:
                return 'regulation'
            elif 'guideline' in doc_type_lower or 'orientation' in doc_type_lower:
                return 'guideline'
            elif 'opinion' in doc_type_lower:
                return 'opinion'
            elif 'recommendation' in doc_type_lower:
                return 'recommendation'

        return 'legal_act'

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all ECB legal acts with full text.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
        """
        logger.info("Querying CELLAR for ECB documents...")

        offset = 0
        batch_size = 500
        fetched = 0

        while True:
            if max_docs is not None and fetched >= max_docs:
                break

            documents = self._get_ecb_documents(limit=batch_size, offset=offset)

            if not documents:
                logger.info(f"No more documents found (offset={offset})")
                break

            logger.info(f"Processing batch of {len(documents)} documents (offset={offset})")

            for doc in documents:
                if max_docs is not None and fetched >= max_docs:
                    return

                celex = doc['celex']
                logger.info(f"Fetching full text for CELEX:{celex}...")

                # Fetch full text
                text = self._fetch_document_text(celex)

                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for {celex} ({len(text)} chars)")
                    continue

                doc['text'] = text
                doc['url'] = f"{EURLEX_BASE}/legal-content/EN/TXT/?uri=CELEX:{celex}"
                doc['document_type'] = self._classify_document_type(celex, doc.get('document_type', ''))

                yield doc
                fetched += 1

                logger.info(f"Fetched {celex} ({len(text):,} chars)")
                time.sleep(1.5)  # Rate limiting

            offset += batch_size
            time.sleep(1.0)

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date"""
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_date = datetime.strptime(doc['date'][:10], '%Y-%m-%d')
                    if doc_date >= since:
                        yield doc
                except Exception:
                    yield doc
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        celex = raw_doc['celex']

        # Parse date
        parsed_date = None
        if raw_doc.get('date'):
            try:
                parsed_date = raw_doc['date'][:10]
            except Exception:
                pass

        # Clean title
        title = raw_doc.get('title', '')
        if not title:
            title = f"ECB Document {celex}"

        return {
            '_id': f"ECB-{celex}",
            '_source': 'EU/ECB',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'celex': celex,
            'document_type': raw_doc.get('document_type', 'legal_act'),
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': parsed_date,
            'url': raw_doc.get('url', f"{EURLEX_BASE}/legal-content/EN/TXT/?uri=CELEX:{celex}"),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ECBFetcher()
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
            if len(normalized.get('text', '')) < 100:
                logger.warning(f"Skipping {normalized['_id']} - insufficient text")
                continue

            # Save to sample directory
            filename = f"{normalized['celex'].replace('/', '_').replace(':', '_')}.json"
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
        fetcher = ECBFetcher()

        print("Testing ECB fetcher...")
        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"CELEX: {normalized['celex']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Type: {normalized['document_type']}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"URL: {normalized['url']}")
            count += 1

        print(f"\nSuccessfully fetched {count} documents")


if __name__ == '__main__':
    main()
