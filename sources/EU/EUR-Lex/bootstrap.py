#!/usr/bin/env python3
"""
EUR-Lex Data Fetcher
Official EU legislation, treaties, and CJEU case law

This fetcher uses two approaches:
1. SPARQL endpoint to discover valid CELEX IDs (replacing brute-force enumeration)
2. CELLAR REST API with content negotiation to retrieve full text XHTML

The SPARQL endpoint at publications.europa.eu/webapi/rdf/sparql allows querying
the Cellar metadata database to discover valid documents efficiently.

The CELLAR endpoint at publications.europa.eu/resource/celex/{CELEX} with
appropriate Accept headers returns full text in XHTML format.
"""

import gc
import json
import logging
import re
import socket
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urlencode, quote

import requests
from bs4 import BeautifulSoup
import html2text

# Issue #502: hard safety net against silent socket hangs
socket.setdefaulttimeout(120)

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://eur-lex.europa.eu"
SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"
CELLAR_CELEX_BASE = "http://publications.europa.eu/resource/celex/"
CELLAR_BASE = "http://publications.europa.eu/resource/cellar/"
SEARCH_API = f"{BASE_URL}/search.html"

# Resource type URIs for SPARQL queries
RESOURCE_TYPES = {
    'REG': 'http://publications.europa.eu/resource/authority/resource-type/REG',
    'REG_IMPL': 'http://publications.europa.eu/resource/authority/resource-type/REG_IMPL',
    'REG_DEL': 'http://publications.europa.eu/resource/authority/resource-type/REG_DEL',
    'DIR': 'http://publications.europa.eu/resource/authority/resource-type/DIR',
    'DIR_IMPL': 'http://publications.europa.eu/resource/authority/resource-type/DIR_IMPL',
    'DIR_DEL': 'http://publications.europa.eu/resource/authority/resource-type/DIR_DEL',
    'DEC': 'http://publications.europa.eu/resource/authority/resource-type/DEC',
    'DEC_IMPL': 'http://publications.europa.eu/resource/authority/resource-type/DEC_IMPL',
}

# Initialize HTML to text converter
h2t = html2text.HTML2Text()
h2t.ignore_links = True
h2t.ignore_images = True


class EurLexFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)'
        })

    def _make_request(self, url: str, params: Optional[Dict] = None,
                     headers: Optional[Dict] = None, silent: bool = False) -> requests.Response:
        """Make HTTP request with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, headers=headers, timeout=(15, 60))
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if not silent:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise

    def _discover_celex_via_sparql(self, resource_types: List[str],
                                    limit: int = 1000,
                                    offset: int = 0,
                                    year: int = 2024) -> List[Dict[str, Any]]:
        """
        Discover valid CELEX IDs using SPARQL endpoint for a specific year.

        Uses year-based filtering to avoid SPARQL OFFSET limits (~10K).
        Returns list of dicts with celex, date, and doc_type fields.
        """
        # Build FILTER clause for resource types
        type_filters = " || ".join([f"?type=<{RESOURCE_TYPES[t]}>" for t in resource_types if t in RESOURCE_TYPES])

        query = f'''
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT DISTINCT ?celex ?date ?type ?inforce
WHERE {{
  ?work cdm:work_has_resource-type ?type .
  ?work cdm:resource_legal_id_celex ?celex .
  OPTIONAL {{ ?work cdm:work_date_document ?date . }}
  OPTIONAL {{ ?work cdm:resource_legal_in-force ?inforce . }}
  FILTER({type_filters})
  FILTER(!CONTAINS(?celex, "R("))
  FILTER(YEAR(?date) = {year})
}}
ORDER BY DESC(?date)
LIMIT {limit}
OFFSET {offset}
'''

        params = {
            'query': query,
            'format': 'application/sparql-results+json'
        }

        try:
            response = self._make_request(SPARQL_ENDPOINT, params=params)
            data = response.json()
            results = data.get('results', {}).get('bindings', [])

            documents = []
            for r in results:
                celex = r.get('celex', {}).get('value', '')
                date_val = r.get('date', {}).get('value', '')
                type_uri = r.get('type', {}).get('value', '')
                inforce = r.get('inforce', {}).get('value', '0')

                # Map type URI back to short code
                doc_type = 'REG'  # default
                for code, uri in RESOURCE_TYPES.items():
                    if uri == type_uri:
                        doc_type = code
                        break

                documents.append({
                    'celex': celex,
                    'date_str': date_val,
                    'doc_type': doc_type,
                    'in_force': inforce == '1'
                })

            logger.info(f"SPARQL query returned {len(documents)} documents (offset={offset})")
            return documents

        except Exception as e:
            logger.error(f"SPARQL query failed: {e}")
            return []

    def _fetch_document_via_cellar(self, celex: str, silent: bool = False) -> Optional[Dict[str, Any]]:
        """
        Fetch document full text from CELLAR using content negotiation.

        Uses the celex resource endpoint with Accept headers to retrieve
        XHTML content directly from the Publications Office.
        """
        url = f"{CELLAR_CELEX_BASE}{celex}"

        headers = {
            'Accept-Language': 'en, fr;q=0.8, de;q=0.7',
            'Content-Language': 'en, fr;q=0.8, de;q=0.7',
            'Accept': 'text/html, text/html;type=simplified, application/xhtml+xml, application/xhtml+xml;type=simplified, text/plain'
        }

        try:
            response = self._make_request(url, headers=headers, silent=silent)

            content_type = response.headers.get('Content-Type', '')

            if 'html' in content_type or 'xhtml' in content_type:
                html_content = response.text
                text = self._extract_text_from_html(html_content)
                title = self._extract_title_from_html(html_content)

                if text and len(text) > 100:
                    return {
                        'html': html_content,
                        'text': text,
                        'title': title,
                        'url': response.url
                    }

            return None

        except Exception as e:
            if not silent:
                logger.warning(f"Failed to fetch CELEX {celex}: {e}")
            return None

    def _extract_title_from_html(self, html: str) -> str:
        """Extract title from EUR-Lex HTML document"""
        soup = BeautifulSoup(html, 'html.parser')

        # Look for the actual document title in paragraphs
        # EU documents typically have the title in a P tag with specific structure
        for p in soup.find_all('p'):
            text = p.get_text(strip=True)
            # Look for regulation/directive/decision patterns
            if any(keyword in text.upper() for keyword in ['REGULATION', 'DIRECTIVE', 'DECISION']):
                if '(' in text and ')' in text:  # Has (EU) or similar
                    # Clean and truncate
                    title = re.sub(r'\s+', ' ', text)
                    if len(title) > 20:  # Reasonable title length
                        return title[:500]  # Limit length

        # Fallback to title tag if it's not just a filename
        title_elem = soup.find('title')
        if title_elem:
            title = title_elem.get_text(strip=True)
            # Skip if it's a filename
            if not title.endswith('.xml') and not title.startswith('L_'):
                title = re.sub(r'\s*-\s*EUR-Lex.*$', '', title)
                return title

        return ""

    def _extract_text_from_html(self, html: str) -> str:
        """Extract clean text from EUR-Lex HTML"""
        soup = BeautifulSoup(html, 'html.parser')

        # Remove script and style elements
        for script in soup(["script", "style", "nav", "header", "footer"]):
            script.decompose()

        # Find the main content area
        content = soup.find('div', class_='texte') or soup.find('div', id='text') or soup.body

        if content:
            # Convert to text
            text = h2t.handle(str(content))
            # Clean up excessive whitespace
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            return text.strip()

        return ""

    def fetch_all(self, max_docs: int = None, min_year: int = 1952,
                  max_year: int = None, checkpoint_file: str = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all available documents using year-based SPARQL pagination + CELLAR full text.

        Iterates year-by-year from max_year down to min_year to avoid the SPARQL
        endpoint's ~10K OFFSET ceiling. Within each year, paginates with OFFSET/LIMIT.

        Args:
            max_docs: Maximum documents to fetch. None = unlimited (fetch all).
            min_year: Earliest document year to include. Default 1952 (ECSC founding).
            max_year: Latest document year. Default = current year.
            checkpoint_file: Path to checkpoint file for resume support.
        """
        if max_year is None:
            max_year = datetime.now().year

        fetched = 0
        batch_size = 500
        resume_year = max_year
        resume_offset = 0

        # Load checkpoint if exists
        if checkpoint_file:
            checkpoint_path = Path(checkpoint_file)
            if checkpoint_path.exists():
                try:
                    with open(checkpoint_path, 'r') as f:
                        checkpoint = json.load(f)
                        resume_year = checkpoint.get('current_year', max_year)
                        resume_offset = checkpoint.get('offset', 0)
                        fetched = checkpoint.get('fetched', 0)
                        logger.info(f"Resuming from checkpoint: year={resume_year}, offset={resume_offset}, fetched={fetched}")
                except Exception as e:
                    logger.warning(f"Failed to load checkpoint: {e}")

        # Document types to fetch
        doc_types = ['REG', 'REG_IMPL', 'REG_DEL', 'DIR', 'DIR_IMPL', 'DIR_DEL', 'DEC', 'DEC_IMPL']

        for year in range(resume_year, min_year - 1, -1):
            if max_docs is not None and fetched >= max_docs:
                break

            offset = resume_offset if year == resume_year else 0
            year_fetched = 0
            year_celex_ids = set()  # Per-year dedup only — reset each year to save memory

            while True:
                if max_docs is not None and fetched >= max_docs:
                    break

                logger.info(f"SPARQL discovery: year={year}, offset={offset}...")
                documents = self._discover_celex_via_sparql(
                    resource_types=doc_types,
                    limit=batch_size,
                    offset=offset,
                    year=year
                )

                if not documents:
                    break

                for doc_meta in documents:
                    if max_docs is not None and fetched >= max_docs:
                        break

                    celex = doc_meta['celex']
                    if celex in year_celex_ids:
                        continue

                    doc_content = self._fetch_document_via_cellar(celex, silent=True)

                    if doc_content and doc_content.get('text'):
                        year_celex_ids.add(celex)
                        yield {
                            'celex': celex,
                            'title': doc_content.get('title', f"Document {celex}"),
                            'url': f"{BASE_URL}/legal-content/EN/TXT/?uri=CELEX:{celex}",
                            'date_str': doc_meta.get('date_str'),
                            'doc_type': doc_meta.get('doc_type', 'REG'),
                            'in_force': doc_meta.get('in_force', False),
                            'text': doc_content['text']
                        }
                        fetched += 1
                        year_fetched += 1

                        if fetched % 50 == 0:
                            logger.info(f"Progress: {fetched} docs total, {year_fetched} from {year}")
                            if checkpoint_file:
                                self._save_checkpoint_v2(checkpoint_file, year, offset, fetched)
                    else:
                        logger.debug(f"No text for {celex}")

                    time.sleep(0.5)  # Rate limiting

                offset += batch_size

                if checkpoint_file:
                    self._save_checkpoint_v2(checkpoint_file, year, offset, fetched)

                if len(documents) < batch_size:
                    break

            logger.info(f"Year {year} complete: {year_fetched} documents fetched")
            # Free per-year tracking and force garbage collection
            del year_celex_ids
            gc.collect()

        logger.info(f"fetch_all complete. Total documents fetched: {fetched}")

    def _save_checkpoint_v2(self, checkpoint_file: str, current_year: int,
                            offset: int, fetched: int):
        """Save lightweight checkpoint (year + offset only, no celex ID set)"""
        try:
            checkpoint_path = Path(checkpoint_file)
            with open(checkpoint_path, 'w') as f:
                json.dump({
                    'current_year': current_year,
                    'offset': offset,
                    'fetched': fetched,
                    'timestamp': datetime.now().isoformat()
                }, f)
            logger.debug(f"Checkpoint saved: year={current_year}, offset={offset}, fetched={fetched}")
        except Exception as e:
            logger.warning(f"Failed to save checkpoint: {e}")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """
        Fetch documents updated since a given date.

        Uses SPARQL to filter by document date, then fetches full text.
        """
        since_year = since.year

        for doc in self.fetch_all(max_docs=None, min_year=since_year, max_year=datetime.now().year):
            if doc.get('date_str'):
                try:
                    # Parse ISO date from SPARQL
                    doc_date = datetime.fromisoformat(doc['date_str'].replace('Z', '+00:00'))
                    if doc_date.replace(tzinfo=None) >= since:
                        yield doc
                except Exception as e:
                    # If date parsing fails, include the document anyway
                    logger.warning(f"Failed to parse date '{doc.get('date_str')}': {e}")
                    yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        # Parse date - SPARQL returns ISO format dates
        parsed_date = None
        if raw_doc.get('date_str'):
            try:
                # ISO format from SPARQL (e.g., "2024-01-15")
                parsed_date = raw_doc['date_str'][:10]  # Take just the date part
            except:
                pass

        # Map document types
        doc_type_map = {
            'REG': 'regulation',
            'REG_IMPL': 'implementing_regulation',
            'REG_DEL': 'delegated_regulation',
            'DIR': 'directive',
            'DIR_IMPL': 'implementing_directive',
            'DIR_DEL': 'delegated_directive',
            'DEC': 'decision',
            'DEC_IMPL': 'implementing_decision',
            'JUDG': 'judgment',
            'OPIN': 'opinion'
        }

        doc_type = doc_type_map.get(raw_doc.get('doc_type', ''), 'legislation')

        # Determine if it's case law or legislation
        _type = 'case_law' if doc_type in ['judgment', 'opinion'] else 'legislation'

        return {
            '_id': raw_doc['celex'],
            '_source': 'EU/EUR-Lex',
            '_type': _type,
            '_fetched_at': datetime.now().isoformat(),
            'celex_id': raw_doc['celex'],
            'document_type': doc_type,
            'title': raw_doc['title'],
            'text': raw_doc.get('text', ''),
            'date': parsed_date,
            'url': raw_doc['url'],
            'in_force': raw_doc.get('in_force', False)
        }


def main():
    """Main entry point for testing"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        # Bootstrap mode - fetch sample data
        fetcher = EurLexFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        # Determine mode
        is_sample = '--sample' in sys.argv
        is_full = '--full' in sys.argv

        if is_sample:
            target_count = 15
            min_year = 2024
            max_year = datetime.now().year
            checkpoint_file = None
            logger.info("Fetching sample documents (15 records from 2024+)...")
        elif is_full:
            target_count = None  # Unlimited
            min_year = 1952
            max_year = datetime.now().year
            checkpoint_file = str(Path(__file__).parent / 'checkpoint.json')
            logger.info("Fetching ALL documents (year-by-year from present to 1952)...")
        else:
            target_count = 100
            min_year = 2020
            max_year = datetime.now().year
            checkpoint_file = None
            logger.info("Fetching 100 documents from 2020+ (use --full for complete bootstrap)...")

        sample_count = 0

        for raw_doc in fetcher.fetch_all(max_docs=target_count, min_year=min_year,
                                          max_year=max_year, checkpoint_file=checkpoint_file):
            if target_count is not None and sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)

            # Validate that we have actual text content
            if len(normalized.get('text', '')) < 100:
                logger.warning(f"Skipping {normalized['_id']} - insufficient text content")
                continue

            # Save to sample directory
            filename = f"{normalized['_id'].replace(':', '_').replace('/', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['title'][:80]}... ({len(normalized['text'])} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary statistics
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
        # Test mode - fetch and print a few documents
        fetcher = EurLexFetcher()

        print("Testing EUR-Lex fetcher with SPARQL discovery...")
        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=5, min_year=2024, max_year=datetime.now().year):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:100]}...")
            print(f"Type: {normalized['document_type']}")
            print(f"Date: {normalized['date']}")
            print(f"In force: {normalized.get('in_force', 'N/A')}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"URL: {normalized['url']}")

            count += 1
            if count >= 3:
                break

        print(f"\nSuccessfully fetched {count} documents")


if __name__ == '__main__':
    main()