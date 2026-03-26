#!/usr/bin/env python3
"""
EU/ECLI Data Fetcher
European Case Law via CELLAR SPARQL + REST API

Uses the CELLAR SPARQL endpoint to discover case law with ECLI identifiers,
then retrieves full text via CELLAR REST API content negotiation.
Covers CJEU and General Court judgments, opinions, and orders.
~66,700 documents available, no authentication required.
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional
from urllib.parse import quote

import requests
from bs4 import BeautifulSoup
import html2text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
CELLAR_CELEX_BASE = "http://publications.europa.eu/resource/celex/"
CDM = "http://publications.europa.eu/ontology/cdm#"
LANG_EN = "http://publications.europa.eu/resource/authority/language/ENG"

h2t = html2text.HTML2Text()
h2t.ignore_links = True
h2t.ignore_images = True
h2t.body_width = 0


class ECLIFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)'
        })

    def _sparql_query(self, query: str, timeout: int = 60) -> list:
        """Execute SPARQL query and return bindings."""
        for attempt in range(3):
            try:
                r = self.session.get(
                    SPARQL_ENDPOINT,
                    params={'query': query, 'format': 'application/json'},
                    timeout=timeout
                )
                r.raise_for_status()
                return r.json()['results']['bindings']
            except Exception as e:
                logger.warning(f"SPARQL attempt {attempt+1}/3 failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return []

    def _get_val(self, row: dict, key: str) -> str:
        """Extract value from SPARQL binding row."""
        return row.get(key, {}).get('value', '')

    def _fetch_full_text(self, celex: str) -> str:
        """Fetch full text of a document via CELLAR REST API."""
        url = f"{CELLAR_CELEX_BASE}{celex}"
        for attempt in range(3):
            try:
                r = self.session.get(
                    url,
                    headers={'Accept': 'text/html', 'Accept-Language': 'en'},
                    timeout=60,
                    allow_redirects=True
                )
                if r.status_code == 200 and len(r.text) > 200:
                    text = h2t.handle(r.text).strip()
                    # Remove excessive whitespace
                    text = re.sub(r'\n{3,}', '\n\n', text)
                    return text
                elif r.status_code == 404:
                    logger.debug(f"No HTML for {celex} (404)")
                    return ''
                else:
                    logger.debug(f"Unexpected status {r.status_code} for {celex}")
            except Exception as e:
                logger.warning(f"Full text fetch attempt {attempt+1}/3 for {celex}: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return ''

    def _discover_cases(self, min_year: int = 1952, max_year: int = None,
                        limit: int = 1000, offset: int = 0) -> list:
        """Discover case law documents via SPARQL."""
        if max_year is None:
            max_year = datetime.now().year

        query = f"""
PREFIX cdm: <{CDM}>
SELECT DISTINCT ?ecli ?celex ?date ?title ?court WHERE {{
  ?work cdm:case-law_ecli ?ecli .
  ?work cdm:resource_legal_id_celex ?celex .
  OPTIONAL {{ ?work cdm:work_date_document ?date }}
  OPTIONAL {{ ?work cdm:case-law_delivered_by_court-formation ?courtUri .
             BIND(STR(?courtUri) AS ?court) }}
  OPTIONAL {{ ?expr cdm:expression_belongs_to_work ?work .
             ?expr cdm:expression_uses_language <{LANG_EN}> .
             ?expr cdm:expression_title ?title }}
  FILTER(?date >= '{min_year}-01-01'^^<http://www.w3.org/2001/XMLSchema#date>
      && ?date <= '{max_year}-12-31'^^<http://www.w3.org/2001/XMLSchema#date>)
}} ORDER BY DESC(?date) LIMIT {limit} OFFSET {offset}
"""
        return self._sparql_query(query)

    def _parse_court(self, court_uri: str, ecli: str = '') -> str:
        """Extract court name from URI or ECLI."""
        if court_uri:
            parts = court_uri.rstrip('/').split('/')
            return parts[-1] if parts else ''
        # Fallback: extract from ECLI (e.g., ECLI:EU:C:2026:245 -> C = Court of Justice)
        if ecli:
            parts = ecli.split(':')
            if len(parts) >= 3:
                court_code = parts[2]
                court_map = {
                    'C': 'Court of Justice',
                    'T': 'General Court',
                    'F': 'Civil Service Tribunal',
                }
                return court_map.get(court_code, court_code)
        return ''

    def fetch_all(self, max_docs: int = None, min_year: int = 1952,
                  max_year: int = None) -> Iterator[Dict[str, Any]]:
        """Yield all case law documents with full text."""
        if max_year is None:
            max_year = datetime.now().year

        fetched = 0
        page_size = 500
        offset = 0
        seen_ecli = set()

        while True:
            if max_docs and fetched >= max_docs:
                break

            rows = self._discover_cases(
                min_year=min_year, max_year=max_year,
                limit=page_size, offset=offset
            )

            if not rows:
                break

            for row in rows:
                if max_docs and fetched >= max_docs:
                    break

                celex = self._get_val(row, 'celex')
                ecli = self._get_val(row, 'ecli')
                if not celex:
                    continue

                # Deduplicate by ECLI (multiple CELEX IDs can map to same ECLI)
                if ecli in seen_ecli:
                    continue
                seen_ecli.add(ecli)

                # Fetch full text
                text = self._fetch_full_text(celex)
                if not text:
                    logger.info(f"No full text for {celex}, skipping")
                    continue

                doc = {
                    'ecli': ecli,
                    'celex': celex,
                    'date_str': self._get_val(row, 'date'),
                    'title': self._get_val(row, 'title'),
                    'court': self._parse_court(self._get_val(row, 'court'), ecli),
                    'text': text,
                    'url': f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"
                }

                fetched += 1
                if fetched % 10 == 0:
                    logger.info(f"Fetched {fetched} documents")

                yield doc
                time.sleep(1)  # Rate limit

            offset += page_size

            if len(rows) < page_size:
                break

        logger.info(f"fetch_all complete. Total: {fetched}")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date."""
        for doc in self.fetch_all(min_year=since.year, max_year=datetime.now().year):
            if doc.get('date_str'):
                try:
                    doc_date = datetime.fromisoformat(doc['date_str'][:10])
                    if doc_date >= since.replace(tzinfo=None):
                        yield doc
                except Exception:
                    yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        parsed_date = None
        if raw_doc.get('date_str'):
            parsed_date = raw_doc['date_str'][:10]

        return {
            '_id': raw_doc['ecli'] or raw_doc['celex'],
            '_source': 'EU/ECLI',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'ecli': raw_doc['ecli'],
            'celex_id': raw_doc['celex'],
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': parsed_date,
            'court': raw_doc.get('court', ''),
            'url': raw_doc['url'],
        }


def main():
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ECLIFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        is_full = '--full' in sys.argv

        if is_sample:
            target = 15
            min_year = 2024
            logger.info(f"Sample mode: fetching {target} recent documents")
        elif is_full:
            target = None
            min_year = 1952
            logger.info("Full mode: fetching all documents")
        else:
            target = 50
            min_year = 2024
            logger.info(f"Default mode: fetching {target} documents")

        count = 0
        for doc in fetcher.fetch_all(max_docs=target, min_year=min_year):
            normalized = fetcher.normalize(doc)
            filename = re.sub(r'[^a-zA-Z0-9_-]', '_', normalized['_id'])[:80]
            filepath = sample_dir / f"{filename}.json"
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
            text_len = len(normalized.get('text', ''))
            logger.info(f"[{count}] {normalized['_id']} - {text_len} chars")

        logger.info(f"Bootstrap complete: {count} documents saved to {sample_dir}")
    else:
        print("Usage: python3 bootstrap.py bootstrap [--sample|--full]")


if __name__ == '__main__':
    main()
