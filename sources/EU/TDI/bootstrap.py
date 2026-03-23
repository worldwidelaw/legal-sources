#!/usr/bin/env python3
"""
EU Trade Defence Instruments (TDI) Data Fetcher

Fetches EU anti-dumping, countervailing duty, and safeguard decisions
via SPARQL discovery on the Publications Office CELLAR endpoint,
then retrieves full text HTML from EUR-Lex.

~5,000+ decisions from 1970 to present.
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
import html2text

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://publications.europa.eu/webapi/rdf/sparql"
EURLEX_HTML = "https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:{celex}"
EURLEX_LINK = "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex}"

# TDI title keywords
TDI_KEYWORDS = [
    'anti-dumping',
    'countervailing',
    'anti-subsidy',
    'safeguard measure',
    'safeguard duty',
    'definitive safeguard',
    'provisional safeguard',
]

h2t = html2text.HTML2Text()
h2t.ignore_links = True
h2t.ignore_images = True
h2t.body_width = 0


class TDIFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)'
        })

    def _sparql_query(self, query: str) -> List[Dict]:
        """Execute SPARQL query and return bindings."""
        for attempt in range(3):
            try:
                resp = self.session.get(
                    SPARQL_ENDPOINT,
                    params={'query': query, 'format': 'application/sparql-results+json'},
                    timeout=120
                )
                resp.raise_for_status()
                return resp.json().get('results', {}).get('bindings', [])
            except Exception as e:
                logger.warning(f"SPARQL attempt {attempt+1}/3 failed: {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                else:
                    raise

    def _discover_tdi_documents(self, limit: int = 500, offset: int = 0,
                                 min_year: int = 1970) -> List[Dict[str, Any]]:
        """Discover TDI documents via SPARQL using title keyword filtering."""
        # Build FILTER for TDI keywords
        keyword_filters = " || ".join(
            [f"CONTAINS(LCASE(?title), '{kw}')" for kw in TDI_KEYWORDS]
        )

        query = f'''
PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>

SELECT DISTINCT ?celex ?title ?date ?type WHERE {{
  ?work cdm:resource_legal_id_celex ?celex ;
        cdm:work_date_document ?date .
  ?work cdm:work_has_resource-type ?type .
  ?expr cdm:expression_belongs_to_work ?work ;
        cdm:expression_uses_language <http://publications.europa.eu/resource/authority/language/ENG> ;
        cdm:expression_title ?title .
  FILTER({keyword_filters})
  FILTER(STRSTARTS(?celex, "3"))
  FILTER(YEAR(?date) >= {min_year})
}}
ORDER BY DESC(?date)
LIMIT {limit}
OFFSET {offset}
'''
        bindings = self._sparql_query(query)
        docs = []
        for b in bindings:
            celex = b.get('celex', {}).get('value', '')
            title = b.get('title', {}).get('value', '')
            date_val = b.get('date', {}).get('value', '')
            type_uri = b.get('type', {}).get('value', '')

            # Classify TDI type
            title_lower = title.lower()
            if 'anti-dumping' in title_lower:
                tdi_type = 'anti-dumping'
            elif 'countervailing' in title_lower or 'anti-subsidy' in title_lower:
                tdi_type = 'countervailing'
            elif 'safeguard' in title_lower:
                tdi_type = 'safeguard'
            else:
                tdi_type = 'other'

            docs.append({
                'celex': celex,
                'title': title,
                'date_str': date_val,
                'type_uri': type_uri,
                'tdi_type': tdi_type,
            })

        logger.info(f"SPARQL returned {len(docs)} TDI documents (offset={offset})")
        return docs

    def _fetch_full_text(self, celex: str) -> Optional[str]:
        """Fetch full text HTML from EUR-Lex and extract clean text."""
        url = EURLEX_HTML.format(celex=celex)
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')
            for tag in soup(["script", "style", "nav", "header", "footer"]):
                tag.decompose()

            # Find main content
            content = soup.find('div', class_='texte') or soup.find('div', id='text') or soup.body
            if not content:
                return None

            text = h2t.handle(str(content))
            text = re.sub(r'\n{3,}', '\n\n', text)
            text = re.sub(r' {2,}', ' ', text)
            text = text.strip()

            return text if len(text) > 100 else None
        except Exception as e:
            logger.warning(f"Failed to fetch full text for {celex}: {e}")
            return None

    def fetch_all(self, max_docs: int = None, min_year: int = 1970) -> Iterator[Dict[str, Any]]:
        """Fetch all TDI documents with full text."""
        fetched = 0
        offset = 0
        batch_size = 500

        while True:
            if max_docs is not None and fetched >= max_docs:
                break

            docs = self._discover_tdi_documents(limit=batch_size, offset=offset, min_year=min_year)
            if not docs:
                break

            for doc in docs:
                if max_docs is not None and fetched >= max_docs:
                    break

                celex = doc['celex']
                logger.info(f"Fetching {celex} ({fetched+1}/{max_docs or '?'})...")

                text = self._fetch_full_text(celex)
                if text:
                    doc['text'] = text
                    yield doc
                    fetched += 1
                else:
                    logger.warning(f"No text for {celex}, skipping")

                time.sleep(1)  # Rate limit

            offset += batch_size
            if len(docs) < batch_size:
                break

        logger.info(f"Fetched {fetched} TDI documents total")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date."""
        for doc in self.fetch_all(min_year=since.year):
            if doc.get('date_str'):
                try:
                    doc_date = datetime.fromisoformat(doc['date_str'][:10])
                    if doc_date >= since:
                        yield doc
                except Exception:
                    yield doc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize to standard schema."""
        date = None
        if raw.get('date_str'):
            date = raw['date_str'][:10]

        return {
            '_id': raw['celex'],
            '_source': 'EU/TDI',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'celex_id': raw['celex'],
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': date,
            'url': EURLEX_LINK.format(celex=raw['celex']),
            'tdi_type': raw.get('tdi_type', 'other'),
        }


def main():
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = TDIFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        max_docs = 15 if is_sample else 100
        min_year = 2023 if is_sample else 1970

        logger.info(f"Fetching {'sample' if is_sample else 'full'} TDI documents...")

        count = 0
        for raw in fetcher.fetch_all(max_docs=max_docs, min_year=min_year):
            normalized = fetcher.normalize(raw)

            if len(normalized.get('text', '')) < 100:
                logger.warning(f"Skipping {normalized['_id']} - insufficient text")
                continue

            filename = f"{normalized['_id'].replace(':', '_').replace('/', '_')}.json"
            with open(sample_dir / filename, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['title'][:80]}... ({len(normalized['text'])} chars)")
            count += 1

        logger.info(f"Bootstrap complete. {count} documents saved to {sample_dir}")

        if count > 0:
            files = list(sample_dir.glob('*.json'))
            total = sum(len(json.load(open(f)).get('text', '')) for f in files)
            logger.info(f"Average text length: {total // len(files):,} chars/doc")
    else:
        fetcher = TDIFetcher()
        print("Testing EU/TDI fetcher...")
        for i, raw in enumerate(fetcher.fetch_all(max_docs=3, min_year=2024)):
            n = fetcher.normalize(raw)
            print(f"\n--- {i+1} ---")
            print(f"ID: {n['_id']}, Type: {n['tdi_type']}")
            print(f"Title: {n['title'][:100]}...")
            print(f"Date: {n['date']}, Text: {len(n['text']):,} chars")
            if i >= 2:
                break


if __name__ == '__main__':
    main()
