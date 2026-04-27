#!/usr/bin/env python3
"""
CL/Contraloria - Chile Contraloría General Administrative Jurisprudence Fetcher

Fetches dictámenes (administrative legal opinions) from Chile's Comptroller General.
~157,000 documents spanning 1950-2026 with full text.

Data source: https://www.contraloria.cl/web/cgr/buscar-jurisprudencia
API: REST JSON backed by Elasticsearch (POST /apibusca/search/dictamenes)
License: Public domain (official government publications)
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.contraloria.cl"
SOURCE_ID = "CL/Contraloria"
SAMPLE_DIR = Path(__file__).parent / "sample"
PAGE_SIZE = 20  # API returns 20 per page


class ContraloriaFetcher:
    """Fetcher for Chile Contraloría General dictámenes."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        })

    def _search(self, year: int, page: int = 0) -> Dict[str, Any]:
        """Execute a search query for dictámenes in a given year."""
        url = f"{BASE_URL}/apibusca/search/dictamenes"
        payload = {
            "search": "*",
            "options": [
                {"inner_id": "av2", "field": "year_doc_id", "value": str(year), "type": "force_obj"}
            ],
            "order": "date",
            "date_name": "fecha_documento",
            "source": "dictamenes",
            "page": page
        }
        resp = self.session.post(url, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _count_year(self, year: int) -> int:
        """Get total count of dictámenes for a year."""
        url = f"{BASE_URL}/apibusca/count/dictamenes"
        payload = {
            "search": "*",
            "options": [
                {"inner_id": "av2", "field": "year_doc_id", "value": str(year), "type": "force_obj"}
            ]
        }
        try:
            resp = self.session.post(url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            total = data.get('hits', {}).get('total', 0)
            if isinstance(total, dict):
                total = total.get('value', 0)
            return total
        except Exception:
            return 0

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace."""
        if not text:
            return ""
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&[a-zA-Z]+;', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def normalize(self, hit: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize an ES hit into standard schema."""
        src = hit.get('_source', {})

        doc_id = src.get('doc_id', '')
        n_dictamen = src.get('n_dictamen', '')
        year = src.get('year_doc_id', '')

        # Full text: prefer plain text, fall back to HTML-cleaned version
        text = src.get('documento_completo', '') or ''
        if not text.strip():
            raw_html = src.get('documento_completo_raw', '') or ''
            text = self._clean_html(raw_html)

        if not text or len(text.strip()) < 50:
            return None

        # Parse date
        fecha = src.get('fecha_documento', '')
        date = None
        if fecha:
            try:
                date = datetime.fromisoformat(fecha.replace('Z', '+00:00')).strftime('%Y-%m-%d')
            except (ValueError, TypeError):
                date = fecha[:10] if len(fecha) >= 10 else None

        title = src.get('materia', '') or f"Dictamen N° {n_dictamen}"
        descriptors = src.get('descriptores', '')
        criterio = src.get('criterio', '')
        fuentes = src.get('fuentes_legales', '')
        origen = src.get('origen_', '')

        url = f"https://www.contraloria.cl/web/cgr/buscar-jurisprudencia?doc_id={doc_id}"

        return {
            '_id': f"CL-CGR-{doc_id}" if doc_id else f"CL-CGR-{year}-{n_dictamen}",
            '_source': SOURCE_ID,
            '_type': 'doctrine',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': text.strip(),
            'date': date,
            'url': url,
            'dictamen_number': n_dictamen,
            'year': year,
            'criterio': criterio,
            'descriptors': descriptors,
            'legal_sources': fuentes,
            'origin': origen,
            'language': 'spa',
        }

    def fetch_year(self, year: int) -> Iterator[Dict[str, Any]]:
        """Yield all normalized dictámenes for a given year."""
        page = 0
        total_fetched = 0
        while page < 500:  # ES max 10,000 results = 500 pages of 20
            try:
                data = self._search(year, page)
            except requests.RequestException as e:
                logger.error(f"Request failed year={year} page={page}: {e}")
                break

            hits = data.get('hits', {}).get('hits', [])
            if not hits:
                break

            for hit in hits:
                doc = self.normalize(hit)
                if doc:
                    yield doc
                    total_fetched += 1

            page += 1
            time.sleep(2)

        logger.info(f"Year {year}: {total_fetched} documents fetched")

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all normalized documents, iterating by year."""
        current_year = datetime.now().year
        for year in range(1950, current_year + 1):
            count = self._count_year(year)
            if count == 0:
                continue
            logger.info(f"Year {year}: {count} dictámenes")
            yield from self.fetch_year(year)
            time.sleep(1)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield documents modified since a given date."""
        since_dt = datetime.fromisoformat(since)
        current_year = datetime.now().year
        for year in range(since_dt.year, current_year + 1):
            for doc in self.fetch_year(year):
                if doc.get('date'):
                    try:
                        doc_dt = datetime.fromisoformat(doc['date'])
                        if doc_dt >= since_dt:
                            yield doc
                    except (ValueError, TypeError):
                        yield doc
                else:
                    yield doc

    def bootstrap_sample(self, n: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of documents for testing."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        results = []

        # Sample from different decades for diversity
        sample_years = [1970, 1980, 1990, 2000, 2005, 2010, 2015, 2018, 2020, 2022, 2024, 2025, 2026]
        per_year = max(2, n // len(sample_years) + 1)

        for year in sample_years:
            if len(results) >= n:
                break

            try:
                data = self._search(year, page=0)
                hits = data.get('hits', {}).get('hits', [])
                if not hits:
                    logger.info(f"Year {year}: no hits, skipping")
                    continue

                fetched_this_year = 0
                for hit in hits:
                    if fetched_this_year >= per_year or len(results) >= n:
                        break
                    doc = self.normalize(hit)
                    if doc:
                        results.append(doc)
                        fname = f"{doc['_id']}.json"
                        with open(SAMPLE_DIR / fname, 'w', encoding='utf-8') as f:
                            json.dump(doc, f, ensure_ascii=False, indent=2)
                        logger.info(f"  Saved: {fname} ({len(doc['text'])} chars)")
                        fetched_this_year += 1

                time.sleep(2)

            except requests.RequestException as e:
                logger.warning(f"Year {year} failed: {e}")
                continue

        logger.info(f"Sample complete: {len(results)} documents saved to {SAMPLE_DIR}")
        return results


def main():
    parser = argparse.ArgumentParser(description='CL/Contraloria - Contraloría General Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only (for bootstrap)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (ISO format)')
    parser.add_argument('--limit', type=int, default=15,
                        help='Max documents for sample')
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    fetcher = ContraloriaFetcher()

    if args.command == 'bootstrap':
        if args.sample:
            results = fetcher.bootstrap_sample(n=args.limit)
            print(f"\nSample: {len(results)} documents fetched")
            for r in results:
                text_len = len(r.get('text', ''))
                print(f"  {r['_id']} | {r.get('dictamen_number', ''):>8} | {text_len:>6} chars | {r['title'][:60]}")
        else:
            count = 0
            for doc in fetcher.fetch_all():
                count += 1
                if count % 100 == 0:
                    logger.info(f"Fetched {count} documents")
            print(f"Total: {count} documents fetched")

    elif args.command == 'bootstrap-fast':
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        written = 0
        errors = 0
        for doc in fetcher.fetch_all():
            count += 1
            try:
                fname = f"{doc['_id']}.json"
                with open(SAMPLE_DIR / fname, 'w', encoding='utf-8') as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
                written += 1
            except Exception as e:
                logger.error(f"Write error for {doc.get('_id')}: {e}")
                errors += 1
            if count % 100 == 0:
                logger.info(f"Progress: {count} fetched, {written} written")
        print(json.dumps({"records": count, "written": written, "errors": errors}))

    elif args.command == 'updates':
        if not args.since:
            print("Error: --since required for updates command", file=sys.stderr)
            sys.exit(1)
        count = 0
        for doc in fetcher.fetch_updates(args.since):
            count += 1
        print(f"Updates: {count} documents fetched since {args.since}")

    elif args.command == 'fetch':
        count = 0
        for doc in fetcher.fetch_all():
            count += 1
        print(f"Total: {count} documents fetched")


if __name__ == '__main__':
    main()
