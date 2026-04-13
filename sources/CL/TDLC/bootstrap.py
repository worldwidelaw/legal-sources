#!/usr/bin/env python3
"""
CL/TDLC - Chile Competition Tribunal (Tribunal de Defensa de la Libre Competencia)

Fetches sentencias (competition decisions) from the TDLC.
~209 decisions from 2004-2026 with full text from PDF downloads.

Data source: https://www.tdlc.cl/sentencia/
API: WordPress REST API (/wp-json/wp/v2/tdlc-sentencias) + PDF extraction
License: Public domain (official tribunal decisions)
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.tdlc.cl"
SOURCE_ID = "CL/TDLC"
SAMPLE_DIR = Path(__file__).parent / "sample"


class TDLCFetcher:
    """Fetcher for Chile TDLC competition tribunal sentencias."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/json, text/html, */*',
        })

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and decode entities."""
        if not text:
            return ""
        text = unescape(text)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _get_all_sentencias(self) -> List[Dict[str, Any]]:
        """Fetch all sentencias metadata from WP REST API."""
        all_items = []
        page = 1
        while True:
            url = f"{BASE_URL}/wp-json/wp/v2/tdlc-sentencias"
            params = {'per_page': 100, 'page': page, '_embed': ''}
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 400:
                    break  # past last page
                resp.raise_for_status()
                items = resp.json()
                if not items:
                    break
                all_items.extend(items)
                total = int(resp.headers.get('X-WP-Total', 0))
                logger.info(f"Page {page}: {len(items)} items (total: {total})")
                page += 1
                time.sleep(1)
            except requests.RequestException as e:
                logger.error(f"API request failed page {page}: {e}")
                break
        return all_items

    def _extract_pdf_url(self, page_url: str) -> Optional[str]:
        """Fetch an HTML sentencia page and extract the PDF link."""
        try:
            resp = self.session.get(page_url, timeout=30)
            resp.raise_for_status()
            pdfs = re.findall(r'href=["\']([^"\']*\.pdf)["\']', resp.text, re.IGNORECASE)
            # Filter to sentencia PDFs only
            for pdf in pdfs:
                if 'sentencia' in pdf.lower() or 'SENTENCIA' in pdf:
                    return pdf
            # Fall back to first PDF if no sentencia-specific one
            return pdfs[0] if pdfs else None
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch page {page_url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="CL/TDLC",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _resolve_taxonomy(self, item: Dict[str, Any], field: str) -> str:
        """Resolve embedded taxonomy term names."""
        try:
            terms = item.get('_embedded', {}).get('wp:term', [])
            for term_group in terms:
                for term in term_group:
                    if term.get('taxonomy') == field:
                        return term.get('name', '')
        except (KeyError, TypeError, IndexError):
            pass
        return ''

    def normalize(self, item: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize a WP sentencia into standard schema."""
        wp_id = item.get('id', 0)
        title = self._clean_html(item.get('title', {}).get('rendered', ''))
        date_str = item.get('date', '')[:10]
        link = item.get('link', '')

        # Extract sentencia number from title
        num_match = re.search(r'N[°º]\s*(\d+)', title)
        sentencia_num = num_match.group(1) if num_match else str(wp_id)

        # Extract year from title or date
        year_match = re.search(r'/(\d{4})', title)
        year = year_match.group(1) if year_match else date_str[:4]

        # Resolve taxonomy terms
        procedimiento = self._resolve_taxonomy(item, 'procedimiento-sent')
        conducta = self._resolve_taxonomy(item, 'conducta-sent')
        industria = self._resolve_taxonomy(item, 'industria-sent')
        rol = self._resolve_taxonomy(item, 'rol-de-causa-sent')

        return {
            '_id': f"CL-TDLC-{sentencia_num}-{year}",
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.utcnow().isoformat() + 'Z',
            'title': title,
            'text': text.strip(),
            'date': date_str,
            'url': link,
            'sentencia_number': sentencia_num,
            'year': year,
            'procedure_type': procedimiento,
            'conduct_type': conducta,
            'industry': industria,
            'case_role': rol,
            'language': 'spa',
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all normalized sentencias with full text."""
        items = self._get_all_sentencias()
        logger.info(f"Total sentencias from API: {len(items)}")

        for i, item in enumerate(items):
            title = self._clean_html(item.get('title', {}).get('rendered', ''))
            link = item.get('link', '')
            logger.info(f"Processing {i+1}/{len(items)}: {title[:60]}")

            pdf_url = self._extract_pdf_url(link)
            if not pdf_url:
                logger.warning(f"  No PDF found, skipping")
                continue

            text = self._extract_pdf_text(pdf_url)
            if not text:
                logger.warning(f"  No text extracted from PDF, skipping")
                continue

            yield self.normalize(item, text)
            time.sleep(3)

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield sentencias published since a given date."""
        since_dt = datetime.fromisoformat(since)
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_dt = datetime.fromisoformat(doc['date'])
                    if doc_dt >= since_dt:
                        yield doc
                except (ValueError, TypeError):
                    yield doc

    def bootstrap_sample(self, n: int = 15) -> List[Dict[str, Any]]:
        """Fetch a diverse sample of sentencias."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

        items = self._get_all_sentencias()
        if not items:
            logger.error("No sentencias found")
            return []

        # Pick diverse samples: newest, oldest, and spread across the list
        total = len(items)
        indices = set()
        # First 3 (newest)
        for i in range(min(3, total)):
            indices.add(i)
        # Last 3 (oldest)
        for i in range(max(0, total - 3), total):
            indices.add(i)
        # Evenly spaced from middle
        if total > 6:
            step = total // (n - 6 + 1)
            for i in range(3, total - 3, max(1, step)):
                indices.add(i)
                if len(indices) >= n + 5:  # extra buffer for failures
                    break

        indices = sorted(indices)[:n + 5]
        results = []

        for idx in indices:
            if len(results) >= n:
                break
            item = items[idx]
            title = self._clean_html(item.get('title', {}).get('rendered', ''))
            link = item.get('link', '')
            logger.info(f"Sample {len(results)+1}: {title[:60]}")

            pdf_url = self._extract_pdf_url(link)
            if not pdf_url:
                logger.warning(f"  No PDF found, skipping")
                continue

            text = self._extract_pdf_text(pdf_url)
            if not text:
                logger.warning(f"  No text from PDF, skipping")
                continue

            doc = self.normalize(item, text)
            results.append(doc)

            fname = f"{doc['_id']}.json"
            with open(SAMPLE_DIR / fname, 'w', encoding='utf-8') as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)
            logger.info(f"  Saved: {fname} ({len(text)} chars)")

            time.sleep(3)

        logger.info(f"Sample complete: {len(results)} documents saved to {SAMPLE_DIR}")
        return results


def main():
    parser = argparse.ArgumentParser(description='CL/TDLC - Competition Tribunal Fetcher')
    parser.add_argument('command', choices=['bootstrap', 'bootstrap-fast', 'fetch', 'updates'],
                        help='Command to run')
    parser.add_argument('--sample', action='store_true',
                        help='Fetch sample data only (for bootstrap)')
    parser.add_argument('--since', type=str,
                        help='Fetch updates since date (ISO format)')
    parser.add_argument('--limit', type=int, default=15,
                        help='Max documents for sample')
    args = parser.parse_args()

    fetcher = TDLCFetcher()

    if args.command == 'bootstrap':
        if args.sample:
            results = fetcher.bootstrap_sample(n=args.limit)
            print(f"\nSample: {len(results)} documents fetched")
            for r in results:
                text_len = len(r.get('text', ''))
                print(f"  {r['_id']} | {text_len:>6} chars | {r['title'][:60]}")
        else:
            count = 0
            for doc in fetcher.fetch_all():
                count += 1
                if count % 10 == 0:
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
            if count % 10 == 0:
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
