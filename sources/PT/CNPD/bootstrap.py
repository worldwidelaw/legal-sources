#!/usr/bin/env python3
"""
PT/CNPD -- Portuguese Data Protection Authority Fetcher

Fetches GDPR decisions, opinions, and guidelines from the CNPD (Comissão Nacional
de Proteção de Dados).

Strategy:
  - Discovery: Paginated listing at cnpd.pt/decisoes/historico-de-decisoes/
    with year and type filters, 50 results per page
  - Full text: PDFs via /umbraco/surface/cnpdDecision/download/{ID}
  - Text extraction: pypdf (some PDFs are scanned, text extraction may fail)
  - Focus: Post-RGPD (2018-2026) decisions (~600+ documents)
  - Also includes pre-RGPD decisions back to 1994 for completeness

Data types:
  - Pareceres (Opinions) - primary active type post-RGPD
  - Deliberações (Deliberations)
  - Autorizações (Authorizations) - mostly pre-RGPD
  - Diretrizes (Guidelines)
  - Regulamentos (Regulations)

License: Public (Portuguese government open data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import html as html_mod
import io
import json
import logging
import math
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

try:
    import pypdf
except ImportError:
    pypdf = None

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.cnpd.pt"
LISTING_URL = f"{BASE_URL}/decisoes/historico-de-decisoes/"
DOWNLOAD_URL = f"{BASE_URL}/umbraco/surface/cnpdDecision/download"
PAGE_SIZE = 50

# Decision types
DECISION_TYPES = {
    1: "autorizacao",
    2: "deliberacao",
    3: "registo",
    4: "parecer",
    5: "diretriz",
    6: "regulamento",
}

# Years to fetch (post-RGPD focus, but include earlier for completeness)
# Pre-RGPD autorizacoes are massive (12K+ per year) so we skip type=1 before 2018
YEARS = list(range(2026, 1993, -1))


class CNPDFetcher:
    """Fetcher for Portuguese Data Protection Authority decisions"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'text/html',
        })

    def _extract_text_from_pdf(self, content: bytes) -> str:
        """Extract text from PDF bytes using pypdf"""
        if not pypdf:
            logger.warning("pypdf not available, cannot extract PDF text")
            return ""
        try:
            reader = pypdf.PdfReader(io.BytesIO(content))
            parts = []
            for page in reader.pages[:200]:
                page_text = page.extract_text()
                if page_text:
                    parts.append(page_text)
            text = "\n\n".join(parts)
            return text.strip()
        except Exception as e:
            logger.debug(f"PDF extraction failed: {e}")
            return ""

    def _fetch_with_retry(self, url: str, params: dict = None, retries: int = 2, timeout: int = 60) -> Optional[requests.Response]:
        """Fetch URL with retries"""
        for attempt in range(retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if attempt < retries:
                    wait = 3 * (attempt + 1)
                    logger.warning(f"Attempt {attempt+1} failed for {url}: {e}. Retry in {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"Failed to fetch {url} after {retries+1} attempts: {e}")
                    return None

    def _parse_listing_page(self, html_content: str) -> List[Dict[str, str]]:
        """Parse listing page to extract decision cards"""
        results = []

        # Extract cards: download ID, title, entity, summary
        cards = re.findall(
            r'cnpdDecision/download/(\d+)[^"]*"[^>]*>\s*'
            r'<div[^>]*c-card-header-medium[^>]*>([^<]+)</div>\s*'
            r'<div[^>]*c-card-header\b[^>]*>([^<]*)</div>\s*'
            r'<div[^>]*c-card-text[^>]*>([^<]*)</div>',
            html_content, re.DOTALL
        )

        for doc_id, title, entity, summary in cards:
            results.append({
                'doc_id': doc_id.strip(),
                'title': html_mod.unescape(title.strip()),
                'entity': html_mod.unescape(entity.strip()),
                'summary': html_mod.unescape(summary.strip()),
            })

        return results

    def _get_total_count(self, html_content: str) -> int:
        """Extract total count from listing page"""
        match = re.search(r'search-count[^>]*>(\d+)', html_content)
        return int(match.group(1)) if match else 0

    def discover_decisions(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Discover all decisions from the listing pages"""
        seen_ids = set()
        total_yielded = 0

        for year in YEARS:
            if limit and total_yielded >= limit:
                return

            # For pre-2018, skip autorizacoes (type 1) as they're massive and mostly formulaic
            # Fetch all types for post-RGPD years
            params = {'year': year, 'pgd': 1}

            response = self._fetch_with_retry(LISTING_URL, params=params)
            if not response:
                continue

            total = self._get_total_count(response.text)
            if total == 0:
                continue

            total_pages = math.ceil(total / PAGE_SIZE)
            logger.info(f"Year {year}: {total} decisions ({total_pages} pages)")

            # Parse first page
            items = self._parse_listing_page(response.text)
            for item in items:
                if limit and total_yielded >= limit:
                    return
                if item['doc_id'] not in seen_ids:
                    seen_ids.add(item['doc_id'])
                    item['year'] = str(year)
                    yield item
                    total_yielded += 1

            # Fetch remaining pages
            for page_num in range(2, total_pages + 1):
                if limit and total_yielded >= limit:
                    return

                params = {'year': year, 'pgd': page_num}
                response = self._fetch_with_retry(LISTING_URL, params=params)
                if not response:
                    break

                items = self._parse_listing_page(response.text)
                if not items:
                    break

                for item in items:
                    if limit and total_yielded >= limit:
                        return
                    if item['doc_id'] not in seen_ids:
                        seen_ids.add(item['doc_id'])
                        item['year'] = str(year)
                        yield item
                        total_yielded += 1

                time.sleep(0.5)

        logger.info(f"Total decisions discovered: {total_yielded}")

    def fetch_decision_text(self, doc_id: str) -> str:
        """Download PDF and extract text for a decision"""
        url = f"{DOWNLOAD_URL}/{doc_id}"
        response = self._fetch_with_retry(url, timeout=90)
        if not response:
            return ""

        content_type = response.headers.get('Content-Type', '').lower()
        if 'pdf' not in content_type:
            return ""

        if len(response.content) > 50_000_000:
            logger.warning(f"Skipping oversized PDF ({len(response.content)} bytes) for id={doc_id}")
            return ""

        return self._extract_text_from_pdf(response.content)

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all decisions with full text"""
        count = 0
        skipped = 0

        for meta in self.discover_decisions(limit=limit * 3 if limit else None):
            if limit and count >= limit:
                return

            doc_id = meta['doc_id']
            logger.info(f"Fetching [{count+1}] id={doc_id}: {meta['title']}...")

            text = self.fetch_decision_text(doc_id)
            if not text or len(text) < 100:
                skipped += 1
                logger.warning(f"No extractable text for id={doc_id} (scanned PDF?). Skipped: {skipped}")
                continue

            meta['text'] = text
            yield meta
            count += 1

            time.sleep(1.5)  # Rate limiting

        logger.info(f"Fetched {count} decisions with full text ({skipped} skipped - scanned PDFs)")

    def _classify_decision(self, title: str) -> str:
        """Classify decision type from title"""
        title_lower = title.lower()
        if 'parecer' in title_lower:
            return 'parecer'
        elif 'delibera' in title_lower:
            return 'deliberacao'
        elif 'autoriza' in title_lower:
            return 'autorizacao'
        elif 'diretriz' in title_lower:
            return 'diretriz'
        elif 'regulamento' in title_lower:
            return 'regulamento'
        elif 'registo' in title_lower:
            return 'registo'
        return 'decision'

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize decision to standard schema"""
        doc_id = raw.get('doc_id', '')
        title = raw.get('title', '')
        decision_type = self._classify_decision(title)

        # Extract date from year
        year = raw.get('year', '')
        date = f"{year}-01-01" if year else ""

        # Try to extract more specific date from title (e.g., "Parecer 54/2025")
        match = re.search(r'(\d+)/(\d{4})', title)
        if match:
            date = f"{match.group(2)}-01-01"

        return {
            '_id': f"PT-CNPD-{doc_id}",
            '_source': 'PT/CNPD',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': raw.get('text', ''),
            'date': date,
            'url': f"{DOWNLOAD_URL}/{doc_id}",
            'language': 'pt',
            'decision_type': decision_type,
            'entity': raw.get('entity', ''),
            'summary': raw.get('summary', ''),
        }


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = CNPDFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None
        limit = target_count + 5 if target_count else None

        logger.info(f"Starting bootstrap (sample={is_sample})...")

        saved = 0
        for raw in fetcher.fetch_all(limit=limit):
            if target_count and saved >= target_count:
                break

            normalized = fetcher.normalize(raw)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            filename = f"{normalized['_id']}.json"
            filename = re.sub(r'[^\w\-.]', '_', filename)
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{saved+1}]: {normalized['title']} ({text_len:,} chars)")
            saved += 1

        logger.info(f"Bootstrap complete. Saved {saved} documents to {sample_dir}")

        # Summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for fp in files:
            with open(fp, 'r', encoding='utf-8') as f:
                data = json.load(f)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        fetcher = CNPDFetcher()
        print("Testing PT/CNPD fetcher...")
        count = 0
        for raw in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw)
            print(f"\n--- Decision {count+1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title']}")
            print(f"Entity: {normalized['entity']}")
            print(f"URL: {normalized['url']}")
            print(f"Text length: {len(normalized['text'])} chars")
            print(f"Text preview: {normalized['text'][:300]}...")
            count += 1


if __name__ == '__main__':
    main()
