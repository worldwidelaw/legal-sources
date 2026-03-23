#!/usr/bin/env python3
"""
PT/AT -- Portuguese Tax Authority Doctrine Fetcher

Fetches tax circulars, official letters (ofícios circulados), and binding rulings
(informações vinculativas) from the Portuguese Tax and Customs Authority
(Autoridade Tributária e Aduaneira).

Strategy:
  - Discovery: Azure Cognitive Search API at sitfiscal.portaldasfinancas.gov.pt
    with broad queries (circular, oficio, despacho, informação vinculativa, etc.)
  - Documents: PDFs hosted on SharePoint (info.portaldasfinancas.gov.pt and
    info-aduaneiro.portaldasfinancas.gov.pt)
  - Text extraction: pypdf for PDF text, HTML cleanup for .aspx pages
  - ~4,700+ documents covering circulars, binding rulings, and tax directives

Data types:
  - Circulares (Circulars)
  - Ofícios-circulados (Circular official letters)
  - Informações vinculativas (Binding rulings)
  - Despachos (Directives/Orders)

License: Public (Portuguese government open data)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import hashlib
import io
import json
import logging
import os
import re
import sys
import time
import html
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, Set
from urllib.parse import urlparse, unquote

import requests

try:
    import pypdf
except ImportError:
    pypdf = None

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SEARCH_BASE = "https://sitfiscal.portaldasfinancas.gov.pt/geral/search"
PAGE_SIZE = 10

# Search queries to cover all doctrine document types
SEARCH_QUERIES = [
    "circular",
    "oficio circulado",
    "despacho normativo",
    "informação vinculativa",
    "oficio circular",
    "instrução administrativa",
    "parecer técnico",
]


class ATFetcher:
    """Fetcher for Portuguese Tax Authority doctrine documents"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/json',
        })

    def _extract_text_from_pdf(self, content: bytes) -> str:
        """Extract text from PDF bytes using pypdf or pdfplumber"""
        text = ""

        # Try pypdf first (lighter weight)
        if pypdf:
            try:
                reader = pypdf.PdfReader(io.BytesIO(content))
                parts = []
                for page in reader.pages[:200]:  # Limit pages for safety
                    page_text = page.extract_text()
                    if page_text:
                        parts.append(page_text)
                text = "\n\n".join(parts)
                if len(text.strip()) > 100:
                    return text.strip()
            except Exception as e:
                logger.debug(f"pypdf failed: {e}")

        # Fallback to pdfplumber
        if pdfplumber:
            try:
                with pdfplumber.open(io.BytesIO(content)) as pdf:
                    parts = []
                    for page in pdf.pages[:200]:
                        page_text = page.extract_text()
                        if page_text:
                            parts.append(page_text)
                    text = "\n\n".join(parts)
            except Exception as e:
                logger.debug(f"pdfplumber failed: {e}")

        return text.strip()

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML to plain text"""
        if not html_content:
            return ""
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = html.unescape(text)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def _fetch_with_retry(self, url: str, retries: int = 2, timeout: int = 60) -> Optional[requests.Response]:
        """Fetch URL with retries"""
        for attempt in range(retries + 1):
            try:
                # Use https
                if url.startswith('http://'):
                    url = url.replace('http://', 'https://', 1)
                response = self.session.get(url, timeout=timeout)
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

    def _classify_document(self, title: str, url: str) -> str:
        """Classify document type from title/URL"""
        title_lower = title.lower()
        url_lower = url.lower()

        if 'circular' in title_lower and 'oficio' not in title_lower and 'ofício' not in title_lower:
            return 'circular'
        elif 'oficio_circulado' in url_lower or 'ofício-circulado' in title_lower or 'oficio circulado' in title_lower:
            return 'oficio_circulado'
        elif 'oficio_circular' in url_lower or 'ofício circular' in title_lower:
            return 'oficio_circular'
        elif 'despacho' in title_lower:
            return 'despacho'
        elif 'piv' in url_lower or 'informac' in url_lower.replace('informacao', 'informac'):
            return 'informacao_vinculativa'
        else:
            return 'doctrine'

    def _extract_date_from_title(self, title: str) -> str:
        """Try to extract a date from the title"""
        # Pattern: dd/mm/yyyy or dd-mm-yyyy
        m = re.search(r'(\d{1,2})[/-](\d{1,2})[/-](\d{4})', title)
        if m:
            day, month, year = m.group(1), m.group(2), m.group(3)
            try:
                return f"{year}-{int(month):02d}-{int(day):02d}"
            except ValueError:
                pass

        # Pattern: year at end like _2024 or _2023
        m = re.search(r'[_\s](\d{4})(?:\.pdf)?$', title, re.IGNORECASE)
        if m:
            return f"{m.group(1)}-01-01"

        return ""

    def _make_id(self, url: str) -> str:
        """Generate a stable document ID from URL"""
        # Extract meaningful part from URL
        parsed = urlparse(url)
        path = unquote(parsed.path)

        # Use filename for PDFs
        if path.endswith('.pdf'):
            filename = Path(path).stem
            return f"PT-AT-{filename}"

        # Use path hash for .aspx pages
        path_hash = hashlib.md5(path.encode()).hexdigest()[:12]
        return f"PT-AT-{path_hash}"

    def discover_documents(self, limit: int = None) -> Iterator[Dict[str, str]]:
        """
        Discover all document URLs via the search API.

        Yields dicts with: title, url, description
        """
        seen_urls: Set[str] = set()
        total_yielded = 0

        for query in SEARCH_QUERIES:
            if limit and total_yielded >= limit:
                return

            logger.info(f"Searching for: {query}")
            skip = 0
            empty_streak = 0

            while True:
                if limit and total_yielded >= limit:
                    return

                try:
                    response = self.session.get(
                        f"{SEARCH_BASE}/informacoes",
                        params={"query": query, "skip": skip},
                        timeout=30
                    )
                    response.raise_for_status()
                    data = response.json()
                except Exception as e:
                    logger.warning(f"Search failed for '{query}' skip={skip}: {e}")
                    break

                results = data.get('results', [])
                if not results:
                    empty_streak += 1
                    if empty_streak >= 2:
                        break
                    skip += PAGE_SIZE
                    time.sleep(0.5)
                    continue

                empty_streak = 0
                new_in_batch = 0

                for res in results:
                    url = res.get('Path', '')
                    if not url or url in seen_urls:
                        continue

                    # Only process PDFs and .aspx pages
                    if not (url.endswith('.pdf') or url.endswith('.aspx')):
                        continue

                    seen_urls.add(url)
                    new_in_batch += 1
                    total_yielded += 1

                    yield {
                        'title': res.get('Title', ''),
                        'url': url,
                        'description': ' '.join(res.get('descriptionHighlight', [])),
                    }

                skip += PAGE_SIZE
                time.sleep(0.5)  # Rate limiting

            logger.info(f"Query '{query}': discovered {total_yielded} unique documents so far")

        logger.info(f"Total unique documents discovered: {total_yielded}")

    def fetch_document_text(self, url: str) -> str:
        """Download and extract text from a document URL (PDF or HTML)"""
        response = self._fetch_with_retry(url)
        if not response:
            return ""

        content_type = response.headers.get('Content-Type', '').lower()

        if url.endswith('.pdf') or 'pdf' in content_type:
            # PDF document
            if len(response.content) > 50_000_000:  # Skip >50MB PDFs
                logger.warning(f"Skipping oversized PDF ({len(response.content)} bytes): {url}")
                return ""
            return self._extract_text_from_pdf(response.content)
        else:
            # HTML page
            return self._clean_html(response.text)

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all doctrine documents with full text.

        Args:
            limit: Maximum number of documents to fetch

        Yields:
            Raw document dictionaries
        """
        count = 0

        for doc_info in self.discover_documents(limit=limit):
            url = doc_info['url']
            title = doc_info['title']

            logger.info(f"Fetching [{count+1}]: {title[:60]}...")

            text = self.fetch_document_text(url)
            if not text or len(text) < 50:
                logger.warning(f"No text extracted from: {url}")
                continue

            doc_info['text'] = text
            yield doc_info
            count += 1

            if limit and count >= limit:
                return

            time.sleep(1)  # Rate limiting

        logger.info(f"Fetched {count} documents with full text")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        url = raw.get('url', '')
        title = raw.get('title', '')
        text = raw.get('text', '')
        description = raw.get('description', '')

        doc_id = self._make_id(url)
        doc_type = self._classify_document(title, url)
        date = self._extract_date_from_title(title)

        # Use https
        if url.startswith('http://'):
            url = url.replace('http://', 'https://', 1)

        return {
            '_id': doc_id,
            '_source': 'PT/AT',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': text,
            'date': date,
            'url': url,
            'language': 'pt',
            'doc_type': doc_type,
            'description': description,
        }


def main():
    """Main entry point"""
    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ATFetcher()
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
            # Sanitize filename
            filename = re.sub(r'[^\w\-.]', '_', filename)
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{saved+1}]: {normalized['title'][:50]}... ({text_len:,} chars)")
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
        # Test mode
        fetcher = ATFetcher()
        print("Testing PT/AT fetcher...")
        count = 0
        for raw in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw)
            print(f"\n--- Document {count+1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Type: {normalized['doc_type']}")
            print(f"Date: {normalized['date']}")
            print(f"URL: {normalized['url']}")
            print(f"Text length: {len(normalized['text'])} chars")
            print(f"Text preview: {normalized['text'][:300]}...")
            count += 1


if __name__ == '__main__':
    main()
