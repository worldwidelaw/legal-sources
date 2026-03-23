#!/usr/bin/env python3
"""
ACER (Agency for the Cooperation of Energy Regulators) Data Fetcher
Fetches individual decisions, opinions, and recommendations.

Two page structures:
- Paginated views (individual_decision, recommendation): Drupal views with ?page=N
- Flat listings (opinion): all PDFs on a single page
"""

import json
import logging
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urljoin, unquote
import io

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    try:
        import PyPDF2
        HAS_PYPDF2 = True
    except ImportError:
        HAS_PYPDF2 = False

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://www.acer.europa.eu"

# Paginated categories use Drupal views with ?page=N
PAGINATED_CATEGORIES = {
    'individual_decision': '/documents/official-documents/individual-decisions',
    'recommendation': '/documents/official-documents/recommendations',
}

# Flat categories list all PDFs on a single page
FLAT_CATEGORIES = {
    'opinion': '/documents/official-documents/opinions',
}


class ACERFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def _get(self, url: str, timeout: int = 60, max_retries: int = 3) -> Optional[requests.Response]:
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {url} -> {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                    parts = []
                    for page in pdf.pages:
                        t = page.extract_text()
                        if t:
                            parts.append(t)
                    text = '\n\n'.join(parts)
                    text = re.sub(r'\n{3,}', '\n\n', text)
                    text = re.sub(r' {2,}', ' ', text)
                    return text.strip()
            except Exception as e:
                logger.warning(f"pdfplumber failed: {e}")

        if not HAS_PDFPLUMBER and HAS_PYPDF2:
            try:
                reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                parts = []
                for page in reader.pages:
                    t = page.extract_text()
                    if t:
                        parts.append(t)
                text = '\n\n'.join(parts)
                text = re.sub(r'\n{3,}', '\n\n', text)
                text = re.sub(r' {2,}', ' ', text)
                return text.strip()
            except Exception as e:
                logger.warning(f"PyPDF2 failed: {e}")

        if not HAS_PDFPLUMBER and not HAS_PYPDF2:
            logger.error("No PDF library available. Install pdfplumber or PyPDF2.")
        return ""

    def _fetch_pdf_text(self, pdf_url: str) -> str:
        response = self._get(pdf_url, timeout=120)
        if response is None:
            return ""
        return self._extract_text_from_pdf(response.content)

    def _make_absolute(self, href: str) -> str:
        if href.startswith('http'):
            return href
        return urljoin(BASE_URL, href)

    def _extract_doc_number(self, text: str) -> Optional[str]:
        m = re.search(r'(?:Decision|Opinion|Recommendation)\s*(?:No\.?\s*)?(\d{1,3})[/-](\d{4})', text, re.I)
        if m:
            return f"{m.group(1)}-{m.group(2)}"
        return None

    def _parse_paginated_page(self, html: str, doc_type: str) -> List[Dict[str, Any]]:
        """Parse a paginated category page using views-row structure."""
        soup = BeautifulSoup(html, 'html.parser')
        documents = []

        view_div = soup.find('div', class_=lambda c: c and 'view-id-documents' in str(c))
        if not view_div:
            logger.warning(f"No view-id-documents div found for {doc_type}")
            # Fallback: try flat parsing
            return self._parse_flat_page(html, doc_type)

        rows = view_div.find_all('div', class_='views-row')
        logger.info(f"Found {len(rows)} views-rows for {doc_type}")

        for row in rows:
            # Get date from .date div
            date_str = None
            date_div = row.find('div', class_='date')
            if date_div:
                raw = date_div.get_text(strip=True)
                dm = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', raw)
                if dm:
                    date_str = f"{dm.group(3)}-{dm.group(2).zfill(2)}-{dm.group(1).zfill(2)}"

            # Get title from .title div link
            title = None
            title_div = row.find('div', class_='title')
            if title_div:
                a = title_div.find('a')
                if a:
                    title = a.get_text(strip=True)

            # Get main PDF URL (from .title link, skip annexes)
            main_pdf_url = None
            if title_div:
                a = title_div.find('a', href=True)
                if a and '.pdf' in a['href'].lower():
                    href = a['href']
                    if '_annex' not in href.lower():
                        main_pdf_url = self._make_absolute(href)

            if not main_pdf_url:
                # Fallback: first non-annex PDF in the row
                for a in row.find_all('a', href=True):
                    href = a['href']
                    if '.pdf' in href.lower() and '_annex' not in href.lower():
                        main_pdf_url = self._make_absolute(href)
                        if not title:
                            title = a.get_text(strip=True)
                        break

            if not main_pdf_url:
                continue

            if not title or len(title) < 5:
                filename = unquote(main_pdf_url.split('/')[-1]).replace('.pdf', '')
                title = filename.replace('-', ' ').replace('_', ' ')

            doc_number = self._extract_doc_number(title)
            if doc_number:
                doc_id = f"ACER-{doc_type}-{doc_number}"
            else:
                url_hash = hashlib.md5(main_pdf_url.encode()).hexdigest()[:12]
                doc_id = f"ACER-{doc_type}-{url_hash}"

            if any(d['document_id'] == doc_id for d in documents):
                continue

            documents.append({
                'document_id': doc_id,
                'document_type': doc_type,
                'document_number': doc_number,
                'title': title,
                'pdf_url': main_pdf_url,
                'date': date_str,
            })

        return documents

    def _parse_flat_page(self, html: str, doc_type: str) -> List[Dict[str, Any]]:
        """Parse a flat page where all PDFs are listed directly (e.g., opinions)."""
        soup = BeautifulSoup(html, 'html.parser')
        documents = []
        seen_urls = set()

        content = soup.find('main') or soup
        for a in content.find_all('a', href=True):
            href = a['href']
            if not href.lower().endswith('.pdf'):
                continue
            if '/sites/default/files/documents/' not in href:
                continue
            # Skip annexes
            if '_annex' in href.lower() or 'annex' in href.split('/')[-1].lower().split('.')[0].split('-')[-1:][0]:
                text = a.get_text(strip=True).lower()
                if 'annex' in text:
                    continue

            pdf_url = self._make_absolute(href)
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            title = a.get_text(strip=True)
            if not title or len(title) < 5:
                filename = unquote(pdf_url.split('/')[-1]).replace('.pdf', '')
                title = filename.replace('-', ' ').replace('_', ' ')

            # Skip if title indicates annex
            if re.search(r'\bannex\b', title, re.I) and not re.search(r'(?:Decision|Opinion|Recommendation)\s+\d', title, re.I):
                continue

            # Try to extract date from nearby text
            date_str = None
            parent = a.find_parent(['p', 'div', 'li', 'td'])
            if parent:
                raw = parent.get_text()
                dm = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', raw)
                if dm:
                    date_str = f"{dm.group(3)}-{dm.group(2).zfill(2)}-{dm.group(1).zfill(2)}"

            doc_number = self._extract_doc_number(title)
            if doc_number:
                doc_id = f"ACER-{doc_type}-{doc_number}"
            else:
                url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
                doc_id = f"ACER-{doc_type}-{url_hash}"

            if any(d['document_id'] == doc_id for d in documents):
                continue

            documents.append({
                'document_id': doc_id,
                'document_type': doc_type,
                'document_number': doc_number,
                'title': title,
                'pdf_url': pdf_url,
                'date': date_str,
            })

        return documents

    def _has_next_page(self, html: str, current_page: int) -> bool:
        soup = BeautifulSoup(html, 'html.parser')
        pager = soup.find('nav', class_='pager') or soup.find('ul', class_='pagination')
        if not pager:
            return False
        for a in pager.find_all('a', href=True):
            href = a['href']
            if 'page=' in href:
                try:
                    pnum = int(href.split('page=')[-1].split('&')[0])
                    if pnum > current_page:
                        return True
                except ValueError:
                    pass
        return False

    def fetch_all(self, max_docs: int = None, categories: List[str] = None) -> Iterator[Dict[str, Any]]:
        all_categories = {**PAGINATED_CATEGORIES, **FLAT_CATEGORIES}
        if categories is None:
            categories = list(all_categories.keys())

        fetched = 0

        # Paginated categories
        for category in categories:
            if category in FLAT_CATEGORIES:
                continue
            if category not in PAGINATED_CATEGORIES:
                continue

            path = PAGINATED_CATEGORIES[category]
            logger.info(f"Fetching paginated category: {category}")

            page = 0
            consecutive_empty = 0

            while True:
                if max_docs is not None and fetched >= max_docs:
                    return

                url = f"{BASE_URL}{path}"
                if page > 0:
                    url = f"{url}?page={page}"

                response = self._get(url)
                if response is None:
                    logger.error(f"Failed to fetch {url}")
                    break

                logger.info(f"Page {page}: status={response.status_code}, len={len(response.text)}")
                documents = self._parse_paginated_page(response.text, category)

                if not documents:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        logger.info(f"No more {category} documents after page {page}")
                        break
                    page += 1
                    time.sleep(1.0)
                    continue

                consecutive_empty = 0
                logger.info(f"Found {len(documents)} docs on page {page}")

                for doc in documents:
                    if max_docs is not None and fetched >= max_docs:
                        return

                    text = self._fetch_pdf_text(doc['pdf_url'])
                    if not text or len(text) < 100:
                        logger.warning(f"Insufficient text for {doc['document_id']}: {len(text) if text else 0} chars")
                        continue

                    doc['text'] = text
                    doc['url'] = doc['pdf_url']
                    yield doc
                    fetched += 1
                    logger.info(f"Fetched {doc['document_id']} ({len(text):,} chars)")
                    time.sleep(2.0)

                if not self._has_next_page(response.text, page):
                    logger.info(f"Last page for {category}: {page}")
                    break

                page += 1
                time.sleep(1.0)

        # Flat categories (single page with all PDFs)
        for category in categories:
            if category not in FLAT_CATEGORIES:
                continue

            path = FLAT_CATEGORIES[category]
            logger.info(f"Fetching flat category: {category}")

            response = self._get(f"{BASE_URL}{path}")
            if response is None:
                logger.error(f"Failed to fetch {category}")
                continue

            logger.info(f"Flat page: status={response.status_code}, len={len(response.text)}")
            documents = self._parse_flat_page(response.text, category)
            logger.info(f"Found {len(documents)} {category} documents")

            for doc in documents:
                if max_docs is not None and fetched >= max_docs:
                    return

                text = self._fetch_pdf_text(doc['pdf_url'])
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for {doc['document_id']}: {len(text) if text else 0} chars")
                    continue

                doc['text'] = text
                doc['url'] = doc['pdf_url']
                yield doc
                fetched += 1
                logger.info(f"Fetched {doc['document_id']} ({len(text):,} chars)")
                time.sleep(2.0)

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_date = datetime.strptime(doc['date'], '%Y-%m-%d')
                    if doc_date >= since:
                        yield doc
                except ValueError:
                    yield doc
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        return {
            '_id': raw_doc['document_id'],
            '_source': 'EU/ACER',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'document_id': raw_doc['document_id'],
            'document_type': raw_doc.get('document_type', 'unknown'),
            'document_number': raw_doc.get('document_number'),
            'title': raw_doc['title'],
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date'),
            'url': raw_doc.get('url', raw_doc.get('pdf_url', '')),
        }


def main():
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ACERFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else 50
        logger.info(f"Fetching up to {target_count} documents...")

        # Log PDF library availability
        logger.info(f"pdfplumber available: {HAS_PDFPLUMBER}")
        if not HAS_PDFPLUMBER:
            logger.info(f"PyPDF2 available: {HAS_PYPDF2 if 'HAS_PYPDF2' in dir() else False}")

        sample_count = 0
        for raw_doc in fetcher.fetch_all(max_docs=target_count):
            normalized = fetcher.normalize(raw_doc)

            if len(normalized.get('text', '')) < 100:
                logger.warning(f"Skipping {normalized['_id']} - insufficient text")
                continue

            filename = f"{normalized['_id'].replace('/', '_').replace(':', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['title'][:60]}... ({len(normalized['text']):,} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        if sample_count > 0:
            files = list(sample_dir.glob('*.json'))
            total_chars = sum(
                len(json.load(open(f)).get('text', ''))
                for f in files
            )
            avg_chars = total_chars // len(files) if files else 0
            logger.info(f"Average text length: {avg_chars:,} characters per document")

    else:
        fetcher = ACERFetcher()
        print("Testing ACER fetcher - listing documents...")
        for cat, path in list(PAGINATED_CATEGORIES.items())[:1]:
            url = f"{BASE_URL}{path}"
            response = fetcher._get(url)
            if response:
                docs = fetcher._parse_paginated_page(response.text, cat)
                print(f"\n{cat}: Found {len(docs)} documents")
                for doc in docs[:3]:
                    print(f"  - {doc['document_id']}: {doc['title'][:60]}...")
        for cat, path in list(FLAT_CATEGORIES.items())[:1]:
            url = f"{BASE_URL}{path}"
            response = fetcher._get(url)
            if response:
                docs = fetcher._parse_flat_page(response.text, cat)
                print(f"\n{cat}: Found {len(docs)} documents")
                for doc in docs[:3]:
                    print(f"  - {doc['document_id']}: {doc['title'][:60]}...")


if __name__ == '__main__':
    main()
