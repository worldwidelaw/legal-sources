#!/usr/bin/env python3
"""
AMLA (EU Anti-Money Laundering Authority) Data Fetcher

Fetches regulatory documents from AMLA's document library including:
- Regulatory Technical Standards (RTS)
- Implementing Technical Standards (ITS)
- Consultation papers
- Final reports
- General publications

Approach:
1. Scrape document library pages to get document metadata and PDF links
2. Download PDFs and extract text using pdfplumber
3. Normalize to standard schema
"""

import json
import logging
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urljoin, urlparse, unquote
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

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.amla.europa.eu"
DOCUMENT_LIBRARY_PATH = "/resources/document-library_en"


class AMLAFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def _make_request(self, url: str, timeout: int = 60, max_retries: int = 3) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None
        return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF content"""
        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                    text_parts = []
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                    text = '\n\n'.join(text_parts)
                    # Clean up text
                    text = re.sub(r'\n{3,}', '\n\n', text)
                    text = re.sub(r' {2,}', ' ', text)
                    return text.strip()
            except Exception as e:
                logger.warning(f"pdfplumber extraction failed: {e}")

        if HAS_PYPDF2:
            try:
                reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                text_parts = []
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                text = '\n\n'.join(text_parts)
                text = re.sub(r'\n{3,}', '\n\n', text)
                text = re.sub(r' {2,}', ' ', text)
                return text.strip()
            except Exception as e:
                logger.warning(f"PyPDF2 extraction failed: {e}")

        logger.error("No PDF extraction library available. Install pdfplumber or PyPDF2.")
        return ""

    def _fetch_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text"""
        response = self._make_request(pdf_url, timeout=120)
        if response is None:
            return ""

        content_type = response.headers.get('Content-Type', '')
        if 'pdf' not in content_type.lower() and not pdf_url.lower().endswith('.pdf'):
            # Check if it's actually a PDF by magic bytes
            if not response.content.startswith(b'%PDF'):
                logger.warning(f"Not a PDF: {pdf_url} (Content-Type: {content_type})")
                return ""

        return self._extract_text_from_pdf(response.content)

    def _parse_document_library_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse document library page to extract document metadata and download links"""
        soup = BeautifulSoup(html, 'html.parser')
        documents = []

        # Find document entries in the document library
        # AMLA uses a typical EU website structure with document cards/rows

        # Look for document download links
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')

            # AMLA uses /document/download/ pattern for PDFs
            if '/document/download/' not in href:
                continue

            # Skip non-PDF documents (Excel templates, etc.)
            if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.docx', '.doc', '.zip']):
                continue

            # Make URL absolute
            if href.startswith('/'):
                pdf_url = urljoin(BASE_URL, href)
            else:
                pdf_url = href

            # Extract filename from URL to get title
            filename_match = re.search(r'filename=([^&]+)', href)
            if filename_match:
                filename = unquote(filename_match.group(1))
                title = filename.replace('.pdf', '').replace('%20', ' ').replace('_', ' ')
            else:
                # Try to get title from link text
                title = link.get_text(strip=True)
                if not title or len(title) < 5:
                    # Extract from parent
                    parent = link.find_parent(['div', 'td', 'li', 'article'])
                    if parent:
                        title = parent.get_text(strip=True)[:200]

            # Clean title
            title = re.sub(r'\s+', ' ', title).strip()

            if not title or len(title) < 5:
                continue

            # Try to find date near this link
            date_str = None
            parent = link.find_parent(['div', 'tr', 'article', 'li'])
            if parent:
                parent_text = parent.get_text()
                # DD Month YYYY pattern
                date_match = re.search(r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})', parent_text)
                if date_match:
                    day = date_match.group(1).zfill(2)
                    month_name = date_match.group(2)
                    year = date_match.group(3)
                    months = {'January': '01', 'February': '02', 'March': '03', 'April': '04',
                              'May': '05', 'June': '06', 'July': '07', 'August': '08',
                              'September': '09', 'October': '10', 'November': '11', 'December': '12'}
                    month = months.get(month_name, '01')
                    date_str = f"{year}-{month}-{day}"
                else:
                    # Try DD/MM/YYYY or DD.MM.YYYY
                    date_match = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', parent_text)
                    if date_match:
                        day, month, year = date_match.groups()
                        date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

            # Determine document type from title/filename
            doc_type = 'publication'
            title_lower = title.lower()
            if 'rts' in title_lower or 'regulatory technical standard' in title_lower:
                doc_type = 'rts'
            elif 'its' in title_lower or 'implementing technical standard' in title_lower:
                doc_type = 'its'
            elif 'consultation' in title_lower:
                doc_type = 'consultation'
            elif 'final report' in title_lower:
                doc_type = 'final_report'
            elif 'press release' in title_lower:
                doc_type = 'press_release'
            elif 'privacy' in title_lower:
                doc_type = 'privacy'
            elif 'record of processing' in title_lower:
                doc_type = 'processing_record'

            # Generate document ID
            url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
            doc_id = f"AMLA-{doc_type}-{url_hash}"

            # Skip duplicates
            if any(d['document_id'] == doc_id for d in documents):
                continue

            documents.append({
                'document_id': doc_id,
                'document_type': doc_type,
                'title': title,
                'pdf_url': pdf_url,
                'date': date_str,
            })

        return documents

    def _get_total_pages(self, html: str) -> int:
        """Determine total number of pages from pagination"""
        soup = BeautifulSoup(html, 'html.parser')

        # Look for pagination info - AMLA shows "Showing 1 - 10 of 54 results"
        results_info = soup.find(string=re.compile(r'of\s+\d+\s+results?', re.I))
        if results_info:
            match = re.search(r'of\s+(\d+)\s+results?', results_info, re.I)
            if match:
                total_results = int(match.group(1))
                return (total_results + 9) // 10  # 10 results per page

        # Look for last page number in pagination
        pager = soup.find('nav', class_='pager') or soup.find('ul', class_='pagination') or soup.find('div', class_='pagination')
        if pager:
            page_numbers = []
            for link in pager.find_all('a'):
                try:
                    num = int(link.get_text(strip=True))
                    page_numbers.append(num)
                except ValueError:
                    continue
            if page_numbers:
                return max(page_numbers)

        return 6  # Default based on 54 docs / 10 per page

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all AMLA documents with full text.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
        """
        fetched = 0

        # First, get the first page to determine total pages
        url = f"{BASE_URL}{DOCUMENT_LIBRARY_PATH}"
        response = self._make_request(url)
        if response is None:
            logger.error("Failed to fetch document library")
            return

        total_pages = self._get_total_pages(response.text)
        logger.info(f"Found approximately {total_pages} pages of documents")

        all_documents = []

        # Fetch all pages to collect document metadata
        for page in range(total_pages):
            if max_docs is not None and fetched >= max_docs:
                break

            page_url = f"{BASE_URL}{DOCUMENT_LIBRARY_PATH}?page={page}"
            logger.info(f"Fetching page {page + 1}/{total_pages}: {page_url}")

            response = self._make_request(page_url)
            if response is None:
                continue

            documents = self._parse_document_library_page(response.text)
            logger.info(f"Found {len(documents)} documents on page {page + 1}")

            # Filter out documents we already have
            for doc in documents:
                if not any(d['document_id'] == doc['document_id'] for d in all_documents):
                    all_documents.append(doc)

            time.sleep(1.0)  # Rate limiting

        logger.info(f"Total unique documents found: {len(all_documents)}")

        # Filter to only substantive documents (skip privacy statements, processing records, etc.)
        priority_types = ['rts', 'its', 'consultation', 'final_report', 'publication']
        substantive_docs = [d for d in all_documents if d['document_type'] in priority_types]
        admin_docs = [d for d in all_documents if d['document_type'] not in priority_types]

        # Process substantive documents first, then admin docs
        ordered_docs = substantive_docs + admin_docs

        for doc in ordered_docs:
            if max_docs is not None and fetched >= max_docs:
                return

            # Skip privacy/processing documents for sample (they're not substantive)
            if doc['document_type'] in ['privacy', 'processing_record']:
                if max_docs is not None and max_docs <= 20:
                    continue

            logger.info(f"Fetching full text: {doc['title'][:60]}...")

            # Download PDF and extract text
            text = self._fetch_pdf_text(doc['pdf_url'])

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {doc['document_id']}: {len(text) if text else 0} chars")
                continue

            doc['text'] = text
            doc['url'] = doc['pdf_url']

            yield doc
            fetched += 1

            logger.info(f"Fetched {doc['document_id']} ({len(text):,} chars)")

            time.sleep(2.0)  # Rate limiting

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date"""
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_date = datetime.strptime(doc['date'], '%Y-%m-%d')
                    if doc_date >= since:
                        yield doc
                except ValueError:
                    yield doc  # Include if date can't be parsed
            else:
                yield doc  # Include if no date

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        return {
            '_id': raw_doc['document_id'],
            '_source': 'EU/AMLA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'document_id': raw_doc['document_id'],
            'document_type': raw_doc.get('document_type', 'unknown'),
            'title': raw_doc['title'],
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date'),
            'url': raw_doc.get('url', raw_doc.get('pdf_url', '')),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = AMLAFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv

        if is_sample:
            target_count = 12
            logger.info("Fetching sample documents (12 records)...")
        else:
            target_count = None  # Fetch ALL documents
            logger.info("Fetching ALL AMLA documents (use --sample for quick test)...")

        sample_count = 0

        for raw_doc in fetcher.fetch_all(max_docs=target_count):
            normalized = fetcher.normalize(raw_doc)

            # Validate text content
            if len(normalized.get('text', '')) < 100:
                logger.warning(f"Skipping {normalized['_id']} - insufficient text")
                continue

            # Save to sample directory
            filename = f"{normalized['_id'].replace('/', '_').replace(':', '_')}.json"
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
        # Test mode - list documents without downloading full text
        fetcher = AMLAFetcher()

        print("Testing AMLA fetcher - listing documents...")

        url = f"{BASE_URL}{DOCUMENT_LIBRARY_PATH}"
        response = fetcher._make_request(url)
        if response:
            docs = fetcher._parse_document_library_page(response.text)
            print(f"\nFound {len(docs)} documents on first page")
            for doc in docs[:5]:
                print(f"  - [{doc['document_type']}] {doc['title'][:60]}...")


if __name__ == '__main__':
    main()
