#!/usr/bin/env python3
"""
BEREC (Body of European Regulators for Electronic Communications) Data Fetcher
Fetches guidelines, opinions, reports, decisions, and regulatory documents with full text.

Approach:
1. Scrape the BEREC document search page to get document metadata and links
2. Visit each document page to find PDF download links
3. Download PDFs and extract text
4. Normalize to standard schema

Document types covered:
- Guidelines
- Opinions
- Reports
- Decisions
- Recommendations
- Public Consultations
- Regulatory Best Practices
"""

import json
import logging
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urljoin, urlparse, parse_qs
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
BASE_URL = "https://www.berec.europa.eu"
SEARCH_URL = f"{BASE_URL}/en/search-documents"


class BERECFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })

    def _make_request(self, url: str, timeout: int = 120, max_retries: int = 3) -> Optional[requests.Response]:
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
            logger.warning(f"Not a PDF: {pdf_url} (Content-Type: {content_type})")
            return ""

        return self._extract_text_from_pdf(response.content)

    def _parse_search_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse BEREC search page to extract document metadata"""
        soup = BeautifulSoup(html, 'html.parser')
        documents = []

        # Find all document rows in the table
        table = soup.find('table')
        if not table:
            logger.warning("No document table found on page")
            return documents

        rows = table.find_all('tr')

        for row in rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 3:
                    continue

                # First cell: Document Number
                doc_num_cell = cells[0]
                doc_number = doc_num_cell.get_text(strip=True)

                # Second cell: Date
                date_cell = cells[1]
                date_text = date_cell.get_text(strip=True)

                # Third cell: Title with link to document page
                title_cell = cells[2]
                title_link = title_cell.find('a')
                if not title_link:
                    continue

                title = title_link.get_text(strip=True)
                doc_page_url = title_link.get('href', '')

                if not doc_page_url:
                    continue

                # Make URL absolute
                if doc_page_url.startswith('/'):
                    doc_page_url = urljoin(BASE_URL, doc_page_url)

                # Fourth cell: Author (optional)
                author = "BEREC"
                if len(cells) > 3:
                    author_cell = cells[3]
                    author = author_cell.get_text(strip=True) or "BEREC"

                # Generate document ID from document number
                if doc_number:
                    # Clean document number for ID
                    doc_id = f"BEREC-{doc_number.replace(' ', '_').replace('/', '-').replace('(', '').replace(')', '')}"
                else:
                    doc_id = f"BEREC-{hashlib.md5(doc_page_url.encode()).hexdigest()[:16]}"

                documents.append({
                    'document_id': doc_id,
                    'document_number': doc_number,
                    'title': title,
                    'date': date_text,
                    'doc_page_url': doc_page_url,
                    'author': author,
                })

            except Exception as e:
                logger.warning(f"Failed to parse row: {e}")
                continue

        return documents

    def _get_pdf_url_from_doc_page(self, doc_page_url: str) -> Optional[str]:
        """Fetch document detail page and extract PDF URL"""
        response = self._make_request(doc_page_url)
        if response is None:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        # BEREC uses /system/files/ path for PDFs
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if '/system/files/' in href and '.pdf' in href.lower():
                if href.startswith('/'):
                    return urljoin(BASE_URL, href)
                return href

        # Also check for direct PDF links
        for link in soup.find_all('a', href=lambda x: x and x.lower().endswith('.pdf')):
            href = link.get('href', '')
            if href.startswith('/'):
                return urljoin(BASE_URL, href)
            elif href.startswith('http'):
                return href

        # Look for download links
        download_links = soup.find_all('a', class_=lambda c: c and 'download' in str(c).lower())
        for link in download_links:
            href = link.get('href', '')
            if '.pdf' in href.lower():
                if href.startswith('/'):
                    return urljoin(BASE_URL, href)
                return href

        return None

    def _fetch_search_page(self, page: int = 0) -> List[Dict[str, Any]]:
        """Fetch a single page of the BEREC document search"""
        url = f"{SEARCH_URL}?page={page}"

        response = self._make_request(url)
        if response is None:
            return []

        return self._parse_search_page(response.text)

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all BEREC publications with full text.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
        """
        fetched = 0
        page = 0
        consecutive_empty = 0

        while True:
            if max_docs is not None and fetched >= max_docs:
                logger.info(f"Reached max_docs limit ({max_docs})")
                return

            logger.info(f"Fetching search page {page}...")
            documents = self._fetch_search_page(page)

            if not documents:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    logger.info(f"No more documents found (page={page})")
                    break
                page += 1
                time.sleep(1.0)
                continue

            consecutive_empty = 0

            for doc in documents:
                if max_docs is not None and fetched >= max_docs:
                    return

                logger.info(f"Processing: {doc['title'][:60]}...")

                # Get PDF URL from document page
                pdf_url = self._get_pdf_url_from_doc_page(doc['doc_page_url'])
                time.sleep(1.0)

                if not pdf_url:
                    logger.warning(f"No PDF found for {doc['document_id']}")
                    continue

                # Download PDF and extract text
                text = self._fetch_pdf_text(pdf_url)

                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for {doc['document_id']}")
                    continue

                doc['text'] = text
                doc['pdf_url'] = pdf_url
                doc['url'] = doc['doc_page_url']

                yield doc
                fetched += 1

                logger.info(f"Fetched {doc['document_id']} ({len(text):,} chars)")

                time.sleep(2.0)  # Rate limiting

            page += 1
            time.sleep(1.0)  # Rate limiting between pages

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date"""
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_date = None
                    date_str = doc['date']

                    # Try various date formats
                    for fmt in ['%d %B %Y', '%d/%m/%Y', '%Y-%m-%d', '%d %b %Y', '%B %d, %Y']:
                        try:
                            doc_date = datetime.strptime(date_str, fmt)
                            break
                        except ValueError:
                            continue

                    if doc_date and doc_date.replace(tzinfo=None) >= since:
                        yield doc
                    elif doc_date is None:
                        yield doc
                except Exception:
                    yield doc
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        # Parse date
        parsed_date = None
        if raw_doc.get('date'):
            try:
                date_str = raw_doc['date']
                for fmt in ['%d %B %Y', '%d/%m/%Y', '%Y-%m-%d', '%d %b %Y', '%B %d, %Y']:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        parsed_date = dt.strftime('%Y-%m-%d')
                        break
                    except ValueError:
                        continue
            except Exception:
                pass

        return {
            '_id': raw_doc['document_id'],
            '_source': 'EU/BEREC',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'document_id': raw_doc['document_id'],
            'document_number': raw_doc.get('document_number', ''),
            'title': raw_doc['title'],
            'text': raw_doc.get('text', ''),
            'date': parsed_date,
            'url': raw_doc.get('url', raw_doc.get('doc_page_url', '')),
            'pdf_url': raw_doc.get('pdf_url', ''),
            'author': raw_doc.get('author', 'BEREC'),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BERECFetcher()
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
        # Test mode
        fetcher = BERECFetcher()

        print("Testing BEREC fetcher...")
        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Document Number: {normalized['document_number']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"URL: {normalized['url']}")
            count += 1

        print(f"\nSuccessfully fetched {count} documents")


if __name__ == '__main__':
    main()
