#!/usr/bin/env python3
"""
EBA (European Banking Authority) Data Fetcher
Fetches guidelines, opinions, decisions, reports and technical standards with full text.

Approach:
1. Scrape publication listing pages to get document metadata and PDF links
2. Download PDFs and extract text using pdfplumber
3. Normalize to standard schema

Document types covered:
- Guidelines (document_type=250)
- Opinions (document_type=252)
- Decisions (document_type=245)
- Reports (document_type=257)
- Recommendations (document_type=255)
- Draft RTS (document_type=248)
- Draft ITS (document_type=247)
"""

import json
import logging
import re
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urljoin, urlparse
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
BASE_URL = "https://www.eba.europa.eu"
PUBLICATIONS_URL = f"{BASE_URL}/publications-and-media/publications"

# Document types to fetch (ID -> name)
DOCUMENT_TYPES = {
    250: 'guideline',
    252: 'opinion',
    245: 'decision',
    257: 'report',
    255: 'recommendation',
    248: 'draft_regulatory_technical_standard',
    247: 'draft_implementing_technical_standard',
}


class EBAFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
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
            logger.warning(f"Not a PDF: {pdf_url} (Content-Type: {content_type})")
            return ""

        return self._extract_text_from_pdf(response.content)

    def _parse_publication_listing(self, html: str) -> List[Dict[str, Any]]:
        """Parse publication listing page to extract document metadata"""
        soup = BeautifulSoup(html, 'html.parser')
        documents = []

        # Find all teaser articles
        for teaser in soup.find_all('article', class_='teaser'):
            try:
                # Find title and PDF link
                title_elem = teaser.find('h4', class_='teaser__title')
                if not title_elem:
                    continue

                link = title_elem.find('a')
                if not link:
                    continue

                href = link.get('href', '')
                title = link.get_text(strip=True)

                # We only want PDF documents
                if not href.lower().endswith('.pdf'):
                    continue

                # Make URL absolute
                if href.startswith('/'):
                    pdf_url = urljoin(BASE_URL, href)
                else:
                    pdf_url = href

                # Extract date from metadata if available
                date_elem = teaser.find('time')
                date_str = None
                if date_elem:
                    date_str = date_elem.get('datetime', date_elem.get_text(strip=True))

                # Generate document ID from URL
                parsed = urlparse(pdf_url)
                path_parts = parsed.path.split('/')
                # Try to extract a unique ID from the path
                doc_id = None
                for part in path_parts:
                    # Look for UUID-like strings
                    if len(part) == 36 and '-' in part:
                        doc_id = part
                        break

                if not doc_id:
                    # Hash the URL to create an ID
                    doc_id = hashlib.md5(pdf_url.encode()).hexdigest()[:16]

                documents.append({
                    'document_id': f"EBA-{doc_id}",
                    'title': title,
                    'pdf_url': pdf_url,
                    'date': date_str,
                })

            except Exception as e:
                logger.warning(f"Failed to parse teaser: {e}")
                continue

        return documents

    def _fetch_publications_page(self, document_type: int, page: int = 0) -> List[Dict[str, Any]]:
        """Fetch a single page of publications for a document type"""
        url = f"{PUBLICATIONS_URL}?text=&document_type={document_type}&media_topics=All&page={page}"

        response = self._make_request(url)
        if response is None:
            return []

        documents = self._parse_publication_listing(response.text)

        # Add document type to each document
        type_name = DOCUMENT_TYPES.get(document_type, 'unknown')
        for doc in documents:
            doc['document_type'] = type_name

        return documents

    def fetch_all(self, max_docs: int = None, doc_types: List[int] = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all EBA publications with full text.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
            doc_types: List of document type IDs to fetch (None = all types)
        """
        if doc_types is None:
            doc_types = list(DOCUMENT_TYPES.keys())

        fetched = 0

        for doc_type in doc_types:
            type_name = DOCUMENT_TYPES.get(doc_type, 'unknown')
            logger.info(f"Fetching {type_name} publications (type={doc_type})...")

            page = 0
            consecutive_empty = 0

            while True:
                if max_docs is not None and fetched >= max_docs:
                    logger.info(f"Reached max_docs limit ({max_docs})")
                    return

                documents = self._fetch_publications_page(doc_type, page)

                if not documents:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        logger.info(f"No more {type_name} documents found (page={page})")
                        break
                    page += 1
                    continue

                consecutive_empty = 0

                for doc in documents:
                    if max_docs is not None and fetched >= max_docs:
                        return

                    logger.info(f"Fetching full text for: {doc['title'][:60]}...")

                    # Download PDF and extract text
                    text = self._fetch_pdf_text(doc['pdf_url'])

                    if not text or len(text) < 100:
                        logger.warning(f"Insufficient text for {doc['document_id']}")
                        continue

                    doc['text'] = text
                    doc['url'] = doc['pdf_url']

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
                    # Try to parse the date
                    doc_date = None
                    date_str = doc['date']

                    # Try ISO format
                    if 'T' in date_str:
                        doc_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    else:
                        # Try common date formats
                        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d %B %Y', '%B %d, %Y']:
                            try:
                                doc_date = datetime.strptime(date_str, fmt)
                                break
                            except ValueError:
                                continue

                    if doc_date and doc_date.replace(tzinfo=None) >= since:
                        yield doc
                    elif doc_date is None:
                        # If we can't parse the date, include the document
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
                if 'T' in date_str:
                    parsed_date = date_str[:10]
                else:
                    # Try to parse and convert to ISO
                    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%d %B %Y', '%B %d, %Y']:
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
            '_source': 'EU/EBA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'document_id': raw_doc['document_id'],
            'document_type': raw_doc.get('document_type', 'unknown'),
            'title': raw_doc['title'],
            'text': raw_doc.get('text', ''),
            'date': parsed_date,
            'url': raw_doc.get('url', raw_doc.get('pdf_url', '')),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = EBAFetcher()
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
        fetcher = EBAFetcher()

        print("Testing EBA fetcher...")
        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Type: {normalized['document_type']}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"URL: {normalized['url']}")
            count += 1

        print(f"\nSuccessfully fetched {count} documents")


if __name__ == '__main__':
    main()
