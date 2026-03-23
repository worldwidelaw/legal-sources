#!/usr/bin/env python3
"""
EIOPA (European Insurance and Occupational Pensions Authority) Data Fetcher
Fetches guidelines, opinions, decisions, reports and technical standards with full text.

Approach:
1. Use the RSS feed to get recent documents
2. Scrape document library pages for complete listing with pagination
3. Fetch publication pages to extract PDF download links
4. Download PDFs and extract text using pdfplumber
5. Normalize to standard schema

Document types covered:
- Guidelines
- Opinions
- Decisions
- Technical standards
- Reports
- Supervisory statements
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
BASE_URL = "https://www.eiopa.europa.eu"
DOCUMENT_LIBRARY_URL = f"{BASE_URL}/document-library_en"
RSS_FEED_URL = f"{BASE_URL}/node/4770/rss_en"

# Document types to fetch (mapped to URL paths)
DOCUMENT_TYPE_PATHS = {
    'guidelines': '/document-library/guidelines_en',
    'opinions': '/document-library/opinion_en',
    'decisions': '/document-library/decision_en',
    'technical_standards': '/document-library/technical-standard_en',
    'supervisory_statements': '/document-library/supervisory-statement_en',
    'reports': '/document-library/report_en',
}


class EIOPAFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        self.seen_ids = set()

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
        # Make URL absolute if needed
        if pdf_url.startswith('/'):
            pdf_url = urljoin(BASE_URL, pdf_url)

        response = self._make_request(pdf_url, timeout=120)
        if response is None:
            return ""

        content_type = response.headers.get('Content-Type', '')
        if 'pdf' not in content_type.lower() and not pdf_url.lower().endswith('.pdf'):
            logger.warning(f"Not a PDF: {pdf_url} (Content-Type: {content_type})")
            return ""

        return self._extract_text_from_pdf(response.content)

    def _extract_pdf_links_from_publication(self, pub_url: str) -> List[Dict[str, str]]:
        """
        Fetch a publication page and extract PDF download links.
        Returns list of dicts with 'title' and 'url' keys.
        """
        response = self._make_request(pub_url)
        if response is None:
            return []

        soup = BeautifulSoup(response.text, 'html.parser')
        pdf_links = []

        # Find all download links (they contain 'document/download')
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if 'document/download' in href and '.pdf' in href.lower():
                title = link.get_text(strip=True) or "Document"
                # Clean title
                title = re.sub(r'\s+', ' ', title).strip()
                pdf_links.append({
                    'title': title,
                    'url': href if href.startswith('http') else urljoin(BASE_URL, href)
                })

        return pdf_links

    def _parse_rss_feed(self) -> List[Dict[str, Any]]:
        """Parse RSS feed to get recent documents"""
        response = self._make_request(RSS_FEED_URL)
        if response is None:
            return []

        soup = BeautifulSoup(response.content, 'xml')
        documents = []

        for item in soup.find_all('item'):
            try:
                title = item.find('title')
                link = item.find('link')
                pub_date = item.find('pubDate')
                description = item.find('description')

                if not title or not link:
                    continue

                # Extract document type from category
                doc_type = 'unknown'
                for cat in item.find_all('category'):
                    domain = cat.get('domain', '')
                    if 'Type (of content document)' in domain:
                        doc_type = cat.get_text(strip=True).lower()
                        break

                # Generate ID from link
                link_text = link.get_text(strip=True)
                doc_id = hashlib.md5(link_text.encode()).hexdigest()[:16]

                # Parse date
                date_str = None
                if pub_date:
                    date_text = pub_date.get_text(strip=True)
                    try:
                        # RSS date format: "Mon, 16 Feb 2026 17:27:38 +0100"
                        dt = datetime.strptime(date_text, "%a, %d %b %Y %H:%M:%S %z")
                        date_str = dt.strftime('%Y-%m-%d')
                    except ValueError:
                        pass

                documents.append({
                    'document_id': f"EIOPA-{doc_id}",
                    'title': title.get_text(strip=True),
                    'publication_url': link_text,
                    'date': date_str,
                    'document_type': doc_type,
                    'description': description.get_text(strip=True) if description else None,
                })

            except Exception as e:
                logger.warning(f"Failed to parse RSS item: {e}")
                continue

        return documents

    def _parse_document_listing_page(self, html: str, doc_type: str) -> List[Dict[str, Any]]:
        """Parse document listing page to extract document metadata"""
        soup = BeautifulSoup(html, 'html.parser')
        documents = []

        # Find all listing items - EIOPA uses ecl-content-item or similar structures
        items = soup.find_all(['article', 'div'], class_=lambda x: x and ('content-item' in x.lower() or 'teaser' in x.lower() or 'listing' in x.lower()))

        if not items:
            # Try finding links within the content area
            content = soup.find('main') or soup.find('div', class_='content')
            if content:
                items = content.find_all('a', href=lambda x: x and '/publications/' in x)

        for item in items:
            try:
                # Find the main link
                if item.name == 'a':
                    link = item
                else:
                    link = item.find('a', href=lambda x: x and '/publications/' in x)

                if not link:
                    continue

                href = link.get('href', '')
                title = link.get_text(strip=True)

                if not href or not title:
                    continue

                # Make URL absolute
                pub_url = href if href.startswith('http') else urljoin(BASE_URL, href)

                # Generate ID
                doc_id = hashlib.md5(pub_url.encode()).hexdigest()[:16]

                # Try to find date
                date_str = None
                time_elem = item.find('time')
                if time_elem:
                    date_str = time_elem.get('datetime', time_elem.get_text(strip=True))
                    if date_str and len(date_str) > 10:
                        date_str = date_str[:10]

                documents.append({
                    'document_id': f"EIOPA-{doc_id}",
                    'title': title,
                    'publication_url': pub_url,
                    'date': date_str,
                    'document_type': doc_type,
                })

            except Exception as e:
                logger.warning(f"Failed to parse listing item: {e}")
                continue

        return documents

    def _fetch_document_type_listing(self, doc_type: str, path: str, max_pages: int = 10) -> List[Dict[str, Any]]:
        """Fetch all documents for a given type using pagination"""
        all_documents = []

        for page in range(max_pages):
            url = f"{BASE_URL}{path}?page={page}"
            response = self._make_request(url)

            if response is None:
                break

            documents = self._parse_document_listing_page(response.text, doc_type)

            if not documents:
                logger.info(f"No more {doc_type} documents at page {page}")
                break

            all_documents.extend(documents)
            logger.info(f"Found {len(documents)} {doc_type} documents on page {page}")

            time.sleep(1.0)  # Rate limiting

        return all_documents

    def fetch_all(self, max_docs: int = None, doc_types: List[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all EIOPA publications with full text.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
            doc_types: List of document types to fetch (None = all types)
        """
        if doc_types is None:
            doc_types = list(DOCUMENT_TYPE_PATHS.keys())

        fetched = 0
        all_documents = []

        # First, get documents from RSS feed (most recent)
        logger.info("Fetching documents from RSS feed...")
        rss_docs = self._parse_rss_feed()
        logger.info(f"Found {len(rss_docs)} documents in RSS feed")

        for doc in rss_docs:
            # Filter by type if specified
            if doc_types and doc.get('document_type'):
                # Normalize type for comparison
                doc_type_norm = doc['document_type'].lower()
                if not any(t.lower() in doc_type_norm or doc_type_norm in t.lower() for t in doc_types):
                    continue

            if doc['document_id'] not in self.seen_ids:
                self.seen_ids.add(doc['document_id'])
                all_documents.append(doc)

        # Then, fetch from document type listings for more coverage
        for doc_type, path in DOCUMENT_TYPE_PATHS.items():
            if doc_types and doc_type not in doc_types:
                continue

            logger.info(f"Fetching {doc_type} documents from listing pages...")
            listing_docs = self._fetch_document_type_listing(doc_type, path, max_pages=5)

            for doc in listing_docs:
                if doc['document_id'] not in self.seen_ids:
                    self.seen_ids.add(doc['document_id'])
                    all_documents.append(doc)

        logger.info(f"Total unique documents found: {len(all_documents)}")

        # Process documents and fetch full text
        for doc in all_documents:
            if max_docs is not None and fetched >= max_docs:
                logger.info(f"Reached max_docs limit ({max_docs})")
                return

            logger.info(f"Processing: {doc['title'][:60]}...")

            # Get publication page to find PDF links
            pub_url = doc.get('publication_url')
            if not pub_url:
                continue

            pdf_links = self._extract_pdf_links_from_publication(pub_url)

            if not pdf_links:
                logger.warning(f"No PDF links found for {doc['document_id']}")
                continue

            # Get text from the first (main) PDF
            # Often there's a "final" or main document and translations
            main_pdf = pdf_links[0]
            logger.info(f"Downloading PDF: {main_pdf['url'][:80]}...")

            text = self._fetch_pdf_text(main_pdf['url'])

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {doc['document_id']}")
                continue

            doc['text'] = text
            doc['pdf_url'] = main_pdf['url']
            doc['url'] = pub_url

            yield doc
            fetched += 1

            logger.info(f"Fetched {doc['document_id']} ({len(text):,} chars)")

            time.sleep(2.0)  # Rate limiting between documents

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date"""
        for doc in self.fetch_all():
            if doc.get('date'):
                try:
                    doc_date = datetime.strptime(doc['date'], '%Y-%m-%d')
                    if doc_date >= since:
                        yield doc
                except ValueError:
                    # If we can't parse the date, include the document
                    yield doc
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        return {
            '_id': raw_doc['document_id'],
            '_source': 'EU/EIOPA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'document_id': raw_doc['document_id'],
            'document_type': raw_doc.get('document_type', 'unknown'),
            'title': raw_doc['title'],
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date'),
            'url': raw_doc.get('url', raw_doc.get('publication_url', '')),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = EIOPAFetcher()
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
        fetcher = EIOPAFetcher()

        print("Testing EIOPA fetcher...")
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
