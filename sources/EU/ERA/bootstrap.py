#!/usr/bin/env python3
"""
ERA (European Union Agency for Railways) Document Fetcher
Fetches opinions, technical advice, and recommendations with full text from PDFs.

Approach:
1. Scrape paginated listing pages for document links
2. Visit each document content page to extract metadata + PDF URL
3. Download PDFs and extract text using pdfplumber
4. Normalize to standard schema

Document types:
- opinion: Non-binding expert guidance to decision-makers
- technical_advice: Implementation guidance issued to the European Commission
- recommendation: Formal recommendations on railway regulations
"""

import json
import logging
import re
import time
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urljoin, urlparse, parse_qs

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
BASE_URL = "https://www.era.europa.eu"

# Document listing endpoints
DOC_ENDPOINTS = {
    'opinions': '/library/documents-regulations/opinions-and-technical-advices',
    'recommendations': '/library/documents-regulations/era-recommendations_en',
}


class ERAFetcher:
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

    def _parse_list_page(self, html: str, doc_type: str) -> List[Dict[str, Any]]:
        """Parse listing page to extract document links"""
        soup = BeautifulSoup(html, 'html.parser')
        documents = []

        # Find content links matching ERA document patterns
        # Opinions: /content/opinion-eraopi...
        # Recommendations: /content/recommendation-era...
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')

            # Match opinion/technical advice pages
            if doc_type == 'opinions' and '/content/' in href:
                if 'opinion-era' in href.lower() or 'advice-era' in href.lower():
                    full_url = urljoin(BASE_URL, href)
                    title = link.get_text(strip=True)
                    if title and full_url not in [d.get('content_url') for d in documents]:
                        documents.append({
                            'content_url': full_url,
                            'title': title,
                            'doc_type': 'opinion' if 'opinion' in href.lower() else 'technical_advice'
                        })

            # Match recommendation pages
            elif doc_type == 'recommendations' and '/content/' in href:
                if 'recommendation' in href.lower():
                    full_url = urljoin(BASE_URL, href)
                    title = link.get_text(strip=True)
                    if title and full_url not in [d.get('content_url') for d in documents]:
                        documents.append({
                            'content_url': full_url,
                            'title': title,
                            'doc_type': 'recommendation'
                        })

        return documents

    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """Extract date from text in various formats"""
        # Try common date patterns
        patterns = [
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(\d{4})-(\d{2})-(\d{2})',
        ]

        months = {
            'january': '01', 'february': '02', 'march': '03', 'april': '04',
            'may': '05', 'june': '06', 'july': '07', 'august': '08',
            'september': '09', 'october': '10', 'november': '11', 'december': '12'
        }

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) == 3 and groups[1].lower() in months:
                    # Format: day month year
                    day = groups[0].zfill(2)
                    month = months[groups[1].lower()]
                    year = groups[2]
                    return f"{year}-{month}-{day}"
                elif len(groups) == 3 and groups[0].isdigit() and len(groups[0]) == 4:
                    # Format: YYYY-MM-DD
                    return f"{groups[0]}-{groups[1]}-{groups[2]}"

        return None

    def _fetch_document_details(self, content_url: str) -> Dict[str, Any]:
        """Fetch document metadata and PDF URL from content page"""
        response = self._make_request(content_url)
        if response is None:
            return {}

        soup = BeautifulSoup(response.text, 'html.parser')
        details = {}

        # Extract title from h1 or title tag
        h1 = soup.find('h1')
        if h1:
            details['title'] = h1.get_text(strip=True)

        # Look for publication date
        # ERA pages often have date in structured format or in page content
        time_tag = soup.find('time')
        if time_tag:
            date_str = time_tag.get('datetime', '') or time_tag.get_text(strip=True)
            if date_str:
                parsed_date = self._extract_date_from_text(date_str)
                if parsed_date:
                    details['date'] = parsed_date

        # If no time tag, look for date in text
        if 'date' not in details:
            page_text = soup.get_text()
            parsed_date = self._extract_date_from_text(page_text)
            if parsed_date:
                details['date'] = parsed_date

        # Extract document number from title (ERA/OPI/YYYY-N, ERA/ADV/YYYY-N, ERANNNN)
        title = details.get('title', '')

        # Pattern for opinions: ERA/OPI/YYYY-N
        opi_match = re.search(r'ERA/OPI/(\d{4})-(\d+)', title, re.IGNORECASE)
        if opi_match:
            details['document_number'] = f"ERA/OPI/{opi_match.group(1)}-{opi_match.group(2)}"

        # Pattern for technical advice: ERA/ADV/YYYY-N
        adv_match = re.search(r'ERA/ADV/(\d{4})-(\d+)', title, re.IGNORECASE)
        if adv_match:
            details['document_number'] = f"ERA/ADV/{adv_match.group(1)}-{adv_match.group(2)}"

        # Pattern for recommendations: ERANNNN
        rec_match = re.search(r'ERA\s*(\d{3,4})', title, re.IGNORECASE)
        if rec_match and 'document_number' not in details:
            details['document_number'] = f"ERA{rec_match.group(1)}"

        # Look for addressee (to France, to Estonia, to European Commission, etc.)
        addr_match = re.search(r'(?:to|for)\s+(?:the\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', title)
        if addr_match:
            details['addressee'] = addr_match.group(1)

        # Find PDF links
        pdf_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if '.pdf' in href.lower():
                full_url = href if href.startswith('http') else urljoin(BASE_URL, href)
                # Clean URL (remove cache busting params for storage)
                pdf_links.append(full_url)

        if pdf_links:
            # Prefer the main document (often first or contains 'signed' or document number)
            main_pdf = pdf_links[0]
            for pdf in pdf_links:
                if 'signed' in pdf.lower() or (details.get('document_number', '').lower() in pdf.lower()):
                    main_pdf = pdf
                    break
            details['pdf_url'] = main_pdf
            details['all_pdf_urls'] = pdf_links

        return details

    def _fetch_list_pages(self, doc_type: str, max_pages: int = 20) -> List[Dict[str, Any]]:
        """Fetch all pages of document listing"""
        endpoint = DOC_ENDPOINTS.get(doc_type, '')
        if not endpoint:
            return []

        all_documents = []
        page = 0
        seen_urls = set()

        while page < max_pages:
            # ERA uses query parameter for pagination
            url = f"{BASE_URL}{endpoint}"
            if page > 0:
                url += f"?page={page}"

            logger.info(f"Fetching {doc_type} list page {page}...")
            response = self._make_request(url)
            if response is None:
                break

            documents = self._parse_list_page(response.text, doc_type)

            # Filter duplicates
            new_docs = []
            for doc in documents:
                if doc['content_url'] not in seen_urls:
                    seen_urls.add(doc['content_url'])
                    new_docs.append(doc)

            if not new_docs:
                logger.info(f"No new documents on page {page}, stopping pagination")
                break

            all_documents.extend(new_docs)
            logger.info(f"Found {len(new_docs)} new documents on page {page} (total: {len(all_documents)})")

            page += 1
            time.sleep(1.0)

        return all_documents

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all ERA documents with full text.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
        """
        fetched = 0

        for doc_type, endpoint in DOC_ENDPOINTS.items():
            if max_docs is not None and fetched >= max_docs:
                return

            logger.info(f"Fetching {doc_type} documents...")
            doc_links = self._fetch_list_pages(doc_type)
            logger.info(f"Found {len(doc_links)} {doc_type} document links")

            for doc_link in doc_links:
                if max_docs is not None and fetched >= max_docs:
                    return

                content_url = doc_link['content_url']
                logger.info(f"Processing: {doc_link['title'][:60]}...")

                # Fetch document details
                details = self._fetch_document_details(content_url)
                time.sleep(1.5)

                if not details.get('pdf_url'):
                    logger.warning(f"No PDF found for: {doc_link['title'][:50]}")
                    continue

                # Download PDF and extract text
                pdf_url = details['pdf_url']
                logger.info(f"Downloading PDF: {pdf_url[:80]}...")
                text = self._fetch_pdf_text(pdf_url)

                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text extracted from PDF: {doc_link['title'][:50]}")
                    continue

                # Merge link info with details
                doc = {
                    'content_url': content_url,
                    'doc_type': doc_link.get('doc_type', doc_type.rstrip('s')),
                    'title': details.get('title', doc_link['title']),
                    'document_number': details.get('document_number', ''),
                    'date': details.get('date', ''),
                    'addressee': details.get('addressee', ''),
                    'pdf_url': pdf_url,
                    'text': text,
                }

                yield doc
                fetched += 1
                logger.info(f"Fetched {doc.get('document_number', 'doc')} ({len(text):,} chars)")

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
                    yield doc
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        # Generate ID from document number or URL
        doc_number = raw_doc.get('document_number', '')
        if not doc_number:
            # Generate from URL
            url_parts = raw_doc.get('content_url', '').split('/')
            doc_number = url_parts[-1] if url_parts else f"era-{hash(raw_doc.get('title', ''))}"

        # Clean document number for ID
        doc_id = doc_number.replace('/', '-').replace(' ', '-')

        return {
            '_id': f"ERA-{doc_id}",
            '_source': 'EU/ERA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'document_number': doc_number,
            'document_type': raw_doc.get('doc_type', 'opinion'),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': raw_doc.get('date', None),
            'addressee': raw_doc.get('addressee', ''),
            'url': raw_doc.get('content_url', ''),
            'pdf_url': raw_doc.get('pdf_url', ''),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ERAFetcher()
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

            logger.info(f"Saved: {normalized['document_number']} - {normalized['title'][:50]}... ({len(normalized['text']):,} chars)")
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
        fetcher = ERAFetcher()

        print("Testing ERA fetcher...")
        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Number: {normalized['document_number']}")
            print(f"Type: {normalized['document_type']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"Addressee: {normalized['addressee']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"URL: {normalized['url']}")
            count += 1

        print(f"\nSuccessfully fetched {count} documents")


if __name__ == '__main__':
    main()
