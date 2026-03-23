#!/usr/bin/env python3
"""
EASA Safety Publications Fetcher
Fetches Airworthiness Directives (ADs), Safety Information Bulletins (SIBs),
Safety Directives (SDs), and related publications with full text from PDFs.

Approach:
1. Scrape paginated lists from ad.easa.europa.eu
2. Parse document metadata from list pages
3. Fetch individual document pages for detailed metadata
4. Download PDFs and extract text using pdfplumber
5. Normalize to standard schema

Document types:
- AD: Airworthiness Directive
- SIB: Safety Information Bulletin
- EAD: Emergency Airworthiness Directive
- PAD: Proposed Airworthiness Directive
- SD: Safety Directive
- PSD: Preliminary Safety Directive
"""

import json
import logging
import re
import time
import io
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urljoin

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
BASE_URL = "https://ad.easa.europa.eu"
AD_LIST_URL = f"{BASE_URL}/ad-list/page-{{page}}"

# Document type endpoints
DOC_ENDPOINTS = {
    'AD': '/ad-list/page-{page}',
    'SIB': '/sib-docs/page-{page}',
    'SD': '/sd-docs/page-{page}',
}

# Map icon filenames to document types
DOC_TYPE_MAP = {
    'adclass_ad.gif': 'AD',
    'adclass_sib.gif': 'SIB',
    'adclass_ead.gif': 'EAD',
    'adclass_pad.gif': 'PAD',
    'adclass_sd.gif': 'SD',
    'adclass_psd.gif': 'PSD',
}


class EASAFetcher:
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

    def _parse_list_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse AD/SIB list page to extract document metadata"""
        soup = BeautifulSoup(html, 'html.parser')
        documents = []

        # Find the AD list table
        table = soup.find('table', class_='ad-list')
        if not table:
            logger.warning("No ad-list table found on page")
            return documents

        rows = table.find_all('tr')

        for row in rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 5:
                    continue

                # Cell 0: Document number with link
                num_cell = cells[0]
                num_link = num_cell.find('a')
                if not num_link:
                    continue

                doc_number = num_link.get_text(strip=True)
                doc_url = num_link.get('href', '')
                if doc_url and not doc_url.startswith('http'):
                    doc_url = urljoin(BASE_URL, doc_url)

                # Cell 1: Issuing authority (flag image)
                issuer_cell = cells[1]
                issuer_img = issuer_cell.find('img')
                issuing_authority = issuer_img.get('alt', '') if issuer_img else ''

                # Cell 2: Issue date
                date_cell = cells[2]
                issue_date = date_cell.get_text(strip=True)

                # Cell 3: Subject (includes document type icon)
                subject_cell = cells[3]

                # Extract document type from icon
                doc_type = 'AD'  # default
                type_img = subject_cell.find('img')
                if type_img:
                    img_src = type_img.get('src', '')
                    for icon, dtype in DOC_TYPE_MAP.items():
                        if icon in img_src:
                            doc_type = dtype
                            break

                # Get subject text (strip the icon)
                subject = subject_cell.get_text(strip=True)
                # Remove any leading/trailing whitespace
                subject = ' '.join(subject.split())

                # Cell 4: Approval holder / Type designation
                holder_cell = cells[4]
                approval_holder = ''
                aircraft_type = ''

                # Parse the tree structure
                tc_holder = holder_cell.find('li', class_='tc_holder')
                if tc_holder:
                    # Get the direct text node (manufacturer name)
                    approval_holder = tc_holder.find(string=True, recursive=False)
                    if approval_holder:
                        approval_holder = approval_holder.strip()

                    # Get aircraft type
                    type_li = holder_cell.find('li', class_='type')
                    if type_li:
                        aircraft_type = type_li.find(string=True, recursive=False)
                        if aircraft_type:
                            aircraft_type = aircraft_type.strip()

                # Cell 5: Effective date
                eff_cell = cells[5] if len(cells) > 5 else None
                effective_date = eff_cell.get_text(strip=True) if eff_cell else ''

                # Cell 6: Attachment (PDF link)
                pdf_url = None
                if len(cells) > 6:
                    attach_cell = cells[6]
                    pdf_link = attach_cell.find('a')
                    if pdf_link:
                        pdf_url = pdf_link.get('href', '')
                        if pdf_url and not pdf_url.startswith('http'):
                            pdf_url = urljoin(BASE_URL, pdf_url)

                documents.append({
                    'document_number': doc_number,
                    'document_type': doc_type,
                    'title': subject,
                    'issuing_authority': issuing_authority,
                    'issue_date': issue_date,
                    'effective_date': effective_date,
                    'approval_holder': approval_holder,
                    'aircraft_type': aircraft_type,
                    'doc_url': doc_url,
                    'pdf_url': pdf_url,
                })

            except Exception as e:
                logger.warning(f"Failed to parse row: {e}")
                continue

        return documents

    def _fetch_document_details(self, doc_url: str) -> Dict[str, Any]:
        """Fetch additional details from individual document page"""
        response = self._make_request(doc_url)
        if response is None:
            return {}

        soup = BeautifulSoup(response.text, 'html.parser')
        details = {}

        # Find the detail table
        detail_table = soup.find('table', class_='table-detail')
        if detail_table:
            rows = detail_table.find_all('tr')
            for row in rows:
                title_cell = row.find('td', class_='title')
                value_cell = row.find_all('td')
                if title_cell and len(value_cell) > 1:
                    key = title_cell.get_text(strip=True).lower().replace(' ', '_')
                    value = value_cell[1].get_text(strip=True)
                    details[key] = value

        # Look for PDF links if not already found
        if 'pdf_url' not in details:
            download_div = soup.find('div', id='EL-download')
            if download_div:
                pdf_link = download_div.find('a', href=lambda x: x and '.pdf' in x.lower())
                if pdf_link:
                    pdf_url = pdf_link.get('href', '')
                    if pdf_url and not pdf_url.startswith('http'):
                        pdf_url = urljoin(BASE_URL, pdf_url)
                    details['pdf_url'] = pdf_url

        return details

    def _fetch_list_page(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch a single page of the AD list"""
        url = f"{BASE_URL}/ad-list/page-{page}"

        response = self._make_request(url)
        if response is None:
            return []

        return self._parse_list_page(response.text)

    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all EASA safety publications with full text.

        Args:
            max_docs: Maximum number of documents to fetch (None = all)
        """
        fetched = 0
        page = 1
        consecutive_empty = 0

        while True:
            if max_docs is not None and fetched >= max_docs:
                logger.info(f"Reached max_docs limit ({max_docs})")
                return

            logger.info(f"Fetching AD list page {page}...")
            documents = self._fetch_list_page(page)

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

                logger.info(f"Processing: {doc['document_number']} - {doc['title'][:50]}...")

                # Get PDF URL
                pdf_url = doc.get('pdf_url')

                # If no PDF URL from list, try fetching details page
                if not pdf_url:
                    logger.info(f"Fetching details for {doc['document_number']}...")
                    details = self._fetch_document_details(doc['doc_url'])
                    pdf_url = details.get('pdf_url')

                    # Update doc with additional details
                    if details.get('easa_approval_number'):
                        doc['easa_approval_number'] = details['easa_approval_number']
                    if details.get('revision'):
                        doc['revision'] = details['revision']
                    if details.get('supersedure') and details['supersedure'] != 'None':
                        doc['supersedes'] = details['supersedure']

                    time.sleep(1.0)

                if not pdf_url:
                    logger.warning(f"No PDF found for {doc['document_number']}")
                    continue

                # Download PDF and extract text
                logger.info(f"Downloading PDF: {pdf_url}")
                text = self._fetch_pdf_text(pdf_url)

                if not text or len(text) < 50:
                    logger.warning(f"Insufficient text for {doc['document_number']}")
                    continue

                doc['text'] = text
                doc['pdf_url'] = pdf_url

                yield doc
                fetched += 1

                logger.info(f"Fetched {doc['document_number']} ({len(text):,} chars)")

                time.sleep(2.0)  # Rate limiting

            page += 1
            time.sleep(1.0)  # Rate limiting between pages

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date"""
        for doc in self.fetch_all():
            if doc.get('issue_date'):
                try:
                    doc_date = datetime.strptime(doc['issue_date'], '%Y-%m-%d')
                    if doc_date >= since:
                        yield doc
                except ValueError:
                    # If we can't parse the date, include the document
                    yield doc
            else:
                yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        # Parse dates
        issue_date = raw_doc.get('issue_date', '')
        effective_date = raw_doc.get('effective_date', '')

        # Clean dates (already in YYYY-MM-DD format from the site)
        if issue_date and not re.match(r'^\d{4}-\d{2}-\d{2}$', issue_date):
            issue_date = None
        if effective_date and not re.match(r'^\d{4}-\d{2}-\d{2}$', effective_date):
            effective_date = None

        return {
            '_id': f"EASA-{raw_doc['document_number']}",
            '_source': 'EU/EASA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'document_number': raw_doc['document_number'],
            'document_type': raw_doc.get('document_type', 'AD'),
            'title': raw_doc['title'],
            'text': raw_doc.get('text', ''),
            'date': issue_date,
            'effective_date': effective_date or None,
            'issuing_authority': raw_doc.get('issuing_authority', ''),
            'approval_holder': raw_doc.get('approval_holder', ''),
            'aircraft_type': raw_doc.get('aircraft_type', ''),
            'easa_approval_number': raw_doc.get('easa_approval_number', ''),
            'revision': raw_doc.get('revision', ''),
            'supersedes': raw_doc.get('supersedes', ''),
            'url': raw_doc.get('doc_url', ''),
            'pdf_url': raw_doc.get('pdf_url', ''),
        }


def main():
    """Main entry point"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = EASAFetcher()
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
            if len(normalized.get('text', '')) < 50:
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
        fetcher = EASAFetcher()

        print("Testing EASA fetcher...")
        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Number: {normalized['document_number']}")
            print(f"Type: {normalized['document_type']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"Issuer: {normalized['issuing_authority']}")
            print(f"Aircraft: {normalized['approval_holder']} / {normalized['aircraft_type']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"URL: {normalized['url']}")
            count += 1

        print(f"\nSuccessfully fetched {count} documents")


if __name__ == '__main__':
    main()
