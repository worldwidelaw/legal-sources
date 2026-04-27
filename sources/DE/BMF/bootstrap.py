#!/usr/bin/env python3
"""
German Federal Ministry of Finance Tax Circulars (BMF-Schreiben) Fetcher

Official doctrine from bundesfinanzministerium.de
https://www.bundesfinanzministerium.de/Web/DE/Service/Publikationen/BMF_Schreiben/bmf_schreiben.html

This fetcher retrieves BMF-Schreiben (tax circulars) using:
- HTML listing pages for document discovery (500+ documents with pagination)
- PDF downloads for full text extraction

Data is public domain (amtliche Werke) under German law (§ 5 UrhG).
"""

import html
import io
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urljoin, quote

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.bundesfinanzministerium.de"
LISTING_URL = f"{BASE_URL}/Web/DE/Service/Publikationen/BMF_Schreiben/bmf_schreiben.html"


class BMFFetcher:
    """Fetcher for German Federal Ministry of Finance Tax Circulars"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_listing_page(self, page: int = 1) -> str:
        """Fetch a specific page of the BMF-Schreiben listing"""
        if page == 1:
            url = LISTING_URL
        else:
            # Pagination parameter format: gtp=246444_list%3D{page}
            url = f"{LISTING_URL}?gtp=246444_list%3D{page}"

        logger.info(f"Fetching listing page {page}")
        response = self.session.get(url, timeout=60)
        response.raise_for_status()
        return response.text

    def _parse_listing_page(self, html_content: str) -> List[Dict[str, Any]]:
        """Parse the HTML listing page to extract document links and metadata"""
        soup = BeautifulSoup(html_content, 'html.parser')
        documents = []

        # Find all bmf-entry blocks
        entries = soup.find_all('div', class_='bmf-entry')

        for entry in entries:
            doc = {}

            # Extract title from h3 > a
            title_elem = entry.find('h3', class_='bmf-entry-title')
            if title_elem:
                link_elem = title_elem.find('a', class_='bmf-resultlist-teaser-link')
                if link_elem:
                    doc['title'] = link_elem.get_text(strip=True)
                    doc['detail_url'] = urljoin(BASE_URL, link_elem.get('href', ''))

            # Extract category from labelbox
            label_elem = entry.find('span', class_='bmf-labelbox-text')
            if label_elem:
                doc['category'] = label_elem.get_text(strip=True)

            # Extract date from time element
            time_elem = entry.find('time')
            if time_elem:
                doc['date'] = time_elem.get('datetime', '')
                doc['date_display'] = time_elem.get_text(strip=True)

            # Extract PDF download link
            download_link = entry.find('a', class_='bmf-link--download')
            if download_link:
                pdf_url = download_link.get('href', '')
                if pdf_url:
                    doc['pdf_url'] = urljoin(BASE_URL, pdf_url)
                    # Extract file size from link text
                    link_text = download_link.find('span', class_='bmf-link-text')
                    if link_text:
                        doc['file_info'] = link_text.get_text(strip=True)

            if doc.get('title') and doc.get('pdf_url'):
                documents.append(doc)

        return documents

    def _get_total_pages(self, html_content: str) -> int:
        """Extract total number of pages from pagination"""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Look for pagination links
        page_links = soup.find_all('a', class_='bmf-navIndex-link')

        max_page = 1
        for link in page_links:
            href = link.get('href', '')
            # Extract page number from URL
            match = re.search(r'gtp=246444_list%[23]D(\d+)', href)
            if match:
                page_num = int(match.group(1))
                max_page = max(max_page, page_num)

        return max_page

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="DE/BMF",
            source_id="",
            pdf_bytes=pdf_content,
            table="doctrine",
        ) or ""

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text"""
        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace but preserve paragraph breaks
        text = re.sub(r'\n{4,}', '\n\n\n', text)
        text = re.sub(r' {2,}', ' ', text)

        # Clean up hyphenation at line breaks
        text = re.sub(r'-\s*\n\s*', '', text)

        return text.strip()

    def _extract_metadata_from_text(self, text: str) -> Dict[str, Any]:
        """Extract metadata (GZ, DOK, etc.) from document text"""
        metadata = {}

        # Extract GZ (Geschäftszeichen) - format: "GZ: III C 3 - S 7117-e/00003/005/058"
        gz_match = re.search(r'GZ:\s*([^\n]+)', text)
        if gz_match:
            metadata['geschaeftszeichen'] = gz_match.group(1).strip()

        # Extract DOK - format: "DOK: COO.7005.100.3.14287537"
        dok_match = re.search(r'DOK:\s*([^\n]+)', text)
        if dok_match:
            metadata['dok_id'] = dok_match.group(1).strip()

        # Extract Betreff (subject) - format: "Betreff: ..."
        betreff_match = re.search(r'Betreff:\s*([^\n]+(?:\n[^\n]+)*?)(?=\n\nGZ:|$)', text)
        if betreff_match:
            metadata['betreff'] = betreff_match.group(1).strip()

        return metadata

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to ISO 8601 format"""
        if not date_str:
            return None

        # Try ISO format (YYYY-MM-DDTHH:MM)
        try:
            if 'T' in date_str:
                dt = datetime.fromisoformat(date_str.split('T')[0])
                return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

        # Try German format (DD.MM.YYYY)
        try:
            dt = datetime.strptime(date_str, '%d.%m.%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

        return date_str

    def _fetch_document(self, doc_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch PDF and extract full text"""
        pdf_url = doc_info.get('pdf_url')
        if not pdf_url:
            return None

        try:
            logger.info(f"Downloading PDF: {doc_info.get('title', 'Unknown')[:60]}...")
            response = self.session.get(pdf_url, timeout=60)
            response.raise_for_status()

            # Verify it's a PDF
            if not response.content.startswith(b'%PDF'):
                logger.warning(f"Response is not a PDF for {pdf_url}")
                return None

            # Extract text
            text = self._extract_pdf_text(response.content)
            if not text:
                logger.warning(f"Could not extract text from PDF: {pdf_url}")
                return None

            text = self._clean_text(text)

            # Extract additional metadata from text
            metadata = self._extract_metadata_from_text(text)

            return {
                'title': doc_info.get('title', ''),
                'text': text,
                'pdf_url': pdf_url,
                'detail_url': doc_info.get('detail_url', ''),
                'category': doc_info.get('category', ''),
                'date': doc_info.get('date', ''),
                'date_display': doc_info.get('date_display', ''),
                'file_info': doc_info.get('file_info', ''),
                **metadata
            }

        except requests.RequestException as e:
            logger.error(f"Error fetching PDF {pdf_url}: {e}")
            return None

    def fetch_all(self, limit: int = None, pages: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch BMF-Schreiben with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)
            pages: Maximum number of pages to scan (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        # Get first page to determine total pages
        first_page_html = self._get_listing_page(1)
        total_pages = self._get_total_pages(first_page_html)

        if pages:
            total_pages = min(total_pages, pages)

        logger.info(f"Found {total_pages} pages of BMF-Schreiben")

        count = 0
        page_num = 1

        while page_num <= total_pages:
            if limit and count >= limit:
                break

            # Use first page HTML if already fetched
            if page_num == 1:
                html_content = first_page_html
            else:
                html_content = self._get_listing_page(page_num)
                time.sleep(1.0)  # Rate limiting

            documents = self._parse_listing_page(html_content)
            logger.info(f"Page {page_num}: Found {len(documents)} documents")

            for doc_info in documents:
                if limit and count >= limit:
                    break

                doc = self._fetch_document(doc_info)

                if doc and doc.get('text') and len(doc.get('text', '')) > 200:
                    yield doc
                    count += 1
                    logger.info(f"[{count}] {doc['title'][:60]}... ({len(doc['text']):,} chars)")
                else:
                    logger.warning(f"Skipping document - insufficient text: {doc_info.get('title', 'Unknown')[:50]}")

                time.sleep(1.5)  # Rate limiting

            page_num += 1

        logger.info(f"Fetched {count} BMF-Schreiben with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent BMF-Schreiben (first few pages only)"""
        yield from self.fetch_all(pages=3)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        # Create document ID from GZ or DOK if available, else from title
        doc_id = raw_doc.get('geschaeftszeichen') or raw_doc.get('dok_id')
        if not doc_id:
            # Generate ID from title and date
            title_slug = re.sub(r'[^a-zA-Z0-9]+', '-', raw_doc.get('title', 'unknown'))[:50]
            date_str = self._parse_date(raw_doc.get('date', '')) or 'nodate'
            doc_id = f"{date_str}-{title_slug}"

        # Clean up doc_id
        doc_id = re.sub(r'[^\w\-./]', '-', doc_id)

        # Parse date
        date = self._parse_date(raw_doc.get('date'))

        # Build URL (prefer detail URL, fallback to PDF)
        url = raw_doc.get('detail_url') or raw_doc.get('pdf_url', '')

        return {
            '_id': doc_id,
            '_source': 'DE/BMF',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', ''),
            'text': raw_doc.get('text', ''),
            'date': date,
            'url': url,
            'pdf_url': raw_doc.get('pdf_url', ''),
            'geschaeftszeichen': raw_doc.get('geschaeftszeichen', ''),
            'dok_id': raw_doc.get('dok_id', ''),
            'category': raw_doc.get('category', ''),
            'betreff': raw_doc.get('betreff', ''),
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BMFFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None

        # For sample, just fetch first 2 pages
        pages_to_scan = 2 if is_sample else None

        for raw_doc in fetcher.fetch_all(limit=target_count, pages=pages_to_scan):
            if target_count and sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_').replace(' ', '_')[:80]
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}]: {normalized['title'][:50]}... ({text_len:,} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob('*.json'))
        total_chars = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        # Test mode
        fetcher = BMFFetcher()
        print("Testing BMF fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3, pages=1):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"GZ: {normalized.get('geschaeftszeichen', 'N/A')}")
            print(f"DOK: {normalized.get('dok_id', 'N/A')}")
            print(f"Date: {normalized['date']}")
            print(f"Category: {normalized.get('category', 'N/A')}")
            print(f"Title: {normalized['title'][:100]}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
