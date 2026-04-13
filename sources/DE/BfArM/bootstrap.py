#!/usr/bin/env python3
"""
German Federal Institute for Drugs and Medical Devices (BfArM) - Rote-Hand-Briefe Fetcher

This fetcher retrieves Rote-Hand-Briefe (Red Hand Letters) - official urgent safety
communications about pharmaceuticals from BfArM.

Data sources:
- RSS feed: Recent entries (approx. 20)
- HTML pagination: Full archive (~540+ documents since 2007)
- PDF documents: Full text extraction

Full text is extracted from PDF documents using pdfminer.
Data is public domain official government works under German law (§ 5 UrhG).
"""

import io
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from xml.etree import ElementTree as ET

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://www.bfarm.de"
RSS_FEED = "/SiteGlobals/Functions/RSSFeed/DE/Pharmakovigilanz/Rote-Hand-Briefe/RSSNewsfeed.xml?nn=591002"
LIST_PAGE = "/DE/Arzneimittel/Pharmakovigilanz/Risikoinformationen/Rote-Hand-Briefe/_node.html"
TOTAL_PAGES = 55  # Approximate number of pages


class BfArMFetcher:
    """Fetcher for BfArM Rote-Hand-Briefe (Red Hand Letters)"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _fetch_rss(self) -> List[Dict[str, str]]:
        """Fetch entries from RSS feed"""
        url = BASE_URL + RSS_FEED
        entries = []

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            root = ET.fromstring(response.content)

            for item in root.findall('.//item'):
                entry = {
                    'title': item.findtext('title', '').strip(),
                    'link': item.findtext('link', '').strip(),
                    'pub_date': item.findtext('pubDate', '').strip(),
                    'description': item.findtext('description', '').strip(),
                }
                if entry['link']:
                    entries.append(entry)

            logger.info(f"Fetched {len(entries)} entries from RSS feed")

        except Exception as e:
            logger.error(f"Error fetching RSS feed: {e}")

        return entries

    def _fetch_page_entries(self, page: int) -> List[Dict[str, str]]:
        """Fetch entries from a specific pagination page"""
        url = f"{BASE_URL}{LIST_PAGE}?gtp=964792_list%253D{page}"
        entries = []

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            html = response.text

            # Extract entry links and dates using regex patterns
            # Pattern for HTML entries - find all SharedDocs links
            link_pattern = r'href="(/SharedDocs/Risikoinformationen/Pharmakovigilanz/DE/RHB/\d{4}/[^"]+\.html)"'
            matches = re.findall(link_pattern, html)

            for link in matches:
                full_url = BASE_URL + link
                if full_url not in [e['link'] for e in entries]:
                    entries.append({
                        'link': full_url,
                        'title': '',  # Will be filled from page content
                        'pub_date': '',
                        'description': '',
                    })

            logger.debug(f"Page {page}: Found {len(entries)} entries")

        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")

        return entries

    def _fetch_entry_metadata(self, entry: Dict[str, str]) -> Dict[str, str]:
        """Fetch full metadata from an entry's HTML page"""
        url = entry.get('link', '')
        if not url:
            return entry

        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            html = response.text

            # Extract title from page
            title_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html)
            if title_match and not entry.get('title'):
                entry['title'] = title_match.group(1).strip()

            # Extract PDF URL
            pdf_match = re.search(r'href="([^"]*\.pdf\?[^"]*)"', html)
            if pdf_match:
                pdf_url = pdf_match.group(1)
                if pdf_url.startswith('/'):
                    pdf_url = BASE_URL + pdf_url
                entry['pdf_url'] = pdf_url

            # Extract date from URL path (e.g., /2026/rhb-xyz.html -> 2026)
            year_match = re.search(r'/RHB/(\d{4})/', url)
            if year_match and not entry.get('year'):
                entry['year'] = year_match.group(1)

            # Extract date from page content
            date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', html)
            if date_match:
                entry['date_str'] = date_match.group(1)

        except Exception as e:
            logger.error(f"Error fetching metadata from {url}: {e}")

        return entry

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="DE/BfArM",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def _parse_date(self, date_str: str, pub_date: str = None, year: str = None) -> Optional[str]:
        """Parse various date formats to ISO 8601"""
        # Try RSS pubDate format first (most reliable)
        # Format: "Fri, 6 Feb 2026 09:00:00 +0100"
        if pub_date:
            try:
                dt = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %z")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Try German date format (DD.MM.YYYY)
        if date_str:
            match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
            if match:
                day, month, year_parsed = match.groups()
                return f"{year_parsed}-{month.zfill(2)}-{day.zfill(2)}"

        # Fall back to year only
        if year:
            return f"{year}-01-01"

        return None

    def _generate_id(self, url: str) -> str:
        """Generate a unique ID from the document URL"""
        # Extract the file name from URL
        # e.g., /SharedDocs/Risikoinformationen/.../2026/rhb-arixtra.html -> rhb_arixtra_2026
        match = re.search(r'/(\d{4})/([^/]+)\.html', url)
        if match:
            year, name = match.groups()
            # Clean the name
            name = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
            return f"bfarm_{name}_{year}"

        # Fallback: hash the URL
        import hashlib
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        return f"bfarm_{url_hash}"

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all Rote-Hand-Briefe with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)

        Yields:
            Raw document dictionaries with full text
        """
        seen_urls = set()
        all_entries = []

        # Step 1: Get entries from RSS feed (most recent)
        logger.info("Fetching entries from RSS feed...")
        rss_entries = self._fetch_rss()
        for entry in rss_entries:
            if entry['link'] not in seen_urls:
                seen_urls.add(entry['link'])
                all_entries.append(entry)

        # Step 2: Crawl pagination pages for full archive
        logger.info("Fetching entries from pagination pages...")
        for page in range(1, TOTAL_PAGES + 1):
            if limit and len(all_entries) >= limit * 2:  # Buffer for deduplication
                break

            page_entries = self._fetch_page_entries(page)
            new_count = 0
            for entry in page_entries:
                if entry['link'] not in seen_urls:
                    seen_urls.add(entry['link'])
                    all_entries.append(entry)
                    new_count += 1

            if new_count > 0:
                logger.info(f"Page {page}: Added {new_count} new entries (total: {len(all_entries)})")

            time.sleep(1)  # Rate limiting

            if not page_entries:
                logger.info(f"No more entries found at page {page}, stopping.")
                break

        logger.info(f"Total entries found: {len(all_entries)}")

        # Step 3: Process each entry
        count = 0
        for entry in all_entries:
            if limit and count >= limit:
                logger.info(f"Reached limit of {limit} documents")
                return

            url = entry.get('link', '')
            logger.info(f"[{count + 1}] Processing: {url}")

            # Fetch metadata if needed
            entry = self._fetch_entry_metadata(entry)
            time.sleep(1)

            # Extract PDF text
            pdf_url = entry.get('pdf_url', '')
            if pdf_url:
                logger.info(f"  Extracting text from PDF...")
                text = self._extract_pdf_text(pdf_url)
                time.sleep(1)
            else:
                logger.warning(f"  No PDF found, using description as text")
                text = entry.get('description', '')

            if not text or len(text) < 100:
                logger.warning(f"  Insufficient text content, skipping")
                continue

            doc = {
                'url': url,
                'pdf_url': pdf_url,
                'title': entry.get('title', ''),
                'description': entry.get('description', ''),
                'pub_date': entry.get('pub_date', ''),
                'date_str': entry.get('date_str', ''),
                'year': entry.get('year', ''),
                'text': text,
            }

            yield doc
            count += 1

        logger.info(f"Fetched {count} documents with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent documents from RSS feed"""
        for entry in self._fetch_rss():
            # Check if entry is recent
            pub_date = entry.get('pub_date', '')
            if pub_date:
                try:
                    entry_date = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %z")
                    if entry_date.replace(tzinfo=None) < since:
                        continue
                except ValueError:
                    pass

            entry = self._fetch_entry_metadata(entry)
            time.sleep(1)

            pdf_url = entry.get('pdf_url', '')
            if pdf_url:
                text = self._extract_pdf_text(pdf_url)
                time.sleep(1)
            else:
                text = entry.get('description', '')

            if not text or len(text) < 100:
                continue

            doc = {
                'url': entry.get('link', ''),
                'pdf_url': pdf_url,
                'title': entry.get('title', ''),
                'description': entry.get('description', ''),
                'pub_date': entry.get('pub_date', ''),
                'date_str': entry.get('date_str', ''),
                'year': entry.get('year', ''),
                'text': text,
            }

            yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        url = raw_doc.get('url', '')
        rhb_id = self._generate_id(url)

        # Parse date
        date = self._parse_date(
            raw_doc.get('date_str', ''),
            raw_doc.get('pub_date', ''),
            raw_doc.get('year', '')
        )

        # Clean title
        title = raw_doc.get('title', '')
        if not title:
            # Extract title from first line of text
            text = raw_doc.get('text', '')
            if text:
                first_line = text.split('\n')[0].strip()
                if len(first_line) > 10:
                    title = first_line[:200]

        return {
            '_id': rhb_id,
            '_source': 'DE/BfArM',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': date,
            'url': url,
            'pdf_url': raw_doc.get('pdf_url', ''),
            'description': raw_doc.get('description', ''),
            'authority': 'Bundesinstitut fuer Arzneimittel und Medizinprodukte (BfArM)',
            'document_type': 'Rote-Hand-Brief',
            'language': 'de',
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BfArMFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 15 if '--sample' in sys.argv else 100

        for raw_doc in fetcher.fetch_all(limit=target_count + 5):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_').replace(':', '_')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['title'][:60]}... ({text_len:,} chars)")
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
        fetcher = BfArMFetcher()
        print("Testing BfArM Rote-Hand-Briefe fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:100]}")
            print(f"Date: {normalized['date']}")
            print(f"URL: {normalized['url']}")
            print(f"PDF: {normalized['pdf_url']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview:\n{normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
