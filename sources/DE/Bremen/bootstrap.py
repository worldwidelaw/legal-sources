#!/usr/bin/env python3
"""
Bremen State Law (Transparenzportal) Fetcher

Official open data from transparenz.bremen.de
https://www.transparenz.bremen.de/daten/gesetze-und-rechtsverordnungen-bremen-8261

This fetcher retrieves Bremen state legislation using:
- XML export feed for document metadata and URLs
- HTML detail pages for full text extraction

Data is licensed under CC-BY-3.0 (Creative Commons Attribution 3.0).
Attribution: Senator für Finanzen, Bremen
"""

import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urlparse, unquote

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.transparenz.bremen.de"
XML_EXPORT_URL = "https://www.transparenz.bremen.de/sixcms/detail.php?template=30_export_template_ifg_d&dt=Gesetze+und+Rechtsverordnungen"


class BremenFetcher:
    """Fetcher for Bremen state legislation from Transparenzportal"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _fetch_xml_feed(self) -> str:
        """Fetch the XML export containing all legislation metadata"""
        logger.info(f"Fetching XML feed from {XML_EXPORT_URL}")
        try:
            response = self.session.get(XML_EXPORT_URL, timeout=120)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.error(f"Error fetching XML feed: {e}")
            raise

    def _parse_xml_articles(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse XML feed to extract article metadata"""
        articles = []

        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            return articles

        for article in root.findall('.//sixcms_article'):
            metadata = {}

            for field in article.findall('field'):
                name = field.get('name')
                if name and field.text:
                    # Handle CDATA content
                    text = field.text.strip() if field.text else ''
                    if text:
                        metadata[name] = text

            # Only include articles with URLs to detail pages
            url = metadata.get('url', '')
            if url and metadata.get('title'):
                # Skip PDF-only entries (we can't extract full text from PDFs reliably)
                if not url.lower().endswith('.pdf'):
                    articles.append(metadata)
                else:
                    # Even for PDF entries, check if there's a transparenz.bremen.de detail page
                    import_id = metadata.get('import_id', '')
                    if import_id and 'bremen203' in import_id:
                        # Try to construct a detail page URL
                        doc_id = import_id.split('.')[-2] if '.' in import_id else ''
                        if doc_id.isdigit():
                            articles.append(metadata)

        logger.info(f"Parsed {len(articles)} articles from XML feed")
        return articles

    def _get_detail_page_url(self, metadata: Dict[str, Any]) -> Optional[str]:
        """Construct or extract the detail page URL for an article"""
        url = metadata.get('url', '')

        # If URL is already a transparenz.bremen.de detail page, use it
        if 'transparenz.bremen.de' in url and '/metainformationen/' in url:
            return url

        # If URL is a PDF or external site, try to construct detail page from import_id
        import_id = metadata.get('import_id', '')
        if import_id:
            # import_id format: bremen203.c.316429.de
            parts = import_id.split('.')
            if len(parts) >= 3:
                doc_id = parts[-2]  # e.g., 316429
                if doc_id.isdigit():
                    # Construct a search/detail URL
                    title = metadata.get('title', '').lower()
                    title_slug = re.sub(r'[^a-z0-9äöüß]+', '-', title)[:80]
                    return f"{BASE_URL}/metainformationen/{title_slug}-{doc_id}?asl=bremen203_tpgesetz.c.55340.de&template=20_gp_ifg_meta_detail_d"

        return url if 'transparenz.bremen.de' in url else None

    def _fetch_detail_page(self, url: str) -> Optional[str]:
        """Fetch HTML content from a detail page"""
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logger.warning(f"Error fetching detail page {url}: {e}")
            return None

    def _extract_full_text(self, html: str) -> str:
        """Extract full text content from detail page HTML"""
        if not html:
            return ''

        # Try multiple extraction patterns for the main content area

        # Pattern 1: Look for the main content div with class "text" or "inhalt"
        content_patterns = [
            r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>\s*(?:<div|<!--)',
            r'<div[^>]*class="[^"]*text[^"]*"[^>]*>(.*?)</div>\s*(?:<div|<!--)',
            r'<div[^>]*id="[^"]*content[^"]*"[^>]*>(.*?)</div>\s*(?:<div|<!--)',
            r'<article[^>]*>(.*?)</article>',
            # Specific to Bremen Transparenzportal structure
            r'<div[^>]*class="[^"]*gp_ifg_meta_detail[^"]*"[^>]*>(.*?)</div>\s*<div[^>]*class="[^"]*footer',
        ]

        text = ''
        for pattern in content_patterns:
            match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
            if match:
                raw_text = match.group(1)
                cleaned = self._clean_html_text(raw_text)
                if len(cleaned) > len(text):
                    text = cleaned

        # If no pattern matched, try extracting from body
        if not text or len(text) < 200:
            body_match = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL | re.IGNORECASE)
            if body_match:
                raw_text = body_match.group(1)
                # Remove navigation, header, footer
                raw_text = re.sub(r'<nav[^>]*>.*?</nav>', '', raw_text, flags=re.DOTALL | re.IGNORECASE)
                raw_text = re.sub(r'<header[^>]*>.*?</header>', '', raw_text, flags=re.DOTALL | re.IGNORECASE)
                raw_text = re.sub(r'<footer[^>]*>.*?</footer>', '', raw_text, flags=re.DOTALL | re.IGNORECASE)
                raw_text = re.sub(r'<aside[^>]*>.*?</aside>', '', raw_text, flags=re.DOTALL | re.IGNORECASE)
                cleaned = self._clean_html_text(raw_text)
                if len(cleaned) > len(text):
                    text = cleaned

        return text

    def _clean_html_text(self, html: str) -> str:
        """Clean HTML to extract plain text"""
        # Remove script and style elements
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<noscript[^>]*>.*?</noscript>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Remove navigation elements commonly found
        text = re.sub(r'<div[^>]*class="[^"]*nav[^"]*"[^>]*>.*?</div>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<div[^>]*class="[^"]*menu[^"]*"[^>]*>.*?</div>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<div[^>]*class="[^"]*breadcrumb[^"]*"[^>]*>.*?</div>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Convert headings to newlines with markers
        text = re.sub(r'</h[1-6]>', '\n\n', text)
        text = re.sub(r'<h[1-6][^>]*>', '\n', text)

        # Convert paragraph and break tags
        text = re.sub(r'</p>', '\n', text)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</div>', '\n', text)
        text = re.sub(r'</li>', '\n', text)
        text = re.sub(r'</tr>', '\n', text)

        # Remove remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Unescape HTML entities
        text = unescape(text)

        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)  # Collapse horizontal whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)  # Limit consecutive newlines
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)  # Remove leading whitespace

        return text.strip()

    def _parse_german_date(self, date_str: str) -> Optional[str]:
        """Convert German date string to ISO 8601"""
        if not date_str:
            return None

        # Try DD.MM.YYYY format first
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month = match.group(2).zfill(2)
            year = match.group(3)
            return f"{year}-{month}-{day}"

        # Map German month names
        months = {
            'Januar': '01', 'Februar': '02', 'März': '03', 'April': '04',
            'Mai': '05', 'Juni': '06', 'Juli': '07', 'August': '08',
            'September': '09', 'Oktober': '10', 'November': '11', 'Dezember': '12'
        }

        # Try parsing "DD. Month YYYY" format
        match = re.match(r'(\d+)\.\s*(\w+)\s*(\d{4})', date_str)
        if match:
            day = match.group(1).zfill(2)
            month_name = match.group(2)
            year = match.group(3)

            month = months.get(month_name)
            if month:
                return f"{year}-{month}-{day}"

        return date_str

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch Bremen legislation with full text.

        Args:
            limit: Maximum number of documents to fetch (None for all)

        Yields:
            Raw document dictionaries with metadata and full text
        """
        # Fetch and parse XML feed
        xml_content = self._fetch_xml_feed()
        articles = self._parse_xml_articles(xml_content)

        if limit:
            articles = articles[:limit * 2]  # Fetch extra in case some fail

        count = 0
        for i, metadata in enumerate(articles):
            if limit and count >= limit:
                break

            title = metadata.get('title', 'Unknown')
            logger.info(f"[{i+1}/{len(articles)}] Processing: {title[:60]}...")

            # Get detail page URL
            detail_url = self._get_detail_page_url(metadata)
            if not detail_url:
                logger.warning(f"No detail URL for: {title}")
                continue

            # Fetch and extract full text
            html = self._fetch_detail_page(detail_url)
            if html:
                full_text = self._extract_full_text(html)

                if full_text and len(full_text) > 200:
                    metadata['full_text'] = full_text
                    metadata['detail_url'] = detail_url
                    yield metadata
                    count += 1
                    logger.info(f"  -> Extracted {len(full_text):,} chars")
                else:
                    logger.warning(f"  -> Insufficient text ({len(full_text) if full_text else 0} chars)")
            else:
                logger.warning(f"  -> Failed to fetch detail page")

            # Rate limiting
            time.sleep(0.5)

        logger.info(f"Fetched {count} documents with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent legislation updates"""
        # For updates, fetch recent documents (sorted by date in XML)
        yield from self.fetch_all(limit=50)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        import_id = raw_doc.get('import_id', '')

        # Extract document ID from import_id (e.g., "bremen203.c.316429.de" -> "316429")
        doc_id = ''
        if import_id:
            parts = import_id.split('.')
            if len(parts) >= 3:
                doc_id = parts[-2]

        title = raw_doc.get('title', '')
        if not title:
            title = f"Vorschrift {doc_id}"

        # Parse date from v_datum field
        date = self._parse_german_date(raw_doc.get('v_datum', ''))

        # All laws, regulations, statutes, directives are classified as legislation
        doc_type = 'legislation'

        # Get the subject area
        subject_area = raw_doc.get('metadaten_sachgebietsfelder_r', '')

        return {
            '_id': f"HB-{doc_id}" if doc_id else f"HB-{import_id.replace('.', '_')}",
            '_source': 'DE/Bremen',
            '_type': doc_type,
            '_fetched_at': datetime.now().isoformat(),
            'import_id': import_id,
            'title': title,
            'subtitle': raw_doc.get('untertitel', ''),
            'text': raw_doc.get('full_text', ''),
            'date': date,
            'v_datum': raw_doc.get('v_datum', ''),
            'url': raw_doc.get('detail_url', raw_doc.get('url', '')),
            'subject_area': subject_area,
            'category': raw_doc.get('metadaten_kategorie_r', ''),
            'jurisdiction': 'Bremen',
            'country': 'DE',
            'language': 'de',
            'license': 'CC-BY-3.0',
            'attribution': 'Senator für Finanzen, Bremen'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BremenFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 12 if '--sample' in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target_count + 20):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            # Require substantial text content
            if text_len < 300:
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_').replace(' ', '_')
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
        fetcher = BremenFetcher()
        print("Testing Bremen fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Date: {normalized['v_datum']} -> {normalized['date']}")
            print(f"Subject: {normalized['subject_area']}")
            print(f"Type: {normalized['_type']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
