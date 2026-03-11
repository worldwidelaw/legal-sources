#!/usr/bin/env python3
"""
Czech Supreme Court (Nejvyšší soud) Data Fetcher

Access to the Collection of Judicial Decisions (Sbírka soudních rozhodnutí a stanovisek)
https://sbirka.nsoud.cz

This fetcher uses:
1. WordPress sitemaps for document discovery (collection-sitemap*.xml)
2. Individual decision pages for full text extraction
3. RSS feed for recent updates

Decision URL pattern: https://sbirka.nsoud.cz/sbirka/{id}/

ECLI format for Czech Supreme Court:
ECLI:CZ:NS:{year}:{senate}.{case_type}.{number}.{year}.{ordinal}
Example: ECLI:CZ:NS:2025:3.TZ.17.2025.1
"""

import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urlparse

import requests

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://sbirka.nsoud.cz"
SITEMAP_INDEX = f"{BASE_URL}/sitemap_index.xml"
RSS_FEED = f"{BASE_URL}/feed/"


class DecisionHTMLParser(HTMLParser):
    """Parser to extract structured content from Supreme Court decision HTML"""

    def __init__(self):
        super().__init__()
        self.in_title = False
        self.in_content_table = False
        self.in_td = False
        self.in_header_cell = False
        self.current_label = None
        self.title = ""
        self.metadata = {}
        self.full_text_parts: List[str] = []
        self.in_full_text = False
        self.keywords: List[str] = []
        self.in_tag_button = False
        self.depth = 0
        self.text_buffer = ""

    def handle_starttag(self, tag, attrs):
        self.depth += 1
        attr_dict = dict(attrs)

        if tag == 'h1':
            self.in_title = True

        if tag == 'div' and 'detail-section__content' in attr_dict.get('class', ''):
            self.in_content_table = True

        if tag == 'div' and 'detail-section__table-heading' in attr_dict.get('class', ''):
            self.in_full_text = True

        if tag == 'td':
            self.in_td = True
            self.text_buffer = ""

        if tag == 'span' and 'btn' in attr_dict.get('class', '') and self.in_content_table:
            self.in_tag_button = True

    def handle_endtag(self, tag):
        self.depth -= 1

        if tag == 'h1':
            self.in_title = False

        if tag == 'td':
            self.in_td = False
            content = self.text_buffer.strip()
            if content:
                # Check if this is a label or value
                if self.current_label is None:
                    # This might be a label
                    if ':' in content or content in ['Soud', 'Datum rozhodnutí', 'Spisová značka',
                                                       'Sbírkové číslo', 'Ročník', 'Sešit',
                                                       'Rozhodnutí', 'Hesla', 'Dotčené předpisy',
                                                       'Druh věci', 'ECLI', 'Právní věta']:
                        self.current_label = content.replace(':', '').strip()
                    elif self.in_full_text:
                        self.full_text_parts.append(content)
                else:
                    # This is the value
                    self.metadata[self.current_label] = content
                    self.current_label = None

        if tag == 'span' and self.in_tag_button:
            self.in_tag_button = False

        if tag == 'table' and self.in_content_table:
            self.in_content_table = False

    def handle_data(self, data):
        stripped = data.strip()
        if not stripped:
            return

        if self.in_title:
            self.title += stripped + " "

        if self.in_td:
            self.text_buffer += " " + stripped

        if self.in_tag_button and stripped:
            self.keywords.append(stripped)

        # Capture full text after the heading
        if self.in_full_text and not self.in_content_table and not self.in_td:
            if stripped and len(stripped) > 2:
                self.full_text_parts.append(stripped)

    def get_full_text(self) -> str:
        """Get the concatenated full text"""
        return '\n'.join(self.full_text_parts)


class SupremeCourtFetcher:
    """Fetcher for Czech Supreme Court decisions from sbirka.nsoud.cz"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'cs,en;q=0.5',
        })
        self._request_count = 0
        self._last_request = 0

    def _rate_limit(self):
        """Ensure we don't make more than 1 request per second"""
        now = time.time()
        elapsed = now - self._last_request
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        self._last_request = time.time()
        self._request_count += 1

    def _fetch_sitemap(self, url: str) -> List[str]:
        """Fetch and parse a sitemap, returning list of URLs"""
        self._rate_limit()
        try:
            response = self.session.get(url, timeout=30)
            if response.status_code != 200:
                logger.warning(f"Sitemap {url} returned status {response.status_code}")
                return []

            # Parse XML
            root = ET.fromstring(response.content)
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            urls = []
            # Check if this is a sitemap index
            for sitemap in root.findall('.//sm:sitemap/sm:loc', ns):
                urls.append(sitemap.text)

            # Or a regular sitemap with URLs
            for url_elem in root.findall('.//sm:url/sm:loc', ns):
                urls.append(url_elem.text)

            return urls
        except Exception as e:
            logger.error(f"Error fetching sitemap {url}: {e}")
            return []

    def _get_collection_sitemaps(self) -> List[str]:
        """Get URLs of collection sitemaps from sitemap index"""
        urls = self._fetch_sitemap(SITEMAP_INDEX)
        return [u for u in urls if 'collection-sitemap' in u]

    def _get_decision_urls(self, limit: int = None) -> Iterator[str]:
        """Get all decision URLs from sitemaps"""
        collection_sitemaps = self._get_collection_sitemaps()
        logger.info(f"Found {len(collection_sitemaps)} collection sitemaps")

        count = 0
        for sitemap_url in collection_sitemaps:
            logger.info(f"Processing sitemap: {sitemap_url}")
            decision_urls = self._fetch_sitemap(sitemap_url)

            for url in decision_urls:
                if '/sbirka/' in url and url != BASE_URL + '/sbirka/':
                    count += 1
                    if limit and count > limit:
                        return
                    yield url

    def _extract_id_from_url(self, url: str) -> str:
        """Extract decision ID from URL"""
        # URL format: https://sbirka.nsoud.cz/sbirka/25170/
        match = re.search(r'/sbirka/(\d+)/?', url)
        if match:
            return match.group(1)
        return ""

    def _parse_decision_html(self, html: str) -> Dict[str, Any]:
        """Parse decision HTML and extract structured data"""
        result = {
            'title': '',
            'metadata': {},
            'full_text': '',
            'keywords': [],
        }

        # Extract title from <title> tag
        title_match = re.search(r'<title>([^<]+)</title>', html)
        if title_match:
            result['title'] = title_match.group(1).replace(' - Nejvyšší soud', '').strip()

        # Extract ECLI from title or content
        ecli_match = re.search(r'ECLI:CZ:NS:\d+:[^<\s,]+', html)
        if ecli_match:
            result['metadata']['ecli'] = ecli_match.group(0)

        # Extract case reference (spisová značka)
        sz_match = re.search(r'sp\.\s*zn\.\s*([^,<]+)', html)
        if sz_match:
            result['metadata']['case_reference'] = sz_match.group(1).strip()

        # Extract date from title or content
        # Format: "ze dne DD. MM. YYYY" or "DD.MM.YYYY"
        date_match = re.search(r'ze dne (\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})', html)
        if date_match:
            day, month, year = date_match.groups()
            result['metadata']['decision_date'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        else:
            date_match2 = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', html)
            if date_match2:
                day, month, year = date_match2.groups()
                result['metadata']['decision_date'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Extract decision type from title
        if 'Rozsudek' in result['title']:
            result['metadata']['decision_type'] = 'Rozsudek'
        elif 'Usnesení' in result['title']:
            result['metadata']['decision_type'] = 'Usnesení'
        elif 'Stanovisko' in result['title']:
            result['metadata']['decision_type'] = 'Stanovisko'

        # Extract legal principle (právní věta)
        pv_match = re.search(r'Právní věta:.*?</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
        if pv_match:
            # Clean HTML tags
            legal_principle = re.sub(r'<[^>]+>', ' ', pv_match.group(1))
            legal_principle = re.sub(r'\s+', ' ', legal_principle).strip()
            result['metadata']['legal_principle'] = legal_principle

        # Extract court name
        court_match = re.search(r'Soud:.*?</td>\s*<td[^>]*>(.*?)</td>', html, re.DOTALL)
        if court_match:
            court = re.sub(r'<[^>]+>', ' ', court_match.group(1))
            result['metadata']['court'] = re.sub(r'\s+', ' ', court).strip()

        # Extract keywords (hesla)
        keywords = []
        for match in re.finditer(r'<span class="btn btn-primary -medium">\s*([^<]+)\s*</span>', html):
            kw = match.group(1).strip()
            if kw and kw not in keywords:
                keywords.append(kw)
        result['keywords'] = keywords

        # Extract full text from "Sbírkový text rozhodnutí" section
        text_section_match = re.search(
            r'Sbírkový text rozhodnutí\s*</span>.*?</div>(.*?)</td>',
            html,
            re.DOTALL
        )
        if text_section_match:
            full_text_html = text_section_match.group(1)
            # Clean HTML tags
            full_text = re.sub(r'<[^>]+>', '\n', full_text_html)
            # Decode HTML entities
            full_text = full_text.replace('&nbsp;', ' ')
            full_text = full_text.replace('&lt;', '<')
            full_text = full_text.replace('&gt;', '>')
            full_text = full_text.replace('&amp;', '&')
            full_text = full_text.replace('&quot;', '"')
            # Clean up whitespace
            lines = [line.strip() for line in full_text.split('\n') if line.strip()]
            result['full_text'] = '\n'.join(lines)

        return result

    def _fetch_decision(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a single decision by URL"""
        self._rate_limit()

        try:
            response = self.session.get(url, timeout=30)

            if response.status_code != 200:
                logger.warning(f"Decision {url} returned status {response.status_code}")
                return None

            html = response.text

            # Check if we got a real decision page
            if 'Sbírkový text rozhodnutí' not in html:
                logger.debug(f"No decision text found at {url}")
                return None

            # Parse the HTML
            parsed = self._parse_decision_html(html)

            # Extract ID from URL
            decision_id = self._extract_id_from_url(url)

            return {
                'id': decision_id,
                'url': url,
                'title': parsed['title'],
                'text': parsed['full_text'],
                'metadata': parsed['metadata'],
                'keywords': parsed['keywords'],
            }

        except requests.RequestException as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all decisions from sitemaps.

        Args:
            limit: Maximum number of documents to fetch

        Yields:
            Raw document dictionaries with full text
        """
        count = 0

        for url in self._get_decision_urls(limit=limit * 2 if limit else None):
            if limit and count >= limit:
                return

            result = self._fetch_decision(url)

            if result and result.get('text'):
                text_len = len(result['text'])
                if text_len > 500:  # Only count meaningful content
                    count += 1
                    logger.info(f"Fetched [{count}]: {result.get('title', '')[:60]}... ({text_len:,} chars)")
                    yield result

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions published since a given date using RSS feed."""
        self._rate_limit()

        try:
            response = self.session.get(RSS_FEED, timeout=30)
            if response.status_code != 200:
                logger.error(f"RSS feed returned status {response.status_code}")
                return

            # Parse RSS
            root = ET.fromstring(response.content)

            for item in root.findall('.//item'):
                # Check date
                pub_date = item.find('pubDate')
                if pub_date is not None:
                    # Parse RSS date format: "Thu, 05 Feb 2026 09:37:02 +0000"
                    try:
                        date_str = pub_date.text
                        item_date = datetime.strptime(date_str, '%a, %d %b %Y %H:%M:%S %z')
                        if item_date.replace(tzinfo=None) < since:
                            continue
                    except ValueError:
                        pass

                # Get link
                link = item.find('link')
                if link is not None:
                    # The RSS contains announcement pages, not individual decisions
                    # Extract decision URLs from the content
                    content = item.find('{http://purl.org/rss/1.0/modules/content/}encoded')
                    if content is not None:
                        # Find all decision URLs in the content
                        for match in re.finditer(r'https://sbirka\.nsoud\.cz/sbirka/\d+/?', content.text):
                            decision_url = match.group(0)
                            result = self._fetch_decision(decision_url)
                            if result and result.get('text'):
                                yield result

        except Exception as e:
            logger.error(f"Error fetching RSS: {e}")

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        metadata = raw_doc.get('metadata', {})

        # Get or build identifiers
        ecli = metadata.get('ecli', '')
        case_ref = metadata.get('case_reference', '')

        # Build ID from ECLI or case reference
        doc_id = ecli if ecli else f"CZ-NS-{raw_doc.get('id', '')}"

        # Get date
        decision_date = metadata.get('decision_date', '')
        if not decision_date:
            # Try to extract year from ECLI
            year_match = re.search(r':(\d{4}):', ecli) if ecli else None
            if year_match:
                decision_date = f"{year_match.group(1)}-01-01"

        # Build title
        title = raw_doc.get('title', '')
        if not title and case_ref:
            title = f"Rozhodnutí {case_ref}"

        return {
            '_id': doc_id,
            '_source': 'CZ/SupremeCourt',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'case_reference': case_ref,
            'ecli': ecli,
            'text': raw_doc.get('text', ''),
            'legal_principle': metadata.get('legal_principle', ''),
            'decision_type': metadata.get('decision_type', ''),
            'court': metadata.get('court', 'Nejvyšší soud'),
            'date': decision_date,
            'keywords': raw_doc.get('keywords', []),
            'url': raw_doc.get('url', ''),
            'language': 'cs',
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = SupremeCourtFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")
        logger.info("Fetching decisions from sbirka.nsoud.cz...")

        sample_count = 0
        target_count = 12 if '--sample' in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target_count * 2):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 500:  # Skip very short decisions
                continue

            # Save to sample directory
            doc_id = raw_doc.get('id', str(sample_count))
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized.get('case_reference', doc_id)} ({text_len:,} chars)")
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
        # Test mode - fetch a few decisions
        fetcher = SupremeCourtFetcher()
        print("Testing Supreme Court fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Case Ref: {normalized['case_reference']}")
            print(f"ECLI: {normalized['ecli']}")
            print(f"Date: {normalized['date']}")
            print(f"Type: {normalized.get('decision_type', 'unknown')}")
            print(f"Keywords: {', '.join(normalized.get('keywords', []))[:100]}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
