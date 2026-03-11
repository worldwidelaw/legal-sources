#!/usr/bin/env python3
"""
LV/ConstitutionalCourt -- Latvian Constitutional Court (Satversmes tiesa) Fetcher

Fetches Constitutional Court decisions from the official website via WordPress sitemap.

Strategy:
  - Parse WordPress sitemap for decision URLs
  - Extract full text from HTML decision pages
  - Normalize into standard schema

Endpoints:
  - Sitemap: https://www.satv.tiesa.gov.lv/wp-sitemap-posts-decision-1.xml
  - Decision pages: https://www.satv.tiesa.gov.lv/decisions/{slug}/

Data:
  - Constitutional Court procedural decisions (collegium decisions)
  - Full text in Latvian (HTML extracted)
  - Decisions on admissibility, procedural matters, and constitutional review
  - ~800+ decisions from 2015 to present

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LV.constitutionalcourt")

# Base URLs
BASE_URL = "https://www.satv.tiesa.gov.lv"
SITEMAP_URL = f"{BASE_URL}/wp-sitemap-posts-decision-1.xml"
DECISION_URL_PREFIX = f"{BASE_URL}/decisions/"

# Headers for requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5,lv;q=0.3",
}


class ConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for LV/ConstitutionalCourt -- Latvian Constitutional Court (Satversmes tiesa).
    Country: LV
    URL: https://www.satv.tiesa.gov.lv

    Data types: case_law
    Auth: none (public)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_sitemap(self) -> List[Dict]:
        """
        Fetch and parse the WordPress sitemap for decision URLs.

        Returns list of dicts with url and lastmod.
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(SITEMAP_URL, timeout=30)
            resp.raise_for_status()

            # Parse XML sitemap (strip leading whitespace that can cause XML errors)
            content = resp.content.strip()
            root = ET.fromstring(content)

            # Handle namespace
            ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            urls = []
            for url_elem in root.findall('.//sm:url', ns):
                loc = url_elem.find('sm:loc', ns)
                lastmod = url_elem.find('sm:lastmod', ns)

                if loc is not None and loc.text:
                    url_data = {
                        'url': loc.text.strip(),
                        'lastmod': lastmod.text.strip() if lastmod is not None and lastmod.text else None
                    }
                    # Only include decision URLs
                    if '/decisions/' in url_data['url']:
                        urls.append(url_data)

            logger.info(f"Found {len(urls)} decision URLs in sitemap")
            return urls

        except Exception as e:
            logger.error(f"Failed to fetch sitemap: {e}")
            return []

    def _extract_slug(self, url: str) -> str:
        """Extract the slug/ID from a decision URL."""
        # Remove trailing slash and base URL
        path = url.rstrip('/').replace(DECISION_URL_PREFIX, '')
        return path

    def _parse_date_from_slug(self, slug: str) -> Optional[str]:
        """
        Try to extract date from slug.

        Examples:
        - kolegijas-2026-gada-19-februara-lemums-pieteikums-nr-18-2026
        - 2015-gada-20-janvara-kolegijas-lemums-pieteikums-nr-22015
        """
        # Month name mapping
        months = {
            'janvara': '01', 'janvari': '01',
            'februara': '02', 'februari': '02',
            'marta': '03', 'marti': '03',
            'aprila': '04', 'aprili': '04',
            'maija': '05', 'maiji': '05',
            'junija': '06', 'juniji': '06',
            'julija': '07', 'juliji': '07',
            'augusta': '08', 'augusti': '08',
            'septembra': '09', 'septembri': '09',
            'oktobra': '10', 'oktobri': '10',
            'novembra': '11', 'novembri': '11',
            'decembra': '12', 'decembri': '12',
        }

        # Pattern: YYYY-gada-DD-monthname
        pattern = r'(\d{4})-gada-(\d{1,2})-(\w+)'
        match = re.search(pattern, slug)

        if match:
            year = match.group(1)
            day = match.group(2).zfill(2)
            month_name = match.group(3).lower()
            month = months.get(month_name)

            if month:
                return f"{year}-{month}-{day}"

        return None

    def _parse_petition_number(self, slug: str) -> Optional[str]:
        """
        Extract petition number from slug.

        Examples:
        - pieteikums-nr-18-2026 -> 18/2026
        - pieteikums-nr-223-2020 -> 223/2020
        """
        # Pattern: pieteikums-nr-NUMBER-YEAR or pieteikums-nr-NUMBER/YEAR
        pattern = r'pieteikums-nr-(\d+)-(\d{4})'
        match = re.search(pattern, slug)

        if match:
            number = match.group(1)
            year = match.group(2)
            return f"{number}/{year}"

        return None

    def _fetch_decision_page(self, url: str) -> Optional[Dict]:
        """
        Fetch a decision page and extract content.

        Returns dict with title, text, and metadata.
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Extract title from <title> tag
            title_tag = soup.find('title')
            title = ""
            if title_tag:
                title = title_tag.get_text()
                # Clean up: remove site name suffix
                title = re.sub(r'\s*[»|–|-]\s*Latvijas Republikas Satversmes.*$', '', title).strip()
                title = html.unescape(title)

            # Extract full text from <p> tags in main content
            # The decision text is in paragraph tags
            paragraphs = soup.find_all('p')
            text_parts = []

            for p in paragraphs:
                text = p.get_text(strip=True)
                # Filter out navigation/UI text
                if text and len(text) > 20:
                    # Skip common UI patterns
                    if any(skip in text.lower() for skip in [
                        'sīkdatnes', 'cookie', 'tīmekļa', 'analītisk',
                        'facebook', 'twitter', 'linkedin', 'draugiem'
                    ]):
                        continue
                    text_parts.append(text)

            full_text = '\n\n'.join(text_parts)

            # Extract metadata keywords/tags
            keywords = []
            keyword_section = soup.find(string=re.compile('Atslēgvārdi:'))
            if keyword_section:
                parent = keyword_section.parent
                if parent:
                    kw_text = parent.get_text()
                    kw_match = re.search(r'Atslēgvārdi:\s*(.+?)(?:\.|$)', kw_text, re.DOTALL)
                    if kw_match:
                        kw_str = kw_match.group(1)
                        keywords = [k.strip() for k in kw_str.split(',') if k.strip()]

            return {
                'url': url,
                'title': title,
                'full_text': full_text,
                'keywords': keywords,
            }

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch decision {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing decision {url}: {e}")
            return None

    def fetch_all(self) -> Generator[Dict, None, None]:
        """
        Yield all Constitutional Court decisions.

        Fetches URLs from WordPress sitemap and extracts full text from each page.
        """
        doc_count = 0

        # Fetch sitemap
        sitemap_entries = self._fetch_sitemap()
        if not sitemap_entries:
            logger.error("No decisions found in sitemap")
            return

        logger.info(f"Processing {len(sitemap_entries)} decision URLs")

        for entry in sitemap_entries:
            url = entry['url']
            lastmod = entry.get('lastmod')

            result = self._fetch_decision_page(url)
            if result and result.get('full_text'):
                # Add lastmod from sitemap
                result['lastmod'] = lastmod

                doc_count += 1
                yield result

                if doc_count % 50 == 0:
                    logger.info(f"Fetched {doc_count} decisions with full text")

        logger.info(f"Total decisions fetched: {doc_count}")

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        """
        Yield decisions modified since the given date.

        Uses lastmod from sitemap to filter.
        """
        sitemap_entries = self._fetch_sitemap()
        if not sitemap_entries:
            return

        since_str = since.isoformat()
        logger.info(f"Fetching updates since {since_str}")

        for entry in sitemap_entries:
            url = entry['url']
            lastmod = entry.get('lastmod')

            # Filter by lastmod
            if lastmod:
                if lastmod >= since_str:
                    result = self._fetch_decision_page(url)
                    if result and result.get('full_text'):
                        result['lastmod'] = lastmod
                        yield result

    def normalize(self, raw: Dict) -> Dict:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        url = raw.get('url', '')
        slug = self._extract_slug(url)
        title = raw.get('title', '')
        full_text = raw.get('full_text', '')

        # Extract date from slug
        date = self._parse_date_from_slug(slug)

        # Extract petition number
        petition_number = self._parse_petition_number(slug)

        # Create unique ID
        doc_id = slug if slug else url.split('/')[-2]

        # Determine decision type from title or text
        decision_type = "Lēmums"  # Default: Decision
        if 'spriedums' in title.lower() or 'spriedums' in full_text[:200].lower():
            decision_type = "Spriedums"  # Judgment
        elif 'kolegijas' in slug.lower() or 'kolēģijas' in title.lower():
            decision_type = "Kolēģijas lēmums"  # Collegium decision
        elif 'ricibas' in slug.lower():
            decision_type = "Rīcības sēdes lēmums"  # Procedural session decision

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "LV/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date or "",
            "url": url,
            # Additional metadata
            "petition_number": petition_number or "",
            "decision_type": decision_type,
            "court": "Satversmes tiesa",
            "keywords": raw.get('keywords', []),
            "lastmod": raw.get('lastmod', ''),
            "language": "lv",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LV/ConstitutionalCourt endpoints...")

        # Test sitemap
        print("\n1. Testing sitemap fetch...")
        try:
            entries = self._fetch_sitemap()
            print(f"   Found {len(entries)} decision URLs")
            if entries:
                print(f"   Sample URL: {entries[0]['url'][:80]}...")
                print(f"   Lastmod: {entries[0].get('lastmod', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test decision page fetch
        print("\n2. Testing decision page fetch...")
        try:
            if entries:
                url = entries[-1]['url']  # Get a recent one
                result = self._fetch_decision_page(url)
                if result:
                    print(f"   Title: {result['title'][:60]}...")
                    print(f"   Text length: {len(result['full_text'])} characters")
                    print(f"   Keywords: {result.get('keywords', [])}")
                    if result['full_text']:
                        print(f"   Preview: {result['full_text'][:200]}...")
                else:
                    print("   ERROR: Could not fetch decision page")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test normalization
        print("\n3. Testing normalization...")
        try:
            if entries:
                url = entries[-1]['url']
                raw = self._fetch_decision_page(url)
                if raw:
                    normalized = self.normalize(raw)
                    print(f"   ID: {normalized['_id']}")
                    print(f"   Date: {normalized.get('date', 'N/A')}")
                    print(f"   Type: {normalized.get('decision_type', 'N/A')}")
                    print(f"   Petition: {normalized.get('petition_number', 'N/A')}")
                    print(f"   Text length: {len(normalized.get('text', ''))}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = ConstitutionalCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
