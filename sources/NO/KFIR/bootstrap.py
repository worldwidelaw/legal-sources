#!/usr/bin/env python3
"""
NO/KFIR - Norwegian Board of Appeal for Industrial Property Rights

Fetches KFIR (Klagenemnda for industrielle rettigheter) decisions.
~900 decisions on patent, trademark, design, and business name disputes.
Full text extracted from PDF attachments.

Data source: https://kfir.no/avgjørelser
Sitemap: https://kfir.no/sitemap-pages-nb.xml
"""

import argparse
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logger = logging.getLogger("legal-data-hunter")

BASE_URL = "https://kfir.no"
SITEMAP_URL = "https://kfir.no/sitemap-pages-nb.xml"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "NO/KFIR"

# URL pattern for decision pages
DECISION_PATTERN = re.compile(r'/avgj%C3%B8relser/(patent|varemerke|design|foretaksnavn|planteforedlerrett|ansvarsmerker|kfirs-saker-for-domstolen)/')

# Category mapping
CATEGORY_MAP = {
    'patent': 'Patent',
    'varemerke': 'Trademark',
    'design': 'Design',
    'foretaksnavn': 'Business Name',
    'planteforedlerrett': 'Plant Breeder Rights',
    'ansvarsmerker': 'Hallmarks',
    'kfirs-saker-for-domstolen': 'Court Cases',
}


class KFIRScraper(BaseScraper):
    """Scraper for NO/KFIR -- Norwegian Board of Appeal for Industrial Property Rights."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "nb-NO,nb;q=0.9,no;q=0.8,en;q=0.5",
        })

    def _load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {'fetched_urls': []}

    def _save_checkpoint(self, checkpoint: dict):
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _get_decision_urls(self) -> list:
        """Parse sitemap to get all decision page URLs."""
        print(f"Fetching sitemap: {SITEMAP_URL}")
        resp = self.session.get(SITEMAP_URL, timeout=30)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        urls = []
        for url_elem in root.findall('.//sm:url/sm:loc', ns):
            url = url_elem.text.strip()
            if DECISION_PATTERN.search(url):
                urls.append(url)

        print(f"Found {len(urls)} decision URLs in sitemap")
        return urls

    def _extract_category(self, url: str) -> str:
        """Extract category from decision URL."""
        match = DECISION_PATTERN.search(url)
        if match:
            cat_slug = match.group(1)
            return CATEGORY_MAP.get(cat_slug, cat_slug)
        return 'Unknown'

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse Norwegian date format (DD.MM.YYYY) to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        try:
            dt = datetime.strptime(date_str, '%d.%m.%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            return None

    def _fetch_decision(self, url: str) -> Optional[dict]:
        """Fetch a single decision page and extract metadata + PDF text."""
        try:
            time.sleep(1.0)
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Extract metadata from the page
            title = ''
            date_str = ''
            case_id = ''
            summary = ''

            # Title is typically the h1
            h1 = soup.find('h1')
            if h1:
                title = h1.get_text(strip=True)

            # Look for date and case ID in the page content
            # Typical structure: date, case number, then summary text
            main_content = soup.find('main') or soup.find('article') or soup.find('div', class_='content')
            if not main_content:
                main_content = soup.body

            if main_content:
                text_content = main_content.get_text(separator='\n', strip=True)

                # Extract date (DD.MM.YYYY pattern)
                date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', text_content)
                if date_match:
                    date_str = date_match.group(1)

                # Extract case ID (PAT YY/NNNNN, VM YY/NNNNN, etc.)
                case_match = re.search(r'((?:PAT|VM|DES|FN|PVR|AM)\s+\d{2}/\d{4,5}(?:\s*[A-Z])?)', text_content)
                if case_match:
                    case_id = case_match.group(1).strip()

                # Get the summary/description paragraph
                paragraphs = main_content.find_all('p')
                for p in paragraphs:
                    p_text = p.get_text(strip=True)
                    # Skip very short or navigational text
                    if len(p_text) > 30 and not p_text.startswith('Hjem'):
                        summary = p_text
                        break

            # Find PDF link
            pdf_url = None
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '/attachments/' in href and href.lower().endswith('.pdf'):
                    pdf_url = urljoin(BASE_URL, href)
                    break

            if not pdf_url:
                print(f"  No PDF found on {url}")
                return None

            # Generate a stable ID from the URL slug if no case_id found
            if not case_id:
                slug = url.rstrip('/').split('/')[-1]
                case_id = unquote(slug).upper()

            # Extract text from PDF
            print(f"  Extracting PDF: {pdf_url}")
            text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=case_id,
                pdf_url=pdf_url,
                table="case_law",
            )

            if not text or len(text) < 50:
                # Fallback: download PDF and try pdfplumber directly
                text = self._download_and_extract_pdf(pdf_url)

            if not text or len(text) < 50:
                print(f"  Could not extract text from PDF for {case_id}")
                return None

            category = self._extract_category(url)

            return {
                'case_id': case_id,
                'title': title or case_id,
                'date': date_str,
                'summary': summary,
                'category': category,
                'url': url,
                'pdf_url': pdf_url,
                'text': text,
            }

        except requests.RequestException as e:
            print(f"  Error fetching {url}: {e}")
            return None

    def _download_and_extract_pdf(self, pdf_url: str) -> Optional[str]:
        """Fallback: download PDF and extract text with pdfplumber."""
        try:
            import pdfplumber
            import io

            time.sleep(1.0)
            resp = self.session.get(pdf_url, timeout=60)
            resp.raise_for_status()

            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                pages = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages.append(text)
                return '\n\n'.join(pages) if pages else None
        except ImportError:
            print("  pdfplumber not available for fallback")
            return None
        except Exception as e:
            print(f"  PDF extraction error: {e}")
            return None

    def normalize(self, raw: dict) -> dict:
        case_id = raw.get('case_id', '')
        title = raw.get('title', case_id)
        date = self._parse_date(raw.get('date', ''))

        return {
            '_id': case_id,
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'text': raw.get('text', ''),
            'date': date,
            'url': raw.get('url', ''),
            'case_id': case_id,
            'category': raw.get('category', ''),
            'summary': raw.get('summary', ''),
            'pdf_url': raw.get('pdf_url', ''),
            'language': 'nob',
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all decisions via sitemap."""
        checkpoint = self._load_checkpoint()
        fetched_urls = set(checkpoint.get('fetched_urls', []))

        decision_urls = self._get_decision_urls()
        print(f"Total decision URLs: {len(decision_urls)}, already fetched: {len(fetched_urls)}")

        count = 0
        for i, url in enumerate(decision_urls):
            if url in fetched_urls:
                continue

            print(f"[{i+1}/{len(decision_urls)}] {url}")
            raw = self._fetch_decision(url)

            if raw and raw.get('text') and len(raw['text']) >= 50:
                yield raw
                count += 1
                fetched_urls.add(url)
                print(f"  -> {raw['case_id']}: {len(raw['text']):,} chars")

                if count % 20 == 0:
                    checkpoint['fetched_urls'] = list(fetched_urls)
                    self._save_checkpoint(checkpoint)
            else:
                # Still mark as fetched to avoid retrying
                fetched_urls.add(url)

        checkpoint['fetched_urls'] = list(fetched_urls)
        self._save_checkpoint(checkpoint)
        print(f"\nTotal records fetched: {count}")

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        urls = self._get_decision_urls()
        if not urls:
            print("ERROR: No decision URLs found in sitemap")
            return False
        print(f"Sitemap OK: {len(urls)} decision URLs")

        raw = self._fetch_decision(urls[0])
        if not raw or not raw.get('text') or len(raw['text']) < 50:
            print("ERROR: Could not extract full text from first decision")
            return False

        print(f"Decision OK: {raw['case_id']} ({len(raw['text']):,} chars)")
        return True


def main():
    parser = argparse.ArgumentParser(description="NO/KFIR decision fetcher")
    parser.add_argument('command', choices=['bootstrap', 'update', 'test'],
                       help="Command to run")
    parser.add_argument('--sample', action='store_true',
                       help="Fetch sample records only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = KFIRScraper()

    if args.command == 'test':
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == 'bootstrap':
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        print(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == 'update':
        stats = scraper.update()
        print(f"Update complete: {stats}")


if __name__ == '__main__':
    main()
