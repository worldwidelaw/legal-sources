#!/usr/bin/env python3
"""
XK/SupremeCourt -- Kosovo Supreme Court Case Law Fetcher

Fetches Kosovo Supreme Court judgments from EULEX Kosovo judicial portal.

Data access method:
  - HTML scraping of EULEX judgment index pages
  - PDF download and text extraction for full judgment text

The primary Kosovo court website (supreme.gjyqesori-rks.org) is blocked by Cloudflare,
so this fetcher uses EULEX Kosovo's published judgments instead.

Coverage:
  - Criminal proceedings: Supreme Court judgments
  - Civil proceedings: Supreme Court judgments
  - Languages: English, Albanian, Serbian

Usage:
  python bootstrap.py bootstrap           # Full historical pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import html
import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple
from urllib.parse import urljoin, quote, unquote

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# PDF extraction - try multiple backends
PDF_EXTRACTOR = None
try:
    PDF_EXTRACTOR = "pypdf2"
except ImportError:
    try:
        PDF_EXTRACTOR = "pdfminer"
    except ImportError:
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Configuration
BASE_URL = "https://www.eulex-kosovo.eu"
JUDGMENTS_BASE = f"{BASE_URL}/en/judgments/"

# Court pages to scrape
COURT_PAGES = [
    # Criminal proceedings
    ("criminal", "CM-Supreme-Court.php"),
    # Civil proceedings
    ("civil", "CV-Supreme-Court.php"),
]

REQUEST_DELAY = 2.0  # seconds between requests
REQUEST_TIMEOUT = 30

# HTTP headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def extract_pdf_text(pdf_content: bytes) -> Optional[str]:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="XK/SupremeCourt",
        source_id="",
        pdf_bytes=pdf_content,
        table="case_law",
    ) or ""

class KosovoSupremeCourtFetcher:
    """Fetcher for Kosovo Supreme Court judgments from EULEX."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._seen_cases: set = set()

    def _get(self, url: str) -> Optional[requests.Response]:
        """Make an HTTP GET request with rate limiting."""
        try:
            time.sleep(REQUEST_DELAY)
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file."""
        try:
            time.sleep(REQUEST_DELAY)
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            # Verify it's a PDF
            content_type = resp.headers.get('Content-Type', '')
            if 'pdf' not in content_type.lower() and not resp.content.startswith(b'%PDF'):
                logger.warning(f"Not a PDF: {url}")
                return None

            return resp.content
        except requests.RequestException as e:
            logger.warning(f"PDF download failed for {url}: {e}")
            return None

    def _parse_judgment_row(self, row, proceeding_type: str) -> Optional[Dict[str, Any]]:
        """Parse a judgment table row."""
        cells = row.find_all('td')
        if len(cells) < 3:
            return None

        # Extract date, case name/number, and judgment links
        date_text = cells[0].get_text(strip=True)
        case_text = cells[1].get_text(strip=True)

        # Find PDF links
        pdf_links = []
        for link in cells[2].find_all('a', href=True):
            href = link['href']
            if href.endswith('.pdf') or href.endswith('.PDF'):
                lang = link.get_text(strip=True).lower()
                if lang in ['english', 'en']:
                    lang = 'en'
                elif lang in ['shqip', 'sq', 'albanian']:
                    lang = 'sq'
                elif lang in ['srpski', 'sr', 'serbian']:
                    lang = 'sr'
                else:
                    lang = 'en'  # Default to English

                # Construct full URL
                full_url = urljoin(JUDGMENTS_BASE, href)
                pdf_links.append((lang, full_url))

        if not pdf_links:
            return None

        # Parse date
        iso_date = None
        for fmt in ["%d %B %Y", "%d %b %Y", "%d.%m.%Y", "%Y.%m.%d"]:
            try:
                dt = datetime.strptime(date_text.strip(), fmt)
                iso_date = dt.strftime("%Y-%m-%d")
                break
            except ValueError:
                continue

        return {
            "date_text": date_text,
            "date": iso_date,
            "case_number": case_text,
            "proceeding_type": proceeding_type,
            "pdf_links": pdf_links,
        }

    def discover_judgments(self, proceeding_type: str, page_file: str) -> List[Dict[str, Any]]:
        """Discover judgments from a court page."""
        url = JUDGMENTS_BASE + page_file
        resp = self._get(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
        judgments = []

        # Find judgment tables
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            for row in rows[1:]:  # Skip header row
                judgment = self._parse_judgment_row(row, proceeding_type)
                if judgment:
                    judgments.append(judgment)

        logger.info(f"Found {len(judgments)} judgments in {page_file}")
        return judgments

    def fetch_judgment_text(self, judgment: Dict[str, Any]) -> Optional[str]:
        """Fetch full text from PDF, preferring English."""
        pdf_links = judgment.get("pdf_links", [])

        # Sort by preference: English first, then Albanian, then Serbian
        lang_order = {'en': 0, 'sq': 1, 'sr': 2}
        sorted_links = sorted(pdf_links, key=lambda x: lang_order.get(x[0], 99))

        for lang, url in sorted_links:
            logger.info(f"Downloading PDF ({lang}): {url}")
            pdf_content = self._download_pdf(url)
            if pdf_content:
                text = extract_pdf_text(pdf_content)
                if text and len(text) > 200:
                    return text
                else:
                    logger.warning(f"PDF had insufficient text: {len(text) if text else 0} chars")

        return None

    def fetch_document(self, judgment: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch complete document with full text."""
        case_number = judgment.get("case_number", "")

        # Skip duplicates
        if case_number in self._seen_cases:
            return None
        self._seen_cases.add(case_number)

        # Get full text
        text = self.fetch_judgment_text(judgment)
        if not text:
            logger.warning(f"No full text for: {case_number}")
            return None

        # Create document ID
        safe_case_num = re.sub(r'[^a-zA-Z0-9-]', '_', case_number)
        doc_id = f"XK-SC-{safe_case_num}"

        # Get primary PDF URL
        pdf_url = judgment.get("pdf_links", [("", "")])[0][1] if judgment.get("pdf_links") else ""

        return {
            "_id": doc_id,
            "_source": "XK/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.utcnow().isoformat() + "Z",
            "case_number": case_number,
            "title": f"Supreme Court of Kosovo - {case_number}",
            "text": text,
            "date": judgment.get("date"),
            "date_text": judgment.get("date_text"),
            "court": "Supreme Court of Kosovo",
            "proceeding_type": judgment.get("proceeding_type"),
            "url": pdf_url,
            "language": "en",  # Primary language of extracted text
        }

    def fetch_all(self, sample_mode: bool = False) -> Iterator[Dict[str, Any]]:
        """Fetch all judgments from EULEX Kosovo."""
        all_judgments = []

        # Discover judgments from all court pages
        for proceeding_type, page_file in COURT_PAGES:
            judgments = self.discover_judgments(proceeding_type, page_file)
            all_judgments.extend(judgments)

        logger.info(f"Total judgments discovered: {len(all_judgments)}")

        # In sample mode, limit fetching
        if sample_mode:
            all_judgments = all_judgments[:25]  # Fetch enough to get 10+ successful

        # Fetch each judgment
        for judgment in all_judgments:
            doc = self.fetch_document(judgment)
            if doc:
                yield doc

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a date."""
        # EULEX site is static, so just fetch all and filter
        for doc in self.fetch_all():
            doc_date = doc.get("date")
            if doc_date and doc_date >= since:
                yield doc

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a document to standard schema."""
        return raw  # Already normalized in fetch_document


def test_api():
    """Quick connectivity test."""
    if not PDF_EXTRACTOR:
        logger.error("No PDF extraction library available. Install PyPDF2: pip install PyPDF2")
        return False

    fetcher = KosovoSupremeCourtFetcher()

    # Test page access
    logger.info("Testing EULEX judgment page access...")
    for proceeding_type, page_file in COURT_PAGES[:1]:
        judgments = fetcher.discover_judgments(proceeding_type, page_file)
        if judgments:
            logger.info(f"Found {len(judgments)} judgments in {page_file}")

            # Test PDF download and extraction
            test_judgment = judgments[0]
            logger.info(f"Testing PDF extraction for: {test_judgment['case_number']}")

            doc = fetcher.fetch_document(test_judgment)
            if doc and len(doc.get('text', '')) > 500:
                logger.info(f"Text extracted: {len(doc['text'])} chars")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("PDF text extraction failed")
                return False

    logger.error("No judgments found")
    return False


def bootstrap_sample(sample_dir: Path, count: int = 10):
    """Fetch sample documents for validation."""
    if not PDF_EXTRACTOR:
        logger.error("No PDF extraction library available. Install PyPDF2: pip install PyPDF2")
        return

    fetcher = KosovoSupremeCourtFetcher()

    sample_dir.mkdir(parents=True, exist_ok=True)
    all_samples = []
    fetched = 0

    for doc in fetcher.fetch_all(sample_mode=True):
        # Save individual sample
        filename = f"{doc['_id']}.json"
        filepath = sample_dir / filename
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(doc, f, indent=2, ensure_ascii=False)

        all_samples.append(doc)
        fetched += 1
        logger.info(f"[{fetched}] Saved: {doc['case_number']} ({len(doc['text'])} chars)")

        if fetched >= count:
            break

    # Save combined samples file
    all_samples_path = sample_dir / "all_samples.json"
    with open(all_samples_path, 'w', encoding='utf-8') as f:
        json.dump(all_samples, f, indent=2, ensure_ascii=False)

    logger.info(f"Bootstrap complete: {fetched} samples saved to {sample_dir}")

    # Print statistics
    if all_samples:
        avg_len = sum(len(s['text']) for s in all_samples) / len(all_samples)
        logger.info(f"Average text length: {avg_len:.0f} chars")
        proceeding_types = set(s.get('proceeding_type', 'unknown') for s in all_samples)
        logger.info(f"Proceeding types: {proceeding_types}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [test-api|bootstrap|bootstrap --sample|update]")
        sys.exit(1)

    command = sys.argv[1]
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        if sample_mode:
            bootstrap_sample(sample_dir, count=15)
        else:
            # Full bootstrap - stream all documents
            if not PDF_EXTRACTOR:
                logger.error("No PDF extraction library available. Install PyPDF2: pip install PyPDF2")
                sys.exit(1)
            fetcher = KosovoSupremeCourtFetcher()
            count = 0
            for doc in fetcher.fetch_all(sample_mode=False):
                print(json.dumps(doc, ensure_ascii=False))
                count += 1
            logger.info(f"Full bootstrap complete: {count} documents")

    elif command == "update":
        # For updates, fetch all and filter by date
        if not PDF_EXTRACTOR:
            logger.error("No PDF extraction library available. Install PyPDF2: pip install PyPDF2")
            sys.exit(1)
        logger.info("Update mode: fetching recent judgments...")
        fetcher = KosovoSupremeCourtFetcher()
        # Since EULEX data is historical (up to 2014), updates are unlikely
        for doc in fetcher.fetch_all():
            print(json.dumps(doc, ensure_ascii=False))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
