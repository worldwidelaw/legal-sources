#!/usr/bin/env python3
"""
CY/DATAPROTECTION -- Cyprus Data Protection Authority Fetcher

Fetches decisions from the Commissioner for Personal Data Protection of Cyprus.

Strategy:
  - Scrape the decisions index page to get decision period pages
  - For each period page, extract links to PDF decision files
  - Download PDFs and extract full text using pdfplumber

Endpoints:
  - Main decisions page: https://www.dataprotection.gov.cy/dataprotection/dataprotection.nsf/dp06/dp06?opendocument
  - Decision periods link to pages with PDF attachments

Data Coverage:
  - GDPR enforcement decisions (fines, warnings, etc.)
  - Decisions on data subject complaints
  - Language: Greek (ELL)
  - EU Member State

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser
from urllib.parse import urljoin, unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

# PDF extraction
try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    print("WARNING: pdfplumber not available. Install with: pip install pdfplumber")

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CY.DATAPROTECTION")

# Configuration
BASE_URL = "https://www.dataprotection.gov.cy"
DECISIONS_INDEX_URL = f"{BASE_URL}/dataprotection/dataprotection.nsf/dp06/dp06?opendocument"


class CyprusDataProtectionScraper(BaseScraper):
    """
    Scraper for CY/DATAPROTECTION -- Cyprus Data Protection Authority.
    Country: CY
    URL: https://www.dataprotection.gov.cy

    Data types: regulatory_decisions
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "el,en;q=0.9",
        })
        # Disable SSL verification due to certificate issues
        self.session.verify = False
        # Suppress InsecureRequestWarning
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _get_decision_period_links(self) -> List[Dict[str, Any]]:
        """
        Get links to decision period pages from the main decisions index.

        Returns list of dicts with 'title', 'url', 'date' keys.
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(DECISIONS_INDEX_URL, timeout=60)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')
            period_links = []

            # Find links containing "Αποφάσεις" (Decisions) or similar patterns
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')
                text = link.get_text(strip=True)

                # Look for links to decision period pages
                if '/All/' in href and 'OpenDocument' in href:
                    # Extract date from nearby span if available
                    parent = link.find_parent('td') or link.find_parent('li')
                    date_span = parent.find('span', class_='date') if parent else None
                    date_str = date_span.get_text(strip=True) if date_span else ""

                    # Skip if it's not a decisions page
                    if 'Αποφάσεις' not in text and 'Decisions' not in text:
                        continue

                    full_url = urljoin(BASE_URL, href)
                    period_links.append({
                        'title': text,
                        'url': full_url,
                        'date': date_str,
                    })

            logger.info(f"Found {len(period_links)} decision period pages")
            return period_links

        except Exception as e:
            logger.error(f"Failed to get decision period links: {e}")
            return []

    def _get_pdf_links_from_period_page(self, period_url: str) -> List[Dict[str, Any]]:
        """
        Get PDF links from a decision period page.

        Returns list of dicts with 'filename', 'url', 'title' keys.
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(period_url, timeout=60)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')
            pdf_links = []

            # Find all PDF links
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')

                # Look for PDF file links
                if '.pdf' in href.lower():
                    # Get the filename
                    filename = href.split('/')[-1]
                    if '?' in filename:
                        filename = filename.split('?')[0]

                    # Decode URL-encoded filename
                    filename = unquote(filename)

                    # Build full URL
                    full_url = urljoin(BASE_URL + '/dataprotection/dataprotection.nsf/', href.lstrip('../'))

                    # Extract title from context - try to find descriptive text nearby
                    parent = link.find_parent('td') or link.find_parent('p') or link.find_parent('font')
                    context_text = ""
                    if parent:
                        # Look for preceding text about the decision
                        prev_text = parent.get_text(strip=True)
                        if prev_text:
                            context_text = prev_text

                    # Try to extract date from filename (format: YYYYMMDD)
                    date_match = re.search(r'(\d{8})', filename)
                    date_str = ""
                    if date_match:
                        date_raw = date_match.group(1)
                        try:
                            date_str = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
                        except:
                            pass

                    pdf_links.append({
                        'filename': filename,
                        'url': full_url,
                        'title': filename.replace('.pdf', '').replace('ΑΠΟΦΑΣΗ', 'Decision').replace('Απόφαση', 'Decision'),
                        'context': context_text,
                        'date': date_str,
                    })

            # Remove duplicates based on URL
            seen_urls = set()
            unique_links = []
            for link in pdf_links:
                if link['url'] not in seen_urls:
                    seen_urls.add(link['url'])
                    unique_links.append(link)

            logger.info(f"Found {len(unique_links)} PDF links in {period_url}")
            return unique_links

        except Exception as e:
            logger.error(f"Failed to get PDF links from {period_url}: {e}")
            return []

    def _download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """Download PDF from URL."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(pdf_url, timeout=120)

            if resp.status_code != 200:
                logger.warning(f"Failed to download PDF: {pdf_url} (HTTP {resp.status_code})")
                return None

            # Verify it's a PDF
            content_type = resp.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and not resp.content.startswith(b'%PDF'):
                logger.warning(f"Not a PDF: {pdf_url}")
                return None

            return resp.content

        except Exception as e:
            logger.warning(f"Error downloading PDF {pdf_url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        if not PDF_SUPPORT:
            return ""

        try:
            pdf_file = io.BytesIO(pdf_bytes)
            full_text = ""

            with pdfplumber.open(pdf_file) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        full_text += text + "\n"

            # Clean up text
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            full_text = full_text.strip()

            return full_text

        except Exception as e:
            logger.warning(f"Failed to extract PDF text: {e}")
            return ""

    def _extract_title_from_text(self, text: str, filename: str) -> str:
        """Extract or generate a meaningful title from the decision text."""
        if not text:
            return filename.replace('.pdf', '')

        # Try to find decision reference number
        patterns = [
            # Pattern: Ref.: XX.XX.XXX.XXX.XXX or similar
            r'(?:Ref\.?|Our ref\.?|Αρ\.? Φακ\.?)[:\s]*(\d+\.\d+[\d.]+)',
            # Pattern: Decision regarding XXX
            r'(?:Decision|Απόφαση)\s+(?:regarding|σχετικά με)\s+([^\n]+)',
            # Pattern: Just the first substantial line
            r'^([A-Z][^\n]{20,100})',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                title = match.group(1).strip()
                return title[:200]  # Limit length

        # Fallback: use cleaned filename
        title = filename.replace('.pdf', '').replace('_', ' ').strip()
        return title

    def _extract_date_from_text(self, text: str) -> str:
        """Extract date from decision text."""
        if not text:
            return ""

        # Greek month names
        greek_months = {
            'Ιανουαρίου': '01', 'Φεβρουαρίου': '02', 'Μαρτίου': '03',
            'Απριλίου': '04', 'Μαΐου': '05', 'Ιουνίου': '06',
            'Ιουλίου': '07', 'Αυγούστου': '08', 'Σεπτεμβρίου': '09',
            'Οκτωβρίου': '10', 'Νοεμβρίου': '11', 'Δεκεμβρίου': '12',
        }

        # English month names
        english_months = {
            'January': '01', 'February': '02', 'March': '03',
            'April': '04', 'May': '05', 'June': '06',
            'July': '07', 'August': '08', 'September': '09',
            'October': '10', 'November': '11', 'December': '12',
        }

        # Try Greek date format: "7 Φεβρουαρίου 2025"
        for month_name, month_num in greek_months.items():
            pattern = rf'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})'
            match = re.search(pattern, text)
            if match:
                day = match.group(1).zfill(2)
                year = match.group(2)
                return f"{year}-{month_num}-{day}"

        # Try English date format: "7 February 2025"
        for month_name, month_num in english_months.items():
            pattern = rf'(\d{{1,2}})\s+{month_name}\s+(\d{{4}})'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                day = match.group(1).zfill(2)
                year = match.group(2)
                return f"{year}-{month_num}-{day}"

        # Try date format with numeric date: "DD/MM/YYYY" or "DD.MM.YYYY"
        pattern = r'(\d{1,2})[/.](\d{1,2})[/.](\d{4})'
        match = re.search(pattern, text)
        if match:
            day = match.group(1).zfill(2)
            month = match.group(2).zfill(2)
            year = match.group(3)
            return f"{year}-{month}-{day}"

        return ""

    def _generate_doc_id(self, filename: str, pdf_url: str) -> str:
        """Generate a unique document ID."""
        # Try to extract date and identifier from filename
        # Format: YYYYMMDD ΑΠΟΦΑΣΗ SUBJECT.pdf
        date_match = re.search(r'(\d{8})', filename)

        # Clean up filename for ID
        clean_name = re.sub(r'[^\w\-]', '_', filename.replace('.pdf', ''))
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')

        if len(clean_name) > 100:
            clean_name = clean_name[:100]

        return f"CY_DPA_{clean_name}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all decision documents from the Cyprus Data Protection Authority.

        Iterates through decision period pages and downloads all PDF decisions.
        """
        # Get all decision period pages
        period_links = self._get_decision_period_links()

        if not period_links:
            logger.warning("No decision period pages found")
            return

        for period in period_links:
            logger.info(f"Processing period: {period['title']}")

            # Get PDF links from this period page
            pdf_links = self._get_pdf_links_from_period_page(period['url'])

            for pdf_info in pdf_links:
                logger.info(f"Fetching: {pdf_info['filename']}")

                # Download PDF
                pdf_bytes = self._download_pdf(pdf_info['url'])
                if not pdf_bytes:
                    continue

                # Extract text
                full_text = self._extract_pdf_text(pdf_bytes)
                if not full_text or len(full_text) < 100:
                    logger.warning(f"Insufficient text from {pdf_info['filename']}")
                    continue

                # Extract title
                title = self._extract_title_from_text(full_text, pdf_info['filename'])

                yield {
                    'filename': pdf_info['filename'],
                    'url': pdf_info['url'],
                    'period_title': period['title'],
                    'period_url': period['url'],
                    'title': title,
                    'date': pdf_info.get('date', ''),
                    'context': pdf_info.get('context', ''),
                    'full_text': full_text,
                    'pdf_size': len(pdf_bytes),
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents created since the given date.

        For the Cyprus DPA, we just re-fetch recent period pages since
        the API doesn't support date-based queries.
        """
        since_year = since.year

        period_links = self._get_decision_period_links()

        for period in period_links:
            # Check if period might contain recent decisions
            # Look for year in the title
            year_match = re.search(r'20\d{2}', period['title'])
            if year_match:
                period_year = int(year_match.group())
                if period_year < since_year:
                    continue

            logger.info(f"Processing period for updates: {period['title']}")

            pdf_links = self._get_pdf_links_from_period_page(period['url'])

            for pdf_info in pdf_links:
                # Check date if available
                if pdf_info.get('date'):
                    try:
                        doc_date = datetime.strptime(pdf_info['date'], '%Y-%m-%d')
                        if doc_date.replace(tzinfo=timezone.utc) < since:
                            continue
                    except:
                        pass

                logger.info(f"Fetching: {pdf_info['filename']}")

                pdf_bytes = self._download_pdf(pdf_info['url'])
                if not pdf_bytes:
                    continue

                full_text = self._extract_pdf_text(pdf_bytes)
                if not full_text or len(full_text) < 100:
                    continue

                title = self._extract_title_from_text(full_text, pdf_info['filename'])

                yield {
                    'filename': pdf_info['filename'],
                    'url': pdf_info['url'],
                    'period_title': period['title'],
                    'period_url': period['url'],
                    'title': title,
                    'date': pdf_info.get('date', ''),
                    'context': pdf_info.get('context', ''),
                    'full_text': full_text,
                    'pdf_size': len(pdf_bytes),
                }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        filename = raw.get('filename', '')
        full_text = raw.get('full_text', '')

        # Generate document ID
        doc_id = self._generate_doc_id(filename, raw.get('url', ''))

        # Extract or use provided title
        title = raw.get('title', filename.replace('.pdf', ''))

        # Get date - try multiple sources
        date_str = raw.get('date', '')
        if not date_str:
            # Try to extract from filename (format: YYYYMMDD)
            date_match = re.search(r'(\d{8})', filename)
            if date_match:
                date_raw = date_match.group(1)
                try:
                    date_str = f"{date_raw[:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
                except:
                    pass
        if not date_str:
            # Try to extract from text content
            date_str = self._extract_date_from_text(full_text)

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "CY/DATAPROTECTION",
            "_type": "doctrine",  # regulatory_decision is closest to "other"
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": raw.get('url', ''),
            # Additional metadata
            "filename": filename,
            "period_title": raw.get('period_title', ''),
            "period_url": raw.get('period_url', ''),
            "context": raw.get('context', ''),
            "pdf_size": raw.get('pdf_size', 0),
            "language": "ell",  # Greek
            "issuing_authority": "Commissioner for Personal Data Protection",
            "document_type": "decision",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Cyprus Data Protection Authority endpoints...")

        # Test main decisions index
        print("\n1. Testing decisions index page...")
        try:
            self.rate_limiter.wait()
            resp = self.session.get(DECISIONS_INDEX_URL, timeout=30)
            print(f"   Status: {resp.status_code}")
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, 'html.parser')
                links = [a for a in soup.find_all('a', href=True) if 'Αποφάσεις' in a.get_text()]
                print(f"   Found {len(links)} decision period links")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test getting decision period pages
        print("\n2. Testing decision period pages...")
        try:
            period_links = self._get_decision_period_links()
            print(f"   Found {len(period_links)} decision periods")
            if period_links:
                print(f"   First: {period_links[0]['title']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test PDF extraction from a period page
        print("\n3. Testing PDF discovery...")
        try:
            if period_links:
                pdf_links = self._get_pdf_links_from_period_page(period_links[0]['url'])
                print(f"   Found {len(pdf_links)} PDFs in first period")
                if pdf_links:
                    print(f"   First PDF: {pdf_links[0]['filename']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test PDF download and text extraction
        print("\n4. Testing PDF download and text extraction...")
        try:
            if period_links and pdf_links:
                pdf_url = pdf_links[0]['url']
                print(f"   Downloading: {pdf_url}")
                pdf_bytes = self._download_pdf(pdf_url)
                if pdf_bytes:
                    print(f"   Downloaded: {len(pdf_bytes)} bytes")
                    text = self._extract_pdf_text(pdf_bytes)
                    print(f"   Extracted: {len(text)} characters")
                    if text:
                        print(f"   First 200 chars: {text[:200]}...")
                else:
                    print("   Failed to download PDF")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = CyprusDataProtectionScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15  # Default to 15 for sample
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
