#!/usr/bin/env python3
"""
AL/QBZ -- Albanian Official Gazette Data Fetcher

Fetches Albanian legislation from Qendra e Botimeve Zyrtare (Official Publishing Center).

Strategy:
  - Browse WebDAV directory structure to discover laws
  - Path pattern: /alfresco/webdav/Aktet/ligj/kuvendi-i-shqiperise/{year}/{month}/{day}/{number}/base/
  - Download PDF files and extract text using pdfplumber
  - ELI URIs: /eli/ligj/{year}/{month}/{day}/{number}

Document Types:
  - ligj = Law (primary legislation from Parliament)
  - vendim = Decision
  - dekret = Decree
  - urdher = Order
  - rregullore = Regulation

Endpoints:
  - WebDAV: https://qbz.gov.al/alfresco/webdav/Aktet/
  - ELI: https://qbz.gov.al/eli/ligj/{year}/{month}/{day}/{number}

Data Coverage:
  - Laws from 1990 to present
  - Language: Albanian (SQI)
  - EU Candidate Country

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent years only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html.parser import HTMLParser

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AL.QBZ")

# Configuration
BASE_URL = "https://qbz.gov.al"
WEBDAV_BASE = "/alfresco/webdav"
LAWS_PATH = "/Aktet/ligj/kuvendi-i-shqiperise"

# Years to scrape (newest first for sample mode)
YEARS_TO_SCRAPE = list(range(2026, 1989, -1))  # 2026 down to 1990


class WebDAVDirectoryParser(HTMLParser):
    """Simple HTML parser to extract directory listings from WebDAV."""

    def __init__(self):
        super().__init__()
        self.entries = []
        self.in_link = False
        self.current_href = None

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for name, value in attrs:
                if name == 'href':
                    self.in_link = True
                    self.current_href = value

    def handle_endtag(self, tag):
        if tag == 'a':
            self.in_link = False
            self.current_href = None

    def handle_data(self, data):
        if self.in_link and self.current_href:
            name = data.strip()
            if name and name not in ['..', 'Parent Directory', 'Name', 'Size', 'Last Modified']:
                # Clean up the href
                href = self.current_href
                if not href.startswith('/'):
                    href = '/' + href
                self.entries.append({
                    'name': name,
                    'href': href,
                    'is_dir': href.endswith('/') or '.' not in name.split('/')[-1]
                })


class QBZScraper(BaseScraper):
    """
    Scraper for AL/QBZ -- Albanian Official Gazette.
    Country: AL
    URL: https://qbz.gov.al

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "sq,en;q=0.9",
        })

    def _list_directory(self, path: str) -> List[Dict[str, Any]]:
        """List contents of a WebDAV directory."""
        url = f"{BASE_URL}{WEBDAV_BASE}{path}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)

            if resp.status_code == 404:
                return []

            resp.raise_for_status()

            # Parse the directory listing HTML
            parser = WebDAVDirectoryParser()
            parser.feed(resp.text)

            return parser.entries

        except Exception as e:
            logger.warning(f"Failed to list directory {path}: {e}")
            return []

    def _get_years(self) -> List[int]:
        """Get list of years with legislation."""
        entries = self._list_directory(LAWS_PATH)
        years = []

        for entry in entries:
            name = entry['name'].strip('/')
            if name.isdigit():
                year = int(name)
                if 1990 <= year <= 2030:
                    years.append(year)

        years.sort(reverse=True)
        return years

    def _get_months(self, year: int) -> List[str]:
        """Get list of months for a given year."""
        path = f"{LAWS_PATH}/{year}"
        entries = self._list_directory(path)
        months = []

        for entry in entries:
            name = entry['name'].strip('/')
            if name.isdigit() and 1 <= int(name) <= 12:
                months.append(name.zfill(2))

        months.sort()
        return months

    def _get_days(self, year: int, month: str) -> List[str]:
        """Get list of days for a given year/month."""
        path = f"{LAWS_PATH}/{year}/{month}"
        entries = self._list_directory(path)
        days = []

        for entry in entries:
            name = entry['name'].strip('/')
            if name.isdigit() and 1 <= int(name) <= 31:
                days.append(name.zfill(2))

        days.sort()
        return days

    def _get_law_numbers(self, year: int, month: str, day: str) -> List[str]:
        """Get list of law numbers for a given date."""
        path = f"{LAWS_PATH}/{year}/{month}/{day}"
        entries = self._list_directory(path)
        numbers = []

        for entry in entries:
            name = entry['name'].strip('/')
            if name.isdigit():
                numbers.append(name)

        return numbers

    def _download_pdf(self, year: int, month: str, day: str, number: str) -> Optional[bytes]:
        """Download PDF for a specific law."""
        # Build the path to the PDF
        pdf_name = f"ligj-{year}-{month}-{day}-{number}.pdf"
        pdf_path = f"{LAWS_PATH}/{year}/{month}/{day}/{number}/base/{pdf_name}"
        url = f"{BASE_URL}{WEBDAV_BASE}{pdf_path}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=120)

            if resp.status_code == 404:
                # Try alternative path without the 'base' directory
                pdf_path_alt = f"{LAWS_PATH}/{year}/{month}/{day}/{number}/{pdf_name}"
                url_alt = f"{BASE_URL}{WEBDAV_BASE}{pdf_path_alt}"
                resp = self.session.get(url_alt, timeout=120)

            if resp.status_code != 200:
                logger.warning(f"Failed to download PDF: {url} (HTTP {resp.status_code})")
                return None

            # Verify it's a PDF
            content_type = resp.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and not resp.content.startswith(b'%PDF'):
                logger.warning(f"Not a PDF: {url}")
                return None

            return resp.content

        except Exception as e:
            logger.warning(f"Error downloading PDF: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes."""
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

    def _extract_title_from_text(self, text: str) -> str:
        """Extract the title from the law text."""
        if not text:
            return ""

        # Albanian law titles typically start with "LIGJ Nr. X" or similar
        # Then followed by "PER..." (FOR/ABOUT)
        patterns = [
            # Pattern: LIGJ Nr. XX/YYYY \n PER ...
            r'LIGJ\s+Nr\.?\s*\d+[^P]*?(PËR\s+[^\n]+)',
            r'LIGJ\s+Nr\.?\s*\d+[^P]*?(PER\s+[^\n]+)',
            # Pattern: Full title including LIGJ Nr.
            r'(LIGJ\s+Nr\.?\s*\d+/\d+[^\n]*)',
            # Pattern: Just "PER" section
            r'^([^\n]*PËR[^\n]+)',
            r'^([^\n]*PER[^\n]+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                title = match.group(1).strip()
                # Clean up title
                title = re.sub(r'\s+', ' ', title)
                return title[:500]  # Limit length

        # Fallback: first non-empty line
        lines = [l.strip() for l in text.split('\n') if l.strip() and len(l.strip()) > 10]
        if lines:
            return lines[0][:200]

        return ""

    def _fetch_law(self, year: int, month: str, day: str, number: str) -> Optional[Dict[str, Any]]:
        """Fetch and process a single law document."""
        pdf_bytes = self._download_pdf(year, month, day, number)
        if not pdf_bytes:
            return None

        text = self._extract_pdf_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text extracted from law {year}/{month}/{day}/{number}")
            return None

        title = self._extract_title_from_text(text)
        if not title:
            title = f"Ligj Nr. {number}/{year}"

        # Build date string
        try:
            date_str = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        except:
            date_str = f"{year}-01-01"

        return {
            "year": year,
            "month": month,
            "day": day,
            "law_number": number,
            "title": title,
            "date": date_str,
            "full_text": text,
            "pdf_size": len(pdf_bytes),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all law documents from the Albanian Official Gazette.

        Iterates through years (newest first), months, days, and law numbers.
        """
        # Get available years
        years = self._get_years()
        if not years:
            # Fallback to predefined list
            years = YEARS_TO_SCRAPE

        logger.info(f"Found {len(years)} years to process")

        for year in years:
            logger.info(f"Processing year {year}...")

            months = self._get_months(year)
            if not months:
                continue

            for month in months:
                days = self._get_days(year, month)
                if not days:
                    continue

                for day in days:
                    numbers = self._get_law_numbers(year, month, day)
                    if not numbers:
                        continue

                    for number in numbers:
                        logger.info(f"Fetching law {year}/{month}/{day}/{number}...")

                        law = self._fetch_law(year, month, day, number)
                        if law:
                            yield law
                        else:
                            logger.warning(f"Failed to fetch law {year}/{month}/{day}/{number}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents created since the given date.

        Filters to years >= since.year.
        """
        since_year = since.year

        years = self._get_years()
        recent_years = [y for y in years if y >= since_year]

        for year in recent_years:
            logger.info(f"Checking year {year} for updates...")

            months = self._get_months(year)

            for month in months:
                days = self._get_days(year, month)

                for day in days:
                    # Check if this date is after 'since'
                    try:
                        date = datetime(year, int(month), int(day), tzinfo=timezone.utc)
                        if date < since:
                            continue
                    except:
                        pass

                    numbers = self._get_law_numbers(year, month, day)

                    for number in numbers:
                        law = self._fetch_law(year, month, day, number)
                        if law:
                            yield law

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        year = raw.get("year", 0)
        month = raw.get("month", "01")
        day = raw.get("day", "01")
        number = raw.get("law_number", "0")

        # Create unique document ID
        doc_id = f"ligj-{year}-{month}-{day}-{number}"

        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_str = raw.get("date", "")

        # Build ELI URI
        eli_uri = f"{BASE_URL}/eli/ligj/{year}/{month}/{day}/{number}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "AL/QBZ",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": eli_uri,
            # Additional metadata
            "doc_id": doc_id,
            "year": year,
            "month": month,
            "day": day,
            "law_number": number,
            "language": "sqi",
            "eli_uri": eli_uri,
            "document_type": "ligj",
            "issuing_authority": "Kuvendi i Shqiperise",  # Parliament of Albania
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Albanian Official Gazette (QBZ) endpoints...")

        # Test WebDAV root
        print("\n1. Testing WebDAV directory access...")
        try:
            entries = self._list_directory(LAWS_PATH)
            print(f"   Found {len(entries)} year directories")
            if entries:
                years = [e['name'] for e in entries if e['name'].isdigit()]
                print(f"   Years: {', '.join(years[:5])}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test year directory
        print("\n2. Testing year directory (2024)...")
        try:
            months = self._get_months(2024)
            print(f"   Found {len(months)} months: {', '.join(months)}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test month directory
        print("\n3. Testing month directory (2024/01)...")
        try:
            days = self._get_days(2024, "01")
            print(f"   Found {len(days)} days: {', '.join(days)}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test a specific law PDF download
        print("\n4. Testing PDF download (2024/01/25/1)...")
        try:
            pdf_bytes = self._download_pdf(2024, "01", "25", "1")
            if pdf_bytes:
                print(f"   Downloaded PDF: {len(pdf_bytes)} bytes")
                text = self._extract_pdf_text(pdf_bytes)
                print(f"   Extracted text: {len(text)} characters")
                if text:
                    title = self._extract_title_from_text(text)
                    print(f"   Title: {title[:80]}..." if len(title) > 80 else f"   Title: {title}")
            else:
                print("   PDF not found (trying different date)")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test ELI endpoint
        print("\n5. Testing ELI endpoint...")
        try:
            self.rate_limiter.wait()
            eli_url = f"{BASE_URL}/eli/ligj/2024/01/25/1"
            resp = self.session.get(eli_url, timeout=30)
            print(f"   ELI URL status: {resp.status_code}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = QBZScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
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
