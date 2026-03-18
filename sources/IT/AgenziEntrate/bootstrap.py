#!/usr/bin/env python3
"""
IT/AgenziEntrate -- Italian Revenue Agency (Agenzia delle Entrate) Data Fetcher

Fetches Italian tax doctrine documents from Agenzia delle Entrate:
  - Interpelli (tax rulings): ~300+ per year
  - Circolari (circulars): ~15 per year
  - Risoluzioni (resolutions): ~70+ per year

Strategy:
  - Scrape index pages organized by year and month
  - Download PDF documents from official portal
  - Extract full text using pdfplumber
  - Normalize to standard schema

URL patterns:
  - Index: /portale/interpelli-{year}, /portale/circolari-{year}, /portale/risoluzioni-{year}
  - Monthly: /portale/gennaio-{year}-interpelli, etc.
  - PDFs: /portale/documents/{folder}/{docname}.pdf/{uuid}

License: Open Government Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (recent only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List, Dict, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

# Optional PDF extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    logging.warning("pdfplumber not installed. PDF text extraction will be limited.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.AgenziEntrate")

BASE_URL = "https://www.agenziaentrate.gov.it"

# Document type configurations
DOC_TYPES = {
    "interpello": {
        "prefix": "Risposta",
        "index_url": "/portale/interpelli-{year}",
        "monthly_pattern": "/portale/{month}-{year}-interpelli",
    },
    "circolare": {
        "prefix": "Circolare",
        "index_url": "/portale/circolari-{year}",
        "monthly_pattern": "/portale/{month}-{year}-circolari",
    },
    "risoluzione": {
        "prefix": "Risoluzione",
        "index_url": "/portale/risoluzioni-{year}",
        "monthly_pattern": "/portale/{month}-{year}-risoluzioni",
    },
}

# Italian month names
MONTHS_IT = [
    "gennaio", "febbraio", "marzo", "aprile",
    "maggio", "giugno", "luglio", "agosto",
    "settembre", "ottobre", "novembre", "dicembre"
]

MONTH_TO_NUM = {m: i + 1 for i, m in enumerate(MONTHS_IT)}


class AgenziEntrateScraper(BaseScraper):
    """
    Scraper for IT/AgenziEntrate -- Italian Revenue Agency tax doctrine.
    Country: IT
    URL: https://www.agenziaentrate.gov.it

    Fetches interpelli, circolari, and risoluzioni.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept-Language": "it,en",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=120,
        )

    def _download_pdf_curl(self, url: str) -> Optional[bytes]:
        """Download PDF using curl (bypasses Python SSL issues)."""
        try:
            full_url = url if url.startswith("http") else f"{BASE_URL}{url}"
            result = subprocess.run(
                ["curl", "-sL", "--max-time", "120", full_url],
                capture_output=True,
                timeout=130,
            )
            if result.returncode == 0 and len(result.stdout) > 1000:
                # Check if it's actually a PDF
                if result.stdout[:4] == b'%PDF':
                    return result.stdout
            return None
        except Exception as e:
            logger.warning(f"curl download failed for {url}: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from PDF bytes using pdfplumber."""
        if not HAS_PDFPLUMBER:
            logger.warning("pdfplumber not available")
            return None

        try:
            pdf_io = io.BytesIO(pdf_bytes)
            text_parts = []

            with pdfplumber.open(pdf_io) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)

            full_text = "\n\n".join(text_parts)

            # Clean up the text
            full_text = self._clean_text(full_text)

            return full_text if len(full_text) > 100 else None

        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return None

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        if not text:
            return ""

        # Normalize whitespace but preserve paragraph breaks
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n+', '\n\n', text)
        text = text.strip()

        return text

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch HTML page content."""
        try:
            time.sleep(1)  # Rate limiting
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.text
            logger.debug(f"Page {url} returned {resp.status_code}")
            return None
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _get_monthly_pages(self, doc_type: str, year: int) -> List[Dict]:
        """
        Get list of monthly pages for a doc type and year.

        Returns list of dicts with: url, month, year
        """
        pages = []
        config = DOC_TYPES[doc_type]

        # Try to fetch the year index page first
        year_url = config["index_url"].format(year=year)
        html = self._fetch_page(year_url)

        if not html:
            return pages

        # Extract monthly page links
        # Pattern: href="/portale/gennaio-2025-interpelli"
        pattern = rf'href="(/portale/([a-z]+)-{year}-{doc_type[:-1] if doc_type.endswith("e") else doc_type}i?)"'
        matches = re.findall(pattern, html, re.IGNORECASE)

        seen = set()
        for url, month in matches:
            if month.lower() in MONTH_TO_NUM and url not in seen:
                seen.add(url)
                pages.append({
                    "url": url,
                    "month": month.lower(),
                    "year": year,
                })

        # If no monthly pages found, try constructing URLs directly
        if not pages:
            for month in MONTHS_IT:
                url = config["monthly_pattern"].format(month=month, year=year)
                pages.append({
                    "url": url,
                    "month": month,
                    "year": year,
                })

        return pages

    def _get_documents_from_monthly_page(
        self, page_url: str, doc_type: str, year: int, month: str
    ) -> List[Dict]:
        """
        Extract document info from a monthly page.

        Returns list of dicts with: title, pdf_url, doc_number, pub_date
        """
        documents = []
        html = self._fetch_page(page_url)

        if not html:
            return documents

        # Extract PDF links - can be full URLs or relative paths
        # Pattern: href="https://www.agenziaentrate.gov.it/portale/documents/...pdf/UUID"
        # or href="/portale/documents/...Risposta+n.+298_2025.pdf/UUID"
        pdf_pattern = re.compile(
            r'href="((?:https?://[^/]+)?/portale/documents/[^"]+\.pdf/[a-f0-9-]+[^"]*)"',
            re.IGNORECASE
        )

        # Also extract titles from links
        # Pattern: <a href="PDF_URL">Title text</a>
        link_pattern = re.compile(
            r'<a[^>]+href="((?:https?://[^/]+)?/portale/documents/[^"]+\.pdf[^"]*)"[^>]*>([^<]+)</a>',
            re.IGNORECASE | re.DOTALL
        )

        seen_urls = set()

        for match in link_pattern.finditer(html):
            pdf_url = match.group(1)
            link_text = match.group(2).strip()

            # Skip duplicates
            base_url = pdf_url.split("?")[0]
            if base_url in seen_urls:
                continue
            seen_urls.add(base_url)

            # Extract document number from URL or link text
            doc_number = None

            # Try URL patterns
            url_match = re.search(r'[Rr]isposta\+?n\.?\+?(\d+)', pdf_url)
            if url_match:
                doc_number = url_match.group(1)

            if not doc_number:
                url_match = re.search(r'[Cc]ircolare\+?n\.?\+?(\d+)', pdf_url)
                if url_match:
                    doc_number = url_match.group(1)

            if not doc_number:
                url_match = re.search(r'[Rr]isoluzione\+?n\.?\+?(\d+)', pdf_url)
                if url_match:
                    doc_number = url_match.group(1)

            if not doc_number:
                url_match = re.search(r'RIS[_+]n[_+](\d+)', pdf_url)
                if url_match:
                    doc_number = url_match.group(1)

            if not doc_number:
                # Try extracting from filename
                name_match = re.search(r'(\d+)[_+]20\d{2}', pdf_url)
                if name_match:
                    doc_number = name_match.group(1)

            if not doc_number:
                continue

            # Extract date from URL or link text
            pub_date = None
            date_match = re.search(r'(\d{1,2})[\._+](\d{1,2})[\._+](20\d{2})', pdf_url)
            if date_match:
                day = int(date_match.group(1))
                month_num = int(date_match.group(2))
                doc_year = int(date_match.group(3))
                pub_date = f"{doc_year}-{month_num:02d}-{day:02d}"

            if not pub_date:
                # Try "del DD mese YYYY" pattern in link text
                date_match = re.search(
                    r'del\s+(\d{1,2})\s+(\w+)\s+(20\d{2})',
                    link_text,
                    re.IGNORECASE
                )
                if date_match:
                    day = int(date_match.group(1))
                    month_name = date_match.group(2).lower()
                    doc_year = int(date_match.group(3))
                    month_num = MONTH_TO_NUM.get(month_name)
                    if month_num:
                        pub_date = f"{doc_year}-{month_num:02d}-{day:02d}"

            if not pub_date:
                # Use month/year approximation
                month_num = MONTH_TO_NUM.get(month, 1)
                pub_date = f"{year}-{month_num:02d}-15"

            documents.append({
                "pdf_url": pdf_url,
                "title": link_text,
                "doc_number": doc_number,
                "pub_date": pub_date,
                "doc_type": doc_type,
            })

        logger.info(f"Found {len(documents)} documents on {page_url}")
        return documents

    def _fetch_document(self, doc_info: Dict) -> Optional[Dict]:
        """
        Download PDF and extract full text for a document.

        Returns dict with full text added, or None if extraction fails.
        """
        pdf_url = doc_info["pdf_url"]

        # Download PDF
        pdf_bytes = self._download_pdf_curl(pdf_url)
        if not pdf_bytes:
            logger.warning(f"Failed to download PDF: {pdf_url}")
            return None

        # Extract text
        full_text = self._extract_text_from_pdf(pdf_bytes)
        if not full_text:
            logger.warning(f"Failed to extract text from: {pdf_url}")
            return None

        return {
            **doc_info,
            "text": full_text,
        }

    def fetch_all(self, start_year: int = None, end_year: int = None) -> Generator[dict, None, None]:
        """
        Fetch all documents from all doc types and years.

        Args:
            start_year: First year to fetch (default: 2019)
            end_year: Last year to fetch (default: current year)
        """
        current_year = datetime.now().year

        if start_year is None:
            start_year = 2019
        if end_year is None:
            end_year = current_year

        doc_count = 0

        # Process each doc type
        for doc_type in ["interpello", "circolare", "risoluzione"]:
            logger.info(f"Processing {doc_type} documents...")

            # Process each year (most recent first)
            for year in range(end_year, start_year - 1, -1):
                logger.info(f"  Year {year}...")

                # Get monthly pages
                monthly_pages = self._get_monthly_pages(doc_type, year)

                for page in monthly_pages:
                    # Get documents from monthly page
                    documents = self._get_documents_from_monthly_page(
                        page["url"], doc_type, year, page["month"]
                    )

                    for doc_info in documents:
                        # Fetch and extract full text
                        doc_with_text = self._fetch_document(doc_info)
                        if doc_with_text and len(doc_with_text.get("text", "")) > 500:
                            yield doc_with_text
                            doc_count += 1

                            if doc_count % 50 == 0:
                                logger.info(f"Progress: {doc_count} documents fetched")

        logger.info(f"Fetch complete: {doc_count} total documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents published since a given date."""
        current_year = datetime.now().year
        since_year = since.year

        for doc_type in ["interpello", "circolare", "risoluzione"]:
            for year in range(current_year, since_year - 1, -1):
                monthly_pages = self._get_monthly_pages(doc_type, year)

                for page in monthly_pages:
                    documents = self._get_documents_from_monthly_page(
                        page["url"], doc_type, year, page["month"]
                    )

                    for doc_info in documents:
                        # Check if document is after since date
                        pub_date_str = doc_info.get("pub_date", "")
                        try:
                            pub_date = datetime.strptime(pub_date_str, "%Y-%m-%d")
                            if pub_date < since.replace(tzinfo=None):
                                continue
                        except ValueError:
                            pass

                        doc_with_text = self._fetch_document(doc_info)
                        if doc_with_text and len(doc_with_text.get("text", "")) > 500:
                            yield doc_with_text

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_type = raw.get("doc_type", "interpello")
        doc_number = raw.get("doc_number", "")
        title = raw.get("title", "")
        pub_date = raw.get("pub_date", "")
        pdf_url = raw.get("pdf_url", "")
        full_text = raw.get("text", "")

        # Extract year from date
        year = 0
        if pub_date and len(pub_date) >= 4:
            try:
                year = int(pub_date[:4])
            except ValueError:
                pass

        # Create unique ID
        doc_id = f"IT:AE:{doc_type}:{doc_number}_{year}"

        # Build title
        type_name = {
            "interpello": "Interpello",
            "circolare": "Circolare",
            "risoluzione": "Risoluzione",
        }.get(doc_type, "Documento")

        if not title or len(title) < 10:
            title = f"{type_name} n. {doc_number}/{year}"

        # Full URL
        full_url = pdf_url if pdf_url.startswith("http") else f"{BASE_URL}{pdf_url}"

        return {
            "_id": doc_id,
            "_source": "IT/AgenziEntrate",
            "_type": "other",  # Administrative guidance (circolari, interpelli, risoluzioni)
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": pub_date,
            "url": full_url,
            "doc_id": doc_id,
            "doc_type": doc_type,
            "doc_number": doc_number,
            "year": year,
            "language": "it",
            "authority": "Agenzia delle Entrate",
            "country": "IT",
        }

    def test_connection(self):
        """Test connectivity by checking index pages."""
        print("Testing Agenzia delle Entrate endpoints...")
        current_year = datetime.now().year

        # Test interpelli index
        print(f"\n1. Testing interpelli index {current_year}...")
        url = f"/portale/interpelli-{current_year}"
        html = self._fetch_page(url)
        if html:
            print(f"   OK - page length: {len(html)} chars")
        else:
            print("   FAILED")

        # Test a monthly page
        print("\n2. Testing monthly interpelli page...")
        pages = self._get_monthly_pages("interpello", current_year)
        if pages:
            print(f"   Found {len(pages)} monthly pages")
            # Test first available month
            for page in reversed(pages):
                docs = self._get_documents_from_monthly_page(
                    page["url"], "interpello", current_year, page["month"]
                )
                if docs:
                    print(f"   Found {len(docs)} documents in {page['month']}")
                    break
        else:
            print("   No monthly pages found")

        # Test PDF download and extraction
        print("\n3. Testing PDF download and extraction...")
        if pages:
            for page in reversed(pages):
                docs = self._get_documents_from_monthly_page(
                    page["url"], "interpello", current_year, page["month"]
                )
                if docs:
                    doc = docs[0]
                    print(f"   Downloading: {doc['pdf_url'][:80]}...")
                    result = self._fetch_document(doc)
                    if result and result.get("text"):
                        print(f"   OK - extracted {len(result['text'])} chars")
                        print(f"   Sample: {result['text'][:200]}...")
                    else:
                        print("   FAILED to extract text")
                    break

        print("\nTest complete!")


def main():
    scraper = AgenziEntrateScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
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
