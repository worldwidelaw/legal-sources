#!/usr/bin/env python3
"""
EE/AKI -- Estonian Data Protection Authority (Andmekaitse Inspektsioon) Fetcher

Fetches GDPR enforcement decisions with full text from AKI website.

Strategy:
  - Discovery: Scrape HTML listing pages for each decision category
  - Full text: Download PDF documents and extract text
  - Categories: ettekirjutused (orders), vaideotsused (appeals), seisukohad (statements)

Data access method:
  - HTML scraping of listing pages
  - PDF download and text extraction
  - ~100+ decisions available (2019-present)

Usage:
  python bootstrap.py bootstrap           # Full historical pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, List, Tuple
from urllib.parse import urljoin, unquote
from bs4 import BeautifulSoup

# PDF text extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    from pypdf import PdfReader
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EE.AKI")

# Base URL
BASE_URL = "https://www.aki.ee"

# Decision listing pages
DECISION_PAGES = {
    "ettekirjutused": "/kiirelt-katte/aki-otsused/ettekirjutused",
    "vaideotsused": "/kiirelt-katte/aki-otsused/vaideotsused",
    "seisukohad": "/kiirelt-katte/aki-otsused/seisukohad",
}

# Month name mapping for Estonian dates
ESTONIAN_MONTHS = {
    'jaanuar': 1, 'veebruar': 2, 'märts': 3, 'aprill': 4,
    'mai': 5, 'juuni': 6, 'juuli': 7, 'august': 8,
    'september': 9, 'oktoober': 10, 'november': 11, 'detsember': 12
}


class AKIScraper(BaseScraper):
    """
    Scraper for EE/AKI -- Estonian Data Protection Authority decisions.
    Country: EE
    URL: https://www.aki.ee

    Data types: regulatory_decisions
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/pdf,application/xhtml+xml",
                "Accept-Language": "et,en;q=0.9",
            },
            timeout=60,
        )

        # Check for PDF support
        if not HAS_PDFPLUMBER and not HAS_PYPDF:
            logger.warning("No PDF library available. Install pdfplumber or pypdf for full text extraction.")

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """
        Extract text from PDF content.

        Tries pdfplumber first (better quality), falls back to pypdf.
        """
        text_parts = []

        if HAS_PDFPLUMBER:
            try:
                with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                if text_parts:
                    return "\n\n".join(text_parts)
            except Exception as e:
                logger.debug(f"pdfplumber extraction failed: {e}")

        if HAS_PYPDF:
            try:
                reader = PdfReader(io.BytesIO(pdf_content))
                for page in reader.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                if text_parts:
                    return "\n\n".join(text_parts)
            except Exception as e:
                logger.debug(f"pypdf extraction failed: {e}")

        return ""

    def _parse_estonian_date(self, date_str: str) -> Optional[str]:
        """
        Parse Estonian date format to ISO format.

        Formats:
          - "02.04.2025" -> "2025-04-02"
          - "2. aprill 2025" -> "2025-04-02"
        """
        if not date_str:
            return None

        date_str = date_str.strip()

        # Try DD.MM.YYYY format first
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            day, month, year = match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # Try "D. month YYYY" format
        for month_name, month_num in ESTONIAN_MONTHS.items():
            if month_name in date_str.lower():
                match = re.search(r'(\d{1,2})\.\s*' + month_name + r'\s*(\d{4})', date_str, re.IGNORECASE)
                if match:
                    day, year = match.groups()
                    return f"{year}-{month_num:02d}-{day.zfill(2)}"

        return None

    def _fetch_listing_page(self, decision_type: str) -> List[Dict]:
        """
        Fetch and parse a decision listing page.

        Returns list of dicts with: title, date, url, decision_type, addressee, topic
        """
        path = DECISION_PAGES.get(decision_type)
        if not path:
            return []

        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch {decision_type} listing: {e}")
            return []

        entries = []
        try:
            soup = BeautifulSoup(resp.content, 'html.parser')

            # Find all document links - they're typically in table rows or list items
            # The AKI website structure varies by page

            # Look for links to PDF files
            for link in soup.find_all('a', href=True):
                href = link.get('href', '')

                # Only process PDF links
                if '.pdf' not in href.lower():
                    continue

                # Skip duplicates and non-document links
                if '/sites/default/files/' not in href and not href.startswith('http'):
                    continue

                # Build full URL
                if href.startswith('/'):
                    pdf_url = urljoin(BASE_URL, href)
                elif not href.startswith('http'):
                    pdf_url = urljoin(BASE_URL, '/' + href)
                else:
                    pdf_url = href

                # Extract title from link text or parent context
                title = link.get_text(strip=True)
                if not title or len(title) < 5:
                    # Try to get title from parent element
                    parent = link.find_parent(['td', 'li', 'div', 'p'])
                    if parent:
                        title = parent.get_text(strip=True)[:200]

                # Extract date from surrounding context
                date_str = None
                parent_row = link.find_parent(['tr', 'li', 'div'])
                if parent_row:
                    row_text = parent_row.get_text()
                    # Look for date pattern
                    date_match = re.search(r'(\d{1,2}\.\d{1,2}\.\d{4})', row_text)
                    if date_match:
                        date_str = date_match.group(1)

                # Extract case number from title or URL
                case_number = None
                case_match = re.search(r'(\d+\.\d+[-./]\d+[-./]?\d*[-./]?\d*[-./]?\d*)', title + ' ' + href)
                if case_match:
                    case_number = case_match.group(1)

                # Decode URL-encoded filename for better title
                filename = unquote(href.split('/')[-1])
                if not title or len(title) < 10:
                    title = filename.replace('.pdf', '').replace('%20', ' ')

                entries.append({
                    "title": title,
                    "date_str": date_str,
                    "pdf_url": pdf_url,
                    "decision_type": decision_type,
                    "case_number": case_number,
                    "filename": filename,
                })

            logger.info(f"Found {len(entries)} {decision_type} documents")

        except Exception as e:
            logger.error(f"Error parsing {decision_type} listing: {e}")

        return entries

    def _fetch_pdf_content(self, pdf_url: str) -> Optional[bytes]:
        """Download PDF file content."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(pdf_url)  # HttpClient handles absolute URLs
            resp.raise_for_status()

            # Verify it's actually a PDF
            content_type = resp.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and not resp.content[:5] == b'%PDF-':
                logger.warning(f"URL {pdf_url} did not return PDF content")
                return None

            return resp.content
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

    # -- Abstract method implementations ---------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all AKI decisions from all categories.
        """
        seen_urls = set()

        for decision_type in DECISION_PAGES.keys():
            logger.info(f"Fetching {decision_type} decisions...")

            entries = self._fetch_listing_page(decision_type)

            for entry in entries:
                pdf_url = entry.get("pdf_url")

                # Skip duplicates (same PDF might appear in multiple places)
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions published since the given date.

        Note: The AKI website doesn't have an update feed, so we re-scan
        the listing pages and filter by date.
        """
        for entry in self.fetch_all():
            date_str = entry.get("date_str")
            if date_str:
                iso_date = self._parse_estonian_date(date_str)
                if iso_date:
                    try:
                        entry_date = datetime.fromisoformat(iso_date)
                        if entry_date.replace(tzinfo=timezone.utc) >= since:
                            yield entry
                    except ValueError:
                        # If date parsing fails, include the entry
                        yield entry
            else:
                # No date available, include it
                yield entry

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform raw data into standard schema.

        CRITICAL: Downloads and extracts FULL TEXT from PDF.
        """
        pdf_url = raw.get("pdf_url")
        if not pdf_url:
            return None

        # Download PDF
        pdf_content = self._fetch_pdf_content(pdf_url)
        if not pdf_content:
            logger.warning(f"Could not download PDF: {pdf_url}")
            return None

        # Extract text
        full_text = self._extract_pdf_text(pdf_content)

        if not full_text or len(full_text) < 100:
            logger.warning(f"Extracted text too short for {pdf_url}: {len(full_text) if full_text else 0} chars")
            return None

        # Clean up the text
        full_text = re.sub(r'\s+', ' ', full_text)  # Normalize whitespace
        full_text = full_text.strip()

        # Parse date
        date_str = raw.get("date_str")
        iso_date = self._parse_estonian_date(date_str) if date_str else None

        # Generate unique ID from URL
        filename = raw.get("filename", pdf_url.split('/')[-1])
        doc_id = re.sub(r'[^\w\-]', '_', filename.replace('.pdf', ''))

        # Extract case number from text if not found
        case_number = raw.get("case_number")
        if not case_number:
            case_match = re.search(r'(\d+\.\d+[-./]\d+[-./]?\d*[-./]?\d*[-./]?\d*)', full_text[:500])
            if case_match:
                case_number = case_match.group(1)

        title = raw.get("title", filename.replace('.pdf', '').replace('_', ' '))

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "EE/AKI",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title[:500],
            "text": full_text,  # MANDATORY FULL TEXT
            "date": iso_date,
            "url": pdf_url,
            # Decision-specific metadata
            "decision_type": raw.get("decision_type"),
            "case_number": case_number,
            "filename": filename,
            "language": "et",
        }

    # -- Custom commands -------------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Estonian AKI connectivity...")

        for decision_type, path in DECISION_PAGES.items():
            try:
                self.rate_limiter.wait()
                resp = self.client.get(path)
                resp.raise_for_status()
                print(f"  {decision_type}: OK ({len(resp.content)} bytes)")
            except Exception as e:
                print(f"  {decision_type}: FAILED ({e})")
                return

        # Test PDF download
        print("\nTesting PDF download...")
        entries = self._fetch_listing_page("ettekirjutused")
        if entries:
            first_entry = entries[0]
            pdf_url = first_entry.get("pdf_url")
            try:
                self.rate_limiter.wait()
                resp = self.client.get(pdf_url)
                resp.raise_for_status()
                print(f"  PDF download: OK ({len(resp.content)} bytes)")

                # Test PDF extraction
                if HAS_PDFPLUMBER or HAS_PYPDF:
                    text = self._extract_pdf_text(resp.content)
                    print(f"  PDF text extraction: OK ({len(text)} chars)")
                else:
                    print("  PDF text extraction: SKIPPED (no PDF library)")
            except Exception as e:
                print(f"  PDF download: FAILED ({e})")
                return

        print("\nConnectivity test passed!")

    def run_sample(self, n: int = 12) -> dict:
        """
        Fetch a sample of decisions with full text.
        """
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []
        text_lengths = []

        for raw in self.fetch_all():
            if saved >= n:
                break

            checked += 1
            pdf_url = raw.get("pdf_url", "")
            logger.info(f"Processing {raw.get('filename', pdf_url)[:50]}...")

            try:
                normalized = self.normalize(raw)

                if not normalized:
                    errors.append(f"{pdf_url}: Normalization returned None")
                    continue

                if not normalized.get("text"):
                    errors.append(f"{pdf_url}: No text content")
                    continue

                text_len = len(normalized.get("text", ""))
                if text_len < 500:
                    errors.append(f"{pdf_url}: Text too short ({text_len} chars)")
                    continue

                # Save to sample directory
                doc_id = normalized.get("_id", f"doc_{saved}")
                safe_name = re.sub(r'[^\w\-]', '_', doc_id)[:100]
                sample_path = sample_dir / f"{safe_name}.json"
                with open(sample_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

                saved += 1
                text_lengths.append(text_len)
                logger.info(f"  Saved {doc_id}: {normalized.get('title', '')[:50]}... ({text_len} chars)")

            except Exception as e:
                errors.append(f"{pdf_url}: {str(e)}")
                logger.error(f"Error processing {pdf_url}: {e}")

        stats = {
            "sample_records_saved": saved,
            "documents_checked": checked,
            "errors": errors[:10],
            "avg_text_length": sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            "min_text_length": min(text_lengths) if text_lengths else 0,
            "max_text_length": max(text_lengths) if text_lengths else 0,
            "pdf_library": "pdfplumber" if HAS_PDFPLUMBER else ("pypdf" if HAS_PYPDF else "none"),
        }

        return stats


# -- CLI Entry Point -----------------------------------------------------------


def main():
    scraper = AKIScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
            print(json.dumps(stats, indent=2))
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
