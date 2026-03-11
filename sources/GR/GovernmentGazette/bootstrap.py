#!/usr/bin/env python3
"""
GR/GovernmentGazette -- Greek Government Gazette (FEK) Data Fetcher

Fetches Greek legislation from the Official Gazette (Εφημερίς της Κυβερνήσεως - FEK)
via the National Printing House PDF API.

Strategy:
  - Uses direct PDF download API at et.gr with IP address + Host header
  - FEK code format: YYYYSSNNNN (Year + Series + Issue number)
  - Full text extracted from PDF using pdfplumber
  - Primary legislation in Series 01 (ΤΕΥΧΟΣ ΠΡΩΤΟ)

Series:
  01 = ΤΕΥΧΟΣ ΠΡΩΤΟ (First Volume - Laws, Presidential Decrees)
  02 = ΤΕΥΧΟΣ ΔΕΥΤΕΡΟ (Second Volume - Regulatory Acts, Ministerial Decisions)
  03 = ΤΕΥΧΟΣ ΤΡΙΤΟ (Third Volume - Appointments)
  04 = ΤΕΥΧΟΣ ΤΕΤΑΡΤΟ (Fourth Volume - Announcements, Competitions)
  10 = ΤΕΥΧΟΣ Α.Σ.Ε.Π. (ASEP - Civil Service Examinations)

License: Public Domain (Official Government Acts)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import ssl
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

# PDF extraction - use pypdf (lighter memory footprint than pdfplumber)
try:
    from pypdf import PdfReader
    PDF_SUPPORT = True
except ImportError:
    try:
        # Fallback to older PyPDF2
        from PyPDF2 import PdfReader
        PDF_SUPPORT = True
    except ImportError:
        PDF_SUPPORT = False
        print("WARNING: pypdf not available. Install with: pip install pypdf")

import gc

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.GovernmentGazette")

# API configuration
API_IP = "20.95.103.179"
API_HOST = "et.gr"
API_ENDPOINT = "/api/DownloadFeksApi/"

# FEK Series codes
FEK_SERIES = {
    "01": "ΤΕΥΧΟΣ ΠΡΩΤΟ",      # Laws, Presidential Decrees
    "02": "ΤΕΥΧΟΣ ΔΕΥΤΕΡΟ",    # Regulatory Acts
    "03": "ΤΕΥΧΟΣ ΤΡΙΤΟ",      # Appointments
    "04": "ΤΕΥΧΟΣ ΤΕΤΑΡΤΟ",    # Announcements
    "10": "ΤΕΥΧΟΣ Α.Σ.Ε.Π.",   # Civil Service Exams
}

# Greek month names for date parsing
GREEK_MONTHS = {
    "Ιανουαρίου": 1, "Φεβρουαρίου": 2, "Μαρτίου": 3, "Απριλίου": 4,
    "Μαΐου": 5, "Ιουνίου": 6, "Ιουλίου": 7, "Αυγούστου": 8,
    "Σεπτεμβρίου": 9, "Οκτωβρίου": 10, "Νοεμβρίου": 11, "Δεκεμβρίου": 12,
}


class SSLAdapter(HTTPAdapter):
    """Adapter to handle SSL with specific settings."""
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        kwargs['ssl_context'] = ctx
        return super().init_poolmanager(*args, **kwargs)


class GovernmentGazetteScraper(BaseScraper):
    """
    Scraper for GR/GovernmentGazette -- Greek Official Gazette (FEK).
    Country: GR
    URL: https://www.et.gr

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Create session with SSL adapter
        self.session = requests.Session()
        self.session.mount('https://', SSLAdapter())
        self.session.headers.update({
            "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
            "Host": API_HOST,
            "Accept": "application/pdf,*/*",
        })

    def _build_fek_code(self, year: int, series: str, issue: int) -> str:
        """Build FEK code from components."""
        return f"{year}{series}{issue:05d}"

    def _parse_fek_code(self, fek_code: str) -> Tuple[int, str, int]:
        """Parse FEK code into components."""
        year = int(fek_code[:4])
        series = fek_code[4:6]
        issue = int(fek_code[6:])
        return year, series, issue

    def _download_fek_pdf(self, fek_code: str) -> Optional[bytes]:
        """Download FEK PDF by code."""
        try:
            self.rate_limiter.wait()
            url = f"https://{API_IP}{API_ENDPOINT}?fek_pdf={fek_code}"

            resp = self.session.get(url, timeout=60, verify=False)

            if resp.status_code != 200:
                logger.warning(f"Failed to download FEK {fek_code}: HTTP {resp.status_code}")
                return None

            # Check if response is PDF
            content_type = resp.headers.get('content-type', '')
            if 'pdf' not in content_type.lower() and not resp.content.startswith(b'%PDF'):
                logger.debug(f"FEK {fek_code} not found (returned HTML)")
                return None

            return resp.content

        except Exception as e:
            logger.warning(f"Error downloading FEK {fek_code}: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes with memory-efficient streaming.

        Uses pypdf (lighter than pdfplumber) and processes pages one at a time,
        discarding each page after extraction to minimize memory usage.
        """
        if not PDF_SUPPORT:
            return ""

        pdf_file = None
        reader = None
        try:
            pdf_file = io.BytesIO(pdf_bytes)
            reader = PdfReader(pdf_file)

            text_parts = []
            num_pages = len(reader.pages)

            # Process pages one at a time
            for i in range(num_pages):
                try:
                    page = reader.pages[i]
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    # Explicitly dereference page to help GC
                    del page
                except Exception as page_err:
                    logger.debug(f"Failed to extract page {i}: {page_err}")
                    continue

            full_text = "\n".join(text_parts)

            # Clean up text
            full_text = re.sub(r'\n{3,}', '\n\n', full_text)
            full_text = full_text.strip()

            return full_text

        except Exception as e:
            logger.warning(f"Failed to extract PDF text: {e}")
            return ""
        finally:
            # Explicit cleanup to free memory
            if reader is not None:
                del reader
            if pdf_file is not None:
                pdf_file.close()
                del pdf_file
            # Force garbage collection after each PDF
            gc.collect()

    def _parse_fek_metadata(self, text: str, fek_code: str) -> Dict[str, Any]:
        """Extract metadata from FEK text content."""
        year, series, issue = self._parse_fek_code(fek_code)

        metadata = {
            "fek_code": fek_code,
            "fek_year": year,
            "fek_series": series,
            "fek_series_name": FEK_SERIES.get(series, f"Unknown Series {series}"),
            "fek_issue": issue,
            "title": None,
            "date": None,
        }

        # Parse date from text
        # Format: "29 Απριλίου 2024 ΤΕΥΧΟΣ..."
        date_pattern = r'(\d{1,2})\s+(\w+)\s+(\d{4})\s+ΤΕΥΧΟΣ'
        match = re.search(date_pattern, text)
        if match:
            day = int(match.group(1))
            month_name = match.group(2)
            year_str = int(match.group(3))
            month = GREEK_MONTHS.get(month_name)
            if month:
                try:
                    date_obj = datetime(year_str, month, day, tzinfo=timezone.utc)
                    metadata["date"] = date_obj.isoformat()
                except ValueError:
                    pass

        # Extract title (usually first significant law/decree)
        # Look for "ΝΟΜΟΣ ΥΠ' ΑΡΙΘΜ." or "ΠΡΟΕΔΡΙΚΟ ΔΙΑΤΑΓΜΑ"
        title_patterns = [
            r'(ΝΟΜΟΣ\s+ΥΠ[\'΄]\s*ΑΡΙΘΜ?\.?\s*\d+[^\.]*)',
            r'(ΠΡΟΕΔΡΙΚΟ\s+ΔΙΑΤΑΓΜΑ\s+ΥΠ[\'΄]\s*ΑΡΙΘΜ?\.?\s*\d+[^\.]*)',
            r'(ΠΡΑΞΗ\s+ΝΟΜΟΘΕΤΙΚΟΥ\s+ΠΕΡΙΕΧΟΜΕΝΟΥ[^\.]*)',
        ]

        for pattern in title_patterns:
            match = re.search(pattern, text)
            if match:
                title = match.group(1).strip()
                # Clean up
                title = re.sub(r'\s+', ' ', title)
                metadata["title"] = title[:200]  # Truncate if too long
                break

        if not metadata["title"]:
            # Fallback to generic title
            metadata["title"] = f"ΦΕΚ {FEK_SERIES.get(series, 'Α')} {issue}/{year}"

        return metadata

    def _fetch_fek(self, fek_code: str) -> Optional[Dict[str, Any]]:
        """Fetch and process a single FEK document.

        Memory-optimized: explicitly frees PDF bytes after extraction.
        """
        pdf_bytes = self._download_fek_pdf(fek_code)
        if not pdf_bytes:
            return None

        pdf_size = len(pdf_bytes)
        text = self._extract_pdf_text(pdf_bytes)

        # Free PDF bytes immediately after extraction
        del pdf_bytes
        gc.collect()

        if not text or len(text) < 100:
            logger.warning(f"Insufficient text extracted from FEK {fek_code}")
            return None

        metadata = self._parse_fek_metadata(text, fek_code)
        metadata["full_text"] = text
        metadata["pdf_size"] = pdf_size

        return metadata

    def _find_latest_issue(self, year: int, series: str, start: int = 200) -> int:
        """Binary search to find the latest issue number for a series/year."""
        low, high = 1, start
        latest_found = 0

        # First, check if start exists
        fek_code = self._build_fek_code(year, series, start)
        pdf = self._download_fek_pdf(fek_code)

        if pdf:
            # Start exists, search higher
            high = start * 2
            while True:
                fek_code = self._build_fek_code(year, series, high)
                pdf = self._download_fek_pdf(fek_code)
                if not pdf:
                    break
                high *= 2
                if high > 10000:  # Safety limit
                    break

        # Binary search
        while low <= high:
            mid = (low + high) // 2
            fek_code = self._build_fek_code(year, series, mid)
            pdf = self._download_fek_pdf(fek_code)

            if pdf:
                latest_found = mid
                low = mid + 1
            else:
                high = mid - 1

        return latest_found

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all FEK documents."""
        current_year = datetime.now().year

        # Focus on primary legislation (Series 01)
        for year in range(current_year, 2000, -1):
            logger.info(f"Fetching FEK Series 01 for year {year}...")

            # Find latest issue
            latest = self._find_latest_issue(year, "01")
            if latest == 0:
                continue

            logger.info(f"Year {year}: {latest} issues found")

            for issue in range(1, latest + 1):
                fek_code = self._build_fek_code(year, "01", issue)
                record = self._fetch_fek(fek_code)
                if record:
                    yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield FEK documents published since the given date."""
        current_year = datetime.now().year
        since_year = since.year

        for year in range(current_year, since_year - 1, -1):
            for series in ["01", "02"]:  # Focus on laws and regulatory acts
                logger.info(f"Checking FEK Series {series} for year {year}...")

                # Start from recent issues
                for issue in range(1, 300):  # Reasonable upper bound
                    fek_code = self._build_fek_code(year, series, issue)
                    record = self._fetch_fek(fek_code)

                    if not record:
                        break  # No more issues in this series

                    # Check date
                    if record.get("date"):
                        doc_date = datetime.fromisoformat(record["date"].replace("Z", "+00:00"))
                        if doc_date < since:
                            break  # Reached older documents

                    yield record

    def normalize(self, raw: dict) -> dict:
        """Transform raw FEK data to standard schema."""
        year, series, issue = self._parse_fek_code(raw["fek_code"])

        return {
            "_id": raw["fek_code"],
            "_source": "GR/GovernmentGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", f"ΦΕΚ {series} {issue}/{year}"),
            "text": raw.get("full_text", ""),
            "date": raw.get("date"),
            "url": f"https://www.et.gr/api/DownloadFeksApi/?fek_pdf={raw['fek_code']}",
            "fek_code": raw["fek_code"],
            "fek_year": year,
            "fek_series": series,
            "fek_series_name": raw.get("fek_series_name"),
            "fek_issue": issue,
            "pdf_size": raw.get("pdf_size"),
        }

    def _fetch_sample(self, sample_size: int = 12) -> list:
        """Fetch sample records for validation."""
        samples = []
        current_year = datetime.now().year

        # Sample from different years and series
        test_codes = []

        # Recent Series 01 (Laws)
        for issue in [1, 10, 50, 100]:
            test_codes.append(self._build_fek_code(2024, "01", issue))

        # Series 02 (Regulatory)
        for issue in [1, 50]:
            test_codes.append(self._build_fek_code(2024, "02", issue))

        # Different years
        for year in [2023, 2022, 2021]:
            test_codes.append(self._build_fek_code(year, "01", 1))

        # Additional recent Series 01
        for issue in [20, 30, 40]:
            test_codes.append(self._build_fek_code(2024, "01", issue))

        for fek_code in test_codes:
            if len(samples) >= sample_size:
                break

            logger.info(f"Fetching sample FEK {fek_code}...")
            record = self._fetch_fek(fek_code)

            if record:
                normalized = self.normalize(record)
                samples.append(normalized)
                text_len = len(normalized.get("text", ""))
                logger.info(f"  -> {normalized['title'][:60]}... ({text_len} chars)")

        return samples


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GR/GovernmentGazette Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    args = parser.parse_args()

    scraper = GovernmentGazetteScraper()

    if args.command == "test":
        print("Testing Greek Government Gazette API connection...")

        if not PDF_SUPPORT:
            print("ERROR: pdfplumber not installed. Run: pip install pdfplumber")
            sys.exit(1)

        # Test a known FEK
        fek_code = "20240100061"
        print(f"Fetching FEK {fek_code}...")

        record = scraper._fetch_fek(fek_code)
        if record:
            print(f"SUCCESS: Retrieved FEK {fek_code}")
            print(f"  Title: {record.get('title', 'N/A')[:80]}")
            print(f"  Date: {record.get('date', 'N/A')}")
            print(f"  Text length: {len(record.get('full_text', ''))} chars")
            print(f"  PDF size: {record.get('pdf_size', 0)} bytes")
        else:
            print("FAILED: Could not retrieve FEK")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records from Greek Government Gazette...")

            if not PDF_SUPPORT:
                print("\nERROR: pdfplumber not installed. Run: pip install pdfplumber")
                sys.exit(1)

            samples = scraper._fetch_sample(sample_size=12)

            # Save samples
            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)

            for record in samples:
                filepath = sample_dir / f"{record['_id']}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to {sample_dir}/")

            # Print summary
            if samples:
                text_lengths = [len(s.get("text", "")) for s in samples]
                avg_len = sum(text_lengths) / len(text_lengths)
                print(f"Average text length: {avg_len:.0f} characters")
                print(f"Min text length: {min(text_lengths)} chars")
                print(f"Max text length: {max(text_lengths)} chars")
        else:
            print("Full bootstrap would fetch thousands of gazette issues.")
            print("Use --sample flag to fetch sample records first.")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        print(f"Fetching updates since {since.isoformat()}...")

        count = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
            count += 1

        print(f"\nFetched {count} new gazette issues")


if __name__ == "__main__":
    main()
