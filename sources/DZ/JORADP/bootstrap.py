#!/usr/bin/env python3
"""
DZ/JORADP -- Algerian Official Journal Data Fetcher

Fetches Algerian legislation from the Journal Officiel de la Republique
Algerienne Democratique et Populaire (JORADP).

Strategy:
  - PDFs are available at predictable URLs since 1994
  - French edition: /FTP/jo-francais/{year}/F{year}{issue:03d}.pdf
  - Arabic edition: /FTP/jo-arabe/{year}/A{year}{issue:03d}.pdf
  - Extract text from PDFs using PyPDF2
  - Each issue contains multiple legal acts (laws, decrees, orders, etc.)

Endpoints:
  - Base URL: https://www.joradp.dz
  - PDF French: /FTP/jo-francais/{year}/F{year}{issue:03d}.pdf
  - PDF Arabic: /FTP/jo-arabe/{year}/A{year}{issue:03d}.pdf
  - Homepage shows recent issues

Data:
  - Official Journal issues from 1994-present
  - Each issue contains: laws, decrees, orders, decisions, announcements
  - French and Arabic editions available
  - Open access, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent issues)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DZ.JORADP")

BASE_URL = "https://www.joradp.dz"

# Arabic month names for date parsing
ARABIC_MONTHS = {
    "جانفي": 1, "يناير": 1,
    "فيفري": 2, "فبراير": 2,
    "مارس": 3,
    "أفريل": 4, "ابريل": 4,
    "ماي": 5, "مايو": 5,
    "جوان": 6, "يونيو": 6,
    "جويلية": 7, "يوليو": 7,
    "أوت": 8, "اغسطس": 8,
    "سبتمبر": 9,
    "أكتوبر": 10,
    "نوفمبر": 11,
    "ديسمبر": 12,
}

# French month names
FRENCH_MONTHS = {
    "janvier": 1, "fevrier": 2, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "aout": 8, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "decembre": 12, "décembre": 12,
}


class JORADPScraper(BaseScraper):
    """
    Scraper for DZ/JORADP -- Algerian Official Journal.
    Country: DZ
    URL: https://www.joradp.dz

    Data types: legislation
    Auth: none (Open access)
    """

    def __init__(self):
        if not HAS_PYPDF2:
            raise ImportError("PyPDF2 is required for PDF text extraction. Install with: pip install PyPDF2")

        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf",
                "Accept-Language": "fr,en",
            },
            timeout=120,  # PDFs can take time
        )

        # Track issues
        self.current_year = datetime.now().year

    def _build_pdf_url(self, year: int, issue: int, language: str = "fr") -> str:
        """Build the URL for a specific issue PDF."""
        if language == "fr":
            return f"/FTP/jo-francais/{year}/F{year}{issue:03d}.pdf"
        else:
            return f"/FTP/jo-arabe/{year}/A{year}{issue:03d}.pdf"

    def _check_issue_exists(self, year: int, issue: int, language: str = "fr") -> bool:
        """Check if a specific issue exists by making a HEAD request."""
        url = self._build_pdf_url(year, issue, language)
        full_url = f"{BASE_URL}{url}"
        try:
            # Use session.head directly since HttpClient doesn't have head method
            resp = self.client.session.head(full_url, timeout=10)
            return resp.status_code == 200
        except:
            return False

    def _find_latest_issue(self, year: int, language: str = "fr") -> int:
        """Find the latest issue number for a given year using binary search."""
        # Start with known bounds
        low, high = 1, 100

        # Check if any issues exist
        if not self._check_issue_exists(year, 1, language):
            return 0

        # Binary search for the last valid issue
        while low < high:
            mid = (low + high + 1) // 2
            if self._check_issue_exists(year, mid, language):
                low = mid
            else:
                high = mid - 1

        return low

    def _download_pdf(self, year: int, issue: int, language: str = "fr") -> Optional[bytes]:
        """Download a PDF issue."""
        url = self._build_pdf_url(year, issue, language)
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.content
            return None
        except Exception as e:
            logger.warning(f"Failed to download PDF {url}: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> Tuple[str, int]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="DZ/JORADP",
            source_id="",
            pdf_bytes=pdf_content,
            table="legislation",
        ) or ""

    def _parse_issue_date(self, text: str, year: int) -> str:
        """
        Try to extract the publication date from the issue text.

        Returns ISO date string or empty string.
        """
        # French format: "Correspondant au 8 janvier 2026"
        fr_match = re.search(
            r"[Cc]orrespondant au (\d{1,2})\s+(\w+)\s+(\d{4})",
            text[:2000]
        )
        if fr_match:
            day = int(fr_match.group(1))
            month_name = fr_match.group(2).lower()
            year_str = fr_match.group(3)
            month = FRENCH_MONTHS.get(month_name, 0)
            if month:
                return f"{year_str}-{month:02d}-{day:02d}"

        # Try simpler date pattern
        simple_match = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text[:2000])
        if simple_match:
            day = int(simple_match.group(1))
            month_name = simple_match.group(2).lower()
            year_str = simple_match.group(3)
            month = FRENCH_MONTHS.get(month_name, 0)
            if month:
                return f"{year_str}-{month:02d}-{day:02d}"

        return ""

    def _fetch_issue(self, year: int, issue: int, language: str = "fr") -> Optional[Dict[str, Any]]:
        """
        Fetch a single issue and extract its content.

        Returns dict with issue data or None if not found.
        """
        logger.info(f"Fetching {year} issue {issue} ({language})...")

        pdf_content = self._download_pdf(year, issue, language)
        if not pdf_content:
            return None

        full_text, page_count = self._extract_text_from_pdf(pdf_content)
        if not full_text or len(full_text) < 100:
            logger.warning(f"No text extracted from {year}/{issue}")
            return None

        # Parse date from text
        pub_date = self._parse_issue_date(full_text, year)

        lang_prefix = "F" if language == "fr" else "A"

        return {
            "year": year,
            "issue_number": issue,
            "language": language,
            "full_text": full_text,
            "page_count": page_count,
            "pdf_url": f"{BASE_URL}{self._build_pdf_url(year, issue, language)}",
            "publication_date": pub_date,
            "pdf_size": len(pdf_content),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Official Journal issues from 1994 to present.

        Iterates through years and issues, downloading PDFs and extracting text.
        """
        start_year = 1994
        end_year = self.current_year

        for year in range(end_year, start_year - 1, -1):
            logger.info(f"Processing year {year}...")

            # Find the last issue of the year
            last_issue = self._find_latest_issue(year)
            if last_issue == 0:
                logger.warning(f"No issues found for year {year}")
                continue

            logger.info(f"Year {year} has {last_issue} issues")

            for issue in range(1, last_issue + 1):
                # Fetch French edition (primary)
                issue_data = self._fetch_issue(year, issue, "fr")
                if issue_data:
                    yield issue_data
                else:
                    # Try Arabic if French not available
                    issue_data = self._fetch_issue(year, issue, "ar")
                    if issue_data:
                        yield issue_data

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield issues published since the given date.

        Fetches recent issues from the current year and previous year.
        """
        years_to_check = [self.current_year]
        if since.year < self.current_year:
            years_to_check.append(self.current_year - 1)

        for year in years_to_check:
            last_issue = self._find_latest_issue(year)
            if last_issue == 0:
                continue

            # Check last 20 issues for updates
            start_issue = max(1, last_issue - 20)

            for issue in range(last_issue, start_issue - 1, -1):
                issue_data = self._fetch_issue(year, issue, "fr")
                if not issue_data:
                    continue

                # Check if this issue is new enough
                if issue_data.get("publication_date"):
                    try:
                        pub_dt = datetime.fromisoformat(issue_data["publication_date"])
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                        if pub_dt < since:
                            break  # Older issues, stop here
                    except:
                        pass

                yield issue_data

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw issue data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        year = raw.get("year", 0)
        issue = raw.get("issue_number", 0)
        language = raw.get("language", "fr")
        full_text = raw.get("full_text", "")
        pub_date = raw.get("publication_date", "")
        pdf_url = raw.get("pdf_url", "")

        # Build unique ID
        lang_prefix = "FR" if language == "fr" else "AR"
        doc_id = f"JORADP-{year}-{issue:03d}-{lang_prefix}"

        # Build title
        lang_name = "French" if language == "fr" else "Arabic"
        title = f"Journal Officiel de la Republique Algerienne - N° {issue}/{year} ({lang_name})"

        # Use pub_date or estimate from year
        if not pub_date:
            pub_date = f"{year}-01-01"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "DZ/JORADP",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": pub_date,
            "url": pdf_url,
            # Additional metadata
            "year": year,
            "issue_number": issue,
            "language": language,
            "page_count": raw.get("page_count", 0),
            "pdf_size_bytes": raw.get("pdf_size", 0),
            "document_type": "official_journal",
            "country": "DZ",
            "jurisdiction": "national",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Algerian Official Journal (JORADP) endpoints...")

        # Test homepage
        print("\n1. Testing homepage...")
        try:
            resp = self.client.get("/HAR/Index.htm")
            print(f"   Status: {resp.status_code}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test recent PDF
        print("\n2. Testing recent PDF availability...")
        try:
            # Check latest 2026 issue
            for issue in range(20, 0, -1):
                url = self._build_pdf_url(2026, issue)
                full_url = f"{BASE_URL}{url}"
                resp = self.client.session.head(full_url, timeout=10)
                if resp.status_code == 200:
                    print(f"   Found 2026 issue {issue}: {url}")
                    break
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test PDF download and text extraction
        print("\n3. Testing PDF download and text extraction...")
        try:
            issue_data = self._fetch_issue(2026, 1, "fr")
            if issue_data:
                text_len = len(issue_data.get("full_text", ""))
                print(f"   Issue 2026/001 downloaded successfully")
                print(f"   Pages: {issue_data.get('page_count', 0)}")
                print(f"   Text length: {text_len} characters")
                print(f"   Publication date: {issue_data.get('publication_date', 'N/A')}")
                if text_len > 0:
                    sample = issue_data["full_text"][:500].replace("\n", " ")
                    print(f"   Sample: {sample}...")
            else:
                print("   ERROR: Failed to fetch issue")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test Arabic edition
        print("\n4. Testing Arabic edition...")
        try:
            url = self._build_pdf_url(2026, 1, "ar")
            full_url = f"{BASE_URL}{url}"
            resp = self.client.session.head(full_url, timeout=10)
            print(f"   Arabic 2026/001 status: {resp.status_code}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = JORADPScraper()

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
