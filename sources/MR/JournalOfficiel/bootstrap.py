#!/usr/bin/env python3
"""
MR/JournalOfficiel -- Mauritania Official Journal Data Fetcher

Fetches Mauritanian legislation from the Journal Officiel published by the
Secrétariat Général du Gouvernement at msgg.gov.mr.

Strategy:
  - The search page at /fr/droit-mauritanien/recherche.html lists 29,293 legal texts
  - Each text references a JO issue PDF at /JO/{year}/mauritanie-jo-{year}-{issue}.pdf
  - We iterate JO issue listing pages, download each PDF, extract text via PyPDF2
  - Each JO issue becomes one record with full extracted text

Endpoints:
  - JO listing: https://www.msgg.gov.mr/fr/droit-mauritanien/le-journal-officiel.html
  - JO PDFs:    https://msgg.gov.mr/JO/{year}/mauritanie-jo-{year}-{issue}.pdf
  - Search:     https://www.msgg.gov.mr/fr/droit-mauritanien/recherche.html

Data:
  - 1,633 JO issues from 1959-present
  - Each issue contains multiple legal acts (laws, decrees, orders, decisions)
  - French text (Arabic originals also exist)
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
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

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
logger = logging.getLogger("legal-data-hunter.MR.JournalOfficiel")

BASE_URL = "https://www.msgg.gov.mr"
JO_LIST_URL = f"{BASE_URL}/fr/droit-mauritanien/le-journal-officiel.html"
SEARCH_URL = f"{BASE_URL}/fr/droit-mauritanien/recherche.html"

# French month names for date parsing
FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
    "fevrier": 2, "aout": 8,  # accented variants
}


class MRJournalOfficielScraper(BaseScraper):
    """Scraper for Mauritania Official Journal (Journal Officiel)."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
            },
            timeout=60,
        )

    def _parse_jo_listing_page(self, page_num: int) -> List[Dict[str, str]]:
        """Parse a page of the JO issue listing to get issue metadata and PDF URLs."""
        url = f"{JO_LIST_URL}?page={page_num}&row=1633"
        resp = self.http.get(url)
        if not resp or resp.status_code != 200:
            logger.warning(f"Failed to fetch JO listing page {page_num}")
            return []

        content = resp.text
        issues = []

        # Extract JO issue entries: each has a PDF link and issue info
        # Pattern: href="https://msgg.gov.mr/JO/{year}/mauritanie-jo-{year}-{issue}.pdf"
        # And associated text with issue number and date
        pdf_pattern = re.compile(
            r'href="(https://msgg\.gov\.mr/JO/(\d{4})/mauritanie-jo-\d{4}-(\d+(?:-bis)?).pdf)"',
            re.IGNORECASE
        )

        # Find all PDF links and surrounding context
        for match in pdf_pattern.finditer(content):
            pdf_url = match.group(1)
            year = match.group(2)
            issue_num = match.group(3)

            issues.append({
                "pdf_url": pdf_url,
                "year": year,
                "issue_num": issue_num,
                "jo_id": f"JO-{year}-{issue_num}",
            })

        return issues

    def _parse_jo_listing_with_dates(self, page_num: int) -> List[Dict[str, str]]:
        """Parse JO listing page and extract dates from the HTML."""
        url = f"{JO_LIST_URL}?page={page_num}&row=1633"
        resp = self.http.get(url)
        if not resp or resp.status_code != 200:
            logger.warning(f"Failed to fetch JO listing page {page_num}")
            return []

        content = resp.text
        issues = []

        # Each JO item has a structure like:
        # <li> ... n°{year}-{issue} ... {date} ... <a href="...pdf"> ... </li>
        # Extract items between <li> tags in the listing
        li_pattern = re.compile(r'<li[^>]*>(.*?)</li>', re.DOTALL)
        pdf_pattern = re.compile(
            r'href="(https://msgg\.gov\.mr/JO/(\d{4})/mauritanie-jo-\d{4}-(\d+(?:-bis)?).pdf)"'
        )
        date_pattern = re.compile(r'(\d{1,2})/(\d{1,2})/(\d{4})')

        for li_match in li_pattern.finditer(content):
            li_content = li_match.group(1)
            pdf_match = pdf_pattern.search(li_content)
            if not pdf_match:
                continue

            pdf_url = pdf_match.group(1)
            year = pdf_match.group(2)
            issue_num = pdf_match.group(3)

            # Try to find date
            date_str = None
            dm = date_pattern.search(li_content)
            if dm:
                day, month, yr = dm.group(1), dm.group(2), dm.group(3)
                date_str = f"{yr}-{month.zfill(2)}-{day.zfill(2)}"

            issues.append({
                "pdf_url": pdf_url,
                "year": year,
                "issue_num": issue_num,
                "jo_id": f"JO-{year}-{issue_num}",
                "date": date_str,
            })

        return issues

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="MR/JournalOfficiel",
            source_id="",
            pdf_bytes=pdf_content,
            table="legislation",
        ) or ""

    def _extract_date_from_pdf_text(self, text: str, year: str) -> Optional[str]:
        """Try to extract the publication date from the PDF text."""
        # Look for patterns like "15 Mars 2026" or "30 Janvier 2025"
        for month_name, month_num in FRENCH_MONTHS.items():
            pattern = re.compile(
                rf'(\d{{1,2}})\s+{re.escape(month_name)}\s+(\d{{4}})',
                re.IGNORECASE
            )
            m = pattern.search(text[:1000])  # Only search first page
            if m:
                day = int(m.group(1))
                yr = m.group(2)
                return f"{yr}-{month_num:02d}-{day:02d}"

        # Fallback: use year-01-01
        return f"{year}-01-01"

    def _extract_issue_title(self, text: str, issue_num: str, year: str) -> str:
        """Generate a title for the JO issue."""
        # Try to find the issue number in the text
        num_match = re.search(r'N°\s*(\d+)', text[:500])
        official_num = num_match.group(1) if num_match else issue_num
        return f"Journal Officiel de la République Islamique de Mauritanie n°{year}-{official_num}"

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all JO issues with full text extracted from PDFs.
        Iterates the JO listing pages, downloads each PDF, extracts text.
        """
        if not HAS_PYPDF2:
            raise RuntimeError("PyPDF2 is required. Install: pip install PyPDF2")

        # Iterate through JO listing pages (82 pages, 20 per page)
        total_pages = 82
        seen_ids = set()

        for page_num in range(total_pages):
            logger.info(f"Fetching JO listing page {page_num + 1}/{total_pages}")
            issues = self._parse_jo_listing_with_dates(page_num)

            if not issues:
                logger.info(f"No issues found on page {page_num}, stopping")
                break

            for issue in issues:
                jo_id = issue["jo_id"]
                if jo_id in seen_ids:
                    continue
                seen_ids.add(jo_id)

                # Download PDF
                logger.info(f"Downloading {jo_id}: {issue['pdf_url']}")
                try:
                    pdf_resp = self.http.get(issue["pdf_url"])
                    if not pdf_resp or pdf_resp.status_code != 200:
                        logger.warning(f"Failed to download {issue['pdf_url']}: status {getattr(pdf_resp, 'status_code', 'None')}")
                        continue
                except Exception as e:
                    logger.warning(f"Error downloading {issue['pdf_url']}: {e}")
                    continue

                # Extract text
                text = self._extract_pdf_text(pdf_resp.content)
                if not text or len(text.strip()) < 100:
                    logger.warning(f"No meaningful text extracted from {jo_id}")
                    continue

                # Extract date from PDF text or use listing date
                date = issue.get("date")
                if not date:
                    date = self._extract_date_from_pdf_text(text, issue["year"])

                yield {
                    "jo_id": jo_id,
                    "year": issue["year"],
                    "issue_num": issue["issue_num"],
                    "pdf_url": issue["pdf_url"],
                    "date": date,
                    "text": text,
                    "pdf_size": len(pdf_resp.content),
                    "page_count": len(PyPDF2.PdfReader(io.BytesIO(pdf_resp.content)).pages),
                }

            self.rate_limiter.wait()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently published JO issues."""
        if not HAS_PYPDF2:
            raise RuntimeError("PyPDF2 is required. Install: pip install PyPDF2")

        # Only check first few listing pages for recent issues
        for page_num in range(3):
            issues = self._parse_jo_listing_with_dates(page_num)
            for issue in issues:
                # Download and yield
                try:
                    pdf_resp = self.http.get(issue["pdf_url"])
                    if not pdf_resp or pdf_resp.status_code != 200:
                        continue
                except Exception:
                    continue

                text = self._extract_pdf_text(pdf_resp.content)
                if not text or len(text.strip()) < 100:
                    continue

                date = issue.get("date") or self._extract_date_from_pdf_text(text, issue["year"])

                yield {
                    "jo_id": issue["jo_id"],
                    "year": issue["year"],
                    "issue_num": issue["issue_num"],
                    "pdf_url": issue["pdf_url"],
                    "date": date,
                    "text": text,
                    "pdf_size": len(pdf_resp.content),
                }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw JO issue into the standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 100:
            return None

        jo_id = raw["jo_id"]
        year = raw["year"]
        issue_num = raw["issue_num"]

        title = self._extract_issue_title(text, issue_num, year)
        date = raw.get("date", f"{year}-01-01")

        return {
            "_id": f"MR/JournalOfficiel/{jo_id}",
            "_source": "MR/JournalOfficiel",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw["pdf_url"],
            "jo_issue": jo_id,
            "year": int(year),
            "issue_number": issue_num,
            "language": "fr",
            "pdf_size": raw.get("pdf_size"),
            "page_count": raw.get("page_count"),
        }


# ── CLI entry point ──────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="MR/JournalOfficiel data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Sample mode: fetch only 10-15 records for testing",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MRJournalOfficielScraper()

    if args.command == "test":
        print("Testing connectivity...")
        try:
            resp = scraper.http.get(JO_LIST_URL)
            print(f"  JO listing page: {resp.status_code}")

            # Test PDF download
            issues = scraper._parse_jo_listing_page(0)
            if issues:
                test_issue = issues[0]
                print(f"  First issue: {test_issue['jo_id']}")
                pdf_resp = scraper.http.get(test_issue["pdf_url"])
                print(f"  PDF download: {pdf_resp.status_code} ({len(pdf_resp.content)} bytes)")

                if HAS_PYPDF2:
                    text = scraper._extract_pdf_text(pdf_resp.content)
                    print(f"  PDF text extraction: {len(text)} chars")
                    print(f"  First 200 chars: {text[:200]}")
                else:
                    print("  PyPDF2 not available - cannot extract PDF text")
            else:
                print("  No issues found on listing page")

            print("\nConnectivity test PASSED")
        except Exception as e:
            print(f"\nConnectivity test FAILED: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample
        sample_size = 15 if sample_mode else None

        if not HAS_PYPDF2:
            print("ERROR: PyPDF2 is required for PDF text extraction.")
            print("Install: pip install PyPDF2")
            sys.exit(1)

        print(f"Starting bootstrap (sample={sample_mode})...")
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=sample_size or 10)
        print(f"\nBootstrap complete:")
        print(f"  Records fetched: {stats.get('records_fetched', 0)}")
        print(f"  Records new: {stats.get('records_new', 0)}")
        print(f"  Errors: {stats.get('errors', 0)}")

        if sample_mode:
            sample_dir = scraper.source_dir / "sample"
            print(f"  Sample records saved to: {sample_dir}")

    elif args.command == "update":
        if not HAS_PYPDF2:
            print("ERROR: PyPDF2 is required.")
            sys.exit(1)

        print("Starting incremental update...")
        stats = scraper.update()
        print(f"\nUpdate complete:")
        print(f"  Records fetched: {stats.get('records_fetched', 0)}")
        print(f"  Records new: {stats.get('records_new', 0)}")


if __name__ == "__main__":
    main()
