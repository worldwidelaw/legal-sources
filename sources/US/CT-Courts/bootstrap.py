#!/usr/bin/env python3
"""
US/CT-Courts -- Connecticut Supreme Court & Appellate Court Opinions

Fetches case law from the Connecticut Judicial Branch website (jud.ct.gov).

Strategy:
  - Parse yearly archive pages for Supreme Court and Appellate Court
  - Supreme Court archives: archiveAROsup{YY}.htm (2002-present)
  - Appellate Court archives: archiveAROap{YY}.htm (2003-present)
  - Download opinion PDFs and extract full text via pdfplumber
  - Skip dissent/concurrence PDFs (identified by case name containing
    "Dissent" or "Concurrence", or filename suffix E/A)

Data Coverage:
  - Supreme Court opinions from 2002 to present
  - Appellate Court opinions from 2003 to present
  - Language: English
  - Open access, no authentication required

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
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

# PDF extraction
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.CT-Courts")

# Configuration
BASE_URL = "https://www.jud.ct.gov"
ARCHIVE_BASE = f"{BASE_URL}/external/supapp/"

# Archive URL patterns
# Supreme Court: archiveAROsup{YY}.htm (2002-present)
# Appellate Court: archiveAROap{YY}.htm (2003-present)
CURRENT_YEAR = datetime.now().year
SUPREME_COURT_YEARS = list(range(CURRENT_YEAR, 2001, -1))
APPELLATE_COURT_YEARS = list(range(CURRENT_YEAR, 2002, -1))

# Pattern to match opinion PDF links and extract case info
# HTML structure: <li><a href="Cases/AROcr/CR353/CR353.8.pdf" title="opens a pdf">SC21125</a> - State v. Enrrique H.</li>
OPINION_PATTERN = re.compile(
    r'<li>\s*<a\s+href=["\']([^"\']+\.pdf)["\'][^>]*>([^<]+)</a>\s*-\s*([^<]+)</li>',
    re.IGNORECASE
)

# Pattern to extract publication date from headings
DATE_HEADING_PATTERN = re.compile(
    r'Published in the Connecticut Law Journal of\s+([A-Za-z]+\s+\d{1,2},?\s+\d{4})',
    re.IGNORECASE
)


def _is_dissent_or_concurrence(case_name: str, filename: str) -> bool:
    """Check if an opinion entry is a dissent or concurrence rather than the main opinion."""
    # Check case name for explicit dissent/concurrence labels
    name_lower = case_name.strip().lower()
    if "dissent" in name_lower or "concurrence" in name_lower:
        return True
    # Check filename: dissent/concurrence PDFs end with a letter suffix before .pdf
    # e.g., CR354.25E.pdf (E = dissent), CR354.17A.pdf (A = appendix/concurrence)
    base = filename.replace('.pdf', '').replace('.PDF', '')
    if re.search(r'\d+[A-Za-z]$', base):
        return True
    return False


def clean_opinion_text(text: str) -> str:
    """Remove standard disclaimer headers and clean extracted text."""
    if not text:
        return ""

    # Remove the standard disclaimer block (appears at start of most opinions)
    # Find where actual opinion content starts (after the asterisk block)
    lines = text.split('\n')
    start_idx = 0
    asterisk_count = 0

    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('*' * 10):
            asterisk_count += 1
            if asterisk_count >= 2:
                start_idx = i + 1
                break

    # If we found the disclaimer boundary, skip it
    if start_idx > 0:
        text = '\n'.join(lines[start_idx:])

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = text.strip()

    return text


def parse_date(date_str: str) -> Optional[str]:
    """Parse a date string into ISO 8601 format."""
    if not date_str:
        return None
    date_str = date_str.strip().rstrip(',')
    formats = [
        "%B %d, %Y",    # "December 30, 2025"
        "%B %d %Y",     # "December 30 2025"
        "%b %d, %Y",    # "Dec 30, 2025"
        "%b. %d, %Y",   # "Dec. 30, 2025"
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _build_session() -> requests.Session:
    """Build a requests.Session with retry logic and browser-like headers."""
    session = requests.Session()
    # Use a browser-like User-Agent -- some government sites block bot UAs
    # from datacenter/VPS IP ranges while allowing browser-like UAs.
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    # Retry on transient errors (502, 503, 504, connection errors)
    retry_strategy = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class CTCourtsScraper(BaseScraper):
    """
    Scraper for US/CT-Courts -- Connecticut Supreme & Appellate Court Opinions.
    Country: US
    URL: https://www.jud.ct.gov

    Data types: case_law
    Auth: none (open access)
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = _build_session()

    def _get_archive_url(self, court: str, year: int) -> str:
        """Build archive page URL for a given court and year."""
        yy = str(year)[2:]  # e.g., 2025 -> "25"
        if court == "supreme":
            return f"{ARCHIVE_BASE}archiveAROsup{yy}.htm"
        else:
            return f"{ARCHIVE_BASE}archiveAROap{yy}.htm"

    def _parse_archive_page(self, html: str, court: str) -> List[Dict[str, Any]]:
        """Parse an archive HTML page and extract opinion entries."""
        opinions = []

        # Find all date headings and opinion links
        # Process line by line to track current publication date
        for date_match in DATE_HEADING_PATTERN.finditer(html):
            date_str = date_match.group(1)
            pub_date = parse_date(date_str)

            # Find next date heading position
            next_date = DATE_HEADING_PATTERN.search(html, date_match.end())
            end_pos = next_date.start() if next_date else len(html)

            # Extract all opinion links in this date section
            section = html[date_match.start():end_pos]
            for op_match in OPINION_PATTERN.finditer(section):
                pdf_href = op_match.group(1)
                case_number = op_match.group(2).strip()
                case_name = op_match.group(3).strip()

                # Skip dissent/concurrence variants
                filename = pdf_href.split('/')[-1]
                if _is_dissent_or_concurrence(case_name, filename):
                    logger.debug(f"Skipping dissent/concurrence: {case_number} - {filename}")
                    continue

                # Build full PDF URL
                if pdf_href.startswith('http'):
                    pdf_url = pdf_href
                elif pdf_href.startswith('/'):
                    pdf_url = BASE_URL + pdf_href
                else:
                    pdf_url = ARCHIVE_BASE + pdf_href

                opinions.append({
                    "case_number": case_number,
                    "case_name": case_name,
                    "pdf_url": pdf_url,
                    "publication_date": pub_date,
                    "court": f"Connecticut {'Supreme Court' if court == 'supreme' else 'Appellate Court'}",
                })

        return opinions

    def _extract_pdf_text(self, pdf_data: bytes, source_id: str) -> str:
        """Extract text from PDF using centralized extractor.

        Args:
            pdf_data: Raw PDF bytes.
            source_id: Unique identifier for this document (used for Neon
                       idempotency). Must NOT be empty.
        """
        return extract_pdf_markdown(
            source="US/CT-Courts",
            source_id=source_id,
            pdf_bytes=pdf_data,
            table="case_law",
        ) or ""

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file with retry logic."""
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            content = resp.content
            # Sanity check: PDF should start with %PDF
            if content[:5] != b'%PDF-':
                ct = resp.headers.get('Content-Type', 'unknown')
                logger.warning(
                    f"Response from {url} does not look like a PDF "
                    f"(Content-Type: {ct}, first bytes: {content[:20]})"
                )
                # Still return it -- the extraction backend will reject bad data
            return content
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return None

    def _build_doc_id(self, case_number: str, court: str) -> str:
        """Build a stable document ID from court + case number."""
        court_abbr = "CTSC" if "Supreme" in court else "CTAC"
        return f"US-CT-{court_abbr}-{case_number}"

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all opinions from both courts, all years."""
        courts = [
            ("supreme", SUPREME_COURT_YEARS),
            ("appellate", APPELLATE_COURT_YEARS),
        ]

        total_archives_fetched = 0
        total_opinions_found = 0
        total_pdfs_failed = 0
        total_text_failed = 0

        for court_type, years in courts:
            for year in years:
                logger.info(f"Fetching {court_type} court archive for {year}...")
                url = self._get_archive_url(court_type, year)

                try:
                    resp = self.session.get(url, timeout=30)
                    if resp.status_code == 404:
                        logger.info(f"No archive for {court_type} {year} (404)")
                        continue
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"Failed to fetch archive {url}: {e}")
                    continue

                # Verify we got HTML, not an error page
                content_type = resp.headers.get('Content-Type', '')
                if 'text/html' not in content_type and 'text/plain' not in content_type:
                    logger.warning(
                        f"Unexpected content type for archive {url}: {content_type}"
                    )

                total_archives_fetched += 1
                opinions = self._parse_archive_page(resp.text, court_type)
                total_opinions_found += len(opinions)
                logger.info(f"Found {len(opinions)} opinions for {court_type} {year}")

                if not opinions and '<li>' in resp.text:
                    # Archive page has content but our parser found nothing --
                    # HTML structure may have changed.
                    logger.warning(
                        f"Archive page {url} has <li> tags but parser found 0 opinions. "
                        f"HTML structure may have changed. Response length: {len(resp.text)}"
                    )

                for opinion in opinions:
                    delay = self.config.get("fetch", {}).get("delay", 2.0)
                    time.sleep(delay)

                    doc_id = self._build_doc_id(
                        opinion["case_number"], opinion["court"]
                    )

                    pdf_data = self._download_pdf(opinion["pdf_url"])
                    if not pdf_data:
                        total_pdfs_failed += 1
                        continue

                    raw_text = self._extract_pdf_text(pdf_data, source_id=doc_id)
                    text = clean_opinion_text(raw_text)

                    if not text or len(text) < 100:
                        total_text_failed += 1
                        logger.warning(
                            f"Insufficient text for {opinion['case_number']}: "
                            f"{len(text) if text else 0} chars"
                        )
                        continue

                    opinion["text"] = text
                    yield opinion

                time.sleep(1.0)  # Rate limit between archive pages

        logger.info(
            f"fetch_all summary: {total_archives_fetched} archives fetched, "
            f"{total_opinions_found} opinions found, "
            f"{total_pdfs_failed} PDF downloads failed, "
            f"{total_text_failed} text extractions failed"
        )

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent opinions (current year and previous year)."""
        current_year = datetime.now().year
        years = [current_year, current_year - 1]

        for court_type in ["supreme", "appellate"]:
            for year in years:
                logger.info(f"Fetching {court_type} court updates for {year}...")
                url = self._get_archive_url(court_type, year)

                try:
                    resp = self.session.get(url, timeout=30)
                    if resp.status_code == 404:
                        continue
                    resp.raise_for_status()
                except Exception as e:
                    logger.warning(f"Failed to fetch archive {url}: {e}")
                    continue

                opinions = self._parse_archive_page(resp.text, court_type)

                for opinion in opinions:
                    # Filter by date if since is provided
                    if since and opinion.get("publication_date"):
                        if opinion["publication_date"] < since:
                            continue

                    delay = self.config.get("fetch", {}).get("delay", 2.0)
                    time.sleep(delay)

                    doc_id = self._build_doc_id(
                        opinion["case_number"], opinion["court"]
                    )

                    pdf_data = self._download_pdf(opinion["pdf_url"])
                    if not pdf_data:
                        continue

                    raw_text = self._extract_pdf_text(pdf_data, source_id=doc_id)
                    text = clean_opinion_text(raw_text)

                    if not text or len(text) < 100:
                        continue

                    opinion["text"] = text
                    yield opinion

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw opinion record into the standard schema."""
        case_num = raw.get("case_number", "")
        court = raw.get("court", "Connecticut Court")
        doc_id = self._build_doc_id(case_num, court)

        return {
            "_id": doc_id,
            "_source": "US/CT-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"{raw.get('case_name', '')} ({case_num})",
            "text": raw.get("text", ""),
            "date": raw.get("publication_date"),
            "url": raw.get("pdf_url", ""),
            "case_number": case_num,
            "court": court,
            "jurisdiction": "US-CT",
        }

    def test_connection(self) -> bool:
        """Test that the archive pages are accessible."""
        try:
            url = self._get_archive_url("supreme", CURRENT_YEAR)
            resp = self.session.get(url, timeout=15)
            if resp.status_code == 404:
                # Try previous year if current year archive not posted yet
                url = self._get_archive_url("supreme", CURRENT_YEAR - 1)
                resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
            opinions = self._parse_archive_page(resp.text, "supreme")
            logger.info(f"Connection test: found {len(opinions)} Supreme Court opinions")

            # Also test that we can download a PDF
            if opinions:
                pdf_data = self._download_pdf(opinions[0]["pdf_url"])
                if pdf_data:
                    logger.info(
                        f"PDF download test: OK ({len(pdf_data)} bytes from "
                        f"{opinions[0]['pdf_url']})"
                    )
                else:
                    logger.error("PDF download test: FAILED")
                    return False

            return len(opinions) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CT-Courts data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample for validation",
    )
    parser.add_argument(
        "--since",
        help="ISO date for incremental updates (YYYY-MM-DD)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CTCourtsScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999

        gen = scraper.fetch_all()
        for raw in gen:
            record = scraper.normalize(raw)

            # Save to sample directory
            safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record['_id']}: {record['title'][:60]} "
                f"({text_len} chars)"
            )
            count += 1

            if count >= target:
                break

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        gen = scraper.fetch_updates(since=args.since)
        for raw in gen:
            record = scraper.normalize(raw)
            safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
