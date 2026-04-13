#!/usr/bin/env python3
"""
IE/SupremeCourt -- Irish Courts Service Case Law Fetcher

Fetches Irish court judgments from the Courts Service of Ireland.

Strategy:
  - Paginate through the Drupal listing at www2.courts.ie/Judgments
  - Extract PDF URLs from the HTML table
  - Download PDFs and extract full text using pdfplumber

Endpoints:
  - Listing: https://www2.courts.ie/Judgments?sort=desc:DateOfDelivery&page=0
  - PDFs: https://www2.courts.ie/acc/alfresco/{uuid}/{citation}.pdf/pdf

Data:
  - Supreme Court (IESC), Court of Appeal (IECA), High Court (IEHC)
  - Judgments from 2001+ for Supreme Court, 2014+ for Court of Appeal
  - Rate limit: conservative 2 requests/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IE.SupremeCourt")

# Base URL for Courts Service
BASE_URL = "https://www2.courts.ie"

# Court code mappings
COURT_CODES = {
    "IESC": "Supreme Court",
    "IECA": "Court of Appeal",
    "IEHC": "High Court",
    "IECC": "Circuit Court",
    "IECOA": "Court of Criminal Appeal",  # Pre-2014
}


class IrishCourtsScraper(BaseScraper):
    """
    Scraper for IE/SupremeCourt -- Irish Courts Service Case Law.
    Country: IE
    URL: https://www2.courts.ie/Judgments

    Data types: case_law
    Auth: none (Public Access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-IE,en;q=0.9",
            },
            timeout=120,
        )

    def _get_listings_page(self, page: int = 0) -> Tuple[List[Dict], int]:
        """
        Fetch a page of judgment listings.

        Returns (list of judgments, total count).
        """
        judgments = []
        total = 0

        try:
            self.rate_limiter.wait()
            url = f"/Judgments?sort=desc:DateOfDelivery&page={page}"
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            # Extract total count from "Showing X of Y" text
            total_match = re.search(r'Showing\s+\d+\s+of\s+(\d+)', content)
            if total_match:
                total = int(total_match.group(1))

            # The HTML structure is:
            # <tr>
            #   <td>13/02/2026</td>  (date)
            #   <td><a href='/view/.../{citation}.pdf/pdf'>Title</a></td>  (title)
            #   <td class="pdf"><a href="/acc/alfresco/.../{citation}.pdf/pdf#view=fitH"...></a></td>  (PDF)
            #   <td>Court of Appeal</td>  (court)
            #   <td>Meenan J.</td>  (judge)
            # </tr>

            # Extract rows with judgment data - each row has view link with citation
            row_pattern = re.compile(
                r"<tr[^>]*>\s*"
                r"<td[^>]*>(\d{1,2}/\d{1,2}/\d{4})</td>\s*"  # Date (dd/mm/yyyy)
                r"<td[^>]*>\s*<a[^>]*href='([^']+)'[^>]*>([^<]+)</a>\s*</td>\s*"  # View link and title
                r"<td[^>]*class=\"pdf\"[^>]*>.*?href=\"([^\"]+)\"[^>]*>.*?</td>\s*"  # PDF link
                r"<td[^>]*>([^<]*)</td>\s*"  # Court
                r"<td[^>]*>([^<]*)</td>",  # Judge
                re.IGNORECASE | re.DOTALL
            )

            for match in row_pattern.finditer(content):
                date_delivered = match.group(1).strip()
                view_link = match.group(2).strip()
                title = html.unescape(match.group(3).strip())
                pdf_url = match.group(4).strip()
                court = match.group(5).strip()
                judge = match.group(6).strip()

                # Extract citation from view link
                # Format: /view/Judgments/{uuid}/{uuid}/{citation}.pdf/pdf
                citation_match = re.search(r'/([^/]+)\.pdf/pdf', view_link)
                citation = citation_match.group(1) if citation_match else ""

                # Clean up PDF URL (remove #view=fitH suffix)
                pdf_url = re.sub(r'#.*$', '', pdf_url)

                if citation and pdf_url:
                    judgments.append({
                        "citation": citation,
                        "title": title,
                        "court": court,
                        "judge": judge,
                        "date_delivered": date_delivered,
                        "date_uploaded": "",
                        "pdf_url": pdf_url,
                    })

            # Fallback: simpler extraction if complex pattern fails
            if not judgments:
                logger.debug("Complex pattern failed, trying simple extraction")
                # Find all PDF links and associated data
                pdf_links = re.findall(
                    r'href="(/acc/alfresco/[a-f0-9\-]+/([^"#]+)\.pdf(?:/pdf)?)',
                    content, re.IGNORECASE
                )

                for pdf_url, citation in pdf_links:
                    if citation.lower() in ['searching-judgments']:
                        continue

                    # Find the view link row for this citation to get metadata
                    view_pattern = re.compile(
                        r"<td[^>]*>(\d{1,2}/\d{1,2}/\d{4})</td>\s*"
                        r"<td[^>]*>\s*<a[^>]*href='[^']*" + re.escape(citation) + r"[^']*'[^>]*>([^<]+)</a>",
                        re.IGNORECASE | re.DOTALL
                    )
                    view_match = view_pattern.search(content)

                    if view_match:
                        date_delivered = view_match.group(1).strip()
                        title = html.unescape(view_match.group(2).strip())
                    else:
                        date_delivered = ""
                        title = ""

                    # Find court and judge after the PDF cell for this row
                    court = ""
                    judge = ""
                    court_pattern = re.compile(
                        re.escape(citation) + r"\.pdf.*?</td>\s*<td[^>]*>([^<]+)</td>\s*<td[^>]*>([^<]+)</td>",
                        re.IGNORECASE | re.DOTALL
                    )
                    court_match = court_pattern.search(content)
                    if court_match:
                        court = court_match.group(1).strip()
                        judge = court_match.group(2).strip()

                    judgments.append({
                        "citation": citation,
                        "title": title,
                        "court": court,
                        "judge": judge,
                        "date_delivered": date_delivered,
                        "date_uploaded": "",
                        "pdf_url": pdf_url,
                    })

            # Deduplicate by citation
            seen = set()
            unique_judgments = []
            for j in judgments:
                if j["citation"] not in seen:
                    seen.add(j["citation"])
                    unique_judgments.append(j)
            judgments = unique_judgments

            logger.info(f"Page {page}: Found {len(judgments)} judgments (total: {total})")
            return judgments, total

        except Exception as e:
            logger.warning(f"Failed to get listings page {page}: {e}")
            return [], 0

    def _download_and_extract_pdf(self, pdf_url: str, source_id: str = "") -> str:
        """
        Download PDF and extract text using common/pdf_extract.

        Returns extracted text or empty string on failure.
        """
        full_url = f"{BASE_URL}{pdf_url}" if pdf_url.startswith("/") else pdf_url
        text = extract_pdf_markdown(
            source="IE/SupremeCourt",
            source_id=source_id,
            pdf_url=full_url,
            table="case_law",
        )
        return text or ""

    def _parse_citation(self, citation: str) -> Dict[str, Any]:
        """
        Parse neutral citation to extract year, court, and number.

        Format: YYYY_IE{Court}_{Number} (underscore-separated)
        Or: [YYYY] IE{Court} {Number} (bracketed format in text)
        """
        # Handle underscore format: 2026_IEHC_83
        match = re.match(r'(\d{4})_([A-Z]+)_(\d+)', citation)
        if match:
            return {
                "year": int(match.group(1)),
                "court_code": match.group(2),
                "number": int(match.group(3)),
            }

        # Handle bracketed format from text
        match = re.match(r'\[(\d{4})\]\s*([A-Z]+)\s*(\d+)', citation)
        if match:
            return {
                "year": int(match.group(1)),
                "court_code": match.group(2),
                "number": int(match.group(3)),
            }

        return {"year": 0, "court_code": "", "number": 0}

    def _parse_date(self, date_str: str) -> str:
        """Parse date string to ISO 8601 format."""
        if not date_str:
            return ""

        # Remove ordinal suffixes
        date_str = re.sub(r'(\d)(st|nd|rd|th)', r'\1', date_str)

        formats = [
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y-%m-%d",
            "%d %B %Y",
            "%d %b %Y",
            "%B %d, %Y",
        ]

        for fmt in formats:
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all judgment documents from the Courts Service.

        Paginates through the listing and downloads PDFs.
        """
        page = 0
        seen_citations = set()

        while True:
            judgments, total = self._get_listings_page(page)

            if not judgments:
                logger.info(f"No more judgments at page {page}, stopping")
                break

            for j in judgments:
                citation = j["citation"]

                if citation in seen_citations:
                    continue
                seen_citations.add(citation)

                # Download and extract PDF
                full_text = self._download_and_extract_pdf(j["pdf_url"], source_id=citation)

                if not full_text:
                    logger.warning(f"No text extracted for {citation}, skipping")
                    continue

                yield {
                    "citation": citation,
                    "title": j["title"],
                    "court": j["court"],
                    "judge": j["judge"],
                    "date_delivered": j["date_delivered"],
                    "date_uploaded": j["date_uploaded"],
                    "pdf_url": j["pdf_url"],
                    "full_text": full_text,
                }

            page += 1

            # Safety limit
            if page > 5000:
                logger.warning("Reached page limit, stopping")
                break

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield judgments uploaded since the given date.

        Uses date_uploaded field to filter.
        """
        page = 0
        seen_citations = set()
        reached_old_content = False

        while not reached_old_content:
            judgments, total = self._get_listings_page(page)

            if not judgments:
                break

            for j in judgments:
                citation = j["citation"]

                if citation in seen_citations:
                    continue
                seen_citations.add(citation)

                # Check date
                date_str = self._parse_date(j["date_uploaded"]) or self._parse_date(j["date_delivered"])
                if date_str:
                    try:
                        doc_date = datetime.strptime(date_str, "%Y-%m-%d")
                        doc_date = doc_date.replace(tzinfo=timezone.utc)
                        if doc_date < since:
                            reached_old_content = True
                            continue
                    except:
                        pass

                full_text = self._download_and_extract_pdf(j["pdf_url"], source_id=citation)

                if not full_text:
                    continue

                yield {
                    "citation": citation,
                    "title": j["title"],
                    "court": j["court"],
                    "judge": j["judge"],
                    "date_delivered": j["date_delivered"],
                    "date_uploaded": j["date_uploaded"],
                    "pdf_url": j["pdf_url"],
                    "full_text": full_text,
                }

            page += 1

            if page > 100:  # Limit for updates
                break

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw judgment data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        citation = raw.get("citation", "")
        parsed = self._parse_citation(citation)

        year = parsed.get("year", 0)
        court_code = parsed.get("court_code", "")
        number = parsed.get("number", 0)

        # Format neutral citation properly
        neutral_citation = f"[{year}] {court_code} {number}" if year else citation

        # Get court name
        court = raw.get("court", "") or COURT_CODES.get(court_code, "")

        # Build document ID
        doc_id = f"{court_code}/{year}/{number}" if year else citation

        # Parse dates
        date_delivered = self._parse_date(raw.get("date_delivered", ""))
        date_uploaded = self._parse_date(raw.get("date_uploaded", ""))

        # Build URL
        pdf_url = raw.get("pdf_url", "")
        full_url = f"{BASE_URL}{pdf_url}" if pdf_url.startswith("/") else pdf_url

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IE/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),  # MANDATORY FULL TEXT
            "date": date_delivered,
            "url": full_url,
            # Additional metadata
            "citation": citation,
            "neutral_citation": neutral_citation,
            "court": court,
            "court_code": court_code,
            "judge": raw.get("judge", ""),
            "year": year,
            "number": number,
            "date_delivered": date_delivered,
            "date_uploaded": date_uploaded,
            "jurisdiction": "IE",
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Irish Courts Service endpoints...")
        print(f"pdfplumber available: {HAS_PDFPLUMBER}")

        # Test listing page
        print("\n1. Testing judgment listing page...")
        try:
            judgments, total = self._get_listings_page(0)
            print(f"   Found {len(judgments)} judgments on page 0 (total: {total})")
            if judgments:
                j = judgments[0]
                print(f"   Sample: {j['citation']} - {j['title'][:50]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test PDF download and extraction
        print("\n2. Testing PDF download and extraction...")
        if judgments and HAS_PDFPLUMBER:
            try:
                pdf_url = judgments[0]["pdf_url"]
                print(f"   PDF URL: {pdf_url}")
                full_text = self._download_and_extract_pdf(pdf_url)
                print(f"   Extracted {len(full_text)} characters")
                if full_text:
                    # Show first 200 chars
                    print(f"   Sample: {full_text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")
        else:
            print("   SKIPPED: No judgments or pdfplumber not available")

        # Test pagination
        print("\n3. Testing pagination (page 1)...")
        try:
            judgments_p1, _ = self._get_listings_page(1)
            print(f"   Found {len(judgments_p1)} judgments on page 1")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = IrishCourtsScraper()

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
