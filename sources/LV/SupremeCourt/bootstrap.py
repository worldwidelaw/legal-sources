#!/usr/bin/env python3
"""
LV/SupremeCourt -- Latvian Supreme Court (Senāts) Case Law Fetcher

Fetches Latvian Supreme Court decisions from the National Courts Portal.

Strategy:
  - Search for decisions using Court ID 44 (Augstākās tiesas Senāts)
  - Extract ECLI identifiers and PDF IDs from search results
  - Fetch ECLI metadata via JSON API
  - Download PDFs and extract full text using PyPDF2

Endpoints:
  - Search: https://manas.tiesas.lv/eTiesasMvc/lv/nolemumi (POST)
  - ECLI Metadata: https://manas.tiesas.lv/eTiesasMvc/geteclimetadata/{ecli}
  - PDF Download: https://manas.tiesas.lv/eTiesasMvc/nolemumi/pdf/{id}.pdf

Data:
  - Supreme Court Senate decisions (Senāta spriedumi, lēmumi)
  - Civil, Criminal, and Administrative departments
  - Full text in Latvian (PDF extracted)
  - ECLI identifiers since 2017

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LV.supremecourt")

# Base URLs
BASE_URL = "https://manas.tiesas.lv/eTiesasMvc"
SEARCH_URL = f"{BASE_URL}/lv/nolemumi"
ECLI_METADATA_URL = f"{BASE_URL}/geteclimetadata"
PDF_URL = f"{BASE_URL}/nolemumi/pdf"
INIT_URL = f"{BASE_URL}/nolemumi"

# Court IDs
SUPREME_COURT_SENATE_ID = 44  # Augstākās tiesas Senāts

# Headers for requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5,lv;q=0.3",
    "Content-Type": "application/x-www-form-urlencoded",
}


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for LV/SupremeCourt -- Latvian Supreme Court (Senāts).
    Country: LV
    URL: https://manas.tiesas.lv

    Data types: case_law
    Auth: none (public)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._verification_token = None

    def _get_verification_token(self) -> str:
        """Get the anti-CSRF verification token from the search page."""
        if self._verification_token:
            return self._verification_token

        try:
            self.rate_limiter.wait()
            resp = self.session.get(INIT_URL, timeout=30)
            resp.raise_for_status()

            # Extract __RequestVerificationToken
            match = re.search(
                r'__RequestVerificationToken.*?value="([^"]+)"',
                resp.text
            )
            if match:
                self._verification_token = match.group(1)
                logger.debug(f"Got verification token: {self._verification_token[:20]}...")
                return self._verification_token
            else:
                logger.warning("Could not find verification token")
                return ""
        except Exception as e:
            logger.error(f"Failed to get verification token: {e}")
            return ""

    def _search_decisions(
        self,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        page_size: int = 100,
    ) -> List[Dict]:
        """
        Search for Supreme Court decisions.

        Returns list of decision metadata dicts.
        Note: The search API caps results at 100, so use narrow date ranges.
        """
        token = self._get_verification_token()

        # Build search form data
        data = {
            "CourtId": str(SUPREME_COURT_SENATE_ID),
            "DateFrom": date_from or "",
            "DateTo": date_to or "",
            "DateFromDisplay": "",
            "DateToDisplay": "",
            "ProcTypeId": "0",
            "ProcSubjTypeId": "0",
            "DecisionHistFinalStat": "0",
            "RoundId": "0",
            "LawId": "0",
            "LawPant": "",
            "LawPantCaption": "",
            "SearchTypeId": "0",
            "AnonymisedText": "",
            "SearchStrNot": "",
            "CaseNum": "",
            "ArchiveNum": "",
            "EcliIdentifikators": "",
            "__RequestVerificationToken": token,
            "default-submit": "Meklēt",
            "PageSize": str(page_size),  # Max results per page
            "OrderBy": "",
            "OrderDirection": "desc",
        }

        if date_from:
            # Format: 2024-01-01 to 01.01.2024.
            try:
                dt = datetime.strptime(date_from, "%Y-%m-%d")
                data["DateFromDisplay"] = dt.strftime("%d.%m.%Y.")
            except:
                pass

        if date_to:
            try:
                dt = datetime.strptime(date_to, "%Y-%m-%d")
                data["DateToDisplay"] = dt.strftime("%d.%m.%Y.")
            except:
                pass

        try:
            self.rate_limiter.wait()
            resp = self.session.post(SEARCH_URL, data=data, timeout=60)
            resp.raise_for_status()

            html_content = resp.text

            # Update verification token for next request
            match = re.search(
                r'__RequestVerificationToken.*?value="([^"]+)"',
                html_content
            )
            if match:
                self._verification_token = match.group(1)

            # Extract results
            results = []

            # Find all PDF links and ECLI identifiers
            # Pattern: href=/eTiesasMvc/nolemumi/pdf/552677.pdf
            pdf_pattern = r'href=/eTiesasMvc/nolemumi/pdf/(\d+)\.pdf[^>]*>([^<]+)</a>'
            pdf_matches = re.findall(pdf_pattern, html_content)

            # Build result list with PDF IDs
            for pdf_id, title in pdf_matches:
                result = {
                    "pdf_id": pdf_id,
                    "title_short": html.unescape(title.strip()),
                }
                results.append(result)

            # Try to match ECLIs to PDF IDs (they appear near each other in HTML)
            ecli_pdf_pattern = r'href=/eTiesasMvc/nolemumi/pdf/(\d+)\.pdf.*?ECLI[:%]3a([A-Za-z0-9:.%]+)'
            ecli_pdf_matches = re.findall(ecli_pdf_pattern, html_content, re.DOTALL)

            ecli_map = {}
            for pdf_id, ecli_encoded in ecli_pdf_matches:
                ecli = urllib.parse.unquote(ecli_encoded.replace('%3a', ':').replace('%3A', ':'))
                if not ecli.startswith('ECLI:'):
                    ecli = 'ECLI:' + ecli
                ecli_map[pdf_id] = ecli

            # Attach ECLIs to results
            for result in results:
                pdf_id = result["pdf_id"]
                if pdf_id in ecli_map:
                    result["ecli"] = ecli_map[pdf_id]

            logger.info(f"Search {date_from} to {date_to}: {len(results)} results")
            return results

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def _fetch_ecli_metadata(self, ecli: str) -> Optional[Dict]:
        """
        Fetch metadata for a decision using the ECLI API.

        Returns JSON metadata dict or None on failure.
        """
        encoded_ecli = urllib.parse.quote(ecli, safe='')
        url = f"{ECLI_METADATA_URL}/{encoded_ecli}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            data = resp.json()
            return data

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch ECLI metadata for {ecli}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON from ECLI API for {ecli}: {e}")
            return None

    def _download_pdf(self, pdf_id: str) -> Optional[bytes]:
        """Download PDF document by ID."""
        url = f"{PDF_URL}/{pdf_id}.pdf"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=60)

            if resp.status_code == 302:
                # Follow redirect
                redirect_url = resp.headers.get('Location')
                if redirect_url and 'Error' not in redirect_url:
                    resp = self.session.get(redirect_url, timeout=60)

            if resp.status_code == 200 and resp.content[:4] == b'%PDF':
                return resp.content

            logger.warning(f"PDF download failed for {pdf_id}: HTTP {resp.status_code}")
            return None

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to download PDF {pdf_id}: {e}")
            return None

    def _fetch_decision(self, pdf_id: str, ecli: Optional[str] = None) -> Optional[Dict]:
        """
        Fetch a complete decision with metadata and full text.

        Returns raw dict with all fields or None on failure.
        """
        # First try to get ECLI metadata
        metadata = None
        if ecli:
            metadata = self._fetch_ecli_metadata(ecli)

        # Download PDF and extract text via centralized extractor
        pdf_content = self._download_pdf(pdf_id)
        if not pdf_content:
            logger.warning(f"Could not download PDF for {pdf_id}")
            return None

        full_text = extract_pdf_markdown(
            source="LV/SupremeCourt",
            source_id=ecli or f"pdf-{pdf_id}",
            pdf_bytes=pdf_content,
            table="case_law",
        )
        if not full_text:
            logger.warning(f"Could not extract text from PDF {pdf_id}")
            return None

        # Extract ECLI from text if not provided
        if not ecli:
            ecli_match = re.search(r'ECLI:LV:[A-Z]+:\d{4}:\d{4}\.[A-Za-z0-9.]+', full_text)
            if ecli_match:
                ecli = ecli_match.group(0)
                # Try to fetch metadata now
                if ecli:
                    metadata = self._fetch_ecli_metadata(ecli)

        # Build result
        result = {
            "pdf_id": pdf_id,
            "ecli": ecli or "",
            "full_text": full_text,
            "pdf_size": len(pdf_content),
        }

        if metadata:
            result.update({
                "metadata": metadata,
                "title": metadata.get("title", ""),
                "act_date": metadata.get("act_date", ""),
                "issued": metadata.get("issued", ""),
                "creator": metadata.get("creator", ""),
                "contributor": metadata.get("contributor", ""),
                "coverage": metadata.get("coverage", ""),
                "lang": metadata.get("lang", ""),
                "ecli_type": metadata.get("ecli_type", ""),
                "identifier_url": metadata.get("identifier_url", ""),
            })

        return result

    def _generate_week_ranges(self, start_year: int, end_year: int) -> Generator[Tuple[str, str], None, None]:
        """
        Generate weekly date ranges from start_year to end_year.

        The search API caps results at 100, so weekly ranges ensure we don't miss any.
        Most weeks have fewer than 100 decisions.
        """
        for year in range(end_year, start_year - 1, -1):
            # Generate weekly ranges for the year
            current = datetime(year, 1, 1)
            year_end = datetime(year, 12, 31)

            while current <= year_end:
                week_end = min(current + timedelta(days=6), year_end)
                yield current.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")
                current = week_end + timedelta(days=1)

    def fetch_all(self) -> Generator[Dict, None, None]:
        """
        Yield all Supreme Court decisions.

        Iterates through search results by week to avoid the 100-result cap.
        The search API limits results to 100 per query, so we use weekly
        date ranges to ensure complete coverage.
        """
        doc_count = 0
        seen_pdf_ids = set()
        current_year = datetime.now().year

        logger.info(f"Fetching decisions from 2017 to {current_year} using weekly ranges")

        # Iterate through weekly ranges (most recent first)
        for date_from, date_to in self._generate_week_ranges(2017, current_year):
            results = self._search_decisions(
                date_from=date_from,
                date_to=date_to,
                page_size=100
            )

            if not results:
                continue

            # Warn if we hit the cap (might be missing some)
            if len(results) >= 100:
                logger.warning(f"Week {date_from} hit 100-result cap, may be missing decisions")

            for item in results:
                pdf_id = item.get("pdf_id")
                ecli = item.get("ecli")

                if not pdf_id or pdf_id in seen_pdf_ids:
                    continue

                seen_pdf_ids.add(pdf_id)

                raw = self._fetch_decision(pdf_id, ecli)
                if raw and raw.get("full_text"):
                    doc_count += 1
                    yield raw

                    if doc_count % 50 == 0:
                        logger.info(f"Fetched {doc_count} decisions with full text")

        logger.info(f"Total decisions fetched: {doc_count}")

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        """
        Yield decisions published recently.

        Searches for decisions from the given date until now using weekly ranges.
        """
        date_from = since.strftime("%Y-%m-%d")
        date_to = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching updates from {date_from} to {date_to}")

        seen_pdf_ids = set()

        # Use weekly ranges to avoid 100-result cap
        current = since
        while current.date() <= datetime.now().date():
            week_end = min(current + timedelta(days=6), datetime.now())

            results = self._search_decisions(
                date_from=current.strftime("%Y-%m-%d"),
                date_to=week_end.strftime("%Y-%m-%d"),
                page_size=100
            )

            for item in results:
                pdf_id = item.get("pdf_id")
                ecli = item.get("ecli")

                if not pdf_id or pdf_id in seen_pdf_ids:
                    continue

                seen_pdf_ids.add(pdf_id)

                raw = self._fetch_decision(pdf_id, ecli)
                if raw and raw.get("full_text"):
                    yield raw

            current = week_end + timedelta(days=1)

    def normalize(self, raw: Dict) -> Dict:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        ecli = raw.get("ecli", "")
        pdf_id = raw.get("pdf_id", "")
        full_text = raw.get("full_text", "")

        # Extract ECLI from text if not present
        if not ecli and full_text:
            ecli_match = re.search(r'ECLI:LV:[A-Z]+:\d{4}:\d{4}\.[A-Za-z0-9.]+', full_text)
            if ecli_match:
                ecli = ecli_match.group(0)

        # Create unique ID
        doc_id = ecli if ecli else f"pdf-{pdf_id}"

        # Get title
        title = raw.get("title", "")
        if not title:
            # Extract from metadata or text
            metadata = raw.get("metadata", {})
            title = metadata.get("title", "")

        if not title and full_text:
            # Try to extract case type from text
            first_lines = full_text[:500]
            if "Civillieta" in first_lines:
                title = "Civillieta"
            elif "Krimināllieta" in first_lines:
                title = "Krimināllieta"
            elif "Administratīvā lieta" in first_lines:
                title = "Administratīvā lieta"
            else:
                title = "Tiesas nolēmums"

        # Get date
        act_date = raw.get("act_date", "")
        if not act_date:
            # Try to extract from ECLI: ECLI:LV:AT:2025:0320...
            if ecli:
                date_match = re.search(r':(\d{4}):(\d{2})(\d{2})\.', ecli)
                if date_match:
                    year, month, day = date_match.groups()
                    act_date = f"{year}-{month}-{day}"

        # Build URL
        url = raw.get("identifier_url", "")
        if not url:
            url = f"{PDF_URL}/{pdf_id}.pdf"

        # Get creator/court
        creator = raw.get("creator", "Augstākās tiesas Senāts")

        # Get judge names
        contributor = raw.get("contributor", "")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "LV/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": act_date,
            "url": url,
            # Additional metadata
            "ecli": ecli,
            "pdf_id": pdf_id,
            "court": creator,
            "judges": contributor,
            "issued": raw.get("issued", ""),
            "ecli_type": raw.get("ecli_type", ""),
            "language": raw.get("lang", "LV").lower(),
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LV/SupremeCourt endpoints...")

        results = []

        # Test search
        print("\n1. Testing search...")
        try:
            results = self._search_decisions(
                date_from="2024-01-01",
                date_to="2024-01-07"  # One week
            )
            print(f"   Search returned {len(results)} results")
            if results:
                print(f"   Sample: PDF {results[0].get('pdf_id')}, {results[0].get('title_short', 'N/A')[:50]}")
                if results[0].get('ecli'):
                    print(f"   ECLI: {results[0].get('ecli')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test ECLI metadata
        print("\n2. Testing ECLI metadata...")
        try:
            if results and results[0].get('ecli'):
                ecli = results[0].get('ecli')
                metadata = self._fetch_ecli_metadata(ecli)
                if metadata:
                    print(f"   Got metadata for {ecli[:40]}...")
                    print(f"   Title: {metadata.get('title', 'N/A')}")
                    print(f"   Date: {metadata.get('act_date', 'N/A')}")
                    print(f"   Court: {metadata.get('creator', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test PDF download and text extraction
        print("\n3. Testing PDF download...")
        try:
            if results:
                pdf_id = results[0].get('pdf_id')
                pdf_content = self._download_pdf(pdf_id)
                if pdf_content:
                    print(f"   Downloaded PDF {pdf_id}: {len(pdf_content)} bytes")

                    text = extract_pdf_markdown(
                        source="LV/SupremeCourt", source_id=f"test-{pdf_id}",
                        pdf_bytes=pdf_content, table="case_law", force=True,
                    ) or ""
                    print(f"   Extracted text: {len(text)} characters")
                    if text:
                        print(f"   Preview: {text[:200]}...")
                else:
                    print("   ERROR: Could not download PDF")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = SupremeCourtScraper()

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
