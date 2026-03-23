#!/usr/bin/env python3
"""
RO/LegislationDatabase -- Portal Legislativ Data Fetcher

Fetches Romanian legislation from the Ministry of Justice SOAP API.
Full text is returned directly in the API response.

Strategy:
  - Bootstrap: Paginates through legislation by year using SOAP Search method.
  - Update: Searches recent years for newly added/modified legislation.
  - Sample: Fetches 10+ records from recent years for validation.

API: https://legislatie.just.ro/apiws/FreeWebService.svc/SOAP
WSDL: https://legislatie.just.ro/apiws/FreeWebService.svc?wsdl
Docs: http://legislatie.just.ro/ServiciulWebLegislatie.htm

Usage:
  python bootstrap.py bootstrap          # Full initial pull (150K+ records)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (recent years)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import html
import random
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RO.LegislationDatabase")

# SOAP endpoint
SOAP_ENDPOINT = "https://legislatie.just.ro/apiws/FreeWebService.svc/SOAP"

# XML namespaces
SOAP_NS = "http://schemas.xmlsoap.org/soap/envelope/"
SERVICE_NS = "http://tempuri.org/"
DATA_NS = "http://schemas.datacontract.org/2004/07/FreeWebService"

# Years to search (Romania provides data from 1989 onwards)
START_YEAR = 1989
CURRENT_YEAR = datetime.now().year


class RoLegislationScraper(BaseScraper):
    """
    Scraper for RO/LegislationDatabase -- Romanian Legislative Portal.
    Country: RO
    URL: https://legislatie.just.ro

    Data types: legislation
    Auth: token (obtained via GetToken SOAP call, no registration)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",  # Using full URLs for SOAP
            headers={
                "Content-Type": "text/xml; charset=utf-8",
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            },
            timeout=60,
        )
        self.token = None

    # -- SOAP helpers --------------------------------------------------------

    def _get_token(self) -> str:
        """Obtain authentication token from the API."""
        if self.token:
            return self.token

        envelope = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="{SOAP_NS}">
  <soap:Body>
    <GetToken xmlns="{SERVICE_NS}"/>
  </soap:Body>
</soap:Envelope>'''

        self.rate_limiter.wait()
        resp = self.client.post(
            SOAP_ENDPOINT,
            data=envelope.encode("utf-8"),
            headers={
                "SOAPAction": f"{SERVICE_NS}IFreeWebService/GetToken",
                "Content-Type": "text/xml; charset=utf-8",
            },
        )
        resp.raise_for_status()

        # Parse response
        root = ET.fromstring(resp.content)
        # Find token in response
        for elem in root.iter():
            if elem.tag.endswith("GetTokenResult") and elem.text:
                self.token = elem.text
                logger.info(f"Obtained API token: {self.token[:20]}...")
                return self.token

        raise RuntimeError("Failed to obtain API token")

    def _search(
        self,
        page: int = 1,
        results_per_page: int = 50,
        year: Optional[str] = None,
        number: Optional[str] = None,
        title: Optional[str] = None,
        text: Optional[str] = None,
        max_retries: int = 5,
    ) -> list:
        """
        Execute a SOAP search query with robust retry logic.

        Returns a list of legislation records (raw dicts).
        Retries with exponential backoff on 500 errors.
        """
        token = self._get_token()

        # Build optional search parameters
        search_an = f"<free:SearchAn>{year}</free:SearchAn>" if year else "<free:SearchAn/>"
        search_numar = f"<free:SearchNumar>{number}</free:SearchNumar>" if number else "<free:SearchNumar/>"
        search_titlu = f"<free:SearchTitlu>{html.escape(title)}</free:SearchTitlu>" if title else "<free:SearchTitlu/>"
        search_text = f"<free:SearchText>{html.escape(text)}</free:SearchText>" if text else "<free:SearchText/>"

        envelope = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="{SOAP_NS}" xmlns:tem="{SERVICE_NS}" xmlns:free="{DATA_NS}">
  <soap:Body>
    <tem:Search>
      <tem:SearchModel>
        <free:NumarPagina>{page}</free:NumarPagina>
        <free:RezultatePagina>{results_per_page}</free:RezultatePagina>
        {search_an}
        {search_numar}
        {search_text}
        {search_titlu}
      </tem:SearchModel>
      <tem:tokenKey>{token}</tem:tokenKey>
    </tem:Search>
  </soap:Body>
</soap:Envelope>'''

        last_error = None
        for attempt in range(max_retries):
            self.rate_limiter.wait()

            try:
                resp = self.client.post(
                    SOAP_ENDPOINT,
                    data=envelope.encode("utf-8"),
                    headers={
                        "SOAPAction": f"{SERVICE_NS}IFreeWebService/Search",
                        "Content-Type": "text/xml; charset=utf-8",
                    },
                )

                # Check for 500-level errors specifically
                if resp.status_code >= 500:
                    # Server error - back off and retry
                    backoff = (2 ** attempt) + random.uniform(0, 1)
                    logger.warning(
                        f"Server error {resp.status_code} on page {page}, "
                        f"attempt {attempt + 1}/{max_retries}. Backing off {backoff:.1f}s"
                    )
                    time.sleep(backoff)

                    # On 3rd+ failure, refresh token in case it's stale
                    if attempt >= 2:
                        logger.info("Refreshing token after multiple failures")
                        self.token = None
                        token = self._get_token()
                        # Update envelope with new token
                        envelope = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="{SOAP_NS}" xmlns:tem="{SERVICE_NS}" xmlns:free="{DATA_NS}">
  <soap:Body>
    <tem:Search>
      <tem:SearchModel>
        <free:NumarPagina>{page}</free:NumarPagina>
        <free:RezultatePagina>{results_per_page}</free:RezultatePagina>
        {search_an}
        {search_numar}
        {search_text}
        {search_titlu}
      </tem:SearchModel>
      <tem:tokenKey>{token}</tem:tokenKey>
    </tem:Search>
  </soap:Body>
</soap:Envelope>'''
                    continue

                resp.raise_for_status()

                # Parse response
                root = ET.fromstring(resp.content)
                records = []

                # Find all Legi elements
                for legi in root.iter():
                    if legi.tag.endswith("Legi"):
                        record = {}
                        for child in legi:
                            # Strip namespace prefix from tag
                            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
                            record[tag] = child.text or ""
                        if record:
                            records.append(record)

                return records

            except Exception as e:
                last_error = e
                backoff = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    f"SOAP request failed (attempt {attempt + 1}/{max_retries}): {e}. "
                    f"Backing off {backoff:.1f}s"
                )
                time.sleep(backoff)

                # Refresh token after failures
                if attempt >= 2:
                    self.token = None
                    try:
                        token = self._get_token()
                        envelope = f'''<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="{SOAP_NS}" xmlns:tem="{SERVICE_NS}" xmlns:free="{DATA_NS}">
  <soap:Body>
    <tem:Search>
      <tem:SearchModel>
        <free:NumarPagina>{page}</free:NumarPagina>
        <free:RezultatePagina>{results_per_page}</free:RezultatePagina>
        {search_an}
        {search_numar}
        {search_text}
        {search_titlu}
      </tem:SearchModel>
      <tem:tokenKey>{token}</tem:tokenKey>
    </tem:Search>
  </soap:Body>
</soap:Envelope>'''
                    except Exception as token_err:
                        logger.error(f"Failed to refresh token: {token_err}")

        # All retries exhausted
        logger.error(f"All {max_retries} retries failed for page {page}: {last_error}")
        return []

    def _paginate_year(
        self, year: int, max_pages: Optional[int] = None, max_consecutive_empty: int = 3
    ) -> Generator[dict, None, None]:
        """
        Generator that paginates through all legislation for a given year.

        Yields individual legislation records (raw dicts from the API).

        Args:
            year: Year to fetch legislation for
            max_pages: Maximum number of pages to fetch (None = unlimited)
            max_consecutive_empty: How many consecutive empty responses before giving up
                                   (helps recover from temporary server issues)
        """
        page = 1
        results_per_page = 50  # API supports up to 50 per page
        consecutive_empty = 0
        total_yielded = 0

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            logger.info(f"Fetching year {year}, page {page}...")
            records = self._search(page=page, results_per_page=results_per_page, year=str(year))

            if not records:
                consecutive_empty += 1
                logger.warning(
                    f"Empty response for year {year}, page {page} "
                    f"(consecutive empty: {consecutive_empty}/{max_consecutive_empty})"
                )

                if consecutive_empty >= max_consecutive_empty:
                    logger.info(
                        f"Stopping pagination for year {year} after {max_consecutive_empty} "
                        f"consecutive empty responses. Total yielded: {total_yielded}"
                    )
                    return

                # Wait a bit longer before retrying on empty response
                wait_time = 5 * consecutive_empty
                logger.info(f"Waiting {wait_time}s before trying next page...")
                time.sleep(wait_time)
                page += 1
                continue

            # Reset consecutive empty counter on success
            consecutive_empty = 0

            for record in records:
                yield record
                total_yielded += 1

            # If we got fewer than requested, we've reached the end
            if len(records) < results_per_page:
                logger.info(f"Fetched all records for year {year} ({page} pages, {total_yielded} total)")
                return

            page += 1

    # -- Text cleaning -------------------------------------------------------

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove HTML tags if any
        text = re.sub(r"<[^>]+>", " ", text)

        # Normalize whitespace
        text = re.sub(r"\s+", " ", text)

        return text.strip()

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation records from the Romanian Legislative Portal.

        WARNING: Full fetch is 150K+ records. Use sample mode for testing.
        """
        for year in range(START_YEAR, CURRENT_YEAR + 1):
            logger.info(f"Fetching legislation for year {year}")
            for record in self._paginate_year(year):
                yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield records from recent years (no date filter in API, so we fetch all).
        """
        # API doesn't support date-based filtering, so we fetch recent years
        current_year = datetime.now().year
        for year in range(current_year - 1, current_year + 1):
            logger.info(f"Fetching updates for year {year}")
            for record in self._paginate_year(year):
                yield record

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw SOAP API response into standard schema.

        The API already returns full text in the 'Text' field.
        """
        # Extract fields from raw response
        tip_act = raw.get("TipAct", "").strip()
        numar = raw.get("Numar", "").strip()
        titlu = raw.get("Titlu", "").strip()
        text = raw.get("Text", "")
        data_vigoare = raw.get("DataVigoare", "").strip()
        emitent = raw.get("Emitent", "").strip()
        publicatie = raw.get("Publicatie", "").strip()
        link_html = raw.get("LinkHtml", "").strip()

        # Clean the text (it's already full text from the API)
        full_text = self._clean_text(text)

        # Generate unique ID
        # Format: TipAct_Numar_Year (e.g., LEGE_123_2024)
        year = ""
        if data_vigoare:
            try:
                year = data_vigoare.split("-")[0]
            except (ValueError, IndexError):
                pass
        doc_id = f"{tip_act}_{numar}_{year}".replace(" ", "_")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "RO/LegislationDatabase",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": titlu,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": data_vigoare,
            "url": link_html,
            # Source-specific fields
            "tip_act": tip_act,
            "numar": numar,
            "emitent": emitent,
            "publicatie": publicatie,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Romanian Legislative Portal API...")

        # Get token
        try:
            token = self._get_token()
            print(f"  Token obtained: {token[:30]}...")
        except Exception as e:
            print(f"  FAILED to get token: {e}")
            return

        # Search for a few records from current year
        print(f"  Searching for legislation from {CURRENT_YEAR}...")
        records = self._search(page=1, results_per_page=5, year=str(CURRENT_YEAR))
        print(f"  Found {len(records)} records")

        if records:
            # Show first record summary
            r = records[0]
            print(f"\n  Sample record:")
            print(f"    Type: {r.get('TipAct', 'N/A')}")
            print(f"    Number: {r.get('Numar', 'N/A')}")
            print(f"    Title: {r.get('Titlu', 'N/A')[:80]}...")
            text = r.get("Text", "")
            print(f"    Text length: {len(text)} chars")
            if text:
                print(f"    Text preview: {text[:200]}...")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = RoLegislationScraper()

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
