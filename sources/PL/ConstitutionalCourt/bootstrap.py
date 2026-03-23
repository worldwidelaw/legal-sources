#!/usr/bin/env python3
"""
PL/ConstitutionalCourt -- Polish Constitutional Court Data Fetcher

Fetches case law from the Polish Constitutional Court (Trybunał Konstytucyjny)
via the SAOS API (System Analizy Orzeczeń Sądowych).

Strategy:
  - Search API: GET /api/search/judgments?courtType=CONSTITUTIONAL_TRIBUNAL
  - Detail API: GET /api/judgments/{id} for full text and metadata
  - Paginate through results (max 100 per page)

API Documentation:
  - Base URL: https://www.saos.org.pl/api
  - Search: /search/judgments with courtType=CONSTITUTIONAL_TRIBUNAL
  - Detail: /judgments/{id} returns full text in textContent field

Data Coverage:
  - Constitutional Court decisions from 1985 to present
  - Includes rulings (wyroki), decisions (postanowienia), resolutions (uchwały)
  - Approximately 8,000+ judgments available

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent judgments)
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.ConstitutionalCourt")

# API configuration
BASE_URL = "https://www.saos.org.pl/api"
COURT_TYPE = "CONSTITUTIONAL_TRIBUNAL"
PAGE_SIZE = 100
MAX_PAGES = 100  # ~10,000 judgments max


class ConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for PL/ConstitutionalCourt -- Polish Constitutional Court.
    Country: PL
    URL: https://trybunal.gov.pl

    Data types: case_law
    Auth: none (Open Data via SAOS)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    def _api_get(self, endpoint: str, params: dict = None, timeout: int = 60) -> Optional[dict]:
        """Make GET request to SAOS API endpoint."""
        url = f"{BASE_URL}{endpoint}"
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"API request failed for {endpoint}: {e}")
            return None

    def _search_judgments(self, page: int = 0, date_from: str = None, date_to: str = None) -> List[Dict[str, Any]]:
        """
        Search for Constitutional Court judgments.

        Returns list of judgment summaries.
        """
        params = {
            "courtType": COURT_TYPE,
            "pageSize": PAGE_SIZE,
            "pageNumber": page,
            "sortingField": "JUDGMENT_DATE",
            "sortingDirection": "DESC",
        }
        if date_from:
            params["judgmentDateFrom"] = date_from
        if date_to:
            params["judgmentDateTo"] = date_to

        data = self._api_get("/search/judgments", params=params)
        if data and "items" in data:
            return data["items"]
        return []

    def _get_judgment_details(self, judgment_id: int) -> Optional[Dict[str, Any]]:
        """
        Get full details and text for a specific judgment.

        Returns dict with full metadata and textContent.
        """
        data = self._api_get(f"/judgments/{judgment_id}")
        if data and "data" in data:
            return data["data"]
        return None

    def _clean_text(self, html_text: str) -> str:
        """Clean HTML/text content and return plain text."""
        if not html_text:
            return ""

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html_text)

        # Decode HTML entities
        text = html.unescape(text)

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)

        return text.strip()

    def _extract_case_number(self, court_cases: List[Dict]) -> str:
        """Extract primary case number from court cases list."""
        if court_cases and len(court_cases) > 0:
            return court_cases[0].get("caseNumber", "")
        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Constitutional Court judgments.

        Iterates through paginated search results and fetches full details.
        """
        logger.info("Starting full Constitutional Court fetch via SAOS API...")

        page = 0
        total_fetched = 0

        while page < MAX_PAGES:
            logger.info(f"Fetching page {page}...")
            items = self._search_judgments(page=page)

            if not items:
                logger.info(f"No more items at page {page}, stopping")
                break

            for item in items:
                judgment_id = item.get("id")
                if not judgment_id:
                    continue

                # Fetch full details
                details = self._get_judgment_details(judgment_id)
                if details and details.get("textContent"):
                    total_fetched += 1
                    yield details
                else:
                    logger.warning(f"Could not fetch details for judgment {judgment_id}")

            page += 1

        logger.info(f"Total judgments fetched: {total_fetched}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield judgments added/modified since the given date.

        Uses date filter in search API.
        """
        since_str = since.strftime("%Y-%m-%d")
        today_str = datetime.now().strftime("%Y-%m-%d")

        logger.info(f"Fetching updates from {since_str} to {today_str}...")

        page = 0
        while page < MAX_PAGES:
            items = self._search_judgments(page=page, date_from=since_str, date_to=today_str)

            if not items:
                break

            for item in items:
                judgment_id = item.get("id")
                if judgment_id:
                    details = self._get_judgment_details(judgment_id)
                    if details and details.get("textContent"):
                        yield details

            page += 1

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw SAOS API data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        judgment_id = raw.get("id", 0)
        court_cases = raw.get("courtCases", [])
        case_number = self._extract_case_number(court_cases)

        # Get judgment date
        judgment_date = raw.get("judgmentDate", "")

        # Get full text
        text_content = raw.get("textContent", "")
        clean_text = self._clean_text(text_content)

        # Build title from case number and type
        judgment_type = raw.get("judgmentType", "DECISION")
        type_names = {
            "DECISION": "Postanowienie",
            "SENTENCE": "Wyrok",
            "RESOLUTION": "Uchwała",
            "REASONS": "Uzasadnienie",
        }
        type_name = type_names.get(judgment_type, judgment_type)
        title = f"{type_name} {case_number}" if case_number else f"{type_name} (ID: {judgment_id})"

        # Get judges
        judges = []
        for judge in raw.get("judges", []):
            name = judge.get("name", "")
            roles = judge.get("specialRoles", [])
            if name:
                judges.append({
                    "name": name,
                    "roles": roles,
                })

        # Get source URL
        source_info = raw.get("source", {})
        source_url = source_info.get("judgmentUrl", "")
        if not source_url:
            source_url = f"https://www.saos.org.pl/judgments/{judgment_id}"

        # Get referenced regulations
        regulations = []
        for reg in raw.get("referencedRegulations", []):
            regulations.append(reg.get("text", ""))

        # Get keywords
        keywords = raw.get("keywords", [])

        return {
            # Required base fields
            "_id": f"PL/TK/{case_number}" if case_number else f"PL/TK/ID-{judgment_id}",
            "_source": "PL/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": clean_text,  # MANDATORY FULL TEXT
            "date": judgment_date,
            "url": source_url,
            # Source-specific fields
            "saos_id": judgment_id,
            "case_number": case_number,
            "all_case_numbers": [cc.get("caseNumber") for cc in court_cases if cc.get("caseNumber")],
            "judgment_type": judgment_type,
            "judges": judges,
            "keywords": keywords,
            "referenced_regulations": regulations,
            "court_type": raw.get("courtType", "CONSTITUTIONAL_TRIBUNAL"),
            "language": "pl",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing SAOS API for Polish Constitutional Court...")

        # Test search endpoint
        print("\n1. Testing search endpoint...")
        items = self._search_judgments(page=0)
        if items:
            print(f"   Found {len(items)} judgments on first page")
            print(f"   Most recent: {items[0].get('courtCases', [{}])[0].get('caseNumber', 'N/A')}")
            print(f"   Date: {items[0].get('judgmentDate', 'N/A')}")
        else:
            print("   ERROR: No judgments returned")
            return

        # Test detail endpoint
        print("\n2. Testing detail endpoint...")
        if items:
            first_id = items[0].get("id")
            details = self._get_judgment_details(first_id)
            if details:
                text = details.get("textContent", "")
                clean = self._clean_text(text)
                print(f"   Judgment ID: {first_id}")
                print(f"   Text length: {len(clean)} characters")
                print(f"   Preview: {clean[:200]}...")
            else:
                print("   ERROR: Could not fetch details")

        # Check total count estimate
        print("\n3. Estimating total count...")
        page = 50
        items_50 = self._search_judgments(page=page)
        if items_50:
            print(f"   Page {page} has {len(items_50)} items")
            print(f"   Estimated total: 5000+ judgments")
        else:
            print(f"   Page {page} empty - fewer total judgments")

        print("\nAPI test complete!")


def main():
    scraper = ConstitutionalCourtScraper()

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
