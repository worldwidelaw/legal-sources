#!/usr/bin/env python3
"""
LT/SupremeCourt -- Lithuanian Supreme Court of Cassation Case Law

Fetches case law from Lithuania's Supreme Court (Lietuvos Aukščiausiasis Teismas)
via the LITEKO public decisions database.

Strategy:
  1. Download monthly CSV files from liteko.teismai.lt/csv/
  2. Filter for "Lietuvos Aukščiausiasis Teismas" court
  3. Fetch full text from tekstas.aspx endpoint using document ID
  4. Extract text content from embedded HTML

Data source:
  - CSV files: https://liteko.teismai.lt/csv/viesi_sprendimai_YYYYMM.csv
  - Full text: https://liteko.teismai.lt/viesasprendimupaieska/tekstas.aspx?id=<doc_id>
  - Dataset info: https://data.gov.lt/datasets/1938/

Coverage:
  - Cases from 2010 onwards
  - Monthly CSV files with case metadata
  - Full text available via separate endpoint

License: CC BY 4.0

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent months)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import csv
import html
import json
import logging
import io
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LT.supremecourt")

# Endpoints
CSV_BASE_URL = "https://liteko.teismai.lt/csv"
TEXT_BASE_URL = "https://liteko.teismai.lt/viesasprendimupaieska"

# data.gov.lt fallback for older files (pre-2023) that are cached there
# NOTE: liteko.teismai.lt is Cloudflare-protected and may block datacenter IPs
DATA_GOV_LT_BASE = "https://data.gov.lt"
DATA_GOV_LT_CSV_MAP = {
    "2022": "/media/filer_public/34/99/34993500-c6f1-4871-a7a2-443effa27713/viesi_sprendimai_2022.csv",
    "2021": "/media/filer_public/91/98/91981580-9767-4ed3-a9b4-87c7f9f14ee4/viesi_sprendimai_2021.csv",
    "2020": "/media/filer_public/31/24/31241741-bb52-4374-9fbb-fe5d3add2e28/viesi_sprendimai_2020.csv",
    "2019": "/media/filer_public/86/c0/86c00af9-9bf9-4bbc-b7f7-302f4098035e/viesi_sprendimai_2019.csv",
    "2018": "/media/filer_public/3e/30/3e3079ea-d0dc-425b-875c-a13f369e2715/viesi_sprendimai_2018.csv",
    "2017": "/media/filer_public/d3/e7/d3e734e7-4128-4d27-83c9-4325ef88786f/viesi_sprendimai_2017.csv",
    "2016": "/media/filer_public/9f/5e/9f5ef3fa-3795-4d7d-aa10-6c4f929030a0/viesi_sprendimai_2016.csv",
    "2015": "/media/filer_public/f6/1f/f61fcde0-4c9c-4fc8-b867-77d8cd09c7fc/viesi_sprendimai_2015.csv",
    "2014": "/media/filer_public/de/4b/de4b8749-cd97-413b-91c2-8209daaad651/viesi_sprendimai_2014.csv",
    "2013": "/media/filer_public/88/c9/88c913e7-cb24-4fd0-af3f-6513e60ff547/viesi_sprendimai_2013.csv",
    "2012": "/media/filer_public/1e/21/1e2198ec-329d-43c5-b981-597cda76372e/viesi_sprendimai_2012.csv",
    "2011": "/media/filer_public/07/c8/07c8b30d-b527-4816-b4a0-08ee2dca50c7/viesi_sprendimai_2011.csv",
    "2010": "/media/filer_public/b3/6c/b36c3e85-26de-426e-ae3b-3f9dd04db683/viesi_sprendimai_2010.csv",
}

# Court name filter (Supreme Court of Cassation)
SUPREME_COURT_NAME = "Lietuvos Aukščiausiasis Teismas"

# CSV columns (semicolon-delimited)
# Teismas;ID;Bylos_Numeris;Bylos_Eilės_Numeris;Procesinis_Numeris;Gauta;Baigta;
# Teisėjo_Kodas;Teisėjas;Kolegija;Instancija;Tipas;Rezultatas;Nagrinėjimo_Trukmė;
# Šabloninė_Byla;Naudotos_ES_Teisės_Normos;Dokumento_ID;Nuoroda;Dokumento_Data;Šalys;Kategorijos


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for LT/SupremeCourt -- Lithuanian Supreme Court of Cassation.
    Country: LT
    URL: https://liteko.teismai.lt/

    Data types: case_law
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "lt,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
        })
        self._session_initialized = False

    def _get_available_years(self) -> List[str]:
        """
        Get list of available years for data.gov.lt cached files.
        Returns years 2010-2022 which have cached copies on data.gov.lt.
        """
        return list(DATA_GOV_LT_CSV_MAP.keys())

    def _get_available_months(self) -> List[str]:
        """
        Get list of available months (YYYYMM format).
        Returns last 24 months of available data.

        Note: CSV files are published with a lag, so we check availability.
        NOTE: Recent months (2023+) may be blocked by Cloudflare on datacenter IPs.
              Use fetch_all_from_data_gov() for reliable access to historical data.
        """
        months = []

        # Generate months from January 2025 backwards for 24 months
        # (January 2025 is the most recent verified available file)
        from datetime import date

        year = 2025
        month = 1

        for i in range(24):
            months.append(f"{year:04d}{month:02d}")
            month -= 1
            if month == 0:
                month = 12
                year -= 1

        return months

    def _download_csv(self, year_month: str) -> List[Dict[str, str]]:
        """
        Download and parse a monthly CSV file.

        Args:
            year_month: YYYYMM format

        Returns:
            List of dicts with case metadata
        """
        url = f"{CSV_BASE_URL}/viesi_sprendimai_{year_month}.csv"

        try:
            self.rate_limiter.wait()
            logger.info(f"Downloading CSV for {year_month}...")

            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()

            # Parse CSV (semicolon-delimited)
            content = resp.content.decode('utf-8-sig')  # Handle BOM
            reader = csv.DictReader(io.StringIO(content), delimiter=';')

            records = []
            for row in reader:
                # Filter for Supreme Court only
                court = row.get('Teismas', '').strip()
                if court == SUPREME_COURT_NAME:
                    records.append(row)

            logger.info(f"  Found {len(records)} Supreme Court cases")
            return records

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"CSV not found for {year_month}")
            elif e.response.status_code == 403:
                logger.warning(f"CSV blocked (Cloudflare) for {year_month}")
            else:
                logger.error(f"HTTP error downloading CSV: {e}")
            return []
        except Exception as e:
            logger.error(f"Error downloading CSV for {year_month}: {e}")
            return []

    def _download_yearly_csv_from_data_gov(self, year: str) -> List[Dict[str, str]]:
        """
        Download and parse a yearly CSV file from data.gov.lt cache.

        This is a fallback for when liteko.teismai.lt blocks datacenter IPs.

        Args:
            year: YYYY format (2010-2022)

        Returns:
            List of dicts with case metadata
        """
        if year not in DATA_GOV_LT_CSV_MAP:
            logger.warning(f"No data.gov.lt cache for year {year}")
            return []

        url = f"{DATA_GOV_LT_BASE}{DATA_GOV_LT_CSV_MAP[year]}"

        try:
            self.rate_limiter.wait()
            logger.info(f"Downloading yearly CSV for {year} from data.gov.lt...")

            resp = self.session.get(url, timeout=180)
            resp.raise_for_status()

            # Parse CSV (semicolon-delimited)
            content = resp.content.decode('utf-8-sig')  # Handle BOM
            reader = csv.DictReader(io.StringIO(content), delimiter=';')

            records = []
            for row in reader:
                # Filter for Supreme Court only
                court = row.get('Teismas', '').strip()
                if court == SUPREME_COURT_NAME:
                    records.append(row)

            logger.info(f"  Found {len(records)} Supreme Court cases for {year}")
            return records

        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error downloading CSV from data.gov.lt: {e}")
            return []
        except Exception as e:
            logger.error(f"Error downloading CSV for {year}: {e}")
            return []

    def _init_session(self):
        """
        Initialize session by visiting the search page first.
        This helps establish cookies and session state.

        NOTE: This won't bypass Cloudflare datacenter IP blocks.
        If running from VPS and getting 403 errors, you need either:
        - Residential IP proxy
        - Browser automation (Playwright/Puppeteer)
        """
        if self._session_initialized:
            return

        try:
            logger.info("Initializing session via search page...")
            self.rate_limiter.wait()
            resp = self.session.get(f"{TEXT_BASE_URL}/", timeout=30)
            if resp.status_code == 200:
                self._session_initialized = True
                logger.info("Session initialized successfully")
            else:
                logger.warning(f"Session init returned {resp.status_code}")
        except Exception as e:
            logger.warning(f"Session init failed: {e}")

    def _fetch_full_text(self, doc_id: str) -> str:
        """
        Fetch full text for a document from tekstas.aspx endpoint.

        Args:
            doc_id: Document GUID

        Returns:
            Cleaned full text content

        NOTE: This endpoint may return 403 from datacenter/VPS IPs due to
        Cloudflare protection. Works fine from residential IPs.
        """
        # Ensure session is initialized
        self._init_session()

        url = f"{TEXT_BASE_URL}/tekstas.aspx?id={doc_id}"

        try:
            self.rate_limiter.wait()

            # Add Referer header for better session handling
            headers = {"Referer": f"{TEXT_BASE_URL}/paieska.aspx"}
            resp = self.session.get(url, timeout=60, headers=headers)
            resp.raise_for_status()

            content = resp.text

            # Extract the embedded HTML from the txthtml span
            start_marker = 'id="ctl00_ContentPlaceHolder1_txthtml" class="plain_text">'
            start_idx = content.find(start_marker)

            if start_idx < 0:
                logger.warning(f"Text container not found for {doc_id}")
                return ""

            start_idx += len(start_marker)

            # Find the end of the embedded HTML
            end_marker = '</body></html></span>'
            end_idx = content.find(end_marker, start_idx)

            if end_idx < 0:
                # Try alternative end marker
                end_idx = content.find('</span>', start_idx)
                if end_idx < 0:
                    logger.warning(f"End marker not found for {doc_id}")
                    return ""
            else:
                end_idx += len('</body></html>')

            html_content = content[start_idx:end_idx]

            # Clean HTML to plain text
            text = self._html_to_text(html_content)

            return text

        except Exception as e:
            logger.error(f"Error fetching text for {doc_id}: {e}")
            return ""

    def _html_to_text(self, html_content: str) -> str:
        """
        Convert HTML content to clean plain text.
        """
        if not html_content:
            return ""

        # Decode HTML entities
        text = html.unescape(html_content)

        # Replace common block elements with newlines
        text = re.sub(r'</p>\s*<p[^>]*>', '\n\n', text)
        text = re.sub(r'</div>\s*<div[^>]*>', '\n\n', text)
        text = re.sub(r'<br\s*/?>', '\n', text)
        text = re.sub(r'</h[1-6]>', '\n\n', text)

        # Remove all remaining HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Clean up whitespace
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&#xa0;', ' ', text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)

        # Clean up lines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(line for line in lines if line)

        return text.strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """
        Parse date string to ISO 8601 format.
        """
        if not date_str:
            return None

        # Handle various formats
        date_str = date_str.strip()

        try:
            # Format: "2025-01-16 00:00:00.000"
            if ' ' in date_str:
                date_str = date_str.split()[0]

            # Validate ISO date
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return date_str

        except ValueError:
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Supreme Court cases from available CSV files.

        Strategy:
        1. Try recent months from liteko.teismai.lt (may be blocked by Cloudflare)
        2. Fall back to yearly files from data.gov.lt for 2010-2022

        NOTE: If liteko.teismai.lt returns 403, only historical data (2010-2022) will be fetched.
        """
        total_fetched = 0

        # First try recent months from liteko (may be blocked)
        months = self._get_available_months()
        liteko_blocked = False

        for month in months:
            records = self._download_csv(month)
            if not records:
                # Check if it's a Cloudflare block (403)
                liteko_blocked = True
                continue

            for record in records:
                total_fetched += 1
                yield record

        # If liteko was blocked or returned nothing, use data.gov.lt fallback
        if liteko_blocked or total_fetched == 0:
            logger.info("Falling back to data.gov.lt cached yearly files...")
            years = self._get_available_years()

            for year in sorted(years, reverse=True):
                records = self._download_yearly_csv_from_data_gov(year)

                for record in records:
                    total_fetched += 1
                    yield record

        logger.info(f"Total Supreme Court cases fetched: {total_fetched}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield cases updated since the given date.
        Only fetches recent months.
        """
        # Get months since 'since' date
        now = datetime.now()
        months = []

        current = now
        while current >= since:
            months.append(current.strftime("%Y%m"))
            current = current - timedelta(days=30)

        for month in months[:6]:  # Limit to 6 months
            records = self._download_csv(month)

            for record in records:
                # Check document date
                doc_date = self._parse_date(record.get('Dokumento_Data', ''))
                if doc_date:
                    try:
                        record_date = datetime.strptime(doc_date, '%Y-%m-%d')
                        record_date = record_date.replace(tzinfo=timezone.utc)

                        if record_date >= since:
                            yield record
                    except ValueError:
                        yield record
                else:
                    yield record

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw case data into standard schema.

        CRITICAL: Fetches and includes full text in the 'text' field.
        """
        # Extract document ID for full text fetch
        doc_id = raw.get('Dokumento_ID', '')

        # Fetch full text
        full_text = ""
        if doc_id:
            full_text = self._fetch_full_text(doc_id)
            if full_text:
                logger.info(f"  Fetched {len(full_text)} chars for {doc_id[:8]}...")
            else:
                logger.warning(f"  No text found for {doc_id[:8]}...")

        # Build case number as ID
        case_number = raw.get('Bylos_Numeris', '')
        unique_id = f"{case_number}_{doc_id}" if doc_id else case_number

        # Parse dates
        doc_date = self._parse_date(raw.get('Dokumento_Data', ''))
        received_date = self._parse_date(raw.get('Gauta', ''))
        completed_date = self._parse_date(raw.get('Baigta', ''))

        # Build URL
        url = raw.get('Nuoroda', '')
        if not url and doc_id:
            url = f"{TEXT_BASE_URL}/tekstas.aspx?id={doc_id}"

        # Extract result/outcome
        result = raw.get('Rezultatas', '')

        # Extract case type
        case_type = raw.get('Tipas', '')
        instance = raw.get('Instancija', '')

        # Categories
        categories = raw.get('Kategorijos', '')

        # Parties
        parties = raw.get('Šalys', '')
        if parties:
            # Clean up HTML from parties
            parties = re.sub(r'<br\s*/?>', '; ', parties)
            parties = re.sub(r'<[^>]+>', '', parties)

        # Build title from available info
        title_parts = []
        if case_type:
            title_parts.append(case_type)
        if case_number:
            title_parts.append(f"Nr. {case_number}")
        if result:
            title_parts.append(f"- {result}")
        title = ' '.join(title_parts) or f"Case {case_number}"

        return {
            # Required base fields
            "_id": unique_id,
            "_source": "LT/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": doc_date,
            "url": url,
            # Case-specific fields
            "case_number": case_number,
            "doc_id": doc_id,
            "court": SUPREME_COURT_NAME,
            "case_type": case_type,
            "instance": instance,
            "result": result,
            "judges": raw.get('Teisėjas', ''),
            "panel": raw.get('Kolegija', ''),
            "parties": parties,
            "categories": categories,
            "received_date": received_date,
            "completed_date": completed_date,
            "duration_days": raw.get('Nagrinėjimo_Trukmė', ''),
            "eu_law_used": raw.get('Naudotos_ES_Teisės_Normos', ''),
            "language": "lt",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LT/SupremeCourt endpoints...")

        # Test CSV download with known available month
        print("\n1. Testing CSV download from liteko.teismai.lt...")
        test_month = "202501"  # January 2025 (verified available)

        try:
            records = self._download_csv(test_month)
            if records:
                print(f"   Found {len(records)} Supreme Court cases for {test_month}")
                sample = records[0]
                print(f"   Sample case: {sample.get('Bylos_Numeris', 'N/A')}")
                print(f"   Document ID: {sample.get('Dokumento_ID', 'N/A')}")

                # Test full text fetch
                print("\n2. Testing full text fetch...")
                doc_id = sample.get('Dokumento_ID', '')
                if doc_id:
                    text = self._fetch_full_text(doc_id)
                    print(f"   Full text length: {len(text)} characters")
                    if text:
                        print(f"   Text preview: {text[:300]}...")
                else:
                    print("   No document ID found")
            else:
                print(f"   liteko.teismai.lt returned no records (likely Cloudflare blocked)")
                print("   This is expected from datacenter IPs")

        except Exception as e:
            print(f"   ERROR: {e}")

        # Test data.gov.lt fallback
        print("\n3. Testing data.gov.lt fallback (yearly files)...")
        try:
            records = self._download_yearly_csv_from_data_gov("2022")
            if records:
                print(f"   Found {len(records)} Supreme Court cases for 2022")
                sample = records[0]
                print(f"   Sample case: {sample.get('Bylos_Numeris', 'N/A')}")
                print(f"   Document ID: {sample.get('Dokumento_ID', 'N/A')}")
            else:
                print("   No records found")

        except Exception as e:
            print(f"   ERROR: {e}")

        # Test older year
        print("\n4. Testing data.gov.lt for 2020...")
        try:
            records = self._download_yearly_csv_from_data_gov("2020")
            print(f"   Found {len(records)} Supreme Court cases for 2020")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")
        print("\nNOTE: If liteko.teismai.lt returns 403 (Cloudflare), use data.gov.lt")
        print("      fallback for historical data (2010-2022).")


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
