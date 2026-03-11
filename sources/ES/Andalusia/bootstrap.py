#!/usr/bin/env python3
"""
ES/Andalusia -- Andalusian Regional Legislation Data Fetcher

Fetches regional legislation from Andalusia via the BOJA REST API.

Strategy:
  - Use REST API at datos.juntadeandalucia.es/api/v0/boja/all for JSON data.
  - API returns full text in 'body' and 'bodyNoHtml' fields.
  - Data available by year (2015-present).

Endpoints:
  - JSON API: https://datos.juntadeandalucia.es/api/v0/boja/all?year={YYYY}&format=json

Data:
  - Legislation types: Ordenes, Decretos, Resoluciones, Leyes, etc.
  - 2,000+ records per year
  - License: CC BY 4.0
  - Language: Spanish (es)

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
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests is required. Install with: pip install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.andalusia")

# API URLs
API_BASE = "https://datos.juntadeandalucia.es/api/v0/boja/all"

# Year range - BOJA data starts from 2015
START_YEAR = 2015


class AndalusiaScraper(BaseScraper):
    """
    Scraper for ES/Andalusia -- Andalusian Regional Legislation.
    Country: ES
    URL: https://datos.juntadeandalucia.es

    Data types: legislation
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace."""
        if not text:
            return ""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode HTML entities
        text = html.unescape(text)
        # Clean whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def _fetch_year(self, year: int) -> list:
        """
        Fetch all records for a given year.

        Returns list of records or empty list on failure.
        """
        url = f"{API_BASE}?year={year}&format=json"

        try:
            self.rate_limiter.wait()
            logger.info(f"Fetching BOJA records for year {year}...")
            resp = self.session.get(url, timeout=120, allow_redirects=True)
            resp.raise_for_status()
            records = resp.json()
            logger.info(f"Found {len(records)} records for {year}")
            return records
        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching year {year}")
            return []
        except Exception as e:
            logger.error(f"API error for year {year}: {e}")
            return []

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all regional legislation from Andalusia.

        Iterates through all available years (2015-present).
        """
        current_year = datetime.now().year

        for year in range(current_year, START_YEAR - 1, -1):
            records = self._fetch_year(year)

            for record in records:
                doc_id = record.get("id", "")
                if not doc_id:
                    continue

                # Check for full text
                body = record.get("bodyNoHtml", "") or record.get("body", "")
                if not body:
                    logger.debug(f"No body for {doc_id}")
                    continue

                # Clean the text if it's HTML
                if "<" in body and ">" in body:
                    body = self._clean_html(body)

                if len(body) < 100:
                    logger.debug(f"Text too short for {doc_id} ({len(body)} chars)")
                    continue

                record["full_text"] = body
                yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Since the API only supports year-based queries, we fetch recent years
        and filter by date.
        """
        current_year = datetime.now().year
        since_year = since.year

        for year in range(current_year, since_year - 1, -1):
            records = self._fetch_year(year)

            for record in records:
                doc_id = record.get("id", "")
                if not doc_id:
                    continue

                # Parse date from record
                date_str = record.get("dateUTC", "") or record.get("dateDispositionUTC", "")
                if date_str:
                    try:
                        doc_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        if doc_date < since.replace(tzinfo=timezone.utc):
                            continue
                    except (ValueError, TypeError):
                        pass

                body = record.get("bodyNoHtml", "") or record.get("body", "")
                if not body:
                    continue

                if "<" in body and ">" in body:
                    body = self._clean_html(body)

                if len(body) < 100:
                    continue

                record["full_text"] = body
                yield record

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = str(raw.get("id", ""))
        summary = raw.get("summaryNoHtml", "") or raw.get("summary", "")
        if summary and "<" in summary:
            summary = self._clean_html(summary)

        full_text = raw.get("full_text", "")

        # Parse dates
        date_disp = raw.get("dateDispositionUTC", "")
        date_pub = raw.get("dateUTC", "")
        if date_disp:
            date_disp = date_disp[:10]
        if date_pub:
            date_pub = date_pub[:10]

        # Get gazette info
        year = raw.get("year", "")
        number = raw.get("number", "")
        disp_number = raw.get("dispositionNumber", "")

        # Document type
        doc_type = raw.get("type", "")  # e.g., "Órdenes", "Decretos"

        # Organization
        org = raw.get("organisation", "") or ""
        if org and "<" in org:
            org = self._clean_html(org)

        # Section info
        section = raw.get("sectionN1", "") or raw.get("titleSec", "")

        # PDF URL
        pdf_url = ""
        if raw.get("hasPdf"):
            # Construct PDF URL from the BOJA pattern
            pdf_url = f"https://www.juntadeandalucia.es/boja/{year}/{number:03d}/d{disp_number}.pdf" if number else ""

        # Internal version
        version = raw.get("version", "")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "ES/Andalusia",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": summary,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_disp or date_pub,
            "url": f"https://www.juntadeandalucia.es/eboja/buscador/disposition/{doc_id}" if doc_id else "",
            # Additional metadata
            "document_type": doc_type,
            "year": year,
            "gazette_number": number,
            "disposition_number": disp_number,
            "publication_date": date_pub,
            "organization": org,
            "section": section,
            "has_pdf": raw.get("hasPdf", False),
            "pdf_url": pdf_url,
            "version": version,
            "language": "es",
            "region": "Andalucía",
            "country": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Andalusia BOJA API...")

        # Test API with current year
        current_year = datetime.now().year
        print(f"\n1. Testing API for year {current_year}...")
        try:
            records = self._fetch_year(current_year)
            print(f"   Found {len(records)} records for {current_year}")
            if records:
                r = records[0]
                print(f"   ID: {r.get('id', 'N/A')}")
                summary = r.get('summaryNoHtml', 'N/A')
                print(f"   Summary: {summary[:70]}..." if len(summary) > 70 else f"   Summary: {summary}")
                print(f"   Type: {r.get('type', 'N/A')}")
                print(f"   Date: {r.get('date', 'N/A')}")
                body = r.get('bodyNoHtml', '')
                print(f"   Body length: {len(body)} characters")
                if body:
                    clean_body = self._clean_html(body) if "<" in body else body
                    print(f"   Body sample: {clean_body[:150]}...")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test historical year
        print("\n2. Testing historical year (2020)...")
        try:
            records_2020 = self._fetch_year(2020)
            print(f"   Found {len(records_2020)} records for 2020")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Summary
        print("\n3. Summary...")
        print(f"   Years available: {START_YEAR}-{current_year}")
        print(f"   Estimated total: ~{len(records) * (current_year - START_YEAR + 1)} records")

        print("\nTest complete!")


def main():
    scraper = AndalusiaScraper()

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
