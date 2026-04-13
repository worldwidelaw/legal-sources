#!/usr/bin/env python3
"""
SM/Legisammarino -- San Marino Official Legislation Data Fetcher

Fetches San Marino legislation from the Official Bulletin (Bollettino Ufficiale).

Strategy:
  - Iterates through monthly bulletin archives (2012-present, when electronic format began).
  - Parses HTML table of contents to extract document links and metadata.
  - Downloads individual PDFs and extracts text via pdfplumber.
  - Covers: Constitutional Laws, Qualified Laws, Ordinary Laws, Decrees, Regulations.

Endpoints:
  - Bulletin search: https://www.bollettinoufficiale.sm/on-line/home/parte-ufficiale/ricerca.html
  - Document API: https://www.bollettinoufficiale.sm/on-line/RicercaBU?operation=getDocBU&id={hash}

Data types:
  - Legge Costituzionale (Constitutional Law)
  - Legge Qualificata (Qualified Law)
  - Legge Ordinaria (Ordinary Law)
  - Decreto Delegato, Decreto Legge, Decreto Consiliare, Decreto Reggenziale (Decrees)
  - Regolamento (Regulations)

License: San Marino Open Government Data
Language: Italian

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
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


# PDF extraction
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SM.legisammarino")

# Base URLs
BASE_URL = "https://www.bollettinoufficiale.sm"
SEARCH_PATH = "/on-line/home/parte-ufficiale/ricerca.html"

# Document types to fetch (in Italian as they appear in the HTML)
DOC_TYPES = {
    "Legge Costituzionale": "constitutional_law",
    "Legge Qualificata": "qualified_law",
    "Legge Ordinaria": "ordinary_law",
    "Decreto Delegato": "delegated_decree",
    "Decreto Legge": "decree_law",
    "Decreto Consiliare": "council_decree",
    "Decreto Reggenziale": "regency_decree",
    "Regolamento": "regulation",
    "Delibera di Ratifica": "ratification_deliberation",
    "Accordo": "agreement",
    "Contratto": "contract",
    "Comunicazione": "communication",
    "Errata corrige": "errata_corrige",
    "Dichiarazione di decadenza": "expiration_declaration",
}

# Year range for bulletin search (electronic format started 2012)
START_YEAR = 2012
CURRENT_YEAR = datetime.now().year


class SanMarinoLegislationScraper(BaseScraper):
    """
    Scraper for SM/Legisammarino -- San Marino Official Legislation.
    Country: SM
    URL: https://www.bollettinoufficiale.sm

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/pdf",
                "Accept-Language": "it,en",
            },
            timeout=60,
        )

    def _fetch_bulletin_page(self, year: int, month: int) -> str:
        """
        Fetch the monthly bulletin table of contents.

        Returns HTML content of the bulletin page.
        """
        self.rate_limiter.wait()

        params = {
            "showfeed": "yes",
            "P0_operation": "getBollettino",
            "P0_anno": str(year),
            "P0_mese": f"{month:02d}",
            "ricerca": "yes",
            "risultati": "yes",
        }

        try:
            resp = self.client.get(SEARCH_PATH, params=params)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch bulletin for {year}/{month:02d}: {e}")
            return ""

    def _parse_bulletin_page(self, html_content: str, year: int, month: int) -> List[Dict[str, Any]]:
        """
        Parse the bulletin HTML page to extract document metadata and links.

        Returns list of document dictionaries with:
          - doc_id: Hash ID from the URL
          - doc_type: Document type (e.g., "Legge Ordinaria")
          - date: Publication date
          - number: Document number
          - title: Document title/description
          - url: Full URL to the document
        """
        documents = []

        # Find all document links in the bulletin
        # Pattern: href="...RicercaBU?...operation=getDocBU&amp;id={hash}">Type Date n. Number
        pattern = r'href="([^"]*operation=getDocBU[^"]*id=([a-f0-9]+))"[^>]*>([^<]+)</a>.*?<span>&#8220;([^<]+)&#8221;'

        for match in re.finditer(pattern, html_content, re.DOTALL):
            url_encoded = match.group(1)
            doc_id = match.group(2)
            type_date_num = match.group(3).strip()
            title = match.group(4).strip()

            # Decode HTML entities
            url_encoded = html.unescape(url_encoded)
            title = html.unescape(title)

            # Parse type, date, and number from the link text
            # Example: "Decreto Delegato 03/01/2024 n. 1"
            parts = type_date_num.split()
            doc_type = ""
            date = ""
            number = ""

            for i, part in enumerate(parts):
                # Check if this is a date (DD/MM/YYYY pattern)
                if re.match(r'\d{2}/\d{2}/\d{4}', part):
                    date = part
                    doc_type = " ".join(parts[:i])
                    # Look for number after "n." or "n"
                    for j in range(i + 1, len(parts)):
                        if parts[j].lower() in ["n.", "n"]:
                            if j + 1 < len(parts):
                                number = parts[j + 1]
                            break
                    break

            # Build full URL
            full_url = f"{BASE_URL}/on-line/RicercaBU?operation=getDocBU&id={doc_id}"

            documents.append({
                "doc_id": doc_id,
                "doc_type": doc_type.strip(),
                "doc_type_normalized": DOC_TYPES.get(doc_type.strip(), "other"),
                "date": date,
                "number": number,
                "title": title,
                "url": full_url,
                "year": year,
                "month": month,
            })

        return documents

    def _download_and_extract_pdf(self, doc_id: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="SM/Legisammarino",
            source_id="",
            pdf_bytes=doc_id,
            table="legislation",
        ) or ""

    def _iterate_bulletins(self, sample_mode: bool = False, sample_size: int = 12) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate through all bulletins from START_YEAR to current year.

        Yields raw document records with metadata and full text.
        """
        count = 0

        # Start from most recent and work backwards
        for year in range(CURRENT_YEAR, START_YEAR - 1, -1):
            for month in range(12, 0, -1):
                # Skip future months
                if year == CURRENT_YEAR and month > datetime.now().month:
                    continue

                logger.info(f"Fetching bulletin {year}/{month:02d}...")

                bulletin_html = self._fetch_bulletin_page(year, month)
                if not bulletin_html:
                    continue

                # Check for "Nessun documento" (no documents)
                if "Nessun documento presente in questa sezione" in bulletin_html and "Parte Ufficiale" not in bulletin_html:
                    logger.debug(f"No documents in {year}/{month:02d}")
                    continue

                documents = self._parse_bulletin_page(bulletin_html, year, month)
                logger.info(f"Found {len(documents)} documents in {year}/{month:02d}")

                for doc in documents:
                    # Download and extract full text
                    logger.info(f"  Fetching: {doc['doc_type']} {doc['date']} n.{doc['number']}")
                    full_text = self._download_and_extract_pdf(doc['doc_id'])

                    if full_text and len(full_text) >= 50:
                        doc['full_text'] = full_text
                        count += 1
                        yield doc

                        if sample_mode and count >= sample_size:
                            logger.info(f"Sample mode: reached {count} documents")
                            return
                    else:
                        logger.warning(f"  Skipped (insufficient text): {len(full_text) if full_text else 0} chars")

        logger.info(f"Completed iteration: {count} documents fetched")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from San Marino Official Bulletin.

        Iterates through all monthly bulletins from 2012 to present.
        """
        for doc in self._iterate_bulletins(sample_mode=False):
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given date.

        Only fetches bulletins from months >= since date.
        """
        since_year = since.year
        since_month = since.month

        for year in range(CURRENT_YEAR, since_year - 1, -1):
            start_month = 12 if year > since_year else since_month
            end_month = since_month if year == since_year else 1

            for month in range(start_month, end_month - 1, -1):
                if year == CURRENT_YEAR and month > datetime.now().month:
                    continue

                logger.info(f"Fetching bulletin {year}/{month:02d}...")

                bulletin_html = self._fetch_bulletin_page(year, month)
                if not bulletin_html:
                    continue

                documents = self._parse_bulletin_page(bulletin_html, year, month)

                for doc in documents:
                    # Check document date is after 'since'
                    date_str = doc.get('date', '')
                    if date_str:
                        try:
                            # Parse DD/MM/YYYY
                            doc_date = datetime.strptime(date_str, '%d/%m/%Y')
                            if doc_date.replace(tzinfo=timezone.utc) < since:
                                continue
                        except:
                            pass

                    full_text = self._download_and_extract_pdf(doc['doc_id'])
                    if full_text and len(full_text) >= 50:
                        doc['full_text'] = full_text
                        yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("doc_id", "")
        doc_type = raw.get("doc_type", "")
        date_str = raw.get("date", "")
        number = raw.get("number", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")

        # Build a unique identifier
        # Format: SM-{type}_{number}_{year}
        year = raw.get("year", "")
        if date_str:
            try:
                year = datetime.strptime(date_str, '%d/%m/%Y').year
            except:
                pass

        # Normalize type for ID
        type_abbrev = {
            "Legge Costituzionale": "LC",
            "Legge Qualificata": "LQ",
            "Legge Ordinaria": "L",
            "Decreto Delegato": "DD",
            "Decreto Legge": "DL",
            "Decreto Consiliare": "DC",
            "Decreto Reggenziale": "DR",
            "Regolamento": "R",
            "Delibera di Ratifica": "RAT",
            "Accordo": "ACC",
            "Contratto": "CON",
            "Comunicazione": "COM",
            "Errata corrige": "EC",
            "Dichiarazione di decadenza": "DEC",
        }.get(doc_type, doc_type[:3].upper() if doc_type else "DOC")

        identifier = f"SM-{type_abbrev}-{number}-{year}" if number else f"SM-{doc_id}"

        # Convert date to ISO format
        iso_date = ""
        if date_str:
            try:
                dt = datetime.strptime(date_str, '%d/%m/%Y')
                iso_date = dt.strftime('%Y-%m-%d')
            except:
                iso_date = date_str

        # Build full title
        full_title = f"{doc_type}"
        if number:
            full_title += f" n. {number}"
        if date_str:
            full_title += f" del {date_str}"
        if title:
            full_title += f" - {title}"

        return {
            # Required base fields
            "_id": identifier,
            "_source": "SM/Legisammarino",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": full_title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": iso_date,
            "url": raw.get("url", ""),
            # Additional metadata
            "doc_id": doc_id,
            "document_type": doc_type,
            "document_type_normalized": raw.get("doc_type_normalized", "other"),
            "number": number,
            "description": title,
            "year": year,
            "month": raw.get("month", ""),
            "language": "it",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing San Marino bollettinoufficiale.sm endpoints...")

        if not PDF_SUPPORT:
            print("\nERROR: pdfplumber not installed. Run: pip install pdfplumber")
            return

        # Test bulletin fetch
        print("\n1. Testing bulletin page (January 2024)...")
        try:
            bulletin_html = self._fetch_bulletin_page(2024, 1)
            print(f"   HTML length: {len(bulletin_html)} chars")

            documents = self._parse_bulletin_page(bulletin_html, 2024, 1)
            print(f"   Documents found: {len(documents)}")

            if documents:
                print(f"   First doc: {documents[0].get('doc_type')} {documents[0].get('date')} n.{documents[0].get('number')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test PDF download
        print("\n2. Testing PDF download and extraction...")
        try:
            # Use a known document ID from the bulletin
            if documents:
                doc = documents[0]
                print(f"   Downloading: {doc['doc_id']}")
                full_text = self._download_and_extract_pdf(doc['doc_id'])
                print(f"   Text length: {len(full_text)} characters")
                if full_text:
                    print(f"   Sample: {full_text[:200]}...")
            else:
                print("   No documents to test")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test sample fetch
        print("\n3. Testing document iteration (first 3)...")
        count = 0
        for doc in self._iterate_bulletins(sample_mode=True, sample_size=3):
            count += 1
            print(f"   [{count}] {doc.get('doc_type', 'N/A')} {doc.get('date', '')} n.{doc.get('number', '')}")
            print(f"       Title: {doc.get('title', 'N/A')[:60]}...")
            print(f"       Text: {len(doc.get('full_text', ''))} chars")

        print(f"\nTest complete! Found {count} documents.")


def main():
    scraper = SanMarinoLegislationScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12  # Default to 12 for validation
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
