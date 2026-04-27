#!/usr/bin/env python3
"""
German Federal Network Agency (Bundesnetzagentur) Decision Database Fetcher

Official regulatory decisions from bundesnetzagentur.de
https://www.bundesnetzagentur.de/DE/Beschlusskammern/BDB/start.html

This fetcher retrieves regulatory decisions from the Beschlussdatenbank using:
- HTML search/listing pages for document discovery (19,217+ decisions)
- PDF downloads for full text extraction

Chambers:
- GBK: Grand Ruling Chamber for Energy (Große Beschlusskammer Energie)
- BK1: Regulatory proceedings for postal services
- BK2: Access to the subscriber number
- BK3: Telecommunications market regulation
- BK4: Energy (electricity/gas) network access
- BK5: Interconnection, access to facilities
- BK6: Metering, energy data communication
- BK7: Market transparency, capacity allocation
- BK8: Determination of revenue caps (electricity)
- BK9: Determination of revenue caps (gas)
- BK10: Market rules, balancing energy
- BK11: Incentive regulation

Data is public (amtliche Werke) but commercial use requires BNetzA consent.
"""

import html
import io
import json
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List
from urllib.parse import urljoin, urlencode

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www.bundesnetzagentur.de"
SEARCH_URL = f"{BASE_URL}/SiteGlobals/Forms/Suche/BDB/Suche_BeschlussDB_Formular.html"

# Chamber codes - mapping from chamber ID to name
CHAMBERS = {
    "000099": "GBK",
    "000001": "BK1",
    "000002": "BK2",
    "000003": "BK3",
    "000004": "BK4",
    "000005": "BK5",
    "000006": "BK6",
    "000007": "BK7",
    "000008": "BK8",
    "000009": "BK9",
    "000010": "BK10",
    "000011": "BK11",
}


class BNetzAFetcher:
    """Fetcher for German Federal Network Agency (Bundesnetzagentur) Decisions"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'de-DE,de;q=0.9,en;q=0.8',
        })

    def _get_search_page(self, chamber: str = None, year_prefix: str = None, page: int = 1) -> str:
        """
        Fetch a search results page.

        Args:
            chamber: Chamber code (e.g., "000009" for BK9)
            year_prefix: Reference pattern (e.g., "BK9-25" for 2025)
            page: Page number (1-indexed)
        """
        # Build URL directly - the server expects unencoded parameters
        params = ["nn=698550"]

        if chamber:
            params.append(f"chamber={chamber}")
        if year_prefix:
            params.append(f"reference={year_prefix}")

        # Pagination - pages after 1 use gtp parameter
        if page > 1:
            params.append(f"gtp=698550_list%3D{page}")

        url = f"{SEARCH_URL}?{'&'.join(params)}"

        logger.info(f"Fetching search page: chamber={chamber}, year={year_prefix}, page={page}")
        response = self.session.get(url, timeout=60)
        response.raise_for_status()
        return response.text

    def _parse_search_results(self, html_content: str) -> List[Dict[str, Any]]:
        """Parse search results page and extract decision metadata"""
        soup = BeautifulSoup(html_content, 'html.parser')
        decisions = []

        # Find all tables with class 'bdb' - the data table is 'textualData links bdb'
        # or just look for all links with class 'beschlussFile'
        tables = soup.find_all('table', class_='bdb')

        results_table = None
        for table in tables:
            # Look for tables that contain beschlussFile links (these are the data tables)
            if table.find('a', class_='beschlussFile'):
                results_table = table
                break

        if not results_table:
            logger.warning("No results table found with beschlussFile links")
            return decisions

        # Find all data rows (skip header row)
        rows = results_table.find_all('tr')

        for row in rows:
            # Skip header rows (those with th or headline class)
            if row.find('th'):
                continue
            row_class = row.get('class', [])
            if row_class and 'headline' in ' '.join(row_class):
                continue

            cells = row.find_all('td')
            if len(cells) < 7:
                continue

            # Check if this row has actual data (odd/even class cells)
            first_cell = cells[0]
            if not first_cell.get('class'):
                continue

            decision = {}

            # Column 0: Chamber (e.g., "BK9")
            chamber_cell = cells[0].get_text(strip=True)
            decision['chamber'] = chamber_cell

            # Column 1: Aktenzeichen (case number) with PDF link
            az_cell = cells[1]
            link = az_cell.find('a', class_='beschlussFile')
            if link:
                # Extract aktenzeichen from link text
                az_text = link.get_text(strip=True)
                # Remove &ZeroWidthSpace; entities
                az_text = az_text.replace('\u200b', '').replace('&ZeroWidthSpace;', '')
                decision['aktenzeichen'] = az_text

                # Extract PDF URL
                pdf_href = link.get('href', '')
                if pdf_href:
                    decision['pdf_url'] = urljoin(BASE_URL, pdf_href)

            # Column 2: Decision date
            date_cell = cells[2].get_text(strip=True)
            decision['decision_date_raw'] = date_cell

            # Column 3: Subject/Content
            subject_cell = cells[3].get_text(strip=True)
            # Decode HTML entities and clean up abbreviations
            subject_cell = html.unescape(subject_cell)
            decision['subject'] = subject_cell

            # Column 4: Affected party (Betroffener)
            affected_cell = cells[4].get_text(strip=True)
            decision['affected_party'] = affected_cell if affected_cell else None

            # Column 5: Applicant (Antragsteller)
            applicant_cell = cells[5].get_text(strip=True)
            decision['applicant'] = applicant_cell if applicant_cell else None

            # Column 6: Court reference (Gerichtliches Aktenzeichen)
            court_ref_cell = cells[6].get_text(strip=True)
            if court_ref_cell and court_ref_cell != "derzeit kein Eintrag":
                decision['court_reference'] = court_ref_cell

            # Only include if we have aktenzeichen and PDF URL
            if decision.get('aktenzeichen') and decision.get('pdf_url'):
                decisions.append(decision)

        return decisions

    def _get_total_results(self, html_content: str) -> int:
        """Extract total result count from search page"""
        soup = BeautifulSoup(html_content, 'html.parser')

        # Look for pagination info
        # Format: "Seite 1 von X" or result count
        pagination = soup.find('div', class_='pagination')
        if pagination:
            text = pagination.get_text()
            match = re.search(r'von\s+(\d+)', text)
            if match:
                return int(match.group(1)) * 25  # 25 per page

        # Count rows in current page
        results = self._parse_search_results(html_content)
        return len(results)

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="DE/BNetzA",
            source_id="",
            pdf_bytes=pdf_content,
            table="doctrine",
        ) or ""

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text"""
        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace but preserve paragraph breaks
        text = re.sub(r'\n{4,}', '\n\n\n', text)
        text = re.sub(r' {2,}', ' ', text)

        # Clean up hyphenation at line breaks
        text = re.sub(r'-\s*\n\s*', '', text)

        # Remove soft hyphens
        text = text.replace('\u00ad', '')

        return text.strip()

    def _parse_german_date(self, date_str: str) -> Optional[str]:
        """Parse German date format (DD.MM.YYYY) to ISO 8601"""
        if not date_str:
            return None

        # Clean up the date string
        date_str = date_str.strip()

        # Try DD.MM.YYYY format
        try:
            dt = datetime.strptime(date_str, '%d.%m.%Y')
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

        # Try ISO format
        try:
            dt = datetime.fromisoformat(date_str)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass

        return None

    def _fetch_decision_pdf(self, decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch PDF and extract full text for a decision"""
        pdf_url = decision.get('pdf_url')
        if not pdf_url:
            return None

        try:
            logger.info(f"Downloading PDF: {decision.get('aktenzeichen', 'Unknown')}")
            response = self.session.get(pdf_url, timeout=90)
            response.raise_for_status()

            # Verify it's a PDF
            if not response.content.startswith(b'%PDF'):
                logger.warning(f"Response is not a PDF for {pdf_url}")
                return None

            # Extract text
            text = self._extract_pdf_text(response.content)
            if not text:
                logger.warning(f"Could not extract text from PDF: {pdf_url}")
                return None

            text = self._clean_text(text)

            # Minimum text requirement
            if len(text) < 100:
                logger.warning(f"Text too short ({len(text)} chars): {decision.get('aktenzeichen')}")
                return None

            # Return enriched decision
            return {
                **decision,
                'text': text,
                'pdf_size': len(response.content),
            }

        except requests.RequestException as e:
            logger.error(f"Error fetching PDF {pdf_url}: {e}")
            return None

    def fetch_all(self, limit: int = None, chambers: List[str] = None, years: List[str] = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch BNetzA decisions with full text.

        Args:
            limit: Maximum number of decisions to fetch (None for all)
            chambers: List of chamber codes to fetch (None for all)
            years: List of year prefixes to fetch (e.g., ["BK?-25", "BK?-24"])

        Yields:
            Raw decision dictionaries with full text
        """
        count = 0

        # Default to recent years if not specified
        if years is None:
            years = ["BK?-25", "BK?-24", "BK?-23"]

        # Default chambers - use most active ones
        if chambers is None:
            chambers = [None]  # None means all chambers

        for chamber in chambers:
            for year_pattern in years:
                if limit and count >= limit:
                    return

                logger.info(f"Fetching decisions: chamber={chamber}, year={year_pattern}")

                page = 1
                empty_pages = 0

                while True:
                    if limit and count >= limit:
                        return

                    try:
                        html_content = self._get_search_page(chamber=chamber, year_prefix=year_pattern, page=page)
                        decisions = self._parse_search_results(html_content)

                        if not decisions:
                            empty_pages += 1
                            if empty_pages >= 2:
                                break
                            page += 1
                            time.sleep(1.0)
                            continue

                        empty_pages = 0
                        logger.info(f"Page {page}: Found {len(decisions)} decisions")

                        for decision in decisions:
                            if limit and count >= limit:
                                return

                            # Fetch PDF and extract text
                            enriched = self._fetch_decision_pdf(decision)

                            if enriched and enriched.get('text'):
                                yield enriched
                                count += 1
                                logger.info(f"[{count}] {decision['aktenzeichen']} ({len(enriched['text']):,} chars)")

                            time.sleep(1.5)  # Rate limiting

                        page += 1
                        time.sleep(1.0)

                    except requests.RequestException as e:
                        logger.error(f"Error fetching page {page}: {e}")
                        break

        logger.info(f"Fetched {count} decisions with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent decisions (current year only)"""
        current_year = datetime.now().year
        year_suffix = str(current_year)[2:]
        yield from self.fetch_all(years=[f"BK?-{year_suffix}"], limit=100)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize decision to standard schema"""
        # Use aktenzeichen as document ID
        aktenzeichen = raw_doc.get('aktenzeichen', '')
        doc_id = aktenzeichen.replace(' ', '-').replace('/', '-')

        # Parse decision date
        decision_date = self._parse_german_date(raw_doc.get('decision_date_raw'))

        # Build title from subject and aktenzeichen
        subject = raw_doc.get('subject', '')
        title = f"{aktenzeichen}: {subject}" if subject else aktenzeichen

        # Build URL - use PDF URL directly
        url = raw_doc.get('pdf_url', '')

        return {
            '_id': doc_id,
            '_source': 'DE/BNetzA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': decision_date,
            'url': url,
            'aktenzeichen': aktenzeichen,
            'chamber': raw_doc.get('chamber', ''),
            'subject': subject,
            'affected_party': raw_doc.get('affected_party'),
            'applicant': raw_doc.get('applicant'),
            'court_reference': raw_doc.get('court_reference'),
            'pdf_size': raw_doc.get('pdf_size'),
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = BNetzAFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        is_sample = '--sample' in sys.argv
        target_count = 12 if is_sample else None

        # For sample, fetch from most recent year only
        years_to_scan = ["BK?-25", "BK?-24"] if is_sample else None

        for raw_doc in fetcher.fetch_all(limit=target_count, years=years_to_scan):
            if target_count and sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']} - text too short ({text_len} chars)")
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_').replace(' ', '_')[:80]
            filename = f"record_{sample_count:04d}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}]: {normalized['aktenzeichen']} ({text_len:,} chars)")
            sample_count += 1

        # Also save all_samples.json
        all_samples = []
        for f in sorted(sample_dir.glob('record_*.json')):
            with open(f, 'r', encoding='utf-8') as fp:
                all_samples.append(json.load(fp))

        with open(sample_dir / 'all_samples.json', 'w', encoding='utf-8') as f:
            json.dump(all_samples, f, indent=2, ensure_ascii=False)

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob('record_*.json'))
        total_chars = 0
        for f in files:
            with open(f, 'r', encoding='utf-8') as fp:
                data = json.load(fp)
                total_chars += len(data.get('text', ''))

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")

    else:
        # Test mode
        fetcher = BNetzAFetcher()
        print("Testing BNetzA fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Decision {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Aktenzeichen: {normalized.get('aktenzeichen', 'N/A')}")
            print(f"Chamber: {normalized.get('chamber', 'N/A')}")
            print(f"Date: {normalized['date']}")
            print(f"Subject: {normalized.get('subject', 'N/A')[:100]}")
            print(f"Affected: {normalized.get('affected_party', 'N/A')}")
            print(f"Applicant: {normalized.get('applicant', 'N/A')}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:500]}...")
            count += 1


if __name__ == '__main__':
    main()
