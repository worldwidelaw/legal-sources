#!/usr/bin/env python3
"""
German Luftfahrt-Bundesamt (LBA) Airworthiness Directives Fetcher

Official data from the German Federal Aviation Authority
https://www2.lba.de/LTAs/

This fetcher downloads Airworthiness Directives (Lufttüchtigkeitsanweisungen - LTAs)
which are legally binding regulatory decisions mandating safety actions for aircraft
operators.

Data structure:
- PDFs available at: https://www2.lba.de/ltadocs/{LTA-Nr}.pdf
- LTA numbers follow the pattern: YYYY-NNN or YYYY-NNNRX (for revisions)
- Database contains LTAs from 1990 to present

No authentication required. Data is public domain.
"""

import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List, Tuple

import requests
import PyPDF2

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://www2.lba.de"
PDF_BASE_URL = f"{BASE_URL}/ltadocs"
LTAS_URL = f"{BASE_URL}/LTAs/"

# LTA numbering: years 1990-current, numbers typically 001-300 per year
START_YEAR = 1990
MAX_LTA_PER_YEAR = 350  # Safety margin


class LBAFetcher:
    """Fetcher for German Airworthiness Directives from LBA"""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)'
        })

    def _check_lta_exists(self, lta_nr: str) -> bool:
        """Check if an LTA PDF exists at the expected URL"""
        url = f"{PDF_BASE_URL}/{lta_nr}.pdf"
        try:
            response = self.session.head(url, timeout=10)
            return response.status_code == 200
        except Exception:
            return False

    def _find_max_lta_for_year(self, year: int) -> int:
        """Binary search to find the highest LTA number for a given year"""
        low, high = 1, MAX_LTA_PER_YEAR
        max_found = 0

        while low <= high:
            mid = (low + high) // 2
            lta_nr = f"{year}-{mid:03d}"

            if self._check_lta_exists(lta_nr):
                max_found = mid
                low = mid + 1
            else:
                high = mid - 1

            time.sleep(0.2)  # Rate limit HEAD requests

        return max_found

    def _get_lta_list(self, start_year: int = None, end_year: int = None) -> List[str]:
        """
        Get list of all LTA numbers that exist.

        This uses a combination of:
        1. Checking sequential numbers for each year
        2. Trying common revision patterns (R1, R2, etc.)
        """
        if start_year is None:
            start_year = START_YEAR
        if end_year is None:
            end_year = datetime.now().year

        all_ltas = []

        for year in range(start_year, end_year + 1):
            logger.info(f"Finding LTAs for year {year}...")

            # Find max LTA number for this year
            max_num = self._find_max_lta_for_year(year)
            logger.info(f"Year {year}: found up to LTA-{max_num:03d}")

            # Generate list of LTAs for this year
            for num in range(1, max_num + 1):
                base_lta = f"{year}-{num:03d}"
                all_ltas.append(base_lta)

                # Check for revisions (R1 through R20)
                for rev in range(1, 21):
                    rev_lta = f"{base_lta}R{rev}"
                    if self._check_lta_exists(rev_lta):
                        all_ltas.append(rev_lta)
                        time.sleep(0.2)
                    else:
                        break  # Stop if revision doesn't exist

            time.sleep(0.5)  # Rate limit between years

        logger.info(f"Found total of {len(all_ltas)} LTAs")
        return all_ltas

    def _download_pdf(self, lta_nr: str) -> Optional[bytes]:
        """Download an LTA PDF"""
        url = f"{PDF_BASE_URL}/{lta_nr}.pdf"
        try:
            response = self.session.get(url, timeout=60)
            if response.status_code == 200:
                return response.content
            return None
        except Exception as e:
            logger.error(f"Error downloading {lta_nr}: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF using PyPDF2"""
        try:
            pdf_reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
            text_parts = []

            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)

            return '\n\n'.join(text_parts)
        except Exception as e:
            logger.error(f"Error extracting PDF text: {e}")
            return ""

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text"""
        # Remove excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = re.sub(r'^\s+', '', text, flags=re.MULTILINE)

        return text.strip()

    def _parse_lta_metadata(self, text: str, lta_nr: str) -> Dict[str, Any]:
        """Extract metadata from LTA text"""
        metadata = {
            'lta_nr': lta_nr,
            'title': '',
            'aircraft_type': '',
            'manufacturer': '',
            'model': '',
            'publication_date': '',
            'easa_ad': '',
            'affected_serial_numbers': '',
        }

        # Extract publication date (format: DD.MM.YYYY)
        date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})\s*$', text[:500], re.MULTILINE)
        if date_match:
            metadata['publication_date'] = date_match.group(1)

        # Extract aircraft type
        type_match = re.search(r'Art des Luftfahrtgerätes:\s*(.+?)(?:\n|$)', text)
        if type_match:
            metadata['aircraft_type'] = type_match.group(1).strip()

        # Extract manufacturer (TC Holder)
        mfr_match = re.search(r'Inhaber der Musterzulassung:\s*(.+?)(?:\n|$)', text)
        if mfr_match:
            metadata['manufacturer'] = mfr_match.group(1).strip()

        # Extract model
        model_match = re.search(r'Muster:\s*(.+?)(?:\n|$)', text)
        if model_match:
            metadata['model'] = model_match.group(1).strip()

        # Extract EASA AD reference
        easa_match = re.search(r'(EASA\s*AD\s*[\d-]+|TC\s*AD\s*[A-Z]{2}-[\d-]+)', text)
        if easa_match:
            metadata['easa_ad'] = easa_match.group(1).strip()

        # Extract subject/title from "Betrifft:" section
        subject_match = re.search(r'Betrifft\s*[:\s]*\n?\s*(.+?)(?:\n\s*(?:Anmerkungen|Gemäß)|$)', text, re.DOTALL)
        if subject_match:
            title = subject_match.group(1).strip()
            # Clean up multi-line titles
            title = re.sub(r'\s+', ' ', title)
            metadata['title'] = title[:500]  # Limit length

        if not metadata['title']:
            # Fallback: use LTA number and model
            metadata['title'] = f"LTA {lta_nr}"
            if metadata['model']:
                metadata['title'] += f" - {metadata['model']}"

        return metadata

    def fetch_all(self, limit: int = None, start_year: int = None) -> Iterator[Dict[str, Any]]:
        """
        Fetch all LBA Airworthiness Directives with full text.

        Args:
            limit: Maximum number of LTAs to fetch (None for all)
            start_year: Start from this year (default: 1990)

        Yields:
            Raw document dictionaries with full text
        """
        # For a full fetch, we need to discover all LTA numbers
        # This is time-consuming, so for sample mode we use a simpler approach
        if limit and limit <= 100:
            # For small samples, just check recent LTAs
            yield from self._fetch_recent_ltas(limit)
        else:
            # Full fetch - discover all LTAs
            lta_list = self._get_lta_list(start_year=start_year)

            count = 0
            for lta_nr in lta_list:
                if limit and count >= limit:
                    break

                logger.info(f"[{count+1}/{len(lta_list) if not limit else limit}] Fetching LTA {lta_nr}...")

                pdf_content = self._download_pdf(lta_nr)
                if not pdf_content:
                    continue

                text = self._extract_text_from_pdf(pdf_content)
                if not text or len(text) < 100:
                    logger.warning(f"LTA {lta_nr}: insufficient text extracted")
                    continue

                text = self._clean_text(text)
                metadata = self._parse_lta_metadata(text, lta_nr)

                yield {
                    **metadata,
                    'text': text,
                    'pdf_url': f"{PDF_BASE_URL}/{lta_nr}.pdf"
                }

                count += 1
                time.sleep(1.0)  # Rate limit

            logger.info(f"Fetched {count} LTAs with full text")

    def _fetch_recent_ltas(self, limit: int) -> Iterator[Dict[str, Any]]:
        """Fetch recent LTAs for sample mode (faster than full discovery)"""
        current_year = datetime.now().year
        count = 0

        # Start from current year and work backwards
        for year in range(current_year, current_year - 3, -1):
            for num in range(300, 0, -1):  # Start from high numbers (most recent)
                if count >= limit:
                    return

                lta_nr = f"{year}-{num:03d}"

                if not self._check_lta_exists(lta_nr):
                    time.sleep(0.1)
                    continue

                logger.info(f"[{count+1}/{limit}] Fetching LTA {lta_nr}...")

                pdf_content = self._download_pdf(lta_nr)
                if not pdf_content:
                    continue

                text = self._extract_text_from_pdf(pdf_content)
                if not text or len(text) < 100:
                    logger.warning(f"LTA {lta_nr}: insufficient text extracted")
                    continue

                text = self._clean_text(text)
                metadata = self._parse_lta_metadata(text, lta_nr)

                yield {
                    **metadata,
                    'text': text,
                    'pdf_url': f"{PDF_BASE_URL}/{lta_nr}.pdf"
                }

                count += 1
                time.sleep(1.0)  # Rate limit

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch LTAs from a specific date onwards"""
        # Only fetch from the year of the 'since' date onwards
        yield from self.fetch_all(start_year=since.year)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        lta_nr = raw_doc.get('lta_nr', '')

        # Parse publication date
        date_str = raw_doc.get('publication_date', '')
        date = None
        if date_str:
            try:
                # Format: DD.MM.YYYY
                dt = datetime.strptime(date_str, '%d.%m.%Y')
                date = dt.strftime('%Y-%m-%d')
            except ValueError:
                pass

        # If no date found, try to extract from LTA number
        if not date and lta_nr:
            year_match = re.match(r'(\d{4})-', lta_nr)
            if year_match:
                date = f"{year_match.group(1)}-01-01"

        return {
            '_id': f"LTA-{lta_nr}",
            '_source': 'DE/LBA',
            '_type': 'doctrine',
            '_fetched_at': datetime.now().isoformat(),
            'title': raw_doc.get('title', f"LTA {lta_nr}"),
            'lta_number': lta_nr,
            'text': raw_doc.get('text', ''),
            'date': date,
            'aircraft_type': raw_doc.get('aircraft_type', ''),
            'manufacturer': raw_doc.get('manufacturer', ''),
            'model': raw_doc.get('model', ''),
            'easa_ad': raw_doc.get('easa_ad', ''),
            'url': raw_doc.get('pdf_url', f"{PDF_BASE_URL}/{lta_nr}.pdf"),
            'language': 'de'
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = LBAFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 10 if '--sample' in sys.argv else 100

        for raw_doc in fetcher.fetch_all(limit=target_count + 5):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get('text', ''))

            if text_len < 100:
                continue

            # Save to sample directory
            doc_id = normalized['_id'].replace('/', '_')
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved [{sample_count+1}/{target_count}]: {normalized['lta_number']} - {normalized['title'][:50]} ({text_len} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob('*.json'))
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
        fetcher = LBAFetcher()
        print("Testing LBA Airworthiness Directives fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"LTA Number: {normalized['lta_number']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Date: {normalized['date']}")
            print(f"Aircraft: {normalized['aircraft_type']} - {normalized['manufacturer']} {normalized['model']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == '__main__':
    main()
