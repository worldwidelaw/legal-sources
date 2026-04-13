#!/usr/bin/env python3
"""
EU DG Competition Data Fetcher
European Commission competition decisions - antitrust, mergers, state aid

Uses the official DG COMP Open Data (data.europa.eu). Data is provided as
daily-updated JSON files containing case metadata and links to decision PDFs.
"""

import io
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Generator, Optional

import requests

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from common.base_scraper import BaseScraper

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logger = logging.getLogger("legal-data-hunter")

S3_BASE = "https://compcases-open-data-portal-files-prod.s3.eu-west-1.amazonaws.com"
DATASETS = {
    'antitrust': f"{S3_BASE}/case-data-AT.json",
    'mergers': f"{S3_BASE}/case-data-M.json",
}
CASES_PORTAL = "https://competition-cases.ec.europa.eu"

SOURCE_DIR = Path(__file__).parent


class DGCompScraper(BaseScraper):
    def __init__(self):
        super().__init__(SOURCE_DIR)
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)'
        })

    def _make_request(self, url: str, timeout: int = 120) -> Optional[requests.Response]:
        """Make HTTP request with retry logic."""
        for attempt in range(3):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/3): {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return None

    def _download_dataset(self, dataset_url: str) -> Optional[Dict]:
        """Download and parse a JSON dataset."""
        logger.info(f"Downloading dataset from {dataset_url}...")
        response = self._make_request(dataset_url)
        if response:
            try:
                return response.json()
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
        return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="EU/DGComp",
            source_id="",
            pdf_bytes=pdf_content,
            table="doctrine",
        ) or ""

    def _fetch_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text."""
        try:
            response = self._make_request(pdf_url, timeout=60)
            if response and response.content:
                content_type = response.headers.get('Content-Type', '')
                if 'pdf' in content_type or pdf_url.endswith('.pdf'):
                    return self._extract_text_from_pdf(response.content)
        except Exception as e:
            logger.warning(f"Failed to fetch PDF from {pdf_url}: {e}")
        return ""

    def _get_best_pdf_url(self, case: Dict) -> Optional[str]:
        """Find the best PDF URL from a case."""
        decisions = case.get('decisions', [])
        for decision in decisions:
            for da in decision.get('decisionAttachments', []):
                links = da.get('metadata', {}).get('attachmentLink', [])
                if links:
                    return links[0]

        for att in case.get('caseAttachments', []):
            links = att.get('metadata', {}).get('attachmentLink', [])
            if links:
                return links[0]

        return None

    def _parse_case(self, case_number: str, case: Dict, dataset_name: str) -> Dict:
        """Parse a single case into raw record format."""
        metadata = case.get('metadata', {})
        title = metadata.get('caseTitle', ['Unknown'])[0]

        decisions = case.get('decisions', [])
        decision_date = None
        decision_types = []

        for d in decisions:
            d_meta = d.get('metadata', {})
            dates = d_meta.get('decisionAdoptionDate', [])
            if dates:
                decision_date = dates[0]
            for t in d_meta.get('decisionTypes', []):
                if isinstance(t, str):
                    try:
                        dt = json.loads(t)
                        decision_types.append(dt.get('label', ''))
                    except (json.JSONDecodeError, TypeError):
                        pass

        if not decision_date:
            init_dates = metadata.get('caseInitiationDate', [])
            if init_dates:
                decision_date = init_dates[0]

        pdf_url = self._get_best_pdf_url(case)

        sectors = []
        for s in metadata.get('caseSectors', []):
            if isinstance(s, str):
                try:
                    sector = json.loads(s)
                    sectors.append(sector.get('label', ''))
                except (json.JSONDecodeError, TypeError):
                    pass

        companies = metadata.get('caseCompanies', [])

        legal_basis = []
        for lb in metadata.get('caseLegalBasis', []):
            if isinstance(lb, str):
                try:
                    basis = json.loads(lb)
                    legal_basis.append(basis.get('label', ''))
                except (json.JSONDecodeError, TypeError):
                    pass

        case_type = metadata.get('caseCartel', ['N/A'])[0]
        instrument = metadata.get('caseInstrument', ['N/A'])[0]

        return {
            'case_number': case_number,
            'case_type': case_type,
            'instrument': instrument,
            'title': title,
            'decision_date': decision_date,
            'decision_types': decision_types,
            'sectors': sectors,
            'companies': companies,
            'legal_basis': legal_basis,
            'pdf_url': pdf_url,
            'url': f"{CASES_PORTAL}/cases/{case_number}",
            'dataset': dataset_name,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all competition cases with PDF text extraction."""
        for dataset_name, dataset_url in DATASETS.items():
            data = self._download_dataset(dataset_url)
            if not data:
                logger.warning(f"Failed to download {dataset_name} dataset")
                continue

            logger.info(f"Processing {len(data)} cases from {dataset_name} dataset...")

            for case_number, case in data.items():
                raw = self._parse_case(case_number, case, dataset_name)

                # Fetch PDF text if available
                pdf_url = raw.get('pdf_url')
                if pdf_url:
                    text = self._fetch_pdf_text(pdf_url)
                    raw['text'] = text
                    if text:
                        logger.debug(f"Extracted {len(text)} chars from {case_number}")
                    time.sleep(0.5)  # Rate limit PDF fetches
                else:
                    raw['text'] = ""

                yield raw

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch cases updated since a given date."""
        for raw in self.fetch_all():
            if raw.get('decision_date'):
                try:
                    case_date = datetime.fromisoformat(raw['decision_date'][:10])
                    case_date = case_date.replace(tzinfo=timezone.utc)
                    if case_date >= since:
                        yield raw
                except (ValueError, TypeError):
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> dict:
        """Normalize case to standard schema."""
        date_str = raw.get('decision_date')
        if date_str:
            try:
                date_str = date_str[:10]  # YYYY-MM-DD
            except (TypeError, IndexError):
                date_str = None

        return {
            '_id': f"EU-DGCOMP-{raw['case_number']}",
            '_source': 'EU/DGComp',
            '_type': 'doctrine',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'case_number': raw['case_number'],
            'case_type': raw.get('case_type', 'N/A'),
            'instrument': raw.get('instrument', 'N/A'),
            'title': raw['title'],
            'text': raw.get('text', ''),
            'date': date_str,
            'url': raw['url'],
            'pdf_url': raw.get('pdf_url'),
            'sectors': raw.get('sectors', []),
            'companies': raw.get('companies', []),
            'legal_basis': raw.get('legal_basis', []),
            'decision_types': raw.get('decision_types', []),
        }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    scraper = DGCompScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {json.dumps(stats, indent=2)}")

    elif command == "bootstrap-fast":
        workers = 3
        batch_size = 100
        for i, arg in enumerate(sys.argv):
            if arg == "--workers" and i + 1 < len(sys.argv):
                workers = int(sys.argv[i + 1])
            if arg == "--batch-size" and i + 1 < len(sys.argv):
                batch_size = int(sys.argv[i + 1])
        stats = scraper.bootstrap_fast(max_workers=workers, batch_size=batch_size)
        print(f"\nBootstrap-fast complete: {json.dumps(stats, indent=2)}")

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {json.dumps(stats, indent=2)}")

    elif command == "test":
        print("Testing DG COMP fetcher...")
        print(f"  PyMuPDF available: {HAS_PYMUPDF}")
        print(f"  pdfminer available: {HAS_PDFMINER}")
        print()

        count = 0
        for raw in scraper.fetch_all():
            normalized = scraper.normalize(raw)
            print(f"--- Case {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"PDF: {normalized.get('pdf_url', 'N/A')}")
            print(f"Text: {len(normalized.get('text', ''))} chars")
            print()
            count += 1
            if count >= 5:
                break
        print(f"Successfully listed {count} cases")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
