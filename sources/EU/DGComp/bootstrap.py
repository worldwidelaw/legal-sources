#!/usr/bin/env python3
"""
EU DG Competition Data Fetcher
European Commission competition decisions - antitrust, mergers, state aid

This fetcher uses the official DG COMP Open Data available from the
European Data Portal (data.europa.eu). The data is provided as daily-updated
JSON files containing case metadata and links to decision PDFs.

Data sources:
- Antitrust & Cartels (case-data-AT.json)
- Mergers (case-data-M.json)
- State Aid (case-data-SA.json) - if available
"""

import io
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

import requests

# Try to import PDF extraction library
try:
    import fitz  # PyMuPDF
    HAS_PYMUPDF = True
except ImportError:
    HAS_PYMUPDF = False

try:
    from pdfminer.high_level import extract_text as pdfminer_extract
    HAS_PDFMINER = True
except ImportError:
    HAS_PDFMINER = False

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
S3_BASE = "https://compcases-open-data-portal-files-prod.s3.eu-west-1.amazonaws.com"
DATASETS = {
    'antitrust': f"{S3_BASE}/case-data-AT.json",
    'mergers': f"{S3_BASE}/case-data-M.json",
}

# Competition cases portal (for PDF links)
CASES_PORTAL = "https://competition-cases.ec.europa.eu"


class DGCompFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'WorldWideLaw/1.0 (https://github.com/worldwidelaw/legal-sources)'
        })

    def _make_request(self, url: str, timeout: int = 120) -> Optional[requests.Response]:
        """Make HTTP request with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    return None

    def _download_dataset(self, dataset_url: str) -> Optional[Dict]:
        """Download and parse a JSON dataset"""
        logger.info(f"Downloading dataset from {dataset_url}...")
        response = self._make_request(dataset_url)
        if response:
            try:
                return response.json()
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse JSON: {e}")
        return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF bytes using available library"""
        if HAS_PYMUPDF:
            try:
                doc = fitz.open(stream=pdf_content, filetype="pdf")
                text_parts = []
                for page in doc:
                    text_parts.append(page.get_text())
                doc.close()
                text = "\n".join(text_parts)
                # Clean up excessive whitespace
                text = re.sub(r'\n{3,}', '\n\n', text)
                text = re.sub(r' {2,}', ' ', text)
                return text.strip()
            except Exception as e:
                logger.warning(f"PyMuPDF extraction failed: {e}")

        if HAS_PDFMINER:
            try:
                text = pdfminer_extract(io.BytesIO(pdf_content))
                text = re.sub(r'\n{3,}', '\n\n', text)
                text = re.sub(r' {2,}', ' ', text)
                return text.strip()
            except Exception as e:
                logger.warning(f"pdfminer extraction failed: {e}")

        logger.warning("No PDF extraction library available (install pymupdf or pdfminer.six)")
        return ""

    def _fetch_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text"""
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
        """Find the best PDF URL from a case (decision or attachment)"""
        # First check decision attachments (usually the actual decision)
        decisions = case.get('decisions', [])
        for decision in decisions:
            decision_attachments = decision.get('decisionAttachments', [])
            for da in decision_attachments:
                da_meta = da.get('metadata', {})
                links = da_meta.get('attachmentLink', [])
                if links:
                    return links[0]

        # Then check case attachments
        attachments = case.get('caseAttachments', [])
        for att in attachments:
            att_meta = att.get('metadata', {})
            links = att_meta.get('attachmentLink', [])
            if links:
                return links[0]

        return None

    def _get_case_url(self, case_number: str, case_type: str) -> str:
        """Build URL to the case on competition-cases portal"""
        instrument = 'AT' if 'AT' in case_number else 'M' if case_number.startswith('M') else ''
        return f"{CASES_PORTAL}/cases/{case_number}"

    def fetch_all(self, max_cases: int = None, datasets: List[str] = None,
                  with_pdf_only: bool = False) -> Iterator[Dict[str, Any]]:
        """
        Fetch all available competition cases with decisions.

        Args:
            max_cases: Maximum number of cases to fetch. None = unlimited.
            datasets: List of dataset types to fetch. Default is ['antitrust', 'mergers'].
            with_pdf_only: If True, only yield cases that have PDF attachments.
        """
        if datasets is None:
            datasets = ['antitrust', 'mergers']

        fetched = 0

        for dataset_name in datasets:
            if dataset_name not in DATASETS:
                logger.warning(f"Unknown dataset: {dataset_name}")
                continue

            dataset_url = DATASETS[dataset_name]
            data = self._download_dataset(dataset_url)

            if not data:
                logger.warning(f"Failed to download {dataset_name} dataset")
                continue

            logger.info(f"Processing {len(data)} cases from {dataset_name} dataset...")

            for case_number, case in data.items():
                if max_cases is not None and fetched >= max_cases:
                    logger.info(f"Reached max_cases limit ({max_cases})")
                    return

                # Get metadata
                metadata = case.get('metadata', {})
                title = metadata.get('caseTitle', ['Unknown'])[0]

                # Find decision date
                decisions = case.get('decisions', [])
                decision_date = None
                decision_types = []

                for d in decisions:
                    d_meta = d.get('metadata', {})
                    dates = d_meta.get('decisionAdoptionDate', [])
                    if dates:
                        decision_date = dates[0]
                    types = d_meta.get('decisionTypes', [])
                    for t in types:
                        if isinstance(t, str) and 'label' in t:
                            try:
                                dt = json.loads(t)
                                decision_types.append(dt.get('label', ''))
                            except:
                                pass

                # Get initiation date as fallback
                if not decision_date:
                    init_dates = metadata.get('caseInitiationDate', [])
                    if init_dates:
                        decision_date = init_dates[0]

                # Get PDF URL for full text
                pdf_url = self._get_best_pdf_url(case)

                # Get sectors
                sectors = []
                for s in metadata.get('caseSectors', []):
                    if isinstance(s, str):
                        try:
                            sector = json.loads(s)
                            sectors.append(sector.get('label', ''))
                        except:
                            pass

                # Get companies involved
                companies = metadata.get('caseCompanies', [])

                # Get legal basis
                legal_basis = []
                for lb in metadata.get('caseLegalBasis', []):
                    if isinstance(lb, str):
                        try:
                            basis = json.loads(lb)
                            legal_basis.append(basis.get('label', ''))
                        except:
                            pass

                case_type = metadata.get('caseCartel', ['N/A'])[0]
                instrument = metadata.get('caseInstrument', ['N/A'])[0]

                # Skip cases without PDF if with_pdf_only is set
                if with_pdf_only and not pdf_url:
                    continue

                yield {
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
                    'url': self._get_case_url(case_number, case_type),
                    'dataset': dataset_name,
                    'raw_metadata': metadata
                }

                fetched += 1

    def fetch_with_text(self, max_cases: int = None, datasets: List[str] = None,
                        with_pdf_only: bool = False) -> Iterator[Dict[str, Any]]:
        """
        Fetch cases and extract full text from PDFs.

        This is the main method for fetching complete records with text content.
        """
        for case in self.fetch_all(max_cases=max_cases, datasets=datasets, with_pdf_only=with_pdf_only):
            pdf_url = case.get('pdf_url')

            if pdf_url:
                logger.info(f"Fetching PDF for {case['case_number']}...")
                text = self._fetch_pdf_text(pdf_url)
                if text:
                    case['text'] = text
                    logger.info(f"Extracted {len(text)} chars from {case['case_number']}")
                else:
                    case['text'] = ""
                    logger.warning(f"No text extracted from PDF for {case['case_number']}")
                time.sleep(1)  # Rate limiting for PDF fetches
            else:
                case['text'] = ""
                logger.info(f"No PDF available for {case['case_number']}")

            yield case

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch cases updated since a given date"""
        for case in self.fetch_with_text():
            if case.get('decision_date'):
                try:
                    case_date = datetime.fromisoformat(case['decision_date'])
                    if case_date >= since:
                        yield case
                except ValueError:
                    # If date parsing fails, include it
                    yield case

    def normalize(self, raw_case: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize case to standard schema"""
        # Parse date
        date_str = raw_case.get('decision_date')
        if date_str:
            try:
                # Ensure ISO format
                date_str = date_str[:10]  # Take just YYYY-MM-DD
            except:
                date_str = None

        return {
            '_id': raw_case['case_number'],
            '_source': 'EU/DGComp',
            '_type': 'regulatory_decisions',
            '_fetched_at': datetime.now().isoformat(),
            'case_number': raw_case['case_number'],
            'case_type': raw_case.get('case_type', 'N/A'),
            'instrument': raw_case.get('instrument', 'N/A'),
            'title': raw_case['title'],
            'text': raw_case.get('text', ''),
            'date': date_str,
            'url': raw_case['url'],
            'pdf_url': raw_case.get('pdf_url'),
            'sectors': raw_case.get('sectors', []),
            'companies': raw_case.get('companies', []),
            'legal_basis': raw_case.get('legal_basis', []),
            'decision_types': raw_case.get('decision_types', [])
        }


def main():
    """Main entry point for testing"""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        # Bootstrap mode - fetch sample data
        fetcher = DGCompFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        # Check for PDF extraction capability
        if not HAS_PYMUPDF and not HAS_PDFMINER:
            logger.warning("No PDF extraction library installed.")
            logger.warning("Install with: pip install pymupdf  OR  pip install pdfminer.six")

        is_sample = '--sample' in sys.argv

        if is_sample:
            target_count = 15
            logger.info("Fetching sample documents (15 records with text)...")
        else:
            target_count = 50
            logger.info("Fetching 50 documents (use --sample for fewer)...")

        sample_count = 0
        cases_with_text = 0

        for raw_case in fetcher.fetch_with_text(max_cases=target_count * 2):  # Fetch extra to ensure we get enough with text
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_case)

            # Skip if no text and we have PDF capability
            if (HAS_PYMUPDF or HAS_PDFMINER) and len(normalized.get('text', '')) < 100:
                if normalized.get('pdf_url'):
                    logger.warning(f"Skipping {normalized['_id']} - insufficient text content")
                    continue

            # Save to sample directory
            filename = f"{normalized['_id'].replace('/', '_').replace(' ', '_')}.json"
            filepath = sample_dir / filename

            # Don't save raw_metadata to keep file sizes reasonable
            save_data = {k: v for k, v in normalized.items() if k != 'raw_metadata'}

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(save_data, f, indent=2, ensure_ascii=False)

            text_len = len(normalized.get('text', ''))
            if text_len > 100:
                cases_with_text += 1

            logger.info(f"Saved: {normalized['case_number']} - {normalized['title'][:60]}... ({text_len} chars)")
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")
        logger.info(f"Documents with substantial text: {cases_with_text}")

        # Print summary statistics
        if sample_count > 0:
            files = list(sample_dir.glob('*.json'))
            total_chars = 0
            for f in files:
                with open(f, 'r') as fp:
                    doc = json.load(fp)
                    total_chars += len(doc.get('text', ''))
            avg_chars = total_chars // len(files) if files else 0
            logger.info(f"Average text length: {avg_chars:,} characters per document")

    else:
        # Test mode - list a few cases
        fetcher = DGCompFetcher()

        print("Testing DG COMP fetcher...")
        print("Checking PDF extraction capability...")
        print(f"  PyMuPDF available: {HAS_PYMUPDF}")
        print(f"  pdfminer available: {HAS_PDFMINER}")
        print()

        count = 0
        for raw_case in fetcher.fetch_all(max_cases=5):
            normalized = fetcher.normalize(raw_case)
            print(f"--- Case {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Type: {normalized['case_type']}")
            print(f"Date: {normalized['date']}")
            print(f"PDF: {normalized.get('pdf_url', 'N/A')}")
            print(f"URL: {normalized['url']}")
            print()

            count += 1
            if count >= 5:
                break

        print(f"Successfully listed {count} cases")


if __name__ == '__main__':
    main()
