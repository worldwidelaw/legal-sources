#!/usr/bin/env python3
"""
EU/EUIPO Data Fetcher
European Union Intellectual Property Office decisions

This fetcher retrieves EUIPO decisions from the eSearch Case Law database:
- Trademark opposition decisions
- Cancellation/invalidity decisions
- Examination decisions
- Board of Appeal decisions

Data flow:
1. Query eSearch Case Law API for decision metadata
2. Download PDF/DOC documents from the decision URLs
3. Extract text content using pdfplumber or python-docx
4. Normalize to standard schema
"""

import json
import logging
import re
import time
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SEARCH_ENDPOINT = "https://euipo.europa.eu/caselaw/officesearch/json/{lang}"
BASE_URL = "https://euipo.europa.eu"

# Try to import PDF and DOCX libraries
try:
    from docx import Document
    HAS_DOCX = True
except ImportError:
    HAS_DOCX = False
    logger.warning("python-docx not installed. DOCX extraction disabled.")


class EUIPOFetcher:
    """Fetcher for EUIPO case law decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        self._session_initialized = False

    def _init_session(self):
        """Initialize session by visiting the search page to establish cookies."""
        if self._session_initialized:
            return
        try:
            logger.info("Initializing session cookies...")
            self.session.get('https://euipo.europa.eu/eSearchCLW/', timeout=30)
            self._session_initialized = True
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Failed to initialize session: {e}")

    def _make_request(self, url: str, method: str = "GET", json_data: Optional[Dict] = None,
                      headers: Optional[Dict] = None, max_retries: int = 3,
                      base_delay: float = 2.0, timeout: int = 60) -> requests.Response:
        """Make HTTP request with retry logic."""
        for attempt in range(max_retries):
            try:
                req_headers = dict(self.session.headers)
                if headers:
                    req_headers.update(headers)

                if method == "POST":
                    response = self.session.post(url, json=json_data,
                                                 headers=req_headers, timeout=timeout)
                else:
                    response = self.session.get(url, headers=req_headers, timeout=timeout)

                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.info(f"Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                else:
                    raise

    def _search_decisions(self, start: int = 0, results_per_page: int = 50,
                          lang: str = "en") -> Dict[str, Any]:
        """Query eSearch Case Law API for decisions.

        Args:
            start: Offset for pagination
            results_per_page: Number of results per page
            lang: Language code

        Returns:
            API response with results and total count
        """
        url = SEARCH_ENDPOINT.format(lang=lang)

        query = {
            "resultsPerPage": results_per_page,
            "start": start,
            "criteria": [],
            "sort": {"field": "DecisionDate", "order": "desc"}
        }

        response = self._make_request(url, method="POST", json_data=query)
        return response.json()

    def _download_document(self, pdf_url: str) -> Optional[bytes]:
        """Download document (PDF or DOC) from EUIPO server.

        Args:
            pdf_url: URL to the document

        Returns:
            Document bytes or None if failed
        """
        # Ensure session is initialized with cookies
        self._init_session()

        try:
            # Use different headers for document download
            headers = {
                'Accept': '*/*',
                'Referer': 'https://euipo.europa.eu/eSearchCLW/'
            }
            response = self._make_request(pdf_url, headers=headers, timeout=120)
            return response.content
        except Exception as e:
            logger.warning(f"Failed to download document: {e}")
            return None

    def _extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="EU/EUIPO",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def _extract_text_from_docx(self, docx_bytes: bytes) -> str:
        """Extract text from DOCX using python-docx."""
        if not HAS_DOCX:
            return ""

        try:
            with tempfile.NamedTemporaryFile(suffix='.docx', delete=False) as tmp:
                tmp.write(docx_bytes)
                tmp_path = tmp.name

            doc = Document(tmp_path)
            text_parts = []
            for para in doc.paragraphs:
                if para.text.strip():
                    text_parts.append(para.text)

            Path(tmp_path).unlink()
            return "\n\n".join(text_parts)

        except Exception as e:
            logger.warning(f"Failed to extract text from DOCX: {e}")
            return ""

    def _extract_text(self, doc_bytes: bytes, url: str) -> str:
        """Extract text from document based on URL extension.

        Args:
            doc_bytes: Document binary content
            url: Document URL (used to determine format)

        Returns:
            Extracted text content
        """
        url_lower = url.lower()

        if '.pdf' in url_lower:
            return self._extract_text_from_pdf(doc_bytes)
        elif '.doc' in url_lower:
            # .doc files from EUIPO are actually DOCX format
            return self._extract_text_from_docx(doc_bytes)
        else:
            # Try PDF first, then DOCX
            text = self._extract_text_from_pdf(doc_bytes)
            if not text:
                text = self._extract_text_from_docx(doc_bytes)
            return text

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse EUIPO date format (DD/MM/YYYY) to ISO format.

        Args:
            date_str: Date string in DD/MM/YYYY format

        Returns:
            ISO date string (YYYY-MM-DD) or None
        """
        if not date_str:
            return None

        try:
            # EUIPO uses DD/MM/YYYY format
            dt = datetime.strptime(date_str, "%d/%m/%Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            return None

    def fetch_all(self, max_docs: Optional[int] = None,
                  lang: str = "en") -> Iterator[Dict[str, Any]]:
        """Fetch all available decisions.

        Args:
            max_docs: Maximum number of documents to fetch. None means unlimited.
            lang: Language code for API queries

        Yields:
            Raw decision documents with full text
        """
        start = 0
        results_per_page = 50
        total_fetched = 0

        while max_docs is None or total_fetched < max_docs:
            logger.info(f"Querying API (start={start})...")
            result = self._search_decisions(start=start, results_per_page=results_per_page, lang=lang)

            if result.get('errorLabel'):
                logger.error(f"API error: {result['errorLabel']}")
                break

            decisions = result.get('results', [])
            if not decisions:
                logger.info("No more results from API")
                break

            total_count = result.get('numFound', 0)
            logger.info(f"Found {len(decisions)} decisions (total: {total_count})")

            for decision in decisions:
                if max_docs is not None and total_fetched >= max_docs:
                    return

                # Get the document URL (prefer English, then any available)
                pdf_url = None
                for lang_doc in decision.get('languagesOriginal', []):
                    if lang_doc.get('code') == 'en':
                        pdf_url = lang_doc.get('pdfUrl')
                        break

                if not pdf_url and decision.get('languagesOriginal'):
                    pdf_url = decision['languagesOriginal'][0].get('pdfUrl')

                if not pdf_url:
                    logger.warning(f"No document URL for {decision.get('uniqueSolrKey')}")
                    continue

                # Download and extract text
                logger.info(f"Downloading {decision.get('uniqueSolrKey')}...")
                doc_bytes = self._download_document(pdf_url)
                if not doc_bytes:
                    continue

                text = self._extract_text(doc_bytes, pdf_url)
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for {decision.get('uniqueSolrKey')}: {len(text) if text else 0} chars")
                    continue

                decision['text'] = text
                decision['document_url'] = pdf_url

                yield decision
                total_fetched += 1

                # Rate limiting
                time.sleep(2)

            start += len(decisions)

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch decisions updated since a given date.

        Args:
            since: Minimum date to fetch

        Yields:
            Decision documents with full text
        """
        # EUIPO API sorts by date descending, so we can stop when we hit old docs
        for decision in self.fetch_all(max_docs=500):
            date_str = decision.get('date')
            if date_str:
                iso_date = self._parse_date(date_str)
                if iso_date:
                    doc_date = datetime.strptime(iso_date, "%Y-%m-%d")
                    if doc_date < since:
                        break
                    yield decision

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema.

        Args:
            raw_doc: Raw decision from API

        Returns:
            Normalized document dict
        """
        unique_key = raw_doc.get('uniqueSolrKey', '')

        # Build title from entity name and case number
        entity_name = raw_doc.get('entityName', '')
        case_number = raw_doc.get('caseNumber', '')
        decision_type = raw_doc.get('type', raw_doc.get('typeLabel', ''))

        if entity_name and entity_name != '(Trade mark without text)':
            title = f"{decision_type}: {entity_name}"
        else:
            title = f"{decision_type} - Case {case_number}"

        if case_number:
            title += f" ({case_number})"

        # Parse date
        date = self._parse_date(raw_doc.get('date'))

        # Get URL
        url = raw_doc.get('url', f"{BASE_URL}/eSearchCLW/#key/trademark/{unique_key}")

        # Get outcome
        outcome = raw_doc.get('outcome', '')

        # Get legal norms
        norms = raw_doc.get('norms', [])

        return {
            '_id': unique_key,
            '_source': 'EU/EUIPO',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'case_number': case_number,
            'decision_type': decision_type,
            'ip_right': raw_doc.get('ipRight', ''),
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': date,
            'url': url,
            'outcome': outcome,
            'legal_norms': norms,
            'entity_name': entity_name,
            'entity_number': raw_doc.get('entityNumber', ''),
            'entity_type': raw_doc.get('entityType', ''),
            'appealed': raw_doc.get('appealed', ''),
        }


def main():
    """Main entry point."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        # Bootstrap mode - fetch sample data
        fetcher = EUIPOFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 15 if is_sample else 100

        logger.info(f"Fetching {'sample' if is_sample else 'full'} data from EUIPO...")
        logger.info(f"Target count: {target_count}")

        sample_count = 0
        text_lengths = []

        for raw_doc in fetcher.fetch_all(max_docs=target_count):
            normalized = fetcher.normalize(raw_doc)

            # Validate: must have substantial text
            text_len = len(normalized.get('text', ''))
            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']}: text too short ({text_len} chars)")
                continue

            # Save to sample directory
            filename = f"{normalized['_id'].replace('/', '_').replace(':', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['_id']} - {normalized['title'][:60]}... ({text_len} chars)")
            text_lengths.append(text_len)
            sample_count += 1

            if sample_count >= target_count:
                break

        # Print summary
        if text_lengths:
            avg_len = sum(text_lengths) // len(text_lengths)
            logger.info(f"\nBootstrap complete.")
            logger.info(f"  Documents saved: {sample_count}")
            logger.info(f"  Average text length: {avg_len:,} characters")
            logger.info(f"  Sample directory: {sample_dir}")
        else:
            logger.error("No documents with valid text content found!")
            sys.exit(1)

    else:
        # Test mode
        fetcher = EUIPOFetcher()

        print("Testing EUIPO fetcher...")
        print("=" * 60)

        # First just test the API
        result = fetcher._search_decisions(start=0, results_per_page=3)
        print(f"API returned {len(result.get('results', []))} results")
        print(f"Total available: {result.get('numFound', 0)}")

        print("\nSample decision metadata:")
        for decision in result.get('results', [])[:3]:
            print(f"  - {decision.get('uniqueSolrKey')}: {decision.get('typeLabel')} - {decision.get('entityName', 'N/A')[:40]}")

        print(f"\n{'=' * 60}")
        print("API test successful.")


if __name__ == '__main__':
    main()
