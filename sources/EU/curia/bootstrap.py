#!/usr/bin/env python3
"""
EU/CURIA Data Fetcher
Court of Justice of the European Union case law

This fetcher retrieves CJEU and General Court judgments, orders, and opinions.
Uses the Publications Office SPARQL endpoint for document discovery and
EUR-Lex HTML endpoint for full text retrieval.

Data flow:
1. Query SPARQL endpoint for case law metadata (CELEX numbers, dates, titles)
2. Fetch full HTML text from EUR-Lex using CELEX identifiers
3. Parse and extract clean text from HTML
4. Normalize to standard schema
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List

import requests
from bs4 import BeautifulSoup
import html2text

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"
EURLEX_HTML_BASE = "https://eur-lex.europa.eu/legal-content/{lang}/TXT/HTML/"
EURLEX_BASE = "https://eur-lex.europa.eu"
CELLAR_BASE = "http://publications.europa.eu/resource"

# Initialize HTML to text converter
h2t = html2text.HTML2Text()
h2t.ignore_links = True
h2t.ignore_images = True
h2t.body_width = 0  # No line wrapping


class CURIAFetcher:
    """Fetcher for CJEU case law documents."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)',
            'Accept': 'application/json'
        })

    def _make_request(self, url: str, params: Optional[Dict] = None,
                      method: str = "GET", data: Optional[Dict] = None,
                      silent: bool = False, headers: Optional[Dict] = None,
                      max_retries: int = 3, base_delay: float = 2.0) -> requests.Response:
        """Make HTTP request with retry logic.

        Args:
            max_retries: Maximum number of retry attempts
            base_delay: Base delay for exponential backoff (seconds)
        """
        for attempt in range(max_retries):
            try:
                req_headers = dict(self.session.headers)
                if headers:
                    req_headers.update(headers)
                if method == "POST":
                    response = self.session.post(url, params=params, data=data,
                                                 headers=req_headers, timeout=120)
                else:
                    response = self.session.get(url, params=params,
                                               headers=req_headers, timeout=60)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if not silent:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    if not silent:
                        logger.info(f"Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                else:
                    raise

    def _query_sparql(self, limit: int = 100, offset: int = 0,
                      doc_types: Optional[List[str]] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      order_asc: bool = False) -> List[Dict[str, Any]]:
        """Query SPARQL endpoint for case law metadata.

        Args:
            limit: Max results per query
            offset: Offset for pagination
            doc_types: Document types to fetch (JUDG, ORDER, OPIN_AG)
            start_date: Minimum date filter (YYYY-MM-DD)
            end_date: Maximum date filter (YYYY-MM-DD)
            order_asc: If True, order by date ascending (oldest first)
        """
        # Default to judgments only - orders often lack full text XHTML in CELLAR
        if doc_types is None:
            doc_types = ["JUDG"]

        type_uris = {
            "JUDG": "<http://publications.europa.eu/resource/authority/resource-type/JUDG>",
            "ORDER": "<http://publications.europa.eu/resource/authority/resource-type/ORDER>",
            "OPIN_AG": "<http://publications.europa.eu/resource/authority/resource-type/OPIN_AG>",
        }

        type_filter = ", ".join(type_uris.get(t, "") for t in doc_types if t in type_uris)

        # Build date filter clause
        date_filter = ""
        if start_date or end_date:
            filters = []
            if start_date:
                filters.append(f'?date >= "{start_date}"^^xsd:date')
            if end_date:
                filters.append(f'?date < "{end_date}"^^xsd:date')
            date_filter = "FILTER(" + " && ".join(filters) + ")"

        order_direction = "ASC" if order_asc else "DESC"

        # CRITICAL: Keep offset low to avoid SPARQL 500 errors
        # The Publications Office SPARQL endpoint (Virtuoso) becomes unstable at high offsets.
        # Each page requires materializing all prior pages, causing exponential slowdown.
        # At offset ~10000, the endpoint returns 500 errors consistently.
        #
        # FIX: Use smaller page sizes and warn if offset is getting high.
        # Callers should use date-based pagination instead of offset-based.
        effective_limit = limit
        if offset >= 5000:
            logger.warning(f"High SPARQL offset ({offset}) - consider using date-based pagination")
            effective_limit = min(limit, 20)
        if offset >= 8000:
            effective_limit = min(limit, 10)

        query = f"""PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT DISTINCT ?celex ?date ?title ?ecli
WHERE {{
  ?work cdm:work_has_resource-type ?type .
  FILTER(?type IN ({type_filter}))
  ?work cdm:resource_legal_id_celex ?celex .
  OPTIONAL {{ ?work cdm:work_date_document ?date }}
  OPTIONAL {{ ?work cdm:work_title ?title }}
  OPTIONAL {{ ?work cdm:resource_legal_id_ecli ?ecli }}
  {date_filter}
}}
ORDER BY {order_direction}(?date)
LIMIT {effective_limit}
OFFSET {offset}"""

        # Use more aggressive retry logic for SPARQL - endpoint is unstable
        max_retries = 8
        base_delay = 10.0

        for attempt in range(max_retries):
            try:
                # Use URL-encoded form data with proper headers
                response = self._make_request(
                    SPARQL_ENDPOINT,
                    method="POST",
                    data={'query': query},
                    params={'format': 'application/json'},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                    max_retries=1,  # Single try per attempt, we handle retries here
                    base_delay=1.0
                )
                result = response.json()

                documents = []
                for binding in result.get('results', {}).get('bindings', []):
                    doc = {
                        'celex': binding.get('celex', {}).get('value'),
                        'date': binding.get('date', {}).get('value'),
                        'title': binding.get('title', {}).get('value'),
                        'ecli': binding.get('ecli', {}).get('value'),
                    }
                    if doc['celex']:  # Only include if CELEX exists
                        documents.append(doc)

                return documents

            except Exception as e:
                logger.warning(f"SPARQL query failed (attempt {attempt + 1}/{max_retries}, offset={offset}): {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)  # Exponential backoff: 10, 20, 40, 80, 160, 320, 640s
                    delay = min(delay, 600)  # Cap at 10 minutes
                    logger.info(f"Waiting {delay:.0f}s before SPARQL retry...")
                    time.sleep(delay)
                else:
                    logger.error(f"SPARQL query failed after {max_retries} attempts at offset {offset}")
                    return []

        return []

    def _query_sparql_by_year(self, year: int, doc_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Query SPARQL endpoint for all case law in a specific year.

        This avoids the OFFSET pagination problem by using date-based filtering.
        Each year typically has <5000 documents, which the SPARQL endpoint can handle.
        """
        start_date = f"{year}-01-01"
        end_date = f"{year + 1}-01-01"

        all_docs = []
        offset = 0
        limit = 100

        while True:
            docs = self._query_sparql(
                limit=limit, offset=offset,
                doc_types=doc_types,
                start_date=start_date,
                end_date=end_date,
                order_asc=True
            )

            if not docs:
                break

            all_docs.extend(docs)
            offset += len(docs)

            # Safety check - if we hit 5000 docs in one year, something is wrong
            if offset >= 5000:
                logger.warning(f"Year {year} has {offset}+ documents - may be incomplete")
                break

        logger.info(f"Year {year}: found {len(all_docs)} documents")
        return all_docs

    def _fetch_cellar_xhtml(self, celex: str, lang: str = "ENG",
                             silent: bool = False) -> Optional[str]:
        """Fetch document XHTML from CELLAR API.

        EUR-Lex HTML endpoint now has AWS WAF protection. Use CELLAR instead.
        The CELLAR API uses content negotiation:
        - Request {CELLAR_BASE}/celex/{celex}.{lang} with Accept: text/html
        - Server returns 303 redirect to the actual HTML content
        """
        # Direct CELLAR content negotiation URL (no .xhtml suffix needed)
        cellar_url = f"{CELLAR_BASE}/celex/{celex}.{lang}"
        try:
            # Request HTML directly - the API handles content negotiation
            response = self._make_request(
                cellar_url,
                headers={'Accept': 'text/html, application/xhtml+xml'},
                silent=silent,
                max_retries=3,
                base_delay=2.0
            )

            # Check if we got HTML content
            content_type = response.headers.get('Content-Type', '')
            if 'html' in content_type.lower() or b'<html' in response.content[:500].lower() or b'<HTML' in response.content[:500]:
                return response.text

            if not silent:
                logger.warning(f"Non-HTML response for CELEX {celex}: {content_type[:50]}")
            return None

        except Exception as e:
            if not silent:
                logger.warning(f"Failed to fetch CELLAR XHTML for CELEX {celex}: {e}")
            return None

    def _fetch_document_html(self, celex: str, lang: str = "EN",
                             silent: bool = False) -> Optional[str]:
        """Fetch document HTML - now uses CELLAR API due to EUR-Lex WAF."""
        # EUR-Lex HTML endpoint now has AWS WAF protection
        # Use CELLAR API instead which provides direct XHTML access
        lang_code = "ENG" if lang == "EN" else lang
        return self._fetch_cellar_xhtml(celex, lang=lang_code, silent=silent)

    def _extract_text_from_html(self, html: str) -> str:
        """Extract clean text from EUR-Lex case law HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        # Check if this is a proper case law document (not a redirect/search page)
        # Case law HTML has specific structure indicators
        title = soup.find('title')
        if title:
            title_text = title.get_text()
            # Redirect pages have generic EUR-Lex titles
            if 'EUR-Lex -' in title_text and 'EN - EUR-Lex' in title_text:
                # Check for judgment indicators in body
                body_text = soup.get_text()[:2000]
                if 'JUDGMENT' not in body_text.upper() and 'ORDER' not in body_text.upper():
                    return ""  # Not a proper case document

        # Remove script and style elements
        for element in soup(["script", "style", "nav", "header", "footer"]):
            element.decompose()

        # Get the body content
        body = soup.find('body')
        if not body:
            return ""

        # Convert to text using html2text
        text = h2t.handle(str(body))

        # Clean up excessive whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)

        # Remove markdown artifacts
        text = re.sub(r'^\s*\*+\s*$', '', text, flags=re.MULTILINE)

        # Additional check: if the text contains EUR-Lex navigation elements, filter it out
        if 'Skip to main content' in text[:200] and 'EUR-Lex home' in text[:500]:
            return ""  # This is a navigation/redirect page, not a document

        return text.strip()

    def _extract_metadata_from_html(self, html: str, celex: str) -> Dict[str, Any]:
        """Extract metadata from HTML if not available from SPARQL."""
        soup = BeautifulSoup(html, 'html.parser')
        metadata = {}

        # Try to extract title
        title_elem = soup.find('title')
        if title_elem:
            metadata['title'] = title_elem.get_text(strip=True)

        # Try to extract date from content
        date_pattern = r'(\d{1,2}\s+\w+\s+\d{4})'
        text = soup.get_text()
        date_match = re.search(date_pattern, text[:1000])  # Look in first 1000 chars
        if date_match:
            metadata['date_str'] = date_match.group(1)

        return metadata

    def _determine_court_from_celex(self, celex: str) -> str:
        """Determine which court from CELEX number."""
        # CELEX format: 6yyyyXXnnnn
        # CJ = Court of Justice, TJ = General Court, FJ = Civil Service Tribunal
        if len(celex) >= 7:
            court_code = celex[5:7]
            if court_code == "CJ":
                return "Court of Justice"
            elif court_code == "TJ":
                return "General Court"
            elif court_code == "FJ":
                return "Civil Service Tribunal"
        return "CJEU"

    def _determine_doc_type_from_celex(self, celex: str) -> str:
        """Determine document type from CELEX number."""
        # The letters after the court code indicate type
        # CJ = Judgment, CO = Order, CC = Opinion of AG, etc.
        if len(celex) >= 7:
            type_indicator = celex[5:7]
            if type_indicator in ["CJ", "TJ", "FJ"]:
                return "judgment"
            elif type_indicator in ["CO", "TO", "FO"]:
                return "order"
            elif type_indicator in ["CC", "TC"]:
                return "opinion"
        return "judgment"

    def fetch_all(self, max_docs: Optional[int] = None,
                  start_date: Optional[str] = None,
                  end_date: Optional[str] = None,
                  order_asc: bool = False) -> Iterator[Dict[str, Any]]:
        """Fetch all available case law documents.

        Args:
            max_docs: Maximum number of documents to fetch. None means unlimited.
            start_date: Minimum date filter (YYYY-MM-DD) for historical data
            end_date: Maximum date filter (YYYY-MM-DD)
            order_asc: If True, fetch oldest first (for historical data)
        """
        offset = 0
        batch_size = 50
        total_fetched = 0

        while max_docs is None or total_fetched < max_docs:
            # Query SPARQL for batch of metadata
            date_info = f" (date range: {start_date or 'any'} to {end_date or 'any'})" if start_date or end_date else ""
            logger.info(f"Querying SPARQL (offset={offset}, limit={batch_size}){date_info}...")
            documents = self._query_sparql(
                limit=batch_size, offset=offset,
                start_date=start_date, end_date=end_date, order_asc=order_asc
            )

            if not documents:
                logger.info("No more documents from SPARQL query")
                break

            actual_batch_size = len(documents)
            for doc in documents:
                if max_docs is not None and total_fetched >= max_docs:
                    return

                celex = doc['celex']
                logger.info(f"Fetching full text for {celex}...")

                # Fetch HTML content
                html = self._fetch_document_html(celex)
                if not html:
                    logger.warning(f"No HTML content for {celex}")
                    continue

                # Extract text
                text = self._extract_text_from_html(html)
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for {celex}: {len(text) if text else 0} chars")
                    continue

                # Merge metadata
                html_metadata = self._extract_metadata_from_html(html, celex)
                doc['text'] = text
                doc['html'] = html
                if not doc.get('title') and html_metadata.get('title'):
                    doc['title'] = html_metadata['title']

                yield doc
                total_fetched += 1

                # Rate limiting
                time.sleep(1.5)

            # Use actual batch size to handle adaptive page sizing at high offsets
            offset += actual_batch_size

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date."""
        for doc in self.fetch_all(max_docs=500):
            if doc.get('date'):
                try:
                    doc_date = datetime.fromisoformat(doc['date'].replace('Z', '+00:00'))
                    if doc_date.replace(tzinfo=None) >= since:
                        yield doc
                except ValueError:
                    pass  # Skip docs with unparseable dates

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        celex = raw_doc['celex']

        # Parse date
        parsed_date = None
        if raw_doc.get('date'):
            try:
                # Date from SPARQL is typically YYYY-MM-DD
                parsed_date = raw_doc['date'][:10]  # Take just the date part
            except (ValueError, TypeError):
                pass

        # Determine court and document type
        court = self._determine_court_from_celex(celex)
        doc_type = self._determine_doc_type_from_celex(celex)

        # Clean title
        title = raw_doc.get('title', '')
        if not title:
            title = f"Case {celex}"

        # Build URL
        url = f"{EURLEX_BASE}/legal-content/EN/TXT/?uri=CELEX:{celex}"

        return {
            '_id': celex,
            '_source': 'EU/CURIA',
            '_type': 'case_law',
            '_fetched_at': datetime.now().isoformat(),
            'celex_id': celex,
            'ecli': raw_doc.get('ecli'),
            'court': court,
            'document_type': doc_type,
            'title': title,
            'text': raw_doc.get('text', ''),
            'date': parsed_date,
            'url': url,
        }


def load_checkpoint(checkpoint_file: Path) -> Dict[str, Any]:
    """Load checkpoint from file."""
    if checkpoint_file.exists():
        try:
            with open(checkpoint_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {'offset': 0, 'completed_celex': set(), 'total_saved': 0}


def save_checkpoint(checkpoint_file: Path, offset: int, completed_celex: set, total_saved: int,
                    current_year: Optional[int] = None):
    """Save checkpoint to file."""
    data = {
        'offset': offset,
        'completed_celex': list(completed_celex),
        'total_saved': total_saved
    }
    if current_year is not None:
        data['current_year'] = current_year
    with open(checkpoint_file, 'w') as f:
        json.dump(data, f)


def fetch_and_save_document(fetcher: 'CURIAFetcher', doc: Dict[str, Any],
                            sample_dir: Path, completed_celex: set) -> Optional[int]:
    """Fetch and save a single document. Returns text length or None if failed."""
    celex = doc['celex']

    # Skip already completed documents
    if celex in completed_celex:
        return None

    logger.info(f"Fetching full text for {celex}...")

    # Fetch HTML content
    html = fetcher._fetch_document_html(celex)
    if not html:
        logger.warning(f"No HTML content for {celex}")
        completed_celex.add(celex)
        return None

    # Extract text
    text = fetcher._extract_text_from_html(html)
    if not text or len(text) < 100:
        logger.warning(f"Insufficient text for {celex}: {len(text) if text else 0} chars")
        completed_celex.add(celex)
        return None

    # Merge metadata
    html_metadata = fetcher._extract_metadata_from_html(html, celex)
    doc['text'] = text
    doc['html'] = html
    if not doc.get('title') and html_metadata.get('title'):
        doc['title'] = html_metadata['title']

    normalized = fetcher.normalize(doc)

    # Validate: must have substantial text
    text_len = len(normalized.get('text', ''))
    if text_len < 500:
        logger.warning(f"Skipping {normalized['_id']}: text too short ({text_len} chars)")
        completed_celex.add(celex)
        return None

    # Save to sample directory
    filename = f"{normalized['_id'].replace(':', '_').replace('/', '_')}.json"
    filepath = sample_dir / filename

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(normalized, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved: {normalized['_id']} - {normalized['title'][:60]}... ({text_len} chars)")
    completed_celex.add(celex)

    # Rate limiting
    time.sleep(1.5)

    return text_len


def main():
    """Main entry point for testing."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        # Bootstrap mode - fetch sample data
        fetcher = CURIAFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)
        checkpoint_file = Path(__file__).parent / '.checkpoint.json'

        is_sample = '--sample' in sys.argv
        use_year_mode = '--by-year' in sys.argv  # NEW: Year-based pagination mode
        target_count = 12 if is_sample else None  # No limit for full bootstrap

        # Parse date range arguments (--start-date=YYYY-MM-DD --end-date=YYYY-MM-DD)
        start_date = None
        end_date = None
        order_asc = False
        start_year = None
        end_year = None
        for arg in sys.argv:
            if arg.startswith('--start-date='):
                start_date = arg.split('=')[1]
                order_asc = True  # When fetching historical, go oldest first
            elif arg.startswith('--end-date='):
                end_date = arg.split('=')[1]
            elif arg.startswith('--start-year='):
                start_year = int(arg.split('=')[1])
                use_year_mode = True
            elif arg.startswith('--end-year='):
                end_year = int(arg.split('=')[1])
            elif arg == '--historical':
                # Convenience flag for pre-2018 data
                end_date = '2018-01-01'
                order_asc = True

        # Load checkpoint for full bootstrap
        if not is_sample:
            checkpoint = load_checkpoint(checkpoint_file)
            start_offset = checkpoint['offset']
            completed_celex = set(checkpoint.get('completed_celex', []))
            sample_count = checkpoint.get('total_saved', 0)
            checkpoint_year = checkpoint.get('current_year')
            if start_offset > 0 or sample_count > 0:
                logger.info(f"Resuming from checkpoint: offset={start_offset}, saved={sample_count}, year={checkpoint_year}")
        else:
            start_offset = 0
            completed_celex = set()
            sample_count = 0
            checkpoint_year = None

        # ========== YEAR-BASED MODE ==========
        # This mode avoids the SPARQL OFFSET problem by iterating year by year.
        # Each year typically has <5000 documents, which the endpoint can handle.
        if use_year_mode:
            # Default year range: 1954 (CJEU founding) to current year
            current_calendar_year = datetime.now().year
            if start_year is None:
                start_year = checkpoint_year if checkpoint_year else 1954
            if end_year is None:
                end_year = current_calendar_year

            logger.info(f"Using YEAR-BASED mode: {start_year} to {end_year}")

            text_lengths = []

            for year in range(start_year, end_year + 1):
                logger.info(f"\n{'='*60}")
                logger.info(f"Processing year {year}...")

                # Query all documents for this year
                documents = fetcher._query_sparql_by_year(year)
                if not documents:
                    logger.info(f"Year {year}: no documents found")
                    continue

                logger.info(f"Year {year}: found {len(documents)} documents")

                for doc in documents:
                    if target_count and sample_count >= target_count:
                        break

                    text_len = fetch_and_save_document(fetcher, doc, sample_dir, completed_celex)
                    if text_len:
                        text_lengths.append(text_len)
                        sample_count += 1

                    # Save checkpoint every 10 documents
                    if not is_sample and sample_count % 10 == 0:
                        save_checkpoint(checkpoint_file, 0, completed_celex, sample_count, current_year=year)
                        logger.info(f"Checkpoint saved: {sample_count} documents (year {year})")

                if target_count and sample_count >= target_count:
                    break

                # Save checkpoint after each year
                if not is_sample:
                    save_checkpoint(checkpoint_file, 0, completed_celex, sample_count, current_year=year + 1)
                    logger.info(f"Year {year} complete: {sample_count} total documents")

            # Print summary
            if text_lengths or sample_count > 0:
                avg_len = sum(text_lengths) // len(text_lengths) if text_lengths else 0
                logger.info(f"\nBootstrap complete (year mode).")
                logger.info(f"  Documents saved: {sample_count}")
                logger.info(f"  Average text length: {avg_len:,} characters")
                logger.info(f"  Sample directory: {sample_dir}")
            else:
                logger.error("No documents with valid text content found!")
            return

        # ========== LEGACY OFFSET-BASED MODE ==========
        # WARNING: This mode may fail at offset ~10K due to SPARQL endpoint limitations.
        # Consider using --by-year mode instead for full bootstrap.

        date_range_info = ""
        if start_date or end_date:
            date_range_info = f" (date range: {start_date or '1954-12-21'} to {end_date or 'present'})"
        logger.info(f"Fetching sample documents from CURIA (OFFSET mode)...{date_range_info}")
        logger.info("WARNING: Offset mode may fail at ~10K documents. Use --by-year for full bootstrap.")

        text_lengths = []
        batch_size = 50
        offset = start_offset
        consecutive_empty = 0  # Track consecutive empty responses

        while True:
            # Query SPARQL for batch of metadata
            # Note: _query_sparql uses adaptive page sizing at high offsets
            logger.info(f"Querying SPARQL (offset={offset}, limit={batch_size})...")
            documents = fetcher._query_sparql(
                limit=batch_size, offset=offset,
                start_date=start_date, end_date=end_date, order_asc=order_asc
            )

            if not documents:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    logger.info("No more documents from SPARQL query (3 consecutive empty responses)")
                    break
                else:
                    # SPARQL endpoint may be temporarily unstable, try next offset
                    logger.warning(f"Empty SPARQL response at offset {offset}, trying next batch...")
                    offset += batch_size
                    time.sleep(10)  # Wait before retry
                    continue
            else:
                consecutive_empty = 0

            actual_batch_size = len(documents)
            for doc in documents:
                if target_count and sample_count >= target_count:
                    break

                text_len = fetch_and_save_document(fetcher, doc, sample_dir, completed_celex)
                if text_len:
                    text_lengths.append(text_len)
                    sample_count += 1

                # Save checkpoint every 10 documents (for full bootstrap)
                if not is_sample and sample_count % 10 == 0:
                    save_checkpoint(checkpoint_file, offset, completed_celex, sample_count)
                    logger.info(f"Checkpoint saved: {sample_count} documents")

            if target_count and sample_count >= target_count:
                break

            # Use actual batch size to handle adaptive page sizing at high offsets
            offset += actual_batch_size

            # Save checkpoint after each batch (for full bootstrap)
            if not is_sample:
                save_checkpoint(checkpoint_file, offset, completed_celex, sample_count)

        # Final checkpoint save
        if not is_sample:
            save_checkpoint(checkpoint_file, offset, completed_celex, sample_count)

        # Print summary
        if text_lengths or sample_count > 0:
            avg_len = sum(text_lengths) // len(text_lengths) if text_lengths else 0
            logger.info(f"\nBootstrap complete.")
            logger.info(f"  Documents saved: {sample_count}")
            logger.info(f"  Average text length: {avg_len:,} characters")
            logger.info(f"  Sample directory: {sample_dir}")
        else:
            logger.error("No documents with valid text content found!")

    else:
        # Test mode - fetch and print a few documents
        fetcher = CURIAFetcher()

        print("Testing CURIA fetcher...")
        print("=" * 60)

        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=5):
            normalized = fetcher.normalize(raw_doc)

            print(f"\n--- Document {count + 1} ---")
            print(f"CELEX: {normalized['_id']}")
            print(f"ECLI: {normalized.get('ecli', 'N/A')}")
            print(f"Court: {normalized['court']}")
            print(f"Type: {normalized['document_type']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"URL: {normalized['url']}")

            count += 1

        print(f"\n{'=' * 60}")
        print(f"Tested {count} documents successfully.")


if __name__ == '__main__':
    main()
