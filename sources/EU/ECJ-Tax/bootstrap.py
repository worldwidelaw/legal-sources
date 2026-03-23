#!/usr/bin/env python3
"""
EU/ECJ-Tax Data Fetcher
European Court of Justice Tax Law Cases

This fetcher retrieves CJEU judgments, orders, and opinions related to taxation.
It uses the same SPARQL + CELLAR approach as EU/curia but filters results
to include only tax-related cases based on:
1. References to tax directives (VAT Directive, Excise Directive, etc.)
2. Tax-related keywords in the judgment text

Data flow:
1. Query SPARQL endpoint for case law metadata (CELEX numbers, dates, titles)
2. Fetch full HTML text from CELLAR API using CELEX identifiers
3. Filter to keep only tax-related cases
4. Parse and extract clean text from HTML
5. Normalize to standard schema with tax-specific metadata
"""

import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional, List, Set, Tuple

import requests
from bs4 import BeautifulSoup
import html2text

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
SPARQL_ENDPOINT = "http://publications.europa.eu/webapi/rdf/sparql"
CELLAR_BASE = "http://publications.europa.eu/resource"
EURLEX_BASE = "https://eur-lex.europa.eu"

# Initialize HTML to text converter
h2t = html2text.HTML2Text()
h2t.ignore_links = True
h2t.ignore_images = True
h2t.body_width = 0  # No line wrapping

# Tax-related keywords (lowercase for matching)
TAX_KEYWORDS = {
    'vat', 'value added tax', 'value-added tax', 'taxation', 'tax',
    'fiscal', 'excise', 'customs duty', 'duty', 'tariff',
    'income tax', 'corporate tax', 'withholding tax',
    'direct taxation', 'indirect taxation', 'tax exemption',
    'tax deduction', 'tax base', 'taxable person', 'taxable amount',
    'input tax', 'output tax', 'reverse charge', 'intra-community',
    'state aid', 'tax credit', 'deductibility', 'taxable transaction',
    'exempt transaction', 'reduced rate', 'zero rate', 'standard rate',
    'place of supply', 'chargeable event', 'tax liability',
    'transfer pricing', 'tax evasion', 'tax avoidance', 'tax fraud',
    'double taxation', 'withholding', 'dividend', 'interest payment',
    'royalties', 'parent company', 'subsidiary', 'permanent establishment',
    'fixed establishment', 'triangular transaction',
}

# Key EU tax directive references
TAX_DIRECTIVE_PATTERNS = [
    (r'directive\s+2006/112', 'VAT Directive'),
    (r'directive\s+77/388', 'Sixth VAT Directive'),
    (r'sixth\s+directive', 'Sixth VAT Directive'),
    (r'directive\s+2008/118', 'Excise Directive'),
    (r'directive\s+92/12', 'Old Excise Directive'),
    (r'directive\s+2011/96', 'Parent-Subsidiary Directive'),
    (r'directive\s+2003/49', 'Interest and Royalties Directive'),
    (r'directive\s+90/435', 'Old Parent-Subsidiary Directive'),
    (r'directive\s+2011/16', 'Administrative Cooperation Directive'),
    (r'directive\s+2016/1164', 'Anti-Tax Avoidance Directive'),
    (r'directive\s+2009/133', 'Merger Directive'),
    (r'directive\s+90/434', 'Old Merger Directive'),
    (r'council\s+regulation.*customs', 'Customs Regulation'),
    (r'common\s+system\s+of\s+value\s+added\s+tax', 'VAT System'),
    (r'vat\s+directive', 'VAT Directive'),
]


class ECJTaxFetcher:
    """Fetcher for ECJ tax law case documents."""

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
        """Make HTTP request with retry logic."""
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
                    delay = base_delay * (2 ** attempt)
                    if not silent:
                        logger.info(f"Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                else:
                    raise

    def _query_sparql(self, limit: int = 100, offset: int = 0,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      order_asc: bool = False) -> List[Dict[str, Any]]:
        """Query SPARQL endpoint for case law metadata."""
        # Query all judgments - filtering will be done after fetching full text
        type_filter = "<http://publications.europa.eu/resource/authority/resource-type/JUDG>"

        date_filter = ""
        if start_date or end_date:
            filters = []
            if start_date:
                filters.append(f'?date >= "{start_date}"^^xsd:date')
            if end_date:
                filters.append(f'?date < "{end_date}"^^xsd:date')
            date_filter = "FILTER(" + " && ".join(filters) + ")"

        order_direction = "ASC" if order_asc else "DESC"

        # Reduce page size at high offsets
        effective_limit = limit
        if offset >= 5000:
            logger.warning(f"High SPARQL offset ({offset}) - consider using date-based pagination")
            effective_limit = min(limit, 20)

        query = f"""PREFIX cdm: <http://publications.europa.eu/ontology/cdm#>
SELECT DISTINCT ?celex ?date ?title ?ecli
WHERE {{
  ?work cdm:work_has_resource-type {type_filter} .
  ?work cdm:resource_legal_id_celex ?celex .
  OPTIONAL {{ ?work cdm:work_date_document ?date }}
  OPTIONAL {{ ?work cdm:work_title ?title }}
  OPTIONAL {{ ?work cdm:resource_legal_id_ecli ?ecli }}
  {date_filter}
}}
ORDER BY {order_direction}(?date)
LIMIT {effective_limit}
OFFSET {offset}"""

        max_retries = 5
        base_delay = 5.0

        for attempt in range(max_retries):
            try:
                response = self._make_request(
                    SPARQL_ENDPOINT,
                    method="POST",
                    data={'query': query},
                    params={'format': 'application/json'},
                    headers={'Content-Type': 'application/x-www-form-urlencoded'},
                    max_retries=1,
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
                    if doc['celex']:
                        documents.append(doc)

                return documents

            except Exception as e:
                logger.warning(f"SPARQL query failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    delay = min(delay, 300)
                    logger.info(f"Waiting {delay:.0f}s before retry...")
                    time.sleep(delay)
                else:
                    return []

        return []

    def _fetch_cellar_xhtml(self, celex: str, lang: str = "ENG",
                           silent: bool = False) -> Optional[str]:
        """Fetch document XHTML from CELLAR API."""
        cellar_url = f"{CELLAR_BASE}/celex/{celex}.{lang}"
        try:
            response = self._make_request(
                cellar_url,
                headers={'Accept': 'text/html, application/xhtml+xml'},
                silent=silent,
                max_retries=3,
                base_delay=2.0
            )

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

    def _extract_text_from_html(self, html: str) -> str:
        """Extract clean text from EUR-Lex case law HTML."""
        soup = BeautifulSoup(html, 'html.parser')

        # Check if this is a proper case law document
        title = soup.find('title')
        if title:
            title_text = title.get_text()
            if 'EUR-Lex -' in title_text and 'EN - EUR-Lex' in title_text:
                body_text = soup.get_text()[:2000]
                if 'JUDGMENT' not in body_text.upper() and 'ORDER' not in body_text.upper():
                    return ""

        # Remove script and style elements
        for element in soup(["script", "style", "nav", "header", "footer"]):
            element.decompose()

        body = soup.find('body')
        if not body:
            return ""

        text = h2t.handle(str(body))

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        text = re.sub(r'^\s*\*+\s*$', '', text, flags=re.MULTILINE)

        if 'Skip to main content' in text[:200] and 'EUR-Lex home' in text[:500]:
            return ""

        return text.strip()

    def _is_tax_related(self, text: str, title: Optional[str] = "") -> Tuple[bool, Set[str], Set[str]]:
        """Check if document is tax-related based on text content.

        Returns:
            Tuple of (is_tax_related, set of matching keywords, set of referenced directives)
        """
        title = title or ""
        combined_text = (title + " " + text).lower()

        # Find matching keywords
        found_keywords = set()
        for keyword in TAX_KEYWORDS:
            # Use word boundary matching for short keywords to avoid false positives
            if len(keyword) <= 4:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, combined_text):
                    found_keywords.add(keyword)
            else:
                if keyword in combined_text:
                    found_keywords.add(keyword)

        # Find directive references
        found_directives = set()
        for pattern, directive_name in TAX_DIRECTIVE_PATTERNS:
            if re.search(pattern, combined_text, re.IGNORECASE):
                found_directives.add(directive_name)

        # Consider tax-related if:
        # 1. References at least one tax directive, OR
        # 2. Contains at least 3 different tax keywords (to avoid false positives)
        is_tax_related = len(found_directives) > 0 or len(found_keywords) >= 3

        return is_tax_related, found_keywords, found_directives

    def _determine_court_from_celex(self, celex: str) -> str:
        """Determine which court from CELEX number."""
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
                  end_date: Optional[str] = None) -> Iterator[Dict[str, Any]]:
        """Fetch tax-related case law documents.

        Args:
            max_docs: Maximum number of tax documents to fetch (after filtering)
            start_date: Minimum date filter (YYYY-MM-DD)
            end_date: Maximum date filter (YYYY-MM-DD)
        """
        offset = 0
        batch_size = 50
        total_fetched = 0
        total_checked = 0

        while max_docs is None or total_fetched < max_docs:
            logger.info(f"Querying SPARQL (offset={offset}, checked={total_checked}, tax_found={total_fetched})...")
            documents = self._query_sparql(
                limit=batch_size, offset=offset,
                start_date=start_date, end_date=end_date
            )

            if not documents:
                logger.info("No more documents from SPARQL query")
                break

            actual_batch_size = len(documents)
            for doc in documents:
                if max_docs is not None and total_fetched >= max_docs:
                    return

                celex = doc['celex']
                total_checked += 1

                # Fetch HTML content
                html = self._fetch_cellar_xhtml(celex, silent=True)
                if not html:
                    continue

                # Extract text
                text = self._extract_text_from_html(html)
                if not text or len(text) < 100:
                    continue

                # Check if tax-related
                title = doc.get('title', '')
                is_tax, keywords, directives = self._is_tax_related(text, title)

                if not is_tax:
                    continue

                logger.info(f"TAX CASE: {celex} - Keywords: {list(keywords)[:5]}, Directives: {list(directives)}")

                doc['text'] = text
                doc['html'] = html
                doc['tax_keywords'] = list(keywords)
                doc['directive_references'] = list(directives)

                yield doc
                total_fetched += 1

                # Rate limiting
                time.sleep(1.5)

            offset += actual_batch_size

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents updated since a given date."""
        since_str = since.strftime('%Y-%m-%d')
        for doc in self.fetch_all(max_docs=100, start_date=since_str):
            yield doc

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema."""
        celex = raw_doc['celex']

        parsed_date = None
        if raw_doc.get('date'):
            try:
                parsed_date = raw_doc['date'][:10]
            except (ValueError, TypeError):
                pass

        court = self._determine_court_from_celex(celex)
        doc_type = self._determine_doc_type_from_celex(celex)

        title = raw_doc.get('title', '')
        if not title:
            title = f"Case {celex}"

        url = f"{EURLEX_BASE}/legal-content/EN/TXT/?uri=CELEX:{celex}"

        return {
            '_id': celex,
            '_source': 'EU/ECJ-Tax',
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
            'tax_keywords': raw_doc.get('tax_keywords', []),
            'directive_references': raw_doc.get('directive_references', []),
        }


def main():
    """Main entry point for testing."""
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'bootstrap':
        fetcher = ECJTaxFetcher()
        sample_dir = Path(__file__).parent / 'sample'
        sample_dir.mkdir(exist_ok=True)

        is_sample = '--sample' in sys.argv
        target_count = 15 if is_sample else None  # Fetch more to ensure we get 10+ tax cases

        # Parse date range arguments
        start_date = None
        end_date = None
        for arg in sys.argv:
            if arg.startswith('--start-date='):
                start_date = arg.split('=')[1]
            elif arg.startswith('--end-date='):
                end_date = arg.split('=')[1]

        logger.info(f"Fetching tax cases from CJEU (target: {target_count or 'unlimited'})...")
        if start_date or end_date:
            logger.info(f"Date range: {start_date or 'any'} to {end_date or 'present'}")

        sample_count = 0
        text_lengths = []

        for raw_doc in fetcher.fetch_all(max_docs=target_count, start_date=start_date, end_date=end_date):
            normalized = fetcher.normalize(raw_doc)

            # Validate
            text_len = len(normalized.get('text', ''))
            if text_len < 500:
                logger.warning(f"Skipping {normalized['_id']}: text too short ({text_len} chars)")
                continue

            # Save to sample directory
            filename = f"{normalized['_id'].replace(':', '_').replace('/', '_')}.json"
            filepath = sample_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(f"Saved: {normalized['_id']} - {normalized['title'][:50]}... ({text_len} chars)")
            logger.info(f"  Tax keywords: {normalized['tax_keywords'][:5]}")
            logger.info(f"  Directives: {normalized['directive_references']}")

            text_lengths.append(text_len)
            sample_count += 1

        # Print summary
        if text_lengths:
            avg_len = sum(text_lengths) // len(text_lengths)
            logger.info(f"\nBootstrap complete.")
            logger.info(f"  Tax cases saved: {sample_count}")
            logger.info(f"  Average text length: {avg_len:,} characters")
            logger.info(f"  Sample directory: {sample_dir}")
        else:
            logger.error("No tax-related documents found!")

    else:
        # Test mode
        fetcher = ECJTaxFetcher()

        print("Testing ECJ Tax fetcher...")
        print("=" * 60)

        count = 0
        for raw_doc in fetcher.fetch_all(max_docs=3):
            normalized = fetcher.normalize(raw_doc)

            print(f"\n--- Tax Case {count + 1} ---")
            print(f"CELEX: {normalized['_id']}")
            print(f"ECLI: {normalized.get('ecli', 'N/A')}")
            print(f"Court: {normalized['court']}")
            print(f"Title: {normalized['title'][:80]}...")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', '')):,} chars")
            print(f"Tax keywords: {normalized['tax_keywords'][:5]}")
            print(f"Directives: {normalized['directive_references']}")
            print(f"URL: {normalized['url']}")

            count += 1

        print(f"\n{'=' * 60}")
        print(f"Found {count} tax cases in sample.")


if __name__ == '__main__':
    main()
