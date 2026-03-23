#!/usr/bin/env python3
"""
LU/LegalDatabase -- Luxembourg Legal Database (Legilux) Data Fetcher

Fetches Luxembourg legislation from the data.legilux.public.lu SPARQL endpoint.

Strategy:
  - Use the SPARQL endpoint at https://data.legilux.public.lu/sparqlendpoint
  - Query for Acts (jolux:Act) with their metadata and HTML manifestation URLs
  - Fetch full text from HTML files in the filestore
  - Use the FRBR model: Work (Act) -> Expression -> Manifestation -> File

Endpoints:
  - SPARQL: https://data.legilux.public.lu/sparqlendpoint
  - Files: http://data.legilux.public.lu/filestore/eli/...
  - Portal: https://legilux.public.lu

Data:
  - Document types: LOI (laws), RGD (grand-ducal regulations), AMIN (ministerial orders), etc.
  - Full text in French (some German)
  - ELI URIs for all resources
  - License: CC BY 4.0

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent documents)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_module
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from html.parser import HTMLParser
from urllib.parse import urlencode, quote

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
logger = logging.getLogger("legal-data-hunter.LU.legaldatabase")

# SPARQL endpoint
SPARQL_ENDPOINT = "https://data.legilux.public.lu/sparqlendpoint"

# Rate limiting
REQUEST_DELAY = 1.0  # seconds between requests

# Document types to fetch (priority legislation types)
DOC_TYPES = [
    "LOI",    # Laws
    "RGD",    # Grand-Ducal Regulations
    "AGD",    # Grand-Ducal Decrees
    "AMIN",   # Ministerial Orders
    "RMIN",   # Ministerial Regulations
    "A",      # Decrees
    "CMIN",   # Ministerial Circulars
]


class TextExtractor(HTMLParser):
    """Extract text from HTML, skipping script/style tags."""

    def __init__(self):
        super().__init__()
        self.in_body = False
        self.text = []
        self.skip_tags = {'script', 'style', 'meta', 'link', 'head'}
        self.skip = False

    def handle_starttag(self, tag, attrs):
        if tag == 'body':
            self.in_body = True
        if tag in self.skip_tags:
            self.skip = True

    def handle_endtag(self, tag):
        if tag in self.skip_tags:
            self.skip = False

    def handle_data(self, data):
        if self.in_body and not self.skip:
            t = data.strip()
            if t:
                self.text.append(t)

    def get_text(self) -> str:
        return ' '.join(self.text)


class LegalDatabaseScraper(BaseScraper):
    """
    Scraper for LU/LegalDatabase -- Luxembourg Legal Database (Legilux).
    Country: LU
    URL: https://legilux.public.lu

    Data types: legislation
    Auth: none (CC BY 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (EU Legal Research; contact@example.com)",
            "Accept": "application/sparql-results+json",
        })
        self.last_request_time = 0

    def _wait_rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
        self.last_request_time = time.time()

    def _sparql_query(self, query: str) -> Dict[str, Any]:
        """
        Execute a SPARQL query and return results.

        Args:
            query: SPARQL query string

        Returns:
            Dict with results or empty dict on error
        """
        self._wait_rate_limit()

        try:
            resp = self.session.get(
                SPARQL_ENDPOINT,
                params={"query": query},
                timeout=120,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"SPARQL query failed: {e}")
            return {"results": {"bindings": []}}

    def _fetch_html_text(self, url: str) -> str:
        """
        Fetch HTML content and extract text.

        Args:
            url: URL to HTML file

        Returns:
            Extracted text content
        """
        self._wait_rate_limit()

        try:
            resp = self.session.get(url, timeout=60, headers={
                "Accept": "text/html",
            })
            resp.raise_for_status()

            parser = TextExtractor()
            parser.feed(resp.text)
            return self._clean_text(parser.get_text())

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch HTML from {url}: {e}")
            return ""

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""

        # Decode HTML entities
        text = html_module.unescape(text)

        # Remove excessive whitespace while preserving paragraph structure
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)

        # Strip leading/trailing whitespace from each line
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _build_query(self, doc_types: list, limit: int = 100, offset: int = 0,
                     since_date: Optional[str] = None) -> str:
        """
        Build SPARQL query for fetching legislation.

        Args:
            doc_types: List of document type codes (e.g., ["LOI", "RGD"])
            limit: Maximum records to fetch
            offset: Offset for pagination
            since_date: Optional date filter (YYYY-MM-DD)

        Returns:
            SPARQL query string
        """
        type_filters = " || ".join([
            f'?docType = restypes:{t}' for t in doc_types
        ])

        date_filter = ""
        if since_date:
            date_filter = f'FILTER(?date >= "{since_date}"^^xsd:date)'

        query = f"""
PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
PREFIX filetypes: <http://publications.europa.eu/resource/authority/file-type/>
PREFIX restypes: <http://data.legilux.public.lu/resource/authority/resource-type/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

SELECT ?act ?title ?date ?docType ?inForce ?url
WHERE {{
  ?act a jolux:Act .
  ?act jolux:typeDocument ?docType .
  ?act jolux:dateDocument ?date .
  ?act jolux:inForceStatus ?inForce .
  ?act jolux:isRealizedBy ?expr .
  ?expr jolux:title ?title .
  ?expr jolux:isEmbodiedBy ?manif .
  ?manif jolux:format filetypes:HTML .
  ?manif jolux:isExemplifiedBy ?url .

  FILTER({type_filters})
  {date_filter}
}}
ORDER BY DESC(?date)
LIMIT {limit}
OFFSET {offset}
"""
        return query

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Luxembourg Legal Database.

        Iterates through all document types with pagination.
        """
        total_fetched = 0
        page_size = 100
        max_pages = 100  # Safety limit per doc type

        for doc_type in DOC_TYPES:
            logger.info(f"Fetching document type: {doc_type}")
            offset = 0
            page = 0
            type_fetched = 0

            while page < max_pages:
                page += 1
                query = self._build_query([doc_type], limit=page_size, offset=offset)

                result = self._sparql_query(query)
                bindings = result.get("results", {}).get("bindings", [])

                if not bindings:
                    logger.info(f"No more {doc_type} documents. Fetched: {type_fetched}")
                    break

                for binding in bindings:
                    total_fetched += 1
                    type_fetched += 1
                    yield binding

                offset += page_size
                logger.info(f"  Page {page}: fetched {len(bindings)} {doc_type} records")

        logger.info(f"Total documents fetched: {total_fetched}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.
        """
        since_date = since.strftime('%Y-%m-%d')
        total_fetched = 0
        page_size = 100
        offset = 0
        max_pages = 50

        for page in range(max_pages):
            query = self._build_query(DOC_TYPES, limit=page_size, offset=offset,
                                      since_date=since_date)

            result = self._sparql_query(query)
            bindings = result.get("results", {}).get("bindings", [])

            if not bindings:
                break

            for binding in bindings:
                total_fetched += 1
                yield binding

            offset += page_size

        logger.info(f"Update fetched: {total_fetched} documents since {since_date}")

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw SPARQL binding into standard schema.

        CRITICAL: Fetches and includes full text in the 'text' field.
        """
        # Extract values from SPARQL bindings
        def get_value(key: str, default: str = "") -> str:
            if key in raw and "value" in raw[key]:
                return raw[key]["value"]
            return default

        act_uri = get_value("act")
        title = get_value("title")
        date = get_value("date")
        doc_type_uri = get_value("docType")
        in_force_uri = get_value("inForce")
        html_url = get_value("url")

        # Extract document type code from URI
        doc_type = doc_type_uri.split("/")[-1] if doc_type_uri else ""

        # Extract in-force status
        in_force_status = in_force_uri.split("/")[-1] if in_force_uri else ""
        is_in_force = "in-force" in in_force_status.lower()

        # Extract document ID from ELI URI
        # e.g., http://data.legilux.public.lu/eli/etat/leg/loi/2026/02/05/a29/jo
        eli_parts = act_uri.replace("http://data.legilux.public.lu/eli/", "").split("/")
        doc_id = "/".join(eli_parts) if eli_parts else act_uri

        # Fetch full text from HTML
        full_text = ""
        if html_url:
            full_text = self._fetch_html_text(html_url)

        # Clean title
        title = self._clean_text(title)

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "LU/LegalDatabase",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date[:10] if date else "",
            "url": act_uri,
            # Additional metadata
            "eli_uri": act_uri,
            "html_url": html_url,
            "document_type": doc_type,
            "document_type_label": self._doc_type_label(doc_type),
            "in_force_status": in_force_status,
            "is_in_force": is_in_force,
            "language": "fr",
        }

    def _doc_type_label(self, code: str) -> str:
        """Map document type code to human-readable label."""
        labels = {
            "LOI": "Loi (Law)",
            "RGD": "Règlement grand-ducal (Grand-Ducal Regulation)",
            "AGD": "Arrêté grand-ducal (Grand-Ducal Decree)",
            "AMIN": "Arrêté ministériel (Ministerial Order)",
            "RMIN": "Règlement ministériel (Ministerial Regulation)",
            "A": "Arrêté (Decree)",
            "CMIN": "Circulaire ministérielle (Ministerial Circular)",
            "PA": "Publication administrative (Administrative Publication)",
            "RC": "Registre de Commerce (Commercial Registry)",
        }
        return labels.get(code, code)

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LU/LegalDatabase (Legilux) endpoints...")

        # Test SPARQL endpoint
        print("\n1. Testing SPARQL endpoint...")
        try:
            query = """
PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
SELECT (COUNT(?s) as ?count) WHERE { ?s a jolux:Act }
"""
            result = self._sparql_query(query)
            bindings = result.get("results", {}).get("bindings", [])
            if bindings:
                count = bindings[0].get("count", {}).get("value", "0")
                print(f"   Total Acts in database: {count}")
            else:
                print("   Could not get count")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test fetching a sample law
        print("\n2. Testing sample law fetch...")
        try:
            query = self._build_query(["LOI"], limit=1)
            result = self._sparql_query(query)
            bindings = result.get("results", {}).get("bindings", [])

            if bindings:
                binding = bindings[0]
                print(f"   ELI URI: {binding.get('act', {}).get('value', 'N/A')}")
                print(f"   Title: {binding.get('title', {}).get('value', 'N/A')[:80]}...")
                print(f"   Date: {binding.get('date', {}).get('value', 'N/A')}")

                html_url = binding.get("url", {}).get("value", "")
                if html_url:
                    print(f"   HTML URL: {html_url}")
                    text = self._fetch_html_text(html_url)
                    print(f"   Full text length: {len(text)} characters")
                    if text:
                        print(f"   Text preview: {text[:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document types
        print("\n3. Testing document types available...")
        try:
            query = """
PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
SELECT ?docType (COUNT(?docType) as ?cnt)
WHERE {
  ?act a jolux:Act .
  ?act jolux:typeDocument ?docType .
}
GROUP BY ?docType
ORDER BY DESC(?cnt)
LIMIT 10
"""
            result = self._sparql_query(query)
            bindings = result.get("results", {}).get("bindings", [])

            for binding in bindings[:5]:
                doc_type = binding.get("docType", {}).get("value", "").split("/")[-1]
                count = binding.get("cnt", {}).get("value", "0")
                print(f"   {doc_type}: {count} documents")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = LegalDatabaseScraper()

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
