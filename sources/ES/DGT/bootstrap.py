#!/usr/bin/env python3
"""
ES/DGT -- Spanish General Tax Directorate (Dirección General de Tributos)

Fetches binding tax rulings (consultas vinculantes) from the DGT PETETE database.

Strategy:
  - Uses the PETETE web interface API endpoints
  - Search endpoint: /consultas/do/search with date filters
  - Document endpoint: /consultas/do/document returns full ruling content
  - Requires proper browser-like headers for authentication

Endpoints:
  - Main page: https://petete.tributos.hacienda.gob.es/consultas/
  - Search: POST /consultas/do/search
  - Document: POST /consultas/do/document

Data:
  - Types: Binding tax rulings (consultas vinculantes), General consultations
  - Languages: Spanish
  - Period: 1997 to present (~68,000+ rulings)
  - License: Open Government Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (since last run)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urlencode
import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.rate_limiter import RateLimiter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ES.DGT")

# Base URL for PETETE system
BASE_URL = "https://petete.tributos.hacienda.gob.es"
SEARCH_URL = f"{BASE_URL}/consultas/do/search"
DOCUMENT_URL = f"{BASE_URL}/consultas/do/document"
MAIN_URL = f"{BASE_URL}/consultas/"

# Headers that mimic browser - required for authentication
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": MAIN_URL,
    "Content-Type": "application/x-www-form-urlencoded",
}


class DGTScraper(BaseScraper):
    """
    Scraper for ES/DGT -- Spanish General Tax Directorate.
    Country: ES
    URL: https://petete.tributos.hacienda.gob.es

    Data types: doctrine (binding tax rulings)
    Auth: none (session-based, requires browser-like headers)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(BROWSER_HEADERS)
        self.session.verify = False  # SSL certificate issue with this site
        self.rate_limiter = RateLimiter(requests_per_second=0.5)  # Be gentle

        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _init_session(self):
        """Initialize session by visiting main page to get cookies."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(MAIN_URL, timeout=30)
            resp.raise_for_status()
            logger.debug(f"Session initialized, cookies: {dict(self.session.cookies)}")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize session: {e}")
            return False

    def _search(self, from_date: str = None, to_date: str = None, page: int = 1) -> Dict[str, Any]:
        """
        Search for binding rulings within a date range.

        Args:
            from_date: Start date in DD/MM/YYYY format
            to_date: End date in DD/MM/YYYY format
            page: Page number (1-indexed)

        Returns:
            Dict with 'total', 'total_pages', and 'documents' list
        """
        # Build search params
        params = {
            "type2": "on",  # Binding rulings only
            "NMCMP_1": "NUM-CONSULTA",
            "VLCMP_1": "",
            "OPCMP_1": ".Y",
            "NMCMP_2": "FECHA-SALIDA",
            "OPCMP_2": ".Y",
            "cmpOrder": "FECHA-SALIDA",
            "dirOrder": "1",  # Descending
            "tab": "2",  # Tab 2 = binding rulings
            "page": str(page),
        }

        if from_date and to_date:
            params["dateIni_2"] = from_date
            params["dateEnd_2"] = to_date
            params["VLCMP_2"] = f"{from_date}..{to_date}"

        try:
            self.rate_limiter.wait()
            resp = self.session.post(SEARCH_URL, data=params, timeout=60)
            resp.raise_for_status()

            html_content = resp.text
            return self._parse_search_results(html_content)

        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {"total": 0, "total_pages": 0, "documents": []}

    def _parse_search_results(self, html_content: str) -> Dict[str, Any]:
        """Parse search results HTML to extract document IDs and metadata."""
        result = {"total": 0, "total_pages": 1, "documents": []}

        # Extract total count
        total_match = re.search(r'updateNumResults\("2",\s*"(\d+)"\)', html_content)
        if total_match:
            result["total"] = int(total_match.group(1))

        # Extract total pages
        pages_match = re.search(r'de <span id="total_pages">(\d+)</span>', html_content)
        if pages_match:
            result["total_pages"] = int(pages_match.group(1))

        # Extract document IDs and metadata
        # Pattern: <td id="doc_XXXXX" onClick="return viewDocument(XXXXX, 2);"
        doc_pattern = re.compile(
            r'<td id="doc_(\d+)"[^>]*>.*?'
            r'<span class="NUM-CONSULTA"><strong>\s*([^<]+)\s*</strong></span>'
            r'(?:.*?<span class="DESCRIPCION-HECHOS">\s*([^<]+)\s*</span>)?'
            r'(?:.*?<span class="CUESTION-PLANTEADA"><i>\s*([^<]+)\s*</i></span>)?',
            re.DOTALL
        )

        for match in doc_pattern.finditer(html_content):
            doc_id = match.group(1)
            num_consulta = match.group(2).strip()
            descripcion = match.group(3).strip() if match.group(3) else ""
            cuestion = match.group(4).strip() if match.group(4) else ""

            result["documents"].append({
                "doc_id": doc_id,
                "num_consulta": num_consulta,
                "descripcion_preview": descripcion[:200] if descripcion else "",
                "cuestion_preview": cuestion[:200] if cuestion else "",
            })

        return result

    def _fetch_document(self, doc_id: str, query: str = "") -> Dict[str, Any]:
        """
        Fetch full document by its internal ID.

        Args:
            doc_id: Internal document ID from search results
            query: Original query string for context

        Returns:
            Dict with document fields including full text
        """
        params = {
            "doc": doc_id,
            "tab": "2",
            "query": query or ".T",
        }

        try:
            self.rate_limiter.wait()
            resp = self.session.post(DOCUMENT_URL, data=params, timeout=60)
            resp.raise_for_status()

            if resp.status_code == 401:
                logger.warning(f"Auth required for doc {doc_id}, reinitializing session")
                self._init_session()
                resp = self.session.post(DOCUMENT_URL, data=params, timeout=60)
                resp.raise_for_status()

            return self._parse_document(resp.text, doc_id)

        except Exception as e:
            logger.error(f"Failed to fetch document {doc_id}: {e}")
            return {}

    def _parse_document(self, html_content: str, doc_id: str) -> Dict[str, Any]:
        """Parse document HTML to extract all fields including full text."""
        doc = {"doc_id": doc_id}

        # Check for auth error
        if "HTTP Status 401" in html_content:
            logger.warning(f"Auth error for doc {doc_id}")
            return {}

        # Extract fields from table rows
        field_patterns = {
            "num_consulta": r'<tr class="NUM-CONSULTA">.*?<p class="NUM-CONSULTA"[^>]*>\s*([^<]+)\s*</p>',
            "organo": r'<tr class="ORGANO">.*?<p class="ORGANO"[^>]*>\s*([^<]+)\s*</p>',
            "fecha_salida": r'<tr class="FECHA-SALIDA">.*?<p class="FECHA-SALIDA"[^>]*>\s*([^<]+)\s*</p>',
            "normativa": r'<tr class="NORMATIVA">.*?<p class="NORMATIVA"[^>]*>\s*([^<]+)\s*</p>',
        }

        for field, pattern in field_patterns.items():
            match = re.search(pattern, html_content, re.DOTALL)
            if match:
                doc[field] = html.unescape(match.group(1).strip())

        # Extract multi-paragraph fields
        multi_fields = {
            "descripcion_hechos": "DESCRIPCION-HECHOS",
            "cuestion_planteada": "CUESTION-PLANTEADA",
            "contestacion_completa": "CONTESTACION-COMPL",
        }

        for field, class_name in multi_fields.items():
            # Find all paragraphs with this class
            pattern = rf'<p class="{class_name}"[^>]*>([^<]+(?:<[^>]+>[^<]*</[^>]+>[^<]*)*)</p>'
            matches = re.findall(pattern, html_content, re.DOTALL)
            if matches:
                texts = [self._clean_html(m) for m in matches]
                doc[field] = "\n\n".join(texts)

        return doc

    def _clean_html(self, text: str) -> str:
        """Clean HTML content: decode entities, remove tags, normalize whitespace."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', text)

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    def _date_range_generator(self, start_date: datetime, end_date: datetime, days_per_chunk: int = 30):
        """Generate date ranges for chunked fetching."""
        current = start_date
        while current < end_date:
            chunk_end = min(current + timedelta(days=days_per_chunk), end_date)
            yield (current, chunk_end)
            current = chunk_end + timedelta(days=1)

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all binding tax rulings from the DGT PETETE database.

        Paginates through results sorted by date (most recent first).
        """
        # Initialize session
        if not self._init_session():
            logger.error("Failed to initialize session")
            return

        total_fetched = 0
        page = 1

        while True:
            logger.info(f"Fetching page {page}")
            result = self._search(page=page)

            if not result["documents"]:
                logger.info(f"No more documents at page {page}")
                break

            logger.info(f"Found {len(result['documents'])} documents on page {page}")

            for doc_meta in result["documents"]:
                doc_id = doc_meta["doc_id"]

                # Fetch full document
                doc = self._fetch_document(doc_id)

                if not doc or not doc.get("contestacion_completa"):
                    logger.warning(f"No full text for {doc_meta.get('num_consulta', doc_id)}")
                    continue

                total_fetched += 1
                yield doc

            # Check if we've reached the last page
            if page >= result["total_pages"]:
                logger.info("Reached last page")
                break

            page += 1

            # Safety limit
            if page > 3500:  # ~70k docs / 20 per page
                logger.warning("Reached maximum page limit")
                break

        logger.info(f"Completed fetching {total_fetched} rulings")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Since results are sorted by date descending, we fetch pages until
        we find documents older than our cutoff date.
        """
        if not self._init_session():
            return

        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_str}")

        page = 1
        found_old = False

        while not found_old:
            result = self._search(page=page)

            if not result["documents"]:
                break

            for doc_meta in result["documents"]:
                doc_id = doc_meta["doc_id"]
                doc = self._fetch_document(doc_id)

                if not doc or not doc.get("contestacion_completa"):
                    continue

                # Check if this doc is older than our cutoff
                fecha = doc.get("fecha_salida", "")
                if fecha:
                    try:
                        parts = fecha.split("/")
                        if len(parts) == 3:
                            doc_date = datetime(int(parts[2]), int(parts[1]), int(parts[0]))
                            if doc_date < since:
                                found_old = True
                                break
                    except:
                        pass

                yield doc

            if page >= result["total_pages"]:
                break

            page += 1

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        num_consulta = raw.get("num_consulta", "")
        doc_id = raw.get("doc_id", "")

        # Parse date from DD/MM/YYYY to ISO format
        fecha = raw.get("fecha_salida", "")
        date_iso = ""
        if fecha:
            try:
                parts = fecha.split("/")
                if len(parts) == 3:
                    date_iso = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                date_iso = fecha

        # Build full text: combine all substantive fields
        text_parts = []
        if raw.get("descripcion_hechos"):
            text_parts.append("DESCRIPCIÓN DE HECHOS:\n" + raw["descripcion_hechos"])
        if raw.get("cuestion_planteada"):
            text_parts.append("CUESTIÓN PLANTEADA:\n" + raw["cuestion_planteada"])
        if raw.get("contestacion_completa"):
            text_parts.append("CONTESTACIÓN:\n" + raw["contestacion_completa"])

        full_text = "\n\n".join(text_parts)

        # Build title from consulta number and normativa
        normativa = raw.get("normativa", "")
        title = f"Consulta Vinculante {num_consulta}"
        if normativa:
            title += f" - {normativa[:100]}"

        return {
            # Required base fields
            "_id": num_consulta or f"DGT_{doc_id}",
            "_source": "ES/DGT",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_iso,
            "url": f"{MAIN_URL}?num_consulta={num_consulta}",
            # Additional metadata
            "num_consulta": num_consulta,
            "organo": raw.get("organo", ""),
            "normativa": normativa,
            "fecha_salida": fecha,
            "descripcion_hechos": raw.get("descripcion_hechos", ""),
            "cuestion_planteada": raw.get("cuestion_planteada", ""),
            "language": "es",
            "jurisdiction": "ES",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing DGT PETETE system...")

        # Initialize session
        print("\n1. Initializing session...")
        if self._init_session():
            print("   Session initialized successfully")
            print(f"   Cookies: {dict(self.session.cookies)}")
        else:
            print("   ERROR: Failed to initialize session")
            return

        # Test search (no date filter - just paginate)
        print("\n2. Testing search endpoint...")
        result = {"documents": []}
        try:
            result = self._search(page=1)
            print(f"   Total rulings found: {result['total']}")
            print(f"   Total pages: {result['total_pages']}")
            print(f"   Documents on page 1: {len(result['documents'])}")

            if result['documents']:
                doc = result['documents'][0]
                print(f"   Sample: {doc.get('num_consulta', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document fetch
        print("\n3. Testing document fetch...")
        try:
            if result['documents']:
                doc_id = result['documents'][0]['doc_id']
                full_doc = self._fetch_document(doc_id)
                if full_doc:
                    print(f"   Document ID: {doc_id}")
                    print(f"   Num consulta: {full_doc.get('num_consulta', 'N/A')}")
                    print(f"   Fecha: {full_doc.get('fecha_salida', 'N/A')}")
                    text = full_doc.get('contestacion_completa', '')
                    print(f"   Full text length: {len(text)} chars")
                    if text:
                        print(f"   Sample: {text[:200]}...")
                else:
                    print("   ERROR: Failed to fetch document")
            else:
                print("   No documents to test (search returned empty)")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = DGTScraper()

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
