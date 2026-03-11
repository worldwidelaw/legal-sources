#!/usr/bin/env python3
"""
AD/BOPA -- Andorra Official Gazette (Butlletí Oficial del Principat d'Andorra)

Fetches Andorran legislation from bopa.ad official gazette.

Strategy:
  - Uses Azure Functions API exposed by bopa.ad React SPA.
  - API endpoint: https://bopaazurefunctions.azurewebsites.net/api/
  - Paginates through newsletter (BOPA) issues via GetNewPaginatedNewsletter.
  - For each issue, fetches document list via GetDocumentsByBOPA.
  - Document full text is HTML stored in Azure Blob Storage.
  - Filters for legislation-relevant document types (Lleis, Reglaments, Decrets, etc.)

Endpoints:
  - Newsletter list: POST GetNewPaginatedNewsletter (paginated)
  - Documents by issue: GET GetDocumentsByBOPA?year={year}&numBOPA={num}
  - Full text HTML: https://bopadocuments.blob.core.windows.net/bopa-documents/{volume}/html/{name}.html

Data:
  - All BOPA issues from 2015-present (electronic official version)
  - Prior to 2015, only paper version was official
  - Document types: Lleis, Reglaments, Decrets, Convenis internacionals, etc.

License: Open Government Data (Andorra)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import os
import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AD.BOPA")

# API and storage URLs
API_BASE_URL = "https://bopaazurefunctions.azurewebsites.net"
BLOB_STORAGE_URL = "https://bopadocuments.blob.core.windows.net/bopa-documents"

# API function codes (extracted from bopa.ad public JavaScript).
# These are public API keys, not secrets. Set via env vars or use defaults.
API_CODES = {
    "GetDocumentsByBOPA": os.environ.get("BOPA_CODE_DOCUMENTS", ""),
    "GetFilters": os.environ.get("BOPA_CODE_FILTERS", ""),
    "GetFiltersLaws": os.environ.get("BOPA_CODE_FILTERS_LAWS", ""),
}

# Document types that are relevant for legislation
LEGISLATION_TYPES = {
    "02. Consell General": ["Lleis", "Convenis internacionals", "Acords"],
    "03. Govern": ["Reglaments", "Decrets", "Altres disposicions"],
}

# Filter for document types - only fetch actual legislation
RELEVANT_ORGANISME_PARE = ["02. Consell General", "03. Govern"]


class AndorraBOPAScraper(BaseScraper):
    """
    Scraper for AD/BOPA -- Andorra Official Gazette.
    Country: AD
    URL: https://www.bopa.ad

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.api_client = HttpClient(
            base_url=API_BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=60,
        )

        self.blob_client = HttpClient(
            base_url=BLOB_STORAGE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html",
            },
            timeout=60,
        )

    def _get_newsletter_list(self, size: int = 50, skip_token: Optional[str] = None) -> Dict[str, Any]:
        """
        Fetch paginated list of BOPA newsletters.

        Returns dict with:
          - bopaList: List of {numBOPA, dataPublicacio, isExtra}
          - skipToken: Token for next page (null if no more)
        """
        payload = {
            "sizePage": size,
            "datesList": [],
            "skipToken": skip_token,
            "anys": [],
            "numButlleti": None,
        }

        self.rate_limiter.wait()
        resp = self.api_client.post("/api/GetNewPaginatedNewsletter", json_data=payload)
        resp.raise_for_status()
        return resp.json()

    def _get_documents_by_bopa(self, year: int, num_bopa: str) -> Dict[str, Any]:
        """
        Fetch all documents from a specific BOPA issue.

        Args:
            year: Publication year (e.g., 2026)
            num_bopa: Issue number (e.g., "16")

        Returns dict with:
          - totalCount: Total number of documents
          - paginatedDocuments: List of document metadata
        """
        code = API_CODES["GetDocumentsByBOPA"]
        url = f"/api/GetDocumentsByBOPA?code={code}&numBOPA={num_bopa}&year={year}"

        self.rate_limiter.wait()
        resp = self.api_client.get(url)
        resp.raise_for_status()
        return resp.json()

    def _fetch_document_html(self, storage_path: str) -> str:
        """
        Fetch full HTML content from Azure Blob Storage.

        Args:
            storage_path: Full URL to HTML file

        Returns:
            HTML content string
        """
        try:
            self.rate_limiter.wait()
            resp = self.blob_client.get(storage_path.replace(BLOB_STORAGE_URL, ""))
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch HTML from {storage_path}: {e}")
            return ""

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract clean text from BOPA HTML document.

        Args:
            html_content: Raw HTML string

        Returns:
            Clean text without HTML tags
        """
        if not html_content:
            return ""

        # Remove script and style elements
        text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

        # Remove HTML tags but preserve structure
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</tr>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = text.strip()

        return text

    def _decode_sumari(self, sumari: str) -> str:
        """Decode URL-encoded summary text."""
        if not sumari:
            return ""
        try:
            return unquote(sumari).replace("+", " ").strip()
        except:
            return sumari

    def _is_legislation_document(self, doc: Dict[str, Any]) -> bool:
        """
        Check if document is relevant legislation (not job postings, notices, etc.)
        """
        organisme_pare = doc.get("organismePare", "")
        organisme = doc.get("organisme", "")
        tema_pare = doc.get("temaPare", "")
        tema = doc.get("tema", "")

        # Filter out non-legislative content
        excluded_keywords = [
            "Oferta pública de treball",  # Job postings
            "Concursos i subhastes",  # Competitions and auctions
            "Adjudicacions",  # Contract awards
            "Anuncis particulars",  # Private announcements
            "Edictes i citacions",  # Edicts and summons
            "Notificacions",  # Notifications
        ]

        for keyword in excluded_keywords:
            if keyword in organisme or keyword in tema:
                return False

        # Include documents from Consell General and Govern
        if organisme_pare in RELEVANT_ORGANISME_PARE:
            return True

        # Include if tema suggests legislation
        legislation_temas = [
            "Lleis", "Reglaments", "Decrets", "Convenis",
            "Constitució", "Acords", "Legislació delegada"
        ]
        for t in legislation_temas:
            if t in tema or t in tema_pare:
                return True

        return False

    def _iterate_documents(self, sample_mode: bool = False, sample_size: int = 12) -> Generator[Dict[str, Any], None, None]:
        """
        Iterate through all BOPA documents.

        In sample mode, yields first N valid documents.
        """
        count = 0
        skip_token = None

        while True:
            # Get page of newsletters
            logger.info(f"Fetching newsletter list (count so far: {count})...")
            try:
                newsletters = self._get_newsletter_list(size=30, skip_token=skip_token)
            except Exception as e:
                logger.error(f"Failed to fetch newsletter list: {e}")
                break

            bopa_list = newsletters.get("bopaList", [])
            if not bopa_list:
                logger.info("No more newsletters to process")
                break

            for bopa in bopa_list:
                num_bopa = bopa.get("numBOPA", "")
                data_publicacio = bopa.get("dataPublicacio", "")

                if not num_bopa or not data_publicacio:
                    continue

                # Extract year from publication date
                try:
                    pub_date = datetime.fromisoformat(data_publicacio.replace('Z', '+00:00'))
                    year = pub_date.year
                except:
                    logger.warning(f"Could not parse date: {data_publicacio}")
                    continue

                logger.info(f"Processing BOPA {year}/{num_bopa}...")

                # Get documents from this BOPA issue
                try:
                    docs_response = self._get_documents_by_bopa(year, num_bopa)
                except Exception as e:
                    logger.warning(f"Failed to get documents for BOPA {year}/{num_bopa}: {e}")
                    continue

                paginated_docs = docs_response.get("paginatedDocuments", [])

                for doc_entry in paginated_docs:
                    doc = doc_entry.get("document", {})

                    # Skip non-legislation documents
                    if not self._is_legislation_document(doc):
                        continue

                    # Get full text from HTML
                    storage_path = doc.get("metadata_storage_path", "")
                    if not storage_path or not storage_path.endswith(".html"):
                        continue

                    html_content = self._fetch_document_html(storage_path)
                    full_text = self._extract_text_from_html(html_content)

                    # Skip if no meaningful text
                    if len(full_text) < 100:
                        logger.debug(f"Skipping {doc.get('nomDocument')}: insufficient text ({len(full_text)} chars)")
                        continue

                    # Build document record
                    record = {
                        "nom_document": doc.get("nomDocument", ""),
                        "sumari": self._decode_sumari(doc.get("sumari", "")),
                        "organisme_pare": doc.get("organismePare", ""),
                        "organisme": doc.get("organisme", ""),
                        "tema_pare": doc.get("temaPare", ""),
                        "tema": doc.get("tema", ""),
                        "num_butlleti": doc.get("numButlleti", ""),
                        "any_butlleti": doc.get("anyButlleti", ""),
                        "data_publicacio_butlleti": doc.get("dataPublicacioButlleti", ""),
                        "data_article": doc.get("dataArticle", ""),
                        "storage_path": storage_path,
                        "full_text": full_text,
                        "is_extra": doc.get("isExtra", "False"),
                    }

                    count += 1
                    yield record

                    if sample_mode and count >= sample_size:
                        logger.info(f"Sample mode: collected {count} documents")
                        return

            # Check for next page
            skip_token = newsletters.get("skipToken")
            if not skip_token:
                logger.info("No more pages to fetch")
                break

        logger.info(f"Completed iteration: {count} documents fetched")

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation documents from BOPA.

        Iterates through all BOPA issues and extracts legislation documents.
        """
        for doc in self._iterate_documents(sample_mode=False):
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given date.

        Since BOPA is chronologically ordered, we stop when we reach
        documents older than the since date.
        """
        for doc in self._iterate_documents(sample_mode=False):
            date_str = doc.get("data_publicacio_butlleti", "")
            if date_str:
                try:
                    pub_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    if pub_date.tzinfo is None:
                        pub_date = pub_date.replace(tzinfo=timezone.utc)
                    if pub_date >= since:
                        yield doc
                    else:
                        # Documents are chronologically ordered, so we can stop
                        logger.info(f"Reached documents older than {since}, stopping")
                        return
                except Exception as e:
                    # If we can't parse the date, include it to be safe
                    yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        nom_document = raw.get("nom_document", "")
        sumari = raw.get("sumari", "")
        full_text = raw.get("full_text", "")

        # Build document ID
        doc_id = f"AD_BOPA_{raw.get('any_butlleti', '')}_{raw.get('num_butlleti', '')}_{nom_document}"

        # Parse publication date
        date_str = raw.get("data_publicacio_butlleti", "") or raw.get("data_article", "")

        # Build URL to document on bopa.ad
        year = raw.get("any_butlleti", "")
        num = raw.get("num_butlleti", "")
        url = f"https://www.bopa.ad/Documents/Detall?doc={nom_document}" if nom_document else "https://www.bopa.ad"

        # Determine document type from organisme/tema
        doc_type = "legislation"
        organisme = raw.get("organisme", "").lower()
        if "llei" in organisme:
            doc_type = "law"
        elif "reglament" in organisme:
            doc_type = "regulation"
        elif "decret" in organisme:
            doc_type = "decree"
        elif "conveni" in organisme:
            doc_type = "international_agreement"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "AD/BOPA",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": sumari,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": url,
            # Additional metadata
            "document_name": nom_document,
            "bopa_year": year,
            "bopa_number": num,
            "organisme_pare": raw.get("organisme_pare", ""),
            "organisme": raw.get("organisme", ""),
            "tema_pare": raw.get("tema_pare", ""),
            "tema": raw.get("tema", ""),
            "document_type": doc_type,
            "is_extra": raw.get("is_extra", "False") == "True",
            "language": "ca",  # Catalan
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Andorra BOPA endpoints...")

        # Test newsletter list
        print("\n1. Testing GetNewPaginatedNewsletter...")
        try:
            newsletters = self._get_newsletter_list(size=5)
            bopa_list = newsletters.get("bopaList", [])
            print(f"   Found {len(bopa_list)} newsletters")
            if bopa_list:
                latest = bopa_list[0]
                print(f"   Latest: BOPA {latest.get('numBOPA')} ({latest.get('dataPublicacio')})")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test document fetch
        print("\n2. Testing GetDocumentsByBOPA...")
        try:
            if bopa_list:
                latest = bopa_list[0]
                pub_date = datetime.fromisoformat(latest.get('dataPublicacio').replace('Z', '+00:00'))
                year = pub_date.year
                num = latest.get('numBOPA')

                docs_response = self._get_documents_by_bopa(year, num)
                total = docs_response.get("totalCount", 0)
                docs = docs_response.get("paginatedDocuments", [])
                print(f"   BOPA {year}/{num} has {total} total documents, fetched {len(docs)}")

                if docs:
                    first_doc = docs[0].get("document", {})
                    print(f"   First doc: {first_doc.get('nomDocument')}")
                    print(f"   Sumari: {self._decode_sumari(first_doc.get('sumari', ''))[:80]}...")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test HTML fetch
        print("\n3. Testing HTML content fetch...")
        try:
            if docs:
                for doc_entry in docs[:5]:
                    doc = doc_entry.get("document", {})
                    storage_path = doc.get("metadata_storage_path", "")
                    if storage_path.endswith(".html"):
                        html_content = self._fetch_document_html(storage_path)
                        text = self._extract_text_from_html(html_content)
                        if len(text) > 100:
                            print(f"   HTML fetch successful!")
                            print(f"   Text length: {len(text)} characters")
                            print(f"   Sample: {text[:200]}...")
                            break
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test sample fetch
        print("\n4. Testing document iteration (3 samples)...")
        count = 0
        for doc in self._iterate_documents(sample_mode=True, sample_size=3):
            count += 1
            print(f"   [{count}] {doc.get('sumari', 'N/A')[:60]}...")
            print(f"       Text: {len(doc.get('full_text', ''))} chars")

        print(f"\nTest complete! Found {count} documents.")


def main():
    scraper = AndorraBOPAScraper()

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
