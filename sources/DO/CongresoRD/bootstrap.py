#!/usr/bin/env python3
"""
DO/CongresoRD -- Dominican Republic Legislation (Consultoría Jurídica)

Fetches laws, decrees, resolutions, and regulations from the Consultoría
Jurídica del Poder Ejecutivo.

Strategy:
  1. POST search to get all document IDs for each document type
  2. For each document: fetch JSON metadata + download PDF for full text
  3. Extract text from PDF using PyPDF2

Document types:
  - Leyes (Laws): ~12,485
  - Decretos (Decrees): ~15,475
  - Resoluciones (Resolutions): ~17,237
  - Reglamentos (Regulations): ~749
  - Varios (Miscellaneous): ~87

API endpoints:
  - Search: POST /Consulta/Home/Search?Length=7 (returns HTML table with IDs)
  - Metadata: GET /Consulta/Home/DocumentInfo?documentId={id} (returns JSON)
  - PDF: GET /Consulta/Home/FileManagement?documentId={id}&managementType=1

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick API connectivity test
"""

import io
import sys
import re
import json
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DO.CongresoRD")

BASE_URL = "https://www.consultoria.gov.do"

# Document type codes and names
DOCUMENT_TYPES = {
    1: "Leyes",
    3: "Decretos",
    4: "Reglamentos",
    5: "Varios",
    7: "Resoluciones",
}

# Spanish month mapping for date parsing
SPANISH_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4,
    "mayo": 5, "junio": 6, "julio": 7, "agosto": 8,
    "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12,
}


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="DO/CongresoRD",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="legislation",
    ) or ""

def _parse_spanish_date(date_str: str) -> Optional[str]:
    """Parse dates like '30 de September de 1920' into ISO format."""
    if not date_str:
        return None
    match = re.match(r"(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", date_str, re.IGNORECASE)
    if match:
        day = int(match.group(1))
        month_str = match.group(2).lower()
        year = int(match.group(3))
        month = SPANISH_MONTHS.get(month_str)
        if month:
            try:
                return f"{year:04d}-{month:02d}-{day:02d}"
            except ValueError:
                pass
    # Try DD/MM/YYYY format
    match = re.match(r"(\d{2})/(\d{2})/(\d{4})", date_str)
    if match:
        return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
    return None


class CongresoRDScraper(BaseScraper):
    """
    Scraper for DO/CongresoRD -- Dominican Republic legislation.
    Country: DO
    URL: https://www.consultoria.gov.do/consulta/

    Data types: legislation (laws, decrees, resolutions, regulations)
    Auth: none (Open Data, CSRF token required for search)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=120,
        )
        self._csrf_token = None
        self._session_cookies = None

    def _get_csrf_token(self):
        """Fetch the search page to get CSRF token and session cookies."""
        if self._csrf_token:
            return

        self.rate_limiter.wait()
        resp = self.client.get("/consulta/")
        resp.raise_for_status()

        # Extract CSRF token
        match = re.search(
            r'__RequestVerificationToken.*?value="([^"]+)"',
            resp.text
        )
        if not match:
            raise RuntimeError("Could not extract CSRF token from Consultoria")

        self._csrf_token = match.group(1)
        self._session_cookies = resp.cookies
        logger.info("CSRF token acquired")

    def _search_documents(self, doc_type_code: int) -> list[dict]:
        """
        Search for all documents of a given type.
        Returns list of dicts with document_id, number, title, gaceta, date.
        """
        self._get_csrf_token()
        self.rate_limiter.wait()

        data = {
            "__RequestVerificationToken": self._csrf_token,
            "DocumentTypeCode": str(doc_type_code),
            "Year": "",
            "Number": "",
            "SearchText": "",
            "DocumentNumber": "",
            "DocumentTitle": "",
            "GacetaOficial": "",
            "PublicationYear": "",
        }

        resp = self.client.post(
            "/Consulta/Home/Search?Length=7",
            data=data,
            cookies=self._session_cookies,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            },
        )
        resp.raise_for_status()
        content = resp.text

        # Parse HTML table rows to extract document data
        documents = []
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", content, re.DOTALL)

        for row in rows:
            # Extract document IDs from links
            id_match = re.search(r"documentId=(\d+)", row)
            if not id_match:
                continue

            doc_id = int(id_match.group(1))

            # Extract cells
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            if len(cells) < 5:
                continue

            def clean_cell(c):
                return re.sub(r"<[^>]+>", "", c).strip()

            documents.append({
                "document_id": doc_id,
                "document_type_code": doc_type_code,
                "document_type": DOCUMENT_TYPES.get(doc_type_code, "Unknown"),
                "number": clean_cell(cells[1]),
                "title": clean_cell(cells[2]),
                "gaceta": clean_cell(cells[3]),
                "date_raw": clean_cell(cells[4]),
            })

        # Deduplicate by document_id (IDs appear twice in view/download links)
        seen = set()
        unique = []
        for doc in documents:
            if doc["document_id"] not in seen:
                seen.add(doc["document_id"])
                unique.append(doc)

        logger.info(
            f"Found {len(unique)} {DOCUMENT_TYPES.get(doc_type_code, '?')} documents"
        )
        return unique

    def _fetch_document_metadata(self, doc_id: int) -> Optional[dict]:
        """Fetch JSON metadata for a document."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(
                f"/Consulta/Home/DocumentInfo?documentId={doc_id}"
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("info")
        except Exception as e:
            logger.debug(f"Metadata fetch failed for {doc_id}: {e}")
            return None

    def _fetch_document_pdf_text(self, doc_id: int) -> str:
        """Download PDF and extract text."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(
                f"/Consulta/Home/FileManagement?documentId={doc_id}&managementType=1"
            )
            if resp.status_code != 200:
                return ""

            content_type = resp.headers.get("content-type", "")
            if "pdf" not in content_type and not resp.content[:5] == b"%PDF-":
                return ""

            return _extract_pdf_text(resp.content)
        except Exception as e:
            logger.debug(f"PDF fetch failed for {doc_id}: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all document types."""
        for type_code, type_name in DOCUMENT_TYPES.items():
            logger.info(f"Searching {type_name}...")
            try:
                documents = self._search_documents(type_code)
            except Exception as e:
                logger.error(f"Search failed for {type_name}: {e}")
                continue

            for i, doc in enumerate(documents):
                if i > 0 and i % 100 == 0:
                    logger.info(
                        f"Progress: {i}/{len(documents)} {type_name} processed"
                    )

                # Fetch metadata
                meta = self._fetch_document_metadata(doc["document_id"])

                # Fetch PDF text
                pdf_text = self._fetch_document_pdf_text(doc["document_id"])

                doc["metadata"] = meta
                doc["pdf_text"] = pdf_text

                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents added since the given date (search by year)."""
        current_year = datetime.now().year
        since_year = since.year

        for type_code, type_name in DOCUMENT_TYPES.items():
            try:
                documents = self._search_documents(type_code)
            except Exception as e:
                logger.error(f"Search failed for {type_name}: {e}")
                continue

            for doc in documents:
                # Filter by date
                date = _parse_spanish_date(doc.get("date_raw", ""))
                if date and date >= since.strftime("%Y-%m-%d"):
                    meta = self._fetch_document_metadata(doc["document_id"])
                    pdf_text = self._fetch_document_pdf_text(doc["document_id"])
                    doc["metadata"] = meta
                    doc["pdf_text"] = pdf_text
                    yield doc

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document data into standard schema."""
        pdf_text = raw.get("pdf_text", "").strip()
        if not pdf_text or len(pdf_text) < 50:
            logger.debug(
                f"Skipping {raw.get('document_id')}: no/insufficient PDF text"
            )
            return None

        meta = raw.get("metadata") or {}
        title = meta.get("Titulo") or raw.get("title", "")
        number = meta.get("Numero") or raw.get("number", "")
        doc_type = raw.get("document_type", "")

        # Parse date from metadata or table
        date = _parse_spanish_date(meta.get("FechaPromulgacion", ""))
        if not date:
            date = _parse_spanish_date(raw.get("date_raw", ""))

        # Build a unique ID
        doc_id = f"DO-{doc_type}-{number}".replace(" ", "_")

        return {
            "_id": doc_id,
            "_source": "DO/CongresoRD",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": pdf_text,
            "date": date,
            "url": f"{BASE_URL}/Consulta/Home/FileManagement?documentId={raw['document_id']}&managementType=1",
            "document_number": number,
            "document_type": doc_type,
            "gaceta_oficial": meta.get("Gaceta") or raw.get("gaceta", ""),
            "president": meta.get("Presidente"),
            "consultor": meta.get("Consultor"),
            "observation": meta.get("Observacion"),
            "publication_date": _parse_spanish_date(
                meta.get("FechaPublicacion", "")
            ),
        }


# ── CLI ─────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="DO/CongresoRD scraper")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample", action="store_true", help="Sample mode (10 records)"
    )
    args = parser.parse_args()

    scraper = CongresoRDScraper()

    if args.command == "test-api":
        logger.info("Testing Consultoria API...")
        scraper._get_csrf_token()
        logger.info(f"CSRF token: {scraper._csrf_token[:20]}...")

        # Search for a small set
        logger.info("Searching Reglamentos (smallest type)...")
        docs = scraper._search_documents(4)  # Reglamentos
        logger.info(f"Found {len(docs)} Reglamentos")

        if docs:
            doc = docs[0]
            logger.info(f"First doc: {doc['title'][:60]}...")
            logger.info(f"Document ID: {doc['document_id']}")

            meta = scraper._fetch_document_metadata(doc["document_id"])
            logger.info(f"Metadata: {json.dumps(meta, ensure_ascii=False)[:200]}")

            pdf_text = scraper._fetch_document_pdf_text(doc["document_id"])
            logger.info(f"PDF text length: {len(pdf_text)}")
            if pdf_text:
                logger.info(f"PDF preview: {pdf_text[:200]}")
                logger.info("API test PASSED")
            else:
                logger.warning("No PDF text extracted")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        last_run = scraper.status.get("last_run")
        if last_run:
            since = datetime.fromisoformat(last_run)
        else:
            since = datetime(2020, 1, 1, tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
