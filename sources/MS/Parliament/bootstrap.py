#!/usr/bin/env python3
"""
MS/Parliament -- Montserrat Legislative Assembly

Fetches ~599 legal documents (Acts, Bills, Laws, Resolutions, SROs) with full
text from parliament.ms using the WordPress REST API.

Strategy:
  - Enumerate documents via WP REST API (/wp-json/wp/v2/legal_document)
  - For each document, fetch attached PDF via media endpoint
  - Download PDF and extract text with pdfplumber

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import hashlib
import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MS.Parliament")

API_BASE = "https://parliament.ms/wp-json/wp/v2"
SITE_BASE = "https://parliament.ms"
MAX_PDF_SIZE = 50 * 1024 * 1024  # 50 MB

# Taxonomy IDs for document-type terms
DOC_TYPE_MAP = {
    23: "act",
    24: "bill",
    27: "law",
    34: "resolution",
    25: "sro",
}

# Map API doc types to our internal types
TYPE_TO_INTERNAL = {
    "act": "legislation",
    "bill": "legislation",
    "law": "legislation",
    "resolution": "legislation",
    "sro": "legislation",
}


class MSParliamentScraper(BaseScraper):
    """Scraper for MS/Parliament -- Montserrat Legislative Assembly."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _api_get(self, url: str, params: dict = None, timeout: int = 60) -> Optional[requests.Response]:
        """Make an API GET request with retry logic."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, params=params, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _fetch_all_documents(self, max_records: int = None) -> List[Dict]:
        """Fetch all legal documents from the WP REST API."""
        documents = []
        page = 1

        while True:
            if max_records and len(documents) >= max_records:
                break

            url = f"{API_BASE}/legal_document"
            params = {"per_page": 100, "page": page}
            resp = self._api_get(url, params=params)
            if resp is None:
                break

            data = resp.json()
            if not data:
                break

            documents.extend(data)
            logger.info(f"Fetched page {page}: {len(data)} documents (total: {len(documents)})")

            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1

        if max_records:
            documents = documents[:max_records]
        return documents

    def _get_pdf_url(self, doc_id: int) -> Optional[str]:
        """Get the PDF URL for a document via its media attachments."""
        url = f"{API_BASE}/media"
        params = {"parent": doc_id, "per_page": 10}
        resp = self._api_get(url, params=params)
        if resp is None:
            return None

        media_items = resp.json()
        for item in media_items:
            if item.get("mime_type") == "application/pdf":
                return item.get("source_url")
        return None

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text via pdfplumber."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(pdf_url, timeout=120, stream=True)
                if resp.status_code != 200:
                    logger.warning(f"PDF download failed ({resp.status_code}): {pdf_url}")
                    return ""
                break
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF download attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
                else:
                    return ""

        cl = resp.headers.get("Content-Length")
        if cl and int(cl) > MAX_PDF_SIZE:
            logger.warning(f"PDF too large ({int(cl)} bytes): {pdf_url}")
            return ""

        pdf_bytes = resp.content
        if len(pdf_bytes) > MAX_PDF_SIZE:
            logger.warning(f"PDF too large ({len(pdf_bytes)} bytes): {pdf_url}")
            return ""

        try:
            pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
            parts = []
            for p in pdf.pages:
                text = p.extract_text()
                if text:
                    parts.append(text)
                try:
                    p.flush_cache(); p.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return "\n\n".join(parts).strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return ""

    def _get_doc_type(self, doc: Dict) -> str:
        """Get the human-readable document type from taxonomy IDs."""
        type_ids = doc.get("document-type", [])
        for tid in type_ids:
            if tid in DOC_TYPE_MAP:
                return DOC_TYPE_MAP[tid]
        return "unknown"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = raw.get("doc_type", "unknown")
        internal_type = TYPE_TO_INTERNAL.get(doc_type, "legislation")

        return {
            "_id": raw.get("doc_id", ""),
            "_source": "MS/Parliament",
            "_type": internal_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("publish_date", ""),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "doc_type": doc_type,
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        documents = self._fetch_all_documents(max_records=max_records)
        logger.info(f"Total documents to process: {len(documents)}")

        count = 0
        for doc in documents:
            doc_id = doc.get("id")
            title_raw = doc.get("title", {}).get("rendered", "")
            # Clean HTML entities from title
            title = re.sub(r'<[^>]+>', '', title_raw).strip()
            slug = doc.get("slug", "")
            date_str = doc.get("date", "")
            doc_type = self._get_doc_type(doc)
            link = doc.get("link", f"{SITE_BASE}/legal_document/{slug}/")

            # Parse date to ISO format
            publish_date = ""
            if date_str:
                try:
                    dt = datetime.fromisoformat(date_str)
                    publish_date = dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError):
                    pass

            # Get PDF URL
            pdf_url = self._get_pdf_url(doc_id)
            if not pdf_url:
                logger.warning(f"No PDF found for: {title} (ID: {doc_id})")
                continue

            # Extract text from PDF
            text = self._extract_pdf_text(pdf_url)
            if not text or len(text) < 100:
                logger.warning(
                    f"Insufficient text ({len(text)} chars) for: {title}"
                )
                continue

            # Generate stable ID
            url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:8]
            safe_slug = slug[:80] if slug else str(doc_id)
            stable_id = f"MS-parl-{safe_slug}-{url_hash}"

            raw = {
                "doc_id": stable_id,
                "title": title,
                "text": text,
                "publish_date": publish_date,
                "url": link,
                "pdf_url": pdf_url,
                "doc_type": doc_type,
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} documents fetched with full text")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=20)

    def test(self) -> bool:
        # Test API endpoint
        resp = self._api_get(f"{API_BASE}/legal_document", params={"per_page": 2})
        if resp is None:
            logger.error("Cannot reach WP REST API")
            return False

        docs = resp.json()
        if not docs:
            logger.error("No documents returned from API")
            return False

        logger.info(f"API OK: {len(docs)} documents returned")

        # Test PDF extraction on first document
        doc = docs[0]
        doc_id = doc.get("id")
        title = doc.get("title", {}).get("rendered", "")
        logger.info(f"Testing PDF for: {title} (ID: {doc_id})")

        pdf_url = self._get_pdf_url(doc_id)
        if not pdf_url:
            logger.error("No PDF found for test document")
            return False

        logger.info(f"PDF URL: {pdf_url}")
        text = self._extract_pdf_text(pdf_url)
        logger.info(f"PDF text: {len(text)} chars")

        return len(text) > 100


def main():
    parser = argparse.ArgumentParser(description="MS/Parliament data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MSParliamentScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            text_len = len(normalized.get("text", ""))
            logger.info(
                f"[{count + 1}] {normalized.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
