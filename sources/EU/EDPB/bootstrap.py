#!/usr/bin/env python3
"""
EU/EDPB -- European Data Protection Board Document Fetcher

Fetches GDPR guidelines, opinions, binding decisions, letters, and reports
with full text extracted from PDFs.

Strategy:
  - Scrape paginated HTML listing (67 pages, ~670 documents)
  - Extract metadata and PDF URLs from each listing page
  - Download English PDFs and extract text via PyPDF2
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
from bs4 import BeautifulSoup

import subprocess
import tempfile

# Try multiple PDF libraries with fallbacks
PDF_BACKEND = None
try:
    import pypdf
    PDF_BACKEND = "pypdf"
except ImportError:
    try:
        import PyPDF2
        PDF_BACKEND = "PyPDF2"
    except ImportError:
        # Check for system pdftotext (poppler-utils)
        try:
            subprocess.run(["pdftotext", "-v"], capture_output=True, timeout=5)
            PDF_BACKEND = "pdftotext"
        except (FileNotFoundError, subprocess.TimeoutExpired):
            PDF_BACKEND = None

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.EDPB")

BASE_URL = "https://www.edpb.europa.eu"
LISTING_URL = f"{BASE_URL}/our-work-tools/documents/our-documents_en"
MAX_PAGES = 67


class EDPBScraper(BaseScraper):
    """Scraper for EU/EDPB -- European Data Protection Board documents."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 15s")
                    time.sleep(15)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF bytes using available backend."""
        if PDF_BACKEND is None:
            logger.error("No PDF backend available (need pypdf, PyPDF2, or pdftotext)")
            return ""
        try:
            if PDF_BACKEND == "pypdf":
                reader = pypdf.PdfReader(io.BytesIO(pdf_content))
                parts = []
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        parts.append(text)
                text = "\n\n".join(parts)
            elif PDF_BACKEND == "PyPDF2":
                reader = PyPDF2.PdfReader(io.BytesIO(pdf_content))
                parts = []
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        parts.append(text)
                text = "\n\n".join(parts)
            elif PDF_BACKEND == "pdftotext":
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                    tmp.write(pdf_content)
                    tmp_path = tmp.name
                try:
                    result = subprocess.run(
                        ["pdftotext", "-layout", tmp_path, "-"],
                        capture_output=True, text=True, timeout=60,
                    )
                    text = result.stdout
                finally:
                    Path(tmp_path).unlink(missing_ok=True)
            else:
                return ""
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            return text.strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed ({PDF_BACKEND}): {e}")
            return ""

    def _parse_date(self, date_str: str) -> str:
        """Parse date like '19 March 2026' to ISO format."""
        date_str = date_str.strip()
        for fmt in ("%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return date_str

    def _parse_listing_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse a listing page and return document metadata."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []

        rows = soup.find_all("div", class_=lambda c: c and "views-row" in str(c))

        for row in rows:
            try:
                # Title and URL
                h4 = row.find("h4")
                if not h4:
                    continue
                link = h4.find("a")
                if not link:
                    continue

                title = link.get_text(strip=True)
                href = link.get("href", "")
                if not href:
                    continue
                doc_url = href if href.startswith("http") else BASE_URL + href

                # Date
                date_span = row.find("span", class_=lambda c: c and "news-date" in str(c))
                date_str = date_span.get_text(strip=True) if date_span else ""
                date_iso = self._parse_date(date_str) if date_str else ""

                # Publication type
                pub_type_div = row.find("div", class_=lambda c: c and "field--name-field-edpb-publication-type" in str(c))
                pub_type = ""
                if pub_type_div:
                    pub_link = pub_type_div.find("a")
                    if pub_link:
                        pub_type = pub_link.get_text(strip=True)

                # Topics
                topics_div = row.find("div", class_=lambda c: c and "field--name-field-edpb-topics" in str(c))
                topics = []
                if topics_div:
                    for t_link in topics_div.find_all("a"):
                        topics.append(t_link.get_text(strip=True))

                # PDF URL from data-url attribute
                pdf_url = ""
                pdf_el = row.find(attrs={"data-url": True})
                if pdf_el:
                    data_url = pdf_el.get("data-url", "")
                    if data_url:
                        pdf_url = data_url if data_url.startswith("http") else BASE_URL + data_url

                # Generate document ID from URL slug
                slug = href.rstrip("/").split("/")[-1]
                slug = re.sub(r"_en$", "", slug)
                doc_id = f"EDPB-{slug}"

                documents.append({
                    "document_id": doc_id,
                    "title": title,
                    "date": date_iso,
                    "url": doc_url,
                    "pdf_url": pdf_url,
                    "publication_type": pub_type,
                    "topics": topics,
                })

            except Exception as e:
                logger.warning(f"Failed to parse row: {e}")
                continue

        return documents

    def _classify_type(self, pub_type: str, title: str) -> str:
        """Classify document as case_law or doctrine."""
        lower = (pub_type + " " + title).lower()
        if any(kw in lower for kw in ["binding decision", "dispute resolution", "art. 65"]):
            return "case_law"
        return "doctrine"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = self._classify_type(
            raw.get("publication_type", ""),
            raw.get("title", ""),
        )
        return {
            "_id": raw.get("document_id", ""),
            "_source": "EU/EDPB",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "publication_type": raw.get("publication_type", ""),
            "topics": raw.get("topics", []),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all EDPB documents from paginated listing."""
        count = 0

        for page_num in range(MAX_PAGES):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch page {page_num}")
                continue

            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No documents on page {page_num}, stopping pagination")
                break

            logger.info(f"Page {page_num}: {len(docs)} documents")

            for doc in docs:
                pdf_url = doc.get("pdf_url", "")
                if not pdf_url:
                    logger.warning(f"No PDF for: {doc['title'][:60]}")
                    continue

                # Download and extract PDF text
                pdf_resp = self._request(pdf_url, timeout=120)
                if pdf_resp is None:
                    logger.warning(f"Failed to download PDF: {pdf_url[:80]}")
                    continue

                text = self._extract_text_from_pdf(pdf_resp.content)
                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text from PDF: {doc['title'][:60]} ({len(text)} chars)")
                    continue

                doc["text"] = text
                count += 1
                yield self.normalize(doc)

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent updates (first few pages)."""
        count = 0
        for page_num in range(3):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                continue

            docs = self._parse_listing_page(resp.text)
            for doc in docs:
                if since and doc.get("date", "") < since:
                    return

                pdf_url = doc.get("pdf_url", "")
                if not pdf_url:
                    continue

                pdf_resp = self._request(pdf_url, timeout=120)
                if pdf_resp is None:
                    continue

                text = self._extract_text_from_pdf(pdf_resp.content)
                if not text or len(text) < 100:
                    continue

                doc["text"] = text
                count += 1
                yield self.normalize(doc)

        logger.info(f"Updates: {count} documents fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{LISTING_URL}?page=0")
        if resp is None:
            logger.error("Cannot reach EDPB listing page")
            return False

        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No documents parsed from listing page")
            return False

        logger.info(f"Listing OK: {len(docs)} documents on page 0")

        # Test PDF download
        for doc in docs:
            pdf_url = doc.get("pdf_url", "")
            if pdf_url:
                pdf_resp = self._request(pdf_url, timeout=60)
                if pdf_resp:
                    text = self._extract_text_from_pdf(pdf_resp.content)
                    logger.info(f"PDF OK: {doc['title'][:60]} ({len(text)} chars)")
                    return True

        logger.error("No PDF could be downloaded")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="EU/EDPB data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    args = parser.parse_args()

    scraper = EDPBScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        max_records = 15 if args.sample else None
        count = 0

        for record in scraper.fetch_all():
            out_path = sample_dir / f"{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )

            count += 1
            if max_records and count >= max_records:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
