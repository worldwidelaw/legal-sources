#!/usr/bin/env python3
"""
HK/SFAT — Hong Kong Securities and Futures Appeals Tribunal

Fetches determinations, rulings, and related decisions from the SFAT
website's published determinations page.

Strategy:
  - Discovery: scrape sfat.gov.hk/en/determination.html for all PDF links
  - Each PDF link has context (application number, applicant name)
  - Download PDFs and extract full text via common/pdf_extract

Data:
  - ~107 PDF documents across ~81 applications (2003–2023)
  - No authentication required
  - Full text in PDF (English)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py bootstrap --full     # Full fetch
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HK.SFAT")

INDEX_URL = "https://www.sfat.gov.hk/en/determination.html"
FILES_BASE = "https://www.sfat.gov.hk/files/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber/pypdf."""
    try:
        import pdfplumber
        import io
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"pdfplumber failed: {e}, trying pypdf")

    try:
        from pypdf import PdfReader
        import io
        reader = PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n".join(pages)
    except Exception as e:
        logger.error(f"All PDF extraction failed: {e}")
        return ""


def _classify_document(filename: str) -> str:
    """Classify document type from PDF filename."""
    fn_lower = filename.lower()
    if "cost" in fn_lower:
        return "costs_ruling"
    if "ruling" in fn_lower or "decision" in fn_lower:
        return "ruling"
    if "corrigend" in fn_lower:
        return "corrigendum"
    if "addendum" in fn_lower:
        return "addendum"
    if "annexure" in fn_lower:
        return "annexure"
    if "stay" in fn_lower:
        return "stay_application"
    if "privacy" in fn_lower:
        return "privacy_direction"
    return "determination"


class HKSFATScraper(BaseScraper):
    """
    Scraper for HK/SFAT — Securities and Futures Appeals Tribunal.
    Country: HK
    URL: https://www.sfat.gov.hk/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 120) -> requests.Response:
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp

    def _discover_documents(self) -> List[Dict[str, Any]]:
        """Scrape determination.html to discover all PDF documents with context."""
        resp = self._get(INDEX_URL)
        soup = BeautifulSoup(resp.text, "html.parser")

        documents = []
        current_app_num = ""
        current_applicant = ""
        current_year = ""

        # Walk through the page content
        for element in soup.find_all(["h3", "h4", "p", "li", "a", "div", "strong", "b"]):
            text = element.get_text(strip=True)

            # Detect year headers
            year_match = re.match(r"^(20\d{2})$", text)
            if year_match:
                current_year = year_match.group(1)
                continue

            # Detect application numbers: "SFAT Application No. X/YYYY" or "Application No. X/YYYY"
            app_match = re.search(r"(?:SFAT\s+)?Application\s+No\.\s*([\d,\s&]+/\d{4})", text, re.IGNORECASE)
            if app_match:
                current_app_num = app_match.group(1).strip()
                # Extract year from app number if not set
                y_match = re.search(r"/(\d{4})", current_app_num)
                if y_match:
                    current_year = y_match.group(1)
                # Try to extract applicant name (usually follows the dash)
                dash_idx = text.find(" - ")
                if dash_idx > 0:
                    current_applicant = text[dash_idx + 3:].strip()
                elif dash_idx < 0:
                    dash_idx = text.find("– ")
                    if dash_idx > 0:
                        current_applicant = text[dash_idx + 2:].strip()

            # Find PDF links
            if element.name == "a" and element.get("href", "").endswith(".pdf"):
                href = element["href"]
                pdf_url = urljoin(INDEX_URL, href)
                filename = unquote(pdf_url.split("/")[-1])
                doc_type = _classify_document(filename)
                link_text = element.get_text(strip=True)

                documents.append({
                    "pdf_url": pdf_url,
                    "filename": filename,
                    "app_number": current_app_num,
                    "applicant": current_applicant,
                    "year": current_year,
                    "document_type": doc_type,
                    "link_text": link_text,
                })

        logger.info(f"Discovery: {len(documents)} PDF documents across applications")
        return documents

    def fetch_all(self) -> Generator[dict, None, None]:
        documents = self._discover_documents()
        for doc in documents:
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        pdf_url = raw["pdf_url"]
        filename = raw["filename"]
        app_number = raw.get("app_number", "")
        doc_type = raw.get("document_type", "determination")

        # Download PDF
        try:
            resp = self._get(pdf_url)
            pdf_bytes = resp.content
        except Exception as e:
            logger.warning(f"Failed to download {filename}: {e}")
            return None

        if len(pdf_bytes) < 500:
            logger.warning(f"PDF too small: {filename} ({len(pdf_bytes)} bytes)")
            return None

        # Extract text
        text = extract_pdf_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for {filename}: {len(text)} chars")
            return None

        # Build title
        applicant = raw.get("applicant", "")
        if applicant and app_number:
            title = f"SFAT Application No. {app_number} — {applicant}"
        elif app_number:
            title = f"SFAT Application No. {app_number}"
        else:
            title = f"SFAT {filename.replace('.pdf', '')}"

        if doc_type != "determination":
            title += f" ({doc_type.replace('_', ' ').title()})"

        # Try to extract date from text
        date_iso = self._extract_date(text, raw.get("year", ""))

        # Build unique ID from app number + doc type
        id_part = re.sub(r'[^a-zA-Z0-9]', '-', f"{app_number}-{doc_type}").strip('-')
        if not id_part:
            id_part = re.sub(r'[^a-zA-Z0-9]', '-', filename.replace('.pdf', ''))

        return {
            "_id": f"HK-SFAT-{id_part}",
            "_source": "HK/SFAT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": pdf_url,
            "case_number": app_number,
            "document_type": doc_type,
            "court": "Securities and Futures Appeals Tribunal",
            "jurisdiction": "Hong Kong",
            "applicant": applicant,
        }

    def _extract_date(self, text: str, fallback_year: str) -> Optional[str]:
        """Extract date from determination text."""
        patterns = [
            r'(\d{1,2})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})',
        ]
        # Search in first 3000 chars
        search_text = text[:3000]
        for pat in patterns:
            m = re.search(pat, search_text)
            if m:
                groups = m.groups()
                try:
                    if groups[0].isdigit():
                        dt = datetime.strptime(f"{groups[0]} {groups[1]} {groups[2]}", "%d %B %Y")
                    else:
                        dt = datetime.strptime(f"{groups[1]} {groups[0]} {groups[2]}", "%d %B %Y")
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        # Try DD/MM/YYYY or DD.MM.YYYY
        m = re.search(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', search_text)
        if m:
            try:
                dt = datetime.strptime(f"{m.group(1)}/{m.group(2)}/{m.group(3)}", "%d/%m/%Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        if fallback_year:
            return f"{fallback_year}-01-01"
        return None


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="HK/SFAT Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test", "status"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (12 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    scraper = HKSFATScraper()

    if args.command == "test":
        print("Testing connectivity...")
        resp = scraper._get(INDEX_URL)
        soup = BeautifulSoup(resp.text, "html.parser")
        pdfs = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".pdf")]
        print(f"OK — Found {len(pdfs)} PDF links on determination page")
        # Test one PDF download
        test_url = urljoin(INDEX_URL, pdfs[0])
        resp = scraper._get(test_url)
        print(f"OK — PDF download: {len(resp.content)} bytes")
        text = extract_pdf_text(resp.content)
        print(f"OK — Extracted text: {len(text)} chars")
        sys.exit(0)

    if args.command == "status":
        print(json.dumps(scraper.status, indent=2, default=str))
        sys.exit(0)

    if args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        result = scraper.bootstrap(
            sample_mode=sample_mode,
            sample_size=12 if sample_mode else 999999,
        )
        print(json.dumps(result, indent=2, default=str))
