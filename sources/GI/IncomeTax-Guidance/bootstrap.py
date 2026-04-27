#!/usr/bin/env python3
"""
GI/IncomeTax-Guidance -- Gibraltar Income Tax Office Practice Notes

Fetches tax guidance PDFs from the Gibraltar Income Tax Office.

Strategy:
  - Discovery: Multiple listing pages on gibraltar.gov.gi/income-tax-office
  - Full text: PDFs downloaded and extracted via common/pdf_extract
  - Categories: Corporate guidance, self-employed, PAYE/employers, pensions,
    dispute resolution, international tax, social insurance

Endpoints:
  - Downloads: https://www.gibraltar.gov.gi/income-tax-office/downloads-ito
  - Corporate: https://www.gibraltar.gov.gi/income-tax-office/businesses-and-corporations/corporate
  - Self-employed: https://www.gibraltar.gov.gi/income-tax-office/individuals-and-employees/self-employed
  - Employers: https://www.gibraltar.gov.gi/income-tax-office/employers-and-trusts/employers

Data:
  - ~35 tax guidance PDF documents
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import quote, unquote

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GI.IncomeTax-Guidance")

BASE_URL = "https://www.gibraltar.gov.gi"

LISTING_PAGES = [
    "/income-tax-office/downloads-ito",
    "/income-tax-office/businesses-and-corporations/corporate",
    "/income-tax-office/individuals-and-employees/self-employed",
    "/income-tax-office/employers-and-trusts/employers",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Keywords to identify guidance/doctrine documents (vs blank forms)
GUIDANCE_KEYWORDS = [
    "guidance", "guide", "explanatory", "practice note", "information leaflet",
    "faq", "notes for", "qnups", "country by country", "map guidance",
    "tax at a glance", "social insurance",
]

# Keywords to EXCLUDE (blank forms, tables, calculators)
EXCLUDE_KEYWORDS = [
    "blank form", "application form", "tax table", "calculator",
    "claim form", "request form", "registration form",
]


def _is_guidance_pdf(title: str, url: str) -> bool:
    """Check if a PDF link is a guidance document (not a blank form)."""
    title_lower = title.lower()
    url_lower = url.lower()

    # Exclude obvious forms
    for excl in EXCLUDE_KEYWORDS:
        if excl in title_lower:
            return False

    # Include if title or URL matches guidance keywords
    for kw in GUIDANCE_KEYWORDS:
        if kw in title_lower or kw in url_lower:
            return True

    # Include PDFs with "guidance" or "guide" in URL path
    if "guidance" in url_lower or "guide" in url_lower:
        return True

    return False


def _make_doc_id(url: str, title: str) -> str:
    """Generate a stable document ID from URL."""
    # Use the filename from URL as the base
    path = unquote(url).split("/")[-1]
    # Remove extension and clean
    name = re.sub(r'\.pdf$', '', path, flags=re.IGNORECASE)
    name = re.sub(r'[^\w\s-]', '', name).strip()
    name = re.sub(r'\s+', '-', name)[:80]
    return name or re.sub(r'[^\w-]', '', title[:60])


def _encode_url(url: str) -> str:
    """Properly encode URL with spaces."""
    # Split into base and path, encode spaces in path
    if " " in url:
        # Only encode spaces, leave other percent-encoded chars alone
        url = url.replace(" ", "%20")
    return url


class GibraltarIncomeTaxScraper(BaseScraper):
    """
    Scraper for GI/IncomeTax-Guidance -- Gibraltar Income Tax Office Practice Notes.
    Country: GI
    URL: https://www.gibraltar.gov.gi/income-tax-office

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 60) -> requests.Response:
        """Make HTTP GET request with rate limiting."""
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp

    def _parse_page_for_pdfs(self, page_path: str) -> List[Dict[str, Any]]:
        """Parse a listing page and extract all guidance PDF links."""
        url = BASE_URL + page_path
        logger.info(f"Scanning: {url}")

        try:
            resp = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return []

        soup = BeautifulSoup(resp.content, "html.parser")
        documents = []
        current_category = "General"

        # Find all headings and links
        content = soup.find("div", class_="field--name-body") or soup.find("main") or soup

        for element in content.find_all(["h2", "h3", "h4", "strong", "a"]):
            # Track category from headings
            if element.name in ("h2", "h3", "h4"):
                cat_text = element.get_text(strip=True)
                if cat_text and len(cat_text) < 100:
                    current_category = cat_text
                continue

            if element.name == "strong" and not element.find("a"):
                cat_text = element.get_text(strip=True)
                if cat_text and len(cat_text) < 80:
                    current_category = cat_text
                continue

            # Process links
            if element.name == "a":
                href = element.get("href", "")
                if not href:
                    continue

                # Only process PDF links
                is_pdf = href.lower().endswith(".pdf") or ".pdf" in href.lower()
                if not is_pdf:
                    continue

                title = element.get("title", "") or element.get_text(strip=True)
                title = re.sub(r'\s*\[\d+[KMG]B\]\s*$', '', title).strip()
                title = re.sub(r'\s+', ' ', title).strip()

                if not title:
                    continue

                # Make absolute URL
                if href.startswith("/"):
                    href = BASE_URL + href
                elif not href.startswith("http"):
                    href = BASE_URL + "/" + href

                # Convert http to https
                href = href.replace("http://www.gibraltar.gov.gi", "https://www.gibraltar.gov.gi")

                # Check if it's a guidance document
                if not _is_guidance_pdf(title, href):
                    continue

                doc_id = _make_doc_id(href, title)
                documents.append({
                    "doc_id": doc_id,
                    "title": title,
                    "category": current_category,
                    "pdf_url": _encode_url(href),
                })

        logger.info(f"  Found {len(documents)} guidance PDFs on {page_path}")
        return documents

    def _discover_all_documents(self) -> List[Dict[str, Any]]:
        """Discover all guidance documents from all listing pages."""
        all_docs = []
        seen_ids = set()

        for page_path in LISTING_PAGES:
            docs = self._parse_page_for_pdfs(page_path)
            for doc in docs:
                if doc["doc_id"] not in seen_ids:
                    seen_ids.add(doc["doc_id"])
                    all_docs.append(doc)

        # Add known documents that might be missed by the scraper
        known_extras = [
            {
                "doc_id": "FAQs-International-Tax-Agreement-Spain",
                "title": "FAQs - International Tax Agreement - Spain",
                "category": "International Tax",
                "pdf_url": "https://www.gibraltar.gov.gi/uploads/Income%20Tax%20Office/docs/FAQs%20-%20International%20Tax%20Agreement%20-%20Spain.pdf",
            },
            {
                "doc_id": "Tax-Return-Guidance2024",
                "title": "Tax Return Guidance Notes 2024",
                "category": "Tax Returns",
                "pdf_url": "https://www.gibraltar.gov.gi/uploads/Income%20Tax%20Office/Tax%20Returns/Tax%20Return%20Guidance2024.pdf",
            },
            {
                "doc_id": "EXPLANATORY-NOTES-ON-COUNTRY-BY-COUNTRY-REPORTING-OBLIGATIONS",
                "title": "Explanatory Notes on Country by Country Reporting Obligations",
                "category": "Corporate",
                "pdf_url": "https://www.gibraltar.gov.gi/new/sites/default/files/HMGoG_Documents/EXPLANATORY%20NOTES%20ON%20COUNTRY%20BY%20COUNTRY%20REPORTING%20OBLIGATIONS.pdf",
            },
            {
                "doc_id": "Guidance-for-split-year-treatment",
                "title": "Guidance for Split Year Treatment",
                "category": "Corporate",
                "pdf_url": "https://www.gibraltar.gov.gi/uploads/Income%20Tax%20Office/Corporate/Guidance%20for%20split%20year%20treatment.pdf",
            },
        ]

        for doc in known_extras:
            if doc["doc_id"] not in seen_ids:
                seen_ids.add(doc["doc_id"])
                all_docs.append(doc)

        logger.info(f"Total unique guidance documents found: {len(all_docs)}")
        return all_docs

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        return {
            "_id": f"GI-ITO-{raw['doc_id']}",
            "_source": "GI/IncomeTax-Guidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("pdf_url", ""),
            "doc_id": raw.get("doc_id", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all tax guidance documents."""
        documents = self._discover_all_documents()
        if not documents:
            logger.error("No documents found on listing pages")
            return

        sample_limit = 15 if sample else None
        fetched = 0

        for doc in documents:
            if sample_limit and fetched >= sample_limit:
                break

            doc_id = doc["doc_id"]
            pdf_url = doc["pdf_url"]
            logger.info(f"Extracting PDF: {doc['title'][:60]}...")

            try:
                text = extract_pdf_markdown(
                    source="GI/IncomeTax-Guidance",
                    source_id=f"GI-ITO-{doc_id}",
                    pdf_url=pdf_url,
                    table="doctrine",
                )
            except Exception as e:
                logger.warning(f"PDF extraction failed for {doc_id}: {e}")
                text = None

            if not text:
                # Fallback: try downloading and extracting locally
                try:
                    text = self._extract_pdf_fallback(pdf_url)
                except Exception as e:
                    logger.warning(f"Fallback extraction also failed for {doc_id}: {e}")
                    continue

            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
                continue

            doc["text"] = text
            record = self.normalize(doc)
            yield record
            fetched += 1

        logger.info(f"Total fetched: {fetched}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates - re-fetches all since there's no date filtering."""
        yield from self.fetch_all(sample=False)

    def _extract_pdf_fallback(self, pdf_url: str) -> Optional[str]:
        """Fallback PDF text extraction using pdfplumber."""
        try:
            import pdfplumber
        except ImportError:
            try:
                import pypdf
                return self._extract_with_pypdf(pdf_url)
            except ImportError:
                return None

        import io
        self.rate_limiter.wait()
        resp = self.session.get(pdf_url, timeout=60)
        resp.raise_for_status()

        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
        return "\n\n".join(pages)

    def _extract_with_pypdf(self, pdf_url: str) -> Optional[str]:
        """Fallback using pypdf."""
        import io
        import pypdf

        self.rate_limiter.wait()
        resp = self.session.get(pdf_url, timeout=60)
        resp.raise_for_status()

        reader = pypdf.PdfReader(io.BytesIO(resp.content))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
        return "\n\n".join(pages)

    def test(self) -> bool:
        """Quick connectivity test."""
        logger.info("Testing GI/IncomeTax-Guidance connectivity...")
        try:
            documents = self._discover_all_documents()
            logger.info(f"Found {len(documents)} documents")

            if documents:
                doc = documents[0]
                text = extract_pdf_markdown(
                    source="GI/IncomeTax-Guidance",
                    source_id=f"GI-ITO-{doc['doc_id']}",
                    pdf_url=doc["pdf_url"],
                    table="doctrine",
                )
                if not text:
                    text = self._extract_pdf_fallback(doc["pdf_url"])

                if text:
                    logger.info(f"Sample text length: {len(text)} chars")
                    logger.info("Test PASSED")
                    return True

            logger.error("Test FAILED: Could not extract text")
            return False
        except Exception as e:
            logger.error(f"Test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GI/IncomeTax-Guidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = GibraltarIncomeTaxScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            filename = re.sub(r'[^\w\-.]', '_', f"{record['_id']}.json")
            filepath = sample_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
