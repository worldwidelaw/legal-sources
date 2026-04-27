#!/usr/bin/env python3
"""
GG/RevenueService-Guidance -- Guernsey Revenue Service Tax Guidance

Fetches tax guidance PDFs from the Guernsey Revenue Service.

Strategy:
  - Discovery: Single listing page at gov.gg/taxationstatementsofpractice
  - Full text: PDFs downloaded from CHttpHandler.ashx, extracted via common/pdf_extract
  - Categories: Statements of Practice (Business/Company/Employment/Interest/
    Miscellaneous/Residence), Codes of Practice, Circulars, Guides

Endpoints:
  - Listing: https://www.gov.gg/taxationstatementsofpractice
  - PDF: https://www.gov.gg/CHttpHandler.ashx?id={ID}&p=0

Data:
  - ~157 tax guidance PDF documents
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
logger = logging.getLogger("legal-data-hunter.GG.RevenueService-Guidance")

LISTING_URL = "https://www.gov.gg/taxationstatementsofpractice"
BASE_URL = "https://www.gov.gg"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class GuernseyRevenueScraper(BaseScraper):
    """
    Scraper for GG/RevenueService-Guidance -- Guernsey Revenue Service Tax Guidance.
    Country: GG
    URL: https://www.gov.gg/taxationstatementsofpractice

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

    def _parse_listing(self) -> List[Dict[str, Any]]:
        """Parse the main listing page to extract all PDF links with metadata."""
        resp = self._get(LISTING_URL)
        soup = BeautifulSoup(resp.content, "html.parser")

        documents = []
        current_category = "General"

        # The content area contains h3 for major sections and strong for sub-sections
        content = soup.find("div", class_="article-body") or soup.find("div", id="content") or soup

        # Find all elements in order - headings and links
        for element in content.find_all(["h3", "h4", "strong", "a"]):
            # Update category from headings
            if element.name in ("h3", "h4"):
                cat_text = element.get_text(strip=True)
                if cat_text:
                    current_category = cat_text
                continue

            if element.name == "strong" and not element.find("a"):
                cat_text = element.get_text(strip=True)
                if cat_text and len(cat_text) < 100:
                    current_category = cat_text
                continue

            # Process PDF links
            if element.name == "a":
                href = element.get("href", "")
                css_class = element.get("class", [])
                css_str = " ".join(css_class) if isinstance(css_class, list) else str(css_class)

                # Match CHttpHandler links or inlinemedia PDF links
                if "CHttpHandler" in href or "typepdf" in css_str:
                    # Extract document ID
                    id_match = re.search(r'id=(\d+)', href)
                    if not id_match:
                        # Try from CSS class
                        id_match = re.search(r'id(\d+)', css_str)
                    if not id_match:
                        continue

                    doc_id = id_match.group(1)
                    title = element.get("title", "") or element.get_text(strip=True)

                    # Clean title - remove file size info like [123KB]
                    title = re.sub(r'\s*\[\d+[KMG]B\]\s*$', '', title).strip()

                    if not href.startswith("http"):
                        href = BASE_URL + href

                    documents.append({
                        "doc_id": doc_id,
                        "title": title,
                        "category": current_category,
                        "pdf_url": href,
                    })

        logger.info(f"Found {len(documents)} documents on listing page")
        return documents

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        return {
            "_id": f"GG-RS-{raw['doc_id']}",
            "_source": "GG/RevenueService-Guidance",
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
        documents = self._parse_listing()
        if not documents:
            logger.error("No documents found on listing page")
            return

        sample_limit = 15 if sample else None
        fetched = 0

        for doc in documents:
            if sample_limit and fetched >= sample_limit:
                break

            doc_id = doc["doc_id"]
            pdf_url = doc["pdf_url"]
            logger.info(f"Extracting PDF {doc_id}: {doc['title'][:60]}...")

            try:
                text = extract_pdf_markdown(
                    source="GG/RevenueService-Guidance",
                    source_id=f"GG-RS-{doc_id}",
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
        """Fetch updates - re-fetches all since there's no date filtering on listing."""
        yield from self.fetch_all(sample=False)

    def _extract_pdf_fallback(self, pdf_url: str) -> Optional[str]:
        """Fallback PDF text extraction using pdfplumber."""
        try:
            import pdfplumber
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

    def test(self) -> bool:
        """Quick connectivity test."""
        logger.info("Testing GG/RevenueService-Guidance connectivity...")
        try:
            documents = self._parse_listing()
            logger.info(f"Found {len(documents)} documents")

            if documents:
                doc = documents[0]
                text = extract_pdf_markdown(
                    source="GG/RevenueService-Guidance",
                    source_id=f"GG-RS-{doc['doc_id']}",
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

    parser = argparse.ArgumentParser(description="GG/RevenueService-Guidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = GuernseyRevenueScraper()

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
