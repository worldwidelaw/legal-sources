#!/usr/bin/env python3
"""
INTL/ECOLEX -- ECOLEX Environmental Law Gateway (FAO/IUCN/UNEP)

Fetches environmental legislation and court decisions from ECOLEX.

Strategy:
  - Paginates through ECOLEX search results (HTML scraping)
  - For each result, scrapes the detail page for metadata
  - Downloads PDFs from FAOLEX links and extracts full text
  - Normalizes records to standard schema

Data:
  - 198K+ environmental law records: legislation + court decisions
  - Full text from FAOLEX PDF documents
  - Joint FAO/IUCN/UNEP initiative
  - EN/FR/ES

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

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
logger = logging.getLogger("legal-data-hunter.INTL.ECOLEX")

BASE_URL = "https://www.ecolex.org"
SEARCH_URL = f"{BASE_URL}/result/"


class EcolexScraper(BaseScraper):
    """
    Scraper for INTL/ECOLEX -- ECOLEX Environmental Law Gateway.
    Country: INTL
    URL: https://www.ecolex.org

    Data types: legislation, case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
        })

    def _get_search_results(self, doc_type: str = "legislation", page: int = 1) -> list:
        """Get detail page URLs from a search results page."""
        url = f"{SEARCH_URL}?type={doc_type}&page={page}"
        r = self.session.get(url, timeout=30)
        r.raise_for_status()

        soup = BeautifulSoup(r.text, "html.parser")
        detail_links = []
        seen = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("/details/") and href not in seen:
                seen.add(href)
                full_url = urljoin(BASE_URL, href.split("?")[0])
                detail_links.append(full_url)

        return detail_links

    def _parse_detail_page(self, url: str) -> Optional[dict]:
        """Parse an ECOLEX detail page for metadata and PDF links."""
        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(r.text, "html.parser")

        # Extract title
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""

        # Extract metadata from the detail table/fields
        metadata = {}
        # Look for field pairs (label + value)
        for dt in soup.find_all(["dt", "th", "strong"]):
            label = dt.get_text(strip=True).rstrip(":")
            # Get next sibling or parent's next element
            dd = dt.find_next_sibling(["dd", "td"])
            if dd:
                value = dd.get_text(separator=" ", strip=True)
                if label and value:
                    metadata[label.lower()] = value

        # Extract country
        country = metadata.get("country/territory", metadata.get("country", ""))

        # Extract date
        date_str = metadata.get("date", "")

        # Extract document type
        doc_type = metadata.get("document type", metadata.get("type of text", ""))

        # Extract subject/keywords
        subject = metadata.get("subject", "")
        keywords = metadata.get("keyword", "")

        # Find PDF links (FAOLEX)
        pdf_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if ".pdf" in href.lower():
                pdf_links.append({"url": href, "text": text})
            elif "faolex" in href.lower() and "docs" in href.lower():
                pdf_links.append({"url": href, "text": text})

        # Extract ECOLEX ID from URL
        # /details/legislation/title-slug-lex-faocNNNNNN/
        ecolex_id = url.rstrip("/").split("/")[-1]

        return {
            "url": url,
            "ecolex_id": ecolex_id,
            "title": title,
            "country": country,
            "date": date_str,
            "document_type": doc_type,
            "subject": subject,
            "keywords": keywords,
            "pdf_links": pdf_links,
            "metadata": metadata,
        }

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/ECOLEX",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _parse_date_to_iso(self, date_str: str) -> Optional[str]:
        """Convert ECOLEX date to ISO 8601."""
        if not date_str:
            return None
        # Formats: "1958", "1958 (2024)", "01/01/2020", "January 2020"
        # Extract year from parenthetical revision dates or plain years
        # Try full date patterns first
        for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%B %d, %Y", "%d %B %Y"]:
            try:
                return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Extract just year
        year_match = re.search(r'\b(19|20)\d{2}\b', date_str)
        if year_match:
            return f"{year_match.group(0)}-01-01"
        return None

    def _determine_type(self, doc_type: str, url: str) -> str:
        """Determine if record is legislation or case_law."""
        if "court_decision" in url or "court" in doc_type.lower() or "decision" in doc_type.lower():
            return "case_law"
        return "legislation"

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ECOLEX records."""
        for doc_type in ["legislation", "court_decision"]:
            page = 1
            while True:
                logger.info(f"Fetching {doc_type} page {page}...")
                try:
                    detail_urls = self._get_search_results(doc_type, page)
                except Exception as e:
                    logger.error(f"Search failed on page {page}: {e}")
                    break

                if not detail_urls:
                    logger.info(f"No more results for {doc_type} at page {page}")
                    break

                for url in detail_urls:
                    detail = self._parse_detail_page(url)
                    if not detail:
                        continue

                    # Try to get full text from PDF
                    text = ""
                    for pdf in detail.get("pdf_links", []):
                        pdf_url = pdf["url"]
                        if not pdf_url.startswith("http"):
                            pdf_url = urljoin(BASE_URL, pdf_url)
                        text = self._extract_pdf_text(pdf_url)
                        if text and len(text.strip()) > 50:
                            break

                    detail["text"] = text
                    yield detail
                    time.sleep(1)

                page += 1
                time.sleep(2)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent records (sort by newest)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw ECOLEX data into standard schema."""
        text = raw.get("text", "")
        title = raw.get("title", "")
        url = raw.get("url", "")

        if not text or len(text.strip()) < 50:
            return None

        ecolex_id = raw.get("ecolex_id", "")
        doc_id = f"ECOLEX-{ecolex_id}"

        date = self._parse_date_to_iso(raw.get("date", ""))
        record_type = self._determine_type(raw.get("document_type", ""), url)

        return {
            "_id": doc_id,
            "_source": "INTL/ECOLEX",
            "_type": record_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "country": raw.get("country", ""),
            "document_type": raw.get("document_type", ""),
            "subject": raw.get("subject", ""),
            "keywords": raw.get("keywords", ""),
            "organization": "FAO/IUCN/UNEP",
        }


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ECOLEX data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = EcolexScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            urls = scraper._get_search_results("legislation", 1)
            logger.info(f"OK: Found {len(urls)} results on page 1")
            logger.info(f"First: {urls[0]}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
