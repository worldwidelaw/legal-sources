#!/usr/bin/env python3
"""
SL/SierraLII -- Sierra Leone Legal Information Institute Fetcher

Fetches Sierra Leonean legislation with full text.

Strategy:
  - Paginate legislation listing pages (~106 acts, 3 pages at 50/page)
  - Fetch each act page, extract text from la-akoma-ntoso element (if available)
  - Fall back to PDF extraction via source.pdf for acts without inline HTML
  - Respect 5-second crawl delay per robots.txt

Usage:
  python bootstrap.py bootstrap          # Fetch all legislation
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SL.SierraLeoneLII")

BASE_URL = "https://sierralii.gov.sl"
LISTING_URL = f"{BASE_URL}/legislation/all"
MAX_PAGES = 10


class SierraLIIScraper(BaseScraper):
    """Scraper for SL/SierraLII -- Sierra Leonean legislation."""

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
        """HTTP GET with 5-second crawl delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(5)  # robots.txt Crawl-delay: 5
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a legislation listing page for document links."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen = set()

        # Match act links: /akn/sl/act/... and /akn/sl/act/si/... (subsidiary)
        links = soup.find_all("a", href=lambda h: h and "/akn/sl/act/" in str(h))
        for link in links:
            href = link.get("href", "")
            if href in seen:
                continue
            seen.add(href)

            title = link.get_text(strip=True)
            if not title:
                continue

            full_url = href if href.startswith("http") else BASE_URL + href
            documents.append({
                "title": title,
                "url": full_url,
                "href": href,
            })

        return documents

    def _extract_full_text(self, html: str, doc_url: str = "", doc_id: str = "") -> Dict[str, str]:
        """Extract full text and metadata from an act page.

        Tries Akoma Ntoso HTML first, falls back to PDF extraction.
        """
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "date": "", "title": ""}

        # Title from h1
        h1 = soup.find("h1")
        if h1:
            result["title"] = h1.get_text(strip=True)

        # Full text from la-akoma-ntoso element
        akn = soup.find("la-akoma-ntoso")
        if akn:
            text = akn.get_text(separator="\n", strip=True)
            # Clean up whitespace
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            result["text"] = text.strip()

        # Fallback: extract from source PDF if no inline HTML
        if (not result["text"] or len(result["text"]) < 100) and doc_url:
            pdf_url = doc_url.rstrip("/") + "/source.pdf"
            logger.info(f"No inline HTML, trying PDF: {pdf_url}")
            try:
                md = extract_pdf_markdown(
                    source="SL/SierraLII",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )
                if md and len(md) >= 100:
                    result["text"] = md
            except Exception as e:
                logger.warning(f"PDF extraction failed: {e}")

        # Date from metadata - try time element first
        date_el = soup.find("time")
        if date_el:
            result["date"] = date_el.get("datetime", "")

        # Fallback date from URL
        if not result["date"]:
            date_m = re.search(r"eng@(\d{4}-\d{2}-\d{2})", html)
            if date_m:
                result["date"] = date_m.group(1)

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        href = raw.get("href", "")
        # Create stable ID from AKN path
        doc_id = re.sub(r"^/akn/sl/act/", "SL-ACT-", href)
        doc_id = re.sub(r"/eng@.*$", "", doc_id)
        doc_id = doc_id.replace("/", "-")

        return {
            "_id": doc_id,
            "_source": "SL/SierraLII",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation from paginated listing."""
        count = 0
        seen_urls = set()

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                break

            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No documents on page {page_num}, stopping")
                break

            logger.info(f"Page {page_num}: {len(docs)} acts")

            for doc in docs:
                doc_url = doc["url"]
                if doc_url in seen_urls:
                    continue
                seen_urls.add(doc_url)

                # Fetch full document page
                doc_resp = self._request(doc_url)
                if doc_resp is None:
                    logger.warning(f"Failed to fetch: {doc['title'][:60]}")
                    continue

                # Build doc_id for PDF extraction
                href = doc["href"]
                doc_id = re.sub(r"^/akn/sl/act/", "SL-ACT-", href)
                doc_id = re.sub(r"/eng@.*$", "", doc_id)
                doc_id = doc_id.replace("/", "-")

                extracted = self._extract_full_text(doc_resp.text, doc_url=doc_url, doc_id=doc_id)
                if not extracted["text"] or len(extracted["text"]) < 100:
                    logger.warning(f"Insufficient text: {doc['title'][:60]}")
                    continue

                raw = {
                    "href": doc["href"],
                    "title": extracted["title"] or doc["title"],
                    "text": extracted["text"],
                    "date": extracted["date"],
                    "url": doc_url,
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} acts fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation (first 2 pages)."""
        count = 0
        for page_num in range(1, 3):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                continue

            docs = self._parse_listing_page(resp.text)
            for doc in docs:
                doc_resp = self._request(doc["url"])
                if doc_resp is None:
                    continue

                href = doc["href"]
                doc_id = re.sub(r"^/akn/sl/act/", "SL-ACT-", href)
                doc_id = re.sub(r"/eng@.*$", "", doc_id)
                doc_id = doc_id.replace("/", "-")

                extracted = self._extract_full_text(doc_resp.text, doc_url=doc["url"], doc_id=doc_id)
                if not extracted["text"] or len(extracted["text"]) < 100:
                    continue

                raw = {
                    "href": doc["href"],
                    "title": extracted["title"] or doc["title"],
                    "text": extracted["text"],
                    "date": extracted["date"],
                    "url": doc["url"],
                }
                count += 1
                yield raw

        logger.info(f"Updates: {count} acts fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{LISTING_URL}?page=1")
        if resp is None:
            logger.error("Cannot reach SierraLeoneLII listing page")
            return False

        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No legislation found on listing page")
            return False

        logger.info(f"Listing OK: {len(docs)} acts on page 1")

        # Test one document
        doc_resp = self._request(docs[0]["url"])
        if doc_resp:
            extracted = self._extract_full_text(doc_resp.text)
            logger.info(f"Doc OK: {docs[0]['title'][:60]} ({len(extracted['text'])} chars)")
            return True

        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SL/SierraLII data fetcher")
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
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SierraLIIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
