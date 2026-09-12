#!/usr/bin/env python3
"""
IQ/KRG-Legislation -- Kurdistan Region Parliament Legislation

Fetches Kurdistan Region (Iraq) legislation from legislation.krd with full text.

Strategy:
  - Crawl year-based listing pages (/years/?year=YYYY) for laws (1992-2022)
  - Crawl paginated order listings (/orders-law?pageNumber=N)
  - Fetch each law's detail page (/law-detail/?id=NNNN)
  - Extract full text from <pre> element (Unicode Kurdish)

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IQ.KRG-Legislation")

BASE_URL = "https://legislation.krd"
LAW_YEARS = list(range(1992, 2023))  # 1992-2022
ORDER_MAX_PAGES = 10


class KRGLegislationScraper(BaseScraper):
    """Scraper for IQ/KRG-Legislation -- Kurdistan Region Parliament laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
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
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a listing page for law-detail links."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen = set()

        links = soup.find_all("a", href=lambda h: h and "law-detail" in str(h))
        for link in links:
            href = link.get("href", "")
            # Extract the ID from href like /law-detail/?id=5437
            id_match = re.search(r"id=(\d+)", href)
            if not id_match:
                continue

            law_id = id_match.group(1)
            if law_id in seen:
                continue
            seen.add(law_id)

            title = link.get_text(strip=True)
            if not title:
                continue

            documents.append({
                "law_id": law_id,
                "title": title,
                "url": f"{BASE_URL}/law-detail/?id={law_id}",
            })

        return documents

    def _extract_detail(self, html: str) -> Dict[str, str]:
        """Extract full text and metadata from a law detail page."""
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "title": "", "law_number": "", "year": "", "status": ""}

        # Title from first h4
        h4 = soup.find("h4")
        if h4:
            result["title"] = h4.get_text(strip=True)

        # Metadata from h5 elements
        for h5 in soup.find_all("h5"):
            text = h5.get_text(strip=True)
            # Law number: "ژمارەی یاسا : N" or similar
            num_match = re.search(r":\s*(\d+)", text)
            if "ژمارە" in text and num_match:
                result["law_number"] = num_match.group(1)
            # Year: "ساڵی دەرچوون : YYYY"
            elif "ساڵ" in text and num_match:
                year_val = num_match.group(1)
                if len(year_val) == 4:
                    result["year"] = year_val
            # Status: بەرکارە (active) or other
            elif text and not num_match and len(text) < 50:
                result["status"] = text

        # Full text from <pre> element
        pre = soup.find("pre")
        if pre:
            text = pre.get_text(separator="\n", strip=False)
            # Clean up excessive whitespace while preserving structure
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r"[ \t]{2,}", " ", text)
            result["text"] = text.strip()

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        law_id = raw.get("law_id", "")
        doc_type = raw.get("doc_type", "law")
        year = raw.get("year", "")
        law_number = raw.get("law_number", "")

        _id = f"KRG-{doc_type}-{year}-{law_number}" if year and law_number else f"KRG-{law_id}"

        date = f"{year}-01-01" if year and len(year) == 4 else ""

        return {
            "_id": _id,
            "_source": "IQ/KRG-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "law_number": raw.get("law_number", ""),
            "year": year,
            "status": raw.get("status", ""),
            "doc_type": doc_type,
        }

    def _crawl_laws_by_year(self) -> Generator[Dict[str, str], None, None]:
        """Crawl law listings by year."""
        for year in LAW_YEARS:
            url = f"{BASE_URL}/years/?year={year}"
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch year {year}")
                continue

            docs = self._parse_listing_page(resp.text)
            logger.info(f"Year {year}: {len(docs)} laws")
            for doc in docs:
                doc["doc_type"] = "law"
                yield doc

    def _crawl_orders(self) -> Generator[Dict[str, str], None, None]:
        """Crawl paginated order listings."""
        for page in range(1, ORDER_MAX_PAGES + 1):
            url = f"{BASE_URL}/orders-law?pageNumber={page}"
            resp = self._request(url)
            if resp is None:
                break

            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No orders on page {page}, stopping")
                break

            logger.info(f"Orders page {page}: {len(docs)} entries")
            for doc in docs:
                doc["doc_type"] = "order"
                yield doc

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation (laws by year + orders by page)."""
        count = 0
        seen_ids = set()

        # First crawl laws by year
        for doc in self._crawl_laws_by_year():
            if doc["law_id"] in seen_ids:
                continue
            seen_ids.add(doc["law_id"])

            detail_resp = self._request(doc["url"])
            if detail_resp is None:
                logger.warning(f"Failed to fetch detail: {doc['title'][:60]}")
                continue

            detail = self._extract_detail(detail_resp.text)
            if not detail["text"] or len(detail["text"]) < 50:
                logger.warning(f"Insufficient text ({len(detail.get('text', ''))} chars): {doc['title'][:60]}")
                continue

            raw = {
                "law_id": doc["law_id"],
                "title": detail["title"] or doc["title"],
                "text": detail["text"],
                "law_number": detail["law_number"],
                "year": detail["year"],
                "status": detail["status"],
                "url": doc["url"],
                "doc_type": doc["doc_type"],
            }
            count += 1
            yield raw

        # Then crawl orders
        for doc in self._crawl_orders():
            if doc["law_id"] in seen_ids:
                continue
            seen_ids.add(doc["law_id"])

            detail_resp = self._request(doc["url"])
            if detail_resp is None:
                logger.warning(f"Failed to fetch detail: {doc['title'][:60]}")
                continue

            detail = self._extract_detail(detail_resp.text)
            if not detail["text"] or len(detail["text"]) < 50:
                logger.warning(f"Insufficient text ({len(detail.get('text', ''))} chars): {doc['title'][:60]}")
                continue

            raw = {
                "law_id": doc["law_id"],
                "title": detail["title"] or doc["title"],
                "text": detail["text"],
                "law_number": detail["law_number"],
                "year": detail["year"],
                "status": detail["status"],
                "url": doc["url"],
                "doc_type": doc["doc_type"],
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent legislation (latest 2 years)."""
        count = 0
        seen_ids = set()

        for year in [2022, 2021]:
            url = f"{BASE_URL}/years/?year={year}"
            resp = self._request(url)
            if resp is None:
                continue

            docs = self._parse_listing_page(resp.text)
            for doc in docs:
                if doc["law_id"] in seen_ids:
                    continue
                seen_ids.add(doc["law_id"])

                detail_resp = self._request(doc["url"])
                if detail_resp is None:
                    continue

                detail = self._extract_detail(detail_resp.text)
                if not detail["text"] or len(detail["text"]) < 50:
                    continue

                raw = {
                    "law_id": doc["law_id"],
                    "title": detail["title"] or doc["title"],
                    "text": detail["text"],
                    "law_number": detail["law_number"],
                    "year": detail["year"],
                    "status": detail["status"],
                    "url": doc["url"],
                    "doc_type": "law",
                }
                count += 1
                yield raw

        logger.info(f"Updates: {count} documents fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{BASE_URL}/years/?year=2022")
        if resp is None:
            logger.error("Cannot reach legislation.krd")
            return False

        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No legislation found for 2022")
            return False

        logger.info(f"Listing OK: {len(docs)} laws for 2022")

        detail_resp = self._request(docs[0]["url"])
        if detail_resp:
            detail = self._extract_detail(detail_resp.text)
            logger.info(f"Detail OK: {docs[0]['title'][:60]} ({len(detail['text'])} chars)")
            return True

        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IQ/KRG-Legislation data fetcher")
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

    scraper = KRGLegislationScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
