#!/usr/bin/env python3
"""
KN/FSRC -- St Kitts & Nevis Financial Services Regulatory Commission

Fetches legislation and regulatory documents from the FSRC Law Library.
PDF documents across 19 regulatory categories (AML, companies, insurance, etc.)

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import urljoin

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
logger = logging.getLogger("legal-data-hunter.KN.FSRC")

BASE_URL = "https://www.fsrc.kn"

CATEGORIES = [
    "anti-money-laundering",
    "anti-proliferation-financing",
    "anti-terrorism",
    "companies",
    "credit-unions",
    "designated-non-financial-businesses-and-professions-dnfbps",
    "escrow-business-for-citizenship-by-investment",
    "exchange-of-information",
    "financial-services",
    "foundations",
    "gaming",
    "insurance-businesses",
    "limited-partnerships",
    "merchant-shipping",
    "money-service-business",
    "non-government-organisations",
    "trusts",
    "un-sanctions-list",
    "virtual-assets",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class FSRCScraper(BaseScraper):
    """Scraper for KN/FSRC -- St Kitts & Nevis FSRC Law Library."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code in (404, 410):
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _iter_category_docs(self, category: str) -> Generator[Dict[str, Any], None, None]:
        """Iterate through all documents in a law library category (with pagination)."""
        offset = 0
        page_size = 20
        seen = set()

        while True:
            if offset == 0:
                url = f"{BASE_URL}/law-library/{category}"
            else:
                url = f"{BASE_URL}/law-library/{category}?limit={page_size}&limitstart={offset}"

            resp = self._request(url)
            if resp is None:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            found = 0

            for a in soup.find_all("a", href=True):
                href = a["href"]
                text = a.get_text(strip=True)

                # Match document download links like /law-library/{cat}/{id}-{slug}/file
                if not href.endswith("/file"):
                    continue
                if "/law-library/" not in href:
                    continue

                full_url = urljoin(BASE_URL, href)
                if full_url in seen:
                    continue
                seen.add(full_url)

                # Extract document title from the slug
                # href format: /law-library/{cat}/{id}-{slug}/file
                path_part = href.replace("/file", "").split("/")[-1]
                # Extract numeric ID and slug
                m = re.match(r"(\d+)-(.*)", path_part)
                if m:
                    doc_num = m.group(1)
                    slug = m.group(2).replace("-", " ").title()
                else:
                    doc_num = hashlib.md5(href.encode()).hexdigest()[:8]
                    slug = path_part.replace("-", " ").title()

                # Use nearby text for title if available
                title = text if text and text.lower() not in ("download", "download(pdf)", "folder") else slug

                found += 1
                yield {
                    "document_id": f"KN-FSRC-{doc_num}",
                    "title": title,
                    "category": category,
                    "pdf_url": full_url,
                    "doc_type": "doctrine",
                }

            if found == 0:
                break
            offset += page_size

    def _extract_pdf_text(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        try:
            text = extract_pdf_markdown("KN/FSRC", doc_id, pdf_url=pdf_url, force=True)
            if text and len(text.strip()) > 50:
                return text.strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url[:60]}: {e}")
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "")

        # Try to extract year from title
        date = None
        m = re.search(r"\b(19\d{2}|20[0-2]\d)\b", title)
        if m:
            date = f"{m.group(1)}-01-01"

        return {
            "_id": raw.get("document_id", ""),
            "_source": "KN/FSRC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("pdf_url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents from the FSRC Law Library."""
        count = 0
        for category in CATEGORIES:
            logger.info(f"Crawling category: {category}")
            for doc_info in self._iter_category_docs(category):
                text = self._extract_pdf_text(doc_info["pdf_url"], doc_info["document_id"])
                if text is None:
                    continue
                doc_info["text"] = text
                count += 1
                yield doc_info

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        resp = self._request(f"{BASE_URL}/law-library")
        if resp is None:
            logger.error("Cannot reach FSRC law library")
            return False
        logger.info("FSRC law library page OK")

        # Test PDF extraction
        test_url = f"{BASE_URL}/law-library/anti-money-laundering/1227-financial-intelligence-unit-amendment-act-no-26-of-2024/file"
        text = self._extract_pdf_text(test_url, "test")
        if text:
            logger.info(f"PDF extraction OK: {len(text)} chars")
        else:
            logger.error("PDF extraction failed")
            return False

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KN/FSRC data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = FSRCScraper()

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
