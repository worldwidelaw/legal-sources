#!/usr/bin/env python3
"""
GE/Parliament -- Georgian Legislative Herald (matsne.gov.ge)

Fetches consolidated Georgian legislation from matsne.gov.ge, the official
legislative database of Georgia.

Strategy:
  - Enumerate laws via search endpoint (type=1000003, paginated)
  - Fetch full text from document view pages (HTML in <div id="maindoc">)
  - Clean HTML to plain text
  - 10-second crawl delay per robots.txt

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
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
logger = logging.getLogger("legal-data-hunter.GE.Parliament")

BASE_URL = "https://matsne.gov.ge"
SEARCH_URL = f"{BASE_URL}/en/document/search"
CRAWL_DELAY = 10  # per robots.txt


class GeorgiaParliamentScraper(BaseScraper):
    """Scraper for Georgian legislation from matsne.gov.ge."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with crawl delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(CRAWL_DELAY)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 60s")
                    time.sleep(60)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(15)
        return None

    def _get_search_results(self, page: int = 1, doc_type: str = "1000003") -> List[Dict[str, Any]]:
        """Get document list from search page."""
        params = {
            "type": doc_type,
            "limit": "100",
            "page": str(page),
            "sort": "publishDate_desc",
        }
        url = f"{SEARCH_URL}?{'&'.join(f'{k}={v}' for k, v in params.items())}"
        resp = self._request(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        documents = []

        # Find document links in search results
        links = soup.find_all("a", href=re.compile(r"/en/document/view/\d+"))
        seen_ids = set()
        for link in links:
            href = link.get("href", "")
            doc_id_match = re.search(r"/en/document/view/(\d+)", href)
            if not doc_id_match:
                continue
            doc_id = doc_id_match.group(1)
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            title = link.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            documents.append({
                "doc_id": doc_id,
                "title": title,
                "url": f"{BASE_URL}/en/document/view/{doc_id}",
            })

        return documents

    def _get_total_pages(self, doc_type: str = "1000003") -> int:
        """Get total number of pages from search."""
        url = f"{SEARCH_URL}?type={doc_type}&limit=100&page=1&sort=publishDate_desc"
        resp = self._request(url)
        if not resp:
            return 0

        soup = BeautifulSoup(resp.text, "html.parser")
        # Look for pagination
        pagination = soup.find("ul", class_="pagination")
        if not pagination:
            return 1

        page_links = pagination.find_all("a", href=re.compile(r"page=\d+"))
        max_page = 1
        for link in page_links:
            href = link.get("href", "")
            page_match = re.search(r"page=(\d+)", href)
            if page_match:
                max_page = max(max_page, int(page_match.group(1)))
        return max_page

    def _fetch_document_text(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """Fetch full text of a document from its view page."""
        url = f"{BASE_URL}/en/document/view/{doc_id}?publication=0"
        resp = self._request(url)
        if not resp:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract full text from main document div
        maindoc = soup.find("div", id="maindoc")
        if not maindoc:
            # Try alternative selectors
            maindoc = soup.find("div", class_="document-content")
            if not maindoc:
                maindoc = soup.find("div", class_="main-content")

        if not maindoc:
            logger.warning(f"No main document content found for {doc_id}")
            return None

        # Clean HTML to text
        text = self._clean_html(maindoc)
        if len(text) < 100:
            logger.warning(f"Insufficient text for {doc_id}: {len(text)} chars")
            return None

        # Extract metadata from the page
        metadata = self._extract_metadata(soup)

        return {
            "text": text,
            "metadata": metadata,
        }

    def _clean_html(self, element) -> str:
        """Clean HTML element to plain text."""
        # Remove script and style elements
        for tag in element.find_all(["script", "style", "nav"]):
            tag.decompose()

        text = element.get_text(separator="\n")
        # Clean up whitespace
        lines = []
        for line in text.split("\n"):
            line = line.strip()
            if line:
                lines.append(line)
        return "\n".join(lines)

    def _extract_metadata(self, soup) -> Dict[str, str]:
        """Extract metadata from document page."""
        metadata = {}

        # Look for metadata table
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    key = cells[0].get_text(strip=True).lower()
                    value = cells[1].get_text(strip=True)
                    if "document number" in key or "number" in key:
                        metadata["document_number"] = value
                    elif "issuer" in key:
                        metadata["issuer"] = value
                    elif "issuing" in key or "signing" in key:
                        metadata["date"] = value
                    elif "type" in key:
                        metadata["document_type"] = value
                    elif "registration" in key:
                        metadata["registration_code"] = value
                    elif "activating" in key:
                        metadata["activating_date"] = value

        return metadata

    def _parse_date(self, date_str: str) -> str:
        """Parse Georgian date formats to ISO 8601."""
        if not date_str:
            return ""
        # Try common formats: DD/MM/YYYY, DD.MM.YYYY, Month DD, YYYY
        patterns = [
            (r"(\d{2})/(\d{2})/(\d{4})", lambda m: f"{m.group(3)}-{m.group(2)}-{m.group(1)}"),
            (r"(\d{2})\.(\d{2})\.(\d{4})", lambda m: f"{m.group(3)}-{m.group(2)}-{m.group(1)}"),
            (r"(\d{4})-(\d{2})-(\d{2})", lambda m: m.group(0)),
        ]
        for pat, fmt in patterns:
            match = re.search(pat, date_str)
            if match:
                return fmt(match)
        return date_str

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw document data to standard schema."""
        doc_id = raw.get("doc_id", "")
        metadata = raw.get("metadata", {})
        date = self._parse_date(metadata.get("date", ""))

        return {
            "_id": f"GE-MATSNE-{doc_id}",
            "_source": "GE/Parliament",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "document_number": metadata.get("document_number", ""),
            "issuer": metadata.get("issuer", ""),
            "registration_code": metadata.get("registration_code", ""),
            "document_type": metadata.get("document_type", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Georgian laws from matsne.gov.ge."""
        count = 0
        page = 1

        # Get first page to determine document list
        while True:
            logger.info(f"Fetching search page {page}...")
            docs = self._get_search_results(page=page)
            if not docs:
                logger.info(f"No more results at page {page}")
                break

            for doc in docs:
                doc_data = self._fetch_document_text(doc["doc_id"])
                if doc_data:
                    doc["text"] = doc_data["text"]
                    doc["metadata"] = doc_data["metadata"]
                    count += 1
                    yield doc

            page += 1
            if page > 20:  # Safety limit
                break

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        url = f"{SEARCH_URL}?type=1000003&limit=10&page=1&sort=publishDate_desc"
        resp = self._request(url)
        if resp is None:
            logger.error("Cannot reach matsne.gov.ge search")
            return False

        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=re.compile(r"/en/document/view/\d+"))
        if not links:
            logger.error("No document links found in search results")
            return False

        doc_id_match = re.search(r"/en/document/view/(\d+)", links[0].get("href", ""))
        if not doc_id_match:
            logger.error("Could not extract document ID")
            return False

        doc_id = doc_id_match.group(1)
        logger.info(f"Search OK: found document links. Testing doc {doc_id}...")

        # Test document fetch
        doc_data = self._fetch_document_text(doc_id)
        if doc_data and len(doc_data.get("text", "")) >= 100:
            logger.info(f"Document fetch OK: {len(doc_data['text'])} chars")
            return True
        else:
            logger.error("Document fetch failed or insufficient text")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GE/Parliament data fetcher")
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

    scraper = GeorgiaParliamentScraper()

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
