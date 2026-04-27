#!/usr/bin/env python3
"""
SH/Legislation -- Saint Helena, Ascension & Tristan da Cunha Legislation Fetcher

Fetches consolidated ordinances and regulations from sainthelena.gov.sh.
Laws are published as PDFs organized in alphabetical lists for each territory.

Strategy:
  - Scrape 3 alphabetical listing pages (St Helena, Ascension, Tristan da Cunha)
  - Extract PDF links from each listing
  - Download and extract full text from each PDF

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import hashlib
import logging
import re
import time
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
logger = logging.getLogger("legal-data-hunter.SH.Legislation")

BASE_URL = "https://www.sainthelena.gov.sh"

# Three territory listing pages
TERRITORY_LISTS = [
    {
        "territory": "St Helena",
        "code": "SH",
        "path": "/government/legislation/laws-of-st-helena/alphabetical-list-st-helena/",
    },
    {
        "territory": "Ascension Island",
        "code": "AC",
        "path": "/st-helena/government/legislation/laws-of-ascension/alphabetical-list-ascension/",
    },
    {
        "territory": "Tristan da Cunha",
        "code": "TA",
        "path": "/st-helena/government/legislation/laws-of-tristan-da-cunha/alphabetical-list-tristan-da-cunha/",
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class SaintHelenaLegislationScraper(BaseScraper):
    """Scraper for SH/Legislation -- Saint Helena territory laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 30, stream: bool = False) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout, allow_redirects=True, stream=stream)
                if resp.status_code in (404, 410):
                    logger.warning(f"Not found: {url[:80]}")
                    return None
                if resp.status_code == 503:
                    logger.warning(f"503 Service Unavailable: {url[:80]}")
                    if attempt < 2:
                        time.sleep(10)
                        continue
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_pdf_links(self, list_url: str) -> list:
        """Extract document PDF links from an alphabetical list page."""
        resp = self._request(list_url)
        if resp is None:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        links = []
        seen = set()

        # Find all links to /documents/ paths (these redirect to PDFs)
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # Skip non-document links
            if "/documents/" not in href and not href.endswith(".pdf"):
                continue

            full_url = urljoin(list_url, href)
            # Normalize
            full_url = full_url.rstrip("/")

            if full_url in seen:
                continue
            seen.add(full_url)

            title = a.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            # Skip non-legislation items (category lists, chronological tables, etc.)
            skip_keywords = ["chronological table", "categories list", "subject matter",
                             "category list", "table of contents"]
            if any(kw in title.lower() for kw in skip_keywords):
                continue

            links.append({"url": full_url, "title": title})

        return links

    def _fetch_document(self, url: str, title: str, territory_code: str) -> Optional[Dict[str, Any]]:
        """Download a PDF and extract full text."""
        # Follow redirect to get actual PDF
        resp = self._request(url, timeout=60)
        if resp is None:
            return None

        content_type = resp.headers.get("Content-Type", "")
        final_url = resp.url

        # If we got HTML instead of PDF, look for PDF link in the page
        if "html" in content_type.lower():
            soup = BeautifulSoup(resp.text, "html.parser")
            pdf_link = None
            for a in soup.find_all("a", href=True):
                if a["href"].lower().endswith(".pdf"):
                    pdf_link = urljoin(final_url, a["href"])
                    break
            if not pdf_link:
                # Check for meta refresh or embedded PDF
                for meta in soup.find_all("meta", attrs={"http-equiv": "refresh"}):
                    content = meta.get("content", "")
                    if ".pdf" in content.lower():
                        m = re.search(r'url=(.+\.pdf)', content, re.IGNORECASE)
                        if m:
                            pdf_link = urljoin(final_url, m.group(1))
                            break
            if pdf_link:
                resp = self._request(pdf_link, timeout=60)
                if resp is None:
                    return None
                final_url = resp.url
            else:
                logger.warning(f"No PDF found at {url[:80]}")
                return None

        pdf_bytes = resp.content
        if len(pdf_bytes) < 200:
            logger.warning(f"PDF too small ({len(pdf_bytes)} bytes): {url[:80]}")
            return None

        # Generate document ID from URL slug
        slug = url.rstrip("/").split("/")[-1]
        slug = re.sub(r'\.pdf$', '', slug, flags=re.IGNORECASE)
        doc_id = hashlib.md5(slug.encode("utf-8")).hexdigest()[:12]

        text = extract_pdf_markdown(
            source="SH/Legislation",
            source_id=f"SH-LEG-{territory_code}-{doc_id}",
            pdf_bytes=pdf_bytes,
            table="legislation",
            force=True,
        )

        if not text or len(text) < 100:
            logger.warning(f"Insufficient text ({len(text) if text else 0} chars): {title[:60]}")
            return None

        return {
            "document_id": f"SH-LEG-{territory_code}-{doc_id}",
            "title": title,
            "text": text,
            "url": final_url,
            "territory": territory_code,
        }

    def _parse_year(self, title: str) -> Optional[str]:
        """Extract year from legislation title."""
        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
        if m:
            return f"{m.group(1)}-01-01"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "")
        return {
            "_id": raw.get("document_id", ""),
            "_source": "SH/Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": self._parse_year(title),
            "url": raw.get("url", ""),
            "territory": raw.get("territory", "SH"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation documents from all three territories."""
        count = 0
        for territory in TERRITORY_LISTS:
            list_url = BASE_URL + territory["path"]
            code = territory["code"]
            logger.info(f"Fetching {territory['territory']} list: {list_url}")

            links = self._extract_pdf_links(list_url)
            logger.info(f"{territory['territory']}: {len(links)} documents found")

            for link in links:
                doc = self._fetch_document(link["url"], link["title"], code)
                if doc is None:
                    continue
                count += 1
                yield doc

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (re-fetches all for this source)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        list_url = BASE_URL + TERRITORY_LISTS[0]["path"]
        resp = self._request(list_url)
        if resp is None:
            logger.error("Cannot reach St Helena legislation page")
            return False

        links = self._extract_pdf_links(list_url)
        logger.info(f"St Helena alphabetical list: {len(links)} links found")

        if not links:
            logger.error("No PDF links found on listing page")
            return False

        # Test one document
        doc = self._fetch_document(links[0]["url"], links[0]["title"], "SH")
        if doc and len(doc.get("text", "")) > 100:
            logger.info(f"Document OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
            return True
        else:
            logger.error("Failed to fetch test document")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SH/Legislation data fetcher")
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

    scraper = SaintHelenaLegislationScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records -- {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
