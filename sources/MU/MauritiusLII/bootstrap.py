#!/usr/bin/env python3
"""
MU/MauritiusLII -- Mauritius Legal Information Institute Fetcher

Fetches consolidated legislation from MauritiusLII (mauritiuslii.org),
a Laws.Africa/AfricanLII PeachJam/Akoma Ntoso platform.

Strategy:
  - Paginate legislation listing (/legislation/all?page=N)
  - Extract AKN links, fetch each document page
  - Extract full text from la-akoma-ntoso element

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
logger = logging.getLogger("legal-data-hunter.MU.MauritiusLII")

BASE_URL = "https://mauritiuslii.org"
LEGIS_URL = f"{BASE_URL}/legislation/all"
MAX_PAGES = 200


class MauritiusLIIScraper(BaseScraper):
    """Scraper for MU/MauritiusLII -- Mauritius legislation."""

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
                time.sleep(5)
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
        """Parse a listing page for AKN document links."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen = set()

        links = soup.find_all("a", href=lambda h: h and "/akn/mu/act/" in str(h))
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

    def _extract_full_text(self, html: str) -> Dict[str, str]:
        """Extract full text and metadata from an AKN document page."""
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "date": "", "title": ""}

        h1 = soup.find("h1")
        if h1:
            result["title"] = h1.get_text(strip=True)

        # Full text from la-akoma-ntoso element
        akn = soup.find("la-akoma-ntoso")
        if akn:
            text = akn.get_text(separator="\n", strip=True)
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            result["text"] = text.strip()

        # Fallback: try main content area
        if not result["text"]:
            content = soup.find("article") or soup.find("main") or soup.find("div", class_="content")
            if content:
                for tag in content.find_all(["script", "style", "nav"]):
                    tag.decompose()
                text = content.get_text(separator="\n", strip=True)
                text = re.sub(r"\n{3,}", "\n\n", text)
                text = re.sub(r" {2,}", " ", text)
                result["text"] = text.strip()

        # Date from <time> element
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
        doc_id = re.sub(r"^/akn/mu/act/", "MU-ACT-", href)
        doc_id = re.sub(r"/eng@.*$", "", doc_id)
        doc_id = doc_id.replace("/", "-")

        return {
            "_id": doc_id,
            "_source": "MU/MauritiusLII",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
        }

    def _fetch_paginated(self, base_url: str, max_docs: Optional[int] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents from paginated listing pages."""
        count = 0
        seen_urls = set()

        for page_num in range(1, MAX_PAGES + 1):
            url = f"{base_url}?page={page_num}" if "?" not in base_url else f"{base_url}&page={page_num}"
            resp = self._request(url)
            if resp is None:
                break

            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No documents on page {page_num}, stopping")
                break

            logger.info(f"Page {page_num}: {len(docs)} documents")

            for doc in docs:
                doc_url = doc["url"]
                if doc_url in seen_urls:
                    continue
                seen_urls.add(doc_url)

                doc_resp = self._request(doc_url)
                if doc_resp is None:
                    continue

                extracted = self._extract_full_text(doc_resp.text)
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

                if max_docs and count >= max_docs:
                    return

        logger.info(f"Fetched {count} legislation documents")

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation."""
        yield from self._fetch_paginated(LEGIS_URL)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(BASE_URL + "/legislation/all")
        if resp is None:
            logger.error("Cannot reach MauritiusLII legislation listing")
            return False

        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No legislation links found")
            return False

        logger.info(f"Legislation listing OK: {len(docs)} acts on page 1")

        # Test fetching one document
        doc_resp = self._request(docs[0]["url"])
        if doc_resp is None:
            logger.error("Cannot fetch document")
            return False

        extracted = self._extract_full_text(doc_resp.text)
        if extracted["text"] and len(extracted["text"]) > 100:
            logger.info(f"Document OK: {extracted['title'][:60]} ({len(extracted['text'])} chars)")
            return True

        logger.error("Failed to extract full text")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MU/MauritiusLII data fetcher")
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

    scraper = MauritiusLIIScraper()

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
