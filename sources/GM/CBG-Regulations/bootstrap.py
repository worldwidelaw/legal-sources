#!/usr/bin/env python3
"""
GM/CBG-Regulations -- Central Bank of The Gambia — Guidelines & Regulations

Fetches financial regulations, guidelines, insurance acts, FX policy, and
payment system rules from the Central Bank of The Gambia website.

Strategy:
  - Scrape multiple CBG pages for download links (UUID-based)
  - Download each PDF via /downloads-file/{uuid}
  - Extract full text via common.pdf_extract

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GM.CBG-Regulations")

BASE_URL = "https://www.cbg.gm"

# All pages containing regulation download links
SOURCE_PAGES = [
    "https://www.cbg.gm/guidelines",
    "https://www.cbg.gm/guidelines-1",
    "https://www.cbg.gm/guidelines-2",
    "https://www.cbg.gm/guidelines-3",
    "https://www.cbg.gm/guidelines-4",
    "https://www.cbg.gm/insurance-act-regulations",
    "https://www.cbg.gm/insurance-guidelines-and-directives",
    "https://www.cbg.gm/foreign-exchange-regulation-and-policy",
    "https://www.cbg.gm/payment-system-pricing-policy",
]


class _DownloadLinkExtractor(HTMLParser):
    """Extract /downloads-file/ links and their titles from HTML."""

    def __init__(self):
        super().__init__()
        self.in_link = False
        self.current_url = ""
        self.text_parts: List[str] = []
        self.links: List[Tuple[str, str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            href = d.get("href", "")
            if "/downloads-file/" in href:
                self.in_link = True
                self.current_url = href
                self.text_parts = []

    def handle_data(self, data):
        if self.in_link:
            self.text_parts.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self.in_link:
            title = " ".join(self.text_parts).strip()
            if self.current_url and title:
                self.links.append((title, self.current_url))
            self.in_link = False


def _clean_title(raw_title: str) -> str:
    """Clean title by removing Size/Type metadata appended by the CMS."""
    title = raw_title
    # Remove "Size: XXX KB/MB  Type: pdf" suffixes
    title = re.sub(r"\s*Size:\s*[\d.]+\s*[KMG]?B.*$", "", title, flags=re.IGNORECASE)
    title = re.sub(r"\s*Type:\s*\w+.*$", "", title, flags=re.IGNORECASE)
    return title.strip()


def _make_id(url: str) -> str:
    """Create a stable document ID from the UUID download URL."""
    # Extract UUID from /downloads-file/{uuid}
    match = re.search(r"/downloads-file/([a-f0-9-]+)", url)
    if match:
        return match.group(1)
    return url[-40:]


class CBGRegulationsScraper(BaseScraper):
    """Scraper for GM/CBG-Regulations -- Gambia central bank regulation PDFs."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
        })
        self.session.verify = False

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    logger.warning(f"404 for {url}")
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _get_doc_list(self) -> List[Tuple[str, str]]:
        """Scrape all pages for download links. Returns (title, full_url)."""
        seen_urls = set()
        results = []

        for page_url in SOURCE_PAGES:
            resp = self._request(page_url)
            if resp is None:
                logger.warning(f"Cannot fetch {page_url}")
                continue

            parser = _DownloadLinkExtractor()
            parser.feed(resp.text)

            for raw_title, href in parser.links:
                full_url = urljoin(BASE_URL, href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                title = _clean_title(raw_title)
                if not title or len(title) < 3:
                    continue

                results.append((title, full_url))

        logger.info(f"Found {len(results)} documents across all pages")
        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "GM/CBG-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        doc_list = self._get_doc_list()
        if not doc_list:
            logger.error("No documents found")
            return

        count = 0
        for title, url in doc_list:
            if max_records and count >= max_records:
                return

            doc_id = _make_id(url)
            logger.info(f"Downloading: {title}")

            text = extract_pdf_markdown(
                source="GM/CBG-Regulations",
                source_id=doc_id,
                pdf_url=url,
                table="legislation",
            )

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text ({len(text or '')} chars): {title}")
                continue

            # Try to extract a year from the title
            date = None
            year_match = re.search(r"\b(19\d{2}|20\d{2})\b", title)
            if year_match:
                date = f"{year_match.group(1)}-01-01"

            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        doc_list = self._get_doc_list()
        if not doc_list:
            logger.error("Cannot fetch document list")
            return False

        logger.info(f"Pages OK: {len(doc_list)} documents found")

        title, url = doc_list[0]
        logger.info(f"Testing download: {title}")
        text = extract_pdf_markdown(
            source="GM/CBG-Regulations",
            source_id="test",
            pdf_url=url,
            table="legislation",
            force=True,
        )
        if text:
            logger.info(f"PDF extraction OK: {len(text)} chars")
        else:
            logger.warning("PDF extraction returned no text")

        return True


def main():
    parser = argparse.ArgumentParser(description="GM/CBG-Regulations data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBGRegulationsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
