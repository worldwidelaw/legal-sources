#!/usr/bin/env python3
"""
BT/RMA-Regulations -- Royal Monetary Authority of Bhutan — Prudential Regulations

Fetches financial regulations, rules, and guidelines from the RMA Legal Framework page.
All documents are PDFs with extractable text (English).

Strategy:
  - Scrape the Legal Framework page and sub-pages for PDF links and titles
  - Download each PDF
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
from urllib.parse import quote, unquote, urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BT.RMA-Regulations")

BASE_URL = "https://www.rma.org.bt"

# All pages containing PDF links to regulations
LEGISLATION_PAGES = [
    "https://www.rma.org.bt/legislation/",       # Main Legal Framework
    "https://www.rma.org.bt/legislation2/17/",    # Foreign Exchange Rules
    "https://www.rma.org.bt/legislation2/18/",    # Payment System Rules
    "https://www.rma.org.bt/legislation2/20/",    # Currency Guidelines
]


class _PDFLinkExtractor(HTMLParser):
    """Extract PDF links and their anchor text from HTML."""

    def __init__(self):
        super().__init__()
        self.in_pdf_link = False
        self.current_url = ""
        self.current_text_parts: List[str] = []
        self.links: List[Tuple[str, str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            href = d.get("href", "")
            if href.lower().endswith(".pdf") or href.lower().endswith('.pdf"'):
                self.in_pdf_link = True
                self.current_url = href.rstrip('"')
                self.current_text_parts = []

    def handle_data(self, data):
        if self.in_pdf_link:
            self.current_text_parts.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self.in_pdf_link:
            title = " ".join(self.current_text_parts).strip()
            if self.current_url:
                self.links.append((title, self.current_url))
            self.in_pdf_link = False


def _clean_title(title: str, url: str) -> str:
    """Clean up a document title, falling back to URL-derived title."""
    title = unescape(title).strip()
    if not title or len(title) < 3:
        fname = unquote(url.rsplit("/", 1)[-1])
        fname = fname.replace(".pdf", "").replace("-", " ").replace("_", " ")
        fname = re.sub(r"\s+", " ", fname).strip()
        title = fname
    return title


def _make_id(url: str) -> str:
    """Create a stable document ID from the PDF URL."""
    fname = unquote(url.rsplit("/", 1)[-1])
    fname = fname.replace(".pdf", "").strip()
    fname = re.sub(r"[^a-zA-Z0-9_-]", "_", fname)
    fname = re.sub(r"_+", "_", fname).strip("_")
    return fname[:120]


def _encode_url(url: str) -> str:
    """Properly encode a URL that may have unencoded spaces/special chars."""
    # Split into base and path
    if "/media/" in url:
        parts = url.split("/media/", 1)
        path = parts[1]
        # Encode the path portion, preserving /
        encoded = quote(path, safe="/")
        return parts[0] + "/media/" + encoded
    return url


class RMARegulationsScraper(BaseScraper):
    """Scraper for BT/RMA-Regulations -- Bhutan central bank regulations PDFs."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
        })
        self.session.verify = False  # RMA cert has issues

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

    def _get_pdf_list(self) -> List[Tuple[str, str]]:
        """Scrape all legislation pages for PDF links and titles."""
        seen_urls = set()
        results = []

        for page_url in LEGISLATION_PAGES:
            resp = self._request(page_url)
            if resp is None:
                logger.warning(f"Cannot fetch {page_url}")
                continue

            parser = _PDFLinkExtractor()
            parser.feed(resp.text)

            for title, url in parser.links:
                full_url = urljoin(BASE_URL, unescape(url))
                # Normalize URL for dedup (decode then re-encode)
                norm_url = unquote(full_url).lower()
                if norm_url in seen_urls:
                    continue
                seen_urls.add(norm_url)

                # Skip non-regulation PDFs (forms, reports, money lender lists)
                skip_patterns = [
                    "Registered Private Money Lenders.pdf",
                    "forms_upload/",
                    "Publication/",
                ]
                if any(pat.lower() in full_url.lower() for pat in skip_patterns):
                    continue

                title = _clean_title(title, full_url)
                results.append((title, full_url))

        logger.info(f"Found {len(results)} regulation PDFs across all pages")
        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "BT/RMA-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        pdf_list = self._get_pdf_list()
        if not pdf_list:
            logger.error("No PDFs found")
            return

        count = 0
        for title, url in pdf_list:
            if max_records and count >= max_records:
                return

            doc_id = _make_id(url)
            encoded_url = _encode_url(url)
            logger.info(f"Downloading: {title}")

            text = extract_pdf_markdown(
                source="BT/RMA-Regulations",
                source_id=doc_id,
                pdf_url=encoded_url,
                table="legislation",
            )

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text ({len(text or '')} chars): {title}")
                continue

            # Try to extract a year from the title for the date
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
        pdf_list = self._get_pdf_list()
        if not pdf_list:
            logger.error("Cannot fetch PDF list")
            return False

        logger.info(f"Legislation pages OK: {len(pdf_list)} PDFs found")

        title, url = pdf_list[0]
        encoded_url = _encode_url(url)
        logger.info(f"Testing download: {title}")
        text = extract_pdf_markdown(
            source="BT/RMA-Regulations",
            source_id="test",
            pdf_url=encoded_url,
            table="legislation",
            force=True,
        )
        if text:
            logger.info(f"PDF extraction OK: {len(text)} chars")
        else:
            logger.warning("PDF extraction returned no text")

        return True


def main():
    parser = argparse.ArgumentParser(description="BT/RMA-Regulations data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = RMARegulationsScraper()

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
