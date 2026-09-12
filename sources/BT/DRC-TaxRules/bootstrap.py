#!/usr/bin/env python3
"""
BT/DRC-TaxRules -- Bhutan Department of Revenue and Customs — Tax Legislation

Fetches tax acts, rules, and regulations from the DRC Acts & Policy page.
All documents are PDFs with extractable text (English).

Strategy:
  - Scrape the Acts & Policy page for PDF links and titles
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
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import unquote, urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BT.DRC-TaxRules")

ACTS_POLICY_URL = "https://www.drc.gov.bt/acts-policy/"
BASE_URL = "https://www.drc.gov.bt"


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
            if href.lower().endswith(".pdf"):
                self.in_pdf_link = True
                self.current_url = href
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


def _title_from_url(url: str) -> str:
    """Derive a title from a PDF URL filename."""
    fname = unquote(url.rsplit("/", 1)[-1])
    fname = fname.replace(".pdf", "").replace("-", " ").replace("_", " ")
    fname = re.sub(r"\s+", " ", fname).strip()
    return fname


def _make_id(url: str) -> str:
    """Create a stable document ID from the PDF URL."""
    fname = unquote(url.rsplit("/", 1)[-1])
    fname = fname.replace(".pdf", "").strip()
    fname = re.sub(r"[^a-zA-Z0-9_-]", "_", fname)
    fname = re.sub(r"_+", "_", fname).strip("_")
    return fname[:120]


class DRCTaxRulesScraper(BaseScraper):
    """Scraper for BT/DRC-TaxRules -- Bhutan tax legislation PDFs."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
        })

    def _request(self, url: str, timeout: int = 60, stream: bool = False) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout, stream=stream)
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
        """Scrape the Acts & Policy page for PDF links and titles."""
        resp = self._request(ACTS_POLICY_URL)
        if resp is None:
            logger.error("Cannot fetch Acts & Policy page")
            return []

        parser = _PDFLinkExtractor()
        parser.feed(resp.text)

        results = []
        seen_urls = set()
        for title, url in parser.links:
            full_url = urljoin(BASE_URL, url)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            if not title or title == "(New)":
                title = _title_from_url(url)

            results.append((title, full_url))

        logger.info(f"Found {len(results)} PDFs on Acts & Policy page")
        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "BT/DRC-TaxRules",
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
            logger.info(f"Downloading: {title}")

            text = extract_pdf_markdown(
                source="BT/DRC-TaxRules",
                source_id=doc_id,
                pdf_url=url,
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
            logger.error("Cannot fetch PDF list from Acts & Policy page")
            return False

        logger.info(f"Acts & Policy page OK: {len(pdf_list)} PDFs found")

        title, url = pdf_list[0]
        logger.info(f"Testing download: {title}")
        text = extract_pdf_markdown(
            source="BT/DRC-TaxRules",
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
    parser = argparse.ArgumentParser(description="BT/DRC-TaxRules data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DRCTaxRulesScraper()

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
