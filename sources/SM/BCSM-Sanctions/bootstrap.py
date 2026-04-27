#!/usr/bin/env python3
"""
SM/BCSM-Sanctions -- San Marino Central Bank Sanctions

Fetches administrative sanctioning measures from BCSM with full text from PDFs.

Strategy:
  - Scrape the sanctions listing page (Italian version) for PDF links
  - Download each PDF and extract full text
  - Also fetch the L.132/2023 consolidated sanctions table PDF
  - ~11 individual sanctions + 1 consolidated table

Usage:
  python bootstrap.py bootstrap          # Fetch all sanctions
  python bootstrap.py bootstrap --sample # Fetch sample records
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
logger = logging.getLogger("legal-data-hunter.SM.BCSM-Sanctions")

BASE_URL = "https://www.bcsm.sm"
LISTING_URL_IT = f"{BASE_URL}/funzioni/sanzioni/provvedimenti-sanzionatori"
L132_URL = f"{BASE_URL}/funzioni/sanzioni/provvedimenti-sanzionatori/sanzioni-pubblicate-l132"


class BCSMSanctionsScraper(BaseScraper):
    """Scraper for SM/BCSM-Sanctions -- San Marino Central Bank sanctioning measures."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "it-IT,it;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with rate limiting and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
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

    def _download_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download a PDF and extract text."""
        resp = self._request(pdf_url, timeout=120)
        if resp is None:
            return ""
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type and not pdf_url.endswith(".pdf"):
            logger.warning(f"Not a PDF: {pdf_url} (Content-Type: {content_type})")
            return ""
        text = extract_pdf_markdown(
            source="SM/BCSM-Sanctions",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="doctrine",
        ) or ""
        return text.strip()

    def _parse_main_page(self, html: str, page_url: str) -> List[Dict[str, Any]]:
        """Parse the main sanctions listing page using schedule-row structure."""
        soup = BeautifulSoup(html, "html.parser")
        records = []
        seen_urls = set()

        # Each sanction is in a div.pwr-schedule-row
        for row in soup.find_all("div", class_="pwr-schedule-row"):
            # Date from left column: h3.pwr-schedule-row__title
            date = ""
            left = row.find("div", class_="pwr-schedule-row__left")
            if left:
                h3 = left.find("h3", class_="pwr-schedule-row__title")
                if h3:
                    date_text = h3.get_text(strip=True)
                    dm = re.match(r'(\d{2})/(\d{2})/(\d{4})', date_text)
                    if dm:
                        date = f"{dm.group(3)}-{dm.group(2)}-{dm.group(1)}"

            # Entity name and PDF link from right column
            right = row.find("div", class_="pwr-schedule-row__right")
            if not right:
                continue

            pdf_link = right.find("a", href=lambda h: h and ".pdf" in h)
            if not pdf_link:
                continue

            href = pdf_link["href"]
            full_url = urljoin(page_url, href.split("?")[0])
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            # Extract entity name from the desc span, excluding link text
            desc = right.find("span", class_="pwr-schedule-row__desc")
            title = ""
            if desc:
                # Get all text nodes except the PDF link text
                for child in desc.children:
                    if hasattr(child, 'name') and child.name == 'p':
                        p_text = child.get_text(strip=True)
                        # Skip the "Scarica il PDF" link paragraph
                        if "Scarica il PDF" in p_text or not p_text:
                            continue
                        title = p_text
                        break
                if not title:
                    # Try getting text from br-separated content
                    all_text = desc.get_text(separator="\n", strip=True)
                    lines = [l.strip() for l in all_text.split("\n")
                             if l.strip() and "Scarica il PDF" not in l]
                    if lines:
                        title = " — ".join(lines)

            if not title:
                title = Path(href).stem

            records.append({
                "title": title,
                "url": full_url,
                "date": date,
                "pdf_url": full_url,
            })

        return records

    def _parse_l132_page(self, html: str, page_url: str) -> List[Dict[str, Any]]:
        """Parse the L.132 sub-page for the consolidated sanctions table PDF."""
        soup = BeautifulSoup(html, "html.parser")
        records = []

        pdf_link = soup.find("a", href=lambda h: h and ".pdf" in h)
        if not pdf_link:
            return records

        href = pdf_link["href"]
        full_url = urljoin(page_url, href.split("?")[0])

        # Extract date from page text like "Agg. 28/02/2026"
        date = ""
        page_text = soup.get_text()
        agg_match = re.search(r'Agg\.\s*(\d{2})/(\d{2})/(\d{4})', page_text)
        if agg_match:
            date = f"{agg_match.group(3)}-{agg_match.group(2)}-{agg_match.group(1)}"

        records.append({
            "title": "Sanzioni pubblicate ex art. 20.2 L.132/2023 — Soggetti sanzionati",
            "url": full_url,
            "date": date,
            "pdf_url": full_url,
        })

        return records

    def _parse_listing_page(self, html: str, page_url: str) -> List[Dict[str, Any]]:
        """Parse a sanctions page for PDF links and metadata."""
        if "sanzioni-pubblicate-l132" in page_url:
            return self._parse_l132_page(html, page_url)
        return self._parse_main_page(html, page_url)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        url = raw.get("url", "")
        # Create a stable ID from the PDF filename
        filename = Path(url).stem if url else "unknown"
        doc_id = re.sub(r'[^a-zA-Z0-9_-]', '_', filename)
        doc_id = f"SM-BCSM-{doc_id}"

        return {
            "_id": doc_id,
            "_source": "SM/BCSM-Sanctions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all sanctioning measures from BCSM."""
        count = 0
        seen_urls = set()

        # Fetch main sanctions listing page (Italian)
        pages = [LISTING_URL_IT, L132_URL]
        for page_url in pages:
            resp = self._request(page_url)
            if resp is None:
                logger.warning(f"Failed to fetch listing page: {page_url}")
                continue

            records = self._parse_listing_page(resp.text, page_url)
            logger.info(f"Found {len(records)} PDF links on {page_url}")

            for rec in records:
                pdf_url = rec["pdf_url"]
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                text = self._download_pdf_text(pdf_url, rec["title"])
                if not text or len(text) < 50:
                    logger.warning(f"Insufficient text from {pdf_url}: {len(text)} chars")
                    continue

                rec["text"] = text
                count += 1
                yield rec

        logger.info(f"Completed: {count} sanctions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent sanctions (same as fetch_all for small dataset)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(LISTING_URL_IT)
        if resp is None:
            logger.error("Cannot reach BCSM sanctions page")
            return False

        records = self._parse_listing_page(resp.text, LISTING_URL_IT)
        if not records:
            logger.error("No PDF links found on sanctions page")
            return False

        logger.info(f"Listing OK: {len(records)} PDF links found")

        # Test downloading first PDF
        text = self._download_pdf_text(records[0]["pdf_url"], records[0]["title"])
        if text:
            logger.info(f"PDF OK: {len(text)} chars from {records[0]['title'][:60]}")
            return True
        else:
            logger.error("Failed to extract text from first PDF")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="SM/BCSM-Sanctions data fetcher")
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

    scraper = BCSMSanctionsScraper()

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
