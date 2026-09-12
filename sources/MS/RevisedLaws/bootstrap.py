#!/usr/bin/env python3
"""
MS/RevisedLaws -- Montserrat Revised Laws

Fetches ~1449 Montserrat legislation documents (Acts, SROs, Omitted Laws)
with full text from gov.ms. PDFs are downloaded and text extracted via pdfplumber.

Strategy:
  - Crawl multiple listing pages on the AG's Chambers site
  - Extract PDF links and titles from <a> tags
  - Download each PDF and extract text with pdfplumber

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import hashlib
import io
import json
import logging
import re
import sys
import time
import urllib3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MS.RevisedLaws")

# Suppress SSL warnings since gov.ms has certificate issues
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://www.gov.ms"
MAX_PDF_SIZE = 50 * 1024 * 1024  # 50MB

# All listing pages with their categories
LISTING_PAGES = [
    ("acts-revised-2019", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-revised-2019/"),
    ("acts-revised-2025", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-revised-2025/"),
    ("acts-passed-2016", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-passed-2016/"),
    ("acts-passed-2018-2020", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-passed-2018-2020/"),
    ("acts-passed-2021", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-passed-2021/"),
    ("acts-passed-2022", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-passed-2022/"),
    ("acts-passed-2023", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-passed-2023/"),
    ("acts-passed-2024", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-passed-2024/"),
    ("acts-passed-2025", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-passed-2025/"),
    ("acts-passed-2026", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/acts-passed-2026/"),
    ("sros", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/sros/"),
    ("sros-2018", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/sros-2018/"),
    ("sros-2015-2016", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/sros-2015-2016/"),
    ("sros-2013-2014", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/sros-2013-2014/"),
    ("omitted-laws", f"{BASE_URL}/government/legal-department/attorney-generals-chambers/omitted-laws/"),
]


class MSRevisedLawsScraper(BaseScraper):
    """Scraper for MS/RevisedLaws -- Montserrat legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False  # gov.ms has SSL cert issues
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _extract_pdf_links(self, page_url: str) -> List[Tuple[str, str]]:
        """Extract (title, pdf_url) pairs from a listing page."""
        resp = self._request(page_url)
        if resp is None:
            return []

        html = resp.text
        # Find <a> tags with PDF hrefs and their text
        links = re.findall(r'<a[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>', html, re.DOTALL)
        results = []
        seen_urls = set()
        for url, title_html in links:
            # Clean title
            title = re.sub(r'<[^>]+>', '', title_html).strip()
            if not title:
                # Derive title from filename
                title = url.rstrip("/").split("/")[-1].replace("-", " ").replace(".pdf", "")
            # Normalize URL
            if not url.startswith("http"):
                url = BASE_URL + url
            if url not in seen_urls:
                seen_urls.add(url)
                results.append((title, url))

        return results

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text via pdfplumber."""
        resp = self._request(pdf_url, timeout=120, stream=True)
        if resp is None:
            return ""

        cl = resp.headers.get("Content-Length")
        if cl and int(cl) > MAX_PDF_SIZE:
            logger.warning(f"PDF too large ({int(cl)} bytes): {pdf_url}")
            return ""

        pdf_bytes = resp.content
        if len(pdf_bytes) > MAX_PDF_SIZE:
            logger.warning(f"PDF too large ({len(pdf_bytes)} bytes): {pdf_url}")
            return ""

        try:
            pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
            parts = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    parts.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return "\n\n".join(parts).strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return ""

    def _generate_id(self, pdf_url: str) -> str:
        """Generate a stable ID from the PDF URL."""
        # Use the filename part of the URL
        slug = pdf_url.rstrip("/").split("/")[-1].replace(".pdf", "")
        # Hash to keep IDs reasonable length
        url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:8]
        return f"MS-law-{slug[:80]}-{url_hash}"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "MS/RevisedLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        count = 0
        seen_urls = set()

        for category, page_url in LISTING_PAGES:
            if max_records and count >= max_records:
                return

            logger.info(f"Fetching listing: {category}")
            links = self._extract_pdf_links(page_url)
            logger.info(f"  Found {len(links)} PDF links")

            for title, pdf_url in links:
                if max_records and count >= max_records:
                    return

                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                text = self._extract_pdf_text(pdf_url)
                if not text or len(text) < 100:
                    logger.warning(
                        f"Insufficient text ({len(text)} chars) from: {title}"
                    )
                    continue

                doc_id = self._generate_id(pdf_url)

                raw = {
                    "doc_id": doc_id,
                    "title": title,
                    "text": text,
                    "date": "",
                    "url": page_url,
                    "pdf_url": pdf_url,
                    "category": category,
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=20)

    def test(self) -> bool:
        links = self._extract_pdf_links(LISTING_PAGES[0][1])
        if not links:
            logger.error("Cannot fetch listing page")
            return False

        logger.info(f"Listing OK: {len(links)} PDF links on first page")

        title, pdf_url = links[1]  # Skip first (Travel Protocols)
        logger.info(f"Testing PDF: {title}")
        text = self._extract_pdf_text(pdf_url)
        logger.info(f"PDF text: {len(text)} chars")

        return len(text) > 100


def main():
    parser = argparse.ArgumentParser(description="MS/RevisedLaws data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = MSRevisedLawsScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('title', '?')[:80]} "
                f"({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
