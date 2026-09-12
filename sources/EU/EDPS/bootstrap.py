#!/usr/bin/env python3
"""
EU/EDPS -- European Data Protection Supervisor Opinions Fetcher

Fetches EDPS Opinions on EU legislative proposals, with full text extracted
from the published PDFs. EDPS Opinions are the Supervisor's official advice to
the EU legislator on the data-protection implications of proposed regulations
and directives -- a core data-protection doctrine corpus.

Strategy:
  - Scrape the paginated HTML "Opinions" listing (~80 pages, 5 opinions/page)
  - Extract title, detail-page URL and date (from the URL slug) per opinion
  - Fetch each detail page to locate its English PDF under /system/files/
  - Download the PDF and extract text via the shared pdf_extract backend
  - Normalize into the standard schema (type: doctrine)

Usage:
  python bootstrap.py bootstrap          # Fetch all opinions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap (VPS runner)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
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

from common.base_scraper import BaseScraper, as_date_str
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.EDPS")

BASE_URL = "https://www.edps.europa.eu"
LISTING_PATH = "/data-protection/our-work/our-work-by-type/opinions_en"
LISTING_URL = f"{BASE_URL}{LISTING_PATH}"
MAX_PAGES = 90  # safety cap; pagination stops when a page yields no rows

SLUG_DATE_RE = re.compile(r"/(\d{4}-\d{2}-\d{2})-")


class EDPSScraper(BaseScraper):
    """Scraper for EU/EDPS -- European Data Protection Supervisor opinions."""

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
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 15s")
                    time.sleep(15)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF using the centralized extractor."""
        return extract_pdf_markdown(
            source="EU/EDPS",
            source_id="",
            pdf_bytes=pdf_content,
            table="doctrine",
            force=True,
        ) or ""

    def _parse_listing_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse one listing page, returning opinion metadata (no PDF yet)."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []
        seen = set()

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if "/publications/opinions/" not in href:
                continue
            # Normalize to absolute URL and dedupe (page lists each item once,
            # but defensive against repeated anchors).
            path = href if href.startswith("/") else "/" + href.split(BASE_URL)[-1]
            if path in seen:
                continue

            title = link.get_text(strip=True)
            if not title:
                continue
            seen.add(path)

            doc_url = href if href.startswith("http") else BASE_URL + href
            m = SLUG_DATE_RE.search(path)
            date_iso = m.group(1) if m else ""

            slug = path.rstrip("/").split("/")[-1]
            slug = re.sub(r"_en$", "", slug)
            doc_id = f"EDPS-{slug}"

            documents.append({
                "document_id": doc_id,
                "title": title,
                "date": date_iso,
                "url": doc_url,
            })

        return documents

    def _find_pdf_url(self, detail_html: str) -> str:
        """Locate the English PDF attachment on an opinion detail page."""
        soup = BeautifulSoup(detail_html, "html.parser")
        candidates = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if href.lower().endswith(".pdf"):
                candidates.append(href)
        if not candidates:
            return ""
        # Prefer an English ("_en") PDF if multiple languages are attached.
        english = [c for c in candidates if "_en" in c.lower()]
        chosen = (english or candidates)[0]
        return chosen if chosen.startswith("http") else BASE_URL + chosen

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("document_id", ""),
            "_source": "EU/EDPS",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "publication_type": "Opinion",
        }

    def _iter_listing(self, max_pages: int) -> Generator[Dict[str, Any], None, None]:
        for page_num in range(max_pages):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch listing page {page_num}")
                continue
            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No opinions on page {page_num}, stopping pagination")
                break
            logger.info(f"Page {page_num}: {len(docs)} opinions")
            for doc in docs:
                yield doc

    def _hydrate(self, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch detail page, locate PDF, download and extract full text."""
        detail = self._request(doc["url"])
        if detail is None:
            logger.warning(f"Failed to fetch detail: {doc['title'][:60]}")
            return None
        pdf_url = self._find_pdf_url(detail.text)
        if not pdf_url:
            logger.warning(f"No PDF found for: {doc['title'][:60]}")
            return None
        pdf_resp = self._request(pdf_url, timeout=120)
        if pdf_resp is None:
            logger.warning(f"Failed to download PDF: {pdf_url[:80]}")
            return None
        text = self._extract_text_from_pdf(pdf_resp.content)
        if not text or len(text) < 200:
            logger.warning(f"Insufficient text: {doc['title'][:60]} ({len(text)} chars)")
            return None
        doc["pdf_url"] = pdf_url
        doc["text"] = text
        return doc

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        count = 0
        for doc in self._iter_listing(MAX_PAGES):
            hydrated = self._hydrate(doc)
            if hydrated is None:
                continue
            count += 1
            yield hydrated
        logger.info(f"Completed: {count} opinions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        count = 0
        for doc in self._iter_listing(max_pages=4):
            if since and doc.get("date", "") < since:
                return
            hydrated = self._hydrate(doc)
            if hydrated is None:
                continue
            count += 1
            yield hydrated
        logger.info(f"Updates: {count} opinions fetched")

    def test(self) -> bool:
        resp = self._request(f"{LISTING_URL}?page=0")
        if resp is None:
            logger.error("Cannot reach EDPS opinions listing")
            return False
        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No opinions parsed from listing page")
            return False
        logger.info(f"Listing OK: {len(docs)} opinions on page 0")
        hydrated = self._hydrate(docs[0])
        if hydrated:
            logger.info(f"PDF OK: {hydrated['title'][:60]} ({len(hydrated['text'])} chars)")
            return True
        logger.error("Could not hydrate first opinion")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="EU/EDPS data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch a small sample")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = EDPSScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)
    elif args.command in ("bootstrap", "bootstrap-fast"):
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
