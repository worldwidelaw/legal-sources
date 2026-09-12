#!/usr/bin/env python3
"""
MW/Judiciary -- Malawi Judiciary Judgments

Fetches court judgments from the official Malawi Judiciary website (judiciary.mw).

Strategy:
  - Paginate listing pages (24 pages, ~240 judgments)
  - Extract PDF links from each page
  - Download PDFs and extract full text via pdfplumber
  - Parse metadata from filenames and file paths

Data:
  - ~240 judgments (Supreme Court, High Court, subordinate courts)
  - Full text in English
  - License: Public Domain (Government Works)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote, urlparse

import pdfplumber
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MW.Judiciary")

BASE_URL = "https://www.judiciary.mw"
LISTING_URL = f"{BASE_URL}/judgements"
MAX_PAGES = 30  # Safety cap (actual ~24)


class MWJudiciaryScraper(BaseScraper):
    """
    Scraper for MW/Judiciary -- Malawi Judiciary Judgments.
    Country: MW
    URL: https://www.judiciary.mw/judgements

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with rate limiting and retry."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
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
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse PDF links from a listing page."""
        soup = BeautifulSoup(html, "html.parser")
        entries = []

        # Find all PDF file links (inside span.file--application-pdf)
        pdf_spans = soup.find_all("span", class_="file--application-pdf")
        for span in pdf_spans:
            link = span.find("a", href=True)
            if not link:
                continue
            href = link["href"]
            if not href.lower().endswith(".pdf"):
                continue

            # Make absolute URL
            if href.startswith("/"):
                href = BASE_URL + href

            # Extract filename as title
            filename = unquote(href.split("/")[-1])
            # Remove .pdf extension
            title = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
            # Clean underscores and common suffixes
            title = title.replace("_", " ")
            title = re.sub(r"_\d+$", "", title)  # Remove trailing _0, _1 etc.

            # Extract date from URL path (e.g., /2026-05/)
            date_match = re.search(r"/(\d{4}-\d{2})/", href)
            upload_date = date_match.group(1) + "-01" if date_match else None

            entries.append({
                "url": href,
                "title": title,
                "filename": filename,
                "upload_date": upload_date,
            })

        return entries

    def _get_all_entries(self) -> Generator[Dict[str, str], None, None]:
        """Paginate through all listing pages and yield entries."""
        page = 0
        seen_urls = set()

        while page < MAX_PAGES:
            url = f"{LISTING_URL}?page={page}"
            logger.info(f"Fetching listing page {page}...")

            resp = self._request(url)
            if not resp:
                logger.warning(f"Failed to fetch page {page}, stopping")
                break

            entries = self._parse_listing_page(resp.text)
            if not entries:
                logger.info(f"No entries on page {page}, stopping pagination")
                break

            new_count = 0
            for entry in entries:
                if entry["url"] not in seen_urls:
                    seen_urls.add(entry["url"])
                    yield entry
                    new_count += 1

            if new_count == 0:
                logger.info(f"All entries on page {page} already seen, stopping")
                break

            page += 1

        logger.info(f"Total unique PDF entries found: {len(seen_urls)}")

    def _extract_text_from_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text using pdfplumber."""
        try:
            logger.info(f"Downloading PDF: {unquote(pdf_url.split('/')[-1])[:60]}...")
            time.sleep(1.5)

            resp = self.session.get(pdf_url, timeout=180, stream=True)
            resp.raise_for_status()

            content = resp.content
            size_mb = len(content) / (1024 * 1024)
            if size_mb > 50:
                logger.warning(f"PDF too large ({size_mb:.1f} MB), skipping")
                return None

            text_parts = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                total_pages = len(pdf.pages)
                for i, page in enumerate(pdf.pages):
                    try:
                        page_text = page.extract_text()
                        if page_text:
                            text_parts.append(page_text)
                    except Exception as e:
                        logger.debug(f"Failed to extract page {i+1}/{total_pages}: {e}")
                        continue

            full_text = "\n\n".join(text_parts)
            if full_text.strip():
                logger.info(f"Extracted {len(full_text)} chars from {total_pages} pages")
                return full_text
            else:
                logger.warning(f"No text extracted (scanned PDF?)")
                return None

        except Exception as e:
            logger.error(f"Failed to extract PDF text: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgment documents with full text."""
        for entry in self._get_all_entries():
            text = self._extract_text_from_pdf(entry["url"])
            if text and len(text.strip()) >= 100:
                entry["text"] = text
                yield entry
            else:
                logger.warning(f"Skipping (no text): {entry['title'][:60]}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Incremental update — re-fetches all (small corpus)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        title = raw.get("title", "")
        text = raw.get("text", "")
        url = raw.get("url", "")

        if not text or len(text.strip()) < 100:
            return None

        # Generate stable ID from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        doc_id = f"MW/Judiciary/{url_hash}"

        return {
            "_id": doc_id,
            "_source": "MW/Judiciary",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": doc_id,
            "title": title,
            "text": text,
            "date": raw.get("upload_date"),
            "url": url,
            "filename": raw.get("filename", ""),
            "country": "MW",
            "language": "en",
        }


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="MW/Judiciary scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    args = parser.parse_args()

    scraper = MWJudiciaryScraper()

    if args.command == "test":
        logger.info("Testing connectivity to judiciary.mw...")
        try:
            resp = scraper._request(f"{LISTING_URL}?page=0")
            if resp:
                entries = scraper._parse_listing_page(resp.text)
                logger.info(f"Connection OK. Found {len(entries)} PDF entries on page 0.")
                for e in entries[:3]:
                    logger.info(f"  {e['title'][:70]}")
                print("TEST PASSED")
            else:
                print("TEST FAILED - no response")
                sys.exit(1)
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            print("TEST FAILED")
            sys.exit(1)

    elif args.command == "bootstrap":
        sample_mode = args.sample or not args.full
        sample_size = 15 if sample_mode else 99999
        logger.info(f"Starting bootstrap (sample={sample_mode}, size={sample_size})")
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
