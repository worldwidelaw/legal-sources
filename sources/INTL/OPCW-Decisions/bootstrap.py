#!/usr/bin/env python3
"""
INTL/OPCW-Decisions -- OPCW Conference & Executive Council Decisions

Fetches decision documents from the OPCW website.

Strategy:
  1. Scrape session listing pages to discover all CSP/EC session URLs
  2. On each session page, parse HTML tables for decision documents (DEC symbol)
  3. Download PDFs and extract full text using PyMuPDF

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from urllib.parse import urljoin

import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OPCW-Decisions")

BASE_URL = "https://www.opcw.org"
CSP_LISTING = f"{BASE_URL}/resourcesdocumentsconference-states-parties/conference-states-parties-documents"
EC_LISTING = f"{BASE_URL}/resources/documents/executive-council/executive-council-documents"
DELAY = 2.0


class OPCWDecisionsScraper(BaseScraper):
    SOURCE_ID = "INTL/OPCW-Decisions"

    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    # ------------------------------------------------------------------ #
    # Session discovery
    # ------------------------------------------------------------------ #

    def _discover_session_urls(self, listing_url: str, link_pattern: str) -> List[str]:
        """Scrape a listing page for session URLs matching a pattern."""
        try:
            r = self.session.get(listing_url, timeout=30)
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch listing %s: %s", listing_url, e)
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        urls = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            full_url = urljoin(BASE_URL, href)
            if re.search(link_pattern, full_url) and full_url not in seen:
                seen.add(full_url)
                urls.append(full_url)
        logger.info("Discovered %d session URLs from %s", len(urls), listing_url)
        return urls

    def _get_csp_session_urls(self) -> List[str]:
        """Get all CSP session page URLs."""
        # CSP URLs are inconsistent, so we combine listing scrape with known patterns
        urls = self._discover_session_urls(
            CSP_LISTING,
            r"opcw\.org/(resources/documents/conference-states-parties/|csp-\d)"
        )

        # Add known short-form URLs that might be missing from listing
        known_short = [f"{BASE_URL}/csp-{n}" for n in range(1, 23)]
        for url in known_short:
            if url not in urls:
                urls.append(url)

        # Also add review conferences and special sessions
        rc_urls = self._discover_session_urls(
            CSP_LISTING,
            r"opcw\.org/(resources/documents/conference-states-parties/.*review|rc-\d)"
        )
        ss_urls = self._discover_session_urls(
            CSP_LISTING,
            r"opcw\.org/(resources/documents/conference-states-parties/.*special|csp-ss)"
        )
        for url in rc_urls + ss_urls:
            if url not in urls:
                urls.append(url)

        return urls

    def _get_ec_session_urls(self) -> List[str]:
        """Get all EC session page URLs."""
        urls = self._discover_session_urls(
            EC_LISTING,
            r"opcw\.org/resources/documents/executive-council/ec-\d"
        )

        # Add EC sessions not on the listing page (EC-112 and beyond)
        for n in range(1, 115):
            url = f"{BASE_URL}/resources/documents/executive-council/ec-{n}"
            if url not in urls:
                urls.append(url)

        return urls

    # ------------------------------------------------------------------ #
    # Session page scraping
    # ------------------------------------------------------------------ #

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse a date string into ISO format."""
        if not date_str:
            return None
        date_str = date_str.strip()

        # Try common OPCW date formats
        for fmt in [
            "%d %B %Y",      # "25 November 2025"
            "%d %b %Y",      # "25 Nov 2025"
            "%B %d, %Y",     # "November 25, 2025"
            "%d/%m/%Y",      # "25/11/2025"
            "%Y-%m-%d",      # "2025-11-25"
        ]:
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _scrape_session_decisions(self, session_url: str) -> List[dict]:
        """Scrape a session page for decision documents."""
        try:
            r = self.session.get(session_url, timeout=30)
            if r.status_code == 404:
                return []
            r.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch session %s: %s", session_url, e)
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        decisions = []

        # Find all tables on the page
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue

                # First cell: document symbol, second: title (with PDF link), third: date
                symbol_text = cells[0].get_text(strip=True)

                # Only keep decision documents
                if "DEC" not in symbol_text.upper():
                    continue

                # Clean up symbol (remove non-breaking spaces, Unicode dashes)
                symbol = re.sub(r"[\u2011\u2012\u2013\u2014]", "-", symbol_text)
                symbol = re.sub(r"\s+", " ", symbol).strip()

                # Get title and PDF link
                title_cell = cells[1] if len(cells) >= 2 else cells[0]
                title = title_cell.get_text(strip=True)

                pdf_url = None
                pdf_link = title_cell.find("a", href=True)
                if pdf_link:
                    href = pdf_link["href"]
                    if ".pdf" in href.lower():
                        pdf_url = urljoin(BASE_URL, href)
                    else:
                        # Some links go to a document page, not directly to PDF
                        # Check all links in the row
                        for a in row.find_all("a", href=True):
                            if ".pdf" in a["href"].lower():
                                pdf_url = urljoin(BASE_URL, a["href"])
                                break

                # If no PDF in title cell, check first cell (symbol might be linked)
                if not pdf_url:
                    for a in cells[0].find_all("a", href=True):
                        if ".pdf" in a["href"].lower():
                            pdf_url = urljoin(BASE_URL, a["href"])
                            break

                # Get date
                date_str = cells[2].get_text(strip=True) if len(cells) >= 3 else None
                date = self._parse_date(date_str)

                if not title:
                    title = symbol

                decisions.append({
                    "doc_symbol": symbol,
                    "title": title,
                    "date": date,
                    "pdf_url": pdf_url,
                    "session_url": session_url,
                })

        if decisions:
            logger.info("Found %d decisions on %s", len(decisions), session_url)

        return decisions

    # ------------------------------------------------------------------ #
    # PDF text extraction
    # ------------------------------------------------------------------ #

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract full text using PyMuPDF."""
        try:
            r = self.session.get(url, timeout=120, stream=True)
            r.raise_for_status()

            content_length = r.headers.get("Content-Length")
            if content_length and int(content_length) > 50_000_000:
                logger.warning("PDF too large (%s bytes): %s", content_length, url)
                return None

            data = r.content
            if len(data) < 100:
                logger.warning("PDF too small (%d bytes): %s", len(data), url)
                return None

            doc = fitz.open(stream=data, filetype="pdf")
            text_parts = []
            for page in doc:
                t = page.get_text()
                if t:
                    text_parts.append(t)
            doc.close()

            text = "\n".join(text_parts).strip()
            if len(text) < 50:
                logger.warning("PDF extraction yielded only %d chars: %s", len(text), url)
                return None

            return text

        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", url, e)
            return None

    # ------------------------------------------------------------------ #
    # Normalization
    # ------------------------------------------------------------------ #

    def normalize(self, raw: dict) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = re.sub(r"[/\s]+", "-", raw["doc_symbol"])
        return {
            "_id": f"OPCW-{doc_id}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": now,
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url") or raw.get("session_url", ""),
            "doc_symbol": raw["doc_symbol"],
            "session_url": raw.get("session_url", ""),
        }

    # ------------------------------------------------------------------ #
    # Main fetch logic
    # ------------------------------------------------------------------ #

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all OPCW decisions with full text.

        Yields RAW document dicts (with a "text" field added) per the
        BaseScraper contract — the framework's bootstrap()/update() then
        call normalize() and stream each record to data/records.jsonl.
        Sample mode is signalled via the instance flag ``_sample_mode`` so
        the framework can call fetch_all() with no arguments.
        """
        sample = getattr(self, "_sample_mode", False)
        # Step 1: Discover session URLs
        logger.info("Discovering CSP session URLs...")
        csp_urls = self._get_csp_session_urls()
        time.sleep(DELAY)

        logger.info("Discovering EC session URLs...")
        ec_urls = self._get_ec_session_urls()
        time.sleep(DELAY)

        all_session_urls = csp_urls + ec_urls
        logger.info("Total session URLs to scrape: %d", len(all_session_urls))

        if sample:
            # For sample, just use a few recent sessions
            sample_urls = []
            # Pick last 2 CSP and last 2 EC sessions
            if csp_urls:
                sample_urls.extend(csp_urls[:2])
            if ec_urls:
                sample_urls.extend(ec_urls[:2])
            all_session_urls = sample_urls
            logger.info("Sample mode: using %d session URLs", len(all_session_urls))

        # Step 2: Scrape all session pages for decisions
        all_decisions = []
        seen_symbols = set()

        for i, url in enumerate(all_session_urls):
            time.sleep(DELAY)
            decisions = self._scrape_session_decisions(url)
            for d in decisions:
                sym = d["doc_symbol"].upper()
                if sym not in seen_symbols:
                    seen_symbols.add(sym)
                    all_decisions.append(d)
            if (i + 1) % 20 == 0:
                logger.info("Scraped %d/%d sessions, %d unique decisions so far",
                            i + 1, len(all_session_urls), len(all_decisions))

        logger.info("Total unique decisions found: %d", len(all_decisions))

        if sample:
            all_decisions = all_decisions[:15]

        # Step 3: Download PDFs and extract text
        count = 0
        skipped = 0

        for d in all_decisions:
            if not d.get("pdf_url"):
                logger.warning("No PDF URL for %s", d["doc_symbol"])
                skipped += 1
                continue

            time.sleep(DELAY)
            text = self._extract_pdf_text(d["pdf_url"])
            if not text:
                skipped += 1
                continue

            d["text"] = text
            count += 1
            # Yield RAW; BaseScraper.bootstrap() normalizes + writes to storage.
            yield d

        logger.info("Done: %d records yielded, %d skipped, %d total decisions",
                     count, skipped, len(all_decisions))

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch recent decisions."""
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            r = self.session.get(CSP_LISTING, timeout=15)
            return r.status_code == 200
        except Exception:
            return False


# ---------------------------------------------------------------------- #
# CLI entry point
# ---------------------------------------------------------------------- #

def main():
    scraper = OPCWDecisionsScraper()
    args = sys.argv[1:]

    if not args or args[0] == "test":
        ok = scraper.test_connection()
        print(f"Connection test: {'OK' if ok else 'FAILED'}")
        sys.exit(0 if ok else 1)

    sample = "--sample" in args
    command = args[0]

    # All fetch paths delegate to BaseScraper.bootstrap(), which normalizes,
    # validates and streams every record to data/records.jsonl (the file the
    # fleet ingests). In sample mode it stops early and also writes sample/.
    if command in ("bootstrap", "bootstrap-fast", "update"):
        scraper._sample_mode = sample
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)


if __name__ == "__main__":
    main()
