#!/usr/bin/env python3
"""
INTL/EBRDLegalTransition -- EBRD Law in Transition Journal Fetcher

Fetches legal reform articles from the EBRD Law in Transition journal.

Strategy:
  - Scrape the hub page to discover year-edition pages
  - For each year page, extract article titles and PDF links
  - Download individual article PDFs and extract text with pdfplumber
  - Normalize into standard schema

Data Coverage:
  - Journal editions from 2011 to present
  - ~10-14 articles per year, 150+ total articles
  - Topics: legal reform, corporate governance, dispute resolution,
    energy/climate, financial law, infrastructure, public contracts
  - Language: English
  - Coverage: 38 EBRD countries of operations

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from html.parser import HTMLParser
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

# PDF extraction
try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    print("WARNING: pdfplumber not available. Install with: pip install pdfplumber")

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.EBRDLegalTransition")

BASE_URL = "https://www.ebrd.com"
HUB_PATH = "/home/what-we-do/policy-and-business-advice/legal-reform/law-in-transition-journal.html"

# Titles to skip (non-substantive content)
SKIP_TITLES = {"glossary", "foreword", "editor's message", "editors message", "editor message"}


class LinkExtractor(HTMLParser):
    """Extract links and their surrounding text from HTML."""

    def __init__(self):
        super().__init__()
        self.links = []  # list of (href, text)
        self._current_href = None
        self._current_text = []
        self._in_a = False

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            attrs_dict = dict(attrs)
            href = attrs_dict.get("href", "")
            self._current_href = href
            self._current_text = []
            self._in_a = True

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            text = " ".join(self._current_text).strip()
            if self._current_href:
                self.links.append((self._current_href, text))
            self._in_a = False
            self._current_href = None

    def handle_data(self, data):
        if self._in_a:
            self._current_text.append(data.strip())


class PDFLinkExtractor(HTMLParser):
    """Simple extractor that finds all PDF links on a page."""

    def __init__(self):
        super().__init__()
        self.pdf_links = []  # list of href strings

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            href = dict(attrs).get("href", "")
            if href.endswith(".pdf"):
                self.pdf_links.append(href)


def _title_from_filename(pdf_url: str) -> str:
    """Derive a human-readable title from a PDF filename."""
    fname = pdf_url.split("/")[-1]
    # Remove .pdf extension
    fname = fname.replace(".pdf", "")
    # Remove common prefixes like "ebrd-lit25-", "law-in-transition-2024-"
    fname = re.sub(r"^ebrd-lit\d{2}-", "", fname)
    fname = re.sub(r"^law-in-transition-\d{4}-?", "", fname)
    # URL decode
    from urllib.parse import unquote
    fname = unquote(fname)
    # Replace separators with spaces
    fname = fname.replace("-", " ").replace("_", " ")
    # Title case
    title = fname.strip().title()
    return title if title else fname


class EBRDLegalTransitionScraper(BaseScraper):
    """Scraper for EBRD Law in Transition journal articles."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)"
        })

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page, return text or None on failure."""
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text using pdfplumber."""
        if not PDF_SUPPORT:
            logger.error("pdfplumber not available")
            return None
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning(f"PDF too small ({len(pdf_bytes)} bytes): {url}")
                return None
            text_parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
            text = "\n\n".join(text_parts).strip()
            if len(text) < 50:
                logger.warning(f"Very little text extracted from {url}: {len(text)} chars")
                return None
            return text
        except Exception as e:
            logger.warning(f"Failed to extract text from {url}: {e}")
            return None

    def _get_year_pages(self) -> List[Tuple[str, str]]:
        """Fetch the hub page and return list of (year_label, full_url) for each edition."""
        hub_url = BASE_URL + HUB_PATH
        html = self._fetch_page(hub_url)
        if not html:
            logger.error("Cannot fetch hub page")
            return []

        parser = LinkExtractor()
        parser.feed(html)

        year_pages = []
        seen = set()
        for href, text in parser.links:
            # Match year-edition links
            if "law-in-transition" in href and href.endswith(".html"):
                # Skip links that go back to the hub itself
                if href.rstrip("/") == HUB_PATH.rstrip("/"):
                    continue
                # Build full URL
                if href.startswith("/"):
                    full_url = BASE_URL + href
                elif href.startswith("http"):
                    full_url = href
                else:
                    full_url = urljoin(hub_url, href)
                # Ensure we use full AEM paths (not short redirecting ones)
                if "/home/what-we-do/" not in full_url and full_url.startswith(BASE_URL):
                    # Prepend the full path prefix
                    path = full_url.replace(BASE_URL, "")
                    full_url = BASE_URL + "/home/what-we-do/policy-and-business-advice/legal-reform/law-in-transition-journal/" + path.split("/")[-1]

                if full_url not in seen:
                    seen.add(full_url)
                    # Extract year from URL or text
                    year_match = re.search(r"20\d{2}", href)
                    label = year_match.group(0) if year_match else text[:30]
                    year_pages.append((label, full_url))

        logger.info(f"Found {len(year_pages)} edition pages on hub")
        return year_pages

    def _get_articles_from_year(self, year_url: str) -> List[Tuple[str, str]]:
        """Fetch a year page and extract (title, pdf_url) pairs."""
        html = self._fetch_page(year_url)
        if not html:
            return []

        parser = PDFLinkExtractor()
        parser.feed(html)

        # Build full PDF URLs and derive titles from filenames
        seen = set()
        result = []
        for pdf_href in parser.pdf_links:
            if pdf_href in seen:
                continue
            seen.add(pdf_href)

            if pdf_href.startswith("/"):
                pdf_url = BASE_URL + pdf_href
            elif pdf_href.startswith("http"):
                pdf_url = pdf_href
            else:
                pdf_url = urljoin(year_url, pdf_href)

            title = _title_from_filename(pdf_url)
            result.append((title, pdf_url))

        return result

    def _is_substantive(self, title: str, pdf_url: str) -> bool:
        """Check if an article is substantive (not a glossary, foreword, etc.)."""
        title_lower = title.lower().strip()
        fname = pdf_url.split("/")[-1].lower()

        # Skip non-substantive items
        for skip in SKIP_TITLES:
            if title_lower == skip or title_lower.startswith(skip):
                return False
        # Also check filename for foreword/glossary/editor
        for skip in ("foreword", "glossary", "editor"):
            if skip in fname:
                return False

        # Skip full journal compilations (we want individual articles)
        # These are named like "law-in-transition-2024.pdf", "law-in-transition-2024-english.pdf",
        # "ebrd-law-in-transition-2025.pdf"
        if re.match(r"^(ebrd-)?law-in-transition-?\d{4}(-english)?\.pdf$", fname):
            return False

        return True

    def _extract_year_from_url(self, url: str) -> Optional[str]:
        """Extract the most relevant year from a URL.

        Prefer years in filenames (last path component) over directory paths.
        """
        fname = url.split("/")[-1]
        match = re.search(r"20\d{2}", fname)
        if match:
            return match.group(0)
        # Check for encoded year like "LiTJ%202025"
        from urllib.parse import unquote
        decoded = unquote(url)
        # Find all years and return the last one (most specific)
        years = re.findall(r"20\d{2}", decoded)
        return years[-1] if years else None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all articles from all journal editions."""
        year_pages = self._get_year_pages()
        if not year_pages:
            logger.error("No year pages found")
            return

        for year_label, year_url in year_pages:
            logger.info(f"Processing edition: {year_label} ({year_url})")
            time.sleep(1)

            articles = self._get_articles_from_year(year_url)
            logger.info(f"  Found {len(articles)} PDFs in {year_label}")

            for title, pdf_url in articles:
                if not self._is_substantive(title, pdf_url):
                    logger.debug(f"  Skipping non-substantive: {title}")
                    continue

                logger.info(f"  Fetching: {title[:60]}...")
                time.sleep(1.5)

                text = self._fetch_pdf_text(pdf_url)
                if not text:
                    logger.warning(f"  No text extracted for: {title}")
                    continue

                year = self._extract_year_from_url(pdf_url) or year_label

                yield {
                    "title": title,
                    "text": text,
                    "pdf_url": pdf_url,
                    "year_page_url": year_url,
                    "year": year,
                    "year_label": year_label,
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch only recent editions (current and previous year)."""
        current_year = datetime.now().year
        year_pages = self._get_year_pages()

        for year_label, year_url in year_pages:
            year_match = re.search(r"20\d{2}", year_url)
            if year_match:
                page_year = int(year_match.group(0))
                if page_year < current_year - 1:
                    continue

            logger.info(f"Checking updates in {year_label}")
            time.sleep(1)

            articles = self._get_articles_from_year(year_url)
            for title, pdf_url in articles:
                if not self._is_substantive(title, pdf_url):
                    continue
                time.sleep(1.5)
                text = self._fetch_pdf_text(pdf_url)
                if not text:
                    continue
                year = self._extract_year_from_url(pdf_url) or year_label
                yield {
                    "title": title,
                    "text": text,
                    "pdf_url": pdf_url,
                    "year_page_url": year_url,
                    "year": year,
                    "year_label": year_label,
                }

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw article data into standard schema."""
        title = raw.get("title", "").strip()
        text = raw.get("text", "").strip()

        if not text or len(text) < 100:
            return None

        # Generate a stable ID from the PDF URL
        pdf_url = raw.get("pdf_url", "")
        fname = pdf_url.split("/")[-1].replace(".pdf", "") if pdf_url else title[:50]
        doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", fname)

        year = raw.get("year", "") or raw.get("year_label", "")
        # Use Jan 1 of the year as the date (journal publications are annual)
        date_str = f"{year}-01-01" if year and year.isdigit() else None

        return {
            "_id": f"INTL_EBRD_{doc_id}",
            "_source": "INTL/EBRDLegalTransition",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": pdf_url,
            "year": year,
            "year_page_url": raw.get("year_page_url", ""),
            "publisher": "European Bank for Reconstruction and Development (EBRD)",
            "publication": "Law in Transition Journal",
            "language": "EN",
        }


# ── CLI entry point ────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="EBRD Legal Transition bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Sample mode: fetch only 10+ records")
    args = parser.parse_args()

    scraper = EBRDLegalTransitionScraper()

    if args.command == "test":
        print("Testing connectivity...")
        year_pages = scraper._get_year_pages()
        if year_pages:
            print(f"OK: Found {len(year_pages)} edition pages")
            # Test first year page
            label, url = year_pages[0]
            articles = scraper._get_articles_from_year(url)
            print(f"OK: {label} has {len(articles)} PDFs")
            if articles:
                title, pdf_url = articles[0]
                print(f"  First article: {title[:60]}")
                print(f"  PDF URL: {pdf_url}")
        else:
            print("FAIL: No edition pages found")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
