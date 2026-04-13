#!/usr/bin/env python3
"""
INTL/ItalawArbitration -- Italaw Investment Treaty Arbitration

Fetches ISDS case law from italaw.com (Drupal 7 site).

Strategy:
  - Parse sitemap.xml to get all case URLs
  - Scrape each case page for metadata + PDF links
  - Download PDFs and extract text via PyMuPDF
  - Respect 10-second crawl delay per robots.txt

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional
from xml.etree import ElementTree

import requests
import fitz  # PyMuPDF

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ItalawArbitration")

BASE_URL = "https://www.italaw.com"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
CRAWL_DELAY = 10  # robots.txt


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="INTL/ItalawArbitration",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

class ItalawArbitrationScraper(BaseScraper):
    """
    Scraper for INTL/ItalawArbitration -- Italaw Investment Treaty Arbitration.
    Country: INTL
    URL: https://www.italaw.com/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_case_urls_from_sitemap(self) -> list:
        """Parse sitemap to get all case page URLs."""
        logger.info("Fetching sitemap index...")
        resp = self.session.get(SITEMAP_URL, timeout=30)
        resp.raise_for_status()

        # Parse sitemap index to find sitemap pages
        root = ElementTree.fromstring(resp.content)
        ns = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        sitemap_urls = []
        # Check if this is a sitemap index
        for sitemap in root.findall('sm:sitemap', ns):
            loc = sitemap.find('sm:loc', ns)
            if loc is not None:
                sitemap_urls.append(loc.text)

        # If no sub-sitemaps, treat as direct sitemap
        if not sitemap_urls:
            sitemap_urls = [SITEMAP_URL]

        case_urls = []
        for sm_url in sitemap_urls:
            if sm_url != SITEMAP_URL:
                time.sleep(2)
                resp = self.session.get(sm_url, timeout=30)
                resp.raise_for_status()
                root = ElementTree.fromstring(resp.content)

            for url_elem in root.findall('sm:url', ns):
                loc = url_elem.find('sm:loc', ns)
                if loc is not None:
                    url = loc.text
                    # Only case pages, not document pages
                    if re.match(r'https?://(?:www\.)?italaw\.com/cases/\d+$', url):
                        case_urls.append(url)

        logger.info("Found %d case URLs in sitemap", len(case_urls))
        return case_urls

    def _scrape_case_page(self, case_url: str) -> list:
        """Scrape a case page for metadata and document info."""
        time.sleep(CRAWL_DELAY)
        try:
            resp = self.session.get(case_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", case_url, e)
            return []

        page_html = resp.text
        documents = []

        # Extract case name from title
        title_match = re.search(r'<title>([^<]+)</title>', page_html)
        case_name = strip_html(title_match.group(1)).replace(' | italaw', '').strip() if title_match else ""

        # Extract respondent state
        respondent_match = re.search(
            r'field-respondent-state.*?<a[^>]*>([^<]+)</a>',
            page_html, re.DOTALL
        )
        respondent = respondent_match.group(1).strip() if respondent_match else ""

        # Extract case type
        case_type_match = re.search(
            r'field-case-type.*?<a[^>]*>([^<]+)</a>',
            page_html, re.DOTALL
        )
        case_type = case_type_match.group(1).strip() if case_type_match else ""

        # Extract documents - look for document rows with date, title, PDF link
        # Pattern: date + title + PDF link in table/list structure
        doc_pattern = re.compile(
            r'<span[^>]*class="date-display-single"[^>]*>([^<]+)</span>'
            r'.*?'
            r'<a\s+href="(/cases/documents/\d+)"[^>]*>([^<]+)</a>'
            r'(?:.*?<a\s+href="(/sites/default/files/case-documents/[^"]+\.pdf)")?',
            re.DOTALL
        )

        for match in doc_pattern.finditer(page_html):
            date_str = match.group(1).strip()
            doc_page_url = match.group(2)
            doc_title = strip_html(match.group(3))
            pdf_path = match.group(4) if match.group(4) else None

            # Parse date
            parsed_date = None
            for fmt in ("%B %d, %Y", "%d %B %Y", "%Y-%m-%d", "%m/%d/%Y"):
                try:
                    parsed_date = datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

            doc = {
                "case_name": case_name,
                "case_url": case_url,
                "respondent_state": respondent,
                "case_type": case_type,
                "doc_title": doc_title,
                "doc_page_url": f"{BASE_URL}{doc_page_url}",
                "date": parsed_date or date_str,
                "pdf_url": f"{BASE_URL}{pdf_path}" if pdf_path else None,
            }
            documents.append(doc)

        # Alternative: try simpler PDF link extraction if above didn't find docs
        if not documents:
            pdf_links = re.findall(
                r'href="(/sites/default/files/case-documents/[^"]+\.pdf)"',
                page_html
            )
            # Get dates from the page
            dates = re.findall(
                r'<span[^>]*class="date-display-single"[^>]*>([^<]+)</span>',
                page_html
            )
            doc_titles = re.findall(
                r'<a\s+href="/cases/documents/\d+"[^>]*>([^<]+)</a>',
                page_html
            )

            for i, pdf_path in enumerate(pdf_links):
                date_str = dates[i] if i < len(dates) else ""
                doc_title = strip_html(doc_titles[i]) if i < len(doc_titles) else f"Document {i+1}"

                parsed_date = None
                for fmt in ("%B %d, %Y", "%d %B %Y", "%Y-%m-%d"):
                    try:
                        parsed_date = datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue

                documents.append({
                    "case_name": case_name,
                    "case_url": case_url,
                    "respondent_state": respondent,
                    "case_type": case_type,
                    "doc_title": doc_title,
                    "date": parsed_date or date_str,
                    "pdf_url": f"{BASE_URL}{pdf_path}",
                    "doc_page_url": case_url,
                })

        return documents

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Download a PDF and extract its text."""
        if not pdf_url:
            return ""
        time.sleep(CRAWL_DELAY)
        try:
            resp = self.session.get(pdf_url, timeout=60)
            resp.raise_for_status()
            if 'application/pdf' not in resp.headers.get('Content-Type', ''):
                logger.warning("Not a PDF: %s (type: %s)", pdf_url,
                             resp.headers.get('Content-Type'))
                return ""
            return extract_pdf_text(resp.content)
        except Exception as e:
            logger.warning("Failed to download PDF %s: %s", pdf_url, e)
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all case documents with full text from PDFs."""
        case_urls = self._get_case_urls_from_sitemap()
        total_docs = 0

        for i, case_url in enumerate(case_urls):
            docs = self._scrape_case_page(case_url)
            for doc in docs:
                if doc.get("pdf_url"):
                    doc["text"] = self._download_pdf_text(doc["pdf_url"])
                    if doc["text"]:
                        total_docs += 1
                        yield doc

            if (i + 1) % 10 == 0:
                logger.info("Processed %d / %d cases, %d docs with text",
                           i + 1, len(case_urls), total_docs)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently posted documents via RSS."""
        rss_url = f"{BASE_URL}/rss/newly-posted.xml"
        resp = self.session.get(rss_url, timeout=30)
        resp.raise_for_status()

        root = ElementTree.fromstring(resp.content)
        for item in root.findall('.//item'):
            link_elem = item.find('link')
            title_elem = item.find('title')
            if link_elem is not None:
                case_url = link_elem.text
                if '/cases/' in case_url:
                    docs = self._scrape_case_page(case_url)
                    for doc in docs:
                        if doc.get("pdf_url"):
                            doc["text"] = self._download_pdf_text(doc["pdf_url"])
                            if doc["text"]:
                                yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        pdf_url = raw.get("pdf_url", "")
        doc_id = re.search(r'italaw(\d+)', pdf_url)
        doc_id_str = doc_id.group(1) if doc_id else str(hash(pdf_url))[-8:]

        return {
            "_id": f"italaw-{doc_id_str}",
            "_source": "INTL/ItalawArbitration",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("doc_title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("pdf_url", raw.get("case_url", "")),
            "case_name": raw.get("case_name", ""),
            "respondent_state": raw.get("respondent_state", ""),
            "case_type": raw.get("case_type", ""),
            "case_url": raw.get("case_url", ""),
            "pdf_url": pdf_url,
        }


# ── CLI ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ItalawArbitrationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing italaw.com connectivity...")
        resp = requests.get(f"{BASE_URL}/cases/35", timeout=30,
                          headers={"User-Agent": "Mozilla/5.0"})
        print(f"  Status: {resp.status_code}")
        # Check for a PDF link
        pdfs = re.findall(r'/sites/default/files/case-documents/[^"]+\.pdf', resp.text)
        print(f"  PDFs on page: {len(pdfs)}")
        if pdfs:
            pdf_url = f"{BASE_URL}{pdfs[0]}"
            print(f"  First PDF: {pdf_url}")
            time.sleep(CRAWL_DELAY)
            pdf_resp = requests.get(pdf_url, timeout=60,
                                  headers={"User-Agent": "Mozilla/5.0"})
            text = extract_pdf_text(pdf_resp.content)
            print(f"  Text: {len(text)} chars")
            if text:
                print(f"  Preview: {text[:200]}...")
        print("OK")

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=90)
        result = scraper.update(since=since)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
