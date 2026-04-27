#!/usr/bin/env python3
"""
NZ/IRD-TaxRulings -- New Zealand Inland Revenue Tax Technical Publications

Fetches tax rulings, interpretation statements, commissioner's statements,
operational statements, revenue alerts, case summaries, and other doctrine
from NZ IRD's Tax Technical site.

Strategy:
  - Parse the sitemap.xml to discover all document URLs
  - Filter to document pages (interpretation-statements, commissioner-s-statements, etc.)
  - For each page, extract metadata and find the PDF download link
  - Download and extract full text from PDFs via common/pdf_extract
  - For pages without PDFs, extract HTML body text directly

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_lib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin
import xml.etree.ElementTree as ET

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
logger = logging.getLogger("legal-data-hunter.NZ.IRD-TaxRulings")

BASE_URL = "https://www.taxtechnical.ird.govt.nz"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"

# Document URL path prefixes that contain actual publications
DOC_PREFIXES = [
    "/interpretation-statements/",
    "/commissioner-s-statements/",
    "/operational-statements/",
    "/operational-positions/",
    "/revenue-alerts/",
    "/case-summaries/",
    "/interpretation-guidelines/",
    "/issues-papers/",
    "/overviews/",
    "/fact-sheets/",
    "/general-articles/",
    "/rulings/",
    "/determinations/",
    "/questions-we-ve-been-asked/",
    "/new-legislation/",
]

# Category mapping from URL path to readable category
CATEGORY_MAP = {
    "interpretation-statements": "Interpretation Statement",
    "commissioner-s-statements": "Commissioner's Statement",
    "operational-statements": "Operational Statement",
    "operational-positions": "Operational Position",
    "revenue-alerts": "Revenue Alert",
    "case-summaries": "Case Summary",
    "interpretation-guidelines": "Interpretation Guideline",
    "issues-papers": "Issues Paper",
    "overviews": "Overview",
    "fact-sheets": "Fact Sheet",
    "general-articles": "General Article",
    "rulings": "Ruling",
    "determinations": "Determination",
    "questions-we-ve-been-asked": "Questions We've Been Asked",
    "new-legislation": "New Legislation",
}


class IRDTaxRulingsScraper(BaseScraper):
    """Scraper for NZ/IRD-TaxRulings -- NZ Inland Revenue Tax Technical Publications."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-NZ,en;q=0.9",
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
                    logger.debug(f"404: {url}")
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _get_sitemap_urls(self) -> List[str]:
        """Parse sitemap.xml and return document URLs."""
        resp = self._request(SITEMAP_URL)
        if resp is None:
            logger.error("Failed to fetch sitemap")
            return []

        try:
            root = ET.fromstring(resp.content)
        except ET.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")
            return []

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = []
        for url_elem in root.findall(".//sm:url/sm:loc", ns):
            url = url_elem.text.strip() if url_elem.text else ""
            if not url:
                continue
            path = url.replace(BASE_URL, "")
            if any(path.startswith(prefix) for prefix in DOC_PREFIXES):
                urls.append(url)

        logger.info(f"Found {len(urls)} document URLs in sitemap")
        return urls

    def _get_category(self, url: str) -> str:
        """Determine document category from URL path."""
        path = url.replace(BASE_URL, "")
        for key, label in CATEGORY_MAP.items():
            if f"/{key}/" in path:
                return label
        return "Other"

    def _get_reference(self, url: str, title: str) -> str:
        """Extract reference number from URL slug or title."""
        path = url.replace(BASE_URL, "")
        slug = path.rstrip("/").split("/")[-1]
        # Try common patterns like "is-24-08", "cs-20-01", "br-prd-24-01"
        ref_match = re.match(r'^([a-z]+-(?:prd-)?[\d]+-[\d]+)', slug)
        if ref_match:
            return ref_match.group(1).upper()
        # Try title patterns like "IS 24/08"
        title_match = re.search(r'\b([A-Z]{2,4}\s+\d{2,4}/?\d{1,4})\b', title)
        if title_match:
            return title_match.group(1)
        return slug

    def _parse_document_page(self, url: str, html_content: str) -> Optional[Dict[str, Any]]:
        """Parse a document page to extract metadata and content."""
        soup = BeautifulSoup(html_content, "html.parser")

        # Title from h1 or og:title
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                title = og_title.get("content", "")
        if not title:
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True)

        if not title:
            return None

        # Date - look for "Issued:" or date patterns on page
        date = ""
        page_text = soup.get_text()
        # Pattern: "Issued: DD Mon YYYY" or "DD Month YYYY"
        date_patterns = [
            r'(?:Issued|Published|Date)[:\s]*(\d{1,2}\s+\w+\s+\d{4})',
            r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
        ]
        for pat in date_patterns:
            dm = re.search(pat, page_text)
            if dm:
                try:
                    date_str = dm.group(1).strip()
                    dt = datetime.strptime(date_str, "%d %B %Y")
                    date = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    try:
                        dt = datetime.strptime(date_str, "%d %b %Y")
                        date = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        continue

        # Find PDF download link
        pdf_url = ""
        pdf_links = soup.find_all("a", href=lambda h: h and (".pdf" in h.lower() or "/media/" in h.lower()))
        for link in pdf_links:
            href = link.get("href", "")
            link_text = link.get_text(strip=True).lower()
            # Skip navigation/irrelevant PDFs
            if href and ("pdf" in href.lower() or "media" in href.lower()):
                pdf_url = urljoin(url, href)
                break

        # Extract HTML body text as fallback
        body_text = ""
        main_content = soup.find("main") or soup.find("article") or soup.find("div", class_="content")
        if main_content:
            # Remove nav, header, footer, script, style elements
            for tag in main_content.find_all(["nav", "header", "footer", "script", "style", "noscript"]):
                tag.decompose()
            body_text = main_content.get_text(separator="\n", strip=True)
            # Clean up excessive whitespace
            body_text = re.sub(r'\n{3,}', '\n\n', body_text)

        category = self._get_category(url)
        reference = self._get_reference(url, title)

        return {
            "title": title,
            "date": date,
            "url": url,
            "pdf_url": pdf_url,
            "category": category,
            "reference": reference,
            "body_text": body_text,
        }

    def _download_pdf_text(self, pdf_url: str, doc_id: str) -> str:
        """Download a PDF and extract text."""
        resp = self._request(pdf_url, timeout=120)
        if resp is None:
            return ""
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not pdf_url.lower().endswith(".pdf"):
            if "html" in content_type.lower():
                logger.debug(f"HTML response instead of PDF: {pdf_url}")
                return ""
        text = extract_pdf_markdown(
            source="NZ/IRD-TaxRulings",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="doctrine",
        ) or ""
        return text.strip()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        url = raw.get("url", "")
        ref = raw.get("reference", "")
        doc_id = f"NZ-IRD-{re.sub(r'[^a-zA-Z0-9_-]', '_', ref)}"

        return {
            "_id": doc_id,
            "_source": "NZ/IRD-TaxRulings",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": url,
            "category": raw.get("category", ""),
            "reference": ref,
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all tax technical publications."""
        urls = self._get_sitemap_urls()
        if not urls:
            logger.error("No document URLs found")
            return

        count = 0
        skipped = 0

        for i, url in enumerate(urls):
            logger.info(f"[{i+1}/{len(urls)}] Processing: {url}")

            resp = self._request(url)
            if resp is None:
                skipped += 1
                continue

            doc = self._parse_document_page(url, resp.text)
            if doc is None:
                skipped += 1
                continue

            # Try PDF first for full text
            text = ""
            if doc["pdf_url"]:
                text = self._download_pdf_text(doc["pdf_url"], doc["reference"])

            # Fall back to HTML body text
            if not text or len(text) < 100:
                text = doc.get("body_text", "")

            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for {url}: {len(text)} chars")
                skipped += 1
                continue

            doc["text"] = text
            count += 1
            yield doc

        logger.info(f"Completed: {count} documents fetched, {skipped} skipped")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates (same as fetch_all for this source)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        # Test sitemap
        urls = self._get_sitemap_urls()
        if not urls:
            logger.error("Failed to get sitemap URLs")
            return False
        logger.info(f"Sitemap OK: {len(urls)} document URLs")

        # Test first document page
        resp = self._request(urls[0])
        if resp is None:
            logger.error(f"Failed to fetch document page: {urls[0]}")
            return False

        doc = self._parse_document_page(urls[0], resp.text)
        if doc is None:
            logger.error("Failed to parse document page")
            return False

        logger.info(f"Page OK: {doc['title'][:60]}")

        # Test PDF if available
        if doc["pdf_url"]:
            text = self._download_pdf_text(doc["pdf_url"], doc["reference"])
            if text:
                logger.info(f"PDF OK: {len(text)} chars extracted")
            else:
                logger.warning("PDF extraction returned no text, will use HTML fallback")

        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="NZ/IRD-TaxRulings data fetcher")
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

    scraper = IRDTaxRulingsScraper()

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
