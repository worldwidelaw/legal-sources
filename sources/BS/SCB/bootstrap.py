#!/usr/bin/env python3
"""
BS/SCB -- Securities Commission of the Bahamas Enforcement Actions

Fetches enforcement documents from scb.gov.bs/enforcement/. The site has
four sub-sections, each a simple HTML page with year headings and PDF links:
  - Judgments and Rulings (/enforcement/judgements/)
  - Disciplinary Decisions (/enforcement/disciplinary-decisions/)
  - Settlements (/enforcement/settlements/)
  - Supervisory Matters (/enforcement/supervisory-matters/)

Strategy:
  1. Fetch each sub-section HTML page
  2. Parse year headings and PDF links from <ul>/<li> elements
  3. Download PDFs and extract full text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BS.SCB")

BASE_URL = "https://www.scb.gov.bs"

ENFORCEMENT_PAGES = {
    "judgments_and_rulings": "/enforcement/judgements/",
    "disciplinary_decisions": "/enforcement/disciplinary-decisions/",
    "settlements": "/enforcement/settlements/",
    "supervisory_matters": "/enforcement/supervisory-matters/",
}


def clean_html(text: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&#8211;|&ndash;', '\u2013', text)
    text = re.sub(r'&#8212;|&mdash;', '\u2014', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def _is_enforcement_doc(title: str, url: str) -> bool:
    """Filter out non-enforcement sidebar/footer PDFs."""
    lower_title = title.lower()
    lower_url = url.lower()
    exclude_keywords = [
        "organisation chart", "organizational chart", "org chart",
        "iosco", "ioeama", "unscr", "annual report",
    ]
    for kw in exclude_keywords:
        if kw in lower_title or kw in lower_url:
            return False
    return True


def _extract_docs_from_html(html: str, category: str) -> List[Dict[str, Any]]:
    """Extract document metadata and PDF URLs from an enforcement page.

    The SCB site uses WordPress accordion widgets. Year labels are in
    <span class="accordions-head-title">YYYY</span> and content follows
    in <div class="accordion-content ..."> blocks with <ul><li><a> PDF links.
    """
    docs = []
    seen_urls: set = set()

    # Find accordion sections: year header + content block
    # Pattern: year in accordions-head-title span, PDFs in accordion-content div
    accordion_pattern = re.compile(
        r'class="accordions-head-title">\s*(\d{4})[^<]*</span>'
        r'.*?'
        r'<div\s+class="accordion-content[^"]*">(.*?)</div>',
        re.DOTALL | re.IGNORECASE,
    )

    for match in accordion_pattern.finditer(html):
        year = match.group(1)
        content_block = match.group(2)

        pdf_links = re.findall(
            r'<a\s[^>]*href="([^"]*\.pdf)"[^>]*>(.*?)</a>',
            content_block,
            re.DOTALL | re.IGNORECASE,
        )

        for url, link_text in pdf_links:
            if url in seen_urls:
                continue
            seen_urls.add(url)

            title = clean_html(link_text)
            if not title:
                title = url.split('/')[-1].replace('.pdf', '').replace('-', ' ')

            # Make URL absolute
            if url.startswith('/'):
                url = BASE_URL + url
            elif not url.startswith('http'):
                url = BASE_URL + '/' + url

            if not _is_enforcement_doc(title, url):
                continue

            doc_id = hashlib.md5(url.encode()).hexdigest()[:12]

            docs.append({
                "doc_id": doc_id,
                "title": title,
                "category": category,
                "year": year,
                "file_url": url,
                "page_url": BASE_URL + ENFORCEMENT_PAGES[category],
            })

    return docs


class SCBScraper(BaseScraper):
    """Scraper for BS/SCB -- Securities Commission of the Bahamas Enforcement."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        return extract_pdf_markdown(
            source="BS/SCB",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

    def _fetch_page_docs(self, category: str, path: str) -> List[Dict[str, Any]]:
        """Fetch and parse one enforcement sub-page."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            return _extract_docs_from_html(resp.text, category)
        except Exception as e:
            logger.warning(f"Failed to fetch {category} page: {e}")
            return []

    def _fetch_all_docs(self) -> List[Dict[str, Any]]:
        """Fetch doc metadata from all enforcement sub-pages."""
        all_docs = []
        for category, path in ENFORCEMENT_PAGES.items():
            docs = self._fetch_page_docs(category, path)
            logger.info(f"  {category}: {len(docs)} documents")
            all_docs.extend(docs)
        logger.info(f"Total enforcement documents found: {len(all_docs)}")
        return all_docs

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"BS/SCB/{raw['doc_id']}",
            "_source": "BS/SCB",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_prefetched_text", ""),
            "date": raw.get("year", ""),
            "url": raw.get("page_url", ""),
            "doc_id": raw["doc_id"],
            "category": raw.get("category", ""),
            "year": raw.get("year", ""),
            "file_url": raw.get("file_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        all_docs = self._fetch_all_docs()
        limit = 15 if sample else None
        count = 0

        for doc in all_docs:
            if limit and count >= limit:
                break

            file_url = doc.get("file_url", "")
            if not file_url:
                continue

            try:
                self.rate_limiter.wait()
                resp = self.client.get(file_url)
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning(f"  Failed to download PDF {file_url}: {e}")
                continue

            text = self._extract_pdf_text(pdf_bytes)
            if not text or len(text) < 50:
                logger.warning(f"  Skipping {doc['title'][:60]} - no/short text from PDF")
                continue

            doc["_prefetched_text"] = text
            yield doc
            count += 1
            logger.info(f"  [{count}] {doc['title'][:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = SCBScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing SCB enforcement pages...")
        all_docs = scraper._fetch_all_docs()
        if all_docs:
            print(f"Connection OK. Found {len(all_docs)} enforcement documents.")
            for cat in ENFORCEMENT_PAGES:
                cat_docs = [d for d in all_docs if d["category"] == cat]
                print(f"  {cat}: {len(cat_docs)}")
            if all_docs:
                print(f"Sample: {all_docs[0]['title'][:80]}")
                print(f"  PDF: {all_docs[0]['file_url']}")
        else:
            print("Connection FAILED - no documents found")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
