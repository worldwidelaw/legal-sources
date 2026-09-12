#!/usr/bin/env python3
"""
LS/LRA-TaxRulings -- Revenue Services Lesotho — Public Rulings & Tax Legislation

Fetches tax legislation, public rulings, legal notices, customs & excise laws,
VAT laws, and tax treaties from rsl.org.ls (formerly lra.org.ls).

Strategy:
  1. Scrape 6 category pages for direct PDF links (/sites/default/files/...)
  2. Scrape paginated /legal-notices for Drupal node links
  3. Visit each node page to extract its PDF download link
  4. Download and extract full text from all PDFs

Note: Uses curl via subprocess because rsl.org.ls requires TLS 1.3 which
the system Python 3.9 LibreSSL 2.8.3 does not support.

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LS.LRA-TaxRulings")

BASE_URL = "https://www.rsl.org.ls"
DELAY = 2.0

CATEGORY_PAGES = {
    "Income Tax": "/income-tax",
    "VAT": "/value-added-tax-vat",
    "Customs & Excise": "/customs-excise-laws",
    "Public Rulings": "/public-rulings-0",
    "Other Revenue Laws": "/other-revenue-laws",
    "Tax Treaties": "/tax-treaties",
}


def _curl_get(url: str, timeout: int = 60, binary: bool = False) -> Optional[bytes]:
    """Fetch a URL using curl subprocess (bypasses Python SSL limitations)."""
    for attempt in range(3):
        try:
            time.sleep(DELAY)
            result = subprocess.run(
                ["curl", "-sL", "--max-time", str(timeout),
                 "-H", "User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                 "-H", "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                 url],
                capture_output=True,
                timeout=timeout + 10,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
            logger.warning("curl attempt %d failed for %s (rc=%d)", attempt + 1, url, result.returncode)
        except subprocess.TimeoutExpired:
            logger.warning("curl timeout attempt %d for %s", attempt + 1, url)
        except Exception as e:
            logger.warning("curl attempt %d error for %s: %s", attempt + 1, url, e)
        if attempt < 2:
            time.sleep(5)
    return None


def _curl_get_text(url: str, timeout: int = 60) -> Optional[str]:
    """Fetch URL text content via curl."""
    data = _curl_get(url, timeout)
    if data:
        return data.decode("utf-8", errors="replace")
    return None


class _PDFLinkExtractor(HTMLParser):
    """Extract PDF links from Drupal pages (/sites/default/files/...)."""

    def __init__(self):
        super().__init__()
        self.in_link = False
        self.current_url = ""
        self.text_parts: List[str] = []
        self.links: List[Tuple[str, str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            href = d.get("href", "")
            if "/sites/default/files/" in href and href.lower().endswith(".pdf"):
                self.in_link = True
                self.current_url = href
                self.text_parts = []

    def handle_data(self, data):
        if self.in_link:
            self.text_parts.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self.in_link:
            title = " ".join(p for p in self.text_parts if p).strip()
            if self.current_url:
                self.links.append((title, self.current_url))
            self.in_link = False


class _NodeLinkExtractor(HTMLParser):
    """Extract /node/NNN links from the legal notices listing page."""

    def __init__(self):
        super().__init__()
        self.in_link = False
        self.current_url = ""
        self.text_parts: List[str] = []
        self.links: List[Tuple[str, str]] = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            href = d.get("href", "")
            if re.match(r"^/node/\d+$", href):
                self.in_link = True
                self.current_url = href
                self.text_parts = []

    def handle_data(self, data):
        if self.in_link:
            self.text_parts.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self.in_link:
            title = " ".join(p for p in self.text_parts if p).strip()
            if self.current_url and title:
                self.links.append((title, self.current_url))
            self.in_link = False


def _make_id(url: str) -> str:
    """Create a stable ID from a PDF URL or node path."""
    m = re.search(r"/files/[\d-]+/(.+\.pdf)", url, re.IGNORECASE)
    if m:
        slug = unquote(m.group(1))
        slug = re.sub(r"\.pdf$", "", slug, flags=re.IGNORECASE)
        slug = re.sub(r"[^a-zA-Z0-9]+", "_", slug).strip("_")
        if len(slug) > 80:
            slug = slug[:80]
        return f"LS_LRA_{slug}"
    m = re.search(r"/node/(\d+)", url)
    if m:
        return f"LS_LRA_node_{m.group(1)}"
    return f"LS_LRA_{abs(hash(url)) % 10**10}"


def _title_from_filename(url: str) -> str:
    """Extract a readable title from a PDF URL."""
    m = re.search(r"/([^/]+\.pdf)", url, re.IGNORECASE)
    if m:
        name = unquote(m.group(1))
        name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
        name = re.sub(r"[_]+", " ", name)
        name = re.sub(r"\s*\(\d+\)\s*$", "", name)
        name = re.sub(r"_\d+$", "", name)
        return name.strip()
    return ""


def _extract_year(text: str) -> Optional[str]:
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    if m:
        year = int(m.group(1))
        if 1950 <= year <= 2030:
            return f"{year}-01-01"
    return None


def _classify_type(title: str) -> str:
    t = title.lower()
    if any(w in t for w in ["act", "regulation", "rules", "order", "bill", "amendment", "levy"]):
        return "legislation"
    if any(w in t for w in ["guide", "ruling", "practice note", "interpretation",
                             "explanatory", "memorandum", "procedure"]):
        return "doctrine"
    if "treaty" in t or "agreement" in t or "dta" in t or "tiea" in t:
        return "legislation"
    return "legislation"


class LRATaxRulingsScraper(BaseScraper):
    """Scraper for LS/LRA-TaxRulings."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _scrape_category_pages(self) -> List[Dict[str, Any]]:
        """Scrape category pages for direct PDF links."""
        seen_urls = set()
        results = []

        for category, path in CATEGORY_PAGES.items():
            url = BASE_URL + path
            html = _curl_get_text(url)
            if html is None:
                logger.warning("Cannot fetch %s", url)
                continue

            parser = _PDFLinkExtractor()
            parser.feed(html)

            for anchor_text, href in parser.links:
                full_url = urljoin(BASE_URL, href)
                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                title = anchor_text if anchor_text else _title_from_filename(full_url)
                if not title or len(title) < 3:
                    title = _title_from_filename(full_url)
                if not title or len(title) < 3:
                    continue

                results.append({
                    "title": title,
                    "url": full_url,
                    "category": category,
                    "source": "category_page",
                })

            logger.info("  %s: %d PDF links", category, len(parser.links))

        logger.info("Category pages total: %d unique PDFs", len(results))
        return results

    def _scrape_legal_notices(self) -> List[Dict[str, Any]]:
        """Scrape paginated legal notices for node links, then get PDFs."""
        node_links = []
        seen_nodes = set()

        for page in range(10):
            url = f"{BASE_URL}/legal-notices?page={page}"
            html = _curl_get_text(url)
            if html is None:
                break

            parser = _NodeLinkExtractor()
            parser.feed(html)

            if not parser.links:
                break

            new_count = 0
            for title, href in parser.links:
                if href in seen_nodes:
                    continue
                seen_nodes.add(href)
                node_links.append((title, href))
                new_count += 1

            logger.info("  Legal notices page %d: %d new nodes", page, new_count)

            if "?page=" + str(page + 1) not in html:
                break

        logger.info("Total legal notice nodes: %d", len(node_links))

        results = []
        for title, node_path in node_links:
            node_url = BASE_URL + node_path
            html = _curl_get_text(node_url)
            if html is None:
                continue

            parser = _PDFLinkExtractor()
            parser.feed(html)

            if parser.links:
                _, pdf_href = parser.links[0]
                full_pdf_url = urljoin(BASE_URL, pdf_href)
                results.append({
                    "title": title,
                    "url": full_pdf_url,
                    "category": "Legal Notices",
                    "source": "legal_notice",
                    "node_url": node_url,
                })
            else:
                logger.warning("No PDF in node %s (%s)", node_path, title[:50])

        logger.info("Legal notices with PDFs: %d", len(results))
        return results

    def _get_all_docs(self) -> List[Dict[str, Any]]:
        """Combine all document sources, deduplicating by PDF URL."""
        category_docs = self._scrape_category_pages()
        notice_docs = self._scrape_legal_notices()

        seen_urls = set()
        all_docs = []

        for doc in category_docs + notice_docs:
            if doc["url"] in seen_urls:
                continue
            seen_urls.add(doc["url"])
            all_docs.append(doc)

        logger.info("Total unique documents: %d", len(all_docs))
        return all_docs

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        title = raw.get("title", "")
        doc_type = _classify_type(title)
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "LS/LRA-TaxRulings",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        doc_list = self._get_all_docs()
        if not doc_list:
            logger.error("No documents found")
            return

        count = 0
        for doc in doc_list:
            if max_records and count >= max_records:
                return

            doc_id = _make_id(doc["url"])
            title = doc["title"]
            url = doc["url"]
            logger.info("Downloading [%d/%d]: %s", count + 1, len(doc_list), title[:60])

            pdf_bytes = _curl_get(url, timeout=90)
            if pdf_bytes is None:
                continue
            if len(pdf_bytes) < 200:
                logger.warning("File too small (%d bytes): %s", len(pdf_bytes), url)
                continue
            if not pdf_bytes[:5].startswith(b"%PDF"):
                logger.warning("Not a PDF file: %s", url)
                continue

            try:
                text = extract_pdf_markdown(
                    source="LS/LRA-TaxRulings",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                )
            except Exception as e:
                logger.warning("Failed to extract %s: %s", url, e)
                continue

            if not text or len(text) < 100:
                logger.warning("Insufficient text (%d chars): %s",
                             len(text or ""), title[:50])
                continue

            date = _extract_year(title)

            raw = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "category": doc.get("category", ""),
            }
            count += 1
            yield raw

        logger.info("Completed: %d documents fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        doc_list = self._get_all_docs()
        if not doc_list:
            logger.error("Cannot fetch any documents")
            return False

        logger.info("Document list OK: %d documents found", len(doc_list))
        cats = {}
        for d in doc_list:
            c = d.get("category", "?")
            cats[c] = cats.get(c, 0) + 1
        for c, n in sorted(cats.items()):
            logger.info("  %s: %d", c, n)

        title, url = doc_list[0]["title"], doc_list[0]["url"]
        logger.info("Testing download: %s", title[:60])
        pdf_bytes = _curl_get(url)
        if pdf_bytes and len(pdf_bytes) > 200:
            logger.info("PDF download OK: %d bytes", len(pdf_bytes))
        else:
            logger.warning("PDF download issue")

        return True


def main():
    parser = argparse.ArgumentParser(description="LS/LRA-TaxRulings data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LRATaxRulingsScraper()

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
