#!/usr/bin/env python3
"""
MO/DICJ -- Macau Gaming Inspection & Coordination Bureau

Fetches regulatory documents from dicj.gov.mo:
  - Gaming legislation (~90 docs in Chinese/Portuguese HTML)
  - Gaming rules (~46 rule documents in Chinese HTML)
  - AML laws and instructions (HTML + English PDFs)
  - EGM technical standards (English PDFs)
  - Concession contracts (Chinese/Portuguese HTML)

Strategy:
  1. Crawl index pages to discover document links
  2. Fetch each HTML document and extract full text
  3. For PDFs, download and extract text via pdf_extract
  Content is delimited by Dreamweaver template markers.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set
from html import unescape
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MO.DICJ")

BASE_URL = "https://www.dicj.gov.mo"

# Index pages to crawl for document links
INDEX_PAGES = [
    # (path, category, language)
    ("/web/cn/legislation/FortunaAzar/index.html", "Gaming Legislation", "cn"),
    ("/web/cn/legislation/LotDesp/index.html", "Sports Lottery Legislation", "cn"),
    ("/web/cn/legislation/LotCh/index.html", "Chinese Lottery Legislation", "cn"),
    ("/web/cn/legislation/others/index.html", "Technical Standards Legislation", "cn"),
    ("/web/cn/rules/index.html", "Gaming Rules", "cn"),
    ("/web/cn/anticrime/lei/index.html", "AML Laws", "cn"),
    ("/web/cn/anticrime/regadm/index.html", "AML Regulations", "cn"),
    ("/web/cn/anticrime/instrucao/index.html", "AML Instructions", "cn"),
    ("/web/en/egm/standards/index.html", "EGM Technical Standards", "en"),
]


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    if not html:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<li[^>]*>', '\n- ', text, flags=re.IGNORECASE)
    text = re.sub(r'<h[1-6][^>]*>', '\n\n## ', text, flags=re.IGNORECASE)
    text = re.sub(r'</h[1-6]>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<tr[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<td[^>]*>', ' | ', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_content(html: str) -> str:
    """Extract content from Dreamweaver template markers or body."""
    # Try Dreamweaver editable region first
    m = re.search(
        r'<!--\s*InstanceBeginEditable\s+name="content"\s*-->(.*?)<!--\s*InstanceEndEditable\s*-->',
        html, re.DOTALL
    )
    if m:
        return strip_html(m.group(1))

    # Fallback: main content area
    m = re.search(r'<div[^>]+id="content"[^>]*>(.*?)</div>', html, re.DOTALL)
    if m:
        return strip_html(m.group(1))

    # Fallback: body
    m = re.search(r'<body[^>]*>(.*?)</body>', html, re.DOTALL)
    if m:
        return strip_html(m.group(1))

    return strip_html(html)


def extract_title(html: str) -> str:
    """Extract page title from HTML."""
    m = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL | re.IGNORECASE)
    if m:
        title = strip_html(m.group(1)).strip()
        # Remove common suffixes
        title = re.sub(r'\s*[-|–]\s*DICJ.*$', '', title)
        return title
    return ""


class MODICJScraper(BaseScraper):
    """Scraper for MO/DICJ -- Macau Gaming Inspection Bureau."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=60,
        )
        self._seen: Set[str] = set()

    def _fetch_page(self, path: str) -> str:
        """Fetch an HTML page and return its content."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {path}: {e}")
            return ""

    def _discover_links(self, index_path: str, category: str,
                        _visited: Set[str] = None, _depth: int = 0) -> List[Dict[str, str]]:
        """Discover document links from an index page."""
        if _visited is None:
            _visited = set()
        if index_path in _visited or _depth > 2:
            return []
        _visited.add(index_path)

        html = self._fetch_page(index_path)
        if not html:
            return []

        # Extract only from content area to avoid navigation links
        content_html = html
        m = re.search(
            r'<!--\s*InstanceBeginEditable\s+name="content"\s*-->(.*?)<!--\s*InstanceEndEditable\s*-->',
            html, re.DOTALL
        )
        if m:
            content_html = m.group(1)

        links = []
        base_url = BASE_URL + index_path.rsplit("/", 1)[0] + "/"

        for anchor in re.finditer(r'href="([^"]*\.(html|pdf|PDF))"[^>]*>(.*?)</a>',
                                   content_html, re.IGNORECASE | re.DOTALL):
            href = anchor.group(1)
            link_text = strip_html(anchor.group(3)).strip()

            # Resolve relative URLs
            resolved = urljoin(base_url, href)
            # Convert back to path
            if resolved.startswith(BASE_URL):
                url = resolved[len(BASE_URL):]
            else:
                continue  # external link, skip

            if url == index_path:
                continue

            # Sub-index pages: recurse (with cycle protection)
            if url.endswith("index.html"):
                sub_links = self._discover_links(url, category, _visited, _depth + 1)
                links.extend(sub_links)
                continue

            links.append({
                "url": url,
                "title": link_text,
                "category": category,
                "is_pdf": url.lower().endswith(".pdf"),
            })

        return links

    def _fetch_html_document(self, path: str) -> Dict[str, str]:
        """Fetch an HTML document and extract its content."""
        html = self._fetch_page(path)
        if not html:
            return {"title": "", "text": ""}

        title = extract_title(html)
        text = extract_content(html)
        return {"title": title, "text": text}

    def _fetch_pdf_document(self, path: str) -> str:
        """Fetch a PDF and extract text."""
        try:
            from common.pdf_extract import extract_pdf_markdown
        except ImportError:
            logger.warning("common.pdf_extract not available")
            return ""
        try:
            url = path if path.startswith("http") else f"{BASE_URL}{path}"
            self.rate_limiter.wait()
            resp = self.client.get(path)
            resp.raise_for_status()
            return extract_pdf_markdown(resp.content)
        except Exception as e:
            logger.warning(f"Failed to extract PDF {path}: {e}")
            return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        url = raw.get("url", "")
        if not url.startswith("http"):
            url = f"{BASE_URL}{url}"

        # Extract title from content headings if page title is generic
        title = raw.get("title", "")
        text = raw.get("text", "")
        if not title or "博彩監察協調局" in title or "DICJ" in title:
            headings = re.findall(r'^## (.+)$', text, re.MULTILINE)
            if len(headings) >= 2:
                title = f"{headings[0].strip()} - {headings[1].strip()}"
            elif headings:
                title = headings[0].strip()

        # Determine _type based on category
        category = raw.get("category", "")
        if "Legislation" in category or "Laws" in category or "Regulations" in category:
            _type = "legislation"
        else:
            _type = "doctrine"

        return {
            "_id": f"MO/DICJ/{doc_id}",
            "_source": "MO/DICJ",
            "_type": _type,
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "doc_id": doc_id,
            "category": category,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        for index_path, category, lang in INDEX_PAGES:
            if limit and count >= limit:
                break

            logger.info(f"Discovering documents: {category}")
            links = self._discover_links(index_path, category)
            logger.info(f"  Found {len(links)} document links")

            for link in links:
                if limit and count >= limit:
                    break

                url = link["url"]
                # Deduplicate
                norm_url = url.split("?")[0].split("#")[0]
                if norm_url in self._seen:
                    continue
                self._seen.add(norm_url)

                # Create doc_id from URL path
                doc_id = norm_url.rsplit("/", 1)[-1].replace(".html", "").replace(".pdf", "")
                if not doc_id:
                    continue

                if link["is_pdf"]:
                    logger.info(f"  [{count+1}] PDF: {doc_id}")
                    text = self._fetch_pdf_document(url)
                    title = link.get("title") or doc_id
                else:
                    logger.info(f"  [{count+1}] HTML: {doc_id}")
                    result = self._fetch_html_document(url)
                    text = result["text"]
                    title = result["title"] or link.get("title") or doc_id

                if not text or len(text.strip()) < 50:
                    logger.debug(f"  Skipping {doc_id}: insufficient text ({len(text.strip()) if text else 0} chars)")
                    continue

                yield {
                    "doc_id": doc_id,
                    "title": title,
                    "text": text,
                    "date": None,
                    "url": url,
                    "category": link["category"],
                }
                count += 1

        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all documents (static site, no date filtering)."""
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = MODICJScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        html = scraper._fetch_page("/web/cn/legislation/FortunaAzar/index.html")
        if html and len(html) > 100:
            logger.info(f"Connection OK: FortunaAzar index page ({len(html)} bytes)")
            sys.exit(0)
        else:
            logger.error("Connection failed")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
