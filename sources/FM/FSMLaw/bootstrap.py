#!/usr/bin/env python3
"""
FM/FSMLaw -- FSM Legal Information System

Fetches legislation and court decisions from the official FSM Legal Information
System (fsmlaw.org). Covers:
  - FSM National Code (Titles 1-58, ~200 chapters)
  - FSM Supreme Court decisions (Volumes 1-22+)
  - State codes for Chuuk, Kosrae, Pohnpei, Yap
  - State court decisions for all 4 states

All content is static HTML. We scrape index pages to discover document links,
then fetch and extract full text from each page.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 12+ sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import re
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional
from html import unescape
from urllib.parse import urljoin
from itertools import zip_longest

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FM.FSMLaw")

BASE_URL = "https://www.fsmlaw.org"

# Jurisdictions and their index pages
JURISDICTIONS = {
    "FSM": {
        "name": "Federated States of Micronesia",
        "code_index": "/fsm/code/index.htm",
        "decisions_indexes": [
            "/fsm/decisions/index.htm",
            "/fsm/decisions/index2.htm",
            "/fsm/decisions/index3.html",
        ],
    },
    "Chuuk": {
        "name": "State of Chuuk",
        "code_index": "/chuuk/code/index.htm",
        "decisions_indexes": ["/chuuk/decisions/index.htm"],
    },
    "Kosrae": {
        "name": "State of Kosrae",
        "code_index": "/kosrae/code/index.htm",
        "decisions_indexes": ["/kosrae/decisions/index.htm"],
    },
    "Pohnpei": {
        "name": "State of Pohnpei",
        "code_index": "/pohnpei/code/indexcode.htm",
        "decisions_indexes": ["/pohnpei/decisions/index.htm"],
    },
    "Yap": {
        "name": "State of Yap",
        "code_index": "/yap/code/index.htm",
        "decisions_indexes": ["/yap/decisions/index.htm"],
    },
}

# Browser-like headers to avoid 406 Not Acceptable from the server
_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

MAX_RETRIES = 3
RETRY_BACKOFF = 2  # seconds, doubles each retry


def _strip_html(html: str) -> str:
    """Strip HTML tags and decode entities, preserving structure."""
    text = re.sub(r'<br\s*/?\s*>', '\n', html, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|td|th|blockquote|section|article)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<(?:p|div|h[1-6]|li|tr|td|th|blockquote|section|article)[^>]*>', '\n', text, flags=re.IGNORECASE)
    # Remove style and script blocks
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.IGNORECASE | re.DOTALL)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _extract_title_from_html(html: str) -> str:
    """Extract title from HTML <title> tag or first <h1>/<h2>."""
    m = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
    if m:
        title = _strip_html(m.group(1)).strip()
        if title and title.lower() not in ("", "untitled", "document"):
            return title
    for tag in ['h1', 'h2', 'h3']:
        m = re.search(rf'<{tag}[^>]*>(.*?)</{tag}>', html, re.IGNORECASE | re.DOTALL)
        if m:
            title = _strip_html(m.group(1)).strip()
            if title:
                return title
    return ""


def _extract_links(html: str, base_url: str) -> List[str]:
    """Extract all href links from HTML, resolved against base_url."""
    links = []
    for m in re.finditer(r'href="([^"]+)"', html, re.IGNORECASE):
        href = m.group(1)
        if href.startswith('#') or href.startswith('mailto:') or href.startswith('javascript:'):
            continue
        # Skip external links
        if href.startswith('http') and 'fsmlaw.org' not in href:
            continue
        full_url = urljoin(base_url, href)
        # Normalize http:// to https://
        full_url = full_url.replace('http://fsmlaw.org', 'https://www.fsmlaw.org')
        full_url = full_url.replace('http://www.fsmlaw.org', 'https://www.fsmlaw.org')
        if full_url.endswith(('.htm', '.html')):
            links.append(full_url)
    return links


class FSMLawScraper(BaseScraper):
    """Scraper for FM/FSMLaw - FSM Legal Information System."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry

            self.session = requests.Session()
            self.session.headers.update(_BROWSER_HEADERS)

            # Retry adapter for transient errors (502, 503, 504, connection resets)
            retry_strategy = Retry(
                total=MAX_RETRIES,
                backoff_factor=RETRY_BACKOFF,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self.session.mount("https://", adapter)
            self.session.mount("http://", adapter)
        return self.session

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page with retry and SSL fallback.

        Returns HTML content or None on error.
        """
        self.rate_limiter.wait()
        sess = self._get_session()

        # Try with SSL verification first, fall back to verify=False
        for verify in (True, False):
            try:
                resp = sess.get(url, timeout=30, verify=verify)
                if resp.status_code == 406:
                    # Server rejected our Accept header -- unlikely with
                    # browser-like headers, but log and skip
                    logger.warning(f"HTTP 406 for {url} (verify={verify})")
                    continue
                resp.raise_for_status()
                # Try to detect encoding
                if resp.encoding and resp.encoding.lower() != 'utf-8':
                    resp.encoding = resp.apparent_encoding or 'utf-8'
                return resp.text
            except Exception as e:
                if verify:
                    logger.debug(f"SSL-verified fetch failed for {url}: {e}, retrying without verify")
                    continue
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        return None

    def _discover_code_pages(self, jurisdiction: str, index_path: str) -> List[Dict[str, str]]:
        """Discover all code chapter pages from an index."""
        index_url = BASE_URL + index_path
        html = self._fetch_page(index_url)
        if not html:
            return []

        links = _extract_links(html, index_url)
        pages = []
        seen = set()
        jur_lower = jurisdiction.lower() if jurisdiction != "FSM" else "fsm"
        code_base = f"/{jur_lower}/code/"
        for link in links:
            link_lower = link.lower()
            # Must be within this jurisdiction's code directory
            if code_base not in link_lower:
                continue
            # Must be in a title subdirectory (actual chapter content)
            if '/title' not in link_lower:
                continue
            basename = link.split('/')[-1].lower()
            if basename.startswith('index'):
                continue
            if link not in seen:
                seen.add(link)
                pages.append({"url": link, "jurisdiction": jurisdiction, "type": "legislation"})

        logger.info(f"  {jurisdiction} code: found {len(pages)} chapter pages")
        return pages

    def _discover_decision_pages(self, jurisdiction: str, index_paths: List[str]) -> List[Dict[str, str]]:
        """Discover all court decision pages from index pages."""
        pages = []
        seen = set()

        for index_path in index_paths:
            index_url = BASE_URL + index_path
            html = self._fetch_page(index_url)
            if not html:
                continue

            links = _extract_links(html, index_url)
            for link in links:
                # Only include links to volume pages (actual decisions)
                if '/vol' not in link.lower():
                    continue
                # Skip PDF links that somehow ended up as .htm
                basename = link.split('/')[-1].lower()
                if basename.startswith('index'):
                    continue
                if link not in seen:
                    seen.add(link)
                    pages.append({"url": link, "jurisdiction": jurisdiction, "type": "case_law"})

        logger.info(f"  {jurisdiction} decisions: found {len(pages)} decision pages")
        return pages

    def _fetch_and_extract(self, page_info: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Fetch a page and extract its content."""
        url = page_info["url"]
        html = self._fetch_page(url)
        if not html:
            return None

        title = _extract_title_from_html(html)
        text = _strip_html(html)

        if not text or len(text) < 50:
            return None

        # If title is empty, derive from URL
        if not title:
            basename = url.split('/')[-1].replace('.htm', '').replace('.html', '')
            title = basename.replace('_', ' ').replace('-', ' ').title()

        # Try to extract date from text for court decisions
        date = ""
        if page_info["type"] == "case_law":
            # Look for date patterns like "January 5, 1981" or "1981"
            date_match = re.search(
                r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+(\d{4})',
                text
            )
            if date_match:
                try:
                    date_str = date_match.group(0).replace(',', '')
                    dt = datetime.strptime(date_str, "%B %d %Y")
                    date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    date = date_match.group(1) + "-01-01"
            else:
                # Try to get year from citation pattern like "1 FSM Intrm. 1 (Pon. 1981)"
                year_match = re.search(r'\((?:[A-Za-z.]+\s+)?(\d{4})\)', text)
                if year_match:
                    date = year_match.group(1) + "-01-01"

        # Extract citation for court decisions
        citation = ""
        if page_info["type"] == "case_law":
            cit_match = re.search(r'(\d+\s+(?:FSM|CSR|KSR|PSR|YSR)\s+(?:Intrm\.?\s+)?\d+)', text)
            if cit_match:
                citation = cit_match.group(1)

        return {
            "url": url,
            "title": title,
            "text": text,
            "date": date,
            "citation": citation,
            "jurisdiction": page_info["jurisdiction"],
            "doc_type": page_info["type"],
        }

    def normalize(self, raw: dict) -> dict:
        url = raw.get("url", "")
        title = raw.get("title", "Unknown")
        text = raw.get("text", "")
        jurisdiction = raw.get("jurisdiction", "FSM")
        doc_type = raw.get("doc_type", "legislation")

        # Build stable ID from URL
        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        stable_id = f"FM/FSMLaw/{url_hash}"

        # Map doc_type to standard types
        _type = "case_law" if doc_type == "case_law" else "legislation"

        return {
            "_id": stable_id,
            "_source": "FM/FSMLaw",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date", ""),
            "url": url,
            "jurisdiction": jurisdiction,
            "citation": raw.get("citation", ""),
            "doc_type": doc_type,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        count = 0

        for jur_key, jur_info in JURISDICTIONS.items():
            # Discover legislation pages
            logger.info(f"Discovering {jur_key} legislation...")
            code_pages = self._discover_code_pages(jur_key, jur_info["code_index"])

            # Discover decision pages
            logger.info(f"Discovering {jur_key} decisions...")
            decision_pages = self._discover_decision_pages(jur_key, jur_info["decisions_indexes"])

            # Interleave legislation and case_law so that the BaseScraper's
            # sample_size cut-off captures both types, not just legislation.
            for code_page, decision_page in zip_longest(code_pages, decision_pages):
                if code_page is not None:
                    result = self._fetch_and_extract(code_page)
                    if result and result["text"] and len(result["text"]) >= 50:
                        yield result
                        count += 1
                        logger.info(
                            f"  [{count}] {result['title'][:60]} "
                            f"({len(result['text'])} chars) [{result['doc_type']}]"
                        )

                if decision_page is not None:
                    result = self._fetch_and_extract(decision_page)
                    if result and result["text"] and len(result["text"]) >= 50:
                        yield result
                        count += 1
                        logger.info(
                            f"  [{count}] {result['title'][:60]} "
                            f"({len(result['text'])} chars) [{result['doc_type']}]"
                        )

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = FSMLawScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing FSM Legal Information System access...")
        import requests
        sess = requests.Session()
        sess.headers.update(_BROWSER_HEADERS)
        resp = sess.get(f"{BASE_URL}/fsm/code/index.htm", timeout=30)
        print(f"  Status: {resp.status_code}")
        links = _extract_links(resp.text, f"{BASE_URL}/fsm/code/index.htm")
        print(f"  Found {len(links)} links on FSM Code index")

        # Test fetching one chapter
        if links:
            test_url = links[0]
            print(f"  Fetching: {test_url}")
            resp2 = sess.get(test_url, timeout=30)
            title = _extract_title_from_html(resp2.text)
            text = _strip_html(resp2.text)
            print(f"  Title: {title[:80]}")
            print(f"  Text length: {len(text)} chars")
            print(f"  Text preview: {text[:200]}...")
        print("Test PASSED")

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
