#!/usr/bin/env python3
"""
SA/SAMARulebook -- Saudi Central Bank (SAMA) Rulebook

Fetches regulatory content from rulebook.sama.gov.sa, the SAMA Rulebook platform.
Covers laws, implementing regulations, sector-specific rules, guidance, and circulars
for banking, finance, insurance, payment systems, and AML/CFT.

Strategy:
  - Crawl Drupal book navigation starting from 9 top-level category pages
  - Recursively follow internal links to discover all content pages
  - Extract full text from HTML using BeautifulSoup
  - ~500+ regulatory documents across all sectors

Source: https://rulebook.sama.gov.sa/
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full crawl
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, urlparse

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 required. Install: pip install beautifulsoup4")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SA.SAMARulebook")

BASE_URL = "https://rulebook.sama.gov.sa"

# Top-level category pages (book-category ID -> section name)
MIN_TEXT_LENGTH = 500  # skip hub/navigation pages

CATEGORIES = {
    1361: "Laws and Implementing Regulations",
    1362: "All Financial Institutions",
    1363: "Banking Sector",
    1365: "Finance Sector",
    1367: "Payment Systems and Providers",
    1366: "Money Exchange Sector",
    5902: "Credit Bureaus",
    1368: "Regulatory Sandbox",
    10291: "SAMA Circulars",
}


class SAMARulebookScraper(BaseScraper):
    """
    Scraper for SA/SAMARulebook -- Saudi Central Bank Rulebook.
    Country: SA
    URL: https://rulebook.sama.gov.sa/
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=60,
        )
        self._visited = set()

    # -- HTML fetching & parsing ---------------------------------------------

    def _fetch_html(self, path: str) -> Optional[BeautifulSoup]:
        """Fetch a page and return parsed BeautifulSoup."""
        url = urljoin(BASE_URL, path)
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url, timeout=60)
            if resp is None or resp.status_code != 200:
                logger.debug(f"HTTP {resp.status_code if resp else 'None'} for {path}")
                return None
            return BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            logger.debug(f"Error fetching {path}: {e}")
            return None

    def _extract_text(self, soup: BeautifulSoup) -> str:
        """Extract main content text from a Drupal node page."""
        # Primary: .node__content or .field--name-body
        content = soup.find("div", class_="node__content")
        if not content:
            content = soup.find("div", class_=re.compile(r"field--name-body"))
        if not content:
            # Fallback: main content area
            content = soup.find("article") or soup.find("main")
        if not content:
            return ""

        # Remove unwanted elements
        for tag in content.find_all(["script", "style", "noscript", "iframe", "nav"]):
            tag.decompose()
        # Remove book navigation blocks
        for nav in content.find_all("div", class_=re.compile(r"book-navigation|book-pager|menu--book")):
            nav.decompose()
        # Remove breadcrumbs
        for bc in content.find_all("nav", class_=re.compile(r"breadcrumb")):
            bc.decompose()

        text = content.get_text(separator="\n", strip=True)
        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        return text.strip()

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extract page title."""
        # Try h1 first
        h1 = soup.find("h1")
        if h1:
            return h1.get_text(strip=True)
        # Fallback to <title>
        title_tag = soup.find("title")
        if title_tag:
            t = title_tag.get_text(strip=True)
            # Remove site name suffix
            return re.sub(r'\s*\|?\s*SAMA Rulebook\s*$', '', t).strip()
        return ""

    def _extract_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract document date if present."""
        # Look for date fields in the page
        date_field = soup.find("div", class_=re.compile(r"field--name-field-date|field--name-created"))
        if date_field:
            text = date_field.get_text(strip=True)
            m = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', text)
            if m:
                return f"{m.group(3)}-{m.group(2):0>2}-{m.group(1):0>2}"
        # Check for date in document number (e.g., "No. 183180000486, dated 17/11/2019")
        body_text = soup.get_text()
        m = re.search(r'dated?\s+(\d{1,2})/(\d{1,2})/(\d{4})', body_text)
        if m:
            return f"{m.group(3)}-{m.group(2):0>2}-{m.group(1):0>2}"
        return None

    # -- Link discovery ------------------------------------------------------

    def _extract_internal_links(self, soup: BeautifulSoup) -> list[str]:
        """Extract internal content links from a page."""
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            # Only internal /en/ paths
            if not href.startswith("/en/"):
                continue
            # Skip navigation, admin, search
            skip_patterns = [
                "/en/search", "/en/user", "/en/admin", "/en/terms-and-conditions",
                "/en/view-revision-updates", "/en/print/", "/en/book/export/",
            ]
            if any(href.startswith(p) for p in skip_patterns):
                continue
            # Skip anchors to same page
            if "#" in href:
                href = href.split("#")[0]
            if not href or href == "/en" or href == "/en/":
                continue
            # Normalize
            href = href.rstrip("/")
            if href not in links:
                links.append(href)
        return links

    # -- Crawling logic ------------------------------------------------------

    def _crawl_category(self, cat_id: int, cat_name: str, limit: int = 0) -> Generator[dict, None, None]:
        """BFS crawl a category page and all reachable sub-pages (max 4 levels)."""
        from collections import deque

        start_path = f"/en/book-category/{cat_id}"
        soup = self._fetch_html(start_path)
        if not soup:
            logger.warning(f"Cannot fetch category page: {cat_name} ({cat_id})")
            return

        seed_links = self._extract_internal_links(soup)
        logger.info(f"  {cat_name}: found {len(seed_links)} seed links")

        # BFS queue: (path, depth)
        queue = deque()
        for link in seed_links:
            if link not in self._visited:
                self._visited.add(link)
                queue.append((link, 0))

        yielded = 0
        while queue:
            if limit and yielded >= limit:
                return
            path, depth = queue.popleft()

            soup = self._fetch_html(path)
            if not soup:
                continue

            title = self._extract_title(soup)
            text = self._extract_text(soup)
            date = self._extract_date(soup)

            if len(text) >= MIN_TEXT_LENGTH:
                doc_id = hashlib.md5(path.encode()).hexdigest()[:12]
                yielded += 1
                yield {
                    "doc_id": doc_id,
                    "path": path,
                    "title": title,
                    "text": text,
                    "date": date,
                    "url": urljoin(BASE_URL, path),
                    "section": cat_name,
                }
                if limit and yielded >= limit:
                    return
            elif depth < 3:
                # Page is a hub — follow its links deeper
                sub_links = self._extract_internal_links(soup)
                for sub_path in sub_links:
                    if sub_path in self._visited:
                        continue
                    if "/book-category/" in sub_path:
                        continue
                    self._visited.add(sub_path)
                    queue.append((sub_path, depth + 1))

        logger.info(f"  {cat_name}: {yielded} documents extracted")

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulatory documents from all categories."""
        self._visited.clear()
        total = 0
        for cat_id, cat_name in CATEGORIES.items():
            logger.info(f"Crawling category: {cat_name} (id={cat_id})")
            for doc in self._crawl_category(cat_id, cat_name):
                total += 1
                yield doc
                if total % 50 == 0:
                    logger.info(f"Progress: {total} documents extracted")
        logger.info(f"Fetch complete: {total} total documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch all (no incremental update mechanism available)."""
        yield from self.fetch_all()

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample documents across categories."""
        self._visited.clear()
        found = 0
        for cat_id, cat_name in CATEGORIES.items():
            if found >= count:
                break
            for doc in self._crawl_category(cat_id, cat_name):
                if found >= count:
                    break
                found += 1
                logger.info(
                    f"Sample {found}/{count}: [{cat_name}] "
                    f"{doc['title'][:60]} ({len(doc['text'])} chars)"
                )
                yield doc
        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw document to standard schema."""
        doc_id = raw.get("doc_id", "unknown")
        title = raw.get("title", "Unknown")
        text = raw.get("text", "")
        section = raw.get("section", "")
        date = raw.get("date")
        url = raw.get("url", "")

        return {
            "_id": f"SA-SAMARulebook-{doc_id}",
            "_source": "SA/SAMARulebook",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "section": section,
            "language": "en",
        }

    def test_api(self) -> bool:
        """Test connectivity to rulebook.sama.gov.sa."""
        logger.info("Testing SAMA Rulebook connectivity...")

        # Test main page
        soup = self._fetch_html("/en")
        if not soup:
            logger.error("Cannot reach rulebook.sama.gov.sa")
            return False
        logger.info("Main page: OK")

        # Test a category page
        soup = self._fetch_html("/en/book-category/1361")
        if not soup:
            logger.error("Category page failed")
            return False
        links = self._extract_internal_links(soup)
        logger.info(f"Laws category: OK ({len(links)} links)")

        # Test a content page
        if links:
            test_path = links[0]
            soup = self._fetch_html(test_path)
            if soup:
                text = self._extract_text(soup)
                title = self._extract_title(soup)
                logger.info(f"Content page: OK - '{title[:60]}' ({len(text)} chars)")
                if len(text) < 100:
                    logger.warning("Content extraction yielded very little text")
            else:
                logger.error(f"Content page {test_path} failed")
                return False

        logger.info("All tests passed!")
        return True


# -- CLI ---------------------------------------------------------------------
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = SAMARulebookScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        logger.info("Running full fetch (no incremental update available)")
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_all():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
