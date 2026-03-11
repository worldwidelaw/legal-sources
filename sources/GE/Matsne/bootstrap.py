#!/usr/bin/env python3
"""
GE/Matsne -- Georgian Legislative Herald Data Fetcher

Fetches Georgian legislation from matsne.gov.ge (Legislative Herald of Georgia).

Strategy:
  - Discovery: Use RSS feed for recent documents, paginated search for full coverage
  - Full text: Extract from HTML (#maindoc div) on document view pages
  - Pagination: Search pages support ?page=N parameter

Endpoints:
  - RSS feed: https://matsne.gov.ge/en/document/feed
  - Search: https://matsne.gov.ge/en/document/search?page=N
  - Document: https://matsne.gov.ge/en/document/view/{doc_id}

Data:
  - Document types: Laws, Orders, Resolutions, Decrees, Agreements
  - Full text in Georgian, some with English translations
  - License: Open Government Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records for validation
  python bootstrap.py update             # Incremental update (recent documents)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, Set
from xml.etree import ElementTree as ET
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GE.Matsne")

# Base URL
BASE_URL = "https://matsne.gov.ge"

# Required User-Agent (site blocks basic curl)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class MatsneScraper(BaseScraper):
    """
    Scraper for GE/Matsne -- Georgian Legislative Herald.
    Country: GE
    URL: https://matsne.gov.ge

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 60) -> requests.Response:
        """Make HTTP GET request with rate limiting and error handling."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP request failed for {url}: {e}")
            raise

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace while preserving paragraph structure
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)

        # Strip leading/trailing whitespace from each line
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _parse_rss_feed(self) -> Generator[Dict[str, Any], None, None]:
        """
        Parse RSS feed to get recent document metadata.
        Yields dicts with doc_id, title, url, author, pubDate.
        """
        feed_url = f"{BASE_URL}/en/document/feed"

        try:
            resp = self._get(feed_url)
            root = ET.fromstring(resp.content)
        except Exception as e:
            logger.error(f"Failed to parse RSS feed: {e}")
            return

        # Find all items in the RSS feed
        for item in root.findall(".//item"):
            title = item.findtext("title", "").strip()
            link = item.findtext("link", "").strip()
            author = item.findtext("author", "").strip()
            pub_date = item.findtext("pubDate", "").strip()

            # Extract doc_id from link (e.g., .../document/view/31252)
            doc_id = ""
            if link:
                match = re.search(r'/document/view/(\d+)', link)
                if match:
                    doc_id = match.group(1)

            if doc_id:
                yield {
                    "doc_id": doc_id,
                    "title": title,
                    "url": link,
                    "author": author,
                    "pub_date": pub_date,
                }

    def _paginate_search(self, max_pages: Optional[int] = None) -> Generator[str, None, None]:
        """
        Paginate through search results to get all document IDs.
        Yields doc_id strings.
        """
        page = 0
        seen_ids: Set[str] = set()

        while True:
            page += 1
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}")
                return

            search_url = f"{BASE_URL}/en/document/search?page={page}"

            try:
                resp = self._get(search_url)
            except Exception as e:
                logger.error(f"Failed to fetch search page {page}: {e}")
                return

            # Parse HTML to extract document links
            soup = BeautifulSoup(resp.content, "html.parser")

            # Find all document links
            doc_links = soup.find_all("a", href=re.compile(r"/document/view/\d+"))

            if not doc_links:
                logger.info(f"No more documents on page {page}")
                return

            page_ids = set()
            for link in doc_links:
                href = link.get("href", "")
                match = re.search(r'/document/view/(\d+)', href)
                if match:
                    doc_id = match.group(1)
                    if doc_id not in seen_ids:
                        page_ids.add(doc_id)
                        seen_ids.add(doc_id)
                        yield doc_id

            if not page_ids:
                logger.info(f"No new documents on page {page}, stopping")
                return

            logger.info(f"Search page {page}: found {len(page_ids)} new document IDs")

            # Small delay between pages
            time.sleep(0.5)

    def _fetch_document_full_text(self, doc_id: str) -> Dict[str, Any]:
        """
        Fetch full document details including full text from the view page.

        Returns dict with title, text, date, url, metadata.
        """
        doc_url = f"{BASE_URL}/en/document/view/{doc_id}"

        try:
            resp = self._get(doc_url)
        except Exception as e:
            logger.warning(f"Failed to fetch document {doc_id}: {e}")
            return {}

        soup = BeautifulSoup(resp.content, "html.parser")

        result = {
            "doc_id": doc_id,
            "url": doc_url,
        }

        # Extract title from og:title or page title
        og_title = soup.find("meta", property="og:title")
        if og_title:
            result["title"] = og_title.get("content", "")
        else:
            title_tag = soup.find("title")
            if title_tag:
                result["title"] = title_tag.get_text().split("|")[0].strip()

        # Extract full text from #maindoc div
        maindoc = soup.find(id="maindoc")
        if maindoc:
            # Get all text content, preserving structure
            text_parts = []
            for elem in maindoc.find_all(["p", "div", "span", "h1", "h2", "h3", "h4", "h5", "li"]):
                text = elem.get_text(separator=" ", strip=True)
                if text:
                    text_parts.append(text)

            result["text"] = self._clean_text("\n\n".join(text_parts))
        else:
            result["text"] = ""
            logger.warning(f"Document {doc_id}: No #maindoc div found")

        # Try to extract metadata from Drupal.settings JSON
        scripts = soup.find_all("script")
        for script in scripts:
            script_text = script.get_text()
            if "Drupal.settings" in script_text:
                # Extract document_view settings
                match = re.search(r'"document_view":\s*\{([^}]+)', script_text)
                if match:
                    settings_str = "{" + match.group(1) + "}"
                    try:
                        # Parse basic fields
                        title_match = re.search(r'"title":\s*"([^"]*)"', script_text)
                        if title_match:
                            result["title_georgian"] = title_match.group(1)

                        doc_id_match = re.search(r'"documentId":\s*(\d+)', script_text)
                        if doc_id_match:
                            result["document_id_verified"] = doc_id_match.group(1)
                    except Exception:
                        pass

        # Extract issuer (author) from metadata
        author_meta = soup.find("meta", attrs={"name": "author"})
        if author_meta:
            result["issuer"] = author_meta.get("content", "")

        # Extract publication info from page content
        # Look for document info sections
        info_section = soup.find(class_="document-info") or soup.find(class_="doc-info")
        if info_section:
            info_text = info_section.get_text()
            # Try to extract date
            date_match = re.search(r'(\d{1,2}[./]\d{1,2}[./]\d{4})', info_text)
            if date_match:
                date_str = date_match.group(1)
                # Convert to ISO format
                try:
                    parts = re.split(r'[./]', date_str)
                    if len(parts) == 3:
                        day, month, year = parts
                        result["date"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except Exception:
                    pass

        # If no date found, try to extract from document number/registration info
        if "date" not in result:
            # Look for publication date pattern in text
            pub_pattern = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', result.get("text", "")[:500])
            if pub_pattern:
                day, month, year = pub_pattern.groups()
                result["date"] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        return result

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from Matsne.
        Uses paginated search to discover all document IDs, then fetches full text.
        """
        logger.info("Starting full fetch from GE/Matsne")

        # First, get recent documents from RSS for quick validation
        logger.info("Fetching RSS feed for recent documents...")
        rss_count = 0
        for rss_item in self._parse_rss_feed():
            rss_count += 1
            yield rss_item
        logger.info(f"RSS feed yielded {rss_count} document references")

        # Then paginate through search for comprehensive coverage
        logger.info("Paginating search for full document discovery...")
        search_count = 0
        for doc_id in self._paginate_search(max_pages=1000):
            search_count += 1
            yield {"doc_id": doc_id}

            # Log progress periodically
            if search_count % 100 == 0:
                logger.info(f"Search discovery: {search_count} document IDs found")

        logger.info(f"Search pagination completed: {search_count} additional document IDs")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.
        Uses RSS feed which is ordered by most recent first.
        """
        logger.info(f"Fetching updates since {since}")

        for rss_item in self._parse_rss_feed():
            pub_date_str = rss_item.get("pub_date", "")
            if pub_date_str:
                try:
                    # Parse RSS date format: "Thu, 26 Jun 2025 04:00:00 +04"
                    pub_date = datetime.strptime(
                        pub_date_str.rsplit(" ", 1)[0],
                        "%a, %d %b %Y %H:%M:%S"
                    )
                    pub_date = pub_date.replace(tzinfo=timezone.utc)

                    if pub_date < since:
                        logger.info(f"Reached documents older than {since}, stopping")
                        return
                except ValueError as e:
                    logger.warning(f"Failed to parse date {pub_date_str}: {e}")

            yield rss_item

        # Also check a few pages of search for recent additions
        for doc_id in self._paginate_search(max_pages=5):
            yield {"doc_id": doc_id}

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from document view page.
        """
        doc_id = raw.get("doc_id", "")

        if not doc_id:
            raise ValueError("No doc_id in raw data")

        # Fetch full document details with text
        doc_data = self._fetch_document_full_text(doc_id)

        if not doc_data:
            raise ValueError(f"Failed to fetch document {doc_id}")

        full_text = doc_data.get("text", "")
        if not full_text:
            logger.warning(f"Document {doc_id}: Empty full text")

        # Build title from document data or raw RSS data
        title = doc_data.get("title", "") or raw.get("title", "")

        # Build date from document data or raw RSS data
        date = doc_data.get("date", "")
        if not date and raw.get("pub_date"):
            try:
                pub_date_str = raw.get("pub_date", "")
                pub_date = datetime.strptime(
                    pub_date_str.rsplit(" ", 1)[0],
                    "%a, %d %b %Y %H:%M:%S"
                )
                date = pub_date.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Build issuer from document data or raw RSS data
        issuer = doc_data.get("issuer", "") or raw.get("author", "")

        return {
            # Required base fields
            "_id": f"GE/Matsne/{doc_id}",
            "_source": "GE/Matsne",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": doc_data.get("url", f"{BASE_URL}/en/document/view/{doc_id}"),
            # Additional metadata
            "doc_id": doc_id,
            "issuer": issuer,
            "title_georgian": doc_data.get("title_georgian", ""),
            "language": "en",  # English version
            "country": "GE",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing GE/Matsne connectivity...")

        # Test RSS feed
        print("\n1. Testing RSS feed...")
        try:
            feed_url = f"{BASE_URL}/en/document/feed"
            resp = self._get(feed_url)
            root = ET.fromstring(resp.content)
            items = root.findall(".//item")
            print(f"   RSS feed status: OK ({len(items)} items)")

            if items:
                first_item = items[0]
                title = first_item.findtext("title", "")[:60]
                link = first_item.findtext("link", "")
                print(f"   Sample: {title}...")
                print(f"   Link: {link}")
        except Exception as e:
            print(f"   RSS feed ERROR: {e}")
            return

        # Test search page
        print("\n2. Testing search page...")
        try:
            search_url = f"{BASE_URL}/en/document/search"
            resp = self._get(search_url)
            soup = BeautifulSoup(resp.content, "html.parser")
            doc_links = soup.find_all("a", href=re.compile(r"/document/view/\d+"))
            print(f"   Search page status: OK ({len(doc_links)} document links)")
        except Exception as e:
            print(f"   Search page ERROR: {e}")
            return

        # Test document view (full text extraction)
        print("\n3. Testing document full text extraction...")
        try:
            # Get first doc_id from RSS
            first_doc_id = None
            for item in self._parse_rss_feed():
                first_doc_id = item.get("doc_id")
                if first_doc_id:
                    break

            if first_doc_id:
                print(f"   Testing document: {first_doc_id}")
                doc_data = self._fetch_document_full_text(first_doc_id)

                if doc_data:
                    text = doc_data.get("text", "")
                    print(f"   Title: {doc_data.get('title', 'N/A')[:60]}...")
                    print(f"   Full text length: {len(text)} characters")
                    if text:
                        print(f"   Text preview: {text[:200]}...")
                else:
                    print("   ERROR: No document data returned")
            else:
                print("   ERROR: No document ID found in RSS")
        except Exception as e:
            print(f"   Document fetch ERROR: {e}")

        print("\nConnectivity test complete!")


def main():
    scraper = MatsneScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
