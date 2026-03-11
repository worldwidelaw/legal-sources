#!/usr/bin/env python3
"""
LV/LegislativeDatabase -- Latvian Legislation Database (Likumi.lv) Data Fetcher

Fetches Latvian legislation from the official likumi.lv portal.

Strategy:
  - Use the sitemap at likumi.lv/sitemap.xml to discover all document IDs
  - Fetch individual documents at /doc.php?id={ID}
  - Extract full text from HTML content (P tags)
  - RSS feeds available for updates (recent documents)

Endpoints:
  - Sitemap: https://likumi.lv/sitemap.xml
  - Document: https://likumi.lv/doc.php?id={ID}
  - RSS feeds: https://likumi.lv/rss/{category}.xml

Data:
  - Document types: Likumi (Laws), Noteikumi (Regulations), Rīkojumi (Orders), etc.
  - Full text in Latvian (HTML parsed)
  - Maintained by Latvijas Vēstnesis (official publisher)

Usage:
  python bootstrap.py bootstrap          # Full initial pull (from sitemap)
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (from RSS)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urlparse, parse_qs

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LV.legislativedatabase")

# Base URLs
BASE_URL = "https://likumi.lv"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
DOC_URL = f"{BASE_URL}/doc.php"

# RSS feeds for different document types
RSS_FEEDS = {
    "likumi": f"{BASE_URL}/rss/likumi.xml",          # Laws
    "mk_not": f"{BASE_URL}/rss/mk_not.xml",          # Cabinet regulations
    "mk_instr": f"{BASE_URL}/rss/mk_instr.xml",      # Cabinet instructions
    "mk_rik": f"{BASE_URL}/rss/mk_rik.xml",          # Cabinet orders
    "visi_ta": f"{BASE_URL}/rss/visi_ta.xml",        # All legal acts
    "st_nolemumi": f"{BASE_URL}/rss/st_nolemumi.xml", # Constitutional Court decisions
}

# Headers for requests
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


class LegislativeDatabaseScraper(BaseScraper):
    """
    Scraper for LV/LegislativeDatabase -- Latvian Legislation (Likumi.lv).
    Country: LV
    URL: https://likumi.lv

    Data types: legislation
    Auth: none (public)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content extracted from HTML."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace while preserving paragraph structure
        text = re.sub(r'\r\n', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)

        # Replace non-breaking spaces
        text = text.replace('\xa0', ' ')

        # Strip leading/trailing whitespace from each line
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _extract_text_from_html(self, html_content: str) -> str:
        """
        Extract full text content from likumi.lv HTML page.

        The document text is contained in <P> tags within the document body.
        """
        if not html_content:
            return ""

        # Find all P tag content
        p_matches = re.findall(r'<P[^>]*>(.*?)</P>', html_content, re.DOTALL | re.IGNORECASE)

        if not p_matches:
            # Try lowercase p tags as fallback
            p_matches = re.findall(r'<p[^>]*>(.*?)</p>', html_content, re.DOTALL | re.IGNORECASE)

        if not p_matches:
            return ""

        # Clean each paragraph and join
        paragraphs = []
        for p in p_matches:
            # Strip HTML tags within paragraph
            clean_p = re.sub(r'<[^>]+>', ' ', p)
            clean_p = self._clean_text(clean_p)
            if clean_p:
                paragraphs.append(clean_p)

        return '\n\n'.join(paragraphs)

    def _extract_title_from_html(self, html_content: str) -> str:
        """Extract title from HTML meta tags or title element."""
        # Try og:title first (more reliable)
        match = re.search(r"<meta[^>]*property=['\"]og:title['\"][^>]*content=['\"]([^'\"]+)['\"]", html_content)
        if match:
            return html.unescape(match.group(1))

        # Fall back to title element
        match = re.search(r'<title>([^<]+)</title>', html_content)
        if match:
            return html.unescape(match.group(1))

        return ""

    def _extract_metadata_from_html(self, html_content: str, doc_id: str) -> dict:
        """Extract metadata like document type, dates, issuer from HTML."""
        metadata = {
            "doc_number": "",
            "doc_type": "",
            "issuer": "",
            "adopted_date": "",
            "effective_date": "",
            "expires_date": "",
            "published_date": "",
            "publication_ref": "",
        }

        # Document number pattern: "Nr. X" or "Nr.X"
        match = re.search(r'Numurs:\s*</font>([^<]+)', html_content)
        if match:
            metadata["doc_number"] = match.group(1).strip()

        # Document type from breadcrumb or class
        match = re.search(r'<a[^>]*href=[\'"]?/ta/veids/[^/]+/([^\'"\s>]+)[\'"]?', html_content)
        if match:
            metadata["doc_type"] = match.group(1)

        # Issuer
        issuer_patterns = [
            r'<b>Ministru kabineta',
            r'<b>Saeima',
            r'<b>Valsts prezidenta',
            r'Latvijas Banka',
            r'FKTK',
        ]
        for pattern in issuer_patterns:
            if re.search(pattern, html_content, re.IGNORECASE):
                # Extract just the issuer name
                if 'Ministru kabineta' in pattern:
                    metadata["issuer"] = "Ministru kabinets"
                elif 'Saeima' in pattern:
                    metadata["issuer"] = "Saeima"
                elif 'Valsts prezidenta' in pattern:
                    metadata["issuer"] = "Valsts prezidents"
                break

        # Dates
        # Adopted date: "Pieņemts: DD.MM.YYYY."
        match = re.search(r'Pieņemts:\s*</font>(\d{2}\.\d{2}\.\d{4})', html_content)
        if match:
            try:
                date_str = match.group(1)
                # Convert DD.MM.YYYY to YYYY-MM-DD
                parts = date_str.split('.')
                if len(parts) == 3:
                    metadata["adopted_date"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                pass

        # Effective date: "Stājas spēkā: DD.MM.YYYY."
        match = re.search(r'Stājas spēkā:\s*</font><a[^>]*>(\d{2}\.\d{2}\.\d{4})', html_content)
        if match:
            try:
                date_str = match.group(1)
                parts = date_str.split('.')
                if len(parts) == 3:
                    metadata["effective_date"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                pass

        # Expiration date: "Zaudē spēku: DD.MM.YYYY."
        match = re.search(r'Zaudē spēku:\s*</font><a[^>]*>(\d{2}\.\d{2}\.\d{4})', html_content)
        if match:
            try:
                date_str = match.group(1)
                parts = date_str.split('.')
                if len(parts) == 3:
                    metadata["expires_date"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                pass

        # Published: "Publicēts: Latvijas Vēstnesis, XX, DD.MM.YYYY."
        match = re.search(r'Publicēts:\s*</font><a[^>]*>Latvijas Vēstnesis</a>,\s*(\d+[A-Z]?),\s*(\d{2}\.\d{2}\.\d{4})', html_content)
        if match:
            metadata["publication_ref"] = f"LV {match.group(1)}"
            try:
                date_str = match.group(2)
                parts = date_str.split('.')
                if len(parts) == 3:
                    metadata["published_date"] = f"{parts[2]}-{parts[1]}-{parts[0]}"
            except:
                pass

        return metadata

    def _fetch_document(self, doc_id: str) -> Optional[dict]:
        """
        Fetch a single document by ID.

        Returns raw dict with HTML content and extracted fields.
        """
        url = f"{DOC_URL}?id={doc_id}"

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()

            html_content = resp.text

            # Extract title
            title = self._extract_title_from_html(html_content)

            # Extract full text
            full_text = self._extract_text_from_html(html_content)

            # Extract metadata
            metadata = self._extract_metadata_from_html(html_content, doc_id)

            return {
                "doc_id": doc_id,
                "url": url,
                "title": title,
                "full_text": full_text,
                "html_content": html_content,
                **metadata,
            }

        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch document {doc_id}: {e}")
            return None

    def _parse_sitemap(self, max_docs: Optional[int] = None) -> Generator[str, None, None]:
        """
        Parse the sitemap to extract document IDs.

        Yields document IDs from URLs like /ta/id/XXXXX-...
        """
        try:
            logger.info("Fetching sitemap...")
            self.rate_limiter.wait()
            resp = self.session.get(SITEMAP_URL, timeout=120)
            resp.raise_for_status()

            # Parse XML
            root = ET.fromstring(resp.content)

            # Handle namespace
            ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

            count = 0
            for url_elem in root.findall('.//ns:url/ns:loc', ns):
                url = url_elem.text
                if url and '/ta/id/' in url:
                    # Extract ID from URL like /ta/id/12345-...
                    match = re.search(r'/ta/id/(\d+)', url)
                    if match:
                        count += 1
                        yield match.group(1)

                        if max_docs and count >= max_docs:
                            logger.info(f"Reached max_docs limit ({max_docs})")
                            return

            logger.info(f"Parsed {count} document IDs from sitemap")

        except Exception as e:
            logger.error(f"Failed to parse sitemap: {e}")
            raise

    def _parse_rss_feed(self, feed_url: str) -> List[dict]:
        """
        Parse an RSS feed and return list of document info.

        Returns list of dicts with doc_id, title, pubDate.
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(feed_url, timeout=30)
            resp.raise_for_status()

            root = ET.fromstring(resp.content)
            items = []

            for item in root.findall('.//item'):
                title_elem = item.find('title')
                link_elem = item.find('link')
                guid_elem = item.find('guid')
                pubdate_elem = item.find('pubDate')

                doc_id = None
                if guid_elem is not None and guid_elem.text:
                    doc_id = guid_elem.text
                elif link_elem is not None and link_elem.text:
                    # Extract from URL
                    match = re.search(r'id=(\d+)', link_elem.text)
                    if match:
                        doc_id = match.group(1)

                if doc_id:
                    items.append({
                        "doc_id": doc_id,
                        "title": title_elem.text if title_elem is not None else "",
                        "pubDate": pubdate_elem.text if pubdate_elem is not None else "",
                    })

            return items

        except Exception as e:
            logger.warning(f"Failed to parse RSS feed {feed_url}: {e}")
            return []

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from likumi.lv.

        Uses sitemap for document discovery.
        """
        doc_count = 0

        for doc_id in self._parse_sitemap():
            raw = self._fetch_document(doc_id)
            if raw and raw.get("full_text"):
                doc_count += 1
                yield raw

                # Log progress every 100 docs
                if doc_count % 100 == 0:
                    logger.info(f"Fetched {doc_count} documents with full text")

            # Safety limit for full bootstrap
            if doc_count >= 100000:
                logger.warning("Reached document limit (100000), stopping")
                break

        logger.info(f"Total documents fetched with full text: {doc_count}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published recently via RSS feeds.

        Checks multiple RSS feeds for new documents.
        """
        seen_ids = set()

        for feed_name, feed_url in RSS_FEEDS.items():
            logger.info(f"Checking RSS feed: {feed_name}")

            items = self._parse_rss_feed(feed_url)

            for item in items:
                doc_id = item.get("doc_id")
                if doc_id and doc_id not in seen_ids:
                    seen_ids.add(doc_id)

                    raw = self._fetch_document(doc_id)
                    if raw and raw.get("full_text"):
                        yield raw

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", "")
        full_text = self._clean_text(raw.get("full_text", ""))
        url = raw.get("url", f"{DOC_URL}?id={doc_id}")

        # Use adopted date as primary, fallback to effective, then published
        date = (
            raw.get("adopted_date") or
            raw.get("effective_date") or
            raw.get("published_date") or
            ""
        )

        # Determine document type
        doc_type_map = {
            "likumi": "Likums",
            "noteikumi": "Noteikumi",
            "rikojumi": "Rīkojums",
            "instrukcijas": "Instrukcija",
        }
        doc_type_raw = raw.get("doc_type", "").lower()
        doc_type = doc_type_map.get(doc_type_raw, raw.get("doc_type", ""))

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "LV/LegislativeDatabase",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "doc_number": raw.get("doc_number", ""),
            "document_type": doc_type,
            "issuer": raw.get("issuer", ""),
            "adopted_date": raw.get("adopted_date", ""),
            "effective_date": raw.get("effective_date", ""),
            "expires_date": raw.get("expires_date", ""),
            "published_date": raw.get("published_date", ""),
            "publication_ref": raw.get("publication_ref", ""),
            "language": "lv",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing LV/LegislativeDatabase endpoints...")

        # Test RSS feed
        print("\n1. Testing RSS feed...")
        try:
            items = self._parse_rss_feed(RSS_FEEDS["likumi"])
            print(f"   RSS feed returned {len(items)} items")
            if items:
                print(f"   Sample: {items[0].get('title', 'N/A')[:60]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test document fetch
        print("\n2. Testing document fetch...")
        try:
            # Fetch a recent law (high ID)
            rss_items = self._parse_rss_feed(RSS_FEEDS["likumi"])
            if rss_items:
                test_id = rss_items[0].get("doc_id")
                print(f"   Fetching document {test_id}...")

                doc = self._fetch_document(test_id)
                if doc:
                    print(f"   Title: {doc.get('title', 'N/A')[:60]}...")
                    print(f"   Full text length: {len(doc.get('full_text', ''))} characters")
                    print(f"   Issuer: {doc.get('issuer', 'N/A')}")
                    print(f"   Adopted: {doc.get('adopted_date', 'N/A')}")

                    if doc.get('full_text'):
                        print(f"   Text preview: {doc['full_text'][:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test sitemap (just count)
        print("\n3. Testing sitemap...")
        try:
            count = 0
            for _ in self._parse_sitemap(max_docs=10):
                count += 1
            print(f"   Sitemap accessible, sampled {count} document IDs")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = LegislativeDatabaseScraper()

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
