#!/usr/bin/env python3
"""
AT/FMA -- Austrian Financial Market Authority (Finanzmarktaufsicht) Fetcher

Fetches regulatory decisions, sanctions, and warnings from the FMA RSS feed.

Strategy:
  - Bootstrap: Paginates through all RSS feed pages (~380 pages, 9 items each).
  - Update: Fetches recent pages (1-5) for new items.
  - Sample: Fetches 10+ records for validation.

Data Source:
  - RSS Feed: https://www.fma.gv.at/feed/
  - Categories: Sanktion, Warnung, Pressemitteilung, Information, Publikation
  - Full text available via content:encoded in RSS items

Note: The FMA website is protected by Cloudflare, but the RSS feed is accessible.
Category-specific feeds (e.g., /category/news/sanktion/feed/) are blocked,
so we filter the main feed by category.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test-api           # Quick RSS feed connectivity test
"""

import sys
import json
import logging
import re
import html
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from email.utils import parsedate_to_datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AT.FMA")

# FMA RSS feed URL
RSS_FEED_URL = "https://www.fma.gv.at/feed/"

# Categories of interest for regulatory decisions
REGULATORY_CATEGORIES = {"Sanktion", "Warnung"}


class FMAScraper(BaseScraper):
    """
    Scraper for AT/FMA -- Austrian Financial Market Authority.
    Country: AT
    URL: https://www.fma.gv.at

    Data types: regulatory_decisions, sanctions, warnings
    Auth: none (Open Government Data via RSS)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/rss+xml, application/xml, text/xml",
            },
            timeout=60,
        )

    # -- RSS parsing helpers ------------------------------------------------

    def _fetch_rss_page(self, page: int = 1) -> str:
        """Fetch a single RSS feed page."""
        url = RSS_FEED_URL
        if page > 1:
            url = f"{RSS_FEED_URL}?paged={page}"

        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.text

    def _parse_rss_items(self, xml_content: str) -> list:
        """Parse RSS XML and return list of item dicts."""
        items = []

        # Parse XML
        try:
            # Define namespaces
            namespaces = {
                "content": "http://purl.org/rss/1.0/modules/content/",
                "dc": "http://purl.org/dc/elements/1.1/",
                "atom": "http://www.w3.org/2005/Atom",
            }

            root = ET.fromstring(xml_content)
            channel = root.find("channel")
            if channel is None:
                return items

            for item in channel.findall("item"):
                item_dict = {}

                # Basic fields
                title_el = item.find("title")
                item_dict["title"] = title_el.text if title_el is not None else ""

                link_el = item.find("link")
                item_dict["link"] = link_el.text if link_el is not None else ""

                pubdate_el = item.find("pubDate")
                item_dict["pubDate"] = pubdate_el.text if pubdate_el is not None else ""

                guid_el = item.find("guid")
                item_dict["guid"] = guid_el.text if guid_el is not None else ""

                # Category
                category_el = item.find("category")
                item_dict["category"] = category_el.text if category_el is not None else ""

                # Description (short summary)
                desc_el = item.find("description")
                item_dict["description"] = desc_el.text if desc_el is not None else ""

                # Full content (content:encoded)
                content_el = item.find("content:encoded", namespaces)
                item_dict["content_encoded"] = content_el.text if content_el is not None else ""

                # Creator (dc:creator)
                creator_el = item.find("dc:creator", namespaces)
                item_dict["creator"] = creator_el.text if creator_el is not None else ""

                items.append(item_dict)

        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")

        return items

    def _paginate(self, max_pages: int = None, categories: set = None):
        """
        Generator that paginates through all RSS feed pages.

        Yields individual items (raw dicts from RSS parsing).

        Args:
            max_pages: Maximum number of pages to fetch (None = all)
            categories: Set of category names to include (None = all)
        """
        page = 1

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}")
                break

            try:
                xml_content = self._fetch_rss_page(page)
                items = self._parse_rss_items(xml_content)

                if not items:
                    logger.info(f"No items on page {page}, stopping")
                    break

                for item in items:
                    # Filter by category if specified
                    if categories:
                        item_category = item.get("category", "")
                        if item_category not in categories:
                            continue
                    yield item

                logger.info(f"Page {page}: {len(items)} items")
                page += 1

            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML content to plain text."""
        if not html_content:
            return ""

        # Handle CDATA wrapper
        text = html_content
        if text.startswith("<![CDATA["):
            text = text[9:]
        if text.endswith("]]>"):
            text = text[:-3]

        # Remove XML encoding declaration if present
        text = re.sub(r'<\?xml[^>]*\?>', '', text)

        # Remove script and style tags
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)

        # Convert common block elements to newlines
        text = re.sub(r"</?(p|div|br|tr|li|h[1-6])[^>]*>", "\n", text, flags=re.IGNORECASE)

        # Remove remaining HTML tags
        text = re.sub(r"<[^>]+>", " ", text)

        # Decode HTML entities
        text = html.unescape(text)

        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()

        return text

    def _parse_date(self, date_str: str) -> str:
        """Parse RFC 2822 date to ISO 8601 format."""
        if not date_str:
            return ""
        try:
            dt = parsedate_to_datetime(date_str)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all FMA RSS items.

        Fetches all pages from the RSS feed.
        """
        logger.info("Fetching all FMA regulatory items from RSS feed")
        # Fetch all pages, no category filter (get everything)
        for item in self._paginate():
            yield item

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield FMA items published recently.

        Fetches only the first few pages of the RSS feed.
        """
        logger.info(f"Fetching FMA updates since {since.isoformat()}")

        # Fetch first 5 pages only
        for item in self._paginate(max_pages=5):
            yield item

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw RSS item into standard schema.

        CRITICAL: Extracts FULL TEXT from content:encoded field.
        """
        # Generate unique ID from GUID or link
        guid = raw.get("guid", "")
        link = raw.get("link", "")
        doc_id = guid or link

        if not doc_id:
            # Fallback: hash the title + date
            title_hash = hash(raw.get("title", "") + raw.get("pubDate", ""))
            doc_id = f"fma-{abs(title_hash)}"

        # Extract post ID from GUID if available (format: https://www.fma.gv.at/?p=31242865)
        post_id_match = re.search(r'\?p=(\d+)', guid)
        post_id = post_id_match.group(1) if post_id_match else ""

        # Clean unique ID
        if doc_id.startswith("https://www.fma.gv.at/?p="):
            doc_id = f"FMA-{post_id}"
        elif doc_id.startswith("https://www.fma.gv.at/"):
            # Extract slug from URL
            slug = doc_id.replace("https://www.fma.gv.at/", "").strip("/")
            doc_id = f"FMA-{slug[:80]}"

        # Get full text from content:encoded
        content_encoded = raw.get("content_encoded", "")
        full_text = self._clean_html(content_encoded)

        # If content:encoded is empty, try description
        if not full_text:
            description = raw.get("description", "")
            full_text = self._clean_html(description)

        # Parse date
        date = self._parse_date(raw.get("pubDate", ""))

        # Determine document type based on category
        # FMA regulatory decisions (sanctions, warnings) are administrative decisions
        category = raw.get("category", "")
        doc_type = "case_law"  # FMA regulatory decisions, sanctions, warnings

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "AT/FMA",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": self._clean_html(raw.get("title", "")),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": link,
            # FMA-specific fields
            "guid": guid,
            "post_id": post_id,
            "category": category,
            "creator": raw.get("creator", ""),
            "description": self._clean_html(raw.get("description", "")),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick RSS feed connectivity test."""
        print("Testing FMA RSS feed...")

        # Fetch first page
        xml_content = self._fetch_rss_page(1)
        items = self._parse_rss_items(xml_content)

        print(f"  Page 1: {len(items)} items")

        if items:
            # Show first item details
            first = items[0]
            print(f"\n  First item:")
            print(f"    Title: {first.get('title', '')[:60]}...")
            print(f"    Date: {first.get('pubDate', '')}")
            print(f"    Category: {first.get('category', '')}")
            print(f"    Link: {first.get('link', '')}")

            # Check content:encoded
            content = first.get("content_encoded", "")
            if content:
                text = self._clean_html(content)
                print(f"    Content length: {len(text)} chars")
                print(f"    Content preview: {text[:150]}...")
            else:
                print("    Content: (empty)")

            # Count categories
            categories = {}
            for item in items:
                cat = item.get("category", "Unknown")
                categories[cat] = categories.get(cat, 0) + 1
            print(f"\n  Categories on page 1: {categories}")

        # Test pagination
        print("\n  Testing pagination...")
        page2_content = self._fetch_rss_page(2)
        page2_items = self._parse_rss_items(page2_content)
        print(f"    Page 2: {len(page2_items)} items")

        print("\nRSS feed test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = FMAScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

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
