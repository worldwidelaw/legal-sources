#!/usr/bin/env python3
"""
AZ/Qanun -- Azerbaijan Legislation Database (e-Qanun)

Fetches Azerbaijani legislation from e-qanun.az, the official legal database
maintained by the Ministry of Justice of the Republic of Azerbaijan.

Strategy:
  - Parse sitemap.xml to get all framework document IDs
  - For each ID, fetch metadata from REST API: api.e-qanun.az/framework/{id}
  - Fetch full text HTML from the URL provided in API response (htmlUrl field)
  - Full text HTML is Word-exported HTML, requires text extraction

Endpoints:
  - Sitemap: https://e-qanun.az/sitemap.xml
  - Metadata API: https://api.e-qanun.az/framework/{id}
  - Full text HTML: https://e-qanun.az/frameworks/{bucket}/f_{id}.html (from htmlUrl)

Data:
  - Document types: Laws (Qanunlar), Presidential Decrees (Fərmanlar),
    Cabinet Decisions (Qərarlar), Orders, etc.
  - 55,000+ documents (1992-present)
  - Full text in Azerbaijani
  - License: Public government data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent documents)
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
from html.parser import HTMLParser

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AZ.qanun")

# URLs
SITEMAP_URL = "https://e-qanun.az/sitemap.xml"
API_URL = "https://api.e-qanun.az/framework/{id}"

# Request headers
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "az,en;q=0.9",
    "Referer": "https://e-qanun.az/",
}


class WordHTMLTextExtractor(HTMLParser):
    """Extract text content from Microsoft Word exported HTML."""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.in_body = False
        self.in_script = False
        self.in_style = False
        self.skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag == "body":
            self.in_body = True
        elif tag == "script":
            self.in_script = True
        elif tag == "style":
            self.in_style = True
        # Skip Word-specific markup
        elif tag.startswith("o:") or tag.startswith("w:") or tag.startswith("m:"):
            self.skip_depth += 1

    def handle_endtag(self, tag):
        if tag == "script":
            self.in_script = False
        elif tag == "style":
            self.in_style = False
        elif tag.startswith("o:") or tag.startswith("w:") or tag.startswith("m:"):
            self.skip_depth = max(0, self.skip_depth - 1)
        # Add line breaks for block elements
        elif tag in ("p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr"):
            if self.text_parts and not self.text_parts[-1].endswith("\n"):
                self.text_parts.append("\n")

    def handle_data(self, data):
        if self.in_body and not self.in_script and not self.in_style and self.skip_depth == 0:
            text = data.strip()
            if text:
                self.text_parts.append(text)

    def get_text(self) -> str:
        return " ".join(self.text_parts)


class QanunScraper(BaseScraper):
    """
    Scraper for AZ/Qanun -- Azerbaijan Legislation Database (e-Qanun).
    Country: AZ
    URL: https://e-qanun.az

    Data types: legislation
    Auth: none (public government data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._framework_ids = None

    def _parse_sitemap(self) -> List[int]:
        """Parse sitemap.xml to get all framework IDs."""
        if self._framework_ids is not None:
            return self._framework_ids

        logger.info("Fetching sitemap from e-qanun.az...")
        resp = self.session.get(SITEMAP_URL, timeout=60)
        resp.raise_for_status()

        # Parse XML
        root = ET.fromstring(resp.content)

        # Extract framework IDs from URLs
        ids = []
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        for url_elem in root.findall(".//sm:url/sm:loc", ns):
            url = url_elem.text
            if url and "/framework/" in url:
                # Extract ID from URL like https://www.e-qanun.az/framework/12345
                match = re.search(r"/framework/(\d+)", url)
                if match:
                    ids.append(int(match.group(1)))

        # Sort by ID (most recent documents typically have higher IDs)
        ids.sort(reverse=True)

        self._framework_ids = ids
        logger.info(f"Found {len(ids)} framework documents in sitemap")
        return ids

    def _clean_text(self, text: str) -> str:
        """Clean and normalize text content."""
        if not text:
            return ""

        # Decode HTML entities
        text = html.unescape(text)

        # Remove excessive whitespace while preserving paragraph structure
        text = re.sub(r"\r\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)

        # Strip leading/trailing whitespace from each line
        lines = [line.strip() for line in text.split("\n")]
        text = "\n".join(lines)

        return text.strip()

    def _fetch_metadata(self, framework_id: int) -> Optional[Dict[str, Any]]:
        """Fetch document metadata from the API."""
        url = API_URL.format(id=framework_id)

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)

            if resp.status_code == 404:
                return None
            resp.raise_for_status()

            data = resp.json()
            return data.get("data", {})

        except Exception as e:
            logger.debug(f"Failed to fetch metadata for ID {framework_id}: {e}")
            return None

    def _fetch_full_text(self, html_url: str) -> Optional[str]:
        """Fetch and extract full text from the HTML URL."""
        if not html_url:
            return None

        try:
            self.rate_limiter.wait()
            resp = self.session.get(html_url, timeout=30)

            if resp.status_code == 403:
                logger.debug(f"403 Forbidden for {html_url}")
                return None
            resp.raise_for_status()

            # Handle different encodings (some files use windows-1251)
            content = resp.content

            # Try to detect encoding from meta tag
            if b"windows-1251" in content.lower():
                html_content = content.decode("windows-1251", errors="replace")
            else:
                html_content = content.decode("utf-8", errors="replace")

            # Parse HTML and extract text
            parser = WordHTMLTextExtractor()
            parser.feed(html_content)
            text = parser.get_text()

            if len(text) > 50:  # Sanity check - should have meaningful content
                return self._clean_text(text)

            return None

        except Exception as e:
            logger.debug(f"Failed to fetch full text from {html_url}: {e}")
            return None

    def _fetch_document(self, framework_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single document with metadata and full text."""
        metadata = self._fetch_metadata(framework_id)
        if not metadata:
            return None

        html_url = metadata.get("htmlUrl", "")
        full_text = self._fetch_full_text(html_url)

        if not full_text:
            logger.debug(f"No full text for ID {framework_id}, skipping")
            return None

        # Combine metadata and full text
        metadata["full_text"] = full_text
        metadata["framework_id"] = framework_id

        return metadata

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from e-Qanun.

        Iterates through all framework IDs from sitemap and fetches each.
        """
        framework_ids = self._parse_sitemap()
        total_count = 0
        success_count = 0

        for framework_id in framework_ids:
            total_count += 1

            doc = self._fetch_document(framework_id)
            if doc:
                success_count += 1
                yield doc

            if total_count % 100 == 0:
                logger.info(f"Processed {total_count}/{len(framework_ids)} documents ({success_count} with full text)...")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Note: We iterate through recent IDs (higher IDs are newer).
        """
        framework_ids = self._parse_sitemap()

        for framework_id in framework_ids:
            doc = self._fetch_document(framework_id)
            if not doc:
                continue

            # Check accept date
            accept_date_str = doc.get("requisite", {}).get("acceptDate", "")
            if accept_date_str:
                try:
                    # Parse DD.MM.YYYY format
                    parts = accept_date_str.split(".")
                    if len(parts) == 3:
                        day, month, year = parts
                        doc_date = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
                        if doc_date < since:
                            continue
                except (ValueError, IndexError):
                    pass

            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        requisite = raw.get("requisite", {})
        framework_id = raw.get("framework_id") or raw.get("id", "")
        title = requisite.get("title", "")
        full_text = raw.get("full_text", "")

        # Parse Azerbaijani date format (DD.MM.YYYY) to ISO 8601
        def parse_date(date_str: str) -> str:
            if not date_str:
                return ""
            try:
                parts = date_str.split(".")
                if len(parts) == 3:
                    day, month, year = parts
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except (ValueError, IndexError):
                pass
            return ""

        accept_date = parse_date(requisite.get("acceptDate", ""))
        effect_date = parse_date(requisite.get("effectDate", ""))
        register_date = parse_date(requisite.get("registerDate", ""))

        # Use accept date as primary date
        date = accept_date or effect_date or register_date

        # Build URL
        url = f"https://e-qanun.az/framework/{framework_id}" if framework_id else ""

        return {
            # Required base fields
            "_id": str(framework_id),
            "_source": "AZ/Qanun",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Additional metadata
            "framework_id": str(framework_id),
            "citation": requisite.get("citation", ""),
            "type_name": requisite.get("typeName", ""),
            "status_name": requisite.get("statusName", ""),
            "accept_date": accept_date,
            "effect_date": effect_date,
            "register_date": register_date,
            "register_code": requisite.get("registerCode", ""),
            "class_codes": requisite.get("classCodes", []),
            "language": "AZ",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing AZ/Qanun endpoints...")

        # Test sitemap
        print("\n1. Testing sitemap endpoint...")
        try:
            resp = self.session.get(SITEMAP_URL, timeout=30)
            print(f"   Status: {resp.status_code}")
            ids = self._parse_sitemap()
            print(f"   Found {len(ids)} framework IDs")
            print(f"   Sample IDs: {ids[:5]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test API endpoint
        print("\n2. Testing metadata API...")
        test_id = 1  # Known good document
        try:
            metadata = self._fetch_metadata(test_id)
            if metadata:
                print(f"   Success! Document ID: {metadata.get('id')}")
                requisite = metadata.get("requisite", {})
                print(f"   Title: {requisite.get('title', 'N/A')[:60]}...")
                print(f"   Type: {requisite.get('typeName', 'N/A')}")
                print(f"   Status: {requisite.get('statusName', 'N/A')}")
                print(f"   HTML URL: {metadata.get('htmlUrl', 'N/A')}")
            else:
                print("   No metadata returned")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test full text endpoint
        print("\n3. Testing full text HTML endpoint...")
        try:
            metadata = self._fetch_metadata(test_id)
            if metadata:
                html_url = metadata.get("htmlUrl", "")
                text = self._fetch_full_text(html_url)
                if text:
                    print(f"   Success! Got {len(text)} characters")
                    print(f"   Preview: {text[:200]}...")
                else:
                    print("   Failed to extract text from HTML")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test a recent document
        print("\n4. Testing recent document...")
        try:
            ids = self._parse_sitemap()
            if ids:
                recent_id = ids[0]
                metadata = self._fetch_metadata(recent_id)
                if metadata:
                    requisite = metadata.get("requisite", {})
                    print(f"   Recent ID: {recent_id}")
                    print(f"   Title: {requisite.get('title', 'N/A')[:60]}...")
                    print(f"   Accept Date: {requisite.get('acceptDate', 'N/A')}")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = QanunScraper()

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
