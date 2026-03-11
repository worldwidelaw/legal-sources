#!/usr/bin/env python3
"""
HR/OfficialGazette -- Croatian Official Gazette Data Fetcher

Fetches Croatian legislation from Narodne novine (Official Gazette).

Strategy:
  - Uses ELI (European Legislation Identifier) URIs for discovery and full text.
  - Sitemap index at /sitemap.xml lists per-issue sitemaps.
  - Per-issue sitemaps (/sitemap_1_{year}_{issue}.xml) list all documents.
  - JSON-LD metadata: /eli/sluzbeni/{year}/{issue}/{doc}/json-ld
  - Full text HTML: /eli/sluzbeni/{year}/{issue}/{doc}/hrv/html

Endpoints:
  - Sitemap: https://narodne-novine.nn.hr/sitemap.xml
  - JSON-LD: https://narodne-novine.nn.hr/eli/sluzbeni/2025/1/1/json-ld
  - Full text: https://narodne-novine.nn.hr/eli/sluzbeni/2025/1/1/hrv/html

Data:
  - Documents from 1990 to present
  - Language: Croatian (HRV)
  - Rate limit: max 3 requests/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent issues only)
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
from urllib.parse import urlparse

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HR.officialgazette")

# Base URL for Croatian Official Gazette
BASE_URL = "https://narodne-novine.nn.hr"

# Years to scrape (most recent first for sample mode)
YEARS_TO_SCRAPE = list(range(2025, 1989, -1))  # 2025 down to 1990


class CroatianOfficialGazetteScraper(BaseScraper):
    """
    Scraper for HR/OfficialGazette -- Croatian Official Gazette.
    Country: HR
    URL: https://narodne-novine.nn.hr

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept-Language": "hr,en",
            },
            timeout=60,
        )

    def _parse_sitemap_index(self) -> List[Dict[str, Any]]:
        """
        Parse the main sitemap index to get per-issue sitemap URLs.

        Returns list of dicts with: url, year, issue
        """
        sitemaps = []

        try:
            self.rate_limiter.wait()
            resp = self.client.get("/sitemap.xml")
            resp.raise_for_status()

            content = resp.text

            # Parse XML
            # Sitemaps follow pattern: sitemap_1_{year}_{issue}.xml
            pattern = re.compile(r'/sitemap_1_(\d{4})_(\d+)\.xml')

            for match in pattern.finditer(content):
                year = int(match.group(1))
                issue = int(match.group(2))
                url = f"{BASE_URL}/sitemap_1_{year}_{issue}.xml"

                sitemaps.append({
                    "url": url,
                    "year": year,
                    "issue": issue,
                })

            # Sort by year (descending), then issue (descending)
            sitemaps.sort(key=lambda x: (x["year"], x["issue"]), reverse=True)

            logger.info(f"Found {len(sitemaps)} issue sitemaps in index")
            return sitemaps

        except Exception as e:
            logger.error(f"Failed to parse sitemap index: {e}")
            return []

    def _parse_issue_sitemap(self, sitemap_url: str) -> List[str]:
        """
        Parse a per-issue sitemap to extract document ELI URLs.

        Returns list of ELI URLs (the /eli/sluzbeni/... format)
        """
        eli_urls = []

        try:
            self.rate_limiter.wait()
            resp = self.client.get(sitemap_url.replace(BASE_URL, ""))
            resp.raise_for_status()

            content = resp.text

            # Extract ELI URLs from the sitemap
            # Pattern: /eli/sluzbeni/{year}/{issue}/{doc_num}
            eli_pattern = re.compile(r'<loc>([^<]*?/eli/sluzbeni/\d+/\d+/\d+)</loc>')

            for match in eli_pattern.finditer(content):
                url = match.group(1)
                if url and url not in eli_urls:
                    eli_urls.append(url)

            logger.info(f"Found {len(eli_urls)} ELI URLs in {sitemap_url}")
            return eli_urls

        except Exception as e:
            logger.warning(f"Failed to parse issue sitemap {sitemap_url}: {e}")
            return []

    def _parse_eli_url(self, eli_url: str) -> Optional[Dict[str, Any]]:
        """Parse ELI URL to extract year, issue, doc_num."""
        pattern = re.compile(r'/eli/sluzbeni/(\d+)/(\d+)/(\d+)')
        match = pattern.search(eli_url)

        if match:
            return {
                "year": int(match.group(1)),
                "issue": int(match.group(2)),
                "doc_num": int(match.group(3)),
            }
        return None

    def _fetch_metadata(self, eli_url: str) -> Optional[Dict[str, Any]]:
        """
        Fetch JSON-LD metadata for a document.

        Returns parsed metadata or None on failure.
        """
        try:
            # Ensure URL has base URL
            if not eli_url.startswith("http"):
                eli_url = f"{BASE_URL}{eli_url}"

            jsonld_url = f"{eli_url}/json-ld"

            self.rate_limiter.wait()
            resp = self.client.get(jsonld_url.replace(BASE_URL, ""))
            resp.raise_for_status()

            data = resp.json()
            return data

        except Exception as e:
            logger.warning(f"Failed to fetch metadata from {eli_url}: {e}")
            return None

    def _fetch_full_text(self, eli_url: str) -> str:
        """
        Fetch and extract full text from an ELI /hrv/html page.

        The page contains the full text of the legislation in HTML format.
        We extract all text content, stripping HTML tags.
        """
        try:
            # Ensure URL has base URL
            if not eli_url.startswith("http"):
                eli_url = f"{BASE_URL}{eli_url}"

            html_url = f"{eli_url}/hrv/html"

            self.rate_limiter.wait()
            resp = self.client.get(html_url.replace(BASE_URL, ""))
            resp.raise_for_status()

            content = resp.text

            # Extract text from the main content
            text_parts = []

            # Remove script and style tags
            content = re.sub(r'<script[^>]*>.*?</script>', '', content, flags=re.DOTALL | re.IGNORECASE)
            content = re.sub(r'<style[^>]*>.*?</style>', '', content, flags=re.DOTALL | re.IGNORECASE)

            # Remove HTML comments
            content = re.sub(r'<!--.*?-->', '', content, flags=re.DOTALL)

            # Extract text between tags
            texts = re.findall(r'>([^<]+)<', content)

            for t in texts:
                t = t.strip()
                if len(t) < 3:
                    continue

                # Skip navigation and boilerplate
                skip_patterns = [
                    'cookie', 'javascript', 'navigation', 'menu',
                    'footer', 'header', 'sidebar', 'print', 'email',
                    'share', 'facebook', 'twitter', 'linkedin',
                    'narodne-novine.nn.hr', 'www.', 'http',
                ]

                lower_t = t.lower()
                if any(x in lower_t for x in skip_patterns):
                    continue

                # Clean up the text
                clean_t = html.unescape(t)
                clean_t = re.sub(r'\s+', ' ', clean_t).strip()

                if clean_t and len(clean_t) > 2:
                    text_parts.append(clean_t)

            full_text = '\n'.join(text_parts)

            # If we got very little text, try a broader extraction
            if len(full_text) < 200:
                # Try extracting all text blocks
                text_parts = []
                for match in re.findall(r'>([^<]{10,})<', content):
                    clean = html.unescape(match.strip())
                    clean = re.sub(r'\s+', ' ', clean).strip()
                    if clean:
                        text_parts.append(clean)
                full_text = '\n'.join(text_parts)

            return full_text.strip()

        except Exception as e:
            logger.warning(f"Failed to fetch full text from {eli_url}: {e}")
            return ""

    def _extract_title_from_metadata(self, metadata: Dict[str, Any]) -> str:
        """Extract title from JSON-LD metadata."""
        if not metadata:
            return ""

        # JSON-LD can have nested structure
        # Look for title in various places
        if isinstance(metadata, list):
            for item in metadata:
                if isinstance(item, dict):
                    title = self._extract_title_from_metadata(item)
                    if title:
                        return title
            return ""

        # Full ELI ontology URI for title (Croatian API uses this)
        eli_title_key = "http://data.europa.eu/eli/ontology#title"
        if eli_title_key in metadata:
            val = metadata[eli_title_key]
            if isinstance(val, list) and val:
                val = val[0]
            if isinstance(val, dict):
                val = val.get("@value", "") or val.get("value", "")
            if val:
                return str(val)

        # Direct title field
        title = metadata.get("title", "")
        if isinstance(title, dict):
            title = title.get("@value", "") or title.get("value", "")

        if title:
            return title

        # Look in eli:title or other ELI fields
        for key in ["eli:title", "eli:title_short", "dcterms:title", "dct:title"]:
            if key in metadata:
                val = metadata[key]
                if isinstance(val, dict):
                    val = val.get("@value", "") or val.get("value", "")
                if isinstance(val, list) and val:
                    val = val[0]
                    if isinstance(val, dict):
                        val = val.get("@value", "") or val.get("value", "")
                if val:
                    return str(val)

        # Look for title in graph structure
        if "@graph" in metadata:
            for item in metadata["@graph"]:
                if isinstance(item, dict):
                    title = self._extract_title_from_metadata(item)
                    if title:
                        return title

        return ""

    def _extract_date_from_metadata(self, metadata: Dict[str, Any]) -> str:
        """Extract publication date from JSON-LD metadata."""
        if not metadata:
            return ""

        if isinstance(metadata, list):
            for item in metadata:
                if isinstance(item, dict):
                    date = self._extract_date_from_metadata(item)
                    if date:
                        return date
            return ""

        # Full ELI ontology URIs for date (Croatian API uses these)
        eli_date_keys = [
            "http://data.europa.eu/eli/ontology#date_publication",
            "http://data.europa.eu/eli/ontology#date_document",
        ]

        for key in eli_date_keys:
            if key in metadata:
                val = metadata[key]
                if isinstance(val, list) and val:
                    val = val[0]
                if isinstance(val, dict):
                    val = val.get("@value", "") or val.get("value", "")
                if val:
                    return str(val)[:10] if len(str(val)) >= 10 else str(val)

        # Look for date fields (short forms)
        for key in ["eli:date_publication", "eli:date_document", "dcterms:date", "dct:date", "date"]:
            if key in metadata:
                val = metadata[key]
                if isinstance(val, dict):
                    val = val.get("@value", "") or val.get("value", "")
                if isinstance(val, list) and val:
                    val = val[0]
                    if isinstance(val, dict):
                        val = val.get("@value", "") or val.get("value", "")
                if val:
                    # Try to parse and format as ISO 8601
                    try:
                        if isinstance(val, str):
                            # Handle various date formats
                            if "T" in val:
                                return val.split("T")[0]
                            return val[:10] if len(val) >= 10 else val
                    except:
                        pass
                    return str(val)

        # Look in graph structure
        if "@graph" in metadata:
            for item in metadata["@graph"]:
                if isinstance(item, dict):
                    date = self._extract_date_from_metadata(item)
                    if date:
                        return date

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the Croatian Official Gazette.

        Iterates through sitemaps by year (newest first), fetching
        document metadata and full text for each.
        """
        # Get sitemap index
        sitemaps = self._parse_sitemap_index()

        if not sitemaps:
            logger.warning("No sitemaps found in index, using fallback approach")
            # Fallback: construct sitemap URLs for recent years
            for year in YEARS_TO_SCRAPE[:5]:  # Last 5 years
                for issue in range(1, 150):  # Up to 150 issues per year
                    sitemaps.append({
                        "url": f"{BASE_URL}/sitemap_1_{year}_{issue}.xml",
                        "year": year,
                        "issue": issue,
                    })

        documents_yielded = 0

        for sitemap_info in sitemaps:
            sitemap_url = sitemap_info["url"]
            year = sitemap_info["year"]
            issue = sitemap_info["issue"]

            logger.info(f"Processing sitemap for {year}/{issue}...")

            eli_urls = self._parse_issue_sitemap(sitemap_url)

            for eli_url in eli_urls:
                # Parse URL to get document identifiers
                parsed = self._parse_eli_url(eli_url)
                if not parsed:
                    continue

                # Fetch metadata
                metadata = self._fetch_metadata(eli_url)

                # Fetch full text
                full_text = self._fetch_full_text(eli_url)

                if not full_text:
                    logger.warning(f"No full text for {eli_url}, skipping")
                    continue

                # Extract title and date from metadata
                title = self._extract_title_from_metadata(metadata)
                date = self._extract_date_from_metadata(metadata)

                # If no title from metadata, try to extract from text
                if not title and full_text:
                    # First non-empty line is often the title
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                    if lines:
                        title = lines[0][:200]  # Limit title length

                yield {
                    "eli_url": eli_url,
                    "year": parsed["year"],
                    "issue": parsed["issue"],
                    "doc_num": parsed["doc_num"],
                    "title": title,
                    "date": date,
                    "full_text": full_text,
                    "metadata": metadata,
                }

                documents_yielded += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Since ELI doesn't have a direct "modified since" filter, we
        fetch recent sitemaps (last 2 years) and filter by date.
        """
        since_year = since.year
        current_year = datetime.now().year

        # Get sitemap index and filter to recent years
        sitemaps = self._parse_sitemap_index()

        recent_sitemaps = [s for s in sitemaps if s["year"] >= since_year]

        for sitemap_info in recent_sitemaps:
            sitemap_url = sitemap_info["url"]
            year = sitemap_info["year"]
            issue = sitemap_info["issue"]

            logger.info(f"Checking {year}/{issue} for updates...")

            eli_urls = self._parse_issue_sitemap(sitemap_url)

            for eli_url in eli_urls:
                parsed = self._parse_eli_url(eli_url)
                if not parsed:
                    continue

                metadata = self._fetch_metadata(eli_url)
                date_str = self._extract_date_from_metadata(metadata)

                # Check if document is newer than since date
                if date_str:
                    try:
                        doc_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
                        doc_date = doc_date.replace(tzinfo=timezone.utc)
                        if doc_date < since:
                            continue
                    except:
                        pass

                full_text = self._fetch_full_text(eli_url)

                if not full_text:
                    continue

                title = self._extract_title_from_metadata(metadata)
                if not title and full_text:
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]
                    if lines:
                        title = lines[0][:200]

                yield {
                    "eli_url": eli_url,
                    "year": parsed["year"],
                    "issue": parsed["issue"],
                    "doc_num": parsed["doc_num"],
                    "title": title,
                    "date": date_str,
                    "full_text": full_text,
                    "metadata": metadata,
                }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        year = raw.get("year", 0)
        issue = raw.get("issue", 0)
        doc_num = raw.get("doc_num", 0)

        # Create unique document ID
        doc_id = f"{year}/{issue}/{doc_num}"

        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        date_str = raw.get("date", "")
        eli_url = raw.get("eli_url", "")

        # Ensure URL has base
        if eli_url and not eli_url.startswith("http"):
            eli_url = f"{BASE_URL}{eli_url}"

        # Build canonical URL if missing
        if not eli_url:
            eli_url = f"{BASE_URL}/eli/sluzbeni/{year}/{issue}/{doc_num}"

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "HR/OfficialGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_str,
            "url": eli_url,
            # Additional metadata
            "doc_id": doc_id,
            "year": year,
            "issue": issue,
            "doc_num": doc_num,
            "language": "hrv",
            "eli_uri": eli_url,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Croatian Official Gazette endpoints...")

        # Test sitemap
        print("\n1. Testing sitemap index...")
        try:
            resp = self.client.get("/sitemap.xml")
            print(f"   Status: {resp.status_code}")
            sitemaps = self._parse_sitemap_index()
            print(f"   Found {len(sitemaps)} issue sitemaps")
            if sitemaps:
                print(f"   Latest: {sitemaps[0]['year']}/{sitemaps[0]['issue']}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test issue sitemap
        print("\n2. Testing issue sitemap (2025/1)...")
        try:
            eli_urls = self._parse_issue_sitemap(f"{BASE_URL}/sitemap_1_2025_1.xml")
            print(f"   Found {len(eli_urls)} ELI URLs")
            if eli_urls:
                print(f"   Sample: {eli_urls[0]}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test JSON-LD metadata
        print("\n3. Testing JSON-LD metadata endpoint...")
        try:
            test_url = "/eli/sluzbeni/2025/1/1"
            metadata = self._fetch_metadata(test_url)
            print(f"   Got metadata: {metadata is not None}")
            if metadata:
                title = self._extract_title_from_metadata(metadata)
                print(f"   Title: {title[:60]}..." if title else "   Title: N/A")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test full text
        print("\n4. Testing full text endpoint...")
        try:
            test_url = "/eli/sluzbeni/2025/1/1"
            text = self._fetch_full_text(test_url)
            print(f"   Text length: {len(text)} characters")
            if text:
                print(f"   Sample: {text[:150]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = CroatianOfficialGazetteScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
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
