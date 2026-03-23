#!/usr/bin/env python3
"""
Legal Data Hunter - UK Gambling Commission Scraper

Fetches regulatory content from gamblingcommission.gov.uk using:
  - Sitemaps for URL discovery
  - HTML scraping for full text content

Coverage: ~2,000+ documents (regulatory actions, guidance, consultations, reports).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import re
import sys
import json
import hashlib
import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple

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
logger = logging.getLogger("UK/GC")

# Content sections to scrape (path prefix -> content type)
CONTENT_SECTIONS = [
    ("/public-register/regulatory-action/detail/", "regulatory_action"),
    ("/public-register/public-statement/detail/", "public_statement"),
    ("/guidance/", "guidance"),
    ("/consultation-response/", "consultation_response"),
    ("/news/article/", "news"),
    ("/report/", "report"),
    ("/policy/", "policy"),
    ("/standards/", "standards"),
]

# Paths to exclude (not content pages)
EXCLUDE_PATTERNS = [
    "/public-register/search",
    "/public-register/operator-filter",
    "/public-register/personal",
    "/public-register/premises",
    "/print/",
    "/sitemap",
]


def classify_url(url: str) -> Optional[str]:
    """Classify a URL by content type based on path prefix."""
    path = url.replace("https://www.gamblingcommission.gov.uk", "")
    for prefix, content_type in CONTENT_SECTIONS:
        if path.startswith(prefix):
            return content_type
    return None


def should_exclude(url: str) -> bool:
    """Check if a URL should be excluded."""
    for pattern in EXCLUDE_PATTERNS:
        if pattern in url:
            return True
    return False


class UKGCScraper(BaseScraper):
    """
    Scraper for UK Gambling Commission.

    Strategy:
    - Parse sitemaps to discover all content URLs
    - Filter by content section (guidance, regulatory actions, etc.)
    - Fetch each page and extract <main> content
    """

    BASE_URL = "https://www.gamblingcommission.gov.uk"
    SITEMAPS = [
        "/sitemap-main.xml",
        "/sitemap-action-and-statement.xml",
    ]

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=30,
        )

    def _parse_sitemap(self, path: str) -> List[Tuple[str, str]]:
        """Parse a sitemap and return list of (url, lastmod) tuples."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(path)
            if resp.status_code != 200:
                logger.warning(f"Sitemap returned {resp.status_code}: {path}")
                return []
            root = ET.fromstring(resp.content)
            ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
            urls = []
            for url_elem in root.findall(".//sm:url", ns):
                loc = url_elem.findtext("sm:loc", default="", namespaces=ns)
                lastmod = url_elem.findtext("sm:lastmod", default="", namespaces=ns)
                if loc:
                    urls.append((loc, lastmod))
            return urls
        except Exception as e:
            logger.error(f"Sitemap parsing failed for {path}: {e}")
            return []

    def _get_all_content_urls(self) -> List[Tuple[str, str, str]]:
        """Get all content URLs from sitemaps, classified by type.
        Returns list of (url, lastmod, content_type) tuples."""
        all_urls = []
        for sitemap_path in self.SITEMAPS:
            urls = self._parse_sitemap(sitemap_path)
            logger.info(f"Sitemap {sitemap_path}: {len(urls)} URLs")
            all_urls.extend(urls)

        content_urls = []
        for url, lastmod in all_urls:
            if should_exclude(url):
                continue
            content_type = classify_url(url)
            if content_type:
                content_urls.append((url, lastmod, content_type))

        logger.info(f"Total content URLs: {len(content_urls)}")
        return content_urls

    def _fetch_page_text(self, url: str) -> Tuple[str, str]:
        """Fetch a page and extract title and main content text."""
        path = url.replace(self.BASE_URL, "")
        self.rate_limiter.wait()
        try:
            resp = self.client.get(path)
            if resp.status_code != 200:
                return "", ""

            soup = BeautifulSoup(resp.content, "html.parser")

            # Title
            title_tag = soup.find("h1")
            title = title_tag.get_text(strip=True) if title_tag else ""
            if not title:
                title_tag = soup.find("title")
                title = title_tag.get_text(strip=True) if title_tag else ""

            # Main content
            main = soup.find("main")
            if not main:
                main = soup.find("article")
            if not main:
                main = soup.find("div", class_=re.compile(r"content|article|body"))

            if not main:
                return title, ""

            # Remove nav, aside, footer, scripts, styles
            for tag in main.find_all(["nav", "aside", "footer", "script", "style", "form"]):
                tag.decompose()

            text = main.get_text(separator="\n")
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = text.strip()

            return title, text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return "", ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Gambling Commission documents with full text."""
        content_urls = self._get_all_content_urls()
        count = 0
        skipped = 0

        for url, lastmod, content_type in content_urls:
            title, text = self._fetch_page_text(url)
            if not text or len(text) < 100:
                skipped += 1
                continue

            # Generate stable ID from URL
            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]

            count += 1
            yield {
                "url": url,
                "url_hash": url_hash,
                "title": title,
                "text": text,
                "content_type": content_type,
                "lastmod": lastmod,
            }

            if count % 50 == 0:
                logger.info(f"  {count} documents fetched ({skipped} skipped)")

        logger.info(f"Total: {count} documents with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents updated since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        content_urls = self._get_all_content_urls()

        for url, lastmod, content_type in content_urls:
            if lastmod and lastmod[:10] < since_str:
                continue

            title, text = self._fetch_page_text(url)
            if not text or len(text) < 100:
                continue

            url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
            yield {
                "url": url,
                "url_hash": url_hash,
                "title": title,
                "text": text,
                "content_type": content_type,
                "lastmod": lastmod,
            }

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        date_str = raw.get("lastmod", "")
        date_iso = date_str[:10] if date_str else None

        return {
            "_id": f"UK/GC/{raw.get('url_hash', '')}",
            "_source": "UK/GC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "url": raw.get("url", ""),
            "title": raw.get("title", ""),
            "text": text,
            "content_type": raw.get("content_type", ""),
            "date": date_iso,
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKGCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
