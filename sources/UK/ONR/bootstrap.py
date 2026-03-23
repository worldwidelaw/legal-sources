#!/usr/bin/env python3
"""
World Wide Law - UK Office for Nuclear Regulation Scraper

Fetches ONR documents from two sources:
1. RSS feeds at onr.org.uk (news + publications with HTML content)
2. GOV.UK Content API (additional regulatory documents)

Skips publications that redirect to .docx/.pdf files (no extraction tool).
No authentication required.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import re
import sys
import json
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

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
logger = logging.getLogger("UK/ONR")

ONR_BASE = "https://www.onr.org.uk"
GOVUK_BASE = "https://www.gov.uk"
GOVUK_ORG = "office-for-nuclear-regulation"

RSS_FEEDS = [
    "/rss-global",
]


def strip_html(html: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class UKONRScraper(BaseScraper):
    """
    Scraper for UK ONR documents via RSS feeds + GOV.UK Content API.

    Strategy:
    - Parse RSS global feed for all news + publication URLs
    - Fetch each URL, skip .docx/.pdf redirects
    - Extract text from HTML pages
    - Supplement with GOV.UK Content API documents
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.onr_client = HttpClient(
            base_url=ONR_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (legal research project)",
                "Accept": "text/html,application/xhtml+xml,application/xml",
            },
            timeout=30,
        )
        self.govuk_client = HttpClient(
            base_url=GOVUK_BASE,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )
        self._seen_urls = set()

    def _parse_rss_feed(self, feed_path: str) -> list:
        """Parse an RSS feed and return items."""
        self.rate_limiter.wait()
        try:
            resp = self.onr_client.get(feed_path)
            if resp.status_code != 200:
                logger.warning(f"RSS feed {feed_path} returned {resp.status_code}")
                return []
            soup = BeautifulSoup(resp.text, "xml")
            items = []
            for item in soup.find_all("item"):
                title_el = item.find("title")
                link_el = item.find("link")
                desc_el = item.find("description")
                updated_el = item.find("updated") or item.find("pubDate")
                guid_el = item.find("guid")

                if not link_el:
                    continue

                items.append({
                    "title": title_el.text.strip() if title_el else "",
                    "link": link_el.text.strip() if link_el else "",
                    "description": desc_el.text.strip() if desc_el else "",
                    "date": updated_el.text.strip() if updated_el else "",
                    "guid": guid_el.text.strip() if guid_el else "",
                })
            return items
        except Exception as e:
            logger.error(f"RSS parse failed for {feed_path}: {e}")
            return []

    def _fetch_page_text(self, url: str) -> Optional[str]:
        """Fetch a page and extract main content text. Returns None for binary files."""
        self.rate_limiter.wait()
        try:
            import requests
            resp = requests.get(
                url,
                headers={"User-Agent": "WorldWideLaw/1.0 (legal research project)"},
                timeout=30,
                allow_redirects=True,
            )
            # Skip binary file redirects
            final_url = resp.url.lower()
            if final_url.endswith((".docx", ".pdf", ".doc", ".xlsx", ".pptx")):
                return None

            if resp.status_code != 200:
                return None

            content_type = resp.headers.get("Content-Type", "")
            if "html" not in content_type:
                return None

            soup = BeautifulSoup(resp.text, "html.parser")

            # Remove nav, header, footer, script, style
            for tag in soup.find_all(["nav", "header", "footer", "script", "style", "noscript"]):
                tag.decompose()

            main = soup.find("main")
            if main:
                text = main.get_text(separator="\n")
            else:
                article = soup.find("article")
                if article:
                    text = article.get_text(separator="\n")
                else:
                    return None

            text = re.sub(r"\n{3,}", "\n\n", text)
            text = text.strip()
            return text if len(text) > 100 else None

        except Exception as e:
            logger.debug(f"Page fetch failed for {url}: {e}")
            return None

    def _fetch_govuk_documents(self) -> Generator[dict, None, None]:
        """Fetch NCA documents from GOV.UK Content API."""
        start = 0
        while True:
            self.rate_limiter.wait()
            try:
                resp = self.govuk_client.get(
                    f"/api/search.json?filter_organisations={GOVUK_ORG}"
                    f"&count=200&start={start}"
                    f"&fields=title,link,public_timestamp,description"
                )
                if resp.status_code != 200:
                    break
                data = resp.json()
                total = data.get("total", 0)
                items = data.get("results", [])
                if not items:
                    break

                if start == 0:
                    logger.info(f"GOV.UK ONR documents: {total}")

                for item in items:
                    link = item.get("link", "")
                    if not link:
                        continue

                    full_url = f"{GOVUK_BASE}{link}"
                    if full_url in self._seen_urls:
                        continue
                    self._seen_urls.add(full_url)

                    self.rate_limiter.wait()
                    content_resp = self.govuk_client.get(f"/api/content{link}")
                    if content_resp.status_code != 200:
                        continue
                    content_data = content_resp.json()

                    details = content_data.get("details", {})
                    body_html = details.get("body", "")
                    text = strip_html(body_html) if body_html else ""

                    if not text or len(text) < 50:
                        continue

                    yield {
                        "title": content_data.get("title", ""),
                        "text": text,
                        "date": content_data.get("public_updated_at", ""),
                        "url": full_url,
                        "source_feed": "govuk",
                    }

                start += len(items)
                if start >= total:
                    break
            except Exception as e:
                logger.error(f"GOV.UK fetch failed: {e}")
                break

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ONR documents with full text."""
        # 1. RSS feeds
        for feed_path in RSS_FEEDS:
            items = self._parse_rss_feed(feed_path)
            logger.info(f"RSS feed {feed_path}: {len(items)} items")

            for item in items:
                url = item.get("link", "")
                if not url or url in self._seen_urls:
                    continue
                self._seen_urls.add(url)

                text = self._fetch_page_text(url)
                if not text:
                    continue

                yield {
                    "title": item.get("title", ""),
                    "text": text,
                    "date": item.get("date", ""),
                    "url": url,
                    "source_feed": "rss",
                    "guid": item.get("guid", ""),
                }

        # 2. GOV.UK Content API
        yield from self._fetch_govuk_documents()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents updated since the given datetime."""
        for raw in self.fetch_all():
            date_str = raw.get("date", "")
            if date_str:
                try:
                    doc_dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                    if doc_dt >= since:
                        yield raw
                except (ValueError, TypeError):
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw data into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        url = raw.get("url", "")
        doc_id = hashlib.md5(url.encode()).hexdigest() if url else ""
        if not doc_id:
            return None

        date_str = raw.get("date", "")
        date_val = None
        if date_str:
            try:
                date_val = datetime.fromisoformat(
                    date_str.replace("Z", "+00:00")
                ).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                # Try parsing RFC 2822 date from RSS
                try:
                    from email.utils import parsedate_to_datetime
                    date_val = parsedate_to_datetime(date_str).strftime("%Y-%m-%d")
                except Exception:
                    date_val = None

        return {
            "_id": doc_id,
            "_source": "UK/ONR",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": date_val,
            "url": url,
            "source_feed": raw.get("source_feed", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKONRScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
