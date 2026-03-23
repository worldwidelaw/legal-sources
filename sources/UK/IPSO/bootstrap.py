#!/usr/bin/env python3
"""
Legal Data Hunter - UK IPSO (Press Standards) Scraper

Fetches IPSO press complaints rulings and resolution statements.
WordPress site with paginated HTML listing, no authentication needed.

Coverage: ~6,800 rulings with full text.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import re
import sys
import json
import logging
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
logger = logging.getLogger("UK/IPSO")

BASE_URL = "https://www.ipso.co.uk"
RULINGS_PER_PAGE = 24
MAX_PAGES = 300  # Safety limit


def strip_html(html: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse IPSO date format like '19th March 2026' to ISO."""
    if not date_str:
        return None
    # Remove ordinal suffixes
    cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str.strip())
    for fmt in ("%d %B %Y", "%B %d %Y", "%d %b %Y"):
        try:
            return datetime.strptime(cleaned, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class UKIPSOScraper(BaseScraper):
    """
    Scraper for UK IPSO press complaints rulings.

    Strategy:
    - Paginate through /rulings/page/{N}/ listing pages
    - Extract ruling URLs from each listing page
    - Fetch each ruling page for full text
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "text/html",
            },
            timeout=30,
        )

    def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.text
            logger.debug(f"HTTP {resp.status_code} for {url}")
            return None
        except Exception as e:
            logger.debug(f"Failed to fetch {url}: {e}")
            return None

    def _extract_ruling_urls(self, html: str) -> list:
        """Extract ruling URLs from a listing page."""
        soup = BeautifulSoup(html, "html.parser")
        urls = set()
        for a in soup.find_all("a", href=re.compile(r"/rulings/\d+")):
            href = a["href"]
            # Skip external share links
            if "facebook.com" in href or "twitter.com" in href or "linkedin.com" in href:
                continue
            if not href.startswith("http"):
                href = BASE_URL + href
            # Only keep ipso.co.uk URLs
            if "ipso.co.uk" in href or not href.startswith("http"):
                urls.add(href)
        return sorted(urls)

    def _parse_ruling(self, html: str, url: str) -> Optional[dict]:
        """Parse a ruling page to extract structured data."""
        soup = BeautifulSoup(html, "html.parser")

        # Get title
        title_el = soup.find("h1")
        title = title_el.get_text().strip() if title_el else ""

        # Get the article/main content
        article = soup.find("article") or soup.find("main")
        if not article:
            return None

        # Extract metadata from introduction-ruling__detail elements
        date_str = ""
        outcome = ""
        clauses = ""

        for li in soup.find_all("li", class_=re.compile(r"introduction-ruling__detail")):
            li_text = li.get_text(separator="\n").strip()
            lines = [l.strip() for l in li_text.split("\n") if l.strip()]
            if len(lines) >= 2:
                label = lines[0].lower()
                value = lines[1]
                if "published date" in label:
                    date_str = value
                elif "outcome" in label:
                    outcome = value
                elif "code provision" in label or "clause" in label:
                    clauses = value

        # Get full text from article
        # Remove navigation elements
        for nav in article.find_all(["nav", "header"]):
            nav.decompose()

        full_text = article.get_text(separator="\n")
        full_text = re.sub(r"\n{3,}", "\n\n", full_text).strip()

        # Extract ruling ID from URL
        ruling_id = re.search(r"/rulings/(\d+-\d+)", url)
        ruling_id = ruling_id.group(1) if ruling_id else ""

        return {
            "ruling_id": ruling_id,
            "title": title,
            "text": full_text,
            "date_str": date_str,
            "outcome": outcome,
            "clauses": clauses,
            "url": url,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IPSO rulings with full text."""
        page = 1
        consecutive_empty = 0

        while page <= MAX_PAGES:
            if page == 1:
                listing_url = "/rulings/"
            else:
                listing_url = f"/rulings/page/{page}/"

            logger.info(f"Fetching listing page {page}...")
            html = self._fetch_page(listing_url)
            if not html:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
                continue

            ruling_urls = self._extract_ruling_urls(html)
            if not ruling_urls:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                page += 1
                continue

            # Detect end of valid pages (when we get too many links = homepage)
            if len(ruling_urls) > 50:
                logger.info(f"Page {page} returned {len(ruling_urls)} links (likely past last page)")
                break

            consecutive_empty = 0
            logger.info(f"Page {page}: {len(ruling_urls)} rulings")

            for url in ruling_urls:
                ruling_html = self._fetch_page(url.replace(BASE_URL, ""))
                if not ruling_html:
                    continue

                parsed = self._parse_ruling(ruling_html, url)
                if parsed:
                    yield parsed

            page += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield recently published rulings."""
        for raw in self.fetch_all():
            date_val = parse_date(raw.get("date_str", ""))
            if date_val:
                try:
                    doc_dt = datetime.strptime(date_val, "%Y-%m-%d")
                    if doc_dt >= since.replace(tzinfo=None):
                        yield raw
                    else:
                        # Rulings are in reverse chronological order
                        # Stop when we hit old ones
                        break
                except ValueError:
                    yield raw
            else:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw ruling data into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        ruling_id = raw.get("ruling_id", "")
        if not ruling_id:
            return None

        date_val = parse_date(raw.get("date_str", ""))

        return {
            "_id": f"uk_ipso_{ruling_id}",
            "_source": "UK/IPSO",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": text,
            "date": date_val,
            "url": raw.get("url", ""),
            "outcome": raw.get("outcome", ""),
            "clauses": raw.get("clauses", ""),
            "ruling_id": ruling_id,
        }


# ── CLI entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKIPSOScraper()

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
