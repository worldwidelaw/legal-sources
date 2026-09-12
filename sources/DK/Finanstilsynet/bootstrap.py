#!/usr/bin/env python3
"""
DK/Finanstilsynet — Danish Financial Supervisory Authority

Fetches supervisory decisions from Finanstilsynet via sitemap discovery
and static HTML page scraping.

Strategy:
  1. Parse XML sitemap at finanstilsynet.dk/sitemap.xml
  2. Filter URLs matching /tilsyn/inspektion-og-afgoerelser/{YEAR}/{MONTH}/{slug}
  3. Fetch each decision page for full text (static HTML, no JS needed)

URL patterns:
  - Sitemap: https://www.finanstilsynet.dk/sitemap.xml
  - Decision: https://www.finanstilsynet.dk/tilsyn/inspektion-og-afgoerelser/YYYY/MONTH/slug

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
  python bootstrap.py test
"""

import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.DK.Finanstilsynet")

BASE_URL = "https://www.finanstilsynet.dk"
SITEMAP_URL = "/sitemap.xml"

# Matches decision URLs: /tilsyn/inspektion-og-afgoerelser/YYYY/MONTH/slug
DECISION_URL_PATTERN = re.compile(
    r"/tilsyn/inspektion-og-afgoerelser/(\d{4})/([a-z]+)/([^/]+)$"
)

DANISH_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "maj": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "okt": 10, "nov": 11, "dec": 12,
}


class FinanstilsynetScraper(BaseScraper):
    """Scraper for DK/Finanstilsynet — Danish Financial Supervisory Authority."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    def _fetch_sitemap(self) -> list:
        """Fetch sitemap and return decision URLs."""
        logger.info("Fetching sitemap...")
        self.rate_limiter.wait()
        resp = self.http.get(SITEMAP_URL)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        ns = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        decision_urls = []
        for url_elem in root.findall(".//ns:url/ns:loc", ns):
            url = url_elem.text
            if url and DECISION_URL_PATTERN.search(url):
                decision_urls.append(url)

        logger.info(f"Found {len(decision_urls)} decision URLs in sitemap")
        return decision_urls

    def _parse_date_from_url(self, year: str, month: str) -> Optional[str]:
        """Parse year+month from URL into ISO date string."""
        m = DANISH_MONTHS.get(month.lower())
        if m:
            return f"{year}-{m:02d}-01"
        return f"{year}-01-01"

    def _extract_text_from_html(self, html_content: str) -> dict:
        """Extract title, date, and text from decision page HTML."""
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html_content, "html.parser")

        # Remove nav, footer, script, style
        for tag in soup.find_all(["nav", "footer", "script", "style", "header"]):
            tag.decompose()

        # Extract title from og:title or <title> or first h1
        title = None
        og_title = soup.find("meta", {"property": "og:title"})
        if og_title:
            title = og_title.get("content", "").strip()
        if not title:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)
        if not title:
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True)

        # Extract published date from structured data, meta, or page text
        date_str = None
        date_meta = soup.find("meta", {"property": "article:published_time"})
        if date_meta:
            date_str = date_meta.get("content", "")[:10]
        if not date_str:
            time_tag = soup.find("time")
            if time_tag:
                dt = time_tag.get("datetime", "")
                if dt:
                    date_str = dt[:10]
        if not date_str:
            # Look for DD-MM-YYYY pattern in page text
            body_text = soup.get_text()
            dm = re.search(r"(\d{2})-(\d{2})-(\d{4})", body_text)
            if dm:
                day, month_num, yr = dm.groups()
                date_str = f"{yr}-{month_num}-{day}"

        # Extract main content
        # Try common content containers
        content_el = None
        for selector in [
            soup.find("div", class_=re.compile(r"(article|content|body|main)", re.I)),
            soup.find("article"),
            soup.find("main"),
        ]:
            if selector:
                content_el = selector
                break

        if not content_el:
            content_el = soup.find("body")

        text = ""
        if content_el:
            text = content_el.get_text(separator="\n", strip=True)

        # Clean up navigation/menu remnants
        lines = text.split("\n")
        cleaned = []
        skip_patterns = [
            "Finanstilsynet", "Forside", "Tilsyn", "Statistik",
            "Cookie", "footer", "menu", "navigation",
        ]
        in_content = False
        for line in lines:
            line = line.strip()
            if not line:
                continue
            # Start tracking content after the title
            if title and title in line:
                in_content = True
                cleaned.append(line)
                continue
            if in_content:
                cleaned.append(line)
            elif len(line) > 60:
                # Long lines are likely content even before title match
                in_content = True
                cleaned.append(line)

        text = "\n".join(cleaned) if cleaned else text

        return {
            "title": title or "",
            "date": date_str,
            "text": text,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Discover all decision URLs from sitemap."""
        urls = self._fetch_sitemap()
        for i, url in enumerate(urls):
            match = DECISION_URL_PATTERN.search(url)
            if match:
                year, month, slug = match.groups()
                yield {
                    "_url": url,
                    "_year": year,
                    "_month": month,
                    "_slug": slug,
                    "_index": i,
                    "_total": len(urls),
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all records (no incremental support)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Fetch decision page and normalize."""
        url = raw["_url"]
        slug = raw["_slug"]
        year = raw["_year"]
        month = raw["_month"]
        idx = raw.get("_index", 0)
        total = raw.get("_total", 0)

        logger.info(f"  [{idx+1}/{total}] Fetching {slug[:40]}...")

        try:
            self.rate_limiter.wait()
            resp = self.http.get(url)
            if resp.status_code == 404:
                logger.warning(f"  404: {url}")
                return None
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Error fetching {url}: {e}")
            return None

        extracted = self._extract_text_from_html(resp.text)
        text = extracted["text"]

        if not text or len(text) < 50:
            logger.warning(f"  Text too short ({len(text)} chars) for {slug}")
            return None

        date = extracted["date"] or self._parse_date_from_url(year, month)
        title = extracted["title"] or slug.replace("-", " ").title()

        doc_id = f"finanstilsynet_{year}_{slug}"

        logger.info(f"  Extracted {len(text)} chars: {title[:60]}")

        return {
            "_id": doc_id,
            "_source": "DK/Finanstilsynet",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "slug": slug,
            "year": year,
            "issuing_body": "Finanstilsynet (Danish Financial Supervisory Authority)",
            "language": "da",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing DK/Finanstilsynet endpoints...")

        print("\n1. Fetching sitemap...")
        urls = self._fetch_sitemap()
        print(f"   Found {len(urls)} decision URLs")

        if urls:
            sample_url = urls[0]
            print(f"\n2. Fetching sample decision: {sample_url}")
            self.rate_limiter.wait()
            resp = self.http.get(sample_url)
            print(f"   Status: {resp.status_code}")

            extracted = self._extract_text_from_html(resp.text)
            print(f"   Title: {extracted['title'][:80]}")
            print(f"   Date: {extracted['date']}")
            print(f"   Text: {len(extracted['text'])} chars")
            print(f"   Preview: {extracted['text'][:300]}...")

        print("\nTest complete!")


def main():
    scraper = FinanstilsynetScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
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
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
