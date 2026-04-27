#!/usr/bin/env python3
"""
PY/BACN -- Biblioteca y Archivo Central del Congreso Nacional (Paraguay)

Fetches Paraguayan legislation from the BACN website.

Data source: https://www.bacn.gov.py/
License: Open Government Data (Paraguay)

Strategy:
  - Use the search endpoint to get all law IDs and titles
  - Fetch each law's HTML page for full text and metadata
  - Extract text from inline HTML content divs
  - Normalize to standard schema

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

# Setup
SOURCE_ID = "PY/BACN"
SOURCE_DIR = Path(__file__).parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PY.BACN")

# API Configuration
BASE_URL = "https://www.bacn.gov.py"
SEARCH_URL = f"{BASE_URL}/buscar.php?j=si&s=ley"
LAW_URL_TEMPLATE = f"{BASE_URL}/leyes-paraguayas/{{id}}/{{slug}}"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

RATE_LIMIT = 1.5  # seconds between requests


def clean_html(html: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html:
        return ""
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL | re.IGNORECASE)
    # Replace br/p/div with newlines for readability
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</(?:p|div|li|tr)>', '\n', html, flags=re.IGNORECASE)
    # Remove remaining tags
    text = re.sub(r'<[^>]+>', ' ', html)
    text = unescape(text)
    # Normalize whitespace within lines, preserve paragraph breaks
    lines = text.split('\n')
    cleaned = []
    for line in lines:
        line = re.sub(r'[ \t]+', ' ', line).strip()
        if line:
            cleaned.append(line)
    text = '\n'.join(cleaned)
    # Collapse excessive newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def make_slug(title: str) -> str:
    """Create URL slug from title."""
    slug = title.lower()
    slug = re.sub(r'[áà]', 'a', slug)
    slug = re.sub(r'[éè]', 'e', slug)
    slug = re.sub(r'[íì]', 'i', slug)
    slug = re.sub(r'[óò]', 'o', slug)
    slug = re.sub(r'[úù]', 'u', slug)
    slug = re.sub(r'[ñ]', 'n', slug)
    slug = re.sub(r'[^a-z0-9]+', '-', slug)
    slug = slug.strip('-')
    return slug[:80]


def fetch_law_index() -> list:
    """Fetch all law entries from the search endpoint."""
    logger.info("Fetching law index from search endpoint...")
    response = requests.get(SEARCH_URL, headers=HEADERS, timeout=30)
    response.raise_for_status()
    data = response.json()
    # Filter to only "Leyes Paraguayas" category
    laws = [
        item for item in data
        if item.get("nombre_categoria") == "Leyes Paraguayas"
    ]
    logger.info(f"Found {len(laws)} laws in index")
    return laws


def fetch_law_page(law_id: str, title: str) -> Optional[dict]:
    """
    Fetch a law page and extract full text and metadata.

    Returns dict with: text, date_promulgation, date_publication, pdf_urls
    """
    slug = make_slug(title)
    url = LAW_URL_TEMPLATE.format(id=law_id, slug=slug)

    try:
        response = requests.get(url, headers=HEADERS, timeout=30, allow_redirects=True)
        if response.status_code == 302 or response.url.endswith("/error"):
            # Try without slug
            url_no_slug = f"{BASE_URL}/leyes-paraguayas/{law_id}/ley"
            response = requests.get(url_no_slug, headers=HEADERS, timeout=30, allow_redirects=True)

        if response.status_code != 200 or response.url.endswith("/error"):
            logger.warning(f"Failed to fetch law {law_id}: HTTP {response.status_code}")
            return None

        html = response.text
        result = {"url": url}

        # Extract entry-content
        m = re.search(
            r'<div class="entry-content">(.*?)(?:<div class="col-sm-4"|</article>)',
            html, re.DOTALL
        )
        if not m:
            logger.warning(f"No entry-content found for law {law_id}")
            return None

        content = m.group(1)

        # Extract dates
        prom = re.findall(r'Fecha de Promulgación:\s*</strong>\s*([\d/-]+)', content)
        pub = re.findall(r'Fecha de Publicación:\s*</strong>\s*([\d/-]+)', content)
        result["date_promulgation"] = prom[0] if prom else None
        result["date_publication"] = pub[0] if pub else None

        # Extract PDF download links
        pdfs = re.findall(
            r'href="(https://www\.bacn\.gov\.py/descarga/[^"]+)"[^>]*title="([^"]*)"',
            content
        )
        result["pdf_urls"] = [(url, title) for url, title in pdfs]

        # Extract body text
        # The law text typically appears after a comment block or after download sections
        # It's in <div style="text-align: ..."> elements
        # Find the text after the metadata/download sections
        body = content

        # Remove the metadata row div
        body = re.sub(
            r'<div class="row">.*?</div>\s*</div>\s*</div>\s*</div>',
            '', body, count=1, flags=re.DOTALL
        )
        # Remove download links and iframes
        body = re.sub(r'<iframe[^>]*>.*?</iframe>', '', body, flags=re.DOTALL)
        body = re.sub(r'<hr><h4>.*?</h4><hr>', '', body, flags=re.DOTALL)
        body = re.sub(r'<hr>\s*<hr>', '', body)
        # Remove comment block
        body = re.sub(r'<!--.*?-->', '', body, flags=re.DOTALL)
        # Remove standalone hr tags
        body = re.sub(r'<hr\s*/?>', '', body)

        text = clean_html(body)
        result["text"] = text

        return result

    except requests.RequestException as e:
        logger.warning(f"Request failed for law {law_id}: {e}")
        return None


def parse_date(date_str: str) -> Optional[str]:
    """Convert DD-MM-YYYY or DD/MM/YYYY to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class BACNScraper(BaseScraper):
    """Scraper for PY/BACN - Paraguay Congressional Law Library."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw law data to standard schema."""
        law_id = raw.get("law_id", "")
        title = raw.get("title", "")
        text = raw.get("text", "")

        if not text or len(text) < 50:
            return None

        date = parse_date(raw.get("date_promulgation")) or \
               parse_date(raw.get("date_publication"))

        return {
            "_id": f"PY_BACN_{law_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("url", f"{BASE_URL}/leyes-paraguayas/{law_id}"),
            "date_promulgation": parse_date(raw.get("date_promulgation")),
            "date_publication": parse_date(raw.get("date_publication")),
            "pdf_urls": [u for u, _ in raw.get("pdf_urls", [])],
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all raw law records."""
        laws = fetch_law_index()
        logger.info(f"Fetching all {len(laws)} laws...")

        for i, law in enumerate(laws):
            law_id = law["id_documento"]
            title = law["titulo_documento"]

            logger.info(f"[{i + 1}/{len(laws)}] Fetching law {law_id}: {title[:60]}...")
            self.rate_limiter.wait()
            page_data = fetch_law_page(law_id, title)

            if not page_data or not page_data.get("text"):
                continue

            page_data["law_id"] = law_id
            page_data["title"] = title
            yield page_data

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = BACNScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command in ("test", "test-api"):
        print("Testing BACN search endpoint...")
        try:
            laws = fetch_law_index()
            print(f"  OK: {len(laws)} laws in index")
            if laws:
                law = laws[0]
                page = fetch_law_page(law["id_documento"], law["titulo_documento"])
                if page and page.get("text"):
                    print(f"  OK: Got {len(page['text'])} chars of text")
                else:
                    print("  WARN: No text from first law")
            print("Test PASSED")
        except Exception as e:
            print(f"  FAIL: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
