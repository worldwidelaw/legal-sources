#!/usr/bin/env python3
"""
EE/EMTA-TaxGuidance -- Estonian Tax and Customs Board Official Tax Guidance

Fetches substantive tax guidance pages from the EMTA (Maksu- ja Tolliamet)
English-language website. Covers CIT (unique 0%/20% system), VAT, social tax,
excise duties, customs, non-resident taxation, and e-residency implications.

Source: https://www.emta.ee/en/business-client/ and /en/private-client/
Volume: ~120 guidance pages with 1,000-5,000 words each

Strategy:
  - Fetch sitemap page to discover all guidance URLs
  - Focus on taxes-and-payment and registration sections
  - Extract main content area, strip navigation/footer
  - Each page = one doctrine record

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Re-fetch all (content may change)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EE.EMTA-TaxGuidance")

BASE_URL = "https://www.emta.ee"
SITEMAP_URL = f"{BASE_URL}/en/sitemap"
DELAY = 1.5

# Sections to include (tax guidance content)
INCLUDE_PATHS = [
    "/en/business-client/taxes-and-payment/",
    "/en/business-client/customs-trade-goods/",
    "/en/business-client/registration-business/non-residents-e-residents/",
    "/en/business-client/registration-business/businesses/",
    "/en/business-client/e-services-training-courses/advice/",
    "/en/private-client/taxes-and-payment/",
    "/en/private-client/foreigner-non-resident/",
]

# Exclude pure category/listing pages (no substantive content)
EXCLUDE_PATTERNS = [
    r"/en/[^/]+$",  # Top-level section pages
    r"/contacts?(/|$)",
    r"/service-points",
    r"/customer-support",
    r"/fraud-hotline",
    r"/how-use-e-services",
    r"/technical-",
    r"/scheduled-maintenance",
    r"/news-services",
]

USER_AGENT = "LegalDataHunter/1.0 (academic research; open legal data)"


def _fetch(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch a URL and return text content."""
    req = Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en,et;q=0.5",
    })
    try:
        resp = urlopen(req, timeout=timeout)
        return resp.read().decode("utf-8", errors="replace")
    except (HTTPError, URLError, Exception) as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def _strip_html(raw_html: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    text = re.sub(r'<(style|script|nav|header|footer)[^>]*>.*?</\1>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<(br|p|div|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def _extract_main_content(html: str) -> str:
    """Extract main content area from EMTA page."""
    # Try to find main content block
    # EMTA uses Drupal: look for <main> or <article> or content region
    main_match = re.search(r'<main[^>]*>(.*?)</main>', html, re.DOTALL | re.IGNORECASE)
    if main_match:
        content = main_match.group(1)
    else:
        # Fallback: look for article or content div
        article_match = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL | re.IGNORECASE)
        if article_match:
            content = article_match.group(1)
        else:
            # Last resort: use region after breadcrumb
            breadcrumb_end = html.find('</nav>')
            if breadcrumb_end > 0:
                # Find content between last nav close and footer
                footer_start = html.find('<footer')
                if footer_start > breadcrumb_end:
                    content = html[breadcrumb_end:footer_start]
                else:
                    content = html[breadcrumb_end:]
            else:
                content = html

    # Remove sidebar navigation if present
    content = re.sub(r'<nav[^>]*>.*?</nav>', '', content, flags=re.DOTALL | re.IGNORECASE)
    # Remove breadcrumb
    content = re.sub(r'<[^>]*class="[^"]*breadcrumb[^"]*"[^>]*>.*?</[^>]+>', '', content, flags=re.DOTALL | re.IGNORECASE)

    text = _strip_html(content)

    # Remove common footer/boilerplate
    for marker in ["Was this article helpful?", "Updated:", "Did you find"]:
        idx = text.rfind(marker)
        if idx > len(text) * 0.7:
            text = text[:idx].rstrip()

    return text


def _extract_title(html: str) -> str:
    """Extract page title from HTML."""
    # Try <h1>
    h1 = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL | re.IGNORECASE)
    if h1:
        return _strip_html(h1.group(1))[:200]
    # Try <title>
    title = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL | re.IGNORECASE)
    if title:
        t = _strip_html(title.group(1))
        # Remove site suffix
        t = re.sub(r'\s*\|\s*Estonian Tax.*$', '', t)
        return t[:200]
    return "Unknown"


def _path_to_id(path: str) -> str:
    """Convert URL path to a stable document ID."""
    # /en/business-client/taxes-and-payment/vat/general -> business_taxes-and-payment_vat_general
    path = path.rstrip("/")
    parts = path.split("/")
    # Skip /en/ and client type prefix
    if len(parts) > 3:
        relevant = parts[3:]  # Skip '', 'en', 'business-client'
    else:
        relevant = parts[1:]
    return "_".join(relevant)


class EMTATaxGuidance(BaseScraper):
    SOURCE_ID = "EE/EMTA-TaxGuidance"

    def __init__(self):
        source_dir = str(Path(__file__).resolve().parent)
        super().__init__(source_dir)

    def _discover_pages(self) -> List[str]:
        """Discover all guidance page URLs from sitemap."""
        html = _fetch(SITEMAP_URL)
        if not html:
            logger.error("Failed to fetch sitemap")
            return []

        # Extract all internal links
        links = re.findall(r'href="(/en/[^"]+)"', html)
        links = list(dict.fromkeys(links))  # deduplicate preserving order

        # Filter to relevant sections
        pages = []
        for link in links:
            if any(link.startswith(prefix) for prefix in INCLUDE_PATHS):
                if not any(re.search(pat, link) for pat in EXCLUDE_PATTERNS):
                    pages.append(link)

        logger.info(f"Discovered {len(pages)} guidance pages from sitemap")
        return pages

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all tax guidance pages."""
        pages = self._discover_pages()

        for i, path in enumerate(pages):
            url = BASE_URL + path
            time.sleep(DELAY)

            html = _fetch(url)
            if not html:
                continue

            text = _extract_main_content(html)
            if len(text) < 300:
                logger.debug(f"Skipping {path} (only {len(text)} chars)")
                continue

            title = _extract_title(html)
            doc_id = _path_to_id(path)

            yield self.normalize({
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "url": url,
                "path": path,
            })

            if (i + 1) % 10 == 0:
                logger.info(f"Progress: {i + 1}/{len(pages)} pages fetched")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Re-fetch all pages (content updates have no date signal)."""
        return self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": raw["doc_id"],
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "url": raw["url"],
        }


# ─── CLI Entry Point ─────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="EE/EMTA-TaxGuidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    args = parser.parse_args()

    scraper = EMTATaxGuidance()

    if args.command == "test":
        html = _fetch(SITEMAP_URL)
        if html:
            links = re.findall(r'href="(/en/business-client/taxes[^"]+)"', html)
            print(f"OK: Sitemap reachable, found {len(links)} tax-related links")
        else:
            print("FAIL: Cannot reach sitemap")
            sys.exit(1)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else 9999

    for record in scraper.fetch_all():
        if record is None:
            continue
        count += 1
        fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        logger.info(f"[{count}] {record['title'][:60]} ({text_len} chars)")

        if count >= limit:
            logger.info(f"Sample limit reached ({limit} records)")
            break

    print(f"\nDone: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
