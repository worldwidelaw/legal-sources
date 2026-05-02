#!/usr/bin/env python3
"""
NG/SEC -- Nigeria Securities and Exchange Commission Circulars

Fetches regulatory circulars from the Nigeria SEC website.

Strategy:
  - Scrape listing page at /for-investors/keep-track-of-circulars/
  - Extract circular URLs, titles, and dates from HTML listing
  - Fetch each individual page and extract paragraph text
  - If PDF attachments exist, download and extract text via pdfplumber
  - ~244 circulars in English (2015-present)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import json
import re
import sys
import time
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html.parser import HTMLParser

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.SEC")

BASE_URL = "https://sec.gov.ng"
LISTING_URL = f"{BASE_URL}/for-investors/keep-track-of-circulars/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def parse_date(date_str: str) -> Optional[str]:
    """Parse 'Month DD, YYYY' to ISO date."""
    m = re.match(r"(\w+)\s+(\d{1,2}),?\s*(\d{4})", date_str.strip())
    if not m:
        return None
    month = MONTH_MAP.get(m.group(1).lower())
    if not month:
        return None
    return f"{m.group(3)}-{month:02d}-{int(m.group(2)):02d}"


def strip_html(html: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<br\s*/?>", "\n", html)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class NGSECScraper(BaseScraper):
    """
    Scraper for NG/SEC -- Nigeria Securities and Exchange Commission.
    Country: NG
    URL: https://sec.gov.ng/for-investors/keep-track-of-circulars/

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_listing(self) -> list[dict]:
        """Fetch the circulars listing page and extract items."""
        r = self.session.get(LISTING_URL, timeout=30)
        r.raise_for_status()
        html = r.text

        items = []
        # Pattern: <a href="/for-investors/keep-track-of-circulars/SLUG/" ...>
        #   <span class="...">Title</span>
        #   <span class="font-normal...">Subtitle</span>
        #   <span class="uppercase">Date</span>
        # </a>
        pattern = re.compile(
            r'<a\s+href="(/for-investors/keep-track-of-circulars/[a-z0-9-]+/)"'
            r'[^>]*>'
            r'(.*?)</a>',
            re.DOTALL,
        )
        for match in pattern.finditer(html):
            url_path = match.group(1)
            block = match.group(2)

            # Extract spans
            spans = re.findall(r"<span[^>]*>(.*?)</span>", block, re.DOTALL)
            title = strip_html(spans[0]) if spans else ""
            # Remove "Article - " prefix
            title = re.sub(r"^Article\s*[-–]\s*", "", title).strip()

            subtitle = strip_html(spans[1]) if len(spans) > 1 else ""

            date_str = ""
            date_iso = None
            if len(spans) > 2:
                date_str = strip_html(spans[2])
                date_iso = parse_date(date_str)
            # Some items have date in span index 3
            if not date_iso and len(spans) > 3:
                date_str = strip_html(spans[3])
                date_iso = parse_date(date_str)

            if not title:
                continue

            slug = url_path.strip("/").split("/")[-1]

            items.append({
                "url_path": url_path,
                "slug": slug,
                "title": title,
                "subtitle": subtitle,
                "date": date_iso,
            })

        # Dedupe by URL
        seen = set()
        deduped = []
        for item in items:
            if item["url_path"] not in seen:
                seen.add(item["url_path"])
                deduped.append(item)

        return deduped

    def _fetch_page_content(self, url_path: str) -> dict:
        """Fetch an individual circular page and extract content."""
        url = f"{BASE_URL}{url_path}"
        r = self.session.get(url, timeout=30)
        r.raise_for_status()
        html = r.text

        # Extract paragraphs with substantial text
        paras = re.findall(r"<p[^>]*>(.*?)</p>", html, re.DOTALL)
        text_parts = []
        for p in paras:
            clean = strip_html(p)
            if len(clean) > 15:
                text_parts.append(clean)

        # Extract list items
        lis = re.findall(r"<li[^>]*>(.*?)</li>", html, re.DOTALL)
        for li in lis:
            clean = strip_html(li)
            if len(clean) > 15 and clean not in text_parts:
                text_parts.append(f"- {clean}")

        # Find PDF download links
        pdf_urls = re.findall(r'href="([^"]+\.pdf)"', html)

        return {
            "text": "\n\n".join(text_parts),
            "pdf_urls": pdf_urls,
        }

    def _download_pdf_text(self, pdf_path: str) -> Optional[str]:
        """Download and extract text from a PDF."""
        url = pdf_path if pdf_path.startswith("http") else f"{BASE_URL}{pdf_path}"
        try:
            import pdfplumber
            r = self.session.get(url, timeout=120)
            r.raise_for_status()
            if not r.content[:5].startswith(b"%PDF"):
                return None

            with tempfile.NamedTemporaryFile(suffix=".pdf") as f:
                f.write(r.content)
                f.flush()
                pdf = pdfplumber.open(f.name)
                pages = []
                for page in pdf.pages:
                    text = page.extract_text() or ""
                    if text.strip():
                        pages.append(text)
                pdf.close()
                return "\n\n".join(pages) if pages else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_path[:60]}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw circular record into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        slug = raw.get("slug", "")
        title = raw.get("title", "").strip()

        return {
            "_id": slug or title[:80],
            "_source": "NG/SEC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": f"{BASE_URL}{raw.get('url_path', '')}",
            "subtitle": raw.get("subtitle", ""),
            "institution": "Securities and Exchange Commission, Nigeria",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all circulars."""
        items = self._fetch_listing()
        logger.info(f"Found {len(items)} circulars on listing page")

        yielded = 0
        errors = 0
        for i, item in enumerate(items):
            try:
                page = self._fetch_page_content(item["url_path"])
                text = page["text"]

                # If HTML text is short and PDF exists, try PDF
                if len(text) < 200 and page["pdf_urls"]:
                    for pdf_url in page["pdf_urls"][:1]:
                        pdf_text = self._download_pdf_text(pdf_url)
                        if pdf_text:
                            text = pdf_text
                            break

                item["text"] = text
                yield item
                yielded += 1

                if (i + 1) % 20 == 0:
                    logger.info(f"  Progress: {i+1}/{len(items)} fetched, {yielded} yielded")

            except Exception as e:
                logger.warning(f"Error fetching {item['slug']}: {e}")
                errors += 1

            time.sleep(1)

        logger.info(f"Finished: {yielded}/{len(items)} circulars yielded, {errors} errors")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent circulars."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        items = self._fetch_listing()
        for item in items:
            if item.get("date") and item["date"] >= since_str:
                try:
                    page = self._fetch_page_content(item["url_path"])
                    text = page["text"]
                    if len(text) < 200 and page["pdf_urls"]:
                        for pdf_url in page["pdf_urls"][:1]:
                            pdf_text = self._download_pdf_text(pdf_url)
                            if pdf_text:
                                text = pdf_text
                                break
                    item["text"] = text
                    yield item
                    time.sleep(1)
                except Exception as e:
                    logger.warning(f"Error fetching {item['slug']}: {e}")


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="NG/SEC data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = NGSECScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            items = scraper._fetch_listing()
            logger.info(f"OK: Found {len(items)} circulars on listing page")
            if items:
                item = items[0]
                logger.info(f"First: {item['title'][:80]}")
                logger.info(f"  Date: {item.get('date')}, URL: {item['url_path']}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
