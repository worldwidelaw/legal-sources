#!/usr/bin/env python3
"""
INTL/SICE -- OAS Foreign Trade Information System

Fetches full text of trade agreements from sice.oas.org.
Over 150 bilateral/multilateral trade agreements for OAS member states.

Strategy:
  - Crawl agreements index page to get all agreement index URLs
  - For each agreement, find full-text ASP pages (skip PDFs)
  - Extract clean text from HTML pages
  - Each agreement = one record with concatenated full text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import html
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, urlparse

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.SICE")

BASE_URL = "http://www.sice.oas.org"
AGREEMENTS_URL = f"{BASE_URL}/agreements_e.asp"

# Known agreement categories on the main page
SKIP_LINK_PATTERNS = [
    r'css', r'\.js', r'\.png', r'\.jpg', r'\.ico', r'\.gif',
    r'facebook\.com', r'twitter\.com', r'youtube\.com',
    r'oas\.org/en', r'oas\.org/ext',
    r'search', r'sitemap', r'disclaim', r'Default',
    r'countries_e', r'resources_e', r'disciplines_e', r'tpd_e',
    r'msme', r'news_e', r'sice-oas\.org', r'RSS',
    r'agreements_e\.asp$', r'agreements_s\.asp$',
    r'clearinghouse',
]


def clean_html_text(raw_html: str) -> str:
    """Strip HTML tags and clean text from an HTML page."""
    if not raw_html:
        return ""
    # Remove script and style blocks
    text = re.sub(r'<script[^>]*>.*?</script>', '', raw_html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    # Remove HTML comments
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    # Remove all tags (including multi-line tags)
    text = re.sub(r'<[^>]+>', ' ', text, flags=re.DOTALL)
    # Decode HTML entities
    text = html.unescape(text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]*\n', '\n\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_body_text(raw_html: str) -> str:
    """Extract meaningful text from agreement page, skipping nav/headers."""
    if not raw_html:
        return ""
    # Try to find the main content area
    # Many SICE pages use <td class="MainText"> or similar
    # Fall back to full body text
    body_match = re.search(r'<body[^>]*>(.*)</body>', raw_html, re.DOTALL | re.IGNORECASE)
    if body_match:
        body = body_match.group(1)
    else:
        body = raw_html

    # Remove navigation header (everything before the main content table)
    # The navigation typically contains links to Agreements, Disciplines, etc.
    nav_end = re.search(
        r'(?:What\'s New|What.s New|SiteMap.*?Search)', body, re.IGNORECASE
    )
    if nav_end:
        body = body[nav_end.end():]

    # Remove footer area
    footer_start = re.search(
        r'(?:SICE.*?Sitemap.*?Disclaimer|Copyright|footer)', body, re.IGNORECASE
    )
    if footer_start:
        body = body[:footer_start.start()]

    text = clean_html_text(body)

    # Remove residual nav items that slipped through
    lines = text.split('\n')
    clean_lines = []
    for line in lines:
        stripped = line.strip()
        if not stripped:
            clean_lines.append('')
            continue
        # Skip very short nav-like lines
        if len(stripped) < 5 and not any(c.isalpha() for c in stripped):
            continue
        clean_lines.append(line)

    return '\n'.join(clean_lines).strip()


def is_agreement_link(href: str) -> bool:
    """Check if a link is an agreement index/text link (not nav/footer)."""
    if not href:
        return False
    for pattern in SKIP_LINK_PATTERNS:
        if re.search(pattern, href, re.IGNORECASE):
            return False
    return True


def extract_title_from_link(link_text: str, href: str) -> str:
    """Extract a clean title from link text or URL path."""
    if link_text:
        clean = re.sub(r'\s+', ' ', link_text).strip()
        if len(clean) > 5:
            return clean
    # Fall back to path-based title
    path = urlparse(href).path
    parts = path.strip('/').split('/')
    if len(parts) >= 2:
        return parts[-2].replace('_', ' ').replace('-', ' ')
    return parts[-1].replace('_', ' ') if parts else "Unknown Agreement"


class SICEScraper(BaseScraper):
    """
    Scraper for INTL/SICE -- OAS Foreign Trade Information System.
    Country: INTL
    URL: http://www.sice.oas.org/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research project)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_page(self, url: str) -> Optional[str]:
        """Fetch a page with retry and rate limiting."""
        for attempt in range(3):
            try:
                r = self.session.get(url, timeout=30)
                r.raise_for_status()
                # Handle encoding: SICE uses various encodings
                if r.encoding and r.encoding.lower() == 'iso-8859-1':
                    r.encoding = 'utf-8'
                return r.text
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(3)
        return None

    def _get_agreement_index_urls(self) -> list:
        """Get all agreement index URLs from the main agreements page."""
        logger.info("Fetching agreements index page...")
        page = self._get_page(AGREEMENTS_URL)
        if not page:
            logger.error("Failed to fetch agreements index")
            return []

        # Extract all links
        links = re.findall(r'href="([^"]*)"', page, re.IGNORECASE)
        agreement_urls = []
        seen = set()

        for href in links:
            if not is_agreement_link(href):
                continue
            # Skip PDFs
            if href.lower().endswith('.pdf'):
                continue
            # Resolve relative URLs
            full_url = urljoin(AGREEMENTS_URL, href)
            # Normalize
            full_url = full_url.split('#')[0]  # Remove anchors
            if full_url in seen:
                continue
            seen.add(full_url)

            # Also capture the link text for title extraction
            # Find link text from HTML
            pattern = re.escape(href) + r'[^>]*>(?:<[^>]*>)*\s*([^<]+)'
            match = re.search(pattern, page)
            link_text = match.group(1).strip() if match else ""

            agreement_urls.append((full_url, link_text))

        logger.info(f"Found {len(agreement_urls)} agreement index URLs")
        return agreement_urls

    def _get_chapter_urls(self, index_url: str) -> list:
        """Get all chapter/text page URLs from an agreement index page."""
        page = self._get_page(index_url)
        if not page:
            return []
        time.sleep(1)

        links = re.findall(r'href="([^"]*)"', page, re.IGNORECASE)
        chapter_urls = []
        seen = set()

        for href in links:
            if not href:
                continue
            # Only ASP pages with text content
            if not href.lower().endswith('.asp'):
                continue
            # Skip nav links
            if not is_agreement_link(href):
                continue
            # Skip back-links to the main agreements/countries pages
            if re.search(r'agreements_e|countries_e|disciplines_e|tpd_e', href, re.IGNORECASE):
                continue

            full_url = urljoin(index_url, href)
            full_url = full_url.split('#')[0]
            if full_url in seen:
                continue
            if full_url == index_url:
                continue
            seen.add(full_url)
            chapter_urls.append(full_url)

        return chapter_urls

    def _fetch_agreement_text(self, index_url: str, page: str = None) -> Optional[str]:
        """Fetch full text of an agreement (possibly across multiple pages)."""
        # First, try to get text from the index page itself
        if page is None:
            page = self._get_page(index_url)
            if not page:
                return None
            time.sleep(1)

        index_text = extract_body_text(page)

        # Check if this page IS the full text (long enough)
        if len(index_text) > 2000:
            return index_text

        # If short, it's likely a table of contents — get chapter pages
        chapter_urls = self._get_chapter_urls(index_url)
        if not chapter_urls:
            # Return whatever text the index page had
            return index_text if len(index_text) > 200 else None

        # Fetch each chapter (limit to avoid huge agreements eating all time)
        all_text_parts = []
        if index_text and len(index_text) > 100:
            all_text_parts.append(index_text)

        for i, ch_url in enumerate(chapter_urls[:50]):  # Max 50 chapters
            ch_page = self._get_page(ch_url)
            if ch_page:
                ch_text = extract_body_text(ch_page)
                if ch_text and len(ch_text) > 50:
                    all_text_parts.append(ch_text)
            time.sleep(1)
            if i % 10 == 0 and i > 0:
                logger.info(f"  Fetched {i}/{len(chapter_urls)} chapters...")

        if not all_text_parts:
            return None

        return "\n\n---\n\n".join(all_text_parts)

    def _extract_agreement_title(self, url: str, link_text: str, page_html: str) -> str:
        """Extract agreement title from page or link text."""
        # Try the HTML title tag
        if page_html:
            title_match = re.search(r'<title[^>]*>([^<]+)</title>', page_html, re.IGNORECASE)
            if title_match:
                title = title_match.group(1).strip()
                # Remove "SICE - " prefix
                title = re.sub(r'^SICE\s*[-–—]\s*', '', title)
                if title and len(title) > 5:
                    return title

        if link_text and len(link_text) > 5:
            return link_text

        return extract_title_from_link(link_text, url)

    def _extract_date_from_text(self, text: str) -> Optional[str]:
        """Try to extract a date from agreement text."""
        # Look for common date patterns in agreement preambles
        patterns = [
            r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
            r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})',
        ]
        month_map = {
            'January': '01', 'February': '02', 'March': '03', 'April': '04',
            'May': '05', 'June': '06', 'July': '07', 'August': '08',
            'September': '09', 'October': '10', 'November': '11', 'December': '12',
        }
        for pattern in patterns:
            match = re.search(pattern, text[:3000])
            if match:
                groups = match.groups()
                try:
                    if groups[1] in month_map:
                        # Format: DD Month YYYY
                        day = int(groups[0])
                        month = month_map[groups[1]]
                        year = groups[2]
                    else:
                        # Format: Month DD, YYYY
                        month = month_map[groups[0]]
                        day = int(groups[1])
                        year = groups[2]
                    return f"{year}-{month}-{day:02d}"
                except (ValueError, KeyError):
                    continue
        return None

    def _extract_parties(self, title: str, url: str) -> Optional[str]:
        """Extract country parties from agreement title or URL path."""
        # Try URL path for country codes
        path = urlparse(url).path
        parts = path.strip('/').split('/')
        for part in parts:
            # Look for country code patterns like CAN_COL, USA_KOR
            if re.match(r'^[A-Z]{2,4}_[A-Z]{2,4}', part):
                codes = part.split('_')
                return ', '.join(c for c in codes if len(c) <= 4 and c.isalpha())
        return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw agreement data into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 200:
            return None

        title = html.unescape(raw.get("title", "Unknown Agreement"))
        url = raw.get("url", "")
        date = raw.get("date") or self._extract_date_from_text(text)
        parties = raw.get("parties") or self._extract_parties(title, url)

        # Create a stable ID from the URL path
        path = urlparse(url).path.strip('/')
        doc_id = re.sub(r'[^a-zA-Z0-9_/-]', '_', path)

        return {
            "_id": f"SICE-{doc_id}",
            "_source": "INTL/SICE",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "parties": parties,
            "agreement_type": raw.get("agreement_type", "trade_agreement"),
            "language": "en",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all trade agreements from SICE."""
        agreement_urls = self._get_agreement_index_urls()
        total = 0

        for i, (url, link_text) in enumerate(agreement_urls):
            logger.info(f"[{i+1}/{len(agreement_urls)}] Fetching: {url}")

            page = self._get_page(url)
            title = self._extract_agreement_title(url, link_text, page)
            time.sleep(1)

            text = self._fetch_agreement_text(url, page=page)
            if not text or len(text) < 200:
                logger.warning(f"  Skipping (no/short text): {title}")
                continue

            yield {
                "title": title,
                "text": text,
                "url": url,
                "date": None,
                "parties": self._extract_parties(title, url),
            }
            total += 1
            logger.info(f"  OK: {title} ({len(text)} chars)")

        logger.info(f"Total agreements fetched: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Static dataset — re-fetch all."""
        yield from self.fetch_all()


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/SICE data fetcher")
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

    scraper = SICEScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            r = scraper.session.get(AGREEMENTS_URL, timeout=30)
            r.raise_for_status()
            logger.info(f"OK: SICE reachable, status {r.status_code}")
            # Quick check: can we find agreement links?
            links = re.findall(r'href="[^"]*Trade/[^"]*\.asp"', r.text)
            logger.info(f"Found {len(links)} Trade/ links on agreements page")
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
