#!/usr/bin/env python3
"""
INTL/OASTreaties -- OAS Inter-American Treaties Database

Fetches multilateral inter-American treaties from the Organization of
American States (OAS) Department of International Law.

Strategy:
  - Parse index page at /dil/treaties_year_text.htm for all treaty links
  - Fetch each treaty page (HTML) with session cookie
  - Extract full text from HTML body, handling JS redirects
  - ~76 treaties, 3-second delay between requests

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Same as bootstrap
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
from urllib.parse import urljoin

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.OASTreaties")

INDEX_URL = "https://www.oas.org/dil/treaties_year_text.htm"
MAIN_PAGE = "https://www.oas.org/en/sla/dil/inter_american_treaties.asp"


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, preserving paragraph breaks."""
    if not text:
        return ""
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_treaty_code(url: str) -> str:
    """Extract treaty code (e.g., A-52, B-32) from URL."""
    m = re.search(r'([ABCH]-\d+(?:\(\d+\))?)', url, re.IGNORECASE)
    return m.group(1).upper() if m else ""


class OASTreatiesScraper(BaseScraper):
    """
    Scraper for INTL/OASTreaties -- OAS Inter-American Treaties.
    Country: INTL
    URL: https://www.oas.org/en/sla/dil/inter_american_treaties.asp

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _init_session(self):
        """Visit main page to establish session cookie."""
        logger.info("Initializing session (getting cookie)...")
        r = self.session.get(MAIN_PAGE, timeout=30)
        r.raise_for_status()
        logger.info(f"Session initialized, cookies: {len(self.session.cookies)}")

    def _get_treaty_urls(self) -> list[dict]:
        """Parse index page to get all treaty URLs with metadata."""
        r = self.session.get(INDEX_URL, timeout=30)
        r.raise_for_status()

        all_hrefs = re.findall(r'href=["\']([^"\']+)["\']', r.text, re.IGNORECASE)

        treaties = []
        seen_codes = set()
        for href in all_hrefs:
            # Skip Spanish versions
            if 'spanish' in href.lower() or 'tratados' in href.lower():
                continue
            # Must be a treaty link
            if 'treat' not in href.lower() and 'jurid' not in href.lower():
                continue
            # Skip the main index
            if href.endswith('treaties.htm') or href.endswith('treaties_year_text.htm'):
                continue

            # Normalize URL
            if href.startswith('http'):
                url = href.replace('http://', 'https://')
            else:
                url = urljoin('https://www.oas.org/dil/', href)

            code = extract_treaty_code(url)
            if not code or code in seen_codes:
                continue
            seen_codes.add(code)

            treaties.append({"url": url, "code": code})

        return treaties

    def _fetch_treaty_text(self, url: str) -> tuple[str, str]:
        """Fetch a treaty page and extract title + text."""
        self.rate_limiter.wait()

        try:
            r = self.session.get(url, timeout=30)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return "", ""

        page_text = r.text

        # Handle JS redirect
        redirect_match = re.search(r'window\.location\s*=\s*["\']([^"\']+)["\']', page_text)
        if redirect_match:
            new_url = redirect_match.group(1)
            if not new_url.startswith('http'):
                new_url = urljoin(url, new_url)
            new_url = new_url.replace('http://', 'https://')
            time.sleep(3)
            try:
                r = self.session.get(new_url, timeout=30)
                r.raise_for_status()
                page_text = r.text
            except Exception as e:
                logger.warning(f"Failed to follow redirect {new_url}: {e}")
                return "", ""

        # Extract title from <title> tag
        title_match = re.search(r'<title[^>]*>(.*?)</title>', page_text, re.DOTALL | re.IGNORECASE)
        title = strip_html(title_match.group(1)) if title_match else ""

        # Extract body text
        body_match = re.search(r'<body[^>]*>(.*?)</body>', page_text, re.DOTALL | re.IGNORECASE)
        if body_match:
            text = strip_html(body_match.group(1))
        else:
            text = strip_html(page_text)

        # Clean common OAS boilerplate
        text = re.sub(r'(?:Department of International Law|Departamento de Derecho Internacional)\s*$', '', text)

        return title, text

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw treaty record into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 2000:
            return None

        # Check that text has real content (not just nav/boilerplate)
        # Real treaty text has "Article" or "ARTICLE" mentions
        article_count = len(re.findall(r'\b(?:Article|ARTICLE)\b', text))
        if article_count < 2:
            return None

        title = raw.get("title", "").strip()
        code = raw.get("code", "")
        # Clean common title prefixes
        title = re.sub(r'^.*?(?:Treaties & Agreements?\s*>\s*|Inter.American Treaties\s*>\s*)', '', title)
        title = re.sub(r'^::\s*', '', title)
        title = title.strip()
        if not title or len(title) < 10:
            title = f"OAS Treaty {code}"

        return {
            "_id": f"OAS-{code}",
            "_source": "INTL/OASTreaties",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,
            "url": raw.get("url", ""),
            "treaty_code": code,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all treaties with full text."""
        self._init_session()
        time.sleep(3)

        treaties = self._get_treaty_urls()
        logger.info(f"Found {len(treaties)} treaty URLs")

        for i, treaty in enumerate(treaties):
            url = treaty["url"]
            code = treaty["code"]
            logger.info(f"[{i+1}/{len(treaties)}] Fetching {code}: {url}")

            title, text = self._fetch_treaty_text(url)

            if len(text) < 200:
                logger.warning(f"Skipping {code}: text too short ({len(text)} chars)")
                continue

            yield {
                "title": title,
                "text": text,
                "code": code,
                "url": url,
            }

        logger.info("All treaties fetched")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental updates — re-fetch all."""
        yield from self.fetch_all()


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/OASTreaties data fetcher")
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

    scraper = OASTreatiesScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            scraper._init_session()
            time.sleep(3)
            treaties = scraper._get_treaty_urls()
            logger.info(f"Found {len(treaties)} treaties in index")

            if treaties:
                t = treaties[0]
                title, text = scraper._fetch_treaty_text(t["url"])
                logger.info(f"Sample: {t['code']} | title={title[:60]} | text_len={len(text)}")

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
