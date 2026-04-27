#!/usr/bin/env python3
"""
LC/Legislation -- Saint Lucia Revised Laws (Attorney General)

Fetches the Revised Laws of Saint Lucia (2023) from the Attorney General Chambers
website. Full text is extracted section by section from individual act pages.

Strategy:
  1. Crawl index page to get act URLs (A-Z letter pages)
  2. For each act, crawl the table-of-contents page for section/schedule links
  3. Download each section page and extract the legal text
  4. Combine all sections into a single full-text record per act

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import logging
import time
import string
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LC.Legislation")

SOURCE_ID = "LC/Legislation"
BASE_URL = "https://attorneygeneralchambers.com"
INDEX_URL = f"{BASE_URL}/laws-of-saint-lucia"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"


class LCLegislationScraper(BaseScraper):
    """Scraper for LC/Legislation -- Saint Lucia Revised Laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def _get(self, url: str) -> Optional[requests.Response]:
        """GET with retry and rate limiting."""
        for attempt in range(3):
            self.rate_limiter.wait()
            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
                return resp
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = 10 * (2 ** attempt)
                logger.warning(f"Connection error (attempt {attempt+1}/3): {e}")
                time.sleep(wait)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code >= 500:
                    time.sleep(10 * (2 ** attempt))
                else:
                    logger.warning(f"HTTP error for {url}: {e}")
                    return None
        return None

    def _get_act_list(self) -> List[dict]:
        """Get all act URLs from the index page."""
        acts = {}

        # The index page shows all acts (letter filtering is client-side JS)
        resp = self._get(INDEX_URL)
        if resp is None:
            return []

        # Extract act links: /laws-of-saint-lucia/{slug}
        links = re.findall(
            r'href="(/laws-of-saint-lucia/([a-z][a-z0-9-]+))"',
            resp.text
        )
        for href, slug in links:
            # Skip non-act paths
            if slug in ('search', 'act') or '/' in slug:
                continue
            if slug.startswith('artifacts') or slug.startswith('images'):
                continue
            acts[slug] = {
                "slug": slug,
                "url": f"{BASE_URL}{href}",
            }

        # Extract titles from link text
        title_pattern = re.compile(
            r'href="/laws-of-saint-lucia/([a-z][a-z0-9-]+)"[^>]*>\s*([^<]+)',
            re.I
        )
        for match in title_pattern.finditer(resp.text):
            slug = match.group(1).lower()
            title = match.group(2).strip()
            if slug in acts and title and len(title) > 3:
                acts[slug]["title"] = title

        result = sorted(acts.values(), key=lambda a: a["slug"])
        logger.info(f"Found {len(result)} acts in index")
        return result

    def _get_section_links(self, act_url: str, act_slug: str) -> List[str]:
        """Get section and schedule links from an act's TOC page."""
        resp = self._get(act_url)
        if resp is None:
            return []

        # Extract section and schedule links
        pattern = re.compile(
            rf'href="(/laws-of-saint-lucia/{re.escape(act_slug)}/([^"]+))"'
        )
        links = []
        seen = set()
        for match in pattern.finditer(resp.text):
            href = match.group(1)
            subpath = match.group(2)
            if href not in seen:
                seen.add(href)
                links.append(href)

        return links

    def _extract_section_text(self, page_html: str) -> str:
        """Extract legal text content from a section page."""
        # Find content between 'Back to Table of Contents' marker and nav buttons
        idx_start = page_html.find('Back to Table of Contents')
        if idx_start < 0:
            # Try alternate marker
            idx_start = page_html.find('sf_colsIn')
            if idx_start < 0:
                return ""

        # Find the end — look for Previous/Next nav or footer
        chunk_start = idx_start
        idx_end = len(page_html)
        for end_marker in ['<a class="prev"', '<a class="next"',
                           'Previous', '<footer', 'id="footer"']:
            pos = page_html.find(end_marker, chunk_start + 30)
            if 0 < pos < idx_end:
                idx_end = pos

        chunk = page_html[chunk_start:idx_end]

        # Strip HTML tags
        text = re.sub(r'<[^>]+>', '\n', chunk)
        text = html_mod.unescape(text)
        lines = [l.strip() for l in text.split('\n')
                 if l.strip() and l.strip() != 'Back to Table of Contents']
        return '\n'.join(lines)

    def _fetch_act_text(self, act: dict) -> Optional[dict]:
        """Fetch full text for a single act by downloading all its sections."""
        act_slug = act["slug"]
        act_url = act["url"]

        section_links = self._get_section_links(act_url, act_slug)
        if not section_links:
            logger.warning(f"No sections found for {act_slug}")
            return None

        # Get the act title from the TOC page
        resp = self._get(act_url)
        title = act.get("title", "")
        amendments = ""
        if resp:
            # Extract title from page
            title_match = re.search(
                r'<h1[^>]*>\s*([^<]+)', resp.text
            )
            if title_match:
                t = title_match.group(1).strip()
                if t and len(t) > 3:
                    title = t

            # Extract amendment info
            amend_match = re.search(
                r'(Act\s+\d+\s+of\s+\d+.*?)(?:ARRANGEMENT|$)',
                re.sub(r'<[^>]+>', ' ', resp.text),
                re.S | re.I
            )
            if amend_match:
                amendments = ' '.join(amend_match.group(1).split())[:500]

        # Download each section
        all_parts = []
        for link in section_links:
            sec_resp = self._get(f"{BASE_URL}{link}")
            if sec_resp is None:
                continue

            text = self._extract_section_text(sec_resp.text)
            if text and len(text) > 10:
                all_parts.append(text)

        if not all_parts:
            logger.warning(f"No text extracted for {act_slug}")
            return None

        full_text = '\n\n'.join(all_parts)

        return {
            "slug": act_slug,
            "title": title or act_slug.replace('-', ' ').title(),
            "full_text": full_text,
            "section_count": len(all_parts),
            "amendments": amendments,
            "url": act_url,
        }

    def normalize(self, raw: dict) -> dict:
        slug = raw.get("slug", "")
        return {
            "_id": f"{SOURCE_ID}/{slug}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("full_text", ""),
            "date": "2023",
            "url": raw.get("url", ""),
            "act_slug": slug,
            "section_count": raw.get("section_count", 0),
            "amendments": raw.get("amendments", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        acts = self._get_act_list()
        limit = 15 if sample else None
        count = 0

        for act in acts:
            if limit and count >= limit:
                break

            logger.info(f"Fetching act: {act['slug']}")
            result = self._fetch_act_text(act)
            if result is None:
                continue

            text = result.get("full_text", "")
            if len(text) < 50:
                logger.warning(f"  Skipping {act['slug']} - text too short ({len(text)} chars)")
                continue

            yield result
            count += 1
            logger.info(f"  [{count}] {result['title'][:60]} ({len(text)} chars, {result['section_count']} sections)")

        logger.info(f"Total acts yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # No update mechanism — re-fetch all
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = LCLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        acts = scraper._get_act_list()
        if not acts:
            print("Connection FAILED - no acts found")
            sys.exit(1)
        print(f"Connection OK. Acts found: {len(acts)}")
        if acts:
            print(f"Sample: {acts[0]['slug']} - {acts[0].get('title', 'N/A')}")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
