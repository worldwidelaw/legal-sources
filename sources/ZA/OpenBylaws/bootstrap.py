#!/usr/bin/env python3
"""
ZA/OpenBylaws -- South Africa Open By-laws Fetcher

Fetches South African municipal by-laws from openbylaws.org.za.
Full text is served as structured Akoma Ntoso HTML (server-rendered).

Strategy:
  1. Fetch /legislation/ to discover all municipality pages
  2. For each municipality, fetch the listing page to get by-law URLs
  3. For each by-law, fetch the HTML page and extract text from akn-* elements

Data:
  - ~367 by-laws across 11 municipalities
  - Full text in Akoma Ntoso HTML markup
  - License: CC BY-NC 4.0 (Laws.Africa / AfricanLII)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZA.OpenBylaws")

BASE_URL = "https://openbylaws.org.za"


class ZAOpenBylawsScraper(BaseScraper):
    """
    Scraper for ZA/OpenBylaws -- South Africa Open By-laws.
    Country: ZA
    URL: https://openbylaws.org.za

    Data types: legislation
    Auth: none (public website, CC BY-NC 4.0)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research; CC BY-NC 4.0 compliance)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=60,
        )

    def _discover_municipalities(self) -> List[str]:
        """Fetch the main listing page and extract municipality codes."""
        logger.info("Discovering municipalities from /legislation/")
        resp = self.client.get("/legislation/")
        resp.raise_for_status()
        html = resp.text
        codes = re.findall(r'href="/legislation/(za-[^/]+)/"', html)
        unique = sorted(set(codes))
        logger.info(f"Found {len(unique)} municipalities: {unique}")
        return unique

    def _discover_bylaws(self, municipality: str) -> List[Dict[str, str]]:
        """Fetch a municipality listing page and extract by-law URLs."""
        logger.info(f"Discovering by-laws for {municipality}")
        resp = self.client.get(f"/legislation/{municipality}/")
        resp.raise_for_status()
        html = resp.text

        # Match by-law links like /akn/za-cpt/act/by-law/2002/community-fire-safety/eng@2015-08-21
        pattern = rf'href="(/akn/{re.escape(municipality)}/act/by-law/[^"]+)"'
        links = re.findall(pattern, html)
        unique = sorted(set(links))
        logger.info(f"  {municipality}: {len(unique)} by-laws found")

        results = []
        for link in unique:
            results.append({
                "municipality": municipality,
                "path": link,
                "url": f"{BASE_URL}{link}",
            })
        return results

    def _strip_html(self, html: str) -> str:
        """Strip HTML tags and clean up text content."""
        # Remove script and style elements
        text = re.sub(r'<(script|style)[^>]*>.*?</\1>', '', html, flags=re.DOTALL | re.IGNORECASE)
        # Replace block elements with newlines
        text = re.sub(r'</(p|div|section|h[1-6]|li|tr|br|blockquote)>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        # Remove remaining tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode HTML entities
        text = unescape(text)
        # Clean whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_bylaw_content(self, html: str) -> Dict[str, Optional[str]]:
        """Extract title and full text from a by-law HTML page."""
        # Extract title from <h1>
        title = None
        title_m = re.search(r'<h1[^>]*>\s*<span>([^<]+)</span>', html, re.DOTALL)
        if title_m:
            title = unescape(title_m.group(1).strip())

        # Extract main body from the akn-act or akn-body container
        text = None

        # Try to find the akn-act div which contains the full structured text
        akn_m = re.search(
            r'<div[^>]*class="[^"]*akn-act[^"]*"[^>]*>(.*?)</div>\s*</div>\s*</div>\s*</main>',
            html, re.DOTALL
        )
        if not akn_m:
            # Fallback: find akn-body
            akn_m = re.search(
                r'<span[^>]*class="[^"]*akn-body[^"]*"[^>]*>(.*?)</span>\s*(?:</div>)',
                html, re.DOTALL
            )
        if not akn_m:
            # Broader fallback: everything between akn-act start and </main>
            start = html.find('class="akn-act"')
            if start > 0:
                # Find the opening tag
                tag_start = html.rfind('<', 0, start)
                end = html.find('</main>', start)
                if tag_start > 0 and end > 0:
                    akn_m_text = html[tag_start:end]
                    text = self._strip_html(akn_m_text)

        if akn_m and text is None:
            text = self._strip_html(akn_m.group(1))

        return {"title": title, "text": text}

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw by-law data into standard schema."""
        path = raw["path"]

        # Parse ID from path: /akn/za-cpt/act/by-law/2002/community-fire-safety/eng@2015-08-21
        # -> za-cpt_by-law_2002_community-fire-safety
        path_m = re.match(
            r'/akn/(za-[^/]+)/act/by-law/(\d{4})/([^/]+)/eng@(\d{4}-\d{2}-\d{2})',
            path
        )
        if path_m:
            muni, year, slug, version_date = path_m.groups()
            doc_id = f"{muni}_by-law_{year}_{slug}"
        else:
            # Fallback ID from path
            doc_id = path.replace("/", "_").strip("_")
            muni = raw.get("municipality", "unknown")
            year = None
            version_date = None

        return {
            "_id": doc_id,
            "_source": "ZA/OpenBylaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or doc_id,
            "text": raw.get("text", ""),
            "date": version_date,
            "url": raw["url"],
            "municipality": muni,
            "year": int(year) if year else None,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all by-laws from all municipalities."""
        municipalities = self._discover_municipalities()

        for muni in municipalities:
            try:
                bylaws = self._discover_bylaws(muni)
            except Exception as e:
                logger.error(f"Failed to list by-laws for {muni}: {e}")
                continue

            for bylaw_info in bylaws:
                try:
                    resp = self.client.get(bylaw_info["path"])
                    resp.raise_for_status()
                    content = self._extract_bylaw_content(resp.text)

                    text = content.get("text") or ""
                    if len(text) < 50:
                        logger.warning(f"Skipping {bylaw_info['path']}: no/short text ({len(text)} chars)")
                        continue

                    raw = {
                        **bylaw_info,
                        "title": content["title"],
                        "text": text,
                    }

                    yield raw

                except Exception as e:
                    logger.error(f"Failed to fetch {bylaw_info['path']}: {e}")
                    continue

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Incremental updates not supported — yields nothing."""
        logger.info("Incremental updates not supported for OpenBylaws; use bootstrap")
        return
        yield  # make it a generator


def main():
    import argparse
    parser = argparse.ArgumentParser(description="ZA/OpenBylaws data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode: fetch ~15 records")
    args = parser.parse_args()

    scraper = ZAOpenBylawsScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        munis = scraper._discover_municipalities()
        if munis:
            bylaws = scraper._discover_bylaws(munis[0])
            logger.info(f"Test passed: found {len(munis)} municipalities, {len(bylaws)} by-laws in {munis[0]}")
        else:
            logger.error("Test FAILED: no municipalities found")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            scraper.run_sample(n=15)
        else:
            scraper.bootstrap()

    elif args.command == "update":
        scraper.bootstrap()


if __name__ == "__main__":
    main()
