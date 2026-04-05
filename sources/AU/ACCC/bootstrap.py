#!/usr/bin/env python3
"""
AU/ACCC -- Australian Competition and Consumer Commission Fetcher

Fetches ACCC public register entries and media releases.

Strategy:
  - Public register entries via Drupal JSON:API (authorisations, undertakings,
    infringement notices, merger reviews, notifications) — full text in field_accc_body
  - Media releases via HTML scraping — full text inline in article tags
  - JSON:API pagination is broken (400 on page 2+), so max 50 per node type
  - Combined yield from both sources for comprehensive coverage

Data:
  - ~250 public register entries (50 per type × 5 types)
  - ~7,700 media releases (paginated HTML scraping)
  - Full text inline for all documents
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent media releases
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
from typing import Generator, Optional, Dict, Any
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.ACCC")

BASE_URL = "https://www.accc.gov.au"
JSONAPI_BASE = f"{BASE_URL}/jsonapi"

# Drupal JSON:API node types with body text
NODE_TYPES = [
    ("acccgov_authorisation", "authorisation"),
    ("acccgov_notification", "notification"),
    ("acccgov_undertaking", "undertaking"),
    ("acccgov_infringement_notice", "infringement_notice"),
    ("acccgov_informal_merger_review", "merger_review"),
]

MEDIA_RELEASES_URL = f"{BASE_URL}/news-centre"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
    "Accept": "application/vnd.api+json, text/html",
}


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    # Remove tags
    clean = re.sub(r'<[^>]+>', ' ', text)
    # Decode entities
    clean = html_module.unescape(clean)
    # Normalize whitespace
    clean = re.sub(r'\s+', ' ', clean).strip()
    return clean


def _fetch_url(url: str, accept: str = "application/vnd.api+json") -> Optional[bytes]:
    """Fetch a URL with error handling."""
    headers = dict(HEADERS)
    headers["Accept"] = accept
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=30) as resp:
            return resp.read()
    except (URLError, HTTPError) as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


class AustraliaACCCScraper(BaseScraper):
    """
    Scraper for AU/ACCC -- Australian Competition and Consumer Commission.
    Country: AU
    URL: https://www.accc.gov.au/

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_jsonapi_nodes(self, node_type: str, doc_type: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch nodes from Drupal JSON:API for a given type."""
        url = f"{JSONAPI_BASE}/node/{node_type}"
        logger.info(f"Fetching JSON:API nodes: {node_type}")

        data = _fetch_url(url)
        if not data:
            return

        try:
            payload = json.loads(data)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for {node_type}: {e}")
            return

        nodes = payload.get("data", [])
        logger.info(f"Got {len(nodes)} {node_type} nodes")

        for node in nodes:
            attrs = node.get("attributes", {})
            body_field = attrs.get("field_accc_body") or attrs.get("body")

            body_html = ""
            if isinstance(body_field, dict):
                body_html = body_field.get("processed", "") or body_field.get("value", "")
            elif isinstance(body_field, str):
                body_html = body_field

            text = strip_html(body_html)
            if not text or len(text) < 50:
                continue

            path_alias = ""
            path_data = attrs.get("path")
            if isinstance(path_data, dict):
                path_alias = path_data.get("alias", "")

            yield {
                "uuid": node.get("id", ""),
                "title": attrs.get("title", ""),
                "body_html": body_html,
                "text": text,
                "created": attrs.get("created", ""),
                "changed": attrs.get("changed", ""),
                "path_alias": path_alias,
                "doc_type": doc_type,
                "node_type": node_type,
                "source": "jsonapi",
            }
            time.sleep(0.2)

    def _fetch_media_release_page(self, page: int) -> list:
        """Fetch a page of media release listings."""
        url = f"{MEDIA_RELEASES_URL}?page={page}"
        data = _fetch_url(url, accept="text/html")
        if not data:
            return []

        html_text = data.decode("utf-8", errors="replace")

        # Extract article links from listing page
        links = re.findall(r'href="(/media-release/[^"]+)"', html_text)
        # Deduplicate while preserving order
        seen = set()
        unique_links = []
        for link in links:
            if link not in seen:
                seen.add(link)
                unique_links.append(link)

        return unique_links

    def _fetch_media_release(self, path: str) -> Optional[Dict[str, Any]]:
        """Fetch a single media release and extract full text."""
        url = f"{BASE_URL}{path}"
        data = _fetch_url(url, accept="text/html")
        if not data:
            return None

        html_text = data.decode("utf-8", errors="replace")

        # Extract title
        title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html_text, re.DOTALL)
        title = strip_html(title_match.group(1)) if title_match else path.split("/")[-1]

        # Extract article body - look for main content area
        body = ""
        # Try field--name-body first
        body_match = re.search(
            r'<div[^>]*class="[^"]*field--name-body[^"]*"[^>]*>(.*?)</div>\s*</div>',
            html_text, re.DOTALL
        )
        if body_match:
            body = body_match.group(1)
        else:
            # Try article tag
            article_match = re.search(r'<article[^>]*>(.*?)</article>', html_text, re.DOTALL)
            if article_match:
                body = article_match.group(1)
            else:
                # Try main content region
                main_match = re.search(
                    r'<div[^>]*class="[^"]*content[^"]*"[^>]*>(.*?)</div>',
                    html_text, re.DOTALL
                )
                if main_match:
                    body = main_match.group(1)

        text = strip_html(body)
        if not text or len(text) < 100:
            return None

        # Extract date
        date_str = None
        date_match = re.search(
            r'<time[^>]*datetime="([^"]+)"', html_text
        )
        if date_match:
            date_str = date_match.group(1)
        else:
            date_match = re.search(
                r'(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4})',
                html_text
            )
            if date_match:
                date_str = date_match.group(1)

        # Generate UUID from path
        slug = path.replace("/media-release/", "").strip("/")

        return {
            "uuid": f"media-release-{slug}",
            "title": title,
            "body_html": body,
            "text": text,
            "created": date_str or "",
            "changed": "",
            "path_alias": path,
            "doc_type": "media_release",
            "node_type": "media_release",
            "source": "html",
        }

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None

        # Try ISO format first (from JSON:API)
        for fmt in [
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S+%f",
            "%Y-%m-%d",
        ]:
            try:
                dt = datetime.strptime(date_str[:25], fmt[:25])
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue

        # Try "DD Month YYYY"
        try:
            dt = datetime.strptime(date_str.strip(), "%d %B %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

        # Try extracting date from ISO-like string
        match = re.search(r'(\d{4}-\d{2}-\d{2})', date_str)
        if match:
            return match.group(1)

        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to standard schema."""
        uuid = raw.get("uuid", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        created = raw.get("created", "")
        path_alias = raw.get("path_alias", "")
        doc_type = raw.get("doc_type", "unknown")

        url = f"{BASE_URL}{path_alias}" if path_alias else BASE_URL

        return {
            "_id": uuid,
            "_source": "AU/ACCC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": self._parse_date(created),
            "url": url,
            "doc_type": doc_type,
            "uuid": uuid,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all ACCC documents: JSON:API register entries + media releases."""
        # Phase 1: Public register entries via JSON:API
        for node_type, doc_type in NODE_TYPES:
            try:
                for record in self._fetch_jsonapi_nodes(node_type, doc_type):
                    yield record
            except Exception as e:
                logger.error(f"Error fetching {node_type}: {e}")
            time.sleep(1)

        # Phase 2: Media releases via HTML scraping
        logger.info("Fetching media releases via HTML scraping...")
        page = 0
        empty_pages = 0
        while empty_pages < 3:
            try:
                links = self._fetch_media_release_page(page)
                if not links:
                    empty_pages += 1
                    page += 1
                    continue
                empty_pages = 0

                for link in links:
                    release = self._fetch_media_release(link)
                    if release:
                        yield release
                    time.sleep(1)

                page += 1
                time.sleep(1)

            except Exception as e:
                logger.error(f"Error on media release page {page}: {e}")
                empty_pages += 1
                page += 1

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent media releases (first few pages)."""
        logger.info(f"Fetching media releases since {since}")
        for page in range(5):  # Check first 5 pages
            links = self._fetch_media_release_page(page)
            if not links:
                break
            for link in links:
                release = self._fetch_media_release(link)
                if release:
                    date = self._parse_date(release.get("created", ""))
                    if date and date < since.strftime("%Y-%m-%d"):
                        return
                    yield release
                time.sleep(1)
            time.sleep(1)


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/ACCC data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    args = parser.parse_args()

    scraper = AustraliaACCCScraper()

    if args.command == "test":
        logger.info("Testing JSON:API connectivity...")
        url = f"{JSONAPI_BASE}/node/acccgov_authorisation"
        data = _fetch_url(url)
        if data:
            payload = json.loads(data)
            count = len(payload.get("data", []))
            logger.info(f"OK — got {count} authorisation node(s)")
        else:
            logger.error("FAILED — could not reach JSON:API")
            sys.exit(1)

        logger.info("Testing media releases page...")
        data = _fetch_url(MEDIA_RELEASES_URL, accept="text/html")
        if data:
            links = re.findall(r'href="(/media-release/[^"]+)"', data.decode("utf-8", errors="replace"))
            logger.info(f"OK — found {len(links)} media release links on page 0")
        else:
            logger.error("FAILED — could not reach media releases page")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        since = datetime.now(timezone.utc).replace(day=1)
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
