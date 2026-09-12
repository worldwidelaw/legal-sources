#!/usr/bin/env python3
"""
NO/Personvernnemnda - Norwegian Privacy Appeals Board Decisions Fetcher

Fetches appeal decisions from Personvernnemnda via WordPress REST API.
Covers all PVN case decisions from 2001-2025: privacy rights, GDPR
enforcement, data breach penalties, surveillance complaints, and
data processing disputes.

Index method: WordPress REST API (wp-json/wp/v2/posts)
Full text: Rendered HTML content field, stripped to plain text
License: NLOD 2.0 (Norwegian License for Open Government Data)
"""

import argparse
import html
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.personvernnemnda.no"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
SOURCE_ID = "NO/Personvernnemnda"
SAMPLE_DIR = Path(__file__).parent / "sample"
REQUEST_DELAY = 1.0
PER_PAGE = 100


class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract plain text."""

    def __init__(self):
        super().__init__()
        self._pieces = []
        self._skip = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style", "noscript"):
            self._skip = True
        elif tag in ("br", "p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr"):
            self._pieces.append("\n")

    def handle_endtag(self, tag):
        if tag in ("script", "style", "noscript"):
            self._skip = False
        elif tag in ("p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr", "table"):
            self._pieces.append("\n")

    def handle_data(self, data):
        if not self._skip:
            self._pieces.append(data)

    def get_text(self) -> str:
        raw = "".join(self._pieces)
        lines = [line.strip() for line in raw.splitlines()]
        lines = [line for line in lines if line]
        return "\n".join(lines)


def strip_html(html_content: str) -> str:
    """Strip HTML tags and decode entities."""
    decoded = html.unescape(html_content)
    extractor = _HTMLTextExtractor()
    extractor.feed(decoded)
    return extractor.get_text()


class PersonvernnemndaFetcher:
    """Fetcher for Norwegian Privacy Appeals Board decisions via WP REST API."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _get_page(self, page: int) -> list:
        """Fetch a page of posts from the WP REST API."""
        time.sleep(REQUEST_DELAY)
        params = {
            "per_page": PER_PAGE,
            "page": page,
            "orderby": "date",
            "order": "asc",
            "_fields": "id,date,title,content,link,slug",
        }
        for attempt in range(3):
            try:
                resp = self.session.get(API_URL, params=params, timeout=30)
                if resp.status_code == 400:
                    # Beyond last page
                    return []
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                if attempt < 2:
                    logger.warning(f"Retry {attempt + 1} for page {page}: {e}")
                    time.sleep(5 * (attempt + 1))
                else:
                    raise

    def _extract_reference(self, title: str, slug: str) -> Optional[str]:
        """Extract PVN reference number from title or slug."""
        for text in [title, slug]:
            m = re.search(r"(PVN-\d{4}-\d+)", text, re.I)
            if m:
                return m.group(1).upper()
        return None

    def normalize(self, post: dict) -> Optional[Dict[str, Any]]:
        """Normalize a WP post into a standard record."""
        title_raw = post.get("title", {}).get("rendered", "")
        title = html.unescape(title_raw).strip()
        content_html = post.get("content", {}).get("rendered", "")
        text = strip_html(content_html)

        if not text or len(text) < 50:
            return None

        wp_id = post["id"]
        date_str = post.get("date", "")[:10]
        link = post.get("link", "")
        slug = post.get("slug", "")
        reference = self._extract_reference(title, slug)

        return {
            "_id": f"NO-PVN-{wp_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str if date_str else None,
            "url": link,
            "reference": reference,
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Yield all decisions via paginated WP REST API."""
        page = 1
        total_fetched = 0

        while True:
            logger.info(f"Fetching page {page}...")
            posts = self._get_page(page)

            if not posts:
                break

            for post in posts:
                doc = self.normalize(post)
                if doc:
                    total_fetched += 1
                    yield doc

            if len(posts) < PER_PAGE:
                break

            page += 1

        logger.info(f"Total: {total_fetched} decisions fetched")

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield decisions modified after a date."""
        time.sleep(REQUEST_DELAY)
        params = {
            "per_page": PER_PAGE,
            "after": f"{since}T00:00:00",
            "orderby": "date",
            "order": "asc",
            "_fields": "id,date,title,content,link,slug",
        }
        page = 1
        while True:
            params["page"] = page
            try:
                resp = self.session.get(API_URL, params=params, timeout=30)
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
                posts = resp.json()
            except requests.RequestException:
                break

            if not posts:
                break

            for post in posts:
                doc = self.normalize(post)
                if doc:
                    yield doc

            if len(posts) < PER_PAGE:
                break
            page += 1
            time.sleep(REQUEST_DELAY)


def bootstrap(sample: bool = False, full: bool = False, since: Optional[str] = None):
    """Main entry point."""
    fetcher = PersonvernnemndaFetcher()
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    if since:
        docs = fetcher.fetch_updates(since)
    else:
        docs = fetcher.fetch_all()

    count = 0
    max_docs = 15 if sample else None

    for doc in docs:
        count += 1
        text_len = len(doc.get("text", ""))
        logger.info(f"  → {doc['title'][:80]} | text={text_len} chars | date={doc.get('date')}")

        if sample:
            sample_path = SAMPLE_DIR / f"{doc['_id']}.json"
            with open(sample_path, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=2)

        if max_docs and count >= max_docs:
            logger.info(f"Sample limit reached ({max_docs})")
            break

    logger.info(f"Done: {count} decisions fetched")
    return count


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    parser = argparse.ArgumentParser(description="NO/Personvernnemnda bootstrap")
    parser.add_argument("command", choices=["bootstrap"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Save sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", type=str, help="Fetch updates since date (ISO 8601)")
    args = parser.parse_args()

    if args.command == "bootstrap":
        count = bootstrap(sample=args.sample, full=args.full, since=args.since)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
