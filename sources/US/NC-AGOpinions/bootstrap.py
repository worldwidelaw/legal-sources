#!/usr/bin/env python3
"""
US/NC-AGOpinions -- North Carolina Attorney General Opinions

Fetches the full text of official legal opinions (advisory opinions and
opinion letters) issued by the North Carolina Department of Justice /
Office of the Attorney General. Each opinion answers a legal question
posed by a public official or agency and is an authoritative (advisory)
interpretation of North Carolina law -- classified as doctrine.

Strategy:
  ncdoj.gov is a WordPress site that exposes every opinion as a custom
  post type `opinions` via the public WP REST API. The per-opinion
  front-end pages (ncdoj.gov/opinions/{slug}/) intermittently return
  HTTP 500, but the REST endpoint serves the full text reliably in
  `content.rendered`.

  1. Page through GET /wp-json/wp/v2/opinions?per_page=100&page=N.
  2. Each post carries title.rendered, content.rendered (full body),
     the published date, and the canonical link.
  3. Strip HTML from content.rendered and normalize into the standard
     doctrine schema (text = cleaned body).

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import html
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NC-AGOpinions")

API_BASE = "https://ncdoj.gov/wp-json/wp/v2/opinions"
PUBLIC_URL = "https://ncdoj.gov/legal-services/archived-opinions/"
PAGE_SIZE = 100


def clean_text(raw_html: str) -> str:
    """Strip HTML tags/entities from a WordPress content.rendered body."""
    if not raw_html:
        return ""
    text = raw_html
    # Drop script/style blocks entirely.
    text = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", text)
    # Convert block-level closes to newlines so paragraphs survive.
    text = re.sub(r"(?i)</(p|div|br|li|h[1-6]|tr)\s*>", "\n", text)
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    # Remove remaining tags.
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def clean_title(raw_html: str) -> str:
    return clean_text(raw_html).replace("\n", " ").strip()


def parse_date(raw: str) -> str | None:
    """Convert a WP '2010-11-08T12:30:40' value -> YYYY-MM-DD."""
    raw = (raw or "").strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m and m.group(1) != "0001":
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


class NCAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Accept": "application/json",
            },
            timeout=60,
        )
        self.delay = 0.6

    def _get_json(self, url: str, retries: int = 4):
        """Fetch a JSON URL with rate limiting and retry/backoff."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code == 400:
                    # WP returns 400 (rest_post_invalid_page_number) when paging
                    # past the last page -- a normal terminal signal.
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _page_url(self, page: int, per_page: int) -> str:
        return f"{API_BASE}?per_page={per_page}&page={page}&orderby=date&order=desc"

    def test_api(self) -> bool:
        """Test connectivity and document structure of the WP REST endpoint."""
        logger.info("Testing North Carolina AG opinions WP REST API...")
        try:
            data = self._get_json(self._page_url(1, 3))
            if not data:
                logger.error("  opinions endpoint returned no results")
                return False
            doc = data[0]
            text = clean_text(doc.get("content", {}).get("rendered") or "")
            title = clean_title(doc.get("title", {}).get("rendered") or "")
            logger.info(f"  Endpoint OK; first opinion: {title!r}")
            if text and len(text) > 200:
                logger.info(f"  Full text OK ({len(text)} chars, id {doc.get('id')})")
            else:
                logger.error("  content.rendered missing or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Page through the WP REST opinions endpoint, newest first."""
        emitted = 0
        page = 1
        seen = set()
        while True:
            data = self._get_json(self._page_url(page, PAGE_SIZE))
            if not data:
                break
            logger.info(f"Page {page}: {len(data)} opinions")
            for doc in data:
                doc_id = doc.get("id")
                if doc_id is None or doc_id in seen:
                    continue
                seen.add(doc_id)
                text = clean_text(doc.get("content", {}).get("rendered") or "")
                if not text or len(text) < 100:
                    # Skip metadata-only rows (no usable full-text body).
                    continue
                doc["_clean_text"] = text
                yield doc
                emitted += 1
                if sample and emitted >= 12:
                    return
            if len(data) < PAGE_SIZE:
                break
            page += 1

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw WP opinion post into the standard doctrine schema."""
        doc_id = raw.get("id")
        slug = (raw.get("slug") or "").strip()
        title = clean_title(raw.get("title", {}).get("rendered") or "")
        if not title:
            title = f"North Carolina AG Opinion {doc_id}"
        text = raw.get("_clean_text") or clean_text(
            raw.get("content", {}).get("rendered") or ""
        )
        return {
            "_id": f"US/NC-AGOpinions/{doc_id}",
            "_source": "US/NC-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_id": str(doc_id),
            "slug": slug or None,
            "title": title,
            "text": text,
            "date": parse_date(raw.get("date") or ""),
            "url": raw.get("link") or PUBLIC_URL,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield opinions published on/after `since` (ISO date)."""
        for raw in self.fetch_all():
            d = parse_date(raw.get("date") or "")
            if not since or (d and d >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/NC-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NCAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
