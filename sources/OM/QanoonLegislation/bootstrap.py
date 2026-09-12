#!/usr/bin/env python3
"""
OM/QanoonLegislation - Oman Royal Decrees and Legislation Fetcher

Fetches Omani legislation from qanoon.om via WordPress REST API.
~10,000+ documents: Royal Decrees, Ministerial Decisions, Legal Opinions.

Data access strategy:
  1. WP REST API for document listing + full text (/wp-json/wp/v2/posts)
  2. Category IDs: Royal Decrees (2), Ministerial Decisions (3), Legal Opinions (349)
  3. Full text in content.rendered field (HTML stripped to plain text)

License: Public Domain under Omani Copyright Law
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import logging
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.OM.QanoonLegislation")

BASE_URL = "https://qanoon.om"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
SAMPLE_DIR = Path(__file__).parent / "sample"
CHECKPOINT_FILE = Path(__file__).parent / "checkpoint.json"
SOURCE_ID = "OM/QanoonLegislation"

PER_PAGE = 100

# qanoon.om sits behind Cloudflare, which answers with 52x when its handshake
# to the WordPress origin fails. These are transient — the same page succeeds
# seconds later — so they must be retried rather than skipped, and a page that
# still fails after the retries must abort the run instead of quietly leaving a
# 100-document hole in the corpus (issue #1418).
RETRYABLE_STATUS = {408, 425, 429, 500, 502, 503, 504,
                    520, 521, 522, 523, 524, 525, 526, 527}
MAX_ATTEMPTS = 6
BACKOFF_CAP = 120

# Main legislation categories on qanoon.om
CATEGORIES = {
    2: "royal_decree",       # مرسوم سلطاني (~4,888)
    3: "ministerial_decision",  # قرار وزاري (~4,468)
    349: "legal_opinion",    # فتاوى قانونية (~687)
}


def _strip_html(html_str: str) -> str:
    """Strip HTML tags and decode entities to plain text."""
    if not html_str:
        return ""
    text = re.sub(r'<[^>]+>', ' ', html_str)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


class QanoonLegislationScraper(BaseScraper):
    """Scraper for OM/QanoonLegislation — Omani legislation via qanoon.om."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json,text/html,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.7,en;q=0.3",
        })
        # Sample runs must start at the top of the corpus, not resume a crawl.
        self.use_checkpoint = True

    def _load_checkpoint(self) -> dict:
        if self.use_checkpoint and CHECKPOINT_FILE.exists():
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        return {'current_category': None, 'last_page': 0, 'fetched_ids': []}

    def _save_checkpoint(self, checkpoint: dict):
        if not self.use_checkpoint:
            return
        with open(CHECKPOINT_FILE, 'w') as f:
            json.dump(checkpoint, f, indent=2)

    def _fetch_api_page(self, category_id: int, page: int) -> tuple:
        """Fetch one page of posts, retrying transient Cloudflare/WP failures.

        Returns ``(posts, total, total_pages)``. Raises ``RuntimeError`` if the
        page is still unreachable after ``MAX_ATTEMPTS`` — the caller must not
        treat that as the end of the category.
        """
        url = (
            f"{API_URL}?per_page={PER_PAGE}&page={page}"
            f"&categories={category_id}&orderby=date&order=asc"
        )
        logger.info(f"Fetching cat={category_id} page {page}")

        last_error = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            retry_after = None
            time.sleep(1)
            try:
                resp = self.session.get(url, timeout=(15, 45))
            except requests.RequestException as e:
                last_error = f"{type(e).__name__}: {e}"
            else:
                if resp.status_code == 200:
                    return (
                        resp.json(),
                        int(resp.headers.get("X-WP-Total", 0)),
                        int(resp.headers.get("X-WP-TotalPages", 0)),
                    )
                if resp.status_code == 400 and "rest_post_invalid_page_number" in resp.text:
                    # WP's way of saying "past the last page" — a normal stop.
                    return [], 0, page - 1
                if resp.status_code not in RETRYABLE_STATUS:
                    raise RuntimeError(
                        f"cat={category_id} page={page} returned "
                        f"HTTP {resp.status_code} (not retryable)"
                    )
                last_error = f"HTTP {resp.status_code}"
                retry_after = resp.headers.get("Retry-After")

            if attempt == MAX_ATTEMPTS:
                break

            delay = min(2 ** attempt, BACKOFF_CAP)
            try:
                delay = max(delay, min(int(retry_after), BACKOFF_CAP))
            except (TypeError, ValueError):
                pass
            logger.warning(
                f"cat={category_id} page={page} attempt {attempt}/{MAX_ATTEMPTS} "
                f"failed ({last_error}) — retrying in {delay}s"
            )
            time.sleep(delay)

        raise RuntimeError(
            f"cat={category_id} page={page} unreachable after {MAX_ATTEMPTS} "
            f"attempts (last: {last_error}). Aborting so a partial fetch is not "
            f"reported as success."
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation documents with full text."""
        checkpoint = self._load_checkpoint()
        fetched_ids = set(checkpoint.get('fetched_ids', []))
        start_cat = checkpoint.get('current_category')
        start_page = checkpoint.get('last_page', 0) + 1

        started = start_cat is None
        for cat_id, doc_type in CATEGORIES.items():
            if not started:
                if cat_id == start_cat:
                    started = True
                else:
                    continue

            page = start_page if cat_id == start_cat else 1
            total_pages = None
            cat_posts = 0

            while True:
                # A failed page now raises out of fetch_all: an unreachable page
                # is not the end of the category, and pretending otherwise is
                # what silently truncated the corpus to 1,285 of ~10,100.
                posts, total, tp = self._fetch_api_page(cat_id, page)

                if total_pages is None:
                    total_pages = tp
                    logger.info(f"Category {doc_type} (ID {cat_id}): {total} posts, {total_pages} pages")

                if not posts:
                    if page <= (total_pages or 0):
                        raise RuntimeError(
                            f"cat={cat_id} page={page} of {total_pages} returned an "
                            f"empty list — the API is dropping pages, refusing to "
                            f"report a truncated category as complete"
                        )
                    break

                cat_posts += len(posts)
                for post in posts:
                    wp_id = post.get('id')
                    if wp_id in fetched_ids:
                        continue

                    slug = post.get('slug', '')
                    post_url = post.get('link', f"{BASE_URL}/p/{slug}/")
                    title = _strip_html(post.get('title', {}).get('rendered', slug))
                    content_html = post.get('content', {}).get('rendered', '')
                    full_text = _strip_html(content_html)
                    wp_date = post.get('date', '')

                    record = {
                        '_id': f"Qanoon-{wp_id}",
                        '_source': SOURCE_ID,
                        '_type': 'legislation',
                        '_fetched_at': datetime.now(timezone.utc).isoformat(),
                        'title': title,
                        'text': full_text,
                        'date': wp_date[:10] if wp_date else None,
                        'url': post_url,
                        'doc_type': doc_type,
                        'wp_id': wp_id,
                        'excerpt': _strip_html(post.get('excerpt', {}).get('rendered', '')),
                    }

                    yield record
                    fetched_ids.add(wp_id)

                checkpoint = {
                    'current_category': cat_id,
                    'last_page': page,
                    'fetched_ids': list(fetched_ids),
                }
                self._save_checkpoint(checkpoint)

                if page >= (total_pages or 1):
                    break
                page += 1

            logger.info(
                f"Category {doc_type} (ID {cat_id}) done: {cat_posts} posts seen "
                f"across pages {start_page if cat_id == start_cat else 1}-{page}"
            )

            # Reset page counter for next category
            start_page = 1

        # Every category walked to its last page. Clear the checkpoint so the
        # next refresh re-scans from the top and picks up new legislation —
        # otherwise a completed run leaves the cursor parked past the end and
        # every subsequent run is a no-op. Ingest dedups on _id.
        if self.use_checkpoint and CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Corpus walk complete — checkpoint cleared for next refresh")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield posts modified since a date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        for cat_id, doc_type in CATEGORIES.items():
            page = 1
            while True:
                try:
                    url = (
                        f"{API_URL}?per_page={PER_PAGE}&page={page}"
                        f"&categories={cat_id}&orderby=modified&order=desc"
                        f"&modified_after={since}"
                    )
                    time.sleep(1)
                    resp = self.session.get(url, timeout=30)
                    resp.raise_for_status()
                    posts = resp.json()
                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                except requests.RequestException as e:
                    logger.error(f"API request failed: {e}")
                    break

                if not posts:
                    break

                for post in posts:
                    wp_id = post.get('id')
                    slug = post.get('slug', '')
                    post_url = post.get('link', f"{BASE_URL}/p/{slug}/")
                    title = _strip_html(post.get('title', {}).get('rendered', slug))
                    full_text = _strip_html(post.get('content', {}).get('rendered', ''))

                    yield {
                        '_id': f"Qanoon-{wp_id}",
                        '_source': SOURCE_ID,
                        '_type': 'legislation',
                        '_fetched_at': datetime.now(timezone.utc).isoformat(),
                        'title': title,
                        'text': full_text,
                        'date': post.get('date', '')[:10],
                        'url': post_url,
                        'doc_type': doc_type,
                    }

                if page >= total_pages:
                    break
                page += 1

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record to the standard schema."""
        return {
            '_id': raw.get('_id', ''),
            '_source': SOURCE_ID,
            '_type': 'legislation',
            '_fetched_at': raw.get('_fetched_at', datetime.now(timezone.utc).isoformat()),
            'title': raw.get('title', ''),
            'text': raw.get('text', ''),
            'date': raw.get('date'),
            'url': raw.get('url', ''),
            'doc_type': raw.get('doc_type', ''),
            'excerpt': raw.get('excerpt', ''),
        }


def bootstrap(sample: bool = False, fast: bool = False):
    """Bootstrap the OM/QanoonLegislation data source."""
    scraper = QanoonLegislationScraper()

    if not sample:
        # BaseScraper streams normalized records to data/records.jsonl and
        # surfaces a failed page as a non-zero exit.
        stats = scraper.bootstrap_fast() if fast else scraper.bootstrap()
        logger.info(
            f"Done. {stats.get('records_fetched', 0)} fetched, "
            f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors."
        )
        if stats.get("error_message"):
            logger.error(f"Bootstrap failed: {stats['error_message']}")
            sys.exit(1)
        return stats.get("records_fetched", 0)

    scraper.use_checkpoint = False
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0

    for record in scraper.fetch_all():
        normalized = scraper.normalize(record)

        safe_id = re.sub(r'[/\\:]', '_', normalized['_id'])
        out_file = SAMPLE_DIR / f"{safe_id}.json"
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)
        text_len = len(normalized.get('text', '') or '')
        logger.info(
            f"[{count + 1}] {normalized['_id']} — "
            f"{text_len} chars text, type={normalized.get('doc_type')}, date={normalized.get('date')}"
        )

        count += 1
        if count >= 15:
            break

    logger.info(f"Done. {count} records sampled.")
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="OM/QanoonLegislation bootstrap")
    # The fleet wrapper invokes `bootstrap-fast`; without it argparse exits 2
    # and the wrapper falls back to re-ingesting sample/.
    parser.add_argument("action", choices=["bootstrap", "bootstrap-fast"],
                        help="Action to perform")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    bootstrap(sample=args.sample, fast=args.action == "bootstrap-fast")
