#!/usr/bin/env python3
"""
UG/UCC -- Uganda Communications Commission

Fetches regulatory documents, decisions, rulings, regulations, standards,
and guidelines from UCC via WordPress REST API.

Strategy:
  - WP Media API: ~318 PDF attachments downloaded and text extracted
    via pdfplumber.
  - WP Posts API: ~50 posts with inline text (press releases, notices).
    Posts with substantial text (>200 chars) are included.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import time
import html
import logging
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UG.UCC")

API_BASE = "https://www.ucc.co.ug/wp-json/wp/v2"
USER_AGENT = "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)"

MIN_TEXT_LENGTH = 200


def strip_html(raw_html: str) -> str:
    text = re.sub(r"<style[^>]*>.*?</style>", "", raw_html, flags=re.S | re.I)
    text = re.sub(r"<script[^>]*>.*?</script>", "", raw_html, flags=re.S | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?(p|div|h[1-6]|li|tr|td|th|blockquote)[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def download_pdf_text(url: str, session: requests.Session) -> Optional[str]:
    try:
        resp = session.get(url, timeout=120)
        resp.raise_for_status()
        if len(resp.content) > 50_000_000:
            logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
            return None
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(resp.content)
            tmp_path = f.name
        try:
            with pdfplumber.open(tmp_path) as pdf:
                pages_text = []
                for page in pdf.pages[:200]:
                    page_text = page.extract_text()
                    if page_text:
                        pages_text.append(page_text)
                return "\n\n".join(pages_text) if pages_text else None
        finally:
            os.unlink(tmp_path)
    except Exception as e:
        logger.warning(f"PDF extraction failed for {url}: {e}")
        return None


def classify_type(title: str) -> tuple[str, str]:
    """Return (_type, document_type) based on title keywords."""
    t = title.lower()
    # Case law: court decisions and rulings
    if re.search(r"\b(vs?\.?|versus|misc\.?\s*cause|civil suit|application no|ruling)\b", t):
        return "case_law", "court-decision"
    # Legislation: regulations, acts, statutory instruments
    if re.search(r"\b(regulation|regulations|act |statutory instrument|by-?laws?)\b", t):
        return "legislation", "regulation"
    # Doctrine subtypes
    if "guideline" in t or "guidelines" in t:
        return "doctrine", "guideline"
    if "standard" in t or "framework" in t:
        return "doctrine", "standard"
    if "decision" in t:
        return "doctrine", "decision"
    if "circular" in t:
        return "doctrine", "circular"
    if "notice" in t:
        return "doctrine", "notice"
    if "tariff" in t:
        return "doctrine", "tariff"
    if "report" in t or "market report" in t:
        return "doctrine", "report"
    if "policy" in t:
        return "doctrine", "policy"
    return "doctrine", "publication"


class UCCScraper(BaseScraper):
    """
    Scraper for UG/UCC -- Uganda Communications Commission.
    Country: UG
    URL: https://www.ucc.co.ug/
    Data types: case_law, legislation, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT

    def _paginate_wp(self, endpoint: str, params: dict = None, max_pages: int = 50) -> Generator[dict, None, None]:
        if params is None:
            params = {}
        params.setdefault("per_page", 100)
        page = 1
        while page <= max_pages:
            params["page"] = page
            try:
                resp = self._session.get(
                    f"{API_BASE}/{endpoint}", params=params, timeout=30
                )
                resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 400:
                    break
                raise
            data = resp.json()
            if not data:
                break
            for item in data:
                yield item
            total_pages = int(resp.headers.get("X-WP-TotalPages", max_pages))
            if page >= total_pages:
                break
            page += 1
            time.sleep(1.0)

    def _normalize_media(self, media: dict, text: str) -> dict:
        title = strip_html(media.get("title", {}).get("rendered", ""))
        media_id = media.get("id", 0)
        source_url = media.get("source_url", "")
        date_str = media.get("date", "")[:10] if media.get("date") else None
        _type, doc_type = classify_type(title)

        return {
            "_id": f"ug-ucc-pdf-{media_id}",
            "_source": "UG/UCC",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": source_url,
            "document_type": doc_type,
        }

    def _normalize_post(self, post: dict) -> Optional[dict]:
        title = strip_html(post.get("title", {}).get("rendered", ""))
        content_html = post.get("content", {}).get("rendered", "")
        text = strip_html(content_html)

        if len(text) < MIN_TEXT_LENGTH:
            return None

        date_str = post.get("date", "")[:10] if post.get("date") else None
        post_id = post.get("id", 0)
        link = post.get("link", f"https://www.ucc.co.ug/?p={post_id}")
        _type, doc_type = classify_type(title)
        if doc_type == "publication":
            doc_type = "press-release"

        return {
            "_id": f"ug-ucc-post-{post_id}",
            "_source": "UG/UCC",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": link,
            "document_type": doc_type,
        }

    def normalize(self, raw: dict) -> dict:
        return raw

    def fetch_all(self) -> Generator[dict, None, None]:
        yielded = 0

        logger.info("Fetching PDF media items...")
        for media in self._paginate_wp("media", {"media_type": "application"}):
            mime = media.get("mime_type", "")
            if mime != "application/pdf":
                continue

            source_url = media.get("source_url", "")
            title = strip_html(media.get("title", {}).get("rendered", ""))
            logger.info(f"  Downloading PDF: {title[:50]}...")

            text = download_pdf_text(source_url, self._session)
            if not text or len(text) < MIN_TEXT_LENGTH:
                logger.debug(f"  Skipped (no text): {title[:50]}")
                continue

            record = self._normalize_media(media, text)
            yield record
            yielded += 1
            logger.info(f"  [{yielded}] {title[:60]} ({len(text)} chars)")
            time.sleep(1.0)

        logger.info("Fetching inline-text posts...")
        for post in self._paginate_wp("posts"):
            record = self._normalize_post(post)
            if record:
                yield record
                yielded += 1
                logger.info(f"  [{yielded}] {record['title'][:60]} ({len(record['text'])} chars)")

        logger.info(f"fetch_all complete: {yielded} records")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        for media in self._paginate_wp("media", {"media_type": "application", "after": f"{since}T00:00:00"}):
            if media.get("mime_type") != "application/pdf":
                continue
            source_url = media.get("source_url", "")
            text = download_pdf_text(source_url, self._session)
            if text and len(text) >= MIN_TEXT_LENGTH:
                yield self._normalize_media(media, text)
                time.sleep(1.0)

        for post in self._paginate_wp("posts", {"after": f"{since}T00:00:00", "orderby": "modified"}):
            record = self._normalize_post(post)
            if record:
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="UG/UCC -- Uganda Communications Commission"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = UCCScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            resp = requests.get(
                f"{API_BASE}/posts",
                params={"per_page": 1},
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            resp.raise_for_status()
            total = resp.headers.get("X-WP-Total", "?")
            logger.info(f"Total posts: {total}")

            resp2 = requests.get(
                f"{API_BASE}/media",
                params={"per_page": 1, "media_type": "application"},
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            resp2.raise_for_status()
            total_media = resp2.headers.get("X-WP-Total", "?")
            logger.info(f"Total media: {total_media}")

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
