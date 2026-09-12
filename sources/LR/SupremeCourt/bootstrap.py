#!/usr/bin/env python3
"""
LR/SupremeCourt -- Liberia Supreme Court Opinions

Fetches Supreme Court opinions, decisions, orders, and special master reports
from the Judiciary of Liberia (judiciary.gov.lr). Documents are published as
embedded PDFs in WordPress posts.

Strategy:
  - WP REST API at judiciary.gov.lr/wp-json/wp/v2/posts
  - Filter by legal categories (decisions, opinions, orders)
  - Download each PDF and extract text with pdfminer
  - ~400+ documents, all English language

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import logging
import re
import time
import json
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote

import requests
from pdfminer.high_level import extract_text as pdfminer_extract

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LR.SupremeCourt")

API_BASE = "https://judiciary.gov.lr/wp-json/wp/v2"
PER_PAGE = 20

# Legal categories to fetch (IDs from judiciary.gov.lr WP)
# 6 = Recent Decisions (379), 16 = Orders (5), 17 = Court Orders (5), 18 = Special Master Report (24)
LEGAL_CATEGORY_IDS = [6, 16, 17, 18]
LEGAL_CATEGORIES_CSV = ",".join(str(c) for c in LEGAL_CATEGORY_IDS)


def _extract_pdf_url(content_html: str) -> Optional[str]:
    """Extract the first PDF URL from WP post content HTML."""
    # Try href links first
    urls = re.findall(
        r'href="(https?://judiciary\.gov\.lr/wp-content/uploads/[^"]+\.pdf)"',
        content_html,
    )
    if urls:
        return urls[0]
    # Try Google Docs viewer URL param
    viewer_urls = re.findall(r"url=(http[^&\"]+\.pdf)", content_html)
    if viewer_urls:
        return unquote(viewer_urls[0])
    # Try any wp-content upload link (docx, etc.)
    doc_urls = re.findall(
        r'href="(https?://judiciary\.gov\.lr/wp-content/uploads/[^"]+)"',
        content_html,
    )
    if doc_urls:
        return doc_urls[0]
    viewer_docs = re.findall(
        r"url=(http[^&\"]+judiciary\.gov\.lr/wp-content/uploads/[^&\"]+)",
        content_html,
    )
    if viewer_docs:
        return unquote(viewer_docs[0])
    return None


def _download_and_extract_pdf(session: requests.Session, url: str) -> Optional[str]:
    """Download a PDF and extract text using pdfminer."""
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        if len(resp.content) < 100:
            return None
        text = pdfminer_extract(io.BytesIO(resp.content))
        text = text.strip()
        if len(text) < 50:
            return None
        return text
    except Exception as e:
        logger.warning("PDF extraction failed for %s: %s", url, e)
        return None


class LRSupremeCourtScraper(BaseScraper):
    SOURCE_ID = "LR/SupremeCourt"

    def __init__(self):
        super().__init__(source_dir=str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "LegalDataHunter/1.0 (legal research)"}
        )
        self._cat_names: Dict[int, str] = {}

    def _fetch_category_names(self):
        """Cache WP category ID → name mapping."""
        if self._cat_names:
            return
        try:
            resp = self.session.get(
                f"{API_BASE}/categories", params={"per_page": 100}, timeout=30
            )
            resp.raise_for_status()
            for cat in resp.json():
                self._cat_names[cat["id"]] = html_mod.unescape(cat["name"])
        except Exception as e:
            logger.warning("Failed to fetch categories: %s", e)

    def _iter_posts(
        self, max_posts: Optional[int] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Iterate WP posts from legal categories."""
        page = 1
        count = 0
        while True:
            if max_posts and count >= max_posts:
                break
            params = {
                "categories": LEGAL_CATEGORIES_CSV,
                "per_page": PER_PAGE,
                "page": page,
                "orderby": "date",
                "order": "desc",
            }
            try:
                resp = self.session.get(
                    f"{API_BASE}/posts", params=params, timeout=30
                )
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.error("API request failed page %d: %s", page, e)
                break

            posts = resp.json()
            if not posts:
                break

            for post in posts:
                if max_posts and count >= max_posts:
                    break
                yield post
                count += 1

            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            if page >= total_pages:
                break
            page += 1
            time.sleep(2)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all court documents with full text."""
        self._fetch_category_names()
        for post in self._iter_posts():
            record = self._process_post(post)
            if record:
                yield record

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Yield documents modified since a date."""
        self._fetch_category_names()
        since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
        for post in self._iter_posts():
            post_date = datetime.fromisoformat(
                post["date_gmt"].replace("Z", "+00:00")
            ).replace(tzinfo=timezone.utc)
            if post_date < since_dt:
                break
            record = self._process_post(post)
            if record:
                yield record

    def _process_post(self, post: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single WP post into a normalized record."""
        post_id = post["id"]
        title = html_mod.unescape(post["title"]["rendered"])
        content_html = post["content"]["rendered"]
        date_str = post.get("date_gmt", post.get("date", ""))
        link = post.get("link", "")

        # Extract PDF URL and download
        pdf_url = _extract_pdf_url(content_html)
        text = None
        if pdf_url:
            logger.info("Downloading PDF for post %d: %s", post_id, pdf_url[:80])
            text = _download_and_extract_pdf(self.session, pdf_url)
            time.sleep(1.5)

        if not text:
            # Try extracting text from HTML content itself
            plain = re.sub(r"<[^>]+>", "", content_html)
            plain = html_mod.unescape(plain).strip()
            if len(plain) > 200:
                text = plain
            else:
                logger.warning(
                    "No text extracted for post %d: %s", post_id, title[:60]
                )
                return None

        # Category names
        cat_ids = post.get("categories", [])
        cat_names = [self._cat_names.get(c, str(c)) for c in cat_ids]

        # Parse date
        date_iso = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str)
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        return self.normalize(
            {
                "post_id": post_id,
                "title": title,
                "text": text,
                "date": date_iso,
                "url": link,
                "pdf_url": pdf_url,
                "categories": cat_names,
            }
        )

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": f"lr-sc-{raw['post_id']}",
            "_source": self.SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "post_id": raw["post_id"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw["date"],
            "url": raw["url"],
            "pdf_url": raw.get("pdf_url"),
            "categories": raw.get("categories", []),
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.session.get(
                f"{API_BASE}/posts",
                params={"categories": LEGAL_CATEGORIES_CSV, "per_page": 1},
                timeout=15,
            )
            resp.raise_for_status()
            posts = resp.json()
            if posts:
                logger.info("Test OK: found post '%s'", posts[0]["title"]["rendered"][:60])
                return True
            logger.error("Test failed: no posts returned")
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="LR/SupremeCourt bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "test"], help="Command to run"
    )
    parser.add_argument(
        "--sample", action="store_true", help="Fetch only 15 sample records"
    )
    parser.add_argument(
        "--full", action="store_true", help="Fetch all records"
    )
    args = parser.parse_args()

    scraper = LRSupremeCourtScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    # bootstrap
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    max_records = 15 if args.sample else None
    count = 0
    for record in scraper.fetch_all():
        count += 1
        if max_records and count > max_records:
            break
        fname = sample_dir / f"{record['_id']}.json"
        fname.write_text(json.dumps(record, ensure_ascii=False, indent=2))
        logger.info(
            "[%d] %s — %d chars",
            count,
            record["title"][:60],
            len(record.get("text", "")),
        )
    logger.info("Done: %d records saved to %s", count, sample_dir)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
