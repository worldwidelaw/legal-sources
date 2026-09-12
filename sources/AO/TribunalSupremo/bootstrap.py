#!/usr/bin/env python3
"""
AO/TribunalSupremo -- Angola Supreme Court Decisions

Fetches court decisions (acórdãos) from the Tribunal Supremo de Angola via
the WordPress REST API. Full text is extracted from PDF attachments.

Chambers (WP category IDs):
  - 163: Câmara Criminal (~303 decisions)
  - 209: Câmara do Cível, Administrativo, Fiscal e Aduaneiro (~163)
  - 165: Câmara do Trabalho (~21)
  - 166: Câmara Familiar (~1)
  - 162: Plenário (~2)

Strategy:
  1. Paginate /wp-json/wp/v2/posts per category
  2. Extract PDF URL from content.rendered HTML
  3. Download PDF and extract text via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import io
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from html import unescape

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AO.TribunalSupremo")

BASE_URL = "https://tribunalsupremo.ao"
PER_PAGE = 100

CHAMBERS = [
    {"cat_id": 163, "name": "Câmara Criminal", "slug": "criminal"},
    {"cat_id": 209, "name": "Câmara do Cível, Administrativo, Fiscal e Aduaneiro", "slug": "civil"},
    {"cat_id": 165, "name": "Câmara do Trabalho", "slug": "labor"},
    {"cat_id": 166, "name": "Câmara Familiar", "slug": "family"},
    {"cat_id": 162, "name": "Plenário", "slug": "plenary"},
]

PDF_URL_RE = re.compile(r'href=["\']([^"\']+\.pdf)["\']', re.IGNORECASE)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(html_str: str) -> str:
    """Remove HTML tags and clean whitespace."""
    text = TAG_RE.sub(" ", html_str)
    text = unescape(text)
    text = WS_RE.sub(" ", text).strip()
    return text


class AOTribunalSupremoScraper(BaseScraper):
    """Scraper for AO/TribunalSupremo -- Angola Supreme Court."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    def _fetch_posts_page(self, cat_id: int, page: int) -> tuple:
        """Fetch a page of posts for a category. Returns (posts, total_pages)."""
        params = {
            "categories": cat_id,
            "per_page": PER_PAGE,
            "page": page,
            "_fields": "id,title,date,modified,link,content",
        }
        time.sleep(1.5)
        resp = self.session.get(f"{BASE_URL}/wp-json/wp/v2/posts", params=params, timeout=60)
        resp.raise_for_status()
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        return resp.json(), total_pages

    def _extract_pdf_url(self, content_html: str) -> Optional[str]:
        """Extract PDF URL from post content HTML."""
        match = PDF_URL_RE.search(content_html)
        if match:
            url = match.group(1)
            if not url.startswith("http"):
                url = BASE_URL + url
            return url
        return None

    def _download_pdf_text(self, url: str) -> str:
        """Download a PDF and extract text using pdfplumber."""
        if not url:
            return ""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=90)
                if resp.status_code != 200:
                    logger.warning(f"PDF download HTTP {resp.status_code}: {url}")
                    return ""
                if len(resp.content) > 50_000_000:
                    logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
                    return ""
                with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            pages_text.append(text)
                        try:
                            page.flush_cache(); page.get_textmap.cache_clear()
                        except Exception:
                            pass
                    full_text = "\n\n".join(pages_text)
                    full_text = re.sub(r"\n{3,}", "\n\n", full_text)
                    return full_text.strip()
            except Exception as e:
                logger.warning(f"PDF extraction attempt {attempt + 1}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        wp_id = raw.get("id", "")
        chamber = raw.get("_chamber_name", "")

        title = raw.get("title", {})
        if isinstance(title, dict):
            title = title.get("rendered", "")
        title = unescape(str(title)).strip()

        wp_date = raw.get("date", "")
        date = wp_date[:10] if wp_date else None

        url = raw.get("link", "")
        text = raw.get("_full_text", "")
        pdf_url = raw.get("_pdf_url", "")

        return {
            "_id": f"AO/TribunalSupremo/{wp_id}",
            "_source": "AO/TribunalSupremo",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "pdf_url": pdf_url,
            "doc_id": str(wp_id),
            "chamber": chamber,
            "court": "Tribunal Supremo de Angola",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        for chamber in CHAMBERS:
            if limit and count >= limit:
                break

            cat_id = chamber["cat_id"]
            chamber_name = chamber["name"]
            logger.info(f"Fetching {chamber_name} (cat={cat_id})...")

            page = 1
            total_pages = 1

            while page <= total_pages:
                if limit and count >= limit:
                    break

                try:
                    posts, total_pages = self._fetch_posts_page(cat_id, page)
                except Exception as e:
                    logger.error(f"Failed to fetch page {page} for {chamber_name}: {e}")
                    break

                logger.info(f"  Page {page}/{total_pages}: {len(posts)} posts")

                for post in posts:
                    if limit and count >= limit:
                        break

                    content = post.get("content", {})
                    if isinstance(content, dict):
                        content_html = content.get("rendered", "")
                    else:
                        content_html = str(content)

                    pdf_url = self._extract_pdf_url(content_html)
                    if not pdf_url:
                        title = post.get("title", {})
                        if isinstance(title, dict):
                            title = title.get("rendered", "")
                        logger.warning(f"  No PDF found for '{unescape(str(title))[:60]}'")
                        # Try using content text as fallback
                        text = strip_html(content_html)
                        if len(text) < 200:
                            continue
                        post["_full_text"] = text
                        post["_pdf_url"] = ""
                    else:
                        text = self._download_pdf_text(pdf_url)
                        if len(text) < 100:
                            title = post.get("title", {})
                            if isinstance(title, dict):
                                title = title.get("rendered", "")
                            logger.warning(f"  PDF text too short for '{unescape(str(title))[:60]}' ({len(text)} chars)")
                            continue
                        post["_full_text"] = text
                        post["_pdf_url"] = pdf_url

                    post["_chamber_name"] = chamber_name
                    yield post
                    count += 1

                    title = post.get("title", {})
                    if isinstance(title, dict):
                        title = title.get("rendered", "")
                    logger.info(f"  [{count}] {unescape(str(title))[:70]} ({len(post['_full_text'])} chars)")

                page += 1

        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        for chamber in CHAMBERS:
            cat_id = chamber["cat_id"]
            chamber_name = chamber["name"]
            page = 1
            total_pages = 1

            while page <= total_pages:
                try:
                    params = {
                        "categories": cat_id,
                        "per_page": PER_PAGE,
                        "page": page,
                        "after": f"{since}T00:00:00",
                        "orderby": "date",
                        "order": "asc",
                        "_fields": "id,title,date,modified,link,content",
                    }
                    time.sleep(1.5)
                    resp = self.session.get(
                        f"{BASE_URL}/wp-json/wp/v2/posts",
                        params=params,
                        timeout=60,
                    )
                    resp.raise_for_status()
                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    posts = resp.json()
                except Exception as e:
                    logger.error(f"Failed to fetch updates for {chamber_name}: {e}")
                    break

                for post in posts:
                    content = post.get("content", {})
                    if isinstance(content, dict):
                        content_html = content.get("rendered", "")
                    else:
                        content_html = str(content)

                    pdf_url = self._extract_pdf_url(content_html)
                    if pdf_url:
                        text = self._download_pdf_text(pdf_url)
                        post["_full_text"] = text
                        post["_pdf_url"] = pdf_url
                    else:
                        post["_full_text"] = strip_html(content_html)
                        post["_pdf_url"] = ""

                    post["_chamber_name"] = chamber_name
                    yield post

                page += 1


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = AOTribunalSupremoScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
