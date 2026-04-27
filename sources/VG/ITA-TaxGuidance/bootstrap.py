#!/usr/bin/env python3
"""
VG/ITA-TaxGuidance -- BVI International Tax Authority Guidance

Fetches guidance documents, legislation, TIEAs, and news posts from bviita.vg
via the WordPress REST API. PDFs extracted via common/pdf_extract.

Content: CRS, FATCA, CbCR, Economic Substance, beneficial ownership guidance,
TIEAs with 27 countries, legislation, and news/circulars.

Strategy:
  1. Enumerate all PDFs via /wp-json/wp/v2/media?media_type=application
  2. Download and extract text from PDFs
  3. Also fetch pages and posts for HTML-based content

Usage:
  python bootstrap.py bootstrap          # Full pull (~174 PDFs + 70 posts + 32 pages)
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py update             # Fetch items modified in last 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VG.ITA-TaxGuidance")

USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"
BASE_URL = "https://bviita.vg"
WP_API = f"{BASE_URL}/wp-json/wp/v2"
REQUEST_DELAY = 1.5

TAG_RE = re.compile(r"<[^>]+>")
ENTITY_RE = re.compile(r"&[a-zA-Z]+;|&#\d+;|&#x[0-9a-fA-F]+;")


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode entities."""
    text = TAG_RE.sub(" ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _api_get(endpoint: str, params: dict = None, timeout: int = 30) -> Optional[any]:
    """Fetch from WP REST API. Returns parsed JSON."""
    url = f"{WP_API}/{endpoint}"
    if params:
        query = "&".join(f"{k}={v}" for k, v in params.items())
        url = f"{url}?{query}"
    req = Request(url, headers={
        "User-Agent": USER_AGENT,
        "Accept": "application/json",
    })
    try:
        resp = urlopen(req, timeout=timeout)
        return json.loads(resp.read())
    except (HTTPError, URLError, json.JSONDecodeError) as e:
        logger.warning(f"API error for {url}: {e}")
        return None


def _api_get_all(endpoint: str, params: dict = None) -> List[dict]:
    """Fetch all pages from a paginated WP REST endpoint."""
    if params is None:
        params = {}
    params["per_page"] = "100"
    all_items = []
    page = 1
    while True:
        params["page"] = str(page)
        time.sleep(REQUEST_DELAY)
        items = _api_get(endpoint, params)
        if not items or not isinstance(items, list):
            break
        all_items.extend(items)
        if len(items) < 100:
            break
        page += 1
    return all_items


def _download_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    """Download PDF bytes."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        if data and b"%PDF" in data[:20]:
            return data
    except (HTTPError, URLError) as e:
        logger.debug(f"PDF download failed for {url}: {e}")
    return None


class BVIITAScraper(BaseScraper):
    """
    Scraper for VG/ITA-TaxGuidance.
    Country: VG
    URL: https://bviita.vg

    Data types: doctrine
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_media_docs(self, max_records: int = 999999) -> Generator[dict, None, None]:
        """Fetch all PDF documents from WP media endpoint."""
        logger.info("Fetching media documents via WP REST API...")
        items = _api_get_all("media", {"media_type": "application"})
        logger.info(f"Found {len(items)} media items")

        count = 0
        for item in items:
            if count >= max_records:
                return

            mime = item.get("mime_type", "")
            if mime != "application/pdf":
                continue

            source_url = item.get("source_url", "")
            wp_id = item.get("id", 0)
            title = _clean_html(item.get("title", {}).get("rendered", ""))
            date = item.get("date", "")

            if not source_url:
                continue

            time.sleep(REQUEST_DELAY)
            pdf_bytes = _download_pdf(source_url)
            if not pdf_bytes:
                logger.warning(f"PDF download failed: {title} ({source_url})")
                continue

            source_id = f"media-{wp_id}"
            text = extract_pdf_markdown(
                source="VG/ITA-TaxGuidance",
                source_id=source_id,
                pdf_bytes=pdf_bytes,
                table="doctrine",
            ) or ""

            if not text or len(text) < 50:
                logger.warning(f"Insufficient text for {title}: {len(text)} chars")
                continue

            yield {
                "wp_id": wp_id,
                "content_type": "document",
                "title": title,
                "text": text,
                "date": date,
                "url": source_url,
                "mime_type": mime,
            }
            count += 1
            logger.info(f"  [{count}] {title} ({len(text)} chars)")

    def _fetch_posts(self, max_records: int = 999999) -> Generator[dict, None, None]:
        """Fetch blog posts from WP REST API."""
        logger.info("Fetching posts via WP REST API...")
        items = _api_get_all("posts", {
            "_fields": "id,title,content,date,link,excerpt",
        })
        logger.info(f"Found {len(items)} posts")

        count = 0
        for item in items:
            if count >= max_records:
                return

            wp_id = item.get("id", 0)
            title = _clean_html(item.get("title", {}).get("rendered", ""))
            content_html = item.get("content", {}).get("rendered", "")
            text = _clean_html(content_html)
            date = item.get("date", "")
            link = item.get("link", "")

            if not text or len(text) < 50:
                continue

            yield {
                "wp_id": wp_id,
                "content_type": "post",
                "title": title,
                "text": text,
                "date": date,
                "url": link,
            }
            count += 1

    def _fetch_pages(self, max_records: int = 999999) -> Generator[dict, None, None]:
        """Fetch WP pages."""
        logger.info("Fetching pages via WP REST API...")
        items = _api_get_all("pages", {
            "_fields": "id,title,content,date,link",
        })
        logger.info(f"Found {len(items)} pages")

        count = 0
        for item in items:
            if count >= max_records:
                return

            wp_id = item.get("id", 0)
            title = _clean_html(item.get("title", {}).get("rendered", ""))
            content_html = item.get("content", {}).get("rendered", "")
            text = _clean_html(content_html)
            date = item.get("date", "")
            link = item.get("link", "")

            if not text or len(text) < 100:
                continue

            yield {
                "wp_id": wp_id,
                "content_type": "page",
                "title": title,
                "text": text,
                "date": date,
                "url": link,
            }
            count += 1

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents: PDFs, posts, pages."""
        yield from self._fetch_media_docs()
        yield from self._fetch_posts()
        yield from self._fetch_pages()

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch items modified in the last 90 days."""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
        # WP API supports after parameter
        items = _api_get_all("media", {
            "media_type": "application",
            "after": cutoff,
        })
        for item in items:
            if item.get("mime_type") != "application/pdf":
                continue
            source_url = item.get("source_url", "")
            wp_id = item.get("id", 0)
            title = _clean_html(item.get("title", {}).get("rendered", ""))
            date = item.get("date", "")
            if not source_url:
                continue
            time.sleep(REQUEST_DELAY)
            pdf_bytes = _download_pdf(source_url)
            if not pdf_bytes:
                continue
            source_id = f"media-{wp_id}"
            text = extract_pdf_markdown(
                source="VG/ITA-TaxGuidance",
                source_id=source_id,
                pdf_bytes=pdf_bytes,
                table="doctrine",
            ) or ""
            if text and len(text) >= 50:
                yield {
                    "wp_id": wp_id,
                    "content_type": "document",
                    "title": title,
                    "text": text,
                    "date": date,
                    "url": source_url,
                }

        posts = _api_get_all("posts", {
            "_fields": "id,title,content,date,link",
            "after": cutoff,
        })
        for item in posts:
            wp_id = item.get("id", 0)
            title = _clean_html(item.get("title", {}).get("rendered", ""))
            text = _clean_html(item.get("content", {}).get("rendered", ""))
            if text and len(text) >= 50:
                yield {
                    "wp_id": wp_id,
                    "content_type": "post",
                    "title": title,
                    "text": text,
                    "date": item.get("date", ""),
                    "url": item.get("link", ""),
                }

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        date = None
        date_str = raw.get("date", "")
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                pass

        wp_id = raw.get("wp_id", 0)
        content_type = raw.get("content_type", "document")

        return {
            "_id": f"{content_type}-{wp_id}",
            "_source": "VG/ITA-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw["text"],
            "date": date,
            "url": raw.get("url", ""),
            "wp_id": wp_id,
            "content_type": content_type,
        }


# === CLI entry point ===
if __name__ == "__main__":
    scraper = BVIITAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        items = _api_get("media", {"per_page": "1", "media_type": "application"})
        if items and len(items) > 0:
            print(f"OK: WP REST API returned media item: {items[0].get('title', {}).get('rendered', 'N/A')}")
        else:
            print("FAIL: No media items from WP REST API")
            sys.exit(1)

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 12 if sample else 999999

        if sample:
            logger.info("=== SAMPLE MODE: fetching ~12 records ===")
            # Get a mix: ~8 PDFs + ~4 posts
            for raw in scraper._fetch_media_docs(max_records=8):
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:70]}")

            for raw in scraper._fetch_posts(max_records=4):
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:70]}")

        elif command == "update":
            for raw in scraper.fetch_updates(""):
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:70]}")

        else:
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:70]}")

        logger.info(f"Done. Total records: {count}")
        if count == 0:
            logger.error("No records fetched — check connectivity")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
