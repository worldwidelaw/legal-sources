#!/usr/bin/env python3
"""
AG/Gazette -- Antigua and Barbuda Official Gazette

Fetches weekly gazette PDFs published by gazette.laws.gov.ag. Discovery via
the site's WordPress REST API (/wp-json/wp/v2/media). Full text extracted
with PyMuPDF.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import html
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AG.Gazette")

BASE_URL = "http://gazette.laws.gov.ag"
MEDIA_API = f"{BASE_URL}/wp-json/wp/v2/media"
PER_PAGE = 50


def _extract_text_from_pdf(pdf_bytes: bytes) -> tuple:
    try:
        import fitz
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        page_count = len(doc)
        parts = []
        for page in doc:
            t = page.get_text()
            if t:
                parts.append(t.strip())
        doc.close()
        return "\n\n".join(parts), page_count
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return "", 0


class AGGazetteScraper(BaseScraper):
    """Scraper for AG/Gazette."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json, application/pdf, */*",
        })

    def _iter_media(self, since: Optional[datetime] = None) -> Generator[dict, None, None]:
        page = 1
        total_pages = None
        while True:
            params = {
                "per_page": PER_PAGE,
                "page": page,
                "orderby": "date",
                "order": "desc",
            }
            if since:
                params["after"] = since.isoformat()
            try:
                time.sleep(1.5)
                resp = self.session.get(MEDIA_API, params=params, timeout=60)
            except Exception as e:
                logger.warning(f"Media list error page={page}: {e}")
                return
            if resp.status_code == 400:
                # WP returns 400 when page > total pages
                return
            if resp.status_code != 200:
                logger.warning(f"Media list HTTP {resp.status_code} page={page}")
                return
            if total_pages is None:
                total_pages = int(resp.headers.get("X-WP-TotalPages", "1") or 1)
                logger.info(f"Media catalogue: {resp.headers.get('X-WP-Total')} items across {total_pages} pages")
            items = resp.json() or []
            if not items:
                return
            for it in items:
                src = it.get("source_url") or ""
                if not src.lower().endswith(".pdf"):
                    continue
                yield it
            page += 1
            if total_pages and page > total_pages:
                return

    def _download_pdf(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(1.0)
            resp = self.session.get(url, timeout=180)
            if resp.status_code != 200 or len(resp.content) < 200:
                logger.warning(f"PDF download failed ({resp.status_code}, {len(resp.content)}b): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"PDF download error {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        for item in self._iter_media():
            yield item

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        for item in self._iter_media(since=since):
            yield item

    def normalize(self, raw: dict) -> Optional[dict]:
        media_id = raw.get("id")
        url = raw.get("source_url")
        if not media_id or not url:
            return None

        pdf_bytes = self._download_pdf(url)
        if not pdf_bytes:
            return None

        text, page_count = _extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for media_id={media_id}: {len(text)} chars")
            return None

        # Title: prefer rendered title, fall back to filename
        title_raw = (raw.get("title") or {}).get("rendered") or ""
        title = html.unescape(re.sub(r"<[^>]+>", "", title_raw)).strip()
        if not title:
            title = url.rsplit("/", 1)[-1].replace(".pdf", "")
        title = re.sub(r"\s+", " ", title)

        date_raw = raw.get("date") or raw.get("date_gmt")
        date_str = None
        if date_raw:
            try:
                date_str = datetime.fromisoformat(date_raw.replace("Z", "+00:00")).date().isoformat()
            except Exception:
                date_str = date_raw[:10]

        return {
            "_id": f"AG-Gazette-{media_id}",
            "_source": "AG/Gazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": url,
            "gazette_id": f"AG-Gazette-{media_id}",
            "media_id": int(media_id),
            "page_count": page_count,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = AGGazetteScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        items = []
        for it in scraper._iter_media():
            items.append(it)
            if len(items) >= 3:
                break
        if not items:
            print("FAILED: no media items returned")
            sys.exit(1)
        print(f"OK: fetched {len(items)} media items")
        for it in items:
            print(f"  {it.get('id')} {it.get('date')} {it.get('source_url')}")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
