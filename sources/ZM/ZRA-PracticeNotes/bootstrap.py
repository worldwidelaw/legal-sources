#!/usr/bin/env python3
"""
ZM/ZRA-PracticeNotes -- Zambia Revenue Authority Practice Notes

Fetches annual tax practice notes from the ZRA WordPress site via the
WP REST API (media endpoint), downloads PDFs, and extracts full text.

Strategy:
  - Query WP REST API for media items matching "practice note"
  - Filter for PDF attachments
  - Download each PDF and extract text via pdfplumber
  - Deduplicate by WordPress media ID

Endpoints:
  - Media: https://www.zra.org.zm/wp-json/wp/v2/media?search=practice+note&media_type=application

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import json
import time
import io
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZM.ZRA-PracticeNotes")

BASE_URL = "https://www.zra.org.zm"
MEDIA_API = f"{BASE_URL}/wp-json/wp/v2/media"
USER_AGENT = "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)"


class ZRAPracticeNotesScraper(BaseScraper):
    """Scraper for ZM/ZRA-PracticeNotes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        self.session.verify = False

    def _get_json(self, url: str) -> Optional[list]:
        """GET JSON from WordPress REST API with retry."""
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:100]}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text using pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                return None

            text_parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            full_text = "\n\n".join(text_parts)
            if len(full_text.strip()) < 50:
                return None
            return full_text.strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def _extract_year_from_item(self, item: dict) -> Optional[str]:
        """Try to extract a year from the media item metadata or title."""
        # Try the upload date first
        date_str = item.get("date", "")
        if date_str:
            return date_str[:10]

        # Try to extract year from title
        title = item.get("title", {}).get("rendered", "")
        year_match = re.search(r'20[0-2]\d|199\d', title)
        if year_match:
            return f"{year_match.group(0)}-01-01"
        return None

    def _clean_title(self, title: str) -> str:
        """Clean HTML entities from WordPress title."""
        import html
        return html.unescape(title).strip().strip("_")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all practice note documents with full text from PDFs."""
        url = f"{MEDIA_API}?per_page=100&search=practice+note&media_type=application"
        logger.info("Fetching practice notes from WP REST API...")
        items = self._get_json(url)
        if not items:
            logger.error("Failed to fetch media items")
            return

        logger.info(f"Found {len(items)} media items")
        seen_urls = set()
        total = 0

        for item in items:
            source_url = item.get("source_url", "")
            if not source_url or not source_url.endswith(".pdf"):
                continue

            # Deduplicate by URL (some duplicates with different IDs)
            if source_url in seen_urls:
                continue
            seen_urls.add(source_url)

            media_id = str(item.get("id", ""))
            title = self._clean_title(item.get("title", {}).get("rendered", ""))
            date = self._extract_year_from_item(item)

            logger.info(f"Extracting: {title[:70]}...")
            text = self._extract_pdf_text(source_url)
            if not text:
                logger.debug(f"No text extracted for: {title}")
                continue

            total += 1
            yield {
                "id": media_id,
                "title": title,
                "text": text,
                "date": date,
                "url": source_url,
            }
            time.sleep(1.5)

        logger.info(f"Total: {total} practice notes with full text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents modified since the given date."""
        since_iso = since.strftime("%Y-%m-%dT%H:%M:%S")
        url = (
            f"{MEDIA_API}?per_page=100&search=practice+note"
            f"&media_type=application&after={since_iso}"
        )
        items = self._get_json(url)
        if not items:
            return

        seen_urls = set()
        for item in items:
            source_url = item.get("source_url", "")
            if not source_url or not source_url.endswith(".pdf"):
                continue
            if source_url in seen_urls:
                continue
            seen_urls.add(source_url)

            media_id = str(item.get("id", ""))
            title = self._clean_title(item.get("title", {}).get("rendered", ""))
            date = self._extract_year_from_item(item)

            text = self._extract_pdf_text(source_url)
            if not text:
                continue

            yield {
                "id": media_id,
                "title": title,
                "text": text,
                "date": date,
                "url": source_url,
            }
            time.sleep(1.5)

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        date_str = raw.get("date", "") or ""
        if date_str and "T" in date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        return {
            "_id": raw.get("id", ""),
            "_source": "ZM/ZRA-PracticeNotes",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date_str,
            "url": raw.get("url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    scraper = ZRAPracticeNotesScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to ZRA WP REST API...")
        items = scraper._get_json(f"{MEDIA_API}?per_page=1&search=practice+note&media_type=application")
        if items and len(items) > 0:
            title = items[0].get("title", {}).get("rendered", "?")
            logger.info(f"OK — got: {title}")
            print("Test passed: WP REST API accessible")
        else:
            logger.error("Failed to reach ZRA WP REST API")
            sys.exit(1)

    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=90)
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
