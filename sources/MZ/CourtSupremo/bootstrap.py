#!/usr/bin/env python3
"""
MZ/CourtSupremo -- Mozambique Supreme Court (Tribunal Supremo)

Fetches court decisions (acórdãos) from the Tribunal Supremo de Moçambique via
the WordPress REST API media endpoint. Full text is extracted from PDF attachments
using pdfplumber.

Strategy:
  1. Paginate /wp-json/wp/v2/media?mime_type=application/pdf
  2. Filter PDFs that are court decisions (by title pattern)
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
from typing import Generator, Dict, Any
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
logger = logging.getLogger("legal-data-hunter.MZ.CourtSupremo")

BASE_URL = "https://www.ts.gov.mz"
PER_PAGE = 100

# Patterns that indicate a PDF is a court decision
DECISION_RE = re.compile(
    r"(Ac\b|Acord[aã]o|Proc|Processo|Habeas|Senten[cç]|proc\.|PROCESSO)",
    re.IGNORECASE,
)

# Pattern to extract process number from title
PROC_NUM_RE = re.compile(
    r"(?:proc\.?|processo)\s*(?:n[.ºo°]*\s*)?(\d[\d/\-–]+(?:\s*-?\s*[A-Z])?)",
    re.IGNORECASE,
)

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


class MZCourtSupremoScraper(BaseScraper):
    """Scraper for MZ/CourtSupremo -- Mozambique Supreme Court."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

    def _fetch_media_page(self, page: int) -> tuple:
        """Fetch a page of PDF media items. Returns (items, total_pages)."""
        params = {
            "per_page": PER_PAGE,
            "mime_type": "application/pdf",
            "page": page,
            "orderby": "id",
            "order": "desc",
        }
        time.sleep(1.5)
        resp = self.session.get(
            f"{BASE_URL}/wp-json/wp/v2/media",
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
        return resp.json(), total_pages

    def _is_decision(self, title: str, filename: str) -> bool:
        """Check if a media item looks like a court decision."""
        combined = f"{title} {filename}"
        if DECISION_RE.search(combined):
            return True
        # Also include generic "Processo-NNN" filenames
        if re.search(r"Processo[\-_]\d", combined, re.IGNORECASE):
            return True
        return False

    def _extract_process_number(self, title: str) -> str:
        """Extract process number from title."""
        match = PROC_NUM_RE.search(title)
        if match:
            return match.group(1).strip()
        # Try simpler pattern: number/year or year-number
        match = re.search(r"(\d{1,4}[\-/]\d{2,4}(?:\s*-?\s*[A-Z])?)", title)
        if match:
            return match.group(1).strip()
        return ""

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
        media_id = raw.get("id", "")

        title = raw.get("title", {})
        if isinstance(title, dict):
            title = title.get("rendered", "")
        title = unescape(str(title)).strip()

        wp_date = raw.get("date", "")
        date = wp_date[:10] if wp_date else None

        pdf_url = raw.get("source_url", "")
        text = raw.get("_full_text", "")
        process_number = self._extract_process_number(title)

        return {
            "_id": f"MZ/CourtSupremo/{media_id}",
            "_source": "MZ/CourtSupremo",
            "_type": "case_law",
            "_fetched_at": now,
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "doc_id": str(media_id),
            "process_number": process_number,
            "court": "Tribunal Supremo de Moçambique",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0
        seen_urls = set()

        page = 1
        total_pages = 1

        while page <= total_pages:
            if limit and count >= limit:
                break

            try:
                items, total_pages = self._fetch_media_page(page)
            except Exception as e:
                logger.error(f"Failed to fetch media page {page}: {e}")
                break

            logger.info(f"Media page {page}/{total_pages}: {len(items)} items")

            for item in items:
                if limit and count >= limit:
                    break

                title = item.get("title", {})
                if isinstance(title, dict):
                    title_text = title.get("rendered", "")
                else:
                    title_text = str(title)
                title_text = unescape(title_text).strip()

                source_url = item.get("source_url", "")
                slug = item.get("slug", "")

                # Deduplicate by URL
                if source_url in seen_urls:
                    continue
                seen_urls.add(source_url)

                # Filter: must look like a court decision
                if not self._is_decision(title_text, slug):
                    logger.debug(f"  Skipping non-decision: {title_text[:60]}")
                    continue

                # Download and extract text
                text = self._download_pdf_text(source_url)
                if len(text) < 100:
                    logger.warning(
                        f"  PDF text too short for '{title_text[:60]}' ({len(text)} chars)"
                    )
                    continue

                item["_full_text"] = text
                yield item
                count += 1

                logger.info(f"  [{count}] {title_text[:70]} ({len(text)} chars)")

            page += 1

        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        seen_urls = set()
        page = 1
        total_pages = 1

        while page <= total_pages:
            try:
                params = {
                    "per_page": PER_PAGE,
                    "mime_type": "application/pdf",
                    "page": page,
                    "after": f"{since}T00:00:00",
                    "orderby": "date",
                    "order": "asc",
                }
                time.sleep(1.5)
                resp = self.session.get(
                    f"{BASE_URL}/wp-json/wp/v2/media",
                    params=params,
                    timeout=60,
                )
                resp.raise_for_status()
                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                items = resp.json()
            except Exception as e:
                logger.error(f"Failed to fetch updates page {page}: {e}")
                break

            for item in items:
                title = item.get("title", {})
                if isinstance(title, dict):
                    title_text = title.get("rendered", "")
                else:
                    title_text = str(title)

                source_url = item.get("source_url", "")
                slug = item.get("slug", "")

                if source_url in seen_urls:
                    continue
                seen_urls.add(source_url)

                if not self._is_decision(title_text, slug):
                    continue

                text = self._download_pdf_text(source_url)
                item["_full_text"] = text
                yield item

            page += 1


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = MZCourtSupremoScraper()

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
