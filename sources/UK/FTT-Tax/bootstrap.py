#!/usr/bin/env python3
"""
World Wide Law - UK Tax & Chancery Tribunal Decisions Scraper

Fetches tax tribunal decisions from GOV.UK using:
  - GET /api/search.json?filter_format=tax_tribunal_decision (discovery)
  - GET /api/content/{path} (metadata + PDF attachment URLs)
  - PDF download + text extraction via pypdf

Coverage: ~1,400 decisions. Full text from PDF attachments.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import io
import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import pypdf
from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/FTT-Tax")

MAX_PDF_SIZE = 20 * 1024 * 1024  # 20MB limit


def html_to_text(html: str) -> str:
    """Strip HTML tags and return clean text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pypdf."""
    try:
        reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                parts.append(text)
        return "\n\n".join(parts).strip()
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


class UKFTTTaxScraper(BaseScraper):
    """
    Scraper for UK Tax and Chancery Tribunal decisions on GOV.UK.

    Strategy:
    - Search API to discover all tax tribunal decisions
    - Content Store API for metadata and PDF attachment URLs
    - Download PDFs and extract text with pypdf
    """

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"
    PAGE_SIZE = 500

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )
        # Separate client for PDF downloads (different base URL)
        self.pdf_client = HttpClient(
            base_url="https://assets.publishing.service.gov.uk",
            headers={
                "User-Agent": "WorldWideLaw/1.0 (legal research project)",
            },
            timeout=60,
        )

    def _search_decisions(self, start: int = 0) -> dict:
        """Search for tax tribunal decisions."""
        params = {
            "filter_format": "tax_tribunal_decision",
            "count": self.PAGE_SIZE,
            "start": start,
            "order": "-public_timestamp",
            "fields": "title,link,public_timestamp",
        }
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.SEARCH_URL, params=params)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Search returned {resp.status_code} for start={start}")
            return {}
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {}

    def _fetch_content(self, path: str) -> Optional[dict]:
        """Fetch decision metadata from Content Store."""
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.CONTENT_URL}{path}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Content API returned {resp.status_code} for {path}")
            return None
        except Exception as e:
            logger.warning(f"Content API failed for {path}: {e}")
            return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF from assets.publishing.service.gov.uk."""
        self.rate_limiter.wait()
        try:
            import requests
            resp = requests.get(url, headers={
                "User-Agent": "WorldWideLaw/1.0 (legal research project)",
            }, timeout=60, stream=True)
            if resp.status_code != 200:
                logger.warning(f"PDF download returned {resp.status_code}: {url}")
                return None
            content_length = int(resp.headers.get("content-length", 0))
            if content_length > MAX_PDF_SIZE:
                logger.warning(f"PDF too large ({content_length} bytes): {url}")
                return None
            data = resp.content
            if len(data) > MAX_PDF_SIZE:
                logger.warning(f"PDF too large ({len(data)} bytes): {url}")
                return None
            return data
        except Exception as e:
            logger.warning(f"PDF download failed for {url}: {e}")
            return None

    def _extract_decision_text(self, content: dict) -> str:
        """Extract full text from a tax tribunal decision.

        First tries hidden_indexable_content, then PDF attachments.
        """
        details = content.get("details", {})
        metadata = details.get("metadata", {})

        # Try hidden_indexable_content first
        hic = metadata.get("hidden_indexable_content", "")
        if hic and len(hic.strip()) > 200:
            return hic.strip()

        # Try PDF attachments
        attachments = details.get("attachments", [])
        for att in attachments:
            url = att.get("url", "")
            content_type = att.get("content_type", "")
            if url and ("pdf" in content_type.lower() or url.lower().endswith(".pdf")):
                pdf_bytes = self._download_pdf(url)
                if pdf_bytes:
                    text = extract_pdf_text(pdf_bytes)
                    if text and len(text) > 100:
                        return text

        # Fallback: body HTML
        body = details.get("body", "")
        if body:
            return html_to_text(body)

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all tax tribunal decisions with full text."""
        start = 0
        first_result = self._search_decisions(start)
        total = first_result.get("total", 0)
        logger.info(f"Total tax tribunal decisions: {total}")

        result = first_result
        count = 0
        skipped = 0

        while True:
            results = result.get("results", [])
            if not results:
                break

            for item in results:
                link = item.get("link", "")
                if not link:
                    continue

                content = self._fetch_content(link)
                if not content:
                    skipped += 1
                    continue

                text = self._extract_decision_text(content)
                if not text or len(text) < 100:
                    skipped += 1
                    continue

                count += 1
                yield {
                    "content_id": content.get("content_id", ""),
                    "title": content.get("title", ""),
                    "text": text,
                    "description": content.get("description", ""),
                    "first_published_at": content.get("first_published_at", ""),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "link": link,
                }

                if count % 50 == 0:
                    logger.info(f"  {count} decisions fetched ({skipped} skipped)")

            start += len(results)
            if start >= total:
                break
            result = self._search_decisions(start)

        logger.info(f"Total: {count} decisions with text ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions published since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        start = 0

        while True:
            result = self._search_decisions(start)
            results = result.get("results", [])
            if not results:
                break

            for item in results:
                pub_date = item.get("public_timestamp", "")
                if pub_date and pub_date[:10] < since_str:
                    return

                link = item.get("link", "")
                if not link:
                    continue
                content = self._fetch_content(link)
                if not content:
                    continue
                text = self._extract_decision_text(content)
                if not text or len(text) < 100:
                    continue
                yield {
                    "content_id": content.get("content_id", ""),
                    "title": content.get("title", ""),
                    "text": text,
                    "description": content.get("description", ""),
                    "first_published_at": content.get("first_published_at", ""),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "link": link,
                }

            start += len(results)
            total = result.get("total", 0)
            if start >= total:
                break

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        date_str = raw.get("first_published_at", "") or raw.get("public_updated_at", "")
        date_iso = date_str[:10] if date_str else None

        return {
            "_id": f"UK/FTT-Tax/{raw.get('content_id', '')}",
            "_source": "UK/FTT-Tax",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "content_id": raw.get("content_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "date": date_iso,
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKFTTTaxScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
