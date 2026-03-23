#!/usr/bin/env python3
"""
Legal Data Hunter - UK Immigration & Asylum Chamber Scraper

Fetches tribunal decisions from GOV.UK using:
  - GET /api/search.json?filter_format=utaac_decision (UTAAC decisions)
  - GET /api/search.json?filter_format=asylum_support_decision
  - GET /api/content/{path} (full text via hidden_indexable_content or PDF)

Coverage: ~2,075 decisions. Full text from Content Store or PDF extraction.

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
logger = logging.getLogger("UK/IAC")

MAX_PDF_SIZE = 20 * 1024 * 1024


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes."""
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


class UKIACScraper(BaseScraper):
    """
    Scraper for UK Immigration and Asylum Chamber decisions on GOV.UK.

    Strategy:
    - Search API for utaac_decision and asylum_support_decision formats
    - Content Store for full text (hidden_indexable_content) or PDF fallback
    """

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"
    PAGE_SIZE = 500

    DECISION_FORMATS = [
        "utaac_decision",
        "asylum_support_decision",
    ]

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )

    def _search_decisions(self, fmt: str, start: int = 0) -> dict:
        """Search for decisions of a given format."""
        params = {
            "filter_format": fmt,
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
            logger.warning(f"Search returned {resp.status_code} for {fmt} start={start}")
            return {}
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {}

    def _fetch_content(self, path: str) -> Optional[dict]:
        """Fetch decision from Content Store."""
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.CONTENT_URL}{path}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.json()
            return None
        except Exception as e:
            logger.warning(f"Content failed for {path}: {e}")
            return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF from assets URL."""
        self.rate_limiter.wait()
        try:
            import requests
            resp = requests.get(url, headers={
                "User-Agent": "LegalDataHunter/1.0",
            }, timeout=60)
            if resp.status_code != 200:
                return None
            if len(resp.content) > MAX_PDF_SIZE:
                return None
            return resp.content
        except Exception:
            return None

    def _extract_text(self, content: dict) -> str:
        """Extract full text from decision."""
        details = content.get("details", {})
        metadata = details.get("metadata", {})

        # Try hidden_indexable_content first
        hic = metadata.get("hidden_indexable_content", "")
        if hic and len(hic.strip()) > 200:
            return hic.strip()

        # Try PDF attachments
        for att in details.get("attachments", []):
            url = att.get("url", "")
            if url and (url.lower().endswith(".pdf") or "pdf" in att.get("content_type", "").lower()):
                pdf_bytes = self._download_pdf(url)
                if pdf_bytes:
                    text = extract_pdf_text(pdf_bytes)
                    if text and len(text) > 100:
                        return text

        # Fallback: body HTML
        body = details.get("body", "")
        if body:
            soup = BeautifulSoup(body, "html.parser")
            return soup.get_text(separator="\n").strip()

        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all IAC decisions with full text."""
        for fmt in self.DECISION_FORMATS:
            start = 0
            first_result = self._search_decisions(fmt, start)
            total = first_result.get("total", 0)
            logger.info(f"Format '{fmt}': {total} decisions")

            result = first_result
            count = 0

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
                        continue

                    text = self._extract_text(content)
                    if not text or len(text) < 100:
                        continue

                    count += 1
                    yield {
                        "content_id": content.get("content_id", ""),
                        "title": content.get("title", ""),
                        "text": text,
                        "description": content.get("description", ""),
                        "decision_type": fmt,
                        "first_published_at": content.get("first_published_at", ""),
                        "public_updated_at": content.get("public_updated_at", ""),
                        "link": link,
                    }

                    if count % 50 == 0:
                        logger.info(f"  [{fmt}] {count} decisions fetched")

                start += len(results)
                if start >= total:
                    break
                result = self._search_decisions(fmt, start)

            logger.info(f"  [{fmt}] Total: {count} decisions with text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions published since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        for fmt in self.DECISION_FORMATS:
            for item in self._iterate_format(fmt):
                pub_date = item.get("public_timestamp", "")
                if pub_date and pub_date[:10] < since_str:
                    break
                link = item.get("link", "")
                if not link:
                    continue
                content = self._fetch_content(link)
                if not content:
                    continue
                text = self._extract_text(content)
                if not text or len(text) < 100:
                    continue
                yield {
                    "content_id": content.get("content_id", ""),
                    "title": content.get("title", ""),
                    "text": text,
                    "description": content.get("description", ""),
                    "decision_type": fmt,
                    "first_published_at": content.get("first_published_at", ""),
                    "public_updated_at": content.get("public_updated_at", ""),
                    "link": link,
                }

    def _iterate_format(self, fmt: str):
        """Iterate all search results for a format."""
        start = 0
        while True:
            result = self._search_decisions(fmt, start)
            results = result.get("results", [])
            if not results:
                break
            yield from results
            start += len(results)
            if start >= result.get("total", 0):
                break

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        date_str = raw.get("first_published_at", "") or raw.get("public_updated_at", "")
        date_iso = date_str[:10] if date_str else None

        return {
            "_id": f"UK/IAC/{raw.get('content_id', '')}",
            "_source": "UK/IAC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "content_id": raw.get("content_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "decision_type": raw.get("decision_type", ""),
            "date": date_iso,
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKIACScraper()

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
