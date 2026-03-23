#!/usr/bin/env python3
"""
Legal Data Hunter - UK Information Commissioner (ICO) Scraper

Fetches ICO decision notices and enforcement actions using:
  - POST /api/search (paginated JSON API, 25 results/page)
  - PDF downloads for full text extraction

Coverage: ~25,600 decisions from 2005-present.

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
logger = logging.getLogger("UK/ICO")

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


class UKICOScraper(BaseScraper):
    """
    Scraper for UK Information Commissioner decision notices.

    Strategy:
    - POST /api/search with rootPageId to paginate through decisions
    - Fetch detail page to find PDF link
    - Download and extract PDF text
    - Fall back to API description if PDF unavailable
    """

    BASE_URL = "https://ico.org.uk"
    SEARCH_URL = "/api/search"

    # rootPageId -> (section_name, content_type)
    SECTIONS = [
        (13635, "decision_notice"),
        (17222, "enforcement_action"),
    ]

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
            timeout=30,
        )

    def _search_page(self, root_page_id: int, page_number: int = 1) -> dict:
        """Search for decisions via the POST API."""
        payload = {
            "filters": [],
            "pageNumber": page_number,
            "order": "newest",
            "rootPageId": root_page_id,
            "term": "",
        }
        self.rate_limiter.wait()
        try:
            resp = self.client.post(self.SEARCH_URL, json_data=payload)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Search returned {resp.status_code} for page {page_number}")
            return {}
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return {}

    def _fetch_pdf_url(self, page_url: str) -> Optional[str]:
        """Fetch a decision page and extract the PDF download URL."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(page_url)
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.content, "html.parser")

            # Look for PDF links in further-reading or download sections
            for tag_name in ["further-reading", "a"]:
                for tag in soup.find_all(tag_name):
                    href = tag.get("x-href") or tag.get("href", "")
                    if href and href.lower().endswith(".pdf"):
                        if not href.startswith("http"):
                            href = self.BASE_URL + href
                        return href

            # Also check for links containing /media
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/media" in href and ".pdf" in href.lower():
                    if not href.startswith("http"):
                        href = self.BASE_URL + href
                    return href

            return None
        except Exception as e:
            logger.debug(f"Failed to fetch page {page_url}: {e}")
            return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download PDF content."""
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

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ICO decisions with full text."""
        for root_page_id, section_type in self.SECTIONS:
            first_result = self._search_page(root_page_id, 1)
            pagination = first_result.get("pagination", {})
            total = pagination.get("totalResults", 0)
            total_pages = pagination.get("totalPages", 0)
            logger.info(f"Section {section_type}: {total} results, {total_pages} pages")

            page_num = 1
            result = first_result
            count = 0
            skipped = 0

            while True:
                items = result.get("results", [])
                if not items:
                    break

                for item in items:
                    doc_url = item.get("url", "")
                    if doc_url and not doc_url.startswith("http"):
                        doc_url = self.BASE_URL + doc_url

                    description = item.get("description", "")
                    title = item.get("title", "")
                    doc_id = str(item.get("id", ""))
                    created = item.get("createdDateTime", "")

                    # Try to get PDF for full text
                    text = ""
                    if doc_url:
                        pdf_url = self._fetch_pdf_url(doc_url)
                        if pdf_url:
                            pdf_bytes = self._download_pdf(pdf_url)
                            if pdf_bytes:
                                text = extract_pdf_text(pdf_bytes)

                    # Fall back to description if no PDF text
                    if not text or len(text) < 100:
                        text = description

                    if not text or len(text) < 50:
                        skipped += 1
                        continue

                    # Extract metadata
                    meta = item.get("filterItemMetaData", "")
                    decisions = item.get("filterItemDecisions", [])
                    status_list = [d.get("status", "") for d in decisions if d.get("status")]
                    section_list = [d.get("section", "") for d in decisions if d.get("section")]

                    count += 1
                    yield {
                        "id": doc_id,
                        "title": title,
                        "text": text,
                        "description": description,
                        "section_type": section_type,
                        "metadata": meta,
                        "status": status_list,
                        "sections": section_list,
                        "created": created,
                        "url": doc_url,
                    }

                    if count % 50 == 0:
                        logger.info(f"  [{section_type}] {count} decisions fetched ({skipped} skipped)")

                page_num += 1
                if page_num > total_pages:
                    break
                result = self._search_page(root_page_id, page_num)

            logger.info(f"  [{section_type}] Total: {count} decisions ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions created since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        for root_page_id, section_type in self.SECTIONS:
            page_num = 1
            while True:
                result = self._search_page(root_page_id, page_num)
                items = result.get("results", [])
                if not items:
                    break
                found_old = False
                for item in items:
                    created = item.get("createdDateTime", "")
                    if created and created[:10] < since_str:
                        found_old = True
                        break

                    doc_url = item.get("url", "")
                    if doc_url and not doc_url.startswith("http"):
                        doc_url = self.BASE_URL + doc_url

                    text = item.get("description", "")
                    if doc_url:
                        pdf_url = self._fetch_pdf_url(doc_url)
                        if pdf_url:
                            pdf_bytes = self._download_pdf(pdf_url)
                            if pdf_bytes:
                                extracted = extract_pdf_text(pdf_bytes)
                                if extracted and len(extracted) > 100:
                                    text = extracted

                    if not text or len(text) < 50:
                        continue

                    decisions = item.get("filterItemDecisions", [])
                    yield {
                        "id": str(item.get("id", "")),
                        "title": item.get("title", ""),
                        "text": text,
                        "description": item.get("description", ""),
                        "section_type": section_type,
                        "metadata": item.get("filterItemMetaData", ""),
                        "status": [d.get("status", "") for d in decisions if d.get("status")],
                        "sections": [d.get("section", "") for d in decisions if d.get("section")],
                        "created": created,
                        "url": doc_url,
                    }

                if found_old:
                    break
                page_num += 1
                pagination = result.get("pagination", {})
                if page_num > pagination.get("totalPages", 0):
                    break

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        date_str = raw.get("created", "")
        date_iso = date_str[:10] if date_str else None

        return {
            "_id": f"UK/ICO/{raw.get('id', '')}",
            "_source": "UK/ICO",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": raw.get("id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "section_type": raw.get("section_type", ""),
            "status": raw.get("status", []),
            "sections": raw.get("sections", []),
            "date": date_iso,
            "url": raw.get("url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKICOScraper()

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
