#!/usr/bin/env python3
"""
EU/Ombudsman -- European Ombudsman Decisions Data Fetcher

Fetches decisions from the European Ombudsman REST API.

Strategy:
  - Uses the REST API at ombudsman.europa.eu/rest/documents
  - List endpoint returns full text content in HTML (no separate fetch needed)
  - Paginated (10 per page), ~5,600 EODECISION documents
  - HTML content stripped to plain text

Endpoints:
  - Search: GET /rest/documents?page={n}&lang=en&format=EODECISION
  - Content: GET /rest/docVersionContents/{techKey}?lang=en (backup)

Data:
  - Ombudsman decisions on EU maladministration complaints
  - Covers transparency, access to documents, recruitment, contracts
  - Available since ~2001

License: Open (EU institutions)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.Ombudsman")

API_BASE = "https://www.ombudsman.europa.eu/rest"


def strip_html(html: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html:
        return ""
    # Remove footnote markers like [1], [2] etc. that are just links
    text = re.sub(r'<a[^>]*class="[^"]*footnote[^"]*"[^>]*>.*?</a>', '', html, flags=re.DOTALL)
    # Remove all HTML tags
    text = re.sub(r'<[^>]+>', ' ', text)
    # Decode HTML entities
    text = unescape(text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Restore paragraph breaks (approximate)
    text = re.sub(r'\s*\.\s+(\d+\.)', r'.\n\n\1', text)
    return text


class OmbudsmanScraper(BaseScraper):
    """
    Scraper for EU/Ombudsman -- European Ombudsman Decisions.
    Country: EU
    URL: https://www.ombudsman.europa.eu

    Data types: case_law
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _search_decisions(self, page: int = 1, year: Optional[int] = None) -> Optional[Dict[str, Any]]:
        """Search for Ombudsman decisions."""
        try:
            self.rate_limiter.wait()
            params = {
                "page": page,
                "lang": "en",
                "format": "EODECISION",
            }
            if year:
                params["year"] = year

            resp = self.client.get("/documents", params=params)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to search decisions (page={page}): {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Ombudsman decisions."""
        page = 1
        total_yielded = 0

        while True:
            logger.info(f"Fetching page {page}...")
            result = self._search_decisions(page=page)

            if not result:
                break

            documents = result.get("documents", [])
            if not documents:
                break

            total = result.get("pageItem", {}).get("totalResult", 0)

            for doc in documents:
                yield doc
                total_yielded += 1

            if total_yielded >= total:
                break

            page += 1

        logger.info(f"Fetched {total_yielded} decisions total")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions from recent years."""
        current_year = datetime.now().year
        since_year = since.year

        for year in range(since_year, current_year + 1):
            logger.info(f"Fetching decisions for year {year}...")
            page = 1

            while True:
                result = self._search_decisions(page=page, year=year)
                if not result:
                    break

                documents = result.get("documents", [])
                if not documents:
                    break

                for doc in documents:
                    doc_date_str = doc.get("documentDate", "")
                    if doc_date_str:
                        try:
                            doc_date = datetime.fromisoformat(doc_date_str.replace("Z", "+00:00"))
                            if doc_date.replace(tzinfo=timezone.utc) < since.replace(tzinfo=timezone.utc):
                                continue
                        except (ValueError, TypeError):
                            pass
                    yield doc

                page += 1

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw API response to standard schema."""
        tech_key = raw.get("techKey")
        if not tech_key:
            return None

        dvc = raw.get("docVersionContent", {})
        title = dvc.get("title", "")
        content_html = dvc.get("content", "")
        summary_html = dvc.get("summary", "")

        # Strip HTML to get plain text
        text = strip_html(content_html)
        summary = strip_html(summary_html)

        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for techKey {tech_key}: {len(text)} chars")
            return None

        # Extract date
        doc_date = raw.get("documentDate", "")
        date_str = None
        if doc_date:
            try:
                dt = datetime.fromisoformat(doc_date.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_str = doc_date[:10] if len(doc_date) >= 10 else None

        case_ref = raw.get("caseRef", "")
        case_id = raw.get("caseId")

        # Build URL
        url = f"https://www.ombudsman.europa.eu/en/decision/en/{tech_key}"

        return {
            "_id": f"EU-OMB-{tech_key}",
            "_source": "EU/Ombudsman",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "summary": summary,
            "date": date_str,
            "url": url,
            "case_id": case_id,
            "case_ref": case_ref,
        }


if __name__ == "__main__":
    scraper = OmbudsmanScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\nBootstrap complete: {stats}")

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats}")

    elif command == "test":
        print("Testing EU Ombudsman API...")
        result = scraper._search_decisions(page=1)
        if result and "documents" in result:
            total = result.get("pageItem", {}).get("totalResult", 0)
            print(f"  Total decisions: {total}")
            docs = result["documents"]
            if docs:
                dvc = docs[0].get("docVersionContent", {})
                print(f"  Sample title: {dvc.get('title', '')[:100]}")
                content = strip_html(dvc.get("content", ""))
                print(f"  Content length: {len(content)} chars")
            print("  Connection successful")
        else:
            print("  Connection FAILED")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
