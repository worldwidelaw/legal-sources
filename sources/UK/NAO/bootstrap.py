#!/usr/bin/env python3
"""
Legal Data Hunter - UK National Audit Office Scraper

Fetches NAO audit reports via WordPress REST API at nao.org.uk.
No authentication required. ~2,745 reports covering value for money,
financial audit, and investigation reports.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

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
logger = logging.getLogger("UK/NAO")

BASE_URL = "https://www.nao.org.uk"
API_BASE = "/wp-json/wp/v2"
PER_PAGE = 100


def strip_html(html: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class UKNAOScraper(BaseScraper):
    """
    Scraper for UK National Audit Office reports via WordPress REST API.

    Strategy:
    - Paginate through /wp-json/wp/v2/report endpoint
    - Extract content.rendered HTML as full text (report summaries with
      background, scope, and key findings)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )

    def _fetch_reports_page(self, page: int = 1) -> tuple:
        """Fetch a page of reports. Returns (items, total_pages)."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(
                f"{API_BASE}/report?per_page={PER_PAGE}&page={page}"
                f"&orderby=date&order=desc"
            )
            if resp.status_code == 200:
                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                return resp.json(), total_pages
            if resp.status_code == 400:
                # Beyond last page
                return [], 0
            logger.warning(f"Report page {page} returned {resp.status_code}")
            return [], 0
        except Exception as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            return [], 0

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all NAO reports with full text."""
        page = 1
        items, total_pages = self._fetch_reports_page(page)
        logger.info(f"Total pages: {total_pages}")

        while items:
            for item in items:
                content_html = item.get("content", {}).get("rendered", "")
                text = strip_html(content_html)

                yield {
                    "wp_id": item.get("id"),
                    "title": item.get("title", {}).get("rendered", ""),
                    "text": text,
                    "date": item.get("date", ""),
                    "modified": item.get("modified_gmt", ""),
                    "slug": item.get("slug", ""),
                    "link": item.get("link", ""),
                    "excerpt": strip_html(
                        item.get("excerpt", {}).get("rendered", "")
                    ),
                    "report_type": item.get("report_type", []),
                    "department": item.get("department", []),
                    "topic": item.get("topic", []),
                }

            page += 1
            if page > total_pages:
                break
            items, _ = self._fetch_reports_page(page)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield reports modified since the given datetime."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1

        while True:
            self.rate_limiter.wait()
            try:
                resp = self.client.get(
                    f"{API_BASE}/report?per_page={PER_PAGE}&page={page}"
                    f"&modified_after={since_str}&orderby=modified&order=desc"
                )
                if resp.status_code != 200:
                    break
                items = resp.json()
                if not items:
                    break

                for item in items:
                    content_html = item.get("content", {}).get("rendered", "")
                    text = strip_html(content_html)
                    yield {
                        "wp_id": item.get("id"),
                        "title": item.get("title", {}).get("rendered", ""),
                        "text": text,
                        "date": item.get("date", ""),
                        "modified": item.get("modified_gmt", ""),
                        "slug": item.get("slug", ""),
                        "link": item.get("link", ""),
                        "excerpt": strip_html(
                            item.get("excerpt", {}).get("rendered", "")
                        ),
                        "report_type": item.get("report_type", []),
                        "department": item.get("department", []),
                        "topic": item.get("topic", []),
                    }

                page += 1
            except Exception as e:
                logger.error(f"Update fetch failed on page {page}: {e}")
                break

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw WP API response into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            logger.debug(
                f"Skipping {raw.get('slug', '?')}: no/short text ({len(text)} chars)"
            )
            return None

        wp_id = raw.get("wp_id", "")
        slug = raw.get("slug", "")
        doc_id = f"nao_{wp_id}_{slug}" if wp_id else slug
        if not doc_id:
            return None

        # Clean HTML entities from title
        title = raw.get("title", "")
        if title:
            title = BeautifulSoup(title, "html.parser").get_text()

        date_str = raw.get("date", "")
        date_val = None
        if date_str:
            try:
                date_val = datetime.fromisoformat(date_str).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_val = None

        return {
            "_id": doc_id,
            "_source": "UK/NAO",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_val,
            "url": raw.get("link", ""),
            "excerpt": raw.get("excerpt", ""),
            "report_type_ids": raw.get("report_type", []),
            "department_ids": raw.get("department", []),
            "topic_ids": raw.get("topic", []),
        }


# ── CLI entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKNAOScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
