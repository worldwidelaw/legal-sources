#!/usr/bin/env python3
"""
GR/HCMC -- Hellenic Capital Market Commission Data Fetcher

Fetches decisions, circulars, and press releases from the HCMC Liferay portal.

Strategy:
  - Iterate over years (2007-2026) for each category (decisions, circulars, press)
  - Parse HTML listing pages for document links
  - Download PDFs from /vdrv/elib/ URLs
  - Extract full text from PDFs using pypdf

Endpoints:
  - List: http://www.hcmc.gr/el_GR/web/portal/elib/{category}?catyear={year}
  - PDF: http://www.hcmc.gr/vdrv/elib/{uuid}
  - Detail: http://www.hcmc.gr/el_GR/web/portal/elib/view?auid={id}

Data:
  - ~1000-2000 documents (decisions, circulars, press releases)
  - Language: Greek
  - HTTP only (no HTTPS support)
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import html as html_module
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.HCMC")

BASE_URL = "http://www.hcmc.gr"
CATEGORIES = {
    "decisions": "/el_GR/web/portal/elib/decisions",
    "circulars": "/el_GR/web/portal/elib/circulars",
    "press": "/el_GR/web/portal/elib/press",
}
YEARS = list(range(2026, 2006, -1))


class GreekHCMCScraper(BaseScraper):
    """
    Scraper for GR/HCMC -- Hellenic Capital Market Commission.
    Country: GR
    URL: http://www.hcmc.gr

    Data types: doctrine
    Auth: none (Open public access, HTTP only)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml,application/pdf",
                "Accept-Language": "el,en",
            },
            timeout=60,
        )

    def _scrape_list_page(self, category: str, year: int) -> List[Dict[str, Any]]:
        """Scrape a category listing page for a given year."""
        items = []
        path = CATEGORIES[category]
        url = f"{path}?catyear={year}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            content = resp.text

            # Pattern 1: Direct PDF links from /vdrv/elib/
            # <a href="/vdrv/elib/{uuid}" target="_blank">
            #   <span class="a-p-elib-itemTitle">Title</span>
            # </a>
            pattern1 = re.compile(
                r'<a\s+href="(/vdrv/elib/[^"]+)"[^>]*>\s*'
                r'<span[^>]*class="[^"]*a-p-elib-itemTitle[^"]*"[^>]*>'
                r'(.*?)</span>',
                re.DOTALL | re.IGNORECASE
            )

            for match in pattern1.finditer(content):
                pdf_url = match.group(1)
                title = re.sub(r'<[^>]+>', '', match.group(2)).strip()
                title = html_module.unescape(title)

                if not title or len(title) < 3:
                    continue

                # Create a stable ID from the URL
                doc_id = pdf_url.split("/vdrv/elib/")[-1]

                items.append({
                    "doc_id": doc_id,
                    "title": title,
                    "pdf_url": pdf_url,
                    "detail_url": None,
                    "category": category,
                    "year": year,
                })

            # Pattern 2: Links to /elib/view?auid= (multi-document items)
            pattern2 = re.compile(
                r'<a\s+href="([^"]*elib/view\?auid=[^"]+)"[^>]*>\s*'
                r'<span[^>]*class="[^"]*a-p-elib-itemTitle[^"]*"[^>]*>'
                r'(.*?)</span>',
                re.DOTALL | re.IGNORECASE
            )

            seen_ids = {item["doc_id"] for item in items}
            for match in pattern2.finditer(content):
                detail_url = match.group(1)
                title = re.sub(r'<[^>]+>', '', match.group(2)).strip()
                title = html_module.unescape(title)

                if not title or len(title) < 3:
                    continue

                # Extract auid as ID
                auid_match = re.search(r'auid=([^&]+)', detail_url)
                doc_id = auid_match.group(1) if auid_match else hashlib.md5(detail_url.encode()).hexdigest()[:12]

                if doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)

                items.append({
                    "doc_id": doc_id,
                    "title": title,
                    "pdf_url": None,
                    "detail_url": detail_url,
                    "category": category,
                    "year": year,
                })

            # Pattern 3: Broader fallback - any elib item links
            if not items:
                pattern3 = re.compile(
                    r'a-p-elib-itemTitle[^>]*>(.*?)</span>.*?'
                    r'href="(/vdrv/elib/[^"]+|[^"]*elib/view[^"]+)"',
                    re.DOTALL | re.IGNORECASE
                )
                for match in pattern3.finditer(content):
                    title = re.sub(r'<[^>]+>', '', match.group(1)).strip()
                    title = html_module.unescape(title)
                    href = match.group(2)

                    if not title or len(title) < 3:
                        continue

                    if "/vdrv/elib/" in href:
                        doc_id = href.split("/vdrv/elib/")[-1]
                        items.append({
                            "doc_id": doc_id,
                            "title": title,
                            "pdf_url": href,
                            "detail_url": None,
                            "category": category,
                            "year": year,
                        })
                    elif "elib/view" in href:
                        auid_match = re.search(r'auid=([^&]+)', href)
                        doc_id = auid_match.group(1) if auid_match else hashlib.md5(href.encode()).hexdigest()[:12]
                        items.append({
                            "doc_id": doc_id,
                            "title": title,
                            "pdf_url": None,
                            "detail_url": href,
                            "category": category,
                            "year": year,
                        })

            logger.info(f"{category}/{year}: found {len(items)} items")
            return items

        except Exception as e:
            logger.error(f"Failed to scrape {category}/{year}: {e}")
            return []

    def _fetch_pdf_from_detail(self, detail_url: str) -> Optional[str]:
        """Fetch a detail page and extract the first PDF link."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(detail_url)
            resp.raise_for_status()

            pdf_match = re.search(
                r'href="(/vdrv/elib/[^"]+)"',
                resp.text, re.IGNORECASE
            )
            if pdf_match:
                return pdf_match.group(1)
            return None
        except Exception as e:
            logger.warning(f"Failed to fetch detail page {detail_url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="GR/HCMC",
            source_id="",
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def _fetch_detail(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch full text for an item."""
        result = dict(item)

        # If we have a detail URL but no PDF URL, fetch the detail page first
        if not result.get("pdf_url") and result.get("detail_url"):
            pdf_url = self._fetch_pdf_from_detail(result["detail_url"])
            if pdf_url:
                result["pdf_url"] = pdf_url

        # Extract text from PDF
        if result.get("pdf_url"):
            text = self._extract_pdf_text(result["pdf_url"])
            result["text"] = text
        else:
            result["text"] = ""
            logger.warning(f"No PDF URL for item {item['doc_id']}")

        if not result.get("text"):
            logger.warning(f"No text extracted for item {item['doc_id']}")

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        doc_id = raw.get("doc_id", "unknown")
        category = raw.get("category", "unknown")
        year = raw.get("year", "")

        # Build URL
        if raw.get("pdf_url"):
            url = f"{BASE_URL}{raw['pdf_url']}"
        elif raw.get("detail_url"):
            url = f"{BASE_URL}{raw['detail_url']}"
        else:
            url = f"{BASE_URL}/el_GR/web/portal/elib/{category}?catyear={year}"

        return {
            "_id": f"GR/HCMC/{category}/{doc_id}",
            "_source": "GR/HCMC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": f"{year}-01-01" if year else "",
            "url": url,
            "category": category,
            "year": year,
            "pdf_url": f"{BASE_URL}{raw['pdf_url']}" if raw.get("pdf_url") else None,
            "language": "el",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all documents (raw dicts), iterating by category and year."""
        for category in CATEGORIES:
            for year in YEARS:
                items = self._scrape_list_page(category, year)
                for item in items:
                    detail = self._fetch_detail(item)
                    if detail:
                        yield detail

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield recently added documents (current year + previous year)."""
        current_year = datetime.now().year
        for category in CATEGORIES:
            for year in [current_year, current_year - 1]:
                items = self._scrape_list_page(category, year)
                for item in items:
                    detail = self._fetch_detail(item)
                    if detail:
                        yield detail


def main():
    scraper = GreekHCMCScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
