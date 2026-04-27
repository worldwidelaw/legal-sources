#!/usr/bin/env python3
"""
BM/Legislation -- Bermuda Laws Online Data Fetcher

Fetches consolidated legislation from bermudalaws.bm — a SharePoint-based portal
maintained by Bermuda's Attorney General's Office.

Strategy:
  - Paginate through the consolidated law search index (40 items/page, ~1,542 total)
  - Extract document links from each search result page
  - Download PDFs and extract full text via common/pdf_extract
  - Also fetch annual laws for recent additions

Endpoints:
  - Search index: /search/*/SPContentType:"Consolidated Law"
  - Pagination: appends page number to URL (pages 1-39)
  - Documents: /Laws/Consolidated Law/{YEAR}/{title} (returns PDF)
  - RSS feeds: /consolidatedlaw.rss, /annuallaw.rss

Data:
  - ~1,542 consolidated laws
  - Language: English
  - Format: PDF

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, quote, unquote
import xml.etree.ElementTree as ET

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
logger = logging.getLogger("legal-data-hunter.BM.Legislation")

BASE_URL = "https://www.bermudalaws.bm"

# Search URL for consolidated laws
SEARCH_URL = "/search/*/SPContentType:%22Consolidated%20Law%22"
ANNUAL_SEARCH_URL = "/search/*/SPContentType:%22Annual%20Law%22"
ITEMS_PER_PAGE = 40

# Regex to match document links in search results
# Links are absolute: https://www.bermudalaws.bm/Laws/Consolidated Law/YYYY/Title
DOC_LINK_RE = re.compile(
    r'href="((?:https://www\.bermudalaws\.bm)?/Laws/(?:Consolidated(?:%20| )Law|Annual(?:%20| )Law)/[^"]+)"',
    re.IGNORECASE,
)

# Regex to extract year from path
YEAR_RE = re.compile(r'/Laws/(?:Consolidated%20Law|Consolidated Law|Annual%20Law|Annual Law)/(\d{4})/')

# Regex for pagination info (e.g., "1 - 40 of 1542 items")
TOTAL_RE = re.compile(r'of\s+(\d[\d,]+)\s+items', re.IGNORECASE)


class BMLegislationScraper(BaseScraper):
    """
    Scraper for BM/Legislation -- Bermuda Laws Online.
    Country: BM
    URL: https://www.bermudalaws.bm

    Data types: legislation
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en,en-US;q=0.9",
            },
            timeout=120,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="BM/Legislation",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

    def _get_doc_links_from_page(self, html: str) -> List[str]:
        """Extract document links from a search result page."""
        import html as html_mod
        links = set()
        for match in DOC_LINK_RE.findall(html):
            # Strip base URL to get relative path
            path = match.replace("https://www.bermudalaws.bm", "")
            # Decode HTML entities (e.g., &#x2019; -> ')
            path = html_mod.unescape(path)
            links.add(path)
        return list(links)

    def _get_total_items(self, html: str) -> Optional[int]:
        """Extract total item count from pagination text."""
        m = TOTAL_RE.search(html)
        if m:
            return int(m.group(1).replace(",", ""))
        return None

    def _collect_all_links(self, content_type: str = "Consolidated Law",
                           limit: Optional[int] = None) -> List[str]:
        """Collect document links using year-by-year search queries.

        The site uses Blazor/Telerik for client-side pagination, so we cannot
        paginate beyond the first 40 items. Instead, we search by individual
        year to get complete coverage (most years have ≤40 results).
        """
        all_links = []
        seen = set()
        encoded_type = quote(f'"{content_type}"')

        # First, get initial 40 from default search
        base_search = f"/search/*/SPContentType:{encoded_type}"
        logger.info(f"  Fetching default search page...")
        try:
            self.rate_limiter.wait()
            resp = self.client.get(base_search)
            resp.raise_for_status()
            for link in self._get_doc_links_from_page(resp.text):
                if link not in seen:
                    seen.add(link)
                    all_links.append(link)
        except Exception as e:
            logger.warning(f"  Failed to fetch default search: {e}")

        if limit and len(all_links) >= limit:
            return all_links[:limit]

        # Then search by year to get comprehensive coverage
        current_year = datetime.now().year
        for year in range(1890, current_year + 1):
            if limit and len(all_links) >= limit:
                break

            search_url = f'/search/"{year}"/SPContentType:{encoded_type}'
            try:
                self.rate_limiter.wait()
                resp = self.client.get(search_url)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"  Failed year {year}: {e}")
                continue

            links = self._get_doc_links_from_page(resp.text)
            new_count = 0
            for link in links:
                if link not in seen:
                    seen.add(link)
                    all_links.append(link)
                    new_count += 1

            if new_count > 0:
                total = self._get_total_items(resp.text)
                overflow = f" (overflow: {total} total)" if total and total > 40 else ""
                logger.info(f"  Year {year}: +{new_count} new{overflow} (total: {len(all_links)})")

        logger.info(f"  Collected {len(all_links)} unique document links")
        return all_links

    def _collect_links_from_rss(self, rss_path: str) -> List[Dict[str, Any]]:
        """Collect document links and metadata from RSS feed."""
        results = []
        try:
            self.rate_limiter.wait()
            resp = self.client.get(rss_path)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            for item in root.findall(".//item"):
                title = item.findtext("title", "").strip()
                link = item.findtext("link", "").strip()
                pub_date = item.findtext("pubDate", "").strip()
                if link:
                    # Convert absolute URL to relative path
                    path = link.replace(BASE_URL, "")
                    results.append({
                        "path": path,
                        "title": title,
                        "pub_date": pub_date,
                    })
            logger.info(f"  RSS {rss_path}: {len(results)} items")
        except Exception as e:
            logger.warning(f"  Failed to fetch RSS {rss_path}: {e}")
        return results

    def _download_and_extract(self, doc_path: str) -> Optional[Dict[str, Any]]:
        """Download a document (PDF) and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(doc_path)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Failed to download {doc_path}: {e}")
            return None

        content_type = resp.headers.get("Content-Type", "")

        if "pdf" in content_type.lower() or doc_path.lower().endswith(".pdf"):
            text = self._extract_pdf_text(resp.content)
            return {"text": text, "format": "pdf", "size": len(resp.content)}

        # The site serves PDFs even without .pdf extension
        # Check if content looks like a PDF
        if resp.content[:5] == b"%PDF-":
            text = self._extract_pdf_text(resp.content)
            return {"text": text, "format": "pdf", "size": len(resp.content)}

        # If HTML, try to extract text content
        if "html" in content_type.lower():
            # Sometimes the document page has an embedded PDF link
            # or the text is in the HTML itself
            from html.parser import HTMLParser
            import html

            # Try to find a PDF link in the HTML
            pdf_links = re.findall(r'href="([^"]+\.pdf)"', resp.text, re.IGNORECASE)
            if pdf_links:
                pdf_url = pdf_links[0]
                if not pdf_url.startswith("http"):
                    pdf_url = urljoin(BASE_URL + doc_path, pdf_url)
                    pdf_url = pdf_url.replace(BASE_URL, "")
                try:
                    self.rate_limiter.wait()
                    pdf_resp = self.client.get(pdf_url)
                    pdf_resp.raise_for_status()
                    if pdf_resp.content[:5] == b"%PDF-":
                        text = self._extract_pdf_text(pdf_resp.content)
                        return {"text": text, "format": "pdf", "size": len(pdf_resp.content)}
                except Exception as e:
                    logger.warning(f"  Failed to download linked PDF: {e}")

            # Extract text from HTML as fallback
            text = re.sub(r'<[^>]+>', ' ', resp.text)
            text = re.sub(r'\s+', ' ', text).strip()
            if len(text) > 200:
                return {"text": text, "format": "html", "size": len(resp.content)}

        logger.warning(f"  Unknown content type for {doc_path}: {content_type}")
        return None

    def _extract_title_from_path(self, path: str) -> str:
        """Extract a human-readable title from the document URL path."""
        # /Laws/Consolidated%20Law/1999/Abolition%20of%20Capital%20and%20Corporal%20Punishment%20Act%201999
        parts = unquote(path).split("/")
        if parts:
            title = parts[-1]
            # Remove .pdf extension if present
            title = re.sub(r'\.pdf$', '', title, flags=re.IGNORECASE)
            return title
        return path

    def _extract_year_from_path(self, path: str) -> Optional[str]:
        """Extract year from document path."""
        m = YEAR_RE.search(path)
        if m:
            return m.group(1)
        # Try to extract year from title
        m = re.search(r'\b(1[89]\d{2}|20\d{2})\b', unquote(path))
        if m:
            return m.group(1)
        return None

    def _make_doc_id(self, path: str) -> str:
        """Create a stable document ID from the URL path."""
        return hashlib.sha256(path.encode()).hexdigest()[:16]

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw document data into standard schema."""
        now = datetime.now(timezone.utc).isoformat()
        doc_path = raw.get("path", "")
        title = raw.get("title") or self._extract_title_from_path(doc_path)
        year = raw.get("year") or self._extract_year_from_path(doc_path)

        date_str = None
        if year:
            date_str = f"{year}-01-01"
        if raw.get("pub_date"):
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(raw["pub_date"])
                date_str = dt.strftime("%Y-%m-%d")
            except Exception:
                pass

        doc_id = self._make_doc_id(doc_path)
        full_url = f"{BASE_URL}{doc_path}" if not doc_path.startswith("http") else doc_path

        return {
            "_id": f"BM/Legislation/{doc_id}",
            "_source": "BM/Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": raw.get("text", ""),
            "date": date_str,
            "url": full_url,
            "doc_id": doc_id,
            "year": year,
            "file_url": full_url,
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all consolidated legislation from Bermuda Laws Online."""
        limit = 15 if sample else None

        # Step 1: Collect document links from search index
        logger.info("Collecting consolidated law links from search index...")
        links = self._collect_all_links("Consolidated Law", limit=limit)

        if not links and sample:
            # Fallback: try RSS feeds
            logger.info("Search index returned no links, trying RSS feeds...")
            rss_items = self._collect_links_from_rss("/consolidatedlaw.rss")
            rss_items += self._collect_links_from_rss("/annuallaw.rss")
            for item in rss_items[:15]:
                path = item["path"]
                logger.info(f"  Downloading: {unquote(path)}")
                result = self._download_and_extract(path)
                if result and result.get("text"):
                    yield {
                        "path": path,
                        "title": item.get("title"),
                        "pub_date": item.get("pub_date"),
                        "text": result["text"],
                        "year": self._extract_year_from_path(path),
                    }
            return

        # Step 2: Download and extract each document
        count = 0
        for doc_path in links:
            if limit and count >= limit:
                break

            title = self._extract_title_from_path(doc_path)
            logger.info(f"  [{count + 1}/{len(links)}] Downloading: {title}")

            result = self._download_and_extract(doc_path)
            if not result:
                logger.warning(f"  Skipping {title}: download failed")
                continue

            if not result.get("text") or len(result["text"].strip()) < 50:
                logger.warning(f"  Skipping {title}: no meaningful text extracted")
                continue

            yield {
                "path": doc_path,
                "title": title,
                "text": result["text"],
                "year": self._extract_year_from_path(doc_path),
            }
            count += 1

        logger.info(f"Fetched {count} documents total")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recently added/updated legislation via RSS feeds."""
        logger.info(f"Fetching updates since {since} via RSS feeds...")

        for rss_path in ["/consolidatedlaw.rss", "/annuallaw.rss"]:
            items = self._collect_links_from_rss(rss_path)
            for item in items:
                path = item["path"]
                logger.info(f"  Downloading: {item.get('title', path)}")

                result = self._download_and_extract(path)
                if not result or not result.get("text"):
                    continue

                yield {
                    "path": path,
                    "title": item.get("title"),
                    "pub_date": item.get("pub_date"),
                    "text": result["text"],
                    "year": self._extract_year_from_path(path),
                }


if __name__ == "__main__":
    scraper = BMLegislationScraper()

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
