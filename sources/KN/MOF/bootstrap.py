#!/usr/bin/env python3
"""
KN/MOF -- St Kitts & Nevis Ministry of Finance

Downloads financial legislation, tax exchange agreements, double taxation
conventions, budget documents, and fiscal reports from mof.gov.kn.
Full text extracted from PDFs via pdfplumber.

Strategy:
  - The MOF website is WordPress-based with legislation on 3 sub-pages
    and publications on a separate page.
  - We fetch page content from the WP REST API, parse HTML for PDF links.
  - Each PDF is downloaded and text is extracted.
  - Documents are classified as "legislation" or "doctrine" based on category.

Coverage:
  - Financial Legislation: Finance Administration Act, Procurement Act, CRS Act,
    Mutual Exchange of Information on Taxation Matters Act + amendments
  - Exchange of Information: 23 bilateral tax information exchange orders
  - Double Taxation Conventions: 11 bilateral DTC orders + enabling act
  - Budget Addresses: 2009-2026 (annual)
  - Budget Estimates: 2010-2026 (Volume 1 & 2)
  - Audit Reports: 2018-2024
  - Debt Management: strategies and quarterly bulletins

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import logging
import hashlib
import re
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KN.MOF")

BASE_URL = "https://www.mof.gov.kn"
WP_API = f"{BASE_URL}/wp-json/wp/v2/pages"

# WordPress page slugs to scrape, with their document type
PAGE_SLUGS = [
    ("financial-legislation", "legislation", "Financial Legislation"),
    ("exchange-of-information-legislation", "legislation", "Exchange of Information"),
    ("double-taxation-conventions-dtcs", "legislation", "Double Taxation Conventions"),
    ("publications", "doctrine", "Publications"),
]

# Patterns to skip (non-legislative forms, tenders, etc.)
SKIP_PATTERNS = [
    r"passenger.*bus.*form",
    r"business.*occupation.*licence.*form",
    r"first.*time.*home.*owner",
    r"tender|bid.*design.*build",
    r"savingram",
    r"eccblib.*calendar",
    r"map.*application",
    r"rfp.*digitized",
    r"baico.*distribution",
    r"digitized.*services",
]


def _make_doc_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _normalize_url(url: str) -> str:
    """Normalize URL: ensure https, fix mof.govt.kn -> mof.gov.kn, add domain if relative."""
    url = url.strip()
    # Fix legacy domain
    url = url.replace("mof.govt.kn", "mof.gov.kn")
    # Make relative URLs absolute
    if url.startswith("/wp-content/"):
        url = BASE_URL + url
    # Upgrade to HTTPS
    if url.startswith("http://"):
        url = "https://" + url[7:]
    return url


def _extract_year_from_title(title: str) -> str:
    m = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    return f"{m.group(1)}-01-01" if m else ""


def _should_skip(title: str) -> bool:
    title_lower = title.lower()
    return any(re.search(pat, title_lower) for pat in SKIP_PATTERNS)


def _classify_type(title: str, page_type: str) -> str:
    """Determine if a document is legislation or doctrine."""
    title_lower = title.lower()
    if page_type == "legislation":
        return "legislation"
    # Budget and audit documents are doctrine
    if any(kw in title_lower for kw in ["budget", "audit", "debt", "fiscal", "estimate"]):
        return "doctrine"
    if any(kw in title_lower for kw in ["act", "order", "regulation", "sro"]):
        return "legislation"
    return "doctrine"


class KNMOFScraper(BaseScraper):
    """Scraper for KN/MOF — St Kitts & Nevis Ministry of Finance."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = None

    def _get_session(self):
        if self.session is None:
            import requests
            self.session = requests.Session()
            self.session.headers.update({
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "application/pdf,application/json,*/*",
            })
        return self.session

    def _fetch_page_documents(self, slug: str, page_type: str,
                               label: str) -> List[Tuple[str, str, str]]:
        """Fetch document catalog from a WordPress page.

        Returns list of (url, title, doc_type) tuples.
        """
        sess = self._get_session()
        docs = []

        try:
            self.rate_limiter.wait()
            resp = sess.get(f"{WP_API}?slug={slug}&_fields=id,title,content",
                           timeout=30)
            resp.raise_for_status()
            pages = resp.json()
            if not pages:
                logger.warning("No page found for slug '%s'", slug)
                return docs
        except Exception as e:
            logger.warning("Failed to fetch page '%s': %s", slug, e)
            return docs

        page = pages[0]
        content = page.get("content", {}).get("rendered", "")

        # Extract all PDF links from the HTML content
        links = re.findall(
            r'<a[^>]*href="([^"]+\.pdf)"[^>]*>(.*?)</a>',
            content, re.DOTALL | re.IGNORECASE
        )

        seen_urls = set()
        for url, title in links:
            title = re.sub(r'<[^>]+>', '', title).strip()
            title = unescape(title)
            url = _normalize_url(url)

            if not title or url in seen_urls:
                continue
            if _should_skip(title):
                continue

            seen_urls.add(url)
            doc_type = _classify_type(title, page_type)
            docs.append((url, title, doc_type))

        logger.info("Fetched %d documents from '%s' (%s)", len(docs), label, slug)
        return docs

    def _build_full_catalog(self) -> List[Tuple[str, str, str]]:
        """Build the complete document catalog from all pages."""
        catalog = []
        seen = set()

        for slug, page_type, label in PAGE_SLUGS:
            entries = self._fetch_page_documents(slug, page_type, label)
            for url, title, doc_type in entries:
                if url not in seen:
                    seen.add(url)
                    catalog.append((url, title, doc_type))

        logger.info("Total catalog: %d unique documents", len(catalog))
        return catalog

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"KN/MOF/{raw['doc_id']}",
            "_source": "KN/MOF",
            "_type": raw.get("doc_type", "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["url"],
        }

    def _download_and_extract(self, url: str, doc_id: str,
                               doc_type: str) -> Optional[str]:
        """Download PDF and extract text."""
        self.rate_limiter.wait()
        sess = self._get_session()
        try:
            resp = sess.get(url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to download %s: %s", url, e)
            return None

        if len(resp.content) < 200:
            logger.warning("Skipping %s — too small (%d bytes)", url, len(resp.content))
            return None

        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type and not resp.content[:5] == b"%PDF-":
            logger.warning("Skipping %s — not a PDF (Content-Type: %s)", url, content_type)
            return None

        table = doc_type if doc_type in ("legislation", "doctrine") else "legislation"
        text = extract_pdf_markdown(
            source="KN/MOF",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table=table,
        ) or ""

        return text if len(text) >= 50 else None

    def fetch_all(self, sample=False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        catalog = self._build_full_catalog()

        for url, title, doc_type in catalog:
            if limit and count >= limit:
                break

            doc_id = _make_doc_id(url)
            logger.info("[%d/%d] %s", count + 1, len(catalog), title[:70])

            text = self._download_and_extract(url, doc_id, doc_type)
            if not text:
                continue

            date = _extract_year_from_title(title)

            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "doc_type": doc_type,
            }
            count += 1
            logger.info("  OK: %s (%d chars)", title[:50], len(text))

        logger.info("Total records yielded: %d / %d catalog entries", count, len(catalog))

    def fetch_updates(self, since=None):
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            sess = self._get_session()
            resp = sess.get(f"{WP_API}?slug=financial-legislation&_fields=id,title",
                           timeout=30)
            resp.raise_for_status()
            pages = resp.json()
            ok = len(pages) > 0
            logger.info("API test: financial-legislation page — %s",
                        "OK" if ok else "FAIL")

            # Test PDF download
            url = f"{BASE_URL}/wp-content/uploads/2016/02/Finance-Administration-Act-2007.pdf"
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            pdf_ok = len(resp.content) > 1000
            logger.info("PDF test: Finance Administration Act (%d bytes) — %s",
                        len(resp.content), "OK" if pdf_ok else "FAIL")
            return ok and pdf_ok
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = KNMOFScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
