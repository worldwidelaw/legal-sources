#!/usr/bin/env python3
"""
INTL/ECTEL -- Eastern Caribbean Telecommunications Authority

Downloads telecommunications legislation, regulations, and statutory rules
from the ECTEL website. Full text extracted from PDFs via pdfplumber.

Strategy:
  - ECTEL hosts per-country legislation pages via WordPress with ACF fields.
  - Each country page contains: a Telecommunications Act PDF + a data table
    of ~20 subsidiary regulations (SROs) with PDF links.
  - We fetch page data from the WP REST API, parse ACF sections to build
    a document catalog, then download and extract text from each PDF.
  - Also includes the ECTEL Treaty and Electronic Communications Bill.

Coverage:
  - Dominica (DM): Telecommunications Act 2000 + 21 regulations
  - Grenada (GD): Telecommunications Act 2000 + 22 regulations
  - Saint Lucia (LC): Telecommunications Act Ch. 8.11 + 20 regulations
  - St Kitts & Nevis (KN): Telecommunications Act Ch. 16.05 + 19 regulations
  - St Vincent & Grenadines (VC): Telecommunications Act 2000 + 18 regulations
  - ECTEL Treaty + Protocol Amendment
  - Electronic Communications Bill 2020

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import logging
import hashlib
import json
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List, Tuple
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ECTEL")

BASE_URL = "https://www.ectel.int"
WP_API = f"{BASE_URL}/wp-json/wp/v2/pages"

# WordPress page IDs for each ECTEL member state's legislation page
COUNTRY_PAGES = {
    79: ("DM", "Dominica"),
    494: ("GD", "Grenada"),
    288: ("LC", "Saint Lucia"),
    438: ("KN", "St Kitts & Nevis"),
    434: ("VC", "St Vincent & the Grenadines"),
}

# Additional standalone documents not on country pages
STANDALONE_DOCS = [
    (f"{BASE_URL}/wp-content/uploads/2023/11/ECTEL-Treaty-1.pdf",
     "ECTEL Treaty (2000)", "INTL", "Treaty"),
    (f"{BASE_URL}/wp-content/uploads/2023/11/Protocol-Amending-ECTEL-Treaty-and-Addendum-1.pdf",
     "Protocol Amending ECTEL Treaty and Addendum", "INTL", "Treaty"),
    (f"{BASE_URL}/wp-content/uploads/2020/09/Electronic-Communications-Bill-200703-1.pdf",
     "Electronic Communications Bill 2020", "INTL", "Bill"),
]


def _make_doc_id(url: str) -> str:
    """Create a stable, short document ID from URL."""
    return hashlib.md5(url.encode()).hexdigest()[:12]


def _extract_year_from_title(title: str) -> str:
    """Try to extract a year from the title for the date field."""
    m = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    return f"{m.group(1)}-01-01" if m else ""


class ECTELScraper(BaseScraper):
    """Scraper for INTL/ECTEL — ECTEL telecommunications legislation."""

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

    def _fetch_country_catalog(self, page_id: int, country_code: str,
                                country_name: str) -> List[Tuple[str, str, str, str]]:
        """Fetch legislation catalog from a country's WordPress page via API.

        Returns list of (url, title, country_code, category) tuples.
        """
        sess = self._get_session()
        catalog = []

        try:
            self.rate_limiter.wait()
            resp = sess.get(f"{WP_API}/{page_id}", timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning("Failed to fetch page %d (%s): %s", page_id, country_name, e)
            return catalog

        acf = data.get("acf", {})
        sections = acf.get("sections", [])

        for section in sections:
            for block in section.get("blocks", []):
                layout = block.get("acf_fc_layout", "")

                # Content blocks contain the main Act links
                if layout == "content_block":
                    content = block.get("content_field", "")
                    links = re.findall(
                        r'<a[^>]*href="([^"]+\.pdf)"[^>]*>(.*?)</a>',
                        content, re.DOTALL
                    )
                    for url, title in links:
                        title = re.sub(r'<[^>]+>', '', title).strip()
                        title = unescape(title)
                        if title and url:
                            full_title = f"{country_name} — {title}"
                            catalog.append((url, full_title, country_code, "Act"))

                # Data table blocks contain the regulation rows
                if layout == "data_table_block":
                    table_data = block.get("data", {})
                    for row in table_data.get("body", []):
                        title = ""
                        sro = ""
                        url = ""
                        for cell in row.get("item", []):
                            term = cell.get("term", "")
                            if term == "title":
                                title = cell.get("value", "")
                            elif term == "sr_o_":
                                sro = cell.get("value", "")
                            elif term == "_action_":
                                url = cell.get("value", "")

                        if url and url.lower().endswith(".pdf") and title:
                            sro_part = f" ({sro})" if sro and "not yet" not in sro.lower() else ""
                            full_title = f"{country_name} — {title}{sro_part}"
                            catalog.append((url, full_title, country_code, "Regulation"))

        logger.info("Fetched %d documents from %s page", len(catalog), country_name)
        return catalog

    def _build_full_catalog(self) -> List[Tuple[str, str, str, str]]:
        """Build the complete document catalog from all country pages + standalone docs."""
        catalog = []

        for page_id, (code, name) in COUNTRY_PAGES.items():
            entries = self._fetch_country_catalog(page_id, code, name)
            catalog.extend(entries)

        catalog.extend(STANDALONE_DOCS)
        logger.info("Total catalog: %d documents", len(catalog))
        return catalog

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"INTL/ECTEL/{raw['doc_id']}",
            "_source": "INTL/ECTEL",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["url"],
            "category": raw.get("category", ""),
            "country_code": raw.get("country_code", ""),
        }

    def _download_and_extract(self, url: str, doc_id: str) -> Optional[str]:
        """Download PDF from ECTEL and extract text."""
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

        text = extract_pdf_markdown(
            source="INTL/ECTEL",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
        ) or ""

        return text if len(text) >= 50 else None

    def fetch_all(self, sample=False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        catalog = self._build_full_catalog()

        for url, title, country_code, category in catalog:
            if limit and count >= limit:
                break

            doc_id = _make_doc_id(url)
            logger.info("[%d/%d] %s", count + 1, len(catalog), title[:70])

            text = self._download_and_extract(url, doc_id)
            if not text:
                continue

            date = _extract_year_from_title(title)

            yield {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "date": date,
                "url": url,
                "category": category,
                "country_code": country_code,
            }
            count += 1
            logger.info("  OK: %s (%d chars)", title[:50], len(text))

        logger.info("Total records yielded: %d / %d catalog entries", count, len(catalog))

    def fetch_updates(self, since=None):
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        """Quick connectivity test — fetch API + download one PDF."""
        try:
            sess = self._get_session()
            # Test API
            resp = sess.get(f"{WP_API}/79", timeout=30)
            resp.raise_for_status()
            data = resp.json()
            has_acf = "acf" in data and "sections" in data.get("acf", {})
            logger.info("API test: page 79 (%s) — %s",
                        data.get("title", {}).get("rendered", "?"),
                        "OK" if has_acf else "FAIL (no ACF)")

            # Test PDF download
            url = f"{BASE_URL}/wp-content/uploads/2015/12/DOM-act-8-2000.pdf"
            resp = sess.get(url, timeout=30)
            resp.raise_for_status()
            ok = len(resp.content) > 1000
            logger.info("PDF test: DOM Act 2000 (%d bytes) — %s",
                        len(resp.content), "OK" if ok else "FAIL")
            return has_acf and ok
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ECTELScraper()

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
