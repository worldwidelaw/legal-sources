#!/usr/bin/env python3
"""
NG/NERC -- Nigerian Electricity Regulatory Commission Orders & Regulations

Scrapes the NERC WordPress document library across multiple categories
(orders, regulations, codes, guidelines, market documents). Downloads PDFs
and extracts full text via pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch ~12 sample records
  python bootstrap.py test               # Connectivity test
"""

import sys
import io
import re
import html as html_lib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.NERC")

BASE_URL = "https://nerc.gov.ng"
CATEGORIES = [
    "orders",
    "regulations",
    "codes",
    "guidelines-standards",
    "market-documents",
]
DELAY = 2.0
MAX_PAGES_PER_CATEGORY = 60


def _slug_from_url(url: str) -> str:
    fname = url.rstrip("/").rsplit("/", 1)[-1]
    fname = re.sub(r"\.(pdf|zip)$", "", fname, flags=re.I)
    fname = re.sub(r"[^a-zA-Z0-9_-]", "_", fname)
    return fname[:100]


def _parse_date(date_str: str) -> Optional[str]:
    date_str = date_str.strip()
    for fmt in ["%B %d, %Y", "%b %d, %Y", "%d %B %Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _doc_type(category: str) -> str:
    if category in ("regulations", "codes"):
        return "legislation"
    return "doctrine"


class NERCScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,*/*",
        })

    def _get_category_page(self, category: str, page: int = 1) -> str:
        if page == 1:
            url = f"{BASE_URL}/resource-category/{category}/"
        else:
            url = f"{BASE_URL}/resource-category/{category}/page/{page}/"
        time.sleep(DELAY)
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return ""
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return ""

    def _parse_entries(self, html: str, category: str) -> list[dict]:
        entries = []
        seen_urls = set()

        # Split on publication boundaries; each block is one document entry
        blocks = re.split(r'<div\s+class="publication">', html)
        for block in blocks[1:]:

            title_m = re.search(r'<h6[^>]*class="title"[^>]*>(.*?)</h6>', block, re.DOTALL)
            title = re.sub(r'<[^>]+>', '', title_m.group(1)).strip() if title_m else ""
            title = html_lib.unescape(title)

            date_m = re.search(r'title="([^"]+\d{4})"', block)
            date_str = date_m.group(1) if date_m else None
            date = _parse_date(date_str) if date_str else None

            pdf_m = re.search(
                r'href="(https://nerc\.gov\.ng/wp-content/uploads/[^"]+\.pdf)"',
                block,
            )
            if not pdf_m:
                continue
            pdf_url = pdf_m.group(1)

            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            if not title:
                title = _slug_from_url(pdf_url).replace("_", " ").replace("-", " ")

            entries.append({
                "title": title,
                "date": date,
                "url": pdf_url,
                "category": category,
                "slug": _slug_from_url(pdf_url),
            })

        return entries

    def _get_all_entries(self, categories: list[str] = None, max_per_cat: int = MAX_PAGES_PER_CATEGORY) -> list[dict]:
        if categories is None:
            categories = CATEGORIES

        all_entries = []
        for cat in categories:
            page = 1
            while page <= max_per_cat:
                html = self._get_category_page(cat, page)
                if not html:
                    break
                entries = self._parse_entries(html, cat)
                if not entries:
                    break
                all_entries.extend(entries)
                logger.info(f"Category {cat} page {page}: {len(entries)} documents")
                page += 1

        logger.info(f"Total entries found: {len(all_entries)}")
        return all_entries

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        try:
            time.sleep(DELAY)
            resp = self.session.get(url, timeout=120)
            resp.raise_for_status()

            if len(resp.content) > 100_000_000:
                logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
                return None

            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    pages.append(t)
                # Release per-page cache to avoid pdfplumber OOM (exit 137, #1001)
                try:
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return "\n\n".join(pages) if pages else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._get_all_entries()
        for entry in entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        for entry in self.fetch_all():
            yield entry

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw.get("url", "")
        title = raw.get("title", "")
        slug = raw.get("slug", "")
        date = raw.get("date")
        category = raw.get("category", "orders")

        if not url:
            return None

        text = self._extract_pdf_text(url)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for {slug}: {len(text) if text else 0} chars")
            return None

        doc_id = f"NG-NERC-{slug}"

        return {
            "_id": doc_id,
            "_source": "NG/NERC",
            "_type": _doc_type(category),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "category": category,
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = NERCScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_all_entries(categories=["regulations"], max_per_cat=1)
        if not entries:
            print("FAILED: no documents found")
            sys.exit(1)
        print(f"OK: found {len(entries)} documents on regulations page 1")
        for e in entries[:5]:
            print(f"  {e['title'][:60]} ({e.get('date', 'no date')})")
    elif command in ("bootstrap", "update"):
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
