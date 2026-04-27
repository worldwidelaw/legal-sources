#!/usr/bin/env python3
"""
INTL/AU-Commission -- African Union Legal Instruments

Fetches AU treaties, protocols, conventions, and charters with full text
extracted from PDFs.

Strategy:
  - Scrape listing page at /en/treaties (all 79 on one page)
  - Extract treaty slugs and visit each detail page
  - Parse dates, categories, and PDF URLs from detail pages
  - Download English treaty text PDFs and extract full text via pdfplumber

Data:
  - ~79 legal instruments
  - PDFs in English, French, Arabic, Portuguese
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch all (same as bootstrap)
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.AU-Commission")

BASE_URL = "https://au.int"
LISTING_URL = BASE_URL + "/en/treaties"


class AUCommissionScraper(BaseScraper):
    """Scraper for INTL/AU-Commission -- African Union Legal Instruments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })

    def _fetch_treaty_slugs(self) -> List[str]:
        """Fetch the main listing page and extract treaty detail slugs."""
        self.rate_limiter.wait()
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        slugs = []
        seen = set()

        for link in soup.select('a[href*="/en/treaties/"]'):
            href = link.get("href", "")
            # Skip category links (numeric paths like /en/treaties/1158)
            match = re.search(r"/en/treaties/([a-z][\w-]+)", href)
            if match:
                slug = match.group(1)
                if slug not in seen:
                    seen.add(slug)
                    slugs.append(slug)

        logger.info("Found %d treaty slugs", len(slugs))
        return slugs

    def _fetch_detail_page(self, slug: str) -> Optional[Dict]:
        """Fetch a treaty detail page and extract metadata + PDF URLs."""
        self.rate_limiter.wait()
        url = f"{BASE_URL}/en/treaties/{slug}"
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                logger.warning("Detail page 404: %s", slug)
                return None
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to fetch detail page %s: %s", slug, e)
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Title
        title_el = soup.select_one("h1.page-header, h1#page-title, h1")
        title = title_el.get_text(strip=True) if title_el else slug.replace("-", " ").title()

        # Dates
        adoption_date = self._extract_field_date(soup, "field-date-adoption")
        signature_date = self._extract_field_date(soup, "field-date-signature")
        intoforce_date = self._extract_field_date(soup, "field-date-intoforce")

        # Category
        category = ""
        cat_el = soup.select_one(".field-name-field-tags-treaties .field-item")
        if cat_el:
            category = cat_el.get_text(strip=True)

        # Treaty text PDF URLs (from field-file section)
        pdf_urls = []
        file_section = soup.select_one(".field-name-field-file")
        if file_section:
            for a in file_section.select("a[href$='.pdf']"):
                href = a.get("href", "")
                if href:
                    if href.startswith("/"):
                        href = BASE_URL + href
                    pdf_urls.append(href)

        # If no PDFs in field-file, look for any treaty PDF links
        if not pdf_urls:
            for a in soup.select("a[href*='treaties'][href$='.pdf']"):
                href = a.get("href", "")
                if href and "treaty" in href.lower():
                    if href.startswith("/"):
                        href = BASE_URL + href
                    pdf_urls.append(href)

        # Prefer English PDF
        english_pdf = None
        for u in pdf_urls:
            if "_E.pdf" in u or "_e.pdf" in u or "english" in u.lower():
                english_pdf = u
                break
        if not english_pdf and pdf_urls:
            english_pdf = pdf_urls[0]

        return {
            "slug": slug,
            "title": title,
            "adoption_date": adoption_date,
            "signature_date": signature_date,
            "intoforce_date": intoforce_date,
            "category": category,
            "pdf_url": english_pdf,
            "all_pdf_urls": pdf_urls,
            "detail_url": url,
        }

    def _extract_field_date(self, soup: BeautifulSoup, field_class: str) -> Optional[str]:
        """Extract a date from a Drupal field."""
        field = soup.select_one(f".field-name-{field_class} .field-item, .field-name-{field_class} .date-display-single")
        if not field:
            return None
        text = field.get_text(strip=True)
        # Try various date formats
        for fmt in ("%d %B %Y", "%B %d, %Y", "%d/%m/%Y", "%Y-%m-%d", "%d %b %Y"):
            try:
                dt = datetime.strptime(text, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="INTL/AU-Commission",
            source_id="",
            pdf_url=pdf_url,
            table="legislation",
        ) or ""

    def _make_id(self, slug: str) -> str:
        """Create unique ID from treaty slug."""
        return f"AU-{slug}"

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw AU record into standard schema."""
        # Use adoption date as primary, fall back to signature date
        date = raw.get("adoption_date") or raw.get("signature_date")

        return {
            "_id": raw["_id"],
            "_source": "INTL/AU-Commission",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": date,
            "url": raw["detail_url"],
            "category": raw.get("category", ""),
            "adoption_date": raw.get("adoption_date"),
            "signature_date": raw.get("signature_date"),
            "entry_into_force": raw.get("intoforce_date"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all AU treaties."""
        slugs = self._fetch_treaty_slugs()
        for i, slug in enumerate(slugs):
            logger.info("[%d/%d] Fetching: %s", i + 1, len(slugs), slug)
            doc = self._process_slug(slug)
            if doc:
                yield doc

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all (small dataset, no incremental endpoint)."""
        yield from self.fetch_all()

    def _process_slug(self, slug: str) -> Optional[dict]:
        """Process a single treaty: fetch detail, download PDF, extract text."""
        detail = self._fetch_detail_page(slug)
        if not detail:
            return None

        if not detail.get("pdf_url"):
            logger.warning("No PDF URL for: %s", detail["title"][:80])
            return None

        text = self._download_pdf_text(detail["pdf_url"])
        if not text:
            logger.warning("No text for: %s", detail["title"][:80])
            return None

        raw = {
            "_id": self._make_id(slug),
            "title": detail["title"],
            "text": text,
            "adoption_date": detail.get("adoption_date"),
            "signature_date": detail.get("signature_date"),
            "intoforce_date": detail.get("intoforce_date"),
            "category": detail.get("category", ""),
            "detail_url": detail["detail_url"],
        }

        return self.normalize(raw)

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            slugs = self._fetch_treaty_slugs()
            logger.info("Connection OK: %d treaties found", len(slugs))
            return len(slugs) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info("Running in SAMPLE mode (15 records)")
            slugs = self._fetch_treaty_slugs()
            count = 0
            target = 15

            for slug in slugs:
                if count >= target:
                    break

                doc = self._process_slug(slug)
                if doc:
                    fname = re.sub(r'[^\w\-.]', '_', f"{doc['_id'][:80]}.json")
                    with open(sample_dir / fname, "w", encoding="utf-8") as f:
                        json.dump(doc, f, ensure_ascii=False, indent=2)
                    count += 1
                    logger.info("[%d/%d] %s (%d chars)",
                                count, target, doc["title"][:60], len(doc["text"]))

            logger.info("Sample bootstrap complete: %d records saved", count)
            return count
        else:
            count = 0
            for doc in self.fetch_all():
                self.storage.save(doc)
                count += 1
                if count % 10 == 0:
                    logger.info("Progress: %d records saved", count)
            logger.info("Full bootstrap complete: %d records saved", count)
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/AU-Commission Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = AUCommissionScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for doc in scraper.fetch_updates():
            scraper.storage.save(doc)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
