#!/usr/bin/env python3
"""
RS/NBS -- Serbia National Bank Regulations

Fetches all regulations from the National Bank of Serbia (NBS) website.
PDF documents across 20+ regulatory categories.

Strategy:
  1. Scrape category index pages for PDF links
  2. Download each PDF
  3. Extract text using pdfplumber
  4. Normalize into standard schema

Data source:
  - Index: https://www.nbs.rs/en/drugi-nivo-navigacije/propisi/
  - PDFs: https://www.nbs.rs/documents-eng/propisi/...

License: Public domain (government publications)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import logging
import io
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RS.NBS")

BASE_URL = "https://www.nbs.rs"

# All regulation category pages (English versions)
CATEGORIES = {
    "monetary_policy": "/en/drugi-nivo-navigacije/propisi/propisi-mp/",
    "financial_stability": "/en/drugi-nivo-navigacije/propisi/propisi-fs/",
    "bank_resolution": "/en/drugi-nivo-navigacije/propisi/propisi-rst/",
    "international_operations": "/en/drugi-nivo-navigacije/propisi/propisi-ino/",
    "foreign_exchange": "/en/drugi-nivo-navigacije/propisi/propisi-dp/",
    "public_debt": "/en/drugi-nivo-navigacije/propisi/propisi-sjd/",
    "payment_systems": "/en/drugi-nivo-navigacije/propisi/propisi-ps/",
    "payment_institutions": "/en/drugi-nivo-navigacije/propisi/propisi-piien/",
    "bank_supervision": "/en/drugi-nivo-navigacije/propisi/propisi-kpb/",
    "insurance": "/en/drugi-nivo-navigacije/propisi/propisi-osig/",
    "pension_funds": "/en/drugi-nivo-navigacije/propisi/propisi-pf/",
    "financial_leasing": "/en/drugi-nivo-navigacije/propisi/propisi-fl/",
    "info_systems": "/en/drugi-nivo-navigacije/propisi/propisi-sis/",
    "consumer_protection": "/en/drugi-nivo-navigacije/propisi/propisi-zk/",
    "cash_operations": "/en/drugi-nivo-navigacije/propisi/propisi_trz/",
    "accounting": "/en/drugi-nivo-navigacije/propisi/propisi-rac/",
    "enforced_collection": "/en/drugi-nivo-navigacije/propisi/propisi-pn/",
    "statistics": "/en/drugi-nivo-navigacije/propisi/propisi-stat/",
    "aml": "/en/drugi-nivo-navigacije/propisi/propisi-spn/",
    "digital_assets": "/en/drugi-nivo-navigacije/propisi/propisi-di/",
    "laws": "/en/drugi-nivo-navigacije/propisi/zakoni/",
}


class NBSScraper(BaseScraper):
    """
    Scraper for RS/NBS -- Serbia National Bank Regulations.
    Country: RS
    URL: https://www.nbs.rs/en/drugi-nivo-navigacije/propisi/

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,sr;q=0.8",
        })

    def _scrape_category(self, category_name: str, category_path: str) -> List[Dict[str, str]]:
        """
        Scrape a regulation category page for PDF links.
        Returns list of dicts with title, pdf_url, category.
        """
        url = BASE_URL + category_path

        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"Category {category_name} not found (404)")
            else:
                logger.error(f"HTTP error for {category_name}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error fetching category {category_name}: {e}")
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
        results = []
        seen_urls = set()

        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            if not href.endswith('.pdf'):
                continue

            # Build full URL
            if href.startswith('/'):
                pdf_url = BASE_URL + href
            elif href.startswith('http'):
                pdf_url = href
            else:
                pdf_url = urljoin(url, href)

            # Deduplicate within category
            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            title = a_tag.get_text(strip=True)
            if not title:
                title = href.split('/')[-1].replace('.pdf', '').replace('_', ' ').title()

            results.append({
                "title": title,
                "pdf_url": pdf_url,
                "category": category_name,
                "category_path": category_path,
            })

        logger.info(f"  {category_name}: {len(results)} PDFs found")
        return results

    def _extract_pdf_text(self, pdf_content: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        if not HAS_PDFPLUMBER:
            logger.error("pdfplumber not available — cannot extract PDF text")
            return ""

        try:
            pdf = pdfplumber.open(io.BytesIO(pdf_content))
            text_parts = []
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
                # Release per-page cached objects/textmap before moving on.
                # Without this, pdfplumber holds every visited page's parsed
                # layout in memory and a single large regulation PDF can grow to
                # multiple GB, OOM-killing the VPS (exit 137, issue #934).
                try:
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            return '\n\n'.join(text_parts)
        except Exception as e:
            logger.error(f"PDF extraction error: {e}")
            return ""

    def _download_and_extract(self, pdf_url: str) -> str:
        """Download a PDF and extract its text content."""
        try:
            self.rate_limiter.wait()
            resp = self.session.get(pdf_url, timeout=60, allow_redirects=True)
            resp.raise_for_status()

            if resp.headers.get('Content-Type', '').startswith('text/html'):
                logger.warning(f"Expected PDF but got HTML for {pdf_url}")
                return ""

            text = self._extract_pdf_text(resp.content)
            return text.strip()

        except Exception as e:
            logger.error(f"Error downloading {pdf_url}: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all regulation records from all categories."""
        total = 0

        for cat_name, cat_path in CATEGORIES.items():
            logger.info(f"Scraping category: {cat_name}")
            records = self._scrape_category(cat_name, cat_path)

            for record in records:
                total += 1
                yield record

        logger.info(f"Total regulation PDFs found: {total}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all records (no date filtering available on index pages)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw record into standard schema.
        Downloads PDF and extracts full text.
        """
        pdf_url = raw.get('pdf_url', '')
        title = raw.get('title', '')
        category = raw.get('category', '')

        # Download and extract text
        full_text = self._download_and_extract(pdf_url)
        if full_text:
            logger.info(f"  Extracted {len(full_text)} chars from {pdf_url.split('/')[-1]}")
        else:
            logger.warning(f"  No text extracted from {pdf_url.split('/')[-1]}")

        # Generate stable ID from PDF URL
        url_hash = hashlib.md5(pdf_url.encode()).hexdigest()[:12]
        filename = pdf_url.split('/')[-1].replace('.pdf', '')
        doc_id = f"{category}_{filename}_{url_hash}"

        return {
            "_id": doc_id,
            "_source": "RS/NBS",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": None,
            "url": pdf_url,
            "pdf_url": pdf_url,
            "category": category,
            "issuing_body": "National Bank of Serbia",
            "language": "en",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing RS/NBS endpoints...")

        print("\n1. Testing category page...")
        try:
            records = self._scrape_category("monetary_policy", CATEGORIES["monetary_policy"])
            print(f"   Found {len(records)} PDFs in monetary_policy")

            if records:
                sample = records[0]
                print(f"   Sample: {sample['title'][:60]}")
                print(f"   URL: {sample['pdf_url']}")

                print("\n2. Testing PDF download & extraction...")
                text = self._download_and_extract(sample['pdf_url'])
                print(f"   Text length: {len(text)} characters")
                if text:
                    print(f"   Preview: {text[:300]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    if not HAS_PDFPLUMBER:
        print("ERROR: pdfplumber is required. Install with: pip install pdfplumber")
        sys.exit(1)

    scraper = NBSScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, "
                  f"{stats['records_skipped']} skipped")
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
