#!/usr/bin/env python3
"""
MW/RBM -- Reserve Bank of Malawi Financial Sector Regulations

Fetches financial sector regulations from the RBM website. Documents are
listed on HTML pages by category, each linking to a PDF via GetContentFile.
Full text is extracted from PDFs via pdfplumber.

Categories:
  - Banking Directives & Guidelines
  - Capital Markets & Microfinance
  - Pensions & Insurance
  - Consumer Protection

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MW.RBM")

BASE_URL = "https://www.rbm.mw"
DELAY = 2.0

# (url, category_label) — pages to scrape for document listings
LISTING_PAGES = [
    (f"{BASE_URL}/FinancialSectorRegulation/BankRegulation/", "Banking"),
    (f"{BASE_URL}/FinancialSectorRegulation/BankRegulation/?activeTab=Guidelines", "Banking"),
    (f"{BASE_URL}/Supervision/MicrofinanceandCapitalMarkets/?activeTab=MCSUCapitalMarkets", "Capital Markets & Microfinance"),
    (f"{BASE_URL}/FinancialSectorRegulation/PensionAndInsurance/", "Pensions & Insurance"),
    (f"{BASE_URL}/FinancialSectorRegulation/Consumer/", "Consumer Protection"),
]


def _make_id(content_id: str, name: str) -> str:
    """Generate a stable ID from the ContentID and document name."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    if len(slug) > 60:
        slug = slug[:60]
    return f"MW_RBM_{content_id}_{slug}"


def _parse_date(date_str: str) -> Optional[str]:
    """Parse dates like 'Apr 15, 2025' or 'Jan 24, 2019' to ISO format."""
    if not date_str:
        return None
    date_str = date_str.strip()
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    m = re.search(r"(\d{4})", date_str)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def _classify_type(title: str) -> str:
    """Classify document type from title."""
    t = title.lower()
    if "directive" in t:
        return "directive"
    if "regulation" in t:
        return "regulation"
    if "guideline" in t:
        return "guideline"
    if "circular" in t:
        return "circular"
    if "act" in t:
        return "law"
    return "regulation"


class RBMScraper(BaseScraper):
    """Scraper for Reserve Bank of Malawi financial regulations."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    def _scrape_listing_page(self, url: str, category: str) -> List[Dict[str, Any]]:
        """Scrape a listing page for document entries."""
        logger.info("Fetching listing: %s (%s)", url, category)
        try:
            resp = self.http.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return []
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            return []

        html = resp.text
        docs = []

        # Pattern: table rows with ContentID links
        # Match: <a ... ContentID=XXXXX ...>TITLE</a> and nearby date text
        for m in re.finditer(
            r'<tr[^>]*>.*?ContentID=(\d+)[^>]*>\s*([^<]+)\s*</a>.*?</tr>',
            html,
            re.DOTALL | re.IGNORECASE,
        ):
            content_id = m.group(1)
            title = m.group(2).strip()
            if not title or len(title) < 5:
                continue

            # Try to extract date from the same row
            date_match = re.search(
                r'(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+\d{4}',
                m.group(0),
            )
            date_str = date_match.group(0) if date_match else None

            docs.append({
                "content_id": content_id,
                "title": title,
                "date": date_str,
                "category": category,
            })

        # Fallback: simpler pattern if table-based extraction fails
        if not docs:
            for m in re.finditer(r'ContentID=(\d+)[^>]*>\s*([^<]+)<', html):
                content_id = m.group(1)
                title = m.group(2).strip()
                if title and len(title) > 5:
                    docs.append({
                        "content_id": content_id,
                        "title": title,
                        "date": None,
                        "category": category,
                    })

        logger.info("  Found %d documents in %s", len(docs), category)
        return docs

    def _fetch_all_listings(self) -> List[Dict[str, Any]]:
        """Fetch document listings from all category pages."""
        all_docs = []
        seen_ids = set()
        for url, category in LISTING_PAGES:
            docs = self._scrape_listing_page(url, category)
            for doc in docs:
                cid = doc["content_id"]
                if cid in seen_ids:
                    continue
                seen_ids.add(cid)
                all_docs.append(doc)
            time.sleep(1.0)
        logger.info("Total unique documents: %d", len(all_docs))
        return all_docs

    def _download_and_extract(self, content_id: str, doc_id: str) -> Optional[str]:
        """Download a PDF from GetContentFile and extract text."""
        pdf_url = f"{BASE_URL}/FinancialSectorRegulation/GetContentFile/?ContentID={content_id}"
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading ContentID=%s", resp.status_code, content_id)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 200:
                logger.warning("File too small (%d bytes): ContentID=%s", len(pdf_bytes), content_id)
                return None
            # Check if it's actually a PDF
            if not pdf_bytes[:5].startswith(b'%PDF'):
                logger.warning("Not a PDF (ContentID=%s), skipping", content_id)
                return None
            text = extract_pdf_markdown("MW/RBM", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract ContentID=%s: %s", content_id, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all RBM documents with full text from PDFs."""
        all_docs = self._fetch_all_listings()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            title = doc["title"]
            content_id = doc["content_id"]
            doc_id = _make_id(content_id, title)

            logger.info("Processing: [%s] %s", content_id, title[:70])

            text = self._download_and_extract(content_id, doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for ContentID=%s, skipping", content_id)
                continue

            yield {
                "_id": doc_id,
                "title": title,
                "date": _parse_date(doc.get("date", "")),
                "category": doc["category"],
                "doc_type": _classify_type(title),
                "content_id": content_id,
                "pdf_url": f"{BASE_URL}/FinancialSectorRegulation/GetContentFile/?ContentID={content_id}",
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        """Fetch updates — for a small collection, re-fetch all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "MW/RBM",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "category": raw.get("category", ""),
            "doc_type": raw.get("doc_type", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="MW/RBM bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = RBMScraper()

    if args.command == "test":
        docs = scraper._fetch_all_listings()
        print(f"OK — found {len(docs)} unique documents across {len(LISTING_PAGES)} listing pages")
        cats = {}
        for d in docs:
            c = d.get("category", "?")
            cats[c] = cats.get(c, 0) + 1
        for c, n in sorted(cats.items()):
            print(f"  {c}: {n}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
