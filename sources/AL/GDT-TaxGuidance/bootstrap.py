#!/usr/bin/env python3
"""
AL/GDT-TaxGuidance -- Albanian General Directorate of Taxation

Fetches tax legislation and guidance documents from tatime.gov.al.
Scrapes category pages for PDF download links, downloads PDFs, extracts text.

Strategy:
  - GET each of 10 legislation category pages
  - Extract document links (shkarko.php?id=XXXX)
  - Download each PDF and extract text
  - Skip empty downloads and non-PDF files (scanned images)

Data:
  - ~160 documents across 10 categories
  - Tax procedures, income tax, VAT, national taxes, local taxes,
    social security, gambling, international agreements, GDT acts
  - Mix of laws (Ligj), government decisions (VKM), ministerial
    guidelines (UMF), and technical decisions

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AL.GDT-TaxGuidance")

BASE_URL = "https://www.tatime.gov.al"

CATEGORIES = [
    {"path": "eng/c/6/69/tax-procedures", "name": "Tax Procedures"},
    {"path": "eng/c/6/332/automatic-exchange-of-information", "name": "Automatic Exchange of Information"},
    {"path": "eng/c/6/70/income-tax", "name": "Income Tax"},
    {"path": "eng/c/6/71/value-added-tax", "name": "Value Added Tax"},
    {"path": "eng/c/6/72/national-taxes", "name": "National Taxes"},
    {"path": "eng/c/6/75/local-taxes", "name": "Local Taxes"},
    {"path": "eng/c/6/73/social-security-and-health-care-contributions", "name": "Social Security"},
    {"path": "eng/c/6/74/gambling-casinos-and-racetracks", "name": "Gambling"},
    {"path": "eng/c/6/125/international-agreements", "name": "International Agreements"},
    {"path": "eng/c/6/184/acts-of-the-gdt", "name": "Acts of the GDT"},
]


class DPTScraper(BaseScraper):
    """Scraper for AL/GDT-TaxGuidance -- Albanian Tax Legislation & Guidance."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
        })

    def _fetch_category_docs(self, cat: dict) -> list:
        """Fetch document links from a category page."""
        url = f"{BASE_URL}/{cat['path']}"
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        docs = []
        seen_ids = set()

        for link in soup.find_all("a", href=re.compile(r"shkarko\.php\?id=\d+")):
            match = re.search(r"id=(\d+)", link["href"])
            if not match:
                continue
            doc_id = match.group(1)
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            title = link.get_text(strip=True)
            if not title or len(title) < 3:
                continue

            # Clean up nbsp and extra whitespace
            title = re.sub(r"\s+", " ", title.replace("\xa0", " ")).strip()

            docs.append({
                "doc_id": doc_id,
                "title": title,
                "download_url": f"{BASE_URL}/shkarko.php?id={doc_id}",
                "category": cat["name"],
            })

        return docs

    def _download_and_extract(self, doc: dict) -> Optional[str]:
        """Download a document and extract text. Returns None if not a valid PDF."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(doc["download_url"], timeout=120, allow_redirects=True)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Download failed for %s: %s", doc["doc_id"], e)
            return None

        content = resp.content
        if len(content) < 100:
            logger.debug("Empty/tiny download for %s (%d bytes)", doc["doc_id"], len(content))
            return None

        # Check if it's actually a PDF (starts with %PDF)
        if not content[:5].startswith(b"%PDF"):
            logger.debug("Not a PDF for %s (starts with %s)", doc["doc_id"], content[:10])
            return None

        text = extract_pdf_markdown(
            source="AL/GDT-TaxGuidance",
            source_id=f"dpt-{doc['doc_id']}",
            pdf_bytes=content,
            table="doctrine",
            force=True,
        )
        return text or None

    def _parse_date_from_title(self, title: str) -> Optional[str]:
        """Extract date from title patterns like 'datë 21.04.2011' or 'date 16.12.2009'."""
        m = re.search(r"dat[ëe]\s+(\d{1,2})[./](\d{1,2})[./](\d{4})", title, re.IGNORECASE)
        if m:
            day, month, year = m.group(1), m.group(2), m.group(3)
            try:
                return datetime.strptime(f"{day}/{month}/{year}", "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                pass
        return None

    def _make_id(self, doc: dict) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", doc["title"][:60]).strip("-").lower()
        return f"dpt-{doc['doc_id']}-{slug}"

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": "AL/GDT-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all DPT tax documents."""
        for cat in CATEGORIES:
            logger.info("Fetching category: %s", cat["name"])
            docs = self._fetch_category_docs(cat)
            logger.info("Found %d documents in %s", len(docs), cat["name"])

            for doc in docs:
                text = self._download_and_extract(doc)
                if not text or len(text) < 50:
                    logger.debug("Skipping %s: insufficient text", doc["doc_id"])
                    continue

                raw = {
                    "_id": self._make_id(doc),
                    "title": doc["title"],
                    "text": text,
                    "date": self._parse_date_from_title(doc["title"]),
                    "url": doc["download_url"],
                    "category": doc["category"],
                }
                yield self.normalize(raw)

    def fetch_updates(self, since=None):
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        try:
            docs = self._fetch_category_docs(CATEGORIES[0])
            logger.info("Connection OK: %d documents in Tax Procedures", len(docs))
            return len(docs) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="AL/GDT-TaxGuidance Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DPTScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
