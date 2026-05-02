#!/usr/bin/env python3
"""
KE/CBK -- Central Bank of Kenya Prudential Guidelines & Circulars

Fetches banking circulars, prudential guidelines, and regulatory guidance
from the Central Bank of Kenya website. Documents are PDFs linked from
the legislation-and-guidelines section.

Sources scraped:
  - Circulars page: banking, microfinance, payments, and general circulars
  - Legislation & Guidelines page: prudential guidelines, forex guidelines, etc.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict, Any
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KE.CBK")

BASE_URL = "https://www.centralbank.go.ke"
CIRCULARS_URL = f"{BASE_URL}/policy-procedures/legislation-and-guidelines/circulars/"
GUIDELINES_URL = f"{BASE_URL}/policy-procedures/legislation-and-guidelines/"
DELAY = 2.0

# Pages to scrape for documents
SOURCE_PAGES = [
    ("circulars", CIRCULARS_URL),
    ("guidelines", GUIDELINES_URL),
]


def _make_id(pdf_url: str) -> str:
    """Generate a stable ID from the PDF URL."""
    name = unquote(pdf_url).split("/")[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = re.sub(r"^\d+_", "", name)  # strip leading numeric prefix
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    if len(name) > 120:
        name = name[:120]
    return f"KE_CBK_{name}"


def _classify_doc(title: str, url: str) -> str:
    """Classify document type from title and URL."""
    t = title.lower()
    u = url.lower()
    if "circular" in t or "banking_circulars" in u:
        return "circular"
    if "prudential" in t:
        return "prudential_guideline"
    if "guideline" in t or "guidance" in t:
        return "guideline"
    if "regulation" in t:
        return "regulation"
    if "act" in t or "law" in t:
        return "legislation"
    if "forex" in t or "foreign exchange" in t:
        return "forex_guideline"
    return "regulatory_document"


def _extract_year(title: str) -> Optional[str]:
    """Try to extract a year from the title for approximate dating."""
    m = re.search(r"\b(20\d{2}|19\d{2})\b", title)
    return f"{m.group(1)}-01-01" if m else None


class CBKScraper(BaseScraper):
    """Scraper for Central Bank of Kenya regulatory documents."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
        )

    def _discover_documents(self) -> List[Dict[str, Any]]:
        """Discover all PDF documents from CBK pages."""
        from bs4 import BeautifulSoup

        all_docs = []
        seen_urls = set()

        for label, page_url in SOURCE_PAGES:
            try:
                resp = self.http.get(page_url, timeout=30)
                if resp.status_code != 200:
                    logger.warning("HTTP %d for %s", resp.status_code, page_url)
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")

                for a in soup.find_all("a"):
                    href = a.get("href", "")
                    text = a.get_text(strip=True)
                    if not href or ".pdf" not in href.lower():
                        continue
                    if not text or len(text) < 3:
                        continue

                    # Normalize URL
                    if href.startswith("/"):
                        href = BASE_URL + href

                    # Deduplicate by URL
                    url_key = href.split("?")[0].lower()
                    if url_key in seen_urls:
                        continue
                    seen_urls.add(url_key)

                    all_docs.append({
                        "pdf_url": href,
                        "title": text.strip(),
                        "category": label,
                        "doc_type": _classify_doc(text, href),
                        "date": _extract_year(text),
                    })

                logger.info("Discovered %d PDFs from %s", len(all_docs), label)
                time.sleep(1.0)

            except Exception as e:
                logger.warning("Error scraping %s: %s", page_url, e)

        logger.info("Total unique documents discovered: %d", len(all_docs))
        return all_docs

    def _download_and_extract(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 200:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("KE/CBK", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all CBK documents with full text."""
        all_docs = self._discover_documents()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            doc_id = _make_id(doc["pdf_url"])
            logger.info("Processing: %s", doc["title"][:80])

            text = self._download_and_extract(doc["pdf_url"], doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s, skipping", doc_id)
                continue

            yield {
                "_id": doc_id,
                "title": doc["title"],
                "date": doc["date"],
                "doc_type": doc["doc_type"],
                "category": doc["category"],
                "pdf_url": doc["pdf_url"],
                "text": text,
            }
            time.sleep(DELAY)

    def fetch_updates(self, since: str = "") -> Generator[dict, None, None]:
        """Fetch updates -- re-fetch all for this collection."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "KE/CBK",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "doc_type": raw.get("doc_type", ""),
            "category": raw.get("category", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="KE/CBK bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = CBKScraper()

    if args.command == "test":
        docs = scraper._discover_documents()
        print(f"OK -- found {len(docs)} documents")
        types = {}
        for d in docs:
            t = d["doc_type"]
            types[t] = types.get(t, 0) + 1
        for t, c in sorted(types.items()):
            print(f"  {t}: {c}")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
