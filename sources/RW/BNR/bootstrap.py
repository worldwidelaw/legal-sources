#!/usr/bin/env python3
"""
RW/BNR -- National Bank of Rwanda Financial Regulations

Fetches financial sector regulations from the BNR website via JSON API
endpoints. Each regulatory category (Banking, Insurance, Pension, etc.)
has a dedicated endpoint returning document metadata with PDF links.
Full text is extracted from PDFs via common.pdf_extract.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
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
logger = logging.getLogger("legal-data-hunter.RW.BNR")

BASE_URL = "https://www.bnr.rw"
DELAY = 2.0

# Each tuple: (endpoint, category_label)
CATEGORY_ENDPOINTS = [
    ("banking_laws", "Banking"),
    ("aml_laws", "AML/CFT"),
    ("deposit_laws", "Deposit Guarantee"),
    ("fscp_laws", "Financial Consumer Protection"),
    ("insurance_all", "Insurance"),
    ("microfinance_laws", "Microfinance"),
    ("ndfis_laws", "Non-deposit Taking FSPs"),
    ("pension_all", "Pension"),
    ("regulatory_digest_laws", "Regulatory Digest"),
    ("tcsps_laws", "Trust & Company Service Providers"),
    ("accredlaw", "Accreditation"),
    ("forex_laws", "Foreign Exchange"),
    ("ccrf_laws", "Cross-cutting/BNR Law"),
    ("ccy_laws", "Currency"),
    ("crs_laws", "Credit Reporting"),
    ("fm_laws", "Financial Markets"),
    ("psystem_laws", "Payment Systems"),
]


def _make_id(doc_id: str, name: str) -> str:
    """Generate a stable ID from the API document id and name."""
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", name).strip("_")
    if len(slug) > 60:
        slug = slug[:60]
    return f"RW_BNR_{doc_id}_{slug}"


def _extract_date(date_str: Optional[str]) -> Optional[str]:
    """Parse date from API response into ISO format."""
    if not date_str:
        return None
    try:
        # Handle ISO datetime strings like "2024-09-17T22:00:00.000Z"
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        pass
    # Handle plain date strings like "2024-09-17"
    m = re.search(r"(\d{4}-\d{2}-\d{2})", str(date_str))
    if m:
        return m.group(1)
    return None


def _classify_type(doc_type: Optional[str], name: str) -> str:
    """Map the API type field to our doc_type."""
    t = (doc_type or "").lower()
    n = name.lower()
    if "law" in t or "law" in n:
        return "law"
    if "regulation" in t or "regulation" in n:
        return "regulation"
    if "directive" in t or "directive" in n:
        return "directive"
    if "guideline" in t or "guidelines" in n or "guidance" in n:
        return "guideline"
    if "circular" in t or "circular" in n:
        return "circular"
    return t or "regulation"


class BNRScraper(BaseScraper):
    """Scraper for National Bank of Rwanda financial regulations."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
                "Accept-Language": "en-US,en;q=0.5",
            },
        )

    def _fetch_category(self, endpoint: str, category: str) -> List[Dict[str, Any]]:
        """Fetch all documents from a category endpoint."""
        url = f"{BASE_URL}/{endpoint}"
        logger.info("Fetching category %s from %s", category, url)
        try:
            resp = self.http.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return []
            data = resp.json()
            if not isinstance(data, list):
                logger.warning("Unexpected response type for %s: %s", endpoint, type(data))
                return []
            logger.info("  %s: %d documents", category, len(data))
            return [dict(d, _category=category) for d in data]
        except Exception as e:
            logger.warning("Error fetching %s: %s", url, e)
            return []

    def _fetch_all_categories(self) -> List[Dict[str, Any]]:
        """Fetch documents from all category endpoints."""
        all_docs = []
        seen_ids = set()
        for endpoint, category in CATEGORY_ENDPOINTS:
            docs = self._fetch_category(endpoint, category)
            for doc in docs:
                doc_id = doc.get("id", "")
                if doc_id and doc_id in seen_ids:
                    continue
                seen_ids.add(doc_id)
                all_docs.append(doc)
            time.sleep(0.5)
        logger.info("Total unique documents: %d", len(all_docs))
        return all_docs

    def _download_and_extract(self, file_path: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        pdf_url = f"{BASE_URL}{file_path}" if file_path.startswith("/") else file_path
        try:
            resp = self.http.get(pdf_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, pdf_url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 100:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None
            text = extract_pdf_markdown("RW/BNR", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BNR documents with full text from PDFs."""
        all_docs = self._fetch_all_categories()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            name = doc.get("name", "Unknown")
            file_path = doc.get("file", "")
            api_id = doc.get("id", "0")
            category = doc.get("_category", "")

            if not file_path:
                logger.warning("No file path for document: %s", name)
                continue

            doc_id = _make_id(api_id, name)
            logger.info("Processing: %s", name[:80])

            text = self._download_and_extract(file_path, doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s, skipping", doc_id)
                continue

            yield {
                "_id": doc_id,
                "title": name,
                "date": _extract_date(doc.get("date_last_modified")),
                "category": category,
                "doc_type": _classify_type(doc.get("type"), name),
                "summary": doc.get("summary", ""),
                "pdf_url": f"{BASE_URL}{file_path}" if file_path.startswith("/") else file_path,
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
            "_source": "RW/BNR",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "category": raw.get("category", ""),
            "doc_type": raw.get("doc_type", ""),
            "summary": raw.get("summary", ""),
            "url": raw.get("pdf_url", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="RW/BNR bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = BNRScraper()

    if args.command == "test":
        docs = scraper._fetch_all_categories()
        print(f"OK — found {len(docs)} documents across {len(CATEGORY_ENDPOINTS)} categories")
        cats = {}
        for d in docs:
            c = d.get("_category", "?")
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
