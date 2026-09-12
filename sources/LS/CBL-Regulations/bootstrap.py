#!/usr/bin/env python3
"""
LS/CBL-Regulations -- Central Bank of Lesotho Financial Sector Regulations

Fetches financial sector regulations from the CBL legislations page.
All documents are PDF files hosted at centralbank.org.ls/wp-content/uploads/.
Full text is extracted from PDFs via pdfplumber.

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
logger = logging.getLogger("legal-data-hunter.LS.CBL-Regulations")

BASE_URL = "https://centralbank.org.ls"
LEGISLATIONS_URL = f"{BASE_URL}/legislations/"
DELAY = 2.0


def _make_id(filename: str) -> str:
    """Generate a stable ID from the PDF filename."""
    slug = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", slug).strip("_")
    if len(slug) > 80:
        slug = slug[:80]
    return f"LS_CBL_{slug}"


def _extract_year(filename: str) -> Optional[str]:
    """Extract year from filename like 'Pension-Funds-Regulations-2020.pdf'."""
    m = re.search(r"(\d{4})", filename)
    if m:
        year = int(m.group(1))
        if 1950 <= year <= 2030:
            return f"{year}-01-01"
    return None


def _extract_title(filename: str) -> str:
    """Convert filename to human-readable title."""
    title = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
    title = re.sub(r"[-_]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    # Fix common patterns
    title = re.sub(r"\bmin$", "", title).strip()
    title = re.sub(r"\b(\d)$", r" \1", title).strip()
    return title


def _classify_category(title: str) -> str:
    """Classify document category from title."""
    t = title.lower()
    if "insurance" in t or "insurer" in t or "reinsurance" in t:
        return "Insurance"
    if "pension" in t:
        return "Pensions"
    if "capital market" in t or "collective investment" in t or "securities" in t:
        return "Capital Markets"
    if "money laundering" in t or "aml" in t or "financing of terrorism" in t:
        return "AML/CFT"
    if "payment" in t or "electronic" in t:
        return "Payment Systems"
    if "credit reporting" in t or "data protection" in t:
        return "Data Protection & Credit"
    if "money transfer" in t or "foreign exchange" in t or "forex" in t:
        return "Foreign Exchange"
    if "financial institution" in t or "banking" in t or "lending" in t or "licensing" in t:
        return "Banking"
    if "money lender" in t or "money order" in t:
        return "Banking"
    if "exchange control" in t:
        return "Exchange Control"
    if "financial lease" in t:
        return "Financial Leasing"
    if "consumer" in t:
        return "Consumer Protection"
    return "General"


def _classify_type(title: str) -> str:
    """Classify document type from title."""
    t = title.lower()
    if "act" in t:
        return "act"
    if "regulation" in t:
        return "regulation"
    if "guideline" in t:
        return "guideline"
    if "code" in t:
        return "code"
    if "notice" in t:
        return "notice"
    if "order" in t:
        return "order"
    if "rules" in t:
        return "rules"
    return "regulation"


class CBLScraper(BaseScraper):
    """Scraper for Central Bank of Lesotho financial regulations."""

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

    def _scrape_pdf_links(self) -> List[Dict[str, Any]]:
        """Scrape the legislations page for PDF links."""
        logger.info("Fetching legislations page: %s", LEGISLATIONS_URL)
        try:
            resp = self.http.get(LEGISLATIONS_URL, timeout=30)
            if resp.status_code != 200:
                logger.error("HTTP %d for legislations page", resp.status_code)
                return []
        except Exception as e:
            logger.error("Error fetching legislations page: %s", e)
            return []

        html = resp.text
        docs = []
        seen = set()

        # Extract all PDF links from wp-content/uploads
        for m in re.finditer(
            r'href="([^"]*?/wp-content/uploads/([^"]+\.pdf))"',
            html,
            re.IGNORECASE,
        ):
            full_url = m.group(1)
            filename = m.group(2)

            if filename in seen:
                continue
            seen.add(filename)

            # Normalize URL
            if full_url.startswith("/"):
                full_url = BASE_URL + full_url

            title = _extract_title(filename)
            date = _extract_year(filename)

            docs.append({
                "filename": filename,
                "url": full_url,
                "title": title,
                "date": date,
                "category": _classify_category(title),
                "doc_type": _classify_type(title),
            })

        logger.info("Found %d unique PDF documents", len(docs))
        return docs

    def _download_and_extract(self, url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            resp = self.http.get(url, timeout=60)
            if resp.status_code != 200:
                logger.warning("HTTP %d downloading %s", resp.status_code, url)
                return None
            pdf_bytes = resp.content
            if len(pdf_bytes) < 200:
                logger.warning("File too small (%d bytes): %s", len(pdf_bytes), url)
                return None
            if not pdf_bytes[:5].startswith(b"%PDF"):
                logger.warning("Not a PDF: %s", url)
                return None
            text = extract_pdf_markdown("LS/CBL-Regulations", doc_id, pdf_bytes=pdf_bytes)
            return text
        except Exception as e:
            logger.warning("Failed to download/extract %s: %s", url, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all CBL documents with full text from PDFs."""
        all_docs = self._scrape_pdf_links()
        logger.info("Total documents to process: %d", len(all_docs))

        for doc in all_docs:
            title = doc["title"]
            doc_id = _make_id(doc["filename"])

            logger.info("Processing: %s", title[:70])

            text = self._download_and_extract(doc["url"], doc_id)
            if not text or len(text.strip()) < 50:
                logger.warning("Insufficient text for %s (%d chars), skipping",
                             doc["filename"], len(text.strip()) if text else 0)
                continue

            yield {
                "_id": doc_id,
                "title": title,
                "date": doc["date"],
                "category": doc["category"],
                "doc_type": doc["doc_type"],
                "pdf_url": doc["url"],
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
            "_source": "LS/CBL-Regulations",
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

    parser = argparse.ArgumentParser(description="LS/CBL-Regulations bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = CBLScraper()

    if args.command == "test":
        docs = scraper._scrape_pdf_links()
        print(f"OK — found {len(docs)} unique PDF documents")
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
