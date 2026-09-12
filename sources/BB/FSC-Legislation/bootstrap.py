#!/usr/bin/env python3
"""
BB/FSC-Legislation -- Barbados Financial Services Commission: Legislation & Guidelines

Fetches the legal framework of the Financial Services Commission of Barbados
(FSC) — the regulator of the non-bank financial sector (insurance, securities,
credit unions, pensions). Covers binding legislation (Acts, Regulations),
industry guidelines, AML/CFT guidelines and regulatory notices.

Strategy (no public API; open HTML site):
  - Scrape the FSC legal-framework pages for PDF links at
    fsc.gov.bb/viewPDF/documents/.
  - Download each PDF and extract full text with pdfplumber.
  - Classify Acts/Regulations as `legislation`, guidelines/circulars/notices
    as `doctrine`.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import io
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Set

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BB.FSC-Legislation")

BASE_URL = "https://www.fsc.gov.bb"

# Legal-framework listing pages — each links to /viewPDF/documents/*.pdf acts.
LISTING_PAGES = [
    "/legislation",
    "/industry-guidelines",
    "/legislation-guidelines",
    "/regulatory-notices",
    "/aml-cft",
]

# PDF links are served from /viewPDF/documents/{date-prefix}{name}.pdf
PDF_RE = re.compile(
    r'href=["\'](?:https?://www\.fsc\.gov\.bb)?(/viewPDF/documents/[^"\']+\.pdf)["\']',
    re.I,
)
# Date prefix in filenames: YYYY-MM-DD-HH-MM-SS-filename.pdf
DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})-\d{2}-\d{2}-\d{2}-(.*?)\.pdf$", re.I)

# Filename/title keywords that mark a document as binding legislation.
LEGISLATION_KW = re.compile(
    r"\b(act|regulations?|cap\.?\s*\d|order|by[- ]?laws?|statutory[- ]instrument)\b",
    re.I,
)


def extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        pdf = pdfplumber.open(io.BytesIO(content))
        pages = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        pdf.close()
        return "\n\n".join(pages)
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return ""


def filename_to_title(filename: str) -> str:
    """Convert a PDF filename to a human-readable title."""
    m = DATE_RE.search(filename)
    name = m.group(4) if m else filename.replace(".pdf", "").replace(".PDF", "")
    name = name.replace("---", " — ").replace("--", " — ").replace("-", " ")
    name = re.sub(r"\s+", " ", name).strip()
    # Title-case shortish names; leave already-spaced long names alone.
    if len(name) < 90 and name == name.lower():
        name = name.title()
    return name.strip()


def extract_date(url: str) -> Optional[str]:
    """Extract publication date from the filename date prefix."""
    m = DATE_RE.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


def classify(title: str, filename: str) -> str:
    """Return 'legislation' for Acts/Regulations, else 'doctrine'."""
    blob = f"{title} {filename}"
    if LEGISLATION_KW.search(blob):
        # Guidelines/circulars/bulletins about an act are still doctrine.
        if re.search(r"\b(guideline|circular|bulletin|notice|faq|questionnaire|consultation|methodology|reference guide)\b", blob, re.I):
            return "doctrine"
        return "legislation"
    return "doctrine"


class FSCLegislationScraper(BaseScraper):
    """
    Scraper for BB/FSC-Legislation — Barbados Financial Services Commission.
    Country: BB
    URL: https://www.fsc.gov.bb/
    Data types: legislation, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)",
        })

    def _collect_pdf_urls(self) -> Set[str]:
        """Scrape all listing pages for unique absolute PDF URLs."""
        pdfs: Set[str] = set()
        for page_path in LISTING_PAGES:
            url = f"{BASE_URL}{page_path}"
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code == 200:
                    found = PDF_RE.findall(r.text)
                    abs_found = {f"{BASE_URL}{p}" for p in found}
                    pdfs.update(abs_found)
                    logger.info(f"Page {page_path}: {len(abs_found)} PDFs")
                else:
                    logger.warning(f"Page {page_path}: HTTP {r.status_code}")
            except Exception as e:
                logger.warning(f"Failed to scrape {page_path}: {e}")
            time.sleep(0.8)
        logger.info(f"Total unique PDFs: {len(pdfs)}")
        return pdfs

    def _download_and_extract(self, pdf_url: str) -> Optional[dict]:
        """Download a PDF and extract its text."""
        try:
            r = self.session.get(pdf_url, timeout=90)
            r.raise_for_status()
        except Exception as e:
            logger.warning(f"Download failed for {pdf_url}: {e}")
            return None

        text = extract_pdf_text(r.content)
        if len(text) < 200:
            logger.warning(f"Insufficient text from {pdf_url}: {len(text)} chars")
            return None

        filename = pdf_url.split("/")[-1]
        title = filename_to_title(filename)
        return {
            "url": pdf_url,
            "title": title,
            "text": text,
            "date": extract_date(pdf_url),
            "filename": filename,
            "doc_type": classify(title, filename),
            "pdf_size": len(r.content),
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw PDF data into the standard schema."""
        text = re.sub(r"[ \t]+", " ", raw["text"])
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return {
            "_id": raw["url"],
            "_source": "BB/FSC-Legislation",
            "_type": raw.get("doc_type", "doctrine"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": text,
            "date": raw.get("date"),
            "url": raw["url"],
            "filename": raw.get("filename", ""),
            "language": "en",
            "jurisdiction": "BB",
            "pdf_size": raw.get("pdf_size", 0),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all FSC documents with full text."""
        pdf_urls = self._collect_pdf_urls()
        yielded = 0
        for pdf_url in sorted(pdf_urls):
            result = self._download_and_extract(pdf_url)
            if result:
                yield result
                yielded += 1
                if yielded % 10 == 0:
                    logger.info(f"Processed {yielded}/{len(pdf_urls)} PDFs")
            time.sleep(1.0)
        logger.info(f"fetch_all complete: {yielded} documents")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield documents newer than `since` date."""
        since_s = since.date().isoformat() if hasattr(since, "date") else str(since)
        pdf_urls = self._collect_pdf_urls()
        yielded = 0
        for pdf_url in sorted(pdf_urls):
            date = extract_date(pdf_url)
            if date and date >= since_s:
                result = self._download_and_extract(pdf_url)
                if result:
                    yield result
                    yielded += 1
                time.sleep(1.0)
        logger.info(f"fetch_updates complete: {yielded} documents since {since_s}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="BB/FSC-Legislation — Barbados Financial Services Commission"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = FSCLegislationScraper()

    if args.command == "test":
        pdfs = scraper._collect_pdf_urls()
        logger.info(f"Found {len(pdfs)} unique PDFs")
        if pdfs:
            test_url = sorted(pdfs)[0]
            result = scraper._download_and_extract(test_url)
            if result:
                logger.info(f"Title: {result['title']}")
                logger.info(f"Type: {result['doc_type']}  Date: {result['date']}")
                logger.info(f"Text: {len(result['text'])} chars")
                logger.info(f"Preview: {result['text'][:200]}")
                logger.info("Connectivity test passed!")
            else:
                logger.warning("PDF extraction failed")
                sys.exit(1)
        else:
            logger.warning("No PDFs found")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
