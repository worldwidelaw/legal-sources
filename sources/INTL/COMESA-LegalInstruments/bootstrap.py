#!/usr/bin/env python3
"""
INTL/COMESA-LegalInstruments -- COMESA Treaty, Protocols, Regulations & Gazette

Fetches legal instruments from:
  - comesacourt.org/comesa-treaty/ (treaty + protocols)
  - comesacourt.org/comesa-regulations/ (regulations)
  - comesa.int gazette volumes (official gazette PDFs)

Strategy:
  - Scrape HTML pages for PDF download links
  - Download PDFs and extract text via common.pdf_extract, whose RTL repair
    puts the Arabic instruments in logical rather than visual order (#1560)
  - Normalize to standard schema

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap --full     # Full fetch
  python bootstrap.py bootstrap-fast       # Alias for --full
"""

import re
import sys
import json
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

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
logger = logging.getLogger("legal-data-hunter.INTL.COMESA-LegalInstruments")

SOURCE_ID = "INTL/COMESA-LegalInstruments"
TREATY_URL = "https://comesacourt.org/comesa-treaty/"
REGULATIONS_URL = "https://comesacourt.org/comesa-regulations/"

# Known working gazette PDFs (verified accessible)
GAZETTE_PDFS = [
    {
        "title": "COMESA Official Gazette Vol. 20 No. 1 (March 2015)",
        "url": "https://www.comesa.int/wp-content/uploads/2020/05/comesa-gazette-2015_vol-20_1_email.pdf",
        "date": "2015-03-31",
        "instrument_type": "gazette",
    },
    {
        "title": "COMESA Official Gazette Vol. 17 Annex 12",
        "url": "https://www.comesa.int/wp-content/uploads/2020/05/2012_Gazette_Vol_17_Annex_12.pdf",
        "date": "2012-11-01",
        "instrument_type": "gazette",
    },
]

# Additional legal instruments directly from comesa.int
COMESA_INT_PDFS = [
    {
        "title": "COMESA Treaty",
        "url": "https://www.comesa.int/wp-content/uploads/2020/07/Comesa-Treaty.pdf",
        "date": "1993-11-05",
        "instrument_type": "treaty",
    },
    {
        "title": "COMESA Protocol on Rules of Origin (Revised 2021)",
        "url": "https://www.comesa.int/wp-content/uploads/2022/04/Protocol-on-Rules-of-Origin-Revised-2021_WITH-NEW-FORM_upload.pdf",
        "date": "2021-01-01",
        "instrument_type": "protocol",
    },
    {
        "title": "COMESA Protocol on Rules of Origin",
        "url": "https://www.comesa.int/wp-content/uploads/2022/01/COMESA_Protocol-on-Rules-of-Origin.pdf",
        "date": "1994-01-01",
        "instrument_type": "protocol",
    },
]


class COMESALegalInstrumentsScraper(BaseScraper):
    """
    Scraper for INTL/COMESA-LegalInstruments.
    Country: INTL
    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _scrape_pdf_links(self, page_url: str, instrument_type: str) -> list[dict]:
        """Scrape a comesacourt.org page for PDF download links."""
        logger.info(f"Fetching {instrument_type} links from {page_url}")
        resp = self.session.get(page_url, timeout=60)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        documents = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.endswith(".pdf"):
                continue
            title = link.get_text(strip=True)
            if not title or len(title) < 5:
                # Try parent element text
                parent = link.find_parent(["p", "li", "div"])
                if parent:
                    title = parent.get_text(strip=True)
            if not title or len(title) < 5:
                title = href.split("/")[-1].replace(".pdf", "").replace("-", " ").replace("_", " ")

            # Normalize URL
            if href.startswith("/"):
                href = "https://comesacourt.org" + href
            elif not href.startswith("http"):
                href = "https://comesacourt.org/" + href

            # Extract date from title or filename
            date = self._extract_date(title, href)

            documents.append({
                "title": title,
                "url": href,
                "date": date,
                "instrument_type": instrument_type,
            })

        logger.info(f"Found {len(documents)} {instrument_type} documents")
        return documents

    def _extract_date(self, title: str, url: str) -> Optional[str]:
        """Try to extract a date from document title or URL."""
        combined = f"{title} {url}"

        # Look for year patterns like "(2009)", "(June 2009)", "(December 2004)"
        month_year = re.search(
            r"(?:January|February|March|April|May|June|July|August|September|"
            r"October|November|December)\s+(\d{4})", combined, re.IGNORECASE
        )
        if month_year:
            months = {
                "january": "01", "february": "02", "march": "03", "april": "04",
                "may": "05", "june": "06", "july": "07", "august": "08",
                "september": "09", "october": "10", "november": "11", "december": "12",
            }
            month_match = re.search(
                r"(January|February|March|April|May|June|July|August|September|"
                r"October|November|December)", combined, re.IGNORECASE
            )
            if month_match:
                m = months.get(month_match.group(1).lower())
                yr = month_year.group(1)
                return f"{yr}-{m}-01"

        # Just a year
        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", combined)
        if year_match:
            return f"{year_match.group(1)}-01-01"

        return None

    def _download_pdf_text(self, url: str, doc_id: str) -> str:
        """Download a PDF and extract its text in logical order.

        This went through pdfplumber directly, which emits glyphs in the order
        the content stream lists them — visual order for the Arabic instruments
        here, so their text was stored character-reversed and unsearchable by
        keyword (issue #1560). The shared helper's RTL repair reorders glyph
        clusters by geometry, so extraction has to route through it.

        force=True because the rows already in Neon are the reversed ones this
        is meant to replace; without it the helper skips them as present.
        """
        logger.info(f"Downloading PDF: {url}")
        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()

        if b"%PDF" not in resp.content[:1024]:
            logger.warning(f"Not a valid PDF: {url}")
            return ""

        full_text = extract_pdf_markdown(
            source="INTL/COMESA-LegalInstruments",
            source_id=doc_id,
            pdf_bytes=resp.content,
            table="legislation",
            force=True,
        ) or ""

        # Clean up common PDF artifacts
        full_text = re.sub(r"\n{3,}", "\n\n", full_text).strip()

        logger.info(f"  Extracted {len(full_text)} chars")
        return full_text

    def _make_id(self, doc: dict) -> str:
        """Generate a stable unique ID from document metadata."""
        key = f"{doc['url']}"
        return f"comesa-li-{hashlib.md5(key.encode()).hexdigest()[:12]}"

    def _gather_all_documents(self) -> list[dict]:
        """Collect all document metadata from all sources."""
        all_docs = []

        # 1. Treaty & Protocols from comesacourt.org
        treaty_docs = self._scrape_pdf_links(TREATY_URL, "treaty")
        all_docs.extend(treaty_docs)
        time.sleep(1.5)

        # 2. Regulations from comesacourt.org
        reg_docs = self._scrape_pdf_links(REGULATIONS_URL, "regulation")
        all_docs.extend(reg_docs)

        # 3. Additional instruments from comesa.int (hardcoded, verified)
        all_docs.extend(COMESA_INT_PDFS)

        # 4. Gazette PDFs (hardcoded, verified)
        all_docs.extend(GAZETTE_PDFS)

        # Deduplicate by URL (normalize to avoid http vs https dupes)
        seen_urls = set()
        unique_docs = []
        for doc in all_docs:
            norm_url = doc["url"].replace("http://", "https://").rstrip("/")
            if norm_url not in seen_urls:
                seen_urls.add(norm_url)
                unique_docs.append(doc)

        logger.info(f"Total unique documents: {len(unique_docs)}")
        return unique_docs

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legal instruments with full text."""
        documents = self._gather_all_documents()

        for i, doc in enumerate(documents):
            logger.info(f"[{i+1}/{len(documents)}] Processing: {doc['title'][:70]}...")
            time.sleep(1.5)

            try:
                text = self._download_pdf_text(doc["url"], self._make_id(doc))
            except Exception as e:
                logger.error(f"Failed to download {doc['url']}: {e}")
                continue

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text ({len(text)} chars) for: {doc['title'][:60]}")
                continue

            doc["text"] = text
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all (small corpus, no incremental updates)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw document to standard schema."""
        text = raw.get("text", "")
        if not text:
            return None

        title = raw.get("title", "").strip()
        url = raw.get("url", "")
        if url.startswith("http://"):
            url = "https://" + url[7:]

        instrument_type = raw.get("instrument_type", "legislation")

        # Refine instrument_type from title
        title_lower = title.lower()
        if "treaty" in title_lower:
            instrument_type = "treaty"
        elif "protocol" in title_lower:
            instrument_type = "protocol"
        elif "regulation" in title_lower:
            instrument_type = "regulation"
        elif "gazette" in title_lower:
            instrument_type = "gazette"

        return {
            "_id": self._make_id(raw),
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "instrument_type": instrument_type,
            "organization": "COMESA",
        }


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/COMESA-LegalInstruments Data Fetcher")
    subparsers = parser.add_subparsers(dest="command")

    boot = subparsers.add_parser("bootstrap", help="Fetch data")
    boot.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    subparsers.add_parser("bootstrap-fast", help="Alias for bootstrap --full")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = COMESALegalInstrumentsScraper()

    if args.command == "bootstrap":
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        elif args.full:
            stats = scraper.bootstrap(sample_mode=False)
        else:
            parser.print_help()
            return
        logger.info(f"Bootstrap stats: {json.dumps(stats, indent=2)}")
    elif args.command == "bootstrap-fast":
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Bootstrap stats: {json.dumps(stats, indent=2)}")
    elif args.command == "test":
        logger.info("Testing connectivity...")
        docs = scraper._gather_all_documents()
        logger.info(f"Found {len(docs)} documents to fetch")
        for d in docs:
            logger.info(f"  - {d['title'][:70]} ({d['instrument_type']})")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
