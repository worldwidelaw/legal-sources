#!/usr/bin/env python3
"""
INTL/ITLOS -- International Tribunal for the Law of the Sea

Fetches judgments, advisory opinions, and orders from ITLOS.

Strategy:
  - Scrape case listing pages to discover all case URLs
  - Parse each case page to find judgment/opinion/order PDF links
  - Download PDFs and extract text with pdfplumber

Data Coverage:
  - ~33 cases since 1997
  - Judgments, advisory opinions, orders, separate/dissenting opinions
  - All public documents, PDF with extractable text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import io
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
    print("WARNING: pdfplumber not available. Install with: pip install pdfplumber")

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ITLOS")

BASE_URL = "https://www.itlos.org"
CASES_URL = f"{BASE_URL}/en/main/cases/list-of-cases/"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
    "Accept": "text/html,application/xhtml+xml",
}

# Document type keywords to identify in section headers and link text
JUDGMENT_KEYWORDS = ["judgment", "judgement", "advisory opinion"]
ORDER_KEYWORDS = ["order"]
OPINION_KEYWORDS = ["separate opinion", "dissenting opinion", "declaration of judge",
                    "declaration of", "individual opinion"]


class ITLOSScraper(BaseScraper):
    """Scraper for ITLOS case documents."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _discover_cases(self) -> list:
        """Discover all case page URLs from the listing page."""
        logger.info("Discovering ITLOS cases...")
        resp = self.session.get(CASES_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        cases = []
        seen_urls = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            text = link.get_text(strip=True)
            # Match case page links
            if "/cases/list-of-cases/" in href and href != "/en/main/cases/list-of-cases/":
                full_url = urljoin(BASE_URL, href)
                if full_url not in seen_urls and text:
                    seen_urls.add(full_url)
                    # Extract case number from URL or text
                    case_num = None
                    m = re.search(r"case-no[.-]?(\d+)", href)
                    if m:
                        case_num = m.group(1)
                    else:
                        m = re.search(r"Case\s*(?:No\.?\s*)?(\d+)", text)
                        if m:
                            case_num = m.group(1)

                    cases.append({
                        "url": full_url,
                        "title": text,
                        "case_number": case_num,
                    })

        logger.info(f"Found {len(cases)} cases")
        return cases

    def _find_pdf_links(self, case_url: str) -> list:
        """Parse a case page and find all judgment/order/opinion PDF links."""
        resp = self.session.get(case_url, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"HTTP {resp.status_code} for {case_url}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        documents = []
        seen_urls = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.lower().endswith(".pdf"):
                continue

            full_url = urljoin(BASE_URL, href)
            if full_url in seen_urls:
                continue
            seen_urls.add(full_url)

            link_text = link.get_text(strip=True)
            # Determine document type from link text and surrounding context
            text_lower = link_text.lower()
            href_lower = href.lower()

            doc_type = "other"
            if any(kw in text_lower or kw in href_lower for kw in JUDGMENT_KEYWORDS):
                doc_type = "judgment"
            elif any(kw in text_lower or kw in href_lower for kw in ["adv_op", "advisory"]):
                doc_type = "advisory_opinion"
            elif any(kw in text_lower or kw in href_lower for kw in ORDER_KEYWORDS):
                doc_type = "order"
            elif any(kw in text_lower for kw in OPINION_KEYWORDS):
                doc_type = "separate_opinion"

            # Skip non-substantive documents (e.g., press releases, procedural)
            if doc_type == "other":
                # Check parent sections
                parent = link.parent
                while parent and parent.name not in ["body", "html"]:
                    parent_text = parent.get_text(strip=True).lower()[:200]
                    if any(kw in parent_text for kw in JUDGMENT_KEYWORDS + ORDER_KEYWORDS):
                        if any(kw in parent_text for kw in JUDGMENT_KEYWORDS):
                            doc_type = "judgment"
                        else:
                            doc_type = "order"
                        break
                    parent = parent.parent

            # Skip press releases
            if "press" in text_lower or "press" in href_lower:
                continue

            # Only include substantive documents
            if doc_type in ("judgment", "advisory_opinion", "order", "separate_opinion"):
                documents.append({
                    "pdf_url": full_url,
                    "link_text": link_text,
                    "doc_type": doc_type,
                })

        return documents

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract text."""
        if not PDF_SUPPORT:
            logger.error("pdfplumber not available")
            return None
        try:
            resp = self.session.get(url, timeout=120, stream=True)
            resp.raise_for_status()
            chunks = []
            total = 0
            for chunk in resp.iter_content(chunk_size=65536):
                chunks.append(chunk)
                total += len(chunk)
                if total > 50 * 1024 * 1024:  # 50MB limit
                    logger.warning(f"PDF exceeds 50MB, skipping: {url}")
                    return None
            content = b"".join(chunks)
            if len(content) < 100:
                return None

            text_parts = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
            text = "\n\n".join(text_parts).strip()
            return text if len(text) >= 50 else None
        except Exception as e:
            logger.warning(f"Failed to extract PDF {url}: {e}")
            return None

    def _extract_date(self, title: str, text: str) -> Optional[str]:
        """Extract the document date from title first, then text."""
        months = {
            "January": 1, "February": 2, "March": 3, "April": 4,
            "May": 5, "June": 6, "July": 7, "August": 8,
            "September": 9, "October": 10, "November": 11, "December": 12,
        }
        pattern = r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})"

        # Try title first (most reliable)
        m = re.search(pattern, title)
        if m:
            day, month_name, year = int(m.group(1)), m.group(2), int(m.group(3))
            return f"{year:04d}-{months[month_name]:02d}-{day:02d}"

        # Try first 500 chars of text (document header)
        m = re.search(pattern, text[:500])
        if m:
            day, month_name, year = int(m.group(1)), m.group(2), int(m.group(3))
            return f"{year:04d}-{months[month_name]:02d}-{day:02d}"

        # Try date pattern like "20 March 2023" anywhere
        matches = list(re.finditer(pattern, text))
        if matches:
            m = matches[0]  # First occurrence
            day, month_name, year = int(m.group(1)), m.group(2), int(m.group(3))
            return f"{year:04d}-{months[month_name]:02d}-{day:02d}"

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all ITLOS documents with full text."""
        cases = self._discover_cases()

        for case in cases:
            time.sleep(2)
            logger.info(f"Processing case: {case['title'][:60]}...")
            pdf_docs = self._find_pdf_links(case["url"])

            if not pdf_docs:
                logger.info(f"  No substantive PDFs found for {case['title'][:40]}")
                continue

            logger.info(f"  Found {len(pdf_docs)} documents")

            for doc in pdf_docs:
                time.sleep(2)
                logger.info(f"  Downloading: {doc['link_text'][:50]}...")
                text = self._extract_pdf_text(doc["pdf_url"])
                if not text:
                    logger.warning(f"  No text extracted from {doc['pdf_url']}")
                    continue

                yield {
                    "case_title": case["title"],
                    "case_number": case.get("case_number"),
                    "case_url": case["url"],
                    "pdf_url": doc["pdf_url"],
                    "link_text": doc["link_text"],
                    "doc_type": doc["doc_type"],
                    "_extracted_text": text,
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent documents. For this small corpus, re-fetch all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw ITLOS record into standard schema."""
        text = raw.get("_extracted_text", "").strip()
        if not text or len(text) < 50:
            return None

        case_num = raw.get("case_number", "")
        doc_type = raw.get("doc_type", "")
        link_text = raw.get("link_text", "")
        case_title = raw.get("case_title", "")

        # Build a meaningful title
        title = link_text or f"{case_title} - {doc_type}"

        # Generate unique ID from PDF URL
        pdf_url = raw.get("pdf_url", "")
        pdf_filename = pdf_url.split("/")[-1].replace(".pdf", "").replace(".PDF", "")
        doc_id = f"ITLOS_C{case_num}_{pdf_filename}" if case_num else f"ITLOS_{pdf_filename}"
        # Sanitize ID
        doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", doc_id)

        # Extract date from title or text
        date_str = self._extract_date(title, text)

        return {
            "_id": doc_id,
            "_source": "INTL/ITLOS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": date_str,
            "url": pdf_url,
            "case_url": raw.get("case_url", ""),
            "case_number": case_num,
            "case_title": case_title,
            "document_type": doc_type,
            "court": "International Tribunal for the Law of the Sea (ITLOS)",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ITLOS bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    scraper = ITLOSScraper()

    if args.command == "test":
        print("Testing ITLOS connectivity...")
        try:
            cases = scraper._discover_cases()
            print(f"OK: Found {len(cases)} cases")
            if cases:
                print(f"  First: {cases[0]['title'][:60]}")
                # Test PDF discovery on first case
                time.sleep(2)
                pdfs = scraper._find_pdf_links(cases[0]["url"])
                print(f"  PDFs in first case: {len(pdfs)}")
                if pdfs:
                    time.sleep(2)
                    text = scraper._extract_pdf_text(pdfs[0]["pdf_url"])
                    if text:
                        print(f"  PDF text extraction: OK ({len(text)} chars)")
                    else:
                        print("  PDF text extraction: FAILED")
        except Exception as e:
            print(f"FAIL: {e}")
            import traceback
            traceback.print_exc()
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample)
        print(json.dumps(stats, indent=2))
    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
