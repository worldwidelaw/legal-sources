#!/usr/bin/env python3
"""
TL/BCTL -- Central Bank of Timor-Leste Regulations & Circulars

Fetches regulatory instruments from Banco Central de Timor-Leste (BCTL):
regulations, instructions, circulars, orders, rules, and other instruments.

Strategy:
  - Scrape HTML listing pages for each category on bancocentral.tl
  - Each page contains links to PDF documents
  - Download PDFs and extract full text via pdfplumber

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TL.BCTL")

BASE_URL = "https://www.bancocentral.tl/"

# Categories to scrape: (slug, category_label)
CATEGORIES = [
    ("en/go/regulation", "regulation"),
    ("en/go/instruction", "instruction"),
    ("en/go/circular", "circular"),
    ("en/go/order", "order"),
    ("en/go/rules", "rules"),
    ("en/go/other-regulatory-instruments", "other"),
]


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
        pages_text = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        return "\n\n".join(pages_text)
    except Exception as e:
        logger.warning(f"pdfplumber extraction failed: {e}")
        return ""


def _extract_date_from_title(title: str) -> str:
    """Try to extract a year-based date from the document title."""
    # Look for patterns like "No. 31/2025", "N.º 4/2018", "#012 2026"
    m = re.search(r'[/#]\s*\d+[/-]?\s*(\d{4})\b', title)
    if m:
        return f"{m.group(1)}-01-01"
    # Look for standalone 4-digit year
    m = re.search(r'\b(20\d{2}|19\d{2})\b', title)
    if m:
        return f"{m.group(1)}-01-01"
    return ""


def _clean_title(raw_text: str) -> str:
    """Clean up a raw title string."""
    text = re.sub(r'\s+', ' ', raw_text).strip()
    # Remove leading/trailing dashes and whitespace
    text = text.strip(' -–—')
    return text


class BCTLScraper(BaseScraper):
    """Scraper for TL/BCTL -- Central Bank of Timor-Leste regulatory instruments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/pdf",
            },
            timeout=120,
        )
        self._seen_urls: set = set()

    def _scrape_category(self, rel_url: str, category: str) -> List[Dict[str, Any]]:
        """Scrape a category listing page for PDF document links."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("beautifulsoup4 not installed")
            return []

        full_url = urljoin(BASE_URL, rel_url)
        self.rate_limiter.wait()

        try:
            resp = self.client.session.get(full_url, timeout=60)
        except Exception as e:
            logger.warning(f"Failed to fetch {full_url}: {e}")
            return []

        if resp.status_code != 200:
            logger.warning(f"HTTP {resp.status_code} for {full_url}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        docs = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if not href.lower().endswith(".pdf"):
                continue

            # Resolve to absolute URL
            if href.startswith("http"):
                pdf_url = href
            elif href.startswith("/"):
                pdf_url = urljoin(BASE_URL, href)
            else:
                pdf_url = urljoin(full_url, href)

            if pdf_url in self._seen_urls:
                continue

            link_text = a_tag.get_text(strip=True)
            if not link_text:
                continue

            self._seen_urls.add(pdf_url)
            title = _clean_title(link_text)
            date = _extract_date_from_title(title)

            docs.append({
                "title": title,
                "date": date,
                "pdf_url": pdf_url,
                "category": category,
            })

        logger.info(f"Category '{category}': {len(docs)} documents found")
        return docs

    def _fetch_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.session.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {pdf_url}")
                return None
            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None

            text = _extract_pdf_text(resp.content)
            return text if text else None
        except Exception as e:
            logger.warning(f"Failed to fetch PDF {pdf_url}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all BCTL regulatory instruments."""
        all_docs = []

        for rel_url, category in CATEGORIES:
            docs = self._scrape_category(rel_url, category)
            all_docs.extend(docs)

        logger.info(f"Total documents discovered: {len(all_docs)}")

        for doc in all_docs:
            # Build stable ID from PDF filename
            filename = doc["pdf_url"].rsplit("/", 1)[-1]
            # Remove .pdf extension and use as part of ID
            file_stem = re.sub(r'\.pdf$', '', filename, flags=re.IGNORECASE)
            doc_id = f"TL-BCTL-{doc['category']}-{file_stem}"

            text = self._fetch_pdf_text(doc["pdf_url"])
            if not text or len(text) < 50:
                logger.warning(f"Skipping {doc_id}: insufficient text ({len(text) if text else 0} chars)")
                continue

            yield {
                "doc_id": doc_id,
                "title": doc["title"],
                "date": doc["date"],
                "pdf_url": doc["pdf_url"],
                "category": doc["category"],
                "full_text": text,
            }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all — no date-based filtering available."""
        yield from self.fetch_all()

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing TL/BCTL endpoints...")

        print("\n1. Testing instruction listing...")
        try:
            resp = self.client.session.get(
                BASE_URL + "en/go/instruction", timeout=30
            )
            print(f"   Status: {resp.status_code}, {len(resp.text)} chars")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\n2. Testing PDF download + text extraction...")
        try:
            pdf_url = BASE_URL + "uploads/documentos/documento_1766386574_9887.pdf"
            resp = self.client.session.get(pdf_url, timeout=30)
            print(f"   PDF status: {resp.status_code}, {len(resp.content)} bytes")
            if resp.status_code == 200:
                text = _extract_pdf_text(resp.content)
                print(f"   Extracted: {len(text)} chars")
                if text:
                    print(f"   Sample: {text[:200]}")
        except Exception as e:
            print(f"   ERROR: {e}")
        print("\nTest complete!")

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", "")
        category = raw.get("category", "")
        full_text = raw.get("full_text", "")
        date = raw.get("date", "")
        pdf_url = raw.get("pdf_url", "")

        # Extract document number from title
        doc_number = ""
        m = re.search(r'(?:No\.?|N\.?º|#)\s*(\d+[\s/.-]*\d*)', title, re.IGNORECASE)
        if m:
            doc_number = m.group(1).strip()

        display_title = f"[BCTL {category.title()}] {title}" if title else doc_id

        return {
            "_id": doc_id,
            "_source": "TL/BCTL",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": display_title,
            "text": full_text,
            "date": date,
            "url": pdf_url,
            "doc_number": doc_number,
            "category": category,
            "language": "en",
        }


def main():
    scraper = BCTLScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
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
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
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
