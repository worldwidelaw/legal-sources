#!/usr/bin/env python3
"""
LR/LRA-Laws -- Liberia Revenue Authority Tax and Customs Laws

Fetches tax legislation, customs codes, executive orders, and administrative
regulations from the Liberia Revenue Authority (revenue.lra.gov.lr).

Documents are published as PDFs on a single listing page organized by category:
  - Revenue Code & Amendments (~8)
  - Customs Tariffs (~7)
  - Executive Orders (~13)
  - Administrative Regulations (~16)
  - Related Laws & Guidelines (~20+)

Strategy:
  - Parse HTML listing page for PDF links
  - Download each PDF and extract text with pdfplumber
  - ~65 documents total, all English language

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html.parser import HTMLParser
from urllib.parse import urljoin

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.LR.LRA-Laws")

BASE_URL = "https://revenue.lra.gov.lr"
LISTING_URL = f"{BASE_URL}/laws-issuances/"
SOURCE_ID = "LR/LRA-Laws"


class _LinkParser(HTMLParser):
    """Parse the LRA laws page to extract PDF links and titles."""

    def __init__(self):
        super().__init__()
        self.links: List[Dict[str, str]] = []
        self._in_a = False
        self._href = ""
        self._text = ""

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            d = dict(attrs)
            href = d.get("href", "")
            if href.endswith(".pdf"):
                self._in_a = True
                self._href = href.strip()
                self._text = ""

    def handle_data(self, data):
        if self._in_a:
            self._text += data

    def handle_endtag(self, tag):
        if tag == "a" and self._in_a:
            self._in_a = False
            title = self._text.strip()
            href = self._href
            if title and href:
                if not href.startswith("http"):
                    href = urljoin(BASE_URL, href)
                self.links.append({"title": title, "url": href})


def _categorize(title: str, url: str) -> str:
    """Determine document category from title and URL."""
    t = title.upper()
    u = url.upper()
    if "EXECUTIVE ORDER" in t:
        return "executive_order"
    if "TARIFF" in t or "HS 20" in t or "CET" in t:
        return "customs_tariff"
    if "REVENUE CODE" in t or "REVENUE-CODE" in u:
        return "revenue_code"
    if "REGULATION" in t:
        return "regulation"
    if "EXCISE" in t:
        return "excise"
    if "ACT" in t:
        return "act"
    if "PUBLIC RULING" in t or "GUIDELINE" in t or "BROCHURE" in t:
        return "guidance"
    return "other"


def _extract_year(title: str, url: str) -> Optional[str]:
    """Try to extract a year from title or URL."""
    for text in [title, url]:
        years = re.findall(r"((?:19|20)\d{2})", text)
        if years:
            return years[-1]
    return None


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        pdf = pdfplumber.open(io.BytesIO(content))
        pages_text = []
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages_text.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        pdf.close()
        return "\n\n".join(pages_text)
    except Exception as e:
        logger.warning("PDF extraction failed: %s", e)
        return ""


class LRALawsScraper(BaseScraper):
    def __init__(self):
        super().__init__(str(Path(__file__).resolve().parent))
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research)"
        })

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        logger.info("Fetching listing page: %s", LISTING_URL)
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()

        parser = _LinkParser()
        parser.feed(resp.text)
        logger.info("Found %d PDF links", len(parser.links))

        seen_urls = set()
        for doc in parser.links:
            url = doc["url"]
            if url in seen_urls:
                logger.info("Skipping duplicate URL: %s", url[:80])
                continue
            seen_urls.add(url)

            title = doc["title"]
            logger.info("Downloading: %s", title[:80])

            time.sleep(1.5)

            try:
                pdf_resp = self.session.get(url, timeout=60)
                pdf_resp.raise_for_status()
            except Exception as e:
                logger.warning("Failed to download %s: %s", url[:80], e)
                continue

            content_type = pdf_resp.headers.get("Content-Type", "")
            if "pdf" not in content_type and not url.endswith(".pdf"):
                logger.warning("Skipping non-PDF: %s (content-type: %s)", url[:80], content_type)
                continue

            text = _extract_pdf_text(pdf_resp.content)
            if not text or len(text) < 50:
                logger.warning("Insufficient text from %s (%d chars)", title[:60], len(text))
                continue

            year = _extract_year(title, url)
            category = _categorize(title, url)
            doc_id = hashlib.sha256(url.encode()).hexdigest()[:16]

            yield {
                "id": doc_id,
                "title": title,
                "url": url,
                "text": text,
                "year": year,
                "category": category,
                "pdf_size": len(pdf_resp.content),
            }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        year = raw.get("year")
        date = f"{year}-01-01" if year else None

        return {
            "_id": raw["id"],
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": date,
            "url": raw["url"],
            "category": raw.get("category", "other"),
            "pdf_size": raw.get("pdf_size"),
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = LRALawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
