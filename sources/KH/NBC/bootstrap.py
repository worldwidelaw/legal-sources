#!/usr/bin/env python3
"""
KH/NBC -- Cambodia National Bank of Cambodia Prakas & Circulars

Fetches banking regulations (prakas, circulars, laws) from the National Bank
of Cambodia English legislation pages. Documents are PDFs with extractable text.

Pages scraped:
  - Prakas & Circulars: ~163 documents
  - Laws: ~10 documents

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
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
from urllib.parse import urljoin

import requests
import pdfplumber
import io

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KH.NBC")

BASE_URL = "https://www.nbc.gov.kh"
PAGES = [
    ("prakas", f"{BASE_URL}/english/legislation/prakas_new.php"),
    ("laws", f"{BASE_URL}/english/legislation/laws_applicable_to_banks_and_financial_institutions.php"),
]


class NBCScraper(BaseScraper):
    """
    Scraper for KH/NBC -- Cambodia National Bank Prakas & Circulars.
    Country: KH
    URL: https://www.nbc.gov.kh/english/legislation/prakas_new.php

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data research project)",
        })

    def _resolve_url(self, href: str, page_url: str) -> str:
        """Resolve relative URLs to absolute."""
        if href.startswith("http"):
            return href
        if href.startswith("../../"):
            return f"{BASE_URL}/{href.replace('../../', '')}"
        return urljoin(page_url, href)

    def _parse_date_from_title(self, title: str) -> Optional[str]:
        """Extract date from title string like 'Title,March 20, 2026'."""
        # Pattern: month day, year at end of title
        match = re.search(
            r',\s*(January|February|March|April|May|June|July|August|'
            r'September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})\s*$',
            title
        )
        if match:
            month_str, day, year = match.group(1), match.group(2), match.group(3)
            months = {
                'January': '01', 'February': '02', 'March': '03', 'April': '04',
                'May': '05', 'June': '06', 'July': '07', 'August': '08',
                'September': '09', 'October': '10', 'November': '11', 'December': '12'
            }
            return f"{year}-{months[month_str]}-{int(day):02d}"
        return None

    def _clean_title(self, title: str) -> str:
        """Remove trailing date from title string."""
        cleaned = re.sub(
            r',\s*(January|February|March|April|May|June|July|August|'
            r'September|October|November|December)\s+\d{1,2},?\s+\d{4}\s*$',
            '', title
        )
        return cleaned.strip()

    def _get_document_list(self) -> list[dict]:
        """Scrape all PDF links from NBC legislation pages."""
        from bs4 import BeautifulSoup

        documents = []
        seen_urls = set()

        for category, page_url in PAGES:
            try:
                resp = self.session.get(page_url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch {page_url}: {e}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.find_all("a", href=True)

            for link in links:
                href = link["href"]
                if ".pdf" not in href.lower():
                    continue

                pdf_url = self._resolve_url(href, page_url)
                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                raw_title = link.get_text(strip=True)
                if not raw_title or len(raw_title) < 5:
                    continue

                date = self._parse_date_from_title(raw_title)
                title = self._clean_title(raw_title)

                documents.append({
                    "title": title,
                    "date": date,
                    "pdf_url": pdf_url,
                    "category": category,
                })

        logger.info(f"Found {len(documents)} documents across {len(PAGES)} pages")
        return documents

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=60)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

        if len(resp.content) < 500:
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                # Release per-page layout + cached textmap to cap peak RSS
                # on large PDFs (prevents OOM exit 137 on the fleet).
                page.flush_cache()
                try:
                    page.get_textmap.cache_clear()
                except AttributeError:
                    pass
            pdf.close()
            full_text = "\n\n".join(pages_text)
            return full_text if len(full_text) >= 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 50:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        # Generate stable ID from PDF URL
        url_hash = hashlib.md5(raw["pdf_url"].encode()).hexdigest()[:12]
        doc_id = f"KH-NBC-{url_hash}"

        return {
            "_id": doc_id,
            "_source": "KH/NBC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "category": raw.get("category", "prakas"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all NBC legislation documents with full PDF text."""
        documents = self._get_document_list()
        logger.info(f"Processing {len(documents)} documents")

        yielded = 0
        skipped = 0

        for i, doc in enumerate(documents):
            if (i + 1) % 20 == 0:
                logger.info(f"[{i+1}/{len(documents)}] Yielded: {yielded}, Skipped: {skipped}")

            text = self._extract_pdf_text(doc["pdf_url"])
            if not text:
                skipped += 1
                continue

            doc["text"] = text
            normalized = self.normalize(doc)
            if normalized:
                yielded += 1
                yield normalized

            time.sleep(1.5)  # Rate limit

        logger.info(f"Done. Yielded: {yielded}, Skipped: {skipped}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recently added documents."""
        # NBC doesn't have a reliable date-sorted API, re-fetch all
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        resp = self.session.get(PAGES[0][1], timeout=30)
        return {
            "status": "ok" if resp.status_code == 200 else "error",
            "http_code": resp.status_code,
            "url": PAGES[0][1],
        }


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = NBCScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        result = scraper.test()
        print(json.dumps(result, indent=2))
    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 12 if sample_mode else 99999

        gen = scraper.fetch_all() if command == "bootstrap" else scraper.fetch_updates()

        for record in gen:
            count += 1
            if sample_mode:
                outpath = sample_dir / f"{count:04d}.json"
                outpath.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                print(f"[{count}] {record['title'][:60]} ({len(record['text'])} chars)")
            else:
                print(json.dumps(record, ensure_ascii=False))

            if count >= limit:
                break

        print(f"\nTotal records: {count}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
