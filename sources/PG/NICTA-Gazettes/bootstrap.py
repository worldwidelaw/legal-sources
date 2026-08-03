#!/usr/bin/env python3
"""
PG/NICTA-Gazettes -- Papua New Guinea National ICT Authority regulatory instruments

Fetches PNG telecom/ICT legislation and regulatory instruments published by
NICTA (nicta.gov.pg). The site is WordPress; legal instruments live in the
"legislative" / "regulatory" category tree and are exposed via the WP REST API.
Each post embeds the instrument as a PDF (Adobe `data-media-url` block or a
direct .pdf link), which we download and extract full text from with pdfplumber.

Covered instrument types:
  - The NICT Act 2009 (primary statute), Cybercrime Code Act 2016
  - Operator Licensing / Radio Spectrum / SIM Card Registration Regulations
  - QoS, Type Approval, Reference Interconnection, Licence Conditions Rules
  - Wholesale Service Declarations, National Numbering Plan
  - Consumer Complaints Management Guideline, Cybercrime Policy
  - National Gazette notices

Some older instruments are scanned image-only PDFs with no text layer; those
are skipped (insufficient extractable text). ~11 instruments yield full text.

Strategy:
  1. Query the WP REST API for posts in the legal-instrument categories
  2. Extract the embedded PDF URL from each post's content HTML
  3. Download the PDF, extract text with pdfplumber
  4. Skip documents with no extractable text layer (scanned PDFs)
  5. 1-second delay between requests

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py bootstrap-fast     # Alias for bootstrap
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html
import hashlib
import logging
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PG.NICTA-Gazettes")

BASE_URL = "https://www.nicta.gov.pg"
API_URL = f"{BASE_URL}/wp-json/wp/v2/posts"
SAMPLE_DIR = Path(__file__).parent / "sample"
SOURCE_ID = "PG/NICTA-Gazettes"

# WordPress category IDs holding legal/regulatory instruments on nicta.gov.pg.
# legislative=104, regulatory=61, acts=105, regulations=109, rules=93,
# declarations=106, gazettes=107, guidelines=63, policies=108, numbering=79
LEGAL_CATEGORIES = "104,61,105,109,93,106,107,63,108,79"

HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

CRAWL_DELAY = 1
# Minimum extractable characters to consider a PDF to have a real text layer.
MIN_TEXT_CHARS = 600


def _clean_title(raw_title: str) -> str:
    """Strip HTML tags and decode entities from a WP rendered title."""
    title = re.sub(r"<[^>]+>", "", raw_title)
    title = html.unescape(title)
    # Collapse leftover smart-quote/bracket noise and whitespace.
    title = re.sub(r"\s+", " ", title).strip()
    return title


class NICTAGazettesScraper(BaseScraper):
    """Scraper for PG/NICTA-Gazettes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get_posts(self) -> List[dict]:
        """Fetch all legal-instrument posts via the WP REST API."""
        time.sleep(CRAWL_DELAY)
        params = {
            "categories": LEGAL_CATEGORIES,
            "per_page": 100,
            "_fields": "id,date,title,link,content",
        }
        try:
            resp = self.session.get(API_URL, params=params, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, ValueError) as e:
            logger.error(f"Failed to fetch posts: {e}")
            return []

    @staticmethod
    def _extract_pdf_url(content_html: str) -> Optional[str]:
        """Find the embedded PDF URL inside a post's content HTML."""
        # Adobe PDF-embed block (tropicalista-pdfembed) stores it here.
        m = re.search(r'data-media-url=["\']([^"\']+\.pdf)["\']', content_html, re.I)
        if m:
            return html.unescape(m.group(1).strip())
        # Fallback: any direct .pdf hyperlink.
        m = re.search(r'href=["\']([^"\']+\.pdf)["\']', content_html, re.I)
        if m:
            return html.unescape(m.group(1).strip())
        return None

    def _download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """Download a PDF file."""
        time.sleep(CRAWL_DELAY)
        try:
            resp = self.session.get(pdf_url, timeout=180)
            resp.raise_for_status()
            if len(resp.content) < 100:
                return None
            return resp.content
        except requests.RequestException as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

    def _extract_text_from_pdf_bytes(self, pdf_bytes: bytes) -> Optional[str]:
        """Extract text from PDF bytes via the shared extractor.

        Delegates to common.pdf_extract.extract_pdf_markdown, which cascades
        text-layer backends (opendataloader → pdfplumber → pypdf) and then falls
        back to Tesseract image-OCR for scanned image-only gazettes (issue
        #1133). OCR is gated on the tesseract binary being present; where it is
        absent the cascade behaves exactly like the previous pdfplumber path.
        """
        source_id = hashlib.md5(pdf_bytes[:4096]).hexdigest()[:16]
        try:
            text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=source_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
                force=True,
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return None
        return text or None

    def normalize(self, raw: dict) -> dict:
        """Transform raw document into standard schema."""
        return {
            "_id": f"PG-NICTA-{raw['doc_id']}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date", ""),
            "url": raw["url"],
            "pdf_url": raw["pdf_url"],
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield all documents with full text."""
        posts = self._get_posts()
        logger.info(f"Found {len(posts)} candidate posts in legal categories")

        # Deduplicate by PDF URL — a post can sit in several categories.
        seen_pdf = set()
        total_yielded = 0
        skipped_scanned = 0

        for post in posts:
            title = _clean_title(post.get("title", {}).get("rendered", ""))
            content = post.get("content", {}).get("rendered", "")
            pdf_url = self._extract_pdf_url(content)

            if not pdf_url:
                logger.info(f"  No PDF for: {title[:60]}")
                continue
            if pdf_url in seen_pdf:
                continue
            seen_pdf.add(pdf_url)

            logger.info(f"  [{total_yielded + 1}] {title[:60]}")
            logger.info(f"      PDF: {pdf_url[:90]}")

            pdf_bytes = self._download_pdf(pdf_url)
            if not pdf_bytes:
                logger.warning("      Skipping — could not download PDF")
                continue

            text = self._extract_text_from_pdf_bytes(pdf_bytes)
            if not text or len(text) < MIN_TEXT_CHARS:
                logger.warning(
                    f"      Skipping — scanned/no text layer "
                    f"({len(text) if text else 0} chars)"
                )
                skipped_scanned += 1
                continue

            logger.info(f"      Extracted {len(text)} chars")

            doc_date = (post.get("date") or "")[:10]
            raw = {
                "doc_id": hashlib.md5(pdf_url.encode()).hexdigest()[:10],
                "title": title,
                "text": text,
                "date": doc_date,
                "url": post.get("link") or pdf_url,
                "pdf_url": pdf_url,
            }
            record = self.normalize(raw)

            if sample:
                SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
                sample_path = SAMPLE_DIR / f"{record['_id']}.json"
                with open(sample_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False)

            yield record
            total_yielded += 1

        logger.info(
            f"\nTotal documents yielded: {total_yielded} "
            f"({skipped_scanned} skipped as scanned/no-text)"
        )

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        """Re-fetch all — small static collection, no incremental API."""
        yield from self.fetch_all(sample=False)

    def test_connection(self) -> bool:
        """Quick connectivity test against the WP REST API."""
        try:
            resp = self.session.get(
                API_URL,
                params={"categories": LEGAL_CATEGORIES, "per_page": 1, "_fields": "id"},
                timeout=30,
            )
            if resp.status_code == 200 and isinstance(resp.json(), list):
                logger.info("Connection test PASSED")
                return True
            logger.error(f"Connection test FAILED: status {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Connection test FAILED: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PG/NICTA-Gazettes scraper")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Only fetch sample records")
    parser.add_argument("--full", action="store_true", help="Full fetch")
    args = parser.parse_args()

    scraper = NICTAGazettesScraper()

    if args.command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        sample = args.sample and not args.full
        count = 0
        for record in scraper.fetch_all(sample=sample):
            count += 1
            logger.info(f"Record {count}: {record['_id']} — {record['title'][:50]}")
        logger.info(f"Bootstrap complete: {count} records")

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
