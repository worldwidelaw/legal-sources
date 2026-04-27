#!/usr/bin/env python3
"""
CA/NWTLaws -- Northwest Territories Consolidated Legislation

Fetches consolidated Acts and Regulations of the Northwest Territories from
justice.gov.nt.ca. All documents are PDFs linked from a single listing page.

URL pattern:
  https://www.justice.gov.nt.ca/en/files/legislation/{slug}/{slug}.a.pdf     (Act)
  https://www.justice.gov.nt.ca/en/files/legislation/{slug}/{slug}.r1.pdf    (Regulation)

Strategy:
  1. Fetch the single listing page at /en/legislation/
  2. Extract all PDF links matching /en/files/legislation/*.pdf
  3. Download PDFs and extract full text via common/pdf_extract

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CA.NWTLaws")

BASE_URL = "https://www.justice.gov.nt.ca"
LEGISLATION_URL = "/en/legislation/"


def _slug_to_title(slug: str) -> str:
    """Convert a URL slug to a human-readable title."""
    return slug.replace('-', ' ').replace('_', ' ').title()


def _classify_doc(filename: str) -> str:
    """Classify as 'act' or 'regulation' based on filename pattern."""
    if '.a.pdf' in filename:
        return 'act'
    if re.search(r'\.r\d*\.pdf', filename):
        return 'regulation'
    return 'unknown'


class NWTLawsScraper(BaseScraper):
    """Scraper for CA/NWTLaws -- Northwest Territories Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
            timeout=120,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        return extract_pdf_markdown(
            source="CA/NWTLaws",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="legislation",
        ) or ""

    def _fetch_listing(self) -> List[Dict[str, Any]]:
        """Fetch the legislation listing page and extract all PDF links."""
        self.rate_limiter.wait()
        resp = self.client.get(LEGISLATION_URL)
        resp.raise_for_status()

        # Extract PDF links from listing page
        pdf_links = re.findall(
            r'href="([^"]*?/en/files/legislation/[^"]*?\.pdf)"',
            resp.text,
            re.IGNORECASE,
        )

        docs = []
        seen_urls = set()
        for href in pdf_links:
            # Make absolute
            if href.startswith('/'):
                url = BASE_URL + href
            elif href.startswith('http'):
                url = href
            else:
                url = BASE_URL + '/' + href

            if url in seen_urls:
                continue
            seen_urls.add(url)

            # Parse slug and filename
            match = re.search(r'/legislation/([^/]+)/([^/]+\.pdf)', url)
            if not match:
                continue

            slug = match.group(1)
            filename = match.group(2)
            doc_type = _classify_doc(filename)

            # Skip admin files
            if 'administering-departments' in slug:
                continue

            title = _slug_to_title(slug)
            if doc_type == 'regulation':
                reg_num = re.search(r'\.r(\d+)\.pdf', filename)
                suffix = f" Regulation {reg_num.group(1)}" if reg_num else " Regulations"
                title += suffix

            doc_id = hashlib.md5(url.encode()).hexdigest()[:12]

            docs.append({
                "doc_id": doc_id,
                "title": title,
                "slug": slug,
                "doc_type": doc_type,
                "file_url": url,
                "page_url": BASE_URL + LEGISLATION_URL,
            })

        logger.info(f"Found {len(docs)} legislation PDFs")
        acts = sum(1 for d in docs if d['doc_type'] == 'act')
        regs = sum(1 for d in docs if d['doc_type'] == 'regulation')
        logger.info(f"  Acts: {acts}, Regulations: {regs}")
        return docs

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"CA/NWTLaws/{raw['doc_id']}",
            "_source": "CA/NWTLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_prefetched_text", ""),
            "date": "",
            "url": raw.get("file_url", ""),
            "doc_id": raw["doc_id"],
            "doc_type": raw.get("doc_type", ""),
            "slug": raw.get("slug", ""),
            "file_url": raw.get("file_url", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        all_docs = self._fetch_listing()
        limit = 15 if sample else None
        count = 0

        for doc in all_docs:
            if limit and count >= limit:
                break

            file_url = doc.get("file_url", "")
            if not file_url:
                continue

            try:
                self.rate_limiter.wait()
                resp = self.client.get(file_url)
                resp.raise_for_status()
                pdf_bytes = resp.content
            except Exception as e:
                logger.warning(f"  Failed to download {file_url}: {e}")
                continue

            text = self._extract_pdf_text(pdf_bytes)
            if not text or len(text) < 50:
                logger.warning(f"  Skipping {doc['title'][:60]} - no/short text")
                continue

            doc["_prefetched_text"] = text
            yield doc
            count += 1
            logger.info(f"  [{count}] {doc['title'][:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for doc in self.fetch_all():
            yield doc


if __name__ == "__main__":
    scraper = NWTLawsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing NWT legislation listing...")
        docs = scraper._fetch_listing()
        if docs:
            print(f"Connection OK. Found {len(docs)} legislation documents.")
            acts = sum(1 for d in docs if d['doc_type'] == 'act')
            regs = sum(1 for d in docs if d['doc_type'] == 'regulation')
            print(f"  Acts: {acts}, Regulations: {regs}")
            print(f"Sample: {docs[0]['title']} -> {docs[0]['file_url'][:80]}")
        else:
            print("Connection FAILED - no documents found")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
