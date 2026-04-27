#!/usr/bin/env python3
"""
ET/FSC -- Ethiopian Federal Supreme Court Digital Law Library

Fetches legislation (Federal Laws) and case law (Cassation Decisions,
Supreme Court Decisions) from the official FSC website at fsc.gov.et.

The site is built on DotNetNuke with EasyDNNNews module. Each section has
paginated listings. Detail pages provide JSON-LD metadata and PDF download
links via DocumentDownload.ashx endpoint.

Sections:
  - Federal Laws: PgrID=1179, ~161 pages (legislation)
  - Cassation Decisions: PgrID=943, ~37 pages (case_law)
  - Supreme Court Decisions: PgrID=1211, ~3 pages (case_law)

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap (no incremental API)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ET.FSC")

BASE_URL = "https://www.fsc.gov.et"

# Section definitions: (path, PgrID, max_pages, data_type)
SECTIONS = [
    ("Digital-Law-Library/Federal-Laws", 1179, 161, "legislation"),
    ("Digital-Law-Library/Judgments/Cassation-Decisions", 943, 37, "case_law"),
    ("Digital-Law-Library/Judgments/Supreme-Court-Decisions", 1211, 5, "case_law"),
]


class EthiopiaFSCScraper(BaseScraper):
    """
    Scraper for ET/FSC -- Ethiopian Federal Supreme Court Digital Law Library.
    Country: ET
    URL: https://www.fsc.gov.et/Digital-Law-Library/Federal-Laws
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        self.session.verify = False

    def _get(self, url: str, binary: bool = False):
        """Fetch URL with rate limiting."""
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120)
        resp.raise_for_status()
        return resp.content if binary else resp.text

    def _collect_entries_from_listing(self, section_path: str, pgr_id: int,
                                      max_pages: int, sample: bool = False) -> list:
        """Paginate a section's listing pages and collect detail page URLs."""
        entries = []
        pages_to_fetch = 3 if sample else max_pages

        for page_num in range(1, pages_to_fetch + 1):
            url = f"{BASE_URL}/{section_path}/PgrID/{pgr_id}/PageID/{page_num}"
            logger.info(f"Fetching listing page {page_num}/{pages_to_fetch}: {section_path}")

            try:
                html = self._get(url)
            except Exception as e:
                logger.error(f"Failed to fetch page {page_num}: {e}")
                break

            # Extract article title links
            title_links = re.findall(
                r'class="[^"]*edn_articleTitle[^"]*"[^>]*>.*?<a[^>]*href="([^"]+)"[^>]*>(.*?)</a>',
                html, re.DOTALL
            )

            if not title_links:
                logger.info(f"No entries on page {page_num}, stopping pagination")
                break

            for href, title_html in title_links:
                title = re.sub(r'<[^>]+>', '', title_html).strip()
                detail_url = href if href.startswith('http') else urljoin(BASE_URL + '/', href)
                entries.append({
                    "title": title,
                    "detail_url": detail_url,
                })

            logger.info(f"  Found {len(title_links)} entries (total: {len(entries)})")

            # Stop early for sample
            if sample and len(entries) >= 15:
                break

        return entries

    def _fetch_detail(self, detail_url: str) -> dict:
        """Fetch a detail page and extract metadata + PDF download link."""
        html = self._get(detail_url)

        # Extract JSON-LD for metadata
        date_published = None
        headline = None
        jsonld_match = re.search(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html, re.DOTALL
        )
        if jsonld_match:
            try:
                data = json.loads(jsonld_match.group(1), strict=False)
                date_published = data.get("datePublished", "")
                if date_published:
                    date_published = date_published[:10]  # YYYY-MM-DD
                headline = data.get("headline", "")
            except (json.JSONDecodeError, ValueError):
                pass

        # Extract PDF download link
        pdf_match = re.search(
            r'DocumentDownload\.ashx\?portalid=\d+&moduleid=\d+&articleid=(\d+)&documentid=(\d+)',
            html
        )
        pdf_url = None
        article_id = None
        if pdf_match:
            pdf_url = urljoin(BASE_URL + '/', '/DesktopModules/EasyDNNNews/' + pdf_match.group(0))
            article_id = pdf_match.group(1)

        return {
            "date": date_published,
            "headline": headline,
            "pdf_url": pdf_url,
            "article_id": article_id,
        }

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into standard schema."""
        article_id = raw.get("article_id", "")
        data_type = raw.get("data_type", "legislation")
        prefix = "ET-FSC-L" if data_type == "legislation" else "ET-FSC-J"
        _id = f"{prefix}-{article_id}" if article_id else f"{prefix}-{hash(raw.get('url', ''))}"

        return {
            "_id": _id,
            "_source": "ET/FSC",
            "_type": data_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "section": raw.get("section", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from all sections."""
        yield from self._fetch_documents(sample=False)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield all documents (no incremental API available)."""
        yield from self._fetch_documents(sample=False)

    def _fetch_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Core fetcher: iterate sections, collect entries, download PDFs."""
        total_count = 0
        total_failures = 0

        for section_path, pgr_id, max_pages, data_type in SECTIONS:
            section_name = section_path.split("/")[-1]
            logger.info(f"=== Processing section: {section_name} (type={data_type}) ===")

            entries = self._collect_entries_from_listing(
                section_path, pgr_id, max_pages, sample=sample
            )

            if sample:
                # Take ~5 from each section for a good sample
                entries = entries[:5]

            section_count = 0
            section_failures = 0

            for entry in entries:
                try:
                    detail = self._fetch_detail(entry["detail_url"])
                except Exception as e:
                    logger.warning(f"Failed to fetch detail for '{entry['title'][:40]}': {e}")
                    section_failures += 1
                    continue

                if not detail["pdf_url"]:
                    logger.warning(f"No PDF for '{entry['title'][:40]}'")
                    section_failures += 1
                    continue

                # Download PDF
                try:
                    pdf_bytes = self._get(detail["pdf_url"], binary=True)
                except Exception as e:
                    logger.warning(f"Failed to download PDF for '{entry['title'][:40]}': {e}")
                    section_failures += 1
                    continue

                if len(pdf_bytes) < 500:
                    logger.warning(f"PDF too small ({len(pdf_bytes)} bytes): '{entry['title'][:40]}'")
                    section_failures += 1
                    continue

                # Extract text
                text = extract_pdf_markdown(
                    source="ET/FSC",
                    source_id=detail["article_id"] or "unknown",
                    pdf_bytes=pdf_bytes,
                    table=data_type,
                ) or ""

                if len(text) < 50:
                    logger.warning(f"Insufficient text ({len(text)} chars): '{entry['title'][:40]}'")
                    section_failures += 1
                    continue

                title = detail.get("headline") or entry.get("title", "")
                raw = {
                    "title": title,
                    "text": text,
                    "date": detail.get("date"),
                    "url": entry["detail_url"],
                    "article_id": detail["article_id"],
                    "data_type": data_type,
                    "section": section_name,
                }

                record = self.normalize(raw)
                section_count += 1
                total_count += 1
                logger.info(f"[{total_count}] {section_name}: {title[:50]} ({len(text)} chars)")
                yield record

            total_failures += section_failures
            logger.info(f"Section {section_name}: {section_count} records, {section_failures} failures")

        logger.info(f"TOTAL: {total_count} records, {total_failures} failures")

    def test_api(self):
        """Quick connectivity and extraction test."""
        logger.info("Testing fsc.gov.et connectivity...")

        # Test listing page
        url = f"{BASE_URL}/Digital-Law-Library/Federal-Laws/PgrID/1179/PageID/1"
        html = self._get(url)
        title_links = re.findall(
            r'class="[^"]*edn_articleTitle[^"]*"[^>]*>.*?<a[^>]*href="([^"]+)"',
            html, re.DOTALL
        )
        logger.info(f"OK: listing page has {len(title_links)} entries")

        if title_links:
            detail_url = title_links[0]
            if not detail_url.startswith('http'):
                detail_url = urljoin(BASE_URL + '/', detail_url)
            detail = self._fetch_detail(detail_url)
            logger.info(f"OK: detail page headline='{detail.get('headline', '')[:50]}'")

            if detail["pdf_url"]:
                pdf_bytes = self._get(detail["pdf_url"], binary=True)
                text = extract_pdf_markdown(
                    source="ET/FSC", source_id="test",
                    pdf_bytes=pdf_bytes, table="legislation"
                )
                logger.info(f"OK: extracted {len(text)} chars from PDF")
            else:
                logger.warning("No PDF link found on detail page")


def main():
    scraper = EthiopiaFSCScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper._fetch_documents(sample=sample):
            safe_id = re.sub(r'[^\w\-]', '_', record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info(f"Saved {count} records to {sample_dir}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
