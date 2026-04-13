#!/usr/bin/env python3
"""
ET/FederalCourts -- Ethiopian Federal Supreme Court Cassation Decisions

Fetches unpublished cassation decisions from the Federal Supreme Court of Ethiopia
(fsc.gov.et). Each decision has a listing entry and an individual page with a
downloadable PDF containing the full Amharic text.

Strategy:
  - Paginate the EasyDNNNews listing at /Digital-Law-Library/Judgments/PgrID/814/
    (37 pages, 5 decisions per page, ~185 decisions total)
  - For each decision, fetch the individual page to get the PDF download URL
  - Download and extract text from each PDF using PyPDF2
  - Normalize into standard schema with full text

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ET.FederalCourts")

BASE_URL = "https://www.fsc.gov.et"
LISTING_URL = f"{BASE_URL}/Digital-Law-Library/Judgments/PgrID/814/PageID/{{page}}"
MAX_PAGES = 37


class EthiopiaFederalCourtsScraper(BaseScraper):
    """
    Scraper for ET/FederalCourts -- Ethiopian Federal Supreme Court.
    Country: ET
    URL: https://www.fsc.gov.et/
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "text/html,application/xhtml+xml,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=120,
        )

    def _get_page(self, url: str) -> str:
        """Fetch a page respecting crawl delay."""
        self.rate_limiter.wait()
        resp = self.client.get(url, allow_redirects=True)
        resp.raise_for_status()
        return resp.text

    def _get_binary(self, url: str) -> bytes:
        """Fetch binary content (PDF) respecting crawl delay."""
        self.rate_limiter.wait()
        resp = self.client.get(url, allow_redirects=True)
        resp.raise_for_status()
        return resp.content

    def _extract_listing_entries(self, html: str) -> list:
        """Extract decision entries from a listing page."""
        entries = []
        blocks = re.findall(
            r'class="edn_article edn_clearFix"(.*?)(?=class="edn_article edn_clearFix"|class="article_pager")',
            html, re.DOTALL
        )
        for block in blocks:
            entry = {}
            # Extract link and title
            title_match = re.search(
                r'class="edn_articleTitle">\s*<a\s+href="([^"]+)"[^>]*>([^<]+)</a>',
                block, re.DOTALL
            )
            if title_match:
                entry["url"] = title_match.group(1).strip()
                entry["title"] = title_match.group(2).strip()
            else:
                continue

            # Extract date
            date_match = re.search(
                r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+(\w+)\s+(\d+),\s+(\d{4})',
                block
            )
            if date_match:
                try:
                    date_str = f"{date_match.group(2)} {date_match.group(3)}, {date_match.group(4)}"
                    dt = datetime.strptime(date_str, "%B %d, %Y")
                    entry["date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    entry["date"] = None
            else:
                entry["date"] = None

            # Extract case number from title or URL
            case_match = re.search(r'(\d{5,6})', entry.get("title", "") + entry.get("url", ""))
            entry["case_number"] = case_match.group(1) if case_match else None

            # Extract summary
            summary_match = re.search(
                r'class="edn_articleSummary"[^>]*>(.*?)</div>',
                block, re.DOTALL
            )
            if summary_match:
                summary_html = summary_match.group(1)
                summary = re.sub(r'<[^>]+>', '', summary_html).strip()
                entry["summary"] = summary[:500]
            else:
                entry["summary"] = ""

            entries.append(entry)
        return entries

    def _extract_pdf_url(self, html: str) -> Optional[str]:
        """Extract PDF download URL from an individual decision page."""
        match = re.search(
            r'(DocumentDownload\.ashx\?[^"]+)',
            html
        )
        if match:
            return f"{BASE_URL}/DesktopModules/EasyDNNNews/{match.group(1)}"
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="ET/FederalCourts",
            source_id="",
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

    def normalize(self, raw: dict) -> dict:
        """Transform raw decision data into standard schema."""
        case_number = raw.get("case_number", "")
        _id = f"ET-FSC-{case_number}" if case_number else f"ET-FSC-{hash(raw.get('url', ''))}"

        return {
            "_id": _id,
            "_source": "ET/FederalCourts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "case_number": case_number,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions."""
        yield from self._fetch_decisions(sample=False)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield decisions modified since a date."""
        yield from self._fetch_decisions(sample=False, since=since)

    def _fetch_decisions(self, sample: bool = False, since: str = None) -> Generator[dict, None, None]:
        """Core fetcher: paginate listing, fetch individual pages, download PDFs."""
        count = 0
        max_records = 15 if sample else 999999
        max_pages = 3 if sample else MAX_PAGES

        for page_num in range(1, max_pages + 1):
            if count >= max_records:
                break

            url = LISTING_URL.format(page=page_num)
            logger.info(f"Fetching listing page {page_num}/{max_pages}: {url}")

            try:
                html = self._get_page(url)
            except Exception as e:
                logger.error(f"Failed to fetch listing page {page_num}: {e}")
                continue

            entries = self._extract_listing_entries(html)
            if not entries:
                logger.warning(f"No entries found on page {page_num}, stopping")
                break

            for entry in entries:
                if count >= max_records:
                    break

                decision_url = entry["url"]
                logger.info(f"Fetching decision: {unquote(decision_url)[:80]}...")

                try:
                    detail_html = self._get_page(decision_url)
                except Exception as e:
                    logger.error(f"Failed to fetch decision page: {e}")
                    continue

                # Extract PDF URL from detail page
                pdf_url = self._extract_pdf_url(detail_html)
                if not pdf_url:
                    logger.warning(f"No PDF found for {entry.get('case_number', 'unknown')}")
                    continue

                # Download and extract text from PDF
                try:
                    pdf_bytes = self._get_binary(pdf_url)
                    text = self._extract_pdf_text(pdf_bytes)
                except Exception as e:
                    logger.error(f"Failed to download/extract PDF: {e}")
                    continue

                if not text:
                    logger.warning(f"Empty text for case {entry.get('case_number', 'unknown')}")
                    continue

                # Build raw record
                raw = {
                    "title": entry.get("title", ""),
                    "text": text,
                    "date": entry.get("date"),
                    "url": decision_url,
                    "case_number": entry.get("case_number", ""),
                    "summary": entry.get("summary", ""),
                }

                record = self.normalize(raw)
                count += 1
                logger.info(
                    f"[{count}] Case {record.get('case_number', '?')}: "
                    f"{len(text)} chars of text"
                )
                yield record

        logger.info(f"Total records fetched: {count}")

    def test_api(self):
        """Quick connectivity check."""
        logger.info("Testing FSC website connectivity...")
        try:
            html = self._get_page(LISTING_URL.format(page=1))
            entries = self._extract_listing_entries(html)
            logger.info(f"OK: listing page returned {len(entries)} entries")

            if entries:
                # Test fetching one decision
                detail_html = self._get_page(entries[0]["url"])
                pdf_url = self._extract_pdf_url(detail_html)
                if pdf_url:
                    logger.info(f"OK: found PDF URL for first entry")
                    pdf_bytes = self._get_binary(pdf_url)
                    text = self._extract_pdf_text(pdf_bytes)
                    logger.info(f"OK: extracted {len(text)} chars from PDF")
                else:
                    logger.warning("No PDF URL found on detail page")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            raise


def main():
    scraper = EthiopiaFederalCourtsScraper()

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
        for record in scraper._fetch_decisions(sample=sample):
            # Save to sample directory
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
