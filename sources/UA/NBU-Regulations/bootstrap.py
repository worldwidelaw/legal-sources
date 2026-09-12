#!/usr/bin/env python3
"""
UA/NBU-Regulations — National Bank of Ukraine Regulatory Acts

Fetches NBU regulatory acts (resolutions, decisions, regulations, etc.)
from bank.gov.ua. Search via POST endpoint, full text from PDF downloads.

~2,900 documents. Full text extracted via pdfplumber.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import time
import io
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UA.NBU-Regulations")

BASE_URL = "https://bank.gov.ua"
SEARCH_URL = f"{BASE_URL}/ua/component/source/legislationSearchResults"
PER_PAGE = 100

# Ukrainian month names for date parsing
UA_MONTHS = {
    "січня": 1, "лютого": 2, "березня": 3, "квітня": 4,
    "травня": 5, "червня": 6, "липня": 7, "серпня": 8,
    "вересня": 9, "жовтня": 10, "листопада": 11, "грудня": 12,
}


class NBURegulationsScraper(BaseScraper):
    """
    Scraper for UA/NBU-Regulations — National Bank of Ukraine regulatory acts.
    Country: UA
    URL: https://bank.gov.ua/ua/legislation

    Data types: legislation, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=30,
        )

    # -- Search and listing ------------------------------------------------

    def _search_page(self, page: int = 1, doc_type: str = "") -> tuple:
        """
        Fetch one page of search results.
        Returns (items, total_count) where items is list of dicts.
        """
        data = {
            "type": doc_type,
            "perPage": str(PER_PAGE),
            "page": str(page),
        }
        resp = self.client.post(SEARCH_URL, data=data, timeout=30)
        if not resp or resp.status_code != 200:
            return [], 0

        html = resp.text

        # Total results
        total_match = re.search(r"знайдено <b>(\d+)", html)
        total = int(total_match.group(1)) if total_match else 0

        # Parse items
        items = []
        results = re.findall(
            r'<div class="row cols search-result">(.*?)</div>\s*</div>\s*</div>',
            html, re.DOTALL
        )

        for result in results:
            item = self._parse_search_result(result)
            if item:
                items.append(item)

        return items, total

    def _parse_search_result(self, html: str) -> Optional[dict]:
        """Parse a single search result HTML block."""
        # Link and title
        link_match = re.search(
            r'href="(/ua/legislation/[^"]+)".*?<div class="title primary">\s*(.*?)\s*</div>',
            html, re.DOTALL
        )
        if not link_match:
            return None

        path = link_match.group(1)
        title = re.sub(r"\s+", " ", link_match.group(2)).strip()

        # Date
        date_match = re.search(r"<time>(\d+\s+\w+\s+\d{4})</time>", html)
        date_str = date_match.group(1) if date_match else ""

        # Document number
        num_match = re.search(r'<div class="mark">№\s*(\S+)</div>', html)
        doc_number = num_match.group(1) if num_match else ""

        # Document type
        type_match = re.search(r'<div class="category h4">(\w+)', html)
        doc_type = type_match.group(1).strip() if type_match else ""

        # Publication date
        pub_match = re.search(r"Офіційно опубліковано\s*<time>(\d+\s+\w+\s+\d{4})</time>", html)
        pub_date_str = pub_match.group(1) if pub_match else ""

        # Slug from path
        slug = path.split("/ua/legislation/")[-1] if "/ua/legislation/" in path else path

        return {
            "slug": slug,
            "title": title,
            "path": path,
            "date_str": date_str,
            "doc_number": doc_number,
            "doc_type": doc_type,
            "pub_date_str": pub_date_str,
        }

    def _find_pdf_url(self, page_path: str) -> Optional[str]:
        """Fetch document page and extract PDF URL from iframe."""
        url = f"{BASE_URL}{page_path}"
        resp = self.client.get(url, timeout=30)
        if not resp or resp.status_code != 200:
            return None

        # Look for PDF in iframe src
        iframe = re.search(r'<iframe[^>]+src="([^"]*\.pdf[^"]*)"', resp.text, re.IGNORECASE)
        if iframe:
            pdf_path = iframe.group(1).split("?")[0]  # Remove query params
            if pdf_path.startswith("/"):
                return f"{BASE_URL}{pdf_path}"
            return pdf_path

        # Fallback: look for direct PDF links
        pdf_link = re.search(r'href="(/admin_uploads/law/[^"]+\.pdf)', resp.text)
        if pdf_link:
            return f"{BASE_URL}{pdf_link.group(1)}"

        return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text."""
        import pdfplumber

        resp = self.client.get(pdf_url, timeout=60)
        if not resp or resp.status_code != 200:
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            text_parts = []
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()

            text = "\n".join(text_parts)
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n\s*\n", "\n\n", text)
            text = text.strip()
            return text if len(text) > 20 else None
        except Exception as e:
            logger.debug(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    # -- Date parsing ------------------------------------------------------

    @staticmethod
    def _parse_ua_date(date_str: str) -> Optional[str]:
        """Parse Ukrainian date string like '27 травня 2026' to ISO 8601."""
        if not date_str:
            return None
        m = re.match(r"(\d{1,2})\s+(\w+)\s+(\d{4})", date_str.strip())
        if not m:
            return None
        day = int(m.group(1))
        month_name = m.group(2).lower()
        year = int(m.group(3))
        month = UA_MONTHS.get(month_name)
        if not month:
            return None
        return f"{year:04d}-{month:02d}-{day:02d}"

    # -- Normalize ---------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw search result + full text into standard schema."""
        slug = raw.get("slug", "")
        if not slug:
            return None

        text = raw.get("_full_text", "")
        if not text:
            return None

        title = raw.get("title", "")
        date_iso = self._parse_ua_date(raw.get("date_str", ""))
        pub_date_iso = self._parse_ua_date(raw.get("pub_date_str", ""))
        doc_number = raw.get("doc_number", "")
        doc_type = raw.get("doc_type", "")

        # Build display title with number
        display_title = title
        if doc_number and doc_number not in title:
            display_title = f"№ {doc_number} — {title}"

        page_url = f"{BASE_URL}/ua/legislation/{slug}"

        return {
            "_id": f"UA-NBU-{slug}",
            "_source": "UA/NBU-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_slug": slug,
            "title": display_title or f"NBU Act {slug}",
            "text": text,
            "date": date_iso,
            "date_published": pub_date_iso,
            "url": page_url,
            "doc_number": doc_number or None,
            "doc_type": doc_type or None,
        }

    # -- Fetch methods -----------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all NBU regulatory acts with full text."""
        page = 1
        total = None
        fetched = 0

        while True:
            items, count = self._search_page(page)
            if total is None:
                total = count
                logger.info(f"Total documents: {total}")

            if not items:
                break

            for item in items:
                pdf_url = self._find_pdf_url(item["path"])
                if not pdf_url:
                    logger.debug(f"No PDF for {item['slug']}")
                    continue

                text = self._extract_pdf_text(pdf_url)
                if not text:
                    logger.debug(f"No text from PDF for {item['slug']}")
                    continue

                item["_full_text"] = text
                fetched += 1
                if fetched % 50 == 0:
                    logger.info(f"Fetched {fetched} documents with full text")
                yield item
                time.sleep(0.5)

            page += 1
            time.sleep(1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents adopted since the given date."""
        since_str = since.strftime("%d.%m.%Y")
        page = 1

        while True:
            data = {
                "type": "",
                "perPage": str(PER_PAGE),
                "page": str(page),
                "from": since_str,
            }
            resp = self.client.post(SEARCH_URL, data=data, timeout=30)
            if not resp or resp.status_code != 200:
                break

            html = resp.text
            results = re.findall(
                r'<div class="row cols search-result">(.*?)</div>\s*</div>\s*</div>',
                html, re.DOTALL
            )
            if not results:
                break

            for result_html in results:
                item = self._parse_search_result(result_html)
                if not item:
                    continue

                pdf_url = self._find_pdf_url(item["path"])
                if not pdf_url:
                    continue
                text = self._extract_pdf_text(pdf_url)
                if not text:
                    continue
                item["_full_text"] = text
                yield item
                time.sleep(0.5)

            page += 1
            time.sleep(1)

    # -- Sample mode -------------------------------------------------------

    def bootstrap(self, sample_mode: bool = False, sample_size: int = 15) -> dict:
        """Override bootstrap for sample mode."""
        if not sample_mode:
            return super().bootstrap(sample_mode=False, sample_size=sample_size)

        stats = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "records_fetched": 0,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "errors": 0,
        }

        sample_records = []
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        # Fetch first pages of results
        page = 1
        while len(sample_records) < sample_size and page <= 3:
            items, total = self._search_page(page)
            logger.info(f"Page {page}: {len(items)} items (total: {total})")

            for item in items:
                if len(sample_records) >= sample_size:
                    break

                pdf_url = self._find_pdf_url(item["path"])
                if not pdf_url:
                    stats["errors"] += 1
                    logger.debug(f"No PDF for {item['slug']}")
                    continue

                text = self._extract_pdf_text(pdf_url)
                if not text:
                    stats["errors"] += 1
                    logger.debug(f"No text from {item['slug']}")
                    continue

                item["_full_text"] = text
                record = self.normalize(item)
                if record:
                    sample_records.append(record)
                    stats["records_fetched"] += 1
                    logger.info(
                        f"Sample {len(sample_records)}/{sample_size}: "
                        f"{record['title'][:60]} ({len(record['text'])} chars)"
                    )

                time.sleep(0.5)

            page += 1
            time.sleep(1)

        # Save samples
        for i, rec in enumerate(sample_records):
            path = sample_dir / f"record_{i:04d}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)

        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(sample_records, f, ensure_ascii=False, indent=2)

        stats["finished_at"] = datetime.now(timezone.utc).isoformat()
        stats["sample_records_saved"] = len(sample_records)
        logger.info(f"Saved {len(sample_records)} sample records to {sample_dir}")

        self._save_status()
        return stats

    # -- CLI ---------------------------------------------------------------


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UA/NBU-Regulations Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NBURegulationsScraper()

    if args.command == "test-api":
        logger.info("Testing search endpoint...")
        items, total = scraper._search_page(1)
        logger.info(f"Found {total} total documents, {len(items)} on page 1")
        if items:
            item = items[0]
            logger.info(f"First: {item['title'][:60]}")
            logger.info(f"  Slug: {item['slug']}")
            logger.info(f"  Date: {item['date_str']}")
            logger.info(f"  Type: {item['doc_type']}")

            pdf_url = scraper._find_pdf_url(item["path"])
            if pdf_url:
                logger.info(f"  PDF: {pdf_url}")
                text = scraper._extract_pdf_text(pdf_url)
                if text:
                    logger.info(f"  Text: {len(text)} chars")
                else:
                    logger.error("  PDF text extraction failed")
            else:
                logger.error("  No PDF found")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
