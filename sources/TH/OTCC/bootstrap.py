#!/usr/bin/env python3
"""
TH/OTCC -- Thailand Trade Competition Commission Verdicts

Fetches competition enforcement verdicts from the TCCT website (tcct.or.th).
Scrapes the verdict listing page for PDF links, downloads each PDF, and
extracts full text using the common PDF extraction pipeline.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all ~126 verdicts)
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import re
import hashlib
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TH.OTCC")

BASE_URL = "https://www.tcct.or.th"
VERDICT_PAGE = f"{BASE_URL}/view/1/Verdict/TH-TH"
DELAY = 2.0

# Thai month names → month numbers for date parsing
THAI_MONTHS = {
    "มกราคม": "01", "กุมภาพันธ์": "02", "มีนาคม": "03",
    "เมษายน": "04", "พฤษภาคม": "05", "มิถุนายน": "06",
    "กรกฎาคม": "07", "สิงหาคม": "08", "กันยายน": "09",
    "ตุลาคม": "10", "พฤศจิกายน": "11", "ธันวาคม": "12",
}

THAI_MONTH_PATTERN = "|".join(THAI_MONTHS.keys())


def _parse_thai_date(text: str) -> Optional[str]:
    """Extract a Thai Buddhist-era date from text and convert to ISO 8601."""
    m = re.search(
        rf"(\d{{1,2}})\s+({THAI_MONTH_PATTERN})\s+(\d{{4}})", text
    )
    if not m:
        return None
    day, month_thai, year_be = m.group(1), m.group(2), m.group(3)
    year_ce = int(year_be) - 543
    return f"{year_ce}-{THAI_MONTHS[month_thai]}-{int(day):02d}"


def _make_id(pdf_filename: str) -> str:
    """Generate a stable document ID from the PDF filename."""
    name = Path(pdf_filename).stem
    return f"TH_OTCC_{name}"


class THOTCCScraper(BaseScraper):
    """Scraper for Thailand Trade Competition Commission verdicts."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,*/*",
            },
        )

    def _scrape_verdict_list(self) -> List[Tuple[str, str, Optional[str]]]:
        """Scrape the verdict listing page for PDF links, titles, and dates.

        Returns list of (pdf_url, title, iso_date) tuples.
        """
        logger.info("Fetching verdict listing page: %s", VERDICT_PAGE)
        resp = self.http.get(VERDICT_PAGE, timeout=30)
        resp.raise_for_status()
        html = resp.text

        # Extract all PDF links (excluding PDPA policy docs)
        pdf_urls = [
            m.group(1)
            for m in re.finditer(
                r'href="(/assets/portals/1/files/(?!PDPA)[^"]+\.pdf)"', html
            )
        ]
        logger.info("Found %d verdict PDFs on listing page", len(pdf_urls))

        if not pdf_urls:
            return []

        # Split HTML on each PDF anchor to get the text block preceding each link
        parts = re.split(
            r'<a[^>]*href="/assets/portals/1/files/(?!PDPA)[^"]+\.pdf"[^>]*>',
            html,
        )

        results = []
        for i, url in enumerate(pdf_urls):
            # The text block before this PDF link is in parts[i]
            prev_block = parts[i] if i < len(parts) else ""
            clean = re.sub(r"<[^>]+>", " ", prev_block)
            clean = re.sub(r"\s+", " ", clean).strip()

            # Extract title (starts with ผลคำวินิจฉัย)
            title_match = re.search(r"(ผลคำวินิจฉัย[^0-9]{5,200})", clean)
            title = title_match.group(1).strip() if title_match else ""
            if not title:
                # Fall back to PDF filename
                title = Path(url).stem.replace("_", " ").replace("-", " ")

            # Extract date
            iso_date = _parse_thai_date(clean)

            full_url = f"{BASE_URL}{url}"
            results.append((full_url, title, iso_date))

        return results

    def _download_and_extract(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download a PDF and extract text content."""
        try:
            resp = self.http.get(pdf_url, timeout=60)
            resp.raise_for_status()
            pdf_bytes = resp.content

            if len(pdf_bytes) < 500:
                logger.warning("PDF too small (%d bytes): %s", len(pdf_bytes), pdf_url)
                return None

            text = extract_pdf_markdown("TH/OTCC", doc_id, pdf_bytes=pdf_bytes)
            if text and len(text.strip()) > 50:
                return text.strip()
            logger.warning("No text extracted from PDF: %s", pdf_url)
            return None
        except Exception as e:
            logger.error("Failed to download/extract %s: %s", pdf_url, e)
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw verdict record."""
        return {
            "_id": raw["_id"],
            "_source": "TH/OTCC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "pdf_filename": raw.get("pdf_filename", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all TCCT competition verdicts."""
        verdicts = self._scrape_verdict_list()
        if not verdicts:
            logger.error("No verdicts found on listing page")
            return

        limit = 15 if sample else len(verdicts)
        fetched = 0

        for pdf_url, title, iso_date in verdicts:
            if fetched >= limit:
                break

            pdf_filename = pdf_url.split("/")[-1]
            doc_id = _make_id(pdf_filename)

            logger.info(
                "Fetching [%d/%d] %s ...", fetched + 1, limit, pdf_filename
            )

            text = self._download_and_extract(pdf_url, doc_id)
            if not text:
                logger.warning("Skipping %s (no text)", pdf_filename)
                continue

            raw = {
                "_id": doc_id,
                "title": title,
                "text": text,
                "date": iso_date,
                "url": pdf_url,
                "pdf_filename": pdf_filename,
            }

            yield self.normalize(raw)
            fetched += 1
            time.sleep(DELAY)

        logger.info("Fetched %d verdicts total", fetched)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch verdicts updated since a given date."""
        since_date = datetime.fromisoformat(since).date()
        for record in self.fetch_all():
            if record.get("date"):
                try:
                    rec_date = datetime.fromisoformat(record["date"]).date()
                    if rec_date >= since_date:
                        yield record
                except ValueError:
                    yield record
            else:
                yield record


def main():
    import argparse
    import json

    parser = argparse.ArgumentParser(description="TH/OTCC bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = THOTCCScraper()

    if args.command == "test":
        verdicts = scraper._scrape_verdict_list()
        print(f"OK -- found {len(verdicts)} verdicts on listing page")
        if verdicts:
            url, title, date = verdicts[0]
            print(f"  Latest: {title[:60]}... ({date or 'no date'})")
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
