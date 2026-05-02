#!/usr/bin/env python3
"""
PK/SECP -- Pakistan Securities and Exchange Commission

Fetches regulatory documents (acts, ordinances, rules, regulations,
directives, guidelines, circulars) from the SECP website. Documents
are listed as HTML tables with PDF downloads via WordPress.

Usage:
  python bootstrap.py bootstrap --sample    # Fetch ~15 sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import re
import json
import logging
import time
import hashlib
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List, Tuple
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PK.SECP")

BASE_URL = "https://www.secp.gov.pk"
DELAY = 2.0

CATEGORIES = [
    "acts",
    "ordinances",
    "rules",
    "regulations",
    "directives",
    "guidelines",
    "circulars",
]


def _parse_date(date_str: str) -> Optional[str]:
    """Parse dd/mm/yyyy date to ISO 8601."""
    if not date_str or not date_str.strip():
        return None
    date_str = date_str.strip()
    # Try dd/mm/yyyy
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return f"{y}-{mo.zfill(2)}-{d.zfill(2)}"
    # Try yyyy-mm-dd
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', date_str)
    if m:
        return date_str
    return None


def _make_id(category: str, title: str, wpdmdl: str) -> str:
    """Generate a stable document ID."""
    if wpdmdl:
        return f"PK_SECP_{wpdmdl}"
    raw = f"{category}|{title}"
    h = hashlib.md5(raw.encode()).hexdigest()[:12]
    return f"PK_SECP_{h}"


def _parse_listing_page(html_content: str, category: str) -> List[Dict[str, Any]]:
    """Parse an HTML listing page to extract document rows."""
    records = []
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html_content, re.DOTALL)

    for row in rows[1:]:  # skip header
        tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
        if len(tds) < 2:
            continue

        date_raw = re.sub(r'<[^>]+>', '', tds[0]).strip()
        title = re.sub(r'<[^>]+>', '', tds[1]).strip()
        title = html_mod.unescape(title)

        # Find download URL
        pdf_url = ""
        wpdmdl = ""
        link_match = re.search(r'href="([^"]*)"', row)
        if link_match:
            pdf_url = link_match.group(1)
            wpdm_match = re.search(r'wpdmdl=(\d+)', pdf_url)
            if wpdm_match:
                wpdmdl = wpdm_match.group(1)

        if not title:
            continue

        records.append({
            "date_raw": date_raw,
            "title": title,
            "pdf_url": pdf_url,
            "wpdmdl": wpdmdl,
            "category": category,
        })

    return records


class PKSECPScraper(BaseScraper):
    """Scraper for Pakistan SECP regulatory documents."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
            },
        )

    def _fetch_category_listing(self, category: str) -> List[Dict[str, Any]]:
        """Fetch and parse document listing for a category."""
        url = f"{BASE_URL}/laws/{category}/"
        try:
            resp = self.http.get(url, timeout=30)
            resp.raise_for_status()
            records = _parse_listing_page(resp.text, category)
            logger.info("Category %s: %d documents found", category, len(records))
            return records
        except Exception as e:
            logger.error("Failed to fetch category %s: %s", category, e)
            return []

    def _download_pdf_text(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Download PDF and extract text."""
        if not pdf_url:
            return None
        try:
            text = extract_pdf_markdown(
                source="PK/SECP",
                source_id=doc_id,
                pdf_url=pdf_url,
                table="doctrine",
            )
            return text if text and len(text) > 50 else None
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", doc_id, e)
            return None

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all regulatory documents."""
        total = 0
        categories = CATEGORIES if not sample else ["circulars"]

        for category in categories:
            listings = self._fetch_category_listing(category)
            time.sleep(DELAY)

            for item in listings:
                doc_id = _make_id(item["category"], item["title"], item["wpdmdl"])
                logger.info("Downloading: %s", item["title"][:60])

                text = self._download_pdf_text(item["pdf_url"], doc_id)
                time.sleep(DELAY)

                if not text:
                    logger.warning("No text for: %s", item["title"][:60])
                    continue

                yield {
                    "_id": doc_id,
                    "title": item["title"],
                    "date_raw": item["date_raw"],
                    "date": _parse_date(item["date_raw"]),
                    "category": item["category"],
                    "pdf_url": item["pdf_url"],
                    "text": text,
                }

                total += 1
                if sample and total >= 15:
                    return

        logger.info("Total records fetched: %d", total)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch documents updated since a given date."""
        try:
            since_date = since[:10]
        except (TypeError, IndexError):
            since_date = "2020-01-01"

        for category in CATEGORIES:
            listings = self._fetch_category_listing(category)
            time.sleep(DELAY)

            for item in listings:
                iso_date = _parse_date(item["date_raw"])
                if iso_date and iso_date < since_date:
                    continue

                doc_id = _make_id(item["category"], item["title"], item["wpdmdl"])
                text = self._download_pdf_text(item["pdf_url"], doc_id)
                time.sleep(DELAY)

                if not text:
                    continue

                yield {
                    "_id": doc_id,
                    "title": item["title"],
                    "date_raw": item["date_raw"],
                    "date": iso_date,
                    "category": item["category"],
                    "pdf_url": item["pdf_url"],
                    "text": text,
                }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record to the standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "PK/SECP",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": f"{BASE_URL}/laws/{raw.get('category', '')}/",
            "pdf_url": raw.get("pdf_url", ""),
            "category": raw.get("category", ""),
            "language": "en",
            "authority": "Securities and Exchange Commission of Pakistan",
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="PK/SECP bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run full bootstrap")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--sample-size", type=int, default=15, help="Sample size")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = PKSECPScraper()

    if args.command == "test":
        print("Testing PK/SECP endpoints...")
        for cat in CATEGORIES:
            listings = scraper._fetch_category_listing(cat)
            print(f"  {cat}: {len(listings)} documents")
            if listings:
                print(f"    Sample: {listings[0]['title'][:60]}")
            time.sleep(1)
        return

    if args.command == "bootstrap":
        sample = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample, sample_size=args.sample_size)
        print(json.dumps(stats, indent=2))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
