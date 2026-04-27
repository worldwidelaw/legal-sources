#!/usr/bin/env python3
"""
MY/LFSA-Legislation -- Labuan Financial Services Authority Legislation & Guidelines

Fetches principal acts and regulatory guidelines from the Labuan FSA website.

Strategy:
  - Scrapes the acts page at /regulations/legislation/act for PDF links
  - Scrapes 9 guideline category pages under /regulations/guidelines/
  - Downloads PDFs and extracts full text via common.pdf_extract
  - Acts → _type "legislation", Guidelines → _type "doctrine"

Data:
  - 8 principal acts (Labuan Companies, Trusts, Foundations, etc.)
  - ~100+ guidelines across 9 categories (General, Banking, Insurance,
    Trust Companies, Labuan Companies, Capital Markets, Islamic, Other, Tax)

License: Public regulatory data (Malaysia)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import time
import logging
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MY.LFSA-Legislation")

BASE_URL = "https://www.labuanfsa.gov.my"

# Legislation pages
LEGISLATION_PAGES = [
    "/regulations/legislation/act",
]

# Guideline category pages
GUIDELINE_PAGES = [
    "/regulations/guidelines/general",
    "/regulations/guidelines/banking",
    "/regulations/guidelines/insurance",
    "/regulations/guidelines/trust-companies",
    "/regulations/guidelines/labuan-companies",
    "/regulations/guidelines/capital-markets",
    "/regulations/guidelines/islamic-business",
    "/regulations/guidelines/other-business",
    "/regulations/guidelines/tax-related-matters",
]

# Month mapping
MONTH_MAP = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    "january": 1, "february": 2, "march": 3, "april": 4,
    "june": 6, "july": 7, "august": 8, "september": 9,
    "october": 10, "november": 11, "december": 12,
}


def _strip_html(html_text: str) -> str:
    """Remove HTML tags and decode entities."""
    import html as html_module
    text = re.sub(r'<br\s*/?\s*>', '\n', html_text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    return text.strip()


def _parse_date(date_str: str) -> Optional[str]:
    """Parse dates like 'Sep 10, 2025', 'Oct 31, 2016', '09/10/2025'."""
    if not date_str:
        return None
    date_str = date_str.strip()

    # "Mon DD, YYYY"
    m = re.match(r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', date_str)
    if m:
        mon_str, day, year = m.groups()
        mon = MONTH_MAP.get(mon_str.lower())
        if mon:
            try:
                return datetime(int(year), mon, int(day)).strftime("%Y-%m-%d")
            except ValueError:
                pass

    # "MM/DD/YYYY"
    m = re.match(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
    if m:
        month, day, year = m.groups()
        try:
            return datetime(int(year), int(month), int(day)).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # "DD Mon YYYY"
    m = re.match(r'(\d{1,2})\s+(\w+)\s+(\d{4})', date_str)
    if m:
        day, mon_str, year = m.groups()
        mon = MONTH_MAP.get(mon_str.lower())
        if mon:
            try:
                return datetime(int(year), mon, int(day)).strftime("%Y-%m-%d")
            except ValueError:
                pass

    return None


class LabuanFSAScraper(BaseScraper):

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-GB,en;q=0.9",
            },
        )

    def _extract_pdf_links(self, html: str, page_url: str) -> List[Dict[str, str]]:
        """Extract PDF links with their titles and dates from a page."""
        results = []
        seen_urls = set()

        # Find all links to PDFs
        for m in re.finditer(
            r'<a[^>]+href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
            html, re.DOTALL | re.IGNORECASE
        ):
            href = m.group(1)
            title = _strip_html(m.group(2)).strip()

            if not href.startswith("http"):
                href = urljoin(BASE_URL, href)

            if href in seen_urls:
                continue
            seen_urls.add(href)

            # Skip very short titles (icons, etc.)
            if len(title) < 3:
                # Try to derive title from filename
                fname = unquote(href.split("/")[-1])
                fname = re.sub(r'\.pdf$', '', fname, flags=re.IGNORECASE)
                fname = fname.replace("-", " ").replace("_", " ")
                title = fname.strip()

            if not title or len(title) < 3:
                continue

            results.append({
                "url": href,
                "title": title,
                "page_url": page_url,
            })

        return results

    def _extract_items_with_dates(self, html: str, page_url: str) -> List[Dict[str, str]]:
        """Extract items from listing pages, capturing dates and PDF links."""
        items = []
        seen_urls = set()

        # Find table rows or list items with dates and PDF links
        # Pattern 1: table rows with date and PDF link
        for row_m in re.finditer(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL):
            row = row_m.group(1)
            # Extract date
            date_match = re.search(
                r'(\w{3}\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})',
                _strip_html(row)
            )
            date_str = date_match.group(1) if date_match else ""

            # Extract PDF links from this row
            for pdf_m in re.finditer(
                r'<a[^>]+href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
                row, re.DOTALL | re.IGNORECASE
            ):
                href = pdf_m.group(1)
                title = _strip_html(pdf_m.group(2)).strip()

                if not href.startswith("http"):
                    href = urljoin(BASE_URL, href)

                if href in seen_urls:
                    continue
                seen_urls.add(href)

                if len(title) < 3:
                    fname = unquote(href.split("/")[-1])
                    fname = re.sub(r'\.pdf$', '', fname, flags=re.IGNORECASE)
                    fname = fname.replace("-", " ").replace("_", " ")
                    title = fname.strip()

                items.append({
                    "url": href,
                    "title": title,
                    "date_str": date_str,
                    "page_url": page_url,
                })

        # Also extract any PDF links not in table rows (accordion items, divs, etc.)
        all_pdfs = self._extract_pdf_links(html, page_url)
        for pdf in all_pdfs:
            if pdf["url"] not in seen_urls:
                seen_urls.add(pdf["url"])
                # Try to find a nearby date
                idx = html.find(pdf["url"])
                if idx > 0:
                    context = html[max(0, idx - 500):idx]
                    date_match = re.search(
                        r'(\w{3}\s+\d{1,2},?\s+\d{4}|\d{1,2}/\d{1,2}/\d{4})',
                        _strip_html(context)
                    )
                    date_str = date_match.group(1) if date_match else ""
                else:
                    date_str = ""

                items.append({
                    "url": pdf["url"],
                    "title": pdf["title"],
                    "date_str": date_str,
                    "page_url": page_url,
                })

        return items

    def _get_all_documents(self) -> List[Dict[str, str]]:
        """Fetch all document entries from legislation and guideline pages."""
        all_docs = []
        seen_urls = set()

        # 1. Legislation pages
        for page_path in LEGISLATION_PAGES:
            logger.info(f"Fetching legislation page: {page_path}")
            resp = self.client.get(page_path)
            if not resp or resp.status_code != 200:
                logger.warning(f"  Failed to fetch {page_path}: {resp.status_code if resp else 'no response'}")
                continue

            items = self._extract_items_with_dates(resp.text, page_path)
            for item in items:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    item["doc_type"] = "legislation"
                    item["category"] = "Principal Legislation"
                    all_docs.append(item)

            logger.info(f"  Found {len(items)} documents")
            time.sleep(1)

        # 2. Guideline pages
        for page_path in GUIDELINE_PAGES:
            category = page_path.split("/")[-1].replace("-", " ").title()
            logger.info(f"Fetching guidelines: {category}")
            resp = self.client.get(page_path)
            if not resp or resp.status_code != 200:
                logger.warning(f"  Failed to fetch {page_path}: {resp.status_code if resp else 'no response'}")
                continue

            items = self._extract_items_with_dates(resp.text, page_path)
            for item in items:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    item["doc_type"] = "doctrine"
                    item["category"] = f"Guidelines - {category}"
                    all_docs.append(item)

            logger.info(f"  Found {len(items)} documents")
            time.sleep(1)

        logger.info(f"Total documents to process: {len(all_docs)}")
        return all_docs

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all documents with full text from PDFs."""
        documents = self._get_all_documents()

        for i, doc in enumerate(documents):
            url = doc["url"]
            title = doc["title"]
            logger.info(f"[{i+1}/{len(documents)}] {title[:60]}...")

            source_id = hashlib.md5(url.encode()).hexdigest()
            table = "legislation" if doc["doc_type"] == "legislation" else "doctrine"

            try:
                text = extract_pdf_markdown(
                    source="MY/LFSA-Legislation",
                    source_id=source_id,
                    pdf_url=url,
                    table=table,
                )
            except Exception as e:
                logger.warning(f"  PDF extraction failed: {e}")
                continue

            if not text or len(text.strip()) < 50:
                logger.warning(f"  Insufficient text ({len(text) if text else 0} chars)")
                continue

            yield self.normalize({
                "url": url,
                "title": title,
                "date": _parse_date(doc.get("date_str", "")),
                "text": text,
                "doc_type": doc["doc_type"],
                "category": doc.get("category", ""),
                "page_url": doc.get("page_url", ""),
            })

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Yield documents updated since the given date."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        url = raw["url"]
        doc_id = hashlib.md5(url.encode()).hexdigest()

        return {
            "_id": f"MY/LFSA-Legislation/{doc_id}",
            "_source": "MY/LFSA-Legislation",
            "_type": raw["doc_type"],
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": url,
            "category": raw.get("category"),
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="MY/LFSA-Legislation scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only 10+ sample records")
    args = parser.parse_args()

    scraper = LabuanFSAScraper()

    if args.command == "test":
        logger.info("Testing connectivity to labuanfsa.gov.my...")
        resp = scraper.client.get(LEGISLATION_PAGES[0])
        if resp and resp.status_code == 200:
            logger.info(f"OK — got {len(resp.text)} bytes from legislation page")
        else:
            logger.error(f"FAIL — status {resp.status_code if resp else 'no response'}")
        return

    if args.command in ("bootstrap", "update"):
        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else 999999

        for doc in scraper.fetch_all():
            count += 1
            text_len = len(doc.get("text", ""))
            logger.info(
                f"  #{count} {doc['title'][:50]}... "
                f"({text_len} chars, {doc['_type']})"
            )

            # Save sample
            if count <= 20:
                fname = re.sub(r'[^\w\-]', '_', doc["_id"])[:80] + ".json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)

            if count >= max_records:
                break

        logger.info(f"Done — {count} records fetched")


if __name__ == "__main__":
    main()
