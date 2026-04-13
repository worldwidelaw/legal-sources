#!/usr/bin/env python3
"""
IN/SEBI -- Securities and Exchange Board of India Orders & Circulars

Fetches enforcement orders and regulatory circulars from SEBI with full text
extracted from PDFs.

Strategy:
  - GET a listing page to obtain JSESSIONID cookie
  - POST to /sebiweb/ajax/home/getnewslistinfo.jsp with section params
  - Parse HTML table rows to extract date, title, and detail URL
  - Fetch each detail page, extract PDF URL from <iframe> tag
  - Download PDF and extract full text using pdfplumber

Data:
  - ~6,385 enforcement orders (Orders of Chairperson/Members)
  - ~2,762 circulars
  - PDFs contain selectable text
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch orders from last 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin

import requests

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: BeautifulSoup4 is required. Install with: pip install beautifulsoup4")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.SEBI")

BASE_URL = "https://www.sebi.gov.in"
AJAX_URL = f"{BASE_URL}/sebiweb/ajax/home/getnewslistinfo.jsp"

# Categories to fetch: (sid, ssid, smid, category_name, data_type)
CATEGORIES = [
    (2, 9, 2, "enforcement_orders", "case_law"),      # Orders of Chairperson/Members
    (1, 7, 0, "circulars", "doctrine"),                # Circulars
]

RECORDS_PER_PAGE = 25


class SEBIScraper(BaseScraper):
    """
    Scraper for IN/SEBI -- Securities and Exchange Board of India.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        })

    def _init_session(self, sid: int, ssid: int, smid: int = 0):
        """Get JSESSIONID cookie by visiting a listing page."""
        listing_url = (
            f"{BASE_URL}/sebiweb/home/HomeAction.do"
            f"?doListing=yes&sid={sid}&ssid={ssid}"
        )
        if smid:
            listing_url += f"&smid={smid}"
        resp = self.session.get(listing_url, timeout=30)
        resp.raise_for_status()
        self._referer = listing_url
        logger.info("Session initialized for sid=%d ssid=%d smid=%d", sid, ssid, smid)

    def _fetch_page(self, sid: int, ssid: int, smid: int, page: int) -> str:
        """Fetch one page of listings from the AJAX endpoint."""
        self.rate_limiter.wait()
        data = {
            "sid": str(sid),
            "ssid": str(ssid),
            "smid": str(smid),
            "ssidhidden": str(ssid),
            "nextValue": str(page),
            "next": "s" if page == 0 else "n",
            "search": "",
            "fromDate": "",
            "toDate": "",
            "intmid": "-1",
            "doDirect": "-1",
        }
        headers = {
            "Referer": self._referer,
            "Content-Type": "application/x-www-form-urlencoded",
            "X-Requested-With": "XMLHttpRequest",
        }
        resp = self.session.post(AJAX_URL, data=data, headers=headers, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _parse_listing(self, html_text: str) -> Tuple[List[Dict], int]:
        """Parse AJAX response HTML to extract records and total count."""
        records = []

        # Extract total records
        total_match = re.search(r"of (\d+) records", html_text)
        total = int(total_match.group(1)) if total_match else 0

        # Parse table rows: each <tr> has <td>date</td><td><a href>title</a></td>
        soup = BeautifulSoup(html_text, "html.parser")
        for row in soup.find_all("tr", role="row"):
            cells = row.find_all("td")
            if len(cells) < 2:
                continue
            date_text = cells[0].get_text(strip=True)
            link = cells[1].find("a")
            if not link:
                continue
            title = link.get("title", "") or link.get_text(strip=True)
            href = link.get("href", "")
            if not href:
                continue

            # Make URL absolute
            if not href.startswith("http"):
                href = urljoin(BASE_URL + "/", href)

            records.append({
                "date_raw": date_text,
                "title": title.strip(),
                "url": href,
            })

        return records, total

    def _extract_pdf_url(self, detail_url: str) -> Optional[str]:
        """Fetch a detail page and extract the PDF URL from the iframe."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(detail_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning("Failed to fetch detail page %s: %s", detail_url, e)
            return None

        # Check if the URL itself is a PDF
        if detail_url.lower().endswith(".pdf"):
            return detail_url

        # Look for iframe src containing PDF URL
        # Pattern: <iframe src='https://www.sebi.gov.in/web/?file=ACTUAL_PDF_URL'>
        match = re.search(
            r'<iframe[^>]+src=["\']([^"\']*\?file=([^"\'&]+\.pdf))',
            resp.text, re.IGNORECASE
        )
        if match:
            pdf_url = match.group(2)
            if not pdf_url.startswith("http"):
                pdf_url = urljoin(BASE_URL + "/", pdf_url)
            return pdf_url

        # Fallback: direct PDF link in iframe src
        match = re.search(
            r'<iframe[^>]+src=["\']([^"\']+\.pdf)',
            resp.text, re.IGNORECASE
        )
        if match:
            pdf_url = match.group(1)
            if not pdf_url.startswith("http"):
                pdf_url = urljoin(BASE_URL + "/", pdf_url)
            return pdf_url

        # Fallback: any PDF link on the page
        pdfs = re.findall(r'href=["\']([^"\']+\.pdf)', resp.text, re.IGNORECASE)
        if pdfs:
            pdf_url = pdfs[0]
            if not pdf_url.startswith("http"):
                pdf_url = urljoin(BASE_URL + "/", pdf_url)
            return pdf_url

        logger.warning("No PDF found on detail page: %s", detail_url)
        return None

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IN/SEBI",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse SEBI date formats to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _make_id(self, url: str) -> str:
        """Create a unique ID from the URL."""
        # Extract the slug from URL like .../title_12345.html
        match = re.search(r'/([^/]+?)(?:\.html)?$', url)
        if match:
            return match.group(1)
        # Fallback: hash
        import hashlib
        return hashlib.md5(url.encode()).hexdigest()

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw SEBI record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "IN/SEBI",
            "_type": raw.get("data_type", "case_law"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all SEBI orders and circulars."""
        for sid, ssid, smid, category, data_type in CATEGORIES:
            logger.info("Fetching category: %s (sid=%d ssid=%d smid=%d)",
                        category, sid, ssid, smid)
            yield from self._fetch_category(sid, ssid, smid, category, data_type)

    def fetch_updates(self, since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent SEBI documents (last 90 days)."""
        if since is None:
            since = datetime.now(timezone.utc) - timedelta(days=90)

        from_date = since.strftime("%d-%m-%Y")
        to_date = datetime.now(timezone.utc).strftime("%d-%m-%Y")

        for sid, ssid, smid, category, data_type in CATEGORIES:
            logger.info("Fetching updates for %s since %s", category, from_date)
            yield from self._fetch_category(
                sid, ssid, smid, category, data_type,
                from_date=from_date, to_date=to_date
            )

    def _fetch_category(self, sid: int, ssid: int, smid: int,
                        category: str, data_type: str,
                        from_date: str = "", to_date: str = "",
                        max_records: int = 0) -> Generator[Dict[str, Any], None, None]:
        """Fetch all records from a single SEBI category."""
        self._init_session(sid, ssid, smid)

        # Fetch first page to get total
        html_text = self._fetch_page(sid, ssid, smid, 0)
        records, total = self._parse_listing(html_text)
        logger.info("Category %s: %d total records", category, total)

        if max_records:
            total = min(total, max_records)

        # Process first page records
        for rec in records:
            doc = self._process_record(rec, category, data_type)
            if doc:
                yield doc

        # Paginate through remaining pages
        total_pages = (total + RECORDS_PER_PAGE - 1) // RECORDS_PER_PAGE
        for page in range(1, total_pages):
            if max_records and page * RECORDS_PER_PAGE >= max_records:
                break
            try:
                html_text = self._fetch_page(sid, ssid, smid, page)
                records, _ = self._parse_listing(html_text)
                for rec in records:
                    doc = self._process_record(rec, category, data_type)
                    if doc:
                        yield doc
            except Exception as e:
                logger.error("Failed to fetch page %d of %s: %s", page, category, e)
                continue

    def _process_record(self, rec: dict, category: str, data_type: str) -> Optional[dict]:
        """Process a single listing record: fetch detail page, download PDF, extract text."""
        title = rec["title"]
        url = rec["url"]
        date_iso = self._parse_date(rec["date_raw"])
        doc_id = self._make_id(url)

        # Get PDF URL from detail page
        pdf_url = self._extract_pdf_url(url)
        if not pdf_url:
            logger.warning("No PDF for: %s", title[:80])
            return None

        # Download and extract text
        text = self._download_pdf_text(pdf_url)
        if not text:
            logger.warning("No text extracted for: %s", title[:80])
            return None

        raw = {
            "_id": doc_id,
            "title": title,
            "text": text,
            "date": date_iso,
            "url": url,
            "pdf_url": pdf_url,
            "category": category,
            "data_type": data_type,
        }

        return self.normalize(raw)

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            self._init_session(2, 9, 2)
            html_text = self._fetch_page(2, 9, 2, 0)
            records, total = self._parse_listing(html_text)
            logger.info("Connection OK: %d enforcement orders, %d on page 1", total, len(records))
            return total > 0 and len(records) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample:
            logger.info("Running in SAMPLE mode (15 records)")
            count = 0
            target = 15
            for sid, ssid, smid, category, data_type in CATEGORIES:
                if count >= target:
                    break
                self._init_session(sid, ssid, smid)
                html_text = self._fetch_page(sid, ssid, smid, 0)
                records, total = self._parse_listing(html_text)
                logger.info("Category %s: %d total records", category, total)

                for rec in records:
                    if count >= target:
                        break
                    doc = self._process_record(rec, category, data_type)
                    if doc:
                        fname = f"{doc['_id'][:80]}.json"
                        fname = re.sub(r'[^\w\-.]', '_', fname)
                        with open(sample_dir / fname, "w", encoding="utf-8") as f:
                            json.dump(doc, f, ensure_ascii=False, indent=2)
                        count += 1
                        logger.info("[%d/%d] Saved: %s (%d chars)",
                                    count, target, doc["title"][:60], len(doc["text"]))

            logger.info("Sample bootstrap complete: %d records saved", count)
            return count
        else:
            count = 0
            for doc in self.fetch_all():
                self.storage.save(doc)
                count += 1
                if count % 100 == 0:
                    logger.info("Progress: %d records saved", count)
            logger.info("Full bootstrap complete: %d records saved", count)
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="IN/SEBI Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = SEBIScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for doc in scraper.fetch_updates():
            scraper.storage.save(doc)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
