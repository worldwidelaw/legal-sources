#!/usr/bin/env python3
"""
IN/CCI -- Competition Commission of India Orders

Fetches antitrust and combination orders from the CCI website.

Strategy:
  - GET /antitrust/orders to obtain session cookie + CSRF token
  - POST /antitrust/orders/list with DataTables params to list all antitrust orders
  - GET /combination/orders-section31 with DataTables params for combination orders
  - Download PDFs and extract full text using pdfplumber

Data:
  - ~1,300 antitrust orders (Section 26/27/33 etc.)
  - ~1,400 combination orders (Section 31)
  - PDFs contain selectable text (not scanned images)
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
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.CCI")

BASE_URL = "https://www.cci.gov.in"


class CCIScraper(BaseScraper):
    """
    Scraper for IN/CCI -- Competition Commission of India Orders.
    Country: IN
    URL: https://www.cci.gov.in/
    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research)",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "X-Requested-With": "XMLHttpRequest",
        })
        self._csrf_token = None

    def _get_csrf_token(self):
        """Fetch the antitrust orders page to get session cookie and CSRF token."""
        resp = self.session.get(f"{BASE_URL}/antitrust/orders", timeout=30)
        resp.raise_for_status()
        # Try meta tag first, then hidden input field
        match = re.search(r'<meta name="csrf-token" content="([^"]+)"', resp.text)
        if not match:
            match = re.search(r'name="_token"[^>]*value="([^"]+)"', resp.text)
        if not match:
            match = re.search(r'value="([^"]+)"[^>]*name="_token"', resp.text)
        if not match:
            raise RuntimeError("Could not find CSRF token on CCI antitrust page")
        self._csrf_token = match.group(1)
        self.session.headers["X-CSRF-TOKEN"] = self._csrf_token
        logger.info("Obtained CSRF token and session cookie")

    def _fetch_antitrust_orders(self, start: int = 0, length: int = 500,
                                 from_date: str = "", to_date: str = "") -> dict:
        """Fetch a page of antitrust orders via DataTables endpoint."""
        if not self._csrf_token:
            self._get_csrf_token()

        data = {
            "draw": "1",
            "start": str(start),
            "length": str(length),
            "tab_type": "freetext",
            "freetext": "",
            "textfromdate": from_date,
            "texttodate": to_date,
            "_token": self._csrf_token,
            "order[0][column]": "4",
            "order[0][dir]": "desc",
            "columns[0][data]": "DT_RowIndex",
            "columns[0][name]": "",
            "columns[1][data]": "case_no",
            "columns[1][name]": "",
            "columns[2][data]": "type",
            "columns[2][name]": "",
            "columns[3][data]": "title",
            "columns[3][name]": "",
            "columns[4][data]": "main_order_date",
            "columns[4][name]": "",
            "columns[5][data]": "description",
            "columns[5][name]": "",
            "columns[6][data]": "files",
            "columns[6][name]": "",
        }

        resp = self.session.post(f"{BASE_URL}/antitrust/orders/list", data=data, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _fetch_combination_orders(self, start: int = 0, length: int = 500,
                                   from_date: str = "", to_date: str = "") -> dict:
        """Fetch a page of combination orders via DataTables endpoint."""
        # Combination endpoint is GET, no CSRF needed, but needs session
        if not self._csrf_token:
            self._get_csrf_token()

        params = {
            "draw": "1",
            "start": str(start),
            "length": str(length),
            "form_type": "",
            "order_status": "",
            "searchString": "",
            "search_type": "",
            "fromdate": from_date,
            "todate": to_date,
            "order[0][column]": "5",
            "order[0][dir]": "desc",
            "columns[0][data]": "DT_RowIndex",
            "columns[0][name]": "",
            "columns[1][data]": "combination_no",
            "columns[1][name]": "",
            "columns[2][data]": "party_name",
            "columns[2][name]": "",
            "columns[3][data]": "form_type",
            "columns[3][name]": "",
            "columns[4][data]": "notification_date",
            "columns[4][name]": "",
            "columns[5][data]": "decision_date",
            "columns[5][name]": "",
            "columns[6][data]": "order_status",
            "columns[6][name]": "",
            "columns[7][data]": "order_file_content",
            "columns[7][name]": "",
            "columns[8][data]": "summary_file_content",
            "columns[8][name]": "",
        }

        resp = self.session.get(f"{BASE_URL}/combination/orders-section31",
                                params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _extract_pdf_url(self, file_content_field: str) -> Optional[str]:
        """Extract PDF URL from file_content JSON or HTML field."""
        if not file_content_field:
            return None

        # Unescape HTML entities (CCI returns &quot; etc.)
        from html import unescape
        cleaned = unescape(str(file_content_field))

        # Try parsing as JSON array (antitrust orders use file_name key)
        try:
            files = json.loads(cleaned)
            if isinstance(files, list) and files:
                for f in files:
                    if isinstance(f, dict):
                        # Try file_name first (CCI format), then file
                        path = f.get("file_name") or f.get("file", "")
                        if path and ".pdf" in path.lower():
                            path = path.replace("\\/", "/")
                            return f"{BASE_URL}/{path}" if not path.startswith("http") else path
        except (json.JSONDecodeError, TypeError):
            pass

        # Try extracting href from HTML
        match = re.search(r'href="([^"]*\.pdf[^"]*)"', file_content_field, re.IGNORECASE)
        if match:
            url = match.group(1)
            return url if url.startswith("http") else f"{BASE_URL}/{url.lstrip('/')}"

        return None

    def _download_pdf_text(self, url: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IN/CCI",
            source_id="",
            pdf_url=url,
            table="case_law",
        ) or ""

    @staticmethod
    def _strip_html(text: str) -> str:
        """Strip HTML tags and decode entities."""
        from html import unescape
        clean = re.sub(r'<[^>]+>', '', text)
        clean = unescape(clean)
        return re.sub(r'\s+', ' ', clean).strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str or not date_str.strip():
            return None
        date_str = date_str.strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all CCI orders (antitrust + combination)."""
        yield from self._fetch_all_antitrust()
        yield from self._fetch_all_combinations()

    def _fetch_all_antitrust(self, from_date: str = "", to_date: str = "") -> Generator[Dict[str, Any], None, None]:
        """Fetch all antitrust orders with pagination."""
        start = 0
        length = 500
        total = None

        while True:
            logger.info("Fetching antitrust orders: start=%d, length=%d", start, length)
            result = self._fetch_antitrust_orders(start=start, length=length,
                                                   from_date=from_date, to_date=to_date)

            if total is None:
                total = result.get("recordsTotal", 0)
                logger.info("Total antitrust orders: %d", total)

            records = result.get("data", [])
            if not records:
                break

            for record in records:
                record["_order_type"] = "antitrust"
                pdf_url = self._extract_pdf_url(record.get("file_content", ""))
                if pdf_url:
                    record["_pdf_url"] = pdf_url
                    time.sleep(1.5)
                    text = self._download_pdf_text(pdf_url)
                    if text:
                        record["_full_text"] = text
                    else:
                        logger.warning("No text extracted for antitrust order %s", record.get("case_no", "?"))
                yield record

            start += length
            if start >= total:
                break
            time.sleep(2.0)

    def _fetch_all_combinations(self, from_date: str = "", to_date: str = "") -> Generator[Dict[str, Any], None, None]:
        """Fetch all combination orders with pagination."""
        start = 0
        length = 500
        total = None

        while True:
            logger.info("Fetching combination orders: start=%d, length=%d", start, length)
            result = self._fetch_combination_orders(start=start, length=length,
                                                     from_date=from_date, to_date=to_date)

            if total is None:
                total = result.get("recordsTotal", 0)
                logger.info("Total combination orders: %d", total)

            records = result.get("data", [])
            if not records:
                break

            for record in records:
                record["_order_type"] = "combination"
                # Try order_file_content first, then summary_file_content
                pdf_url = self._extract_pdf_url(record.get("order_file_content", ""))
                if not pdf_url:
                    pdf_url = self._extract_pdf_url(record.get("summary_file_content", ""))
                if pdf_url:
                    record["_pdf_url"] = pdf_url
                    time.sleep(1.5)
                    text = self._download_pdf_text(pdf_url)
                    if text:
                        record["_full_text"] = text
                    else:
                        logger.warning("No text for combination order %s", record.get("combination_no", "?"))
                yield record

            start += length
            if start >= total:
                break
            time.sleep(2.0)

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Fetch orders updated since a given date."""
        from_date = since.strftime("%d/%m/%Y")
        to_date = datetime.now().strftime("%d/%m/%Y")
        yield from self._fetch_all_antitrust(from_date=from_date, to_date=to_date)
        yield from self._fetch_all_combinations(from_date=from_date, to_date=to_date)

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a raw CCI order record to standard schema."""
        order_type = raw.get("_order_type", "antitrust")

        if order_type == "antitrust":
            record_id = raw.get("id", "")
            case_no = (raw.get("case_no") or "").strip()
            # description has parties, title has section type (with HTML)
            description = self._strip_html(raw.get("description") or "")
            section_type = self._strip_html(raw.get("title") or "")
            title = f"Case No. {case_no} - {description}" if description else f"Case No. {case_no}"
            order_date = self._parse_date(raw.get("main_order_date") or raw.get("order_date", ""))
            case_type = (raw.get("type") or "").strip()
            doc_id = f"CCI-AT-{record_id}" if record_id else f"CCI-AT-{case_no}"
        else:
            record_id = raw.get("id", "")
            case_no = (raw.get("combination_no") or "").strip()
            party_name = self._strip_html(raw.get("party_name") or "")
            title = f"Combination {case_no} - {party_name}" if party_name else f"Combination {case_no}"
            description = ""
            section_type = ""
            order_date = self._parse_date(raw.get("decision_date", ""))
            case_type = (raw.get("form_type") or "").strip()
            doc_id = f"CCI-CB-{record_id}" if record_id else f"CCI-CB-{case_no}"

        text = raw.get("_full_text", "")
        if not text:
            return None

        pdf_url = raw.get("_pdf_url", "")
        url = pdf_url or f"{BASE_URL}/antitrust/orders"

        normalized = {
            "_id": doc_id,
            "_source": "IN/CCI",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title or case_no,
            "text": text,
            "date": order_date,
            "url": url,
            "case_no": case_no,
            "order_type": order_type,
            "case_type": case_type,
        }

        if order_type == "antitrust" and description:
            normalized["parties"] = description
            if section_type:
                normalized["section_type"] = section_type

        return normalized


# ----- CLI -----
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="IN/CCI bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full initial pull")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")

    upd = sub.add_parser("update", help="Fetch recent orders")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")
    upd.add_argument("--full", action="store_true", help="Fetch all records")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = CCIScraper()

    if args.command == "test":
        logger.info("Testing CCI connectivity...")
        scraper._get_csrf_token()
        result = scraper._fetch_antitrust_orders(start=0, length=1)
        total = result.get("recordsTotal", 0)
        logger.info("Antitrust orders available: %d", total)
        result2 = scraper._fetch_combination_orders(start=0, length=1)
        total2 = result2.get("recordsTotal", 0)
        logger.info("Combination orders available: %d", total2)
        logger.info("Test PASSED — %d total orders accessible", total + total2)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        limit = 15 if args.sample else 999999

        for raw in scraper.fetch_all():
            rec = scraper.normalize(raw)
            if rec:
                count += 1
                out_path = sample_dir / f"{rec['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                logger.info("[%d] Saved %s (%d chars text)", count, rec["_id"],
                            len(rec.get("text", "")))
                if count >= limit:
                    break

        logger.info("Bootstrap complete: %d records saved to %s", count, sample_dir)

    elif args.command == "update":
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0

        for raw in scraper.fetch_updates(since):
            rec = scraper.normalize(raw)
            if rec:
                count += 1
                out_path = sample_dir / f"{rec['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, ensure_ascii=False, indent=2)
                logger.info("[%d] Saved %s", count, rec["_id"])

        logger.info("Update complete: %d records saved", count)

    else:
        parser.print_help()
