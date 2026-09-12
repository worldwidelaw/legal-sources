#!/usr/bin/env python3
"""
IN/IBBI -- Insolvency and Bankruptcy Board of India

Fetches orders and regulatory documents from IBBI with full text from PDFs.

Strategy:
  - Parse paginated HTML tables for orders across 9 court types
  - Parse paginated HTML tables for legal framework docs (circulars,
    notifications, guidelines)
  - Extract PDF URLs from onclick="newwindow1('/uploads/order/...')"
  - Download PDFs and extract full text via common.pdf_extract

Court types (case_law):
  supreme-court, high-courts, nclat, nclt, drat, drts, ibbi, ipa-rvo,
  other-courts

Legal framework (doctrine):
  circulars, notifications, guidelines

Data:
  - ~40,000+ orders across all court types
  - ~300+ regulatory documents
  - All documents are PDFs, no auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent documents
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
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
logger = logging.getLogger("legal-data-hunter.IN.IBBI")

BASE_URL = "https://ibbi.gov.in"

# Court order categories (case_law)
ORDER_CATEGORIES = [
    "supreme-court",
    "high-courts",
    "nclat",
    "nclt",
    "drat",
    "drts",
    "ibbi",
    "ipa-rvo",
    "other-courts",
]

# Legal framework categories (doctrine)
FRAMEWORK_CATEGORIES = [
    "circulars",
    "notifications",
    "guidelines",
]


class IBBIScraper(BaseScraper):
    """Scraper for IN/IBBI -- Insolvency and Bankruptcy Board of India."""

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

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse 'DD Mon, YYYY' or 'DD Month, YYYY' to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%d %b, %Y", "%d %B, %Y", "%d %b %Y", "%d %B %Y",
                    "%d-%m-%Y", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _extract_pdf_url(self, cell) -> Optional[str]:
        """Extract PDF URL from an onclick handler or href in a table cell."""
        for a in cell.find_all("a"):
            onclick = a.get("onclick", "")
            # Pattern: newwindow1('/uploads/order/xxx.pdf') or newwindow1('https://ibbi.gov.in//uploads/...')
            match = re.search(r"newwindow1\(['\"]([^'\"]+\.pdf)['\"]", onclick)
            if match:
                url = match.group(1)
                if url.startswith("http"):
                    return url
                return urljoin(BASE_URL + "/", url.lstrip("/"))
            # Direct href
            href = a.get("href", "")
            if href.endswith(".pdf") and href != "javascript:void(0)":
                if href.startswith("http"):
                    return href
                return urljoin(BASE_URL + "/", href.lstrip("/"))
        return None

    def _scrape_orders_page(self, category: str, page: int) -> List[Dict]:
        """Scrape a single page of orders for a given court category."""
        url = f"{BASE_URL}/orders/{category}"
        params = {"page": page} if page > 1 else {}
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s page %d: %s", category, page, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="reporttable")
        if not table:
            return []

        records = []
        for row in table.find_all("tr")[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            date_str = cells[1].get_text(strip=True)
            subject_cell = cells[2]
            remarks = cells[3].get_text(strip=True) if len(cells) > 3 else ""

            title = subject_cell.get_text(strip=True)
            # Remove trailing file size like "(487.04 KB)"
            title = re.sub(r"\s*\(\d+[\.\d]*\s*[KMG]?B\)\s*$", "", title)

            pdf_url = self._extract_pdf_url(subject_cell)
            if not pdf_url:
                continue

            records.append({
                "title": title,
                "pdf_url": pdf_url,
                "date": self._parse_date(date_str),
                "date_raw": date_str,
                "court": category,
                "remarks": remarks,
                "doc_type": "case_law",
            })

        return records

    def _scrape_framework_page(self, category: str, page: int) -> List[Dict]:
        """Scrape a single page of legal framework documents."""
        url = f"{BASE_URL}/legal-framework/{category}"
        params = {"page": page} if page > 1 else {}
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s page %d: %s", category, page, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="reporttable")
        if not table:
            return []

        records = []
        for row in table.find_all("tr")[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            date_str = cells[1].get_text(strip=True)
            subject_text = cells[2].get_text(strip=True)

            # PDF may be in the subject cell or in separate English/Hindi columns
            pdf_url = None
            for cell in cells[2:]:
                pdf_url = self._extract_pdf_url(cell)
                if pdf_url:
                    break

            if not pdf_url:
                continue

            # Clean title
            title = re.sub(r"\s*\(\d+[\.\d]*\s*[KMG]?B\)\s*$", "", subject_text)

            records.append({
                "title": title,
                "pdf_url": pdf_url,
                "date": self._parse_date(date_str),
                "date_raw": date_str,
                "court": category,
                "remarks": "",
                "doc_type": "doctrine",
            })

        return records

    def _get_max_page(self, html: str) -> int:
        """Extract the last page number from pagination links."""
        soup = BeautifulSoup(html, "html.parser")
        last_page = 1
        for a in soup.find_all("a", href=True):
            match = re.search(r"page=(\d+)", a["href"])
            if match:
                p = int(match.group(1))
                if p > last_page:
                    last_page = p
        return last_page

    def _scrape_all_pages(self, category: str, is_framework: bool = False) -> List[Dict]:
        """Scrape all pages for a category."""
        url_base = "legal-framework" if is_framework else "orders"
        url = f"{BASE_URL}/{url_base}/{category}"

        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s: %s", category, e)
            return []

        max_page = self._get_max_page(resp.text)
        logger.info("%s: %d pages found", category, max_page)

        # Parse first page from already-fetched HTML
        soup = BeautifulSoup(resp.text, "html.parser")
        table = soup.find("table", class_="reporttable")
        all_records = []
        if table:
            if is_framework:
                all_records.extend(self._scrape_framework_page(category, 1))
            else:
                all_records.extend(self._scrape_orders_page(category, 1))

        # Remaining pages
        for page in range(2, max_page + 1):
            if is_framework:
                records = self._scrape_framework_page(category, page)
            else:
                records = self._scrape_orders_page(category, page)
            all_records.extend(records)

        return all_records

    def _make_id(self, rec: dict) -> str:
        """Create unique ID from court type + PDF hash."""
        court = rec.get("court", "unknown")
        pdf_hash = hashlib.md5(rec["pdf_url"].encode()).hexdigest()[:12]
        return f"IBBI-{court}-{pdf_hash}"

    def _download_pdf_text(self, pdf_url: str, doc_id: str, table: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IN/IBBI",
            source_id=doc_id,
            pdf_url=pdf_url,
            table=table,
        ) or ""

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw IBBI record into standard schema."""
        doc_type = raw.get("doc_type", "case_law")
        _type = "doctrine" if doc_type == "doctrine" else "case_law"
        return {
            "_id": raw["_id"],
            "_source": "IN/IBBI",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "court": raw.get("court", ""),
            "remarks": raw.get("remarks", ""),
            "doc_type": doc_type,
        }

    def _process_record(self, rec: dict) -> Optional[dict]:
        """Process a single record: download PDF and extract text."""
        doc_id = self._make_id(rec)
        table = "doctrine" if rec.get("doc_type") == "doctrine" else "case_law"
        text = self._download_pdf_text(rec["pdf_url"], doc_id, table)
        if not text:
            logger.warning("No text for: %s", rec["title"][:80])
            return None

        raw = {
            "_id": doc_id,
            "title": rec["title"],
            "text": text,
            "date": rec.get("date"),
            "pdf_url": rec["pdf_url"],
            "court": rec.get("court", ""),
            "remarks": rec.get("remarks", ""),
            "doc_type": rec.get("doc_type", "case_law"),
        }
        return self.normalize(raw)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all IBBI orders and regulatory documents."""
        # Orders by court type
        for category in ORDER_CATEGORIES:
            logger.info("Fetching orders: %s", category)
            records = self._scrape_all_pages(category, is_framework=False)
            logger.info("%s: %d orders found", category, len(records))
            for rec in records:
                doc = self._process_record(rec)
                if doc:
                    yield doc

        # Legal framework documents
        for category in FRAMEWORK_CATEGORIES:
            logger.info("Fetching framework: %s", category)
            records = self._scrape_all_pages(category, is_framework=True)
            logger.info("%s: %d documents found", category, len(records))
            for rec in records:
                doc = self._process_record(rec)
                if doc:
                    yield doc

    def fetch_updates(self, since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent orders (first page of each category)."""
        for category in ORDER_CATEGORIES:
            logger.info("Fetching updates: %s page 1", category)
            records = self._scrape_orders_page(category, 1)
            for rec in records:
                doc = self._process_record(rec)
                if doc:
                    yield doc

        for category in FRAMEWORK_CATEGORIES:
            logger.info("Fetching updates: %s page 1", category)
            records = self._scrape_framework_page(category, 1)
            for rec in records:
                doc = self._process_record(rec)
                if doc:
                    yield doc


# ── CLI entry point ─────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="IN/IBBI bootstrap scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only a small sample (15 records)")
    args = parser.parse_args()

    scraper = IBBIScraper()

    if args.command == "test":
        logger.info("Testing connectivity to ibbi.gov.in ...")
        try:
            resp = scraper.session.get(f"{BASE_URL}/orders/ibbi", timeout=15)
            resp.raise_for_status()
            logger.info("OK — status %d, %d bytes", resp.status_code, len(resp.text))
        except Exception as e:
            logger.error("FAILED: %s", e)
            sys.exit(1)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if args.sample else float("inf")

    gen = scraper.fetch_all() if args.command == "bootstrap" else scraper.fetch_updates()

    for doc in gen:
        if count >= limit:
            break
        fname = f"{doc['_id']}.json".replace("/", "_")
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info("[%d] %s — %d chars", count, doc["title"][:60], len(doc.get("text", "")))

    logger.info("Done. %d records saved to %s", count, sample_dir)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
