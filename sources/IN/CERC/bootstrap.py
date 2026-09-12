#!/usr/bin/env python3
"""
IN/CERC -- Central Electricity Regulatory Commission

Fetches orders and regulations from CERC with full text extracted from PDFs.

Strategy:
  - Parse year-based HTML index pages for orders (2017-2025)
  - Parse consolidated regulations page for gazette notifications
  - Download PDFs and extract full text using pdfplumber
  - Each order row has: petition_no, subject, date, category, PDF link

Data:
  - ~4800+ orders across 2017-2025
  - ~200+ regulations with gazette PDFs
  - All documents are PDFs
  - No authentication required

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
logger = logging.getLogger("legal-data-hunter.IN.CERC")

BASE_URL = "https://cercind.gov.in"

# Orders year pages: 2017-2025 use recent_ordersYYYY.html with table format
ORDER_YEARS = list(range(2017, 2026))

REGULATIONS_URL = f"{BASE_URL}/Current_reg.html"


class CERCScraper(BaseScraper):
    """Scraper for IN/CERC -- Central Electricity Regulatory Commission."""

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
        """Parse DD.MM.YYYY date format to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        # DD.MM.YYYY
        match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", date_str)
        if match:
            d, m, y = match.groups()
            try:
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
            except ValueError:
                pass
        return None

    def _fetch_orders_for_year(self, year: int) -> List[Dict]:
        """Fetch all orders from a given year's index page."""
        self.rate_limiter.wait()
        url = f"{BASE_URL}/recent_orders{year}.html"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch orders for %d: %s", year, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        tables = soup.find_all("table")
        if not tables:
            logger.warning("No table found for year %d", year)
            return []

        records = []
        rows = tables[0].find_all("tr")
        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            petition_no = cells[1].get_text(strip=True) if len(cells) > 1 else ""
            subject_cell = cells[2] if len(cells) > 2 else None
            date_str = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            category = cells[5].get_text(strip=True) if len(cells) > 5 else ""

            if not subject_cell:
                continue

            title = subject_cell.get_text(strip=True)
            link = subject_cell.find("a", href=True)
            if not link:
                continue

            href = link["href"]
            if not href.lower().endswith(".pdf"):
                continue

            pdf_url = urljoin(BASE_URL + "/", href)
            date_iso = self._parse_date(date_str)

            records.append({
                "petition_no": petition_no,
                "title": title,
                "pdf_url": pdf_url,
                "date": date_iso,
                "category": category,
                "year": year,
                "doc_type": "order",
            })

        return records

    def _fetch_regulations(self) -> List[Dict]:
        """Fetch all regulations from the consolidated regulations page."""
        self.rate_limiter.wait()
        try:
            resp = self.session.get(REGULATIONS_URL, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch regulations: %s", e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        tables = soup.find_all("table")
        if not tables:
            return []

        records = []
        rows = tables[0].find_all("tr")
        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            sl_no = cells[0].get_text(strip=True).rstrip(".")
            title = cells[1].get_text(strip=True)
            date_str = cells[3].get_text(strip=True) if len(cells) > 3 else ""

            # Find the first PDF link (gazette notification)
            link = None
            for a in row.find_all("a", href=True):
                href = a["href"]
                if href.lower().endswith(".pdf"):
                    link = a
                    break

            if not link:
                continue

            pdf_url = urljoin(BASE_URL + "/", link["href"])
            date_iso = self._parse_date(date_str)

            records.append({
                "petition_no": f"Reg-{sl_no}",
                "title": title,
                "pdf_url": pdf_url,
                "date": date_iso,
                "category": "Regulation",
                "year": int(date_iso[:4]) if date_iso else 0,
                "doc_type": "regulation",
            })

        return records

    def _make_id(self, rec: dict) -> str:
        """Create unique ID from petition number or PDF URL."""
        if rec.get("petition_no"):
            clean = re.sub(r"[^\w\-]", "_", rec["petition_no"])
            return f"CERC-{clean}"
        return f"CERC-{hashlib.md5(rec['pdf_url'].encode()).hexdigest()[:12]}"

    def _download_pdf_text(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="IN/CERC",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw CERC record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "IN/CERC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "petition_no": raw.get("petition_no", ""),
            "category": raw.get("category", ""),
            "doc_type": raw.get("doc_type", "order"),
        }

    def _process_record(self, rec: dict) -> Optional[dict]:
        """Process a single record: download PDF and extract text."""
        doc_id = self._make_id(rec)
        text = self._download_pdf_text(rec["pdf_url"], doc_id)
        if not text:
            logger.warning("No text for: %s", rec["title"][:80])
            return None

        raw = {
            "_id": doc_id,
            "title": rec["title"],
            "text": text,
            "date": rec.get("date"),
            "pdf_url": rec["pdf_url"],
            "petition_no": rec.get("petition_no", ""),
            "category": rec.get("category", ""),
            "doc_type": rec.get("doc_type", "order"),
        }
        return self.normalize(raw)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all CERC orders and regulations."""
        # Orders by year
        for year in ORDER_YEARS:
            logger.info("Fetching orders for %d", year)
            records = self._fetch_orders_for_year(year)
            logger.info("Year %d: %d orders found", year, len(records))
            for rec in records:
                doc = self._process_record(rec)
                if doc:
                    yield doc

        # Regulations
        logger.info("Fetching regulations")
        regs = self._fetch_regulations()
        logger.info("Regulations: %d found", len(regs))
        for rec in regs:
            doc = self._process_record(rec)
            if doc:
                yield doc

    def fetch_updates(self, since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent orders (current year only)."""
        current_year = datetime.now().year
        logger.info("Fetching updates for %d", current_year)
        records = self._fetch_orders_for_year(current_year)
        logger.info("Year %d: %d orders", current_year, len(records))
        for rec in records:
            doc = self._process_record(rec)
            if doc:
                yield doc

    def test_connection(self) -> bool:
        """Quick connectivity test."""
        try:
            records = self._fetch_orders_for_year(2025)
            logger.info("Connection OK: %d orders found for 2025", len(records))
            return len(records) > 0
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

            # 10 from orders (5 from 2025, 5 from 2024)
            for year in [2025, 2024]:
                if count >= 10:
                    break
                records = self._fetch_orders_for_year(year)
                logger.info("Year %d: %d orders on page", year, len(records))
                for rec in records[:6]:
                    if count >= 10:
                        break
                    doc = self._process_record(rec)
                    if doc:
                        fname = re.sub(r'[^\w\-.]', '_', f"{doc['_id'][:80]}.json")
                        with open(sample_dir / fname, "w", encoding="utf-8") as f:
                            json.dump(doc, f, ensure_ascii=False, indent=2)
                        count += 1
                        logger.info("[%d/%d] %s: %s (%d chars)",
                                    count, target, doc.get("category", ""), doc["title"][:50], len(doc["text"]))

            # 5 from regulations
            regs = self._fetch_regulations()
            logger.info("Regulations: %d found", len(regs))
            for rec in regs[:7]:
                if count >= target:
                    break
                doc = self._process_record(rec)
                if doc:
                    fname = re.sub(r'[^\w\-.]', '_', f"{doc['_id'][:80]}.json")
                    with open(sample_dir / fname, "w", encoding="utf-8") as f:
                        json.dump(doc, f, ensure_ascii=False, indent=2)
                    count += 1
                    logger.info("[%d/%d] regulation: %s (%d chars)",
                                count, target, doc["title"][:50], len(doc["text"]))

            logger.info("Sample bootstrap complete: %d records saved", count)
            return count
        else:
            count = 0
            for doc in self.fetch_all():
                self.storage.save(doc)
                count += 1
                if count % 50 == 0:
                    logger.info("Progress: %d records saved", count)
            logger.info("Full bootstrap complete: %d records saved", count)
            return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="IN/CERC Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CERCScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
