#!/usr/bin/env python3
"""
IN/IRDAI -- Insurance Regulatory and Development Authority of India

Fetches circulars, guidelines, and notifications from IRDAI with full text
extracted from PDFs.

Strategy:
  - Scrape paginated Liferay CMS listing pages for each document category
  - Parse HTML table rows: title, date, category link, PDF download URL
  - Download PDFs and extract full text via common.pdf_extract

Categories (all doctrine):
  circulars      ~589 documents
  guidelines     ~93 documents
  notifications  ~41 documents

Data:
  - ~720+ regulatory documents total
  - All PDFs with selectable text (bilingual Hindi/English)
  - No authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch first page of each category
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
logger = logging.getLogger("legal-data-hunter.IN.IRDAI")

BASE_URL = "https://irdai.gov.in"

# Liferay portlet pagination parameter prefix
PORTLET_PREFIX = "_com_irdai_document_media_IRDAIDocumentMediaPortlet_"

# Categories to fetch: (url_path, category_label)
CATEGORIES = [
    ("circulars", "circular"),
    ("guidelines", "guideline"),
    ("notifications", "notification"),
]

ITEMS_PER_PAGE = 60  # max allowed by the Liferay portlet


class IRDAIScraper(BaseScraper):
    """Scraper for IN/IRDAI -- Insurance Regulatory and Development Authority of India."""

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
        """Parse DD-MM-YYYY to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y", "%d %b %Y", "%d %B %Y"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _get_total_pages(self, html: str) -> int:
        """Extract total page count from pagination text 'Showing X - Y of Z results'."""
        soup = BeautifulSoup(html, "html.parser")
        # Look for "Showing X - Y of Z results"
        text = soup.get_text()
        match = re.search(r"Showing\s+\d+\s*-\s*\d+\s+of\s+(\d+)\s+results", text)
        if match:
            total = int(match.group(1))
            return (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
        # Fallback: look for highest page number in pagination links
        max_page = 1
        for a in soup.find_all("a", href=True):
            href = a["href"]
            m = re.search(rf"{re.escape(PORTLET_PREFIX)}cur=(\d+)", href)
            if m:
                p = int(m.group(1))
                if p > max_page:
                    max_page = p
        return max_page

    def _scrape_listing_page(self, category_path: str, category_label: str,
                             page: int) -> List[Dict]:
        """Scrape a single page of a document listing category."""
        url = f"{BASE_URL}/{category_path}"
        params = {
            f"{PORTLET_PREFIX}cur": str(page),
            f"{PORTLET_PREFIX}delta": str(ITEMS_PER_PAGE),
        }
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s page %d: %s", category_path, page, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select("table tbody tr")
        records = []

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            # Cell 2: title text
            title = cells[2].get_text(strip=True)
            if not title:
                continue

            # Cell 3: category link with documentId
            detail_url = None
            doc_id_num = None
            for a in cells[3].find_all("a", href=True):
                href = a["href"]
                if "documentId" in href:
                    detail_url = href if href.startswith("http") else urljoin(BASE_URL + "/", href)
                    m = re.search(r"documentId=(\d+)", href)
                    if m:
                        doc_id_num = m.group(1)
                    break

            # Cell 4: date
            date_str = cells[4].get_text(strip=True)
            date_iso = self._parse_date(date_str)

            # Cell 5: PDF download links
            pdf_url = None
            if len(cells) > 5:
                for a in cells[5].find_all("a", href=True):
                    href = a["href"]
                    if "/documents/" in href:
                        pdf_url = href if href.startswith("http") else urljoin(BASE_URL + "/", href)
                        break

            if not pdf_url:
                logger.debug("No PDF URL for: %s", title[:60])
                continue

            records.append({
                "title": title,
                "pdf_url": pdf_url,
                "detail_url": detail_url or "",
                "document_id": doc_id_num or "",
                "date": date_iso,
                "date_raw": date_str,
                "category": category_label,
            })

        return records

    def _scrape_all_pages(self, category_path: str, category_label: str) -> List[Dict]:
        """Scrape all pages for a category."""
        # Fetch first page to determine total pages
        url = f"{BASE_URL}/{category_path}"
        params = {
            f"{PORTLET_PREFIX}cur": "1",
            f"{PORTLET_PREFIX}delta": str(ITEMS_PER_PAGE),
        }
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning("Failed to fetch %s: %s", category_path, e)
            return []

        total_pages = self._get_total_pages(resp.text)
        logger.info("%s: %d pages found", category_path, total_pages)

        # Parse first page
        all_records = self._parse_page_html(resp.text, category_label)
        logger.info("%s page 1: %d records", category_path, len(all_records))

        # Remaining pages
        for page in range(2, total_pages + 1):
            records = self._scrape_listing_page(category_path, category_label, page)
            all_records.extend(records)
            logger.info("%s page %d: %d records", category_path, page, len(records))

        return all_records

    def _parse_page_html(self, html: str, category_label: str) -> List[Dict]:
        """Parse already-fetched HTML into records."""
        soup = BeautifulSoup(html, "html.parser")
        rows = soup.select("table tbody tr")
        records = []

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 5:
                continue

            title = cells[2].get_text(strip=True)
            if not title:
                continue

            detail_url = None
            doc_id_num = None
            for a in cells[3].find_all("a", href=True):
                href = a["href"]
                if "documentId" in href:
                    detail_url = href if href.startswith("http") else urljoin(BASE_URL + "/", href)
                    m = re.search(r"documentId=(\d+)", href)
                    if m:
                        doc_id_num = m.group(1)
                    break

            date_str = cells[4].get_text(strip=True)
            date_iso = self._parse_date(date_str)

            pdf_url = None
            if len(cells) > 5:
                for a in cells[5].find_all("a", href=True):
                    href = a["href"]
                    if "/documents/" in href:
                        pdf_url = href if href.startswith("http") else urljoin(BASE_URL + "/", href)
                        break

            if not pdf_url:
                continue

            records.append({
                "title": title,
                "pdf_url": pdf_url,
                "detail_url": detail_url or "",
                "document_id": doc_id_num or "",
                "date": date_iso,
                "date_raw": date_str,
                "category": category_label,
            })

        return records

    def _make_id(self, rec: dict) -> str:
        """Create unique ID from category + document hash."""
        cat = rec.get("category", "unknown")
        if rec.get("document_id"):
            return f"IRDAI-{cat}-{rec['document_id']}"
        pdf_hash = hashlib.md5(rec["pdf_url"].encode()).hexdigest()[:12]
        return f"IRDAI-{cat}-{pdf_hash}"

    def _download_pdf_text(self, pdf_url: str, doc_id: str) -> Optional[str]:
        """Extract text from PDF."""
        return extract_pdf_markdown(
            source="IN/IRDAI",
            source_id=doc_id,
            pdf_url=pdf_url,
            table="doctrine",
        ) or ""

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw IRDAI record into standard schema."""
        return {
            "_id": raw["_id"],
            "_source": "IN/IRDAI",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("detail_url") or raw["pdf_url"],
            "category": raw.get("category", ""),
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
            "detail_url": rec.get("detail_url", ""),
            "category": rec.get("category", ""),
        }
        return self.normalize(raw)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all IRDAI documents across all categories."""
        for cat_path, cat_label in CATEGORIES:
            logger.info("Fetching category: %s", cat_path)
            records = self._scrape_all_pages(cat_path, cat_label)
            logger.info("%s: %d records found", cat_path, len(records))
            for rec in records:
                doc = self._process_record(rec)
                if doc:
                    yield doc

    def fetch_updates(self, since: Optional[datetime] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch first page of each category for updates."""
        for cat_path, cat_label in CATEGORIES:
            logger.info("Fetching updates: %s page 1", cat_path)
            records = self._scrape_listing_page(cat_path, cat_label, 1)
            for rec in records:
                doc = self._process_record(rec)
                if doc:
                    yield doc


# ── CLI entry point ─────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="IN/IRDAI bootstrap scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only a small sample (15 records)")
    args = parser.parse_args()

    scraper = IRDAIScraper()

    if args.command == "test":
        logger.info("Testing connectivity to irdai.gov.in ...")
        try:
            resp = scraper.session.get(f"{BASE_URL}/circulars", timeout=15)
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
