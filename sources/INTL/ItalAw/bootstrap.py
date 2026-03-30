#!/usr/bin/env python3
"""
INTL/ItalAw -- italaw: Investment Treaty Arbitration

Fetches investment treaty arbitration awards, decisions, and orders from italaw.com.

Strategy:
  - Parse sitemap.xml?page=1 for all /cases/{id} URLs (~1,787 cases)
  - Scrape each case page for metadata (case title, type, treaties) and PDF links
  - Download PDFs and extract full text via PyMuPDF
  - ~11,000 documents total; 10s crawl delay per robots.txt

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import fitz  # PyMuPDF

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ItalAw")

SITEMAP_URL = "https://italaw.com/sitemap.xml?page=1"
BASE_URL = "https://www.italaw.com"

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    "january": "01", "february": "02", "march": "03", "april": "04",
    "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


class ItalAwScraper(BaseScraper):
    """
    Scraper for INTL/ItalAw -- italaw Investment Treaty Arbitration.
    Country: INTL
    URL: https://www.italaw.com/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })

    def _get_case_urls_from_sitemap(self) -> list[dict]:
        """Parse sitemap for all /cases/{id} URLs (excluding /cases/documents/)."""
        logger.info(f"Fetching sitemap: {SITEMAP_URL}")
        r = self.session.get(SITEMAP_URL, timeout=60)
        r.raise_for_status()

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        root = ET.fromstring(r.text)

        cases = []
        for url_elem in root.findall(".//sm:url", ns):
            loc = url_elem.find("sm:loc", ns).text
            lastmod_elem = url_elem.find("sm:lastmod", ns)
            lastmod = lastmod_elem.text if lastmod_elem is not None else None

            # Only case pages, not document pages
            if "/cases/" in loc and "/documents/" not in loc:
                case_id = loc.rstrip("/").split("/")[-1]
                if case_id.isdigit():
                    cases.append({
                        "url": loc,
                        "case_id": case_id,
                        "lastmod": lastmod,
                    })

        logger.info(f"Found {len(cases)} case pages in sitemap")
        return cases

    def _parse_date(self, date_text: str) -> Optional[str]:
        """Parse date like '19Feb 2010' or '28Jul 2010' to ISO format."""
        if not date_text:
            return None
        date_text = date_text.strip()
        # Pattern: day + month_abbrev + year (e.g., "19Feb 2010")
        m = re.match(r"(\d{1,2})(\w{3,})\s+(\d{4})", date_text)
        if m:
            day = m.group(1).zfill(2)
            month_str = m.group(2).lower()
            year = m.group(3)
            month = MONTH_MAP.get(month_str[:3])
            if month:
                return f"{year}-{month}-{day}"
        # Try "Month Day, Year" format
        m = re.match(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", date_text)
        if m:
            month_str = m.group(1).lower()
            day = m.group(2).zfill(2)
            year = m.group(3)
            month = MONTH_MAP.get(month_str[:3])
            if month:
                return f"{year}-{month}-{day}"
        return None

    def _scrape_case_page(self, case_url: str) -> dict:
        """Scrape a case page for metadata and document list."""
        from bs4 import BeautifulSoup

        r = self.session.get(case_url, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        # Case title from page title (strip " | italaw")
        title_tag = soup.find("title")
        case_title = ""
        if title_tag:
            case_title = title_tag.get_text(strip=True)
            case_title = re.sub(r"\s*\|\s*italaw\s*$", "", case_title).strip()

        # Case metadata from the cases view
        cases_view = soup.find("div", class_=lambda c: c and "view-id-cases" in c)
        case_type = ""
        treaty = ""
        if cases_view:
            ct_div = cases_view.find("div", class_="views-field-field-case-type")
            if ct_div:
                fc = ct_div.find("div", class_="field-content")
                case_type = fc.get_text(strip=True) if fc else ""

            tr_div = cases_view.find("div", class_="views-field-field-case-treaties")
            if tr_div:
                fc = tr_div.find("div", class_="field-content")
                treaty = fc.get_text(strip=True) if fc else ""

        # Parse documents from the documents view
        doc_view = soup.find("div", class_=lambda c: c and "view-id-documents" in c)
        documents = []
        if doc_view:
            rows = doc_view.find_all("div", class_="views-row")
            for row in rows:
                # Date
                date_div = row.find("div", class_="views-field-field-case-document-date")
                date_text = date_div.get_text(strip=True) if date_div else ""
                doc_date = self._parse_date(date_text)

                # Document title from PDF link field
                doc_title = ""
                pdf_url = ""
                doc_file_div = row.find("div", class_="views-field-field-case-doc-file")
                if doc_file_div:
                    # Get PDF link
                    pdf_link = doc_file_div.find("a", href=lambda h: h and ".pdf" in h.lower())
                    if pdf_link:
                        pdf_url = pdf_link.get("href", "")
                        if pdf_url and not pdf_url.startswith("http"):
                            pdf_url = BASE_URL + pdf_url
                    doc_title = doc_file_div.get_text(strip=True)

                # If no PDF in doc-file field, check no-pdf field for title
                if not doc_title:
                    no_pdf_div = row.find("div", class_="views-field-field-case-document-no-pdf-")
                    if no_pdf_div:
                        doc_title = no_pdf_div.get_text(strip=True)

                # Skip entries without a PDF
                if not pdf_url:
                    continue

                documents.append({
                    "doc_title": doc_title,
                    "date": doc_date,
                    "pdf_url": pdf_url,
                })

        return {
            "case_title": case_title,
            "case_type": case_type,
            "treaty": treaty,
            "case_url": case_url,
            "documents": documents,
        }

    def _download_and_extract_pdf(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text using PyMuPDF."""
        try:
            r = self.session.get(pdf_url, timeout=120)
            r.raise_for_status()

            if len(r.content) < 500:
                return None

            # Skip very large PDFs (>50MB)
            if len(r.content) > 50 * 1024 * 1024:
                logger.warning(f"PDF too large ({len(r.content)} bytes): {pdf_url}")
                return None

            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                tmp.write(r.content)
                tmp.flush()

                doc = fitz.open(tmp.name)
                text_parts = []
                for page in doc:
                    text_parts.append(page.get_text())
                doc.close()

            text = "\n".join(text_parts).strip()
            return text if len(text) >= 100 else None

        except Exception as e:
            logger.warning(f"Failed to extract text from {pdf_url}: {e}")
            return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("text", "").strip()
        if not text:
            return None

        doc_title = raw.get("doc_title", "").strip()
        case_title = raw.get("case_title", "").strip()
        pdf_url = raw.get("pdf_url", "")

        full_title = doc_title
        if case_title and doc_title:
            full_title = f"{doc_title} — {case_title}"
        elif case_title:
            full_title = case_title

        # Generate ID from PDF filename
        pdf_name = pdf_url.split("/")[-1].replace(".pdf", "") if pdf_url else ""
        doc_id = re.sub(r"[^a-zA-Z0-9_-]", "_", pdf_name) if pdf_name else str(hash(full_title))

        return {
            "_id": f"ItalAw-{doc_id}",
            "_source": "INTL/ItalAw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("case_url", ""),
            "case_title": case_title,
            "case_type": raw.get("case_type", ""),
            "treaty": raw.get("treaty", ""),
            "pdf_url": pdf_url,
            "court": "Investment Treaty Arbitration (various tribunals)",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all italaw documents."""
        cases = self._get_case_urls_from_sitemap()
        logger.info(f"Processing {len(cases)} cases")

        total_docs = 0
        for i, case_info in enumerate(cases):
            case_url = case_info["url"]
            if not case_url.startswith("http"):
                case_url = BASE_URL + case_url

            logger.info(f"[{i+1}/{len(cases)}] Scraping: {case_url}")

            try:
                case_data = self._scrape_case_page(case_url)
            except Exception as e:
                logger.warning(f"Failed to scrape case page {case_url}: {e}")
                time.sleep(10)
                continue

            if not case_data["documents"]:
                logger.info(f"  No documents with PDFs")
                time.sleep(10)
                continue

            for doc in case_data["documents"]:
                logger.info(f"  Downloading: {doc['doc_title'][:60]}")

                text = self._download_and_extract_pdf(doc["pdf_url"])
                if text:
                    yield {
                        "doc_title": doc["doc_title"],
                        "date": doc.get("date"),
                        "text": text,
                        "pdf_url": doc["pdf_url"],
                        "case_title": case_data["case_title"],
                        "case_type": case_data["case_type"],
                        "treaty": case_data["treaty"],
                        "case_url": case_url,
                    }
                    total_docs += 1
                else:
                    logger.warning(f"  No text extracted: {doc['doc_title'][:60]}")

                time.sleep(10)  # Respect crawl-delay: 10

            time.sleep(10)

        logger.info(f"Total documents fetched: {total_docs}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents from recently modified cases using sitemap lastmod."""
        since_str = since.strftime("%Y-%m-%dT%H:%M") if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching updates since {since_str}")

        cases = self._get_case_urls_from_sitemap()
        recent = [c for c in cases if c.get("lastmod") and c["lastmod"] >= since_str]
        logger.info(f"Found {len(recent)} cases modified since {since_str}")

        for case_info in recent:
            case_url = case_info["url"]
            if not case_url.startswith("http"):
                case_url = BASE_URL + case_url

            try:
                case_data = self._scrape_case_page(case_url)
            except Exception as e:
                logger.warning(f"Failed: {case_url}: {e}")
                time.sleep(10)
                continue

            for doc in case_data["documents"]:
                text = self._download_and_extract_pdf(doc["pdf_url"])
                if text:
                    yield {
                        "doc_title": doc["doc_title"],
                        "date": doc.get("date"),
                        "text": text,
                        "pdf_url": doc["pdf_url"],
                        "case_title": case_data["case_title"],
                        "case_type": case_data["case_type"],
                        "treaty": case_data["treaty"],
                        "case_url": case_url,
                    }
                time.sleep(10)

            time.sleep(10)


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="INTL/ItalAw data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = ItalAwScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            cases = scraper._get_case_urls_from_sitemap()
            logger.info(f"OK: {len(cases)} case pages in sitemap")

            # Test one case page
            if cases:
                case_data = scraper._scrape_case_page(cases[0]["url"])
                logger.info(f"Case: {case_data['case_title'][:80]}")
                logger.info(f"Documents: {len(case_data['documents'])}")

                if case_data["documents"]:
                    text = scraper._download_and_extract_pdf(case_data["documents"][0]["pdf_url"])
                    if text:
                        logger.info(f"PDF text: {len(text)} chars")
                        logger.info(f"Preview: {text[:200]}")
                    else:
                        logger.warning("PDF text extraction failed")

            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
