#!/usr/bin/env python3
"""
EU/SRB-Decisions -- Single Resolution Board Decisions

Fetches Appeal Panel decisions and resolution case documents from srb.europa.eu.

Strategy:
  1. Appeal Panel decisions: paginate thematic register (111+ decisions)
  2. Resolution case pages: scrape 6 known case pages for PDF links
  3. Download PDFs and extract text via common/pdf_extract

Endpoints:
  - Thematic register: /en/cases/thematic-register-search?page=N
  - Case pages: /en/content/{case-slug}
  - PDFs: /system/files/media/document/...

Usage:
  python bootstrap.py bootstrap          # Full bootstrap
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Connectivity test
"""

import re
import sys
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Generator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.EU.SRB-Decisions")

BASE_URL = "https://www.srb.europa.eu"
THEMATIC_REGISTER = "/en/cases/thematic-register-search"

# Known resolution case slugs (rarely changes - only a few cases per decade)
CASE_SLUGS = [
    "sberbank-dd-banka-dd",
    "banco-popular",
    "sberbank-europe-ag",
    "pnb-banka",
    "banca-popolare-di-vicenza-veneto-banca",
    "ablv-bank",
]


class SRBDecisionsScraper(BaseScraper):
    """Scraper for EU/SRB-Decisions - SRB Appeal Panel and resolution decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get(self, url: str, max_retries: int = 3) -> Optional[requests.Response]:
        """GET with retry and rate-limit handling."""
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(url, timeout=60)
                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 10))
                    logger.warning(f"Rate-limited, waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
        return None

    def _scrape_paginated(self, base_path: str, label: str) -> Generator[Dict[str, Any], None, None]:
        """Generic paginated scraper for SRB Drupal views."""
        page = 0
        while True:
            url = f"{BASE_URL}{base_path}?page={page}"
            logger.info(f"Fetching {label} page {page}...")
            resp = self._get(url)
            if not resp:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select(".views-row")
            if not rows:
                break

            found = 0
            for row in rows:
                record = self._parse_views_row(row, label)
                if record:
                    found += 1
                    yield record

            if found == 0:
                break

            # Check for next page link
            next_link = soup.select_one("ul.js-pager__items li a[href*='page=']")
            has_next = False
            for link in soup.select("ul.js-pager__items li a"):
                href = link.get("href", "")
                if f"page={page + 1}" in href:
                    has_next = True
                    break
            if not has_next:
                break
            page += 1

    def _parse_views_row(self, row, source_label: str) -> Optional[Dict[str, Any]]:
        """Parse a single .views-row from SRB Drupal pages."""
        # Find PDF link (in h3 > a or any a with .pdf href)
        pdf_link = row.find("a", href=re.compile(r"\.pdf", re.IGNORECASE))
        if not pdf_link:
            return None

        pdf_url = pdf_link["href"]
        if not pdf_url.startswith("http"):
            pdf_url = urljoin(BASE_URL, pdf_url)

        # Title from h3 or link text
        h3 = row.find("h3")
        title = h3.get_text(strip=True) if h3 else pdf_link.get_text(strip=True)
        if not title:
            title = Path(pdf_url).stem.replace("_", " ").replace("-", " ")

        # Parse metadata spans from srb-list-inline div
        pub_date = ""
        dec_date = ""
        register_num = ""
        meta_div = row.find("div", class_="srb-list-inline")
        if meta_div:
            meta_text = meta_div.get_text(" ", strip=True)
            pub_match = re.search(r"Publishing date:[\s|]*(\d{1,2}/\d{1,2}/\d{4})", meta_text)
            dec_match = re.search(r"Decision date:[\s|]*(\d{1,2}/\d{1,2}/\d{4})", meta_text)
            reg_match = re.search(r"Register number:[\s|]*([\w/]+)", meta_text)
            if pub_match:
                pub_date = pub_match.group(1)
            if dec_match:
                dec_date = dec_match.group(1)
            if reg_match:
                register_num = reg_match.group(1)

        # Use decision date if available, otherwise publishing date
        date_raw = dec_date or pub_date

        # Generate stable doc_id
        if register_num:
            doc_id = register_num.replace("/", "-")
        else:
            doc_id = Path(pdf_url).stem
        doc_id = re.sub(r"[^\w\-]", "_", doc_id)

        # Determine decision type
        decision_type = "appeal_panel" if "appeal" in source_label.lower() else "public_register"

        return {
            "doc_id": f"SRB-{doc_id}",
            "title": title,
            "register_number": register_num,
            "date_raw": date_raw,
            "publishing_date": pub_date,
            "decision_date": dec_date,
            "pdf_url": pdf_url,
            "source_url": f"{BASE_URL}{THEMATIC_REGISTER}" if decision_type == "appeal_panel" else f"{BASE_URL}/en/public-register-of-documents",
            "decision_type": decision_type,
        }

    def _scrape_appeal_panel_decisions(self) -> Generator[Dict[str, Any], None, None]:
        """Scrape Appeal Panel decisions from the thematic register."""
        yield from self._scrape_paginated(THEMATIC_REGISTER, "Appeal Panel")

    def _scrape_public_register(self) -> Generator[Dict[str, Any], None, None]:
        """Scrape public register documents (filtered to PDFs only)."""
        yield from self._scrape_paginated("/en/public-register-of-documents", "Public Register")

    def _scrape_case_pages(self) -> Generator[Dict[str, Any], None, None]:
        """Scrape resolution case pages for PDF documents."""
        for slug in CASE_SLUGS:
            url = f"{BASE_URL}/en/content/{slug}"
            logger.info(f"Fetching case page: {slug}")
            resp = self._get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            page_title = soup.find("h1")
            page_title_text = page_title.get_text(strip=True) if page_title else slug

            # Find all PDF links on the page
            pdf_links = soup.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE))
            for link in pdf_links:
                href = link["href"]
                pdf_url = urljoin(BASE_URL, href)
                link_text = link.get_text(strip=True)

                if not link_text:
                    link_text = Path(href).stem.replace("_", " ").replace("-", " ")

                doc_id = Path(href).stem
                doc_id = re.sub(r"[^\w\-]", "_", doc_id)

                yield {
                    "doc_id": f"SRB-Case-{doc_id}",
                    "title": f"{page_title_text}: {link_text}",
                    "case_slug": slug,
                    "pdf_url": pdf_url,
                    "source_url": url,
                    "decision_type": "resolution_case",
                }

    def _parse_date(self, date_raw: str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_raw:
            return None
        for fmt in ["%d/%m/%Y", "%d.%m.%Y", "%Y-%m-%d", "%d %B %Y"]:
            try:
                dt = datetime.strptime(date_raw, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        # Try extracting year from string
        year_match = re.search(r"(20\d{2})", date_raw)
        if year_match:
            return f"{year_match.group(1)}-01-01"
        return None

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize raw record to standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        title = raw.get("title", "SRB Decision")
        date = self._parse_date(raw.get("date_raw", ""))

        # Try to extract date from filename if not in metadata
        if not date:
            filename = Path(raw.get("pdf_url", "")).stem
            date_match = re.search(r"(\d{4}[-_]\d{2}[-_]\d{2})", filename)
            if date_match:
                date = date_match.group(1).replace("_", "-")

        return {
            "_id": raw.get("doc_id", ""),
            "_source": "EU/SRB-Decisions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("source_url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "decision_type": raw.get("decision_type", ""),
            "case_number": raw.get("case_number", ""),
            "topic": raw.get("topic", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all SRB decisions with full PDF text."""
        existing = preload_existing_ids("EU/SRB-Decisions", table="doctrine")
        logger.info(f"Preloaded {len(existing)} existing IDs")

        # Combine all sources
        sources = [
            ("Appeal Panel", self._scrape_appeal_panel_decisions),
            ("Public Register", self._scrape_public_register),
            ("Case Pages", self._scrape_case_pages),
        ]

        for source_name, scraper_fn in sources:
            logger.info(f"--- Scraping {source_name} ---")
            for raw in scraper_fn():
                doc_id = raw.get("doc_id", "")
                if doc_id in existing:
                    logger.debug(f"Skip existing: {doc_id}")
                    continue

                pdf_url = raw.get("pdf_url", "")
                if not pdf_url:
                    continue

                logger.info(f"Downloading PDF: {doc_id}")
                time.sleep(3)  # Rate limit PDF downloads to avoid 429
                text = extract_pdf_markdown(
                    source="EU/SRB-Decisions",
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="doctrine",
                )

                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for {doc_id}: {len(text) if text else 0} chars")
                    continue

                raw["text"] = text
                yield raw

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()


if __name__ == "__main__":
    scraper = SRBDecisionsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing SRB website connectivity...")
        try:
            resp = scraper._get(f"{BASE_URL}{THEMATIC_REGISTER}")
            if resp:
                soup = BeautifulSoup(resp.text, "html.parser")
                rows = soup.select(".views-row")
                pdf_links = soup.find_all("a", href=re.compile(r"\.pdf", re.IGNORECASE))
                print(f"  Thematic register: {len(rows)} views-rows, {len(pdf_links)} PDF links")
                if pdf_links:
                    print(f"  First PDF: {pdf_links[0].get('href', '')[:80]}")
                print("Test PASSED")
            else:
                print("FAIL: Could not reach SRB website")
                sys.exit(1)
        except Exception as e:
            print(f"  FAIL: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
