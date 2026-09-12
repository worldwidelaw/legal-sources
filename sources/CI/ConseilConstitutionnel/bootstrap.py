#!/usr/bin/env python3
"""
CI/ConseilConstitutionnel -- Côte d'Ivoire Constitutional Council Decisions

Fetches decisions with full text from conseil-constitutionnel.ci.
Each decision is published as a PDF — text is extracted via pdfplumber.

Strategy:
  - Paginate /decisions?page=N (5 decisions/page, ~114 pages, ~570 decisions)
  - Each listing card has a title link and a PDF download link
  - Download PDF, extract text with pdfplumber
  - 2-second delay between requests (conservative)

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CI.ConseilConstitutionnel")

BASE_URL = "https://www.conseil-constitutionnel.ci"
DECISIONS_URL = f"{BASE_URL}/decisions"
MAX_PAGES = 120  # ~114 pages, with margin

FR_MONTHS = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
    "fevrier": "02", "aout": "08", "decembre": "12",
}


class ConseilConstitutionnelScraper(BaseScraper):
    """Scraper for CI/ConseilConstitutionnel — Ivorian Constitutional Council."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.5,en;q=0.3",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF file."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=120)
                if resp.status_code == 200 and len(resp.content) > 100:
                    return resp.content
                logger.warning(f"PDF download returned status {resp.status_code}, size {len(resp.content)}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF download attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages_text = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                return "\n\n".join(pages_text).strip()
        except Exception as e:
            logger.warning(f"PDF text extraction failed: {e}")
            return ""

    def _parse_date_from_title(self, title: str) -> Optional[str]:
        """Extract ISO date from French decision title.

        Examples:
          'du 04 novembre 2025' -> '2025-11-04'
          'du 1er juillet 2025' -> '2025-07-01'
          'du 14 de cembre 2023' -> '2023-12-14' (broken ligatures from PDF)
        """
        # Normalize broken ligatures from PDF extraction (e.g., "de cembre" -> "decembre")
        cleaned = re.sub(r'\s+', ' ', title)
        # Fix common broken month names
        for broken, fixed in [("de cembre", "decembre"), ("fe vrier", "fevrier"),
                              ("se ptembre", "septembre"), ("no vembre", "novembre"),
                              ("oc tobre", "octobre"), ("ja nvier", "janvier"),
                              ("ju illet", "juillet"), ("ju in", "juin")]:
            cleaned = cleaned.replace(broken, fixed)

        # Pattern: du DD month YYYY
        m = re.search(
            r"du\s+(\d{1,2})(?:er)?\s+(\w+)\s+(\d{4})",
            cleaned, re.IGNORECASE
        )
        if m:
            day = int(m.group(1))
            month_str = m.group(2).lower()
            year = m.group(3)
            month = FR_MONTHS.get(month_str)
            if month:
                return f"{year}-{month}-{day:02d}"
        return None

    def _parse_decision_number(self, title: str) -> str:
        """Extract decision reference number from title.

        Example: 'DÉCISION N° CI-2025-EP-007/04-11/CC/SG ...' -> 'CI-2025-EP-007/04-11/CC/SG'
        """
        m = re.search(r"N°\s*(CI-[\w\-/]+)", title, re.IGNORECASE)
        if m:
            return m.group(1).strip()
        # Fallback: use cleaned title
        return re.sub(r"[^\w\-/]", "_", title[:60]).strip("_")

    def _parse_listing_page(self, page_num: int) -> list:
        """Parse a single page of the decisions listing."""
        url = f"{DECISIONS_URL}?page={page_num}"
        resp = self._request(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        rows = soup.select(".views-row")
        results = []

        for row in rows:
            # Title link
            title_link = row.select_one("a[href*='archives-et-decisions']")
            if not title_link:
                continue

            title = title_link.get_text(strip=True)
            detail_href = title_link.get("href", "")
            detail_url = f"{BASE_URL}{detail_href}" if detail_href.startswith("/") else detail_href

            # PDF link
            pdf_link = row.select_one("a[href$='.pdf']")
            pdf_url = ""
            if pdf_link:
                pdf_href = pdf_link.get("href", "")
                pdf_url = pdf_href if pdf_href.startswith("http") else f"{BASE_URL}{pdf_href}"

            results.append({
                "title": title,
                "detail_url": detail_url,
                "pdf_url": pdf_url,
            })

        return results

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from paginated listing."""
        empty_pages = 0
        total = 0

        for page in range(MAX_PAGES):
            logger.info(f"Fetching decisions page {page+1}...")
            items = self._parse_listing_page(page)

            if not items:
                empty_pages += 1
                if empty_pages >= 3:
                    logger.info(f"3 consecutive empty pages at page {page+1}, stopping")
                    break
                continue

            empty_pages = 0

            for item in items:
                if not item.get("pdf_url"):
                    logger.warning(f"No PDF URL for: {item.get('title', '?')[:60]}")
                    continue

                # Download and extract PDF text
                logger.info(f"Downloading PDF: {item['title'][:60]}...")
                pdf_bytes = self._download_pdf(item["pdf_url"])
                if not pdf_bytes:
                    logger.warning(f"Failed to download PDF: {item['pdf_url']}")
                    continue

                text = self._extract_pdf_text(pdf_bytes)
                if not text:
                    logger.warning(f"No text extracted from PDF: {item['pdf_url']}")
                    continue

                item["text"] = text
                total += 1
                yield item

            logger.info(f"Page {page+1} done, {total} decisions fetched so far")

        logger.info(f"Finished: {total} total decisions fetched")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions (first few pages only)."""
        for page in range(5):
            items = self._parse_listing_page(page)
            if not items:
                break
            for item in items:
                date_str = self._parse_date_from_title(item.get("title", ""))
                if date_str:
                    try:
                        d = datetime.fromisoformat(date_str)
                        if d.replace(tzinfo=timezone.utc) < since:
                            return
                    except ValueError:
                        pass

                if not item.get("pdf_url"):
                    continue

                pdf_bytes = self._download_pdf(item["pdf_url"])
                if not pdf_bytes:
                    continue
                text = self._extract_pdf_text(pdf_bytes)
                if not text:
                    continue
                item["text"] = text
                yield item

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw decision into standard schema."""
        title = raw.get("title", "")
        decision_number = self._parse_decision_number(title)
        date = self._parse_date_from_title(title)
        text = raw.get("text", "")

        if not text or len(text) < 50:
            return None

        return {
            "_id": decision_number,
            "_source": "CI/ConseilConstitutionnel",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "decision_number": decision_number,
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("detail_url", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ConseilConstitutionnelScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing connectivity...")
        resp = scraper._request(DECISIONS_URL)
        if resp and resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            rows = soup.select(".views-row")
            print(f"OK: Found {len(rows)} decisions on first page")

            # Test PDF download
            if rows:
                pdf_link = rows[0].select_one("a[href$='.pdf']")
                if pdf_link:
                    pdf_url = pdf_link.get("href", "")
                    if not pdf_url.startswith("http"):
                        pdf_url = f"{BASE_URL}{pdf_url}"
                    pdf_bytes = scraper._download_pdf(pdf_url)
                    if pdf_bytes:
                        text = scraper._extract_pdf_text(pdf_bytes)
                        print(f"PDF test: {len(text)} chars extracted")
                        print(f"First 200 chars: {text[:200]}")
                    else:
                        print("FAIL: Could not download PDF")
        else:
            print(f"FAIL: {resp.status_code if resp else 'no response'}")

    elif command == "bootstrap":
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=15)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
