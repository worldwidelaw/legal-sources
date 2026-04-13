#!/usr/bin/env python3
"""
FJ/SupremeCourt -- Fiji Courts Online Decisions Fetcher

Fetches Fiji judiciary decisions from judiciary.gov.fj using the
WordPress REST API (custom "judgments" post type). Full text is
extracted from linked PDF documents.

Strategy:
  - List judgments via /wp-json/wp/v2/judgments (paginated, 100/page)
  - Each judgment has an ACF field with PDF URL
  - Download PDF and extract text using PyPDF2 (pdfplumber fallback)
  - Court taxonomy provides classification

API:
  - Judgments: /wp-json/wp/v2/judgments?per_page=100&page=N
  - Courts:   /wp-json/wp/v2/courts_tribunals
  - Total:    ~7,400 judgments (2016-present)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.FJ.SupremeCourt")

API_BASE = "https://judiciary.gov.fj/wp-json/wp/v2"
SITE_URL = "https://judiciary.gov.fj"

# Court taxonomy ID -> name mapping
COURT_NAMES = {
    111: "Supreme Court",
    112: "Court of Appeal",
    113: "High Court Criminal",
    114: "High Court Employment",
    115: "High Court Family",
    116: "Magistrates Court Family",
    117: "Magistrates Court",
    118: "Small Claims Tribunal",
    119: "Tax Tribunal",
    120: "Employment Tribunal",
    121: "Agriculture Tribunal",
    122: "Environment Tribunal",
    124: "Public Service Disciplinary Tribunal",
    126: "High Court Civil",
    128: "Statutory Tribunal",
    129: "Anti-Corruption Tribunal",
    130: "Co-operative Tribunal",
    131: "LTA Tribunal",
    143: "ILSC",
}


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="FJ/SupremeCourt",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""

def clean_html_title(raw_title: str) -> str:
    """Clean HTML entities from title."""
    return unescape(raw_title).strip()


def parse_case_info(title: str) -> Dict[str, Optional[str]]:
    """Extract case number and parties from the title.

    Titles follow patterns like:
      "02.04.26 – HC Crim – HAC 322.2025 – State v Serukalou – Ruling [...]"
      "29.10.2024 – SC – CAV-006/2023 – Lino v State – Judgment"
      "24-03-2026 – Civil Action No. HBC 1 of 2024 – Natapoa v Foo – Judgment"
    """
    info = {"case_number": None, "parties": None, "decision_type": None}

    # Try to extract case number patterns directly
    case_match = re.search(
        r'(?:HAC|HBC|HBE|HBJ|HBG|HBM|CAV|ABU|CBV|HAA|HAM|HAR|ERCA|'
        r'Civil Action No\.|Criminal Case No\.|Judicial Review No\.)'
        r'\s*[\w./\-]+(?:\s+(?:of|&)\s+\d{4})?',
        title, re.IGNORECASE
    )
    if case_match:
        info["case_number"] = case_match.group(0).strip()

    # Try to extract parties (X v Y pattern)
    parties_match = re.search(r'([A-Z][\w\s]+?)\s+v\s+([A-Z][\w\s&]+?)(?:\s*[–—\-]|\s*$)', title)
    if parties_match:
        info["parties"] = f"{parties_match.group(1).strip()} v {parties_match.group(2).strip()}"

    # Try to extract decision type
    decision_match = re.search(r'[–—-]\s*(Judgment|Ruling|Sentence|Decision|Order|Decree)\b', title, re.IGNORECASE)
    if decision_match:
        info["decision_type"] = decision_match.group(1).strip()

    return info


class FJSupremeCourtScraper(BaseScraper):
    """
    Scraper for FJ/SupremeCourt -- Fiji Courts Online Decisions.
    Country: FJ
    URL: https://judiciary.gov.fj

    Data types: case_law
    Auth: none (public WordPress REST API)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=120,
        )

        self.pdf_client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            },
            timeout=120,
        )

    def _fetch_judgments_page(self, page: int, per_page: int = 100) -> list:
        """Fetch a page of judgments from the API."""
        resp = self.client.get(
            "/judgments",
            params={"per_page": per_page, "page": page},
        )
        if resp.status_code == 400:
            # Past the last page
            return []
        resp.raise_for_status()
        return resp.json()

    def _download_pdf_text(self, pdf_url: str) -> str:
        """Download a PDF and extract text."""
        try:
            resp = self.pdf_client.get(pdf_url)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.debug(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return ""
            return extract_text_from_pdf(resp.content)
        except Exception as e:
            logger.warning(f"Failed to download/parse PDF {pdf_url}: {e}")
            return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments from the WordPress REST API."""
        page = 1
        total_yielded = 0

        while True:
            logger.info(f"Fetching judgments page {page}...")
            try:
                items = self._fetch_judgments_page(page)
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break

            if not items:
                logger.info(f"No more items at page {page}. Done.")
                break

            for item in items:
                yield item
                total_yielded += 1

            logger.info(f"Page {page}: got {len(items)} items (total: {total_yielded})")

            if len(items) < 100:
                break

            page += 1
            time.sleep(1)  # Be polite between pages

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch judgments modified since a given date."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
        page = 1

        while True:
            try:
                resp = self.client.get(
                    "/judgments",
                    params={
                        "per_page": 100,
                        "page": page,
                        "modified_after": since_str,
                        "orderby": "modified",
                        "order": "desc",
                    },
                )
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
                items = resp.json()
            except Exception as e:
                logger.error(f"Error fetching updates page {page}: {e}")
                break

            if not items:
                break

            for item in items:
                yield item

            if len(items) < 100:
                break
            page += 1
            time.sleep(1)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a WordPress judgment post into a normalized record."""
        post_id = raw.get("id")
        if not post_id:
            return None

        # Extract title
        raw_title = raw.get("title", {}).get("rendered", "")
        title = clean_html_title(raw_title)
        if not title:
            return None

        # Parse case info from title
        case_info = parse_case_info(title)

        # Get court name from taxonomy
        court_ids = raw.get("courts_tribunals", [])
        court_name = "Unknown Court"
        if court_ids:
            court_name = COURT_NAMES.get(court_ids[0], f"Court ID {court_ids[0]}")
        # Also check ACF court field
        acf = raw.get("acf", {})
        court_acf = acf.get("court_or_tribunal", {})
        if court_acf and isinstance(court_acf, dict):
            court_name = court_acf.get("name", court_name)

        # Get PDF URL
        pdf_url = ""
        judgement_doc = acf.get("judgement_document", {})
        if judgement_doc and isinstance(judgement_doc, dict):
            pdf_url = judgement_doc.get("url", "")

        # Download and extract full text from PDF
        text = ""
        if pdf_url:
            text = self._download_pdf_text(pdf_url)

        if not text:
            logger.debug(f"No text extracted for judgment {post_id}: {title[:60]}")
            return None

        # Date
        date_str = raw.get("date", "")
        date_iso = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str)
                date_iso = dt.strftime("%Y-%m-%d")
            except ValueError:
                date_iso = date_str[:10]

        # URL
        link = raw.get("link", f"{SITE_URL}/?post_type=judgments&p={post_id}")

        return {
            "_id": f"FJ_judiciary_{post_id}",
            "_source": "FJ/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": link,
            "court": court_name,
            "case_number": case_info["case_number"],
            "parties": case_info["parties"],
            "decision_type": case_info["decision_type"],
            "pdf_url": pdf_url,
        }


# ── CLI entry point ──────────────────────────────────────────────

if __name__ == "__main__":
    scraper = FJSupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        print("Testing FJ/SupremeCourt connectivity...")
        try:
            items = scraper._fetch_judgments_page(1, per_page=1)
            if items:
                print(f"OK: Got {len(items)} judgment(s)")
                title = clean_html_title(items[0].get("title", {}).get("rendered", ""))
                print(f"  Latest: {title[:80]}")
                acf = items[0].get("acf", {})
                doc = acf.get("judgement_document", {})
                if doc:
                    print(f"  PDF: {doc.get('url', 'N/A')[:80]}")
            else:
                print("FAIL: No judgments returned")
        except Exception as e:
            print(f"FAIL: {e}")

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")

    elif command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=30)
        result = scraper.bootstrap(sample_mode=False)
        print(f"Update complete: {result}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
