#!/usr/bin/env python3
"""
JE/JCRA -- Jersey Competition Regulatory Authority Decisions

Fetches regulatory decisions from jcra.je:
  - Case listing via Umbraco AJAX: /umbraco/surface/SearchSurface/CaseSearch
  - Individual case pages for metadata + PDF links
  - Full text extracted from PDF documents

~545 cases covering competition, telecoms, and ports regulation (2003-present).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JE.JCRA")

BASE_URL = "https://www.jcra.je"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PAGE_SIZE = 600


def clean_html(html: str) -> str:
    """Strip HTML tags and decode entities."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|li|h[1-6]|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_date_dmy(date_str: str) -> Optional[str]:
    """Parse DD/MM/YYYY to ISO 8601."""
    date_str = date_str.strip()
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return None


class JCRAScraper(BaseScraper):
    """Scraper for JCRA regulatory decisions."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _list_cases(self, max_pages: int = 20) -> list[dict]:
        """Fetch case listing from AJAX endpoint."""
        cases = []
        for page in range(1, max_pages + 1):
            url = f"{BASE_URL}/umbraco/surface/SearchSurface/CaseSearch"
            params = {"page": page, "pageSize": PAGE_SIZE}
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Failed to fetch case list page {page}: {e}")
                break

            html = resp.text

            # Parse case rows from HTML table
            rows = re.findall(
                r'<a href="(/cases-documents/cases/[^"]+)"[^>]*title="([^"]*)">'
                r'.*?<p class="body-small">(.*?)</p>'
                r'.*?<p class="body-small">(\d{2}/\d{2}/\d{4})</p>'
                r'.*?<p class="body-small">(.*?)</p>',
                html, re.DOTALL
            )

            if not rows:
                break

            for path, title, case_no, date_str, status in rows:
                cases.append({
                    "path": path,
                    "title": unescape(title).strip(),
                    "case_number": case_no.strip(),
                    "date": parse_date_dmy(date_str),
                    "status": status.strip(),
                })

            logger.info(f"Case list page {page}: {len(rows)} cases")
            time.sleep(0.5)

            if len(rows) < PAGE_SIZE:
                break

        return cases

    def _fetch_case_detail(self, case: dict) -> Optional[dict]:
        """Fetch a case detail page and extract PDF links + metadata."""
        url = BASE_URL + case["path"]
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch case {case['case_number']}: {e}")
            return None

        html = resp.text

        # Extract unique PDF links
        pdf_links = list(dict.fromkeys(
            re.findall(r'href="(/media/[^"]+\.pdf)"', html)
        ))

        # Extract metadata fields from content-table divs
        def _extract_field(label: str) -> Optional[str]:
            m = re.search(
                rf'<p class="body-small v--bold">{re.escape(label)}</p>'
                r'\s*<p class="body-small content">\s*(.*?)\s*</p>',
                html, re.DOTALL,
            )
            return unescape(m.group(1)).strip() if m else None

        sector = _extract_field("Sectors")
        case_type = _extract_field("Case Type")
        purchaser = _extract_field("Purchaser")
        seller = _extract_field("Seller")
        industry = _extract_field("Industry")

        # Combine purchaser/seller as parties
        parties_parts = [p for p in [purchaser, seller] if p]
        parties = " / ".join(parties_parts) if parties_parts else None

        # Extract description text
        desc_match = re.search(
            r'<div class="case-description">(.*?)</div>', html, re.DOTALL
        )
        description = clean_html(desc_match.group(1)).strip() if desc_match else None

        return {
            **case,
            "url": url,
            "pdf_links": pdf_links,
            "sector": sector,
            "case_type": case_type,
            "parties": parties,
            "industry": industry,
            "description": description,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all JCRA cases with full text from PDFs."""
        cases = self._list_cases()
        logger.info(f"Found {len(cases)} cases total")

        for i, case in enumerate(cases):
            time.sleep(0.5)
            detail = self._fetch_case_detail(case)
            if not detail:
                continue

            if not detail["pdf_links"]:
                logger.debug(f"No PDFs for {case['case_number']}, skipping")
                continue

            # Extract text from the primary (first) PDF
            pdf_path = detail["pdf_links"][0]
            pdf_url = BASE_URL + pdf_path
            text = extract_pdf_markdown(
                source="JE/JCRA",
                source_id=detail["case_number"],
                pdf_url=pdf_url,
                table="doctrine",
            )

            if not text or len(text) < 50:
                logger.debug(f"No text from PDF for {case['case_number']}")
                continue

            detail["text"] = text
            yield detail

            if (i + 1) % 50 == 0:
                logger.info(f"Processed {i + 1}/{len(cases)} cases")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent cases only."""
        cases = self._list_cases(max_pages=3)
        since_str = since.strftime("%Y-%m-%d")
        recent = [c for c in cases if c.get("date") and c["date"] >= since_str]
        logger.info(f"Found {len(recent)} cases since {since_str}")

        for case in recent:
            time.sleep(0.5)
            detail = self._fetch_case_detail(case)
            if not detail or not detail["pdf_links"]:
                continue

            pdf_url = BASE_URL + detail["pdf_links"][0]
            text = extract_pdf_markdown(
                source="JE/JCRA",
                source_id=detail["case_number"],
                pdf_url=pdf_url,
                table="doctrine",
            )

            if not text or len(text) < 50:
                continue

            detail["text"] = text
            yield detail

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        # Prepend description to PDF text for richer content
        description = raw.get("description", "")
        full_text = f"{description}\n\n{text}" if description else text

        return {
            "_id": raw["case_number"],
            "_source": "JE/JCRA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": raw["case_number"],
            "title": raw.get("title", raw["case_number"]),
            "text": full_text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "sector": raw.get("sector"),
            "case_type": raw.get("case_type"),
            "status": raw.get("status"),
            "parties": raw.get("parties"),
            "industry": raw.get("industry"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to jcra.je."""
        try:
            url = f"{BASE_URL}/umbraco/surface/SearchSurface/CaseSearch"
            resp = self.session.get(url, params={"page": 1, "pageSize": 1}, timeout=30)
            resp.raise_for_status()
            if "cases-documents" in resp.text:
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: unexpected response")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = JCRAScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)
    elif command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode)
        print(f"Bootstrap complete: {result}")
    elif command == "update":
        result = scraper.update()
        print(f"Update complete: {result}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
