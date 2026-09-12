#!/usr/bin/env python3
"""
INTL/GATT-Disputes -- GATT 1947 Dispute Settlement Database Fetcher

Fetches all 317 GATT dispute cases from the WTO archive database.
Each dispute page provides structured metadata (parties, claims, outcome),
and links to panel report PDFs with full text.

Strategy:
  - Iterate through dispute pages /index.php/dispute/gd-{1..317}
  - Parse HTML for metadata, summary, and document links
  - Download panel report PDFs and extract full text with pdfplumber
  - Combine metadata + page summary + PDF text

Usage:
  python bootstrap.py bootstrap          # Fetch all disputes
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import tempfile
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

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
logger = logging.getLogger("legal-data-hunter.INTL.GATT-Disputes")

BASE_URL = "https://gatt-disputes.wto.org"
MAX_DISPUTE_ID = 320  # Slightly above 317 to catch any gaps


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class GATTDisputesScraper(BaseScraper):
    """Scraper for INTL/GATT-Disputes -- WTO GATT dispute settlement archive."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 60, stream: bool = False) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout, stream=stream)
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_text_between(self, soup: BeautifulSoup, start_text: str, end_texts: List[str]) -> str:
        """Extract text between two heading elements."""
        start_el = None
        for el in soup.find_all(["h2", "h3", "h4", "strong", "b"]):
            if start_text.lower() in el.get_text(strip=True).lower():
                start_el = el
                break

        if not start_el:
            return ""

        texts = []
        for sibling in start_el.find_next_siblings():
            tag_text = sibling.get_text(strip=True)
            if any(end.lower() in tag_text.lower() for end in end_texts):
                break
            if tag_text:
                texts.append(tag_text)

        return "\n".join(texts)

    def _parse_dispute_page(self, dispute_id: int) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single dispute page."""
        url = f"{BASE_URL}/index.php/dispute/gd-{dispute_id}"
        resp = self._request(url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find the dispute-specific content div
        content_div = soup.find("div", class_="region-content") or soup.find("div", class_="content")
        if not content_div:
            content_div = soup.find("body")
            if content_div:
                for tag in content_div.find_all(["nav", "footer", "header", "script", "style"]):
                    tag.decompose()

        if not content_div:
            return None

        # Get dispute title from second h1 (first is site name)
        h1_tags = soup.find_all("h1")
        page_title = ""
        for h1 in h1_tags:
            h1_text = h1.get_text(strip=True)
            if h1_text.startswith("GD/"):
                page_title = h1_text
                break
        if not page_title and h1_tags:
            page_title = h1_tags[-1].get_text(strip=True)

        if not page_title or "not found" in page_title.lower():
            return None

        # Get all text from content area
        page_text = content_div.get_text(separator="\n", strip=True)
        page_text = re.sub(r"\n{3,}", "\n\n", page_text)
        page_text = re.sub(r" {2,}", " ", page_text)

        # Title: strip "GD/N" prefix
        title = re.sub(r"^GD/\d+\s*", "", page_title).strip()
        if not title:
            title = f"GATT Dispute GD/{dispute_id}"

        # Extract parties
        complainant = self._extract_field(page_text, "Complainant")
        respondent = self._extract_field(page_text, "Respondent")

        # Extract dates
        date = None
        # Look for adoption date first, then report date
        for pattern_label in ["Adoption", "Report circulated", "Report issued"]:
            date_match = re.search(
                rf"{pattern_label}[:\s]+(\d{{2}}/\d{{2}}/\d{{4}})",
                page_text, re.IGNORECASE,
            )
            if date_match:
                parts = date_match.group(1).split("/")
                if len(parts) == 3:
                    date = f"{parts[2]}-{parts[1]}-{parts[0]}"
                    break

        # If no specific date, look for any date in timeline section
        if not date:
            date_match = re.search(r"(\d{2})/(\d{2})/(\d{4})", page_text)
            if date_match:
                date = f"{date_match.group(3)}-{date_match.group(2)}-{date_match.group(1)}"

        # Find document links (for panel reports)
        doc_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            link_text = a.get_text(strip=True)
            if "/index.php/document/" in href:
                doc_links.append({
                    "url": urljoin(BASE_URL, href),
                    "text": link_text,
                })

        # Find the main report document (usually labeled as "Report" in timeline)
        report_pdf_url = None
        for doc in doc_links:
            doc_text_lower = doc["text"].lower()
            # The report document codes often start with L/ or BISD
            if any(k in doc_text_lower for k in ["report", "l/", "bisd"]):
                # Fetch the document page to get PDF URL
                report_pdf_url = self._find_pdf_url(doc["url"])
                if report_pdf_url:
                    break

        # Download panel report PDF text
        pdf_text = ""
        if report_pdf_url:
            pdf_text = self._extract_pdf_text(report_pdf_url) or ""

        # Build the full text: page summary + PDF content
        full_text = f"GATT Dispute GD/{dispute_id}\n"
        full_text += f"Title: {title}\n"
        if complainant:
            full_text += f"Complainant: {complainant}\n"
        if respondent:
            full_text += f"Respondent: {respondent}\n"
        full_text += f"\n--- Dispute Summary ---\n{page_text}\n"
        if pdf_text:
            full_text += f"\n--- Panel Report Full Text ---\n{pdf_text}\n"

        full_text = full_text.strip()

        if len(full_text) < 100:
            return None

        return {
            "dispute_id": f"GD-{dispute_id}",
            "dispute_number": dispute_id,
            "title": title,
            "text": full_text,
            "date": date,
            "url": url,
            "complainant": complainant,
            "respondent": respondent,
            "has_panel_report": bool(pdf_text),
            "document_count": len(doc_links),
        }

    def _extract_field(self, text: str, field_name: str) -> str:
        """Extract a field value from page text."""
        pattern = rf"{field_name}\s*[:\n]\s*(.+?)(?:\n|$)"
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            value = m.group(1).strip()
            # Clean up common suffixes
            value = re.sub(r"\s*(Respondent|Third|Products|Legal|Claims).*", "", value)
            return value
        return ""

    def _find_pdf_url(self, doc_page_url: str) -> Optional[str]:
        """Visit a document page and find the PDF download URL."""
        resp = self._request(doc_page_url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Look for PDF links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.lower().endswith(".pdf"):
                return urljoin(BASE_URL, href)

        # Check for file field or download link
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/sites/default/files/" in href:
                return urljoin(BASE_URL, href)

        return None

    def _extract_pdf_text(self, url: str) -> Optional[str]:
        """Download a PDF and extract full text with pdfplumber."""
        resp = self._request(url, timeout=120, stream=True)
        if resp is None:
            return None

        # Check file size — skip if > 50MB
        content_length = resp.headers.get("Content-Length")
        if content_length and int(content_length) > 50 * 1024 * 1024:
            logger.warning(f"Skipping oversized PDF: {url[:80]}")
            return None

        try:
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=True) as tmp:
                for chunk in resp.iter_content(chunk_size=8192):
                    tmp.write(chunk)
                tmp.flush()

                with pdfplumber.open(tmp.name) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            pages_text.append(page_text)
                        try:
                            page.flush_cache(); page.get_textmap.cache_clear()
                        except Exception:
                            pass

                    text = "\n\n".join(pages_text)

            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r" {2,}", " ", text)
            text = text.strip()

            return text if len(text) >= 50 else None

        except Exception as e:
            logger.warning(f"PDF extraction failed for {url[:80]}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("dispute_id", ""),
            "_source": "INTL/GATT-Disputes",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "complainant": raw.get("complainant", ""),
            "respondent": raw.get("respondent", ""),
            "has_panel_report": raw.get("has_panel_report", False),
            "document_count": raw.get("document_count", 0),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all GATT disputes."""
        count = 0
        skipped = 0

        for gd_id in range(1, MAX_DISPUTE_ID + 1):
            if gd_id % 50 == 0:
                logger.info(f"Progress: processing GD/{gd_id}")

            dispute = self._parse_dispute_page(gd_id)
            if dispute is None:
                skipped += 1
                continue

            if dispute.get("text"):
                count += 1
                yield dispute

        logger.info(f"Completed: {count} disputes fetched, {skipped} not found")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Historical archive — re-fetch all."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{BASE_URL}/index.php/dispute/gd-100")
        if resp is None:
            logger.error("Cannot reach gatt-disputes.wto.org")
            return False

        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("h1")
        if title:
            logger.info(f"Dispute page OK: {title.get_text(strip=True)[:60]}")
            return True

        logger.error("Could not parse dispute page")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/GATT-Disputes data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = GATTDisputesScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
