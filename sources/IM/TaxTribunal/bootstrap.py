#!/usr/bin/env python3
"""
IM/TaxTribunal -- Isle of Man Income Tax Division Determinations

Fetches Practice Notes and Guidance Notes from the IoM Income Tax Division:
  - Practice Notes (~236 PDFs, PN series, 1994-present)
  - Guidance Notes (~52 PDFs, GN series)

Full text extracted from PDFs via common/pdf_extract.

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

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IM.TaxTribunal")

BASE_URL = "https://www.gov.im"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

PRACTICE_NOTES_PATH = (
    "/categories/tax-vat-and-your-money/income-tax-and-national-insurance/"
    "tax-practitioners-and-technical-information/practice-notes/"
)
GUIDANCE_NOTES_PATH = (
    "/categories/tax-vat-and-your-money/income-tax-and-national-insurance/"
    "tax-practitioners-and-technical-information/guidance-notes/"
)


def extract_doc_id(filename: str) -> str:
    """Extract a clean doc_id from a PDF filename."""
    name = filename.rsplit("/", 1)[-1]
    name = re.sub(r"\.pdf$", "", name, flags=re.IGNORECASE)
    return name


def extract_note_number(filename: str, text: str) -> Optional[str]:
    """Extract the PN or GN number from filename or text."""
    # Try from text first (more reliable)
    pn_match = re.search(r"(?:PRACTICE NOTE|PN)\s*(\d{1,3}\s*/\s*\d{2,4})", text, re.IGNORECASE)
    if pn_match:
        return f"PN {pn_match.group(1).replace(' ', '')}"

    gn_match = re.search(r"(?:GUIDANCE NOTE|GN)\s*(\d{1,3})", text, re.IGNORECASE)
    if gn_match:
        return f"GN {gn_match.group(1)}"

    # Try from filename
    fn = filename.rsplit("/", 1)[-1].lower()
    pn_fn = re.search(r"pn[_-]?(\d{1,3})[_-]?(\d{2,4})", fn)
    if pn_fn:
        return f"PN {pn_fn.group(1)}/{pn_fn.group(2)}"

    gn_fn = re.search(r"gn(\d{1,3})", fn)
    if gn_fn:
        return f"GN {gn_fn.group(1)}"

    return None


def extract_date_from_text(text: str) -> Optional[str]:
    """Extract date from the PDF text header."""
    date_match = re.search(r"Date:\s*(\d{1,2}\s+\w+\s+\d{4})", text)
    if date_match:
        for fmt in ["%d %B %Y", "%d %b %Y"]:
            try:
                return datetime.strptime(date_match.group(1).strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

    # Try other common patterns
    date_match2 = re.search(r"(?:Published|Issued|Updated)[:\s]*(\d{1,2}\s+\w+\s+\d{4})", text, re.IGNORECASE)
    if date_match2:
        for fmt in ["%d %B %Y", "%d %b %Y"]:
            try:
                return datetime.strptime(date_match2.group(1).strip(), fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue

    return None


def extract_title_from_text(text: str) -> str:
    """Extract title from the first meaningful lines of PDF text."""
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    # Skip header lines (address, phone, etc.) and look for a title-like line
    title_lines = []
    started = False
    for line in lines[:30]:
        # Skip standard header lines
        if any(skip in line.lower() for skip in [
            "income tax division", "the treasury", "government office",
            "isle of man", "telephone", "assessor", "e mail", "website",
            "yn tashtey", "im1 3tx", "incometax@", "www.gov.im",
            "fax:", "british isles", "douglas", "nicola guffogg",
            "paul c cooil", "john connell", "michael crowe",
        ]):
            continue
        if re.match(r"^(?:PRACTICE NOTE|GUIDANCE NOTE)", line, re.IGNORECASE):
            continue
        if re.match(r"^(?:PN|GN)\s*\d", line, re.IGNORECASE):
            continue
        if re.match(r"^Date:", line, re.IGNORECASE):
            continue
        if line and len(line) > 5:
            started = True
            title_lines.append(line)
            if len(title_lines) >= 2 or len(line) > 60:
                break
        elif started:
            break

    title = " ".join(title_lines).strip()
    # Cap at reasonable length
    if len(title) > 200:
        title = title[:197] + "..."
    return title or "Untitled"


class TaxTribunalScraper(BaseScraper):
    """Scraper for Isle of Man Income Tax Division Practice & Guidance Notes."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers=HEADERS,
            max_retries=3,
            backoff_factor=1.0,
            timeout=30,
        )

    def _get_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page (retries handled by HttpClient)."""
        try:
            resp = self.client.get(url, timeout=30)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _get_pdf_links(self, listing_path: str) -> list[str]:
        """Extract PDF links from a listing page."""
        html = self._get_page(BASE_URL + listing_path)
        if not html:
            logger.error(f"Failed to fetch listing page: {listing_path}")
            return []
        links = re.findall(r'href="(/media/[^"]+\.pdf)"', html, re.IGNORECASE)
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for link in links:
            if link not in seen:
                seen.add(link)
                unique.append(link)
        return unique

    def _download_pdf(self, pdf_path: str) -> Optional[bytes]:
        """Download a PDF and return its bytes (retries handled by HttpClient)."""
        url = BASE_URL + pdf_path
        try:
            resp = self.client.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"PDF download failed for {url}: {e}")
            return None

    def _process_pdf(self, pdf_path: str, doc_type: str) -> Optional[dict]:
        """Download and extract text from a single PDF."""
        pdf_bytes = self._download_pdf(pdf_path)
        if not pdf_bytes:
            return None

        doc_id = extract_doc_id(pdf_path)
        text = extract_pdf_markdown("IM/TaxTribunal", doc_id, pdf_bytes=pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"No text extracted from {pdf_path}")
            return None

        return {
            "pdf_path": pdf_path,
            "doc_id": doc_id,
            "doc_type": doc_type,
            "text": text,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all practice notes and guidance notes with full text."""
        # Practice Notes
        logger.info("Fetching practice notes listing...")
        pn_links = self._get_pdf_links(PRACTICE_NOTES_PATH)
        logger.info(f"Found {len(pn_links)} practice note PDFs")

        for i, link in enumerate(pn_links):
            time.sleep(1)
            logger.info(f"Processing practice note {i + 1}/{len(pn_links)}: {link}")
            result = self._process_pdf(link, "practice_note")
            if result:
                yield result

        # Guidance Notes
        logger.info("Fetching guidance notes listing...")
        gn_links = self._get_pdf_links(GUIDANCE_NOTES_PATH)
        logger.info(f"Found {len(gn_links)} guidance note PDFs")

        for i, link in enumerate(gn_links):
            time.sleep(1)
            logger.info(f"Processing guidance note {i + 1}/{len(gn_links)}: {link}")
            result = self._process_pdf(link, "guidance_note")
            if result:
                yield result

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all (reasonably small dataset)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        doc_id = raw["doc_id"]
        pdf_path = raw["pdf_path"]
        doc_type = raw["doc_type"]

        note_number = extract_note_number(pdf_path, text)
        date = extract_date_from_text(text)
        title = extract_title_from_text(text)

        # If we got a note number, prepend it to the title
        if note_number and not title.startswith(note_number):
            title = f"{note_number}: {title}"

        url = BASE_URL + pdf_path

        return {
            "_id": doc_id,
            "_source": "IM/TaxTribunal",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_type": doc_type,
            "note_number": note_number,
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to gov.im."""
        try:
            resp = self.client.get(PRACTICE_NOTES_PATH, timeout=15)
            if resp.status_code == 200 and "practice" in resp.text.lower():
                logger.info("Connection test passed")
                return True
            logger.error(f"Unexpected response: status={resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = TaxTribunalScraper()

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
