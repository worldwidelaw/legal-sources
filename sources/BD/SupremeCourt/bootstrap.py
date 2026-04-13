#!/usr/bin/env python3
"""
BD/SupremeCourt -- Bangladesh Supreme Court Judgments

Fetches court judgments from the Bangladesh Supreme Court portal.

Strategy:
  - Paginate through listing pages (50 per page) for both divisions
  - Parse HTML tables to extract case metadata and PDF URLs
  - Download PDFs with session cookies and referrer header
  - Extract full text via pdfplumber

Data: ~9,300 judgments (Appellate ~400, High Court ~8,900)
License: Open access (government court decisions, no robots.txt)
Rate limit: 0.5 req/sec (respectful, 2 second delay).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Tuple
from urllib.parse import urljoin, quote

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BD.SupremeCourt")

BASE_URL = "https://www.supremecourt.gov.bd"
LISTING_URL = f"{BASE_URL}/web/"
PDF_BASE = f"{BASE_URL}/resources/documents/"

DIVISIONS = {
    1: "Appellate Division",
    2: "High Court Division",
}

# Month abbreviations for date parsing
MONTHS = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}


class BDSupremeCourtScraper(BaseScraper):
    """
    Scraper for BD/SupremeCourt -- Bangladesh Supreme Court.
    Country: BD
    URL: https://www.supremecourt.gov.bd/

    Data types: case_law
    Auth: none (session cookies auto-acquired)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })
        self._session_initialized = False

    def _init_session(self, div_id: int = 1):
        """Initialize session by visiting a listing page to get cookies."""
        if self._session_initialized:
            return
        url = f"{LISTING_URL}?page=judgments.php&menu=00&div_id={div_id}&start=0"
        try:
            self.session.get(url, timeout=20)
            self._session_initialized = True
            logger.info(f"Session initialized with {len(self.session.cookies)} cookies")
        except Exception as e:
            logger.error(f"Failed to initialize session: {e}")

    # -- Listing page parsing ------------------------------------------------

    def _fetch_listing_page(self, div_id: int, start: int) -> List[dict]:
        """Fetch and parse a listing page, returning judgment metadata."""
        url = f"{LISTING_URL}?page=judgments.php&menu=00&div_id={div_id}&start={start}"
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"Listing page HTTP {resp.status_code}: {url}")
                return []
        except Exception as e:
            logger.error(f"Failed to fetch listing: {e}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        judgments = []

        # Find all PDF links (not translation links)
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if ".pdf" not in href.lower() or "translation" in href.lower():
                continue
            if "resources/documents" not in href:
                continue

            case_ref = a_tag.get_text(strip=True)

            # Extract filename from href
            filename = href.split("/")[-1]

            # Build full PDF URL
            pdf_url = f"{PDF_BASE}{quote(filename, safe='()_-.')}"

            # Get parent row for metadata
            tr = a_tag.find_parent("tr")
            parties = ""
            description = ""
            upload_date = None

            if tr:
                tds = tr.find_all("td")
                if len(tds) >= 4:
                    parties = tds[2].get_text(strip=True)[:500]
                    description = tds[3].get_text(strip=True)[:1000]

            # Parse upload date
            parent_td = a_tag.find_parent("td")
            if parent_td:
                date_match = re.search(
                    r"Uploaded on\s*:\s*(\d{1,2})-([A-Z]{3})-(\d{2,4})",
                    parent_td.get_text(),
                )
                if date_match:
                    day = date_match.group(1).zfill(2)
                    month = MONTHS.get(date_match.group(2), "01")
                    year = date_match.group(3)
                    if len(year) == 2:
                        year = f"20{year}"
                    upload_date = f"{year}-{month}-{day}"

            judgments.append({
                "case_ref": case_ref,
                "parties": parties,
                "description": description,
                "upload_date": upload_date,
                "pdf_url": pdf_url,
                "pdf_filename": filename,
                "division": DIVISIONS.get(div_id, f"Division {div_id}"),
                "div_id": div_id,
            })

        return judgments

    # -- PDF text extraction -------------------------------------------------

    def _extract_text_from_pdf(self, pdf_url: str, div_id: int) -> Optional[str]:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="BD/SupremeCourt",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
        ) or ""

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments from both divisions."""
        total_found = 0

        for div_id, div_name in DIVISIONS.items():
            logger.info(f"Scanning {div_name} (div_id={div_id})")
            self._init_session(div_id)
            start = 0
            consecutive_empty = 0

            while True:
                time.sleep(2)  # Respectful delay
                judgments = self._fetch_listing_page(div_id, start)

                if not judgments:
                    consecutive_empty += 1
                    if consecutive_empty >= 2:
                        logger.info(f"No more results for {div_name} at start={start}")
                        break
                    start += 50
                    continue

                consecutive_empty = 0

                for j in judgments:
                    time.sleep(2)  # Delay between PDF downloads
                    text = self._extract_text_from_pdf(j["pdf_url"], div_id)
                    if not text:
                        logger.debug(f"No text for: {j['case_ref'][:60]}")
                        continue

                    j["text"] = text
                    total_found += 1
                    yield j

                    if total_found % 50 == 0:
                        logger.info(f"Progress: {total_found} judgments extracted")

                start += 50

        logger.info(f"Scan complete: {total_found} judgments")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent judgments (first few pages of each division)."""
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_str}")

        for div_id, div_name in DIVISIONS.items():
            self._init_session(div_id)

            for start in range(0, 500, 50):
                time.sleep(2)
                judgments = self._fetch_listing_page(div_id, start)
                if not judgments:
                    break

                for j in judgments:
                    if j.get("upload_date") and j["upload_date"] < since_str:
                        return

                    time.sleep(2)
                    text = self._extract_text_from_pdf(j["pdf_url"], div_id)
                    if text:
                        j["text"] = text
                        yield j

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample judgments from both divisions."""
        found = 0

        for div_id, div_name in DIVISIONS.items():
            if found >= count:
                break

            self._init_session(div_id)
            time.sleep(2)
            judgments = self._fetch_listing_page(div_id, 0)
            logger.info(f"{div_name}: {len(judgments)} judgments on first page")

            for j in judgments:
                if found >= count:
                    break

                time.sleep(2)
                text = self._extract_text_from_pdf(j["pdf_url"], div_id)
                if not text:
                    logger.debug(f"Skipping (no text): {j['case_ref'][:60]}")
                    continue

                j["text"] = text
                found += 1
                logger.info(
                    f"Sample {found}/{count}: {j['case_ref'][:50]} "
                    f"({len(text)} chars, {j['division']})"
                )
                yield j

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw judgment record to standard schema."""
        case_ref = raw.get("case_ref", "Unknown")
        # Clean case reference
        case_ref = re.sub(r"\s+", " ", case_ref).strip()

        parties = raw.get("parties", "")
        # Use parties as title, fall back to case ref
        title = parties if parties else case_ref
        title = re.sub(r"\s+", " ", title).strip()[:500]

        # Create a stable ID from the filename
        filename = raw.get("pdf_filename", "")
        doc_id = re.sub(r"[^a-zA-Z0-9]", "-", filename.replace(".pdf", ""))

        return {
            "_id": f"BD-SC-{doc_id}",
            "_source": "BD/SupremeCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("upload_date"),
            "url": raw.get("pdf_url", ""),
            "case_ref": case_ref,
            "division": raw.get("division", ""),
            "description": raw.get("description", ""),
        }

    def test_api(self) -> bool:
        """Test site connectivity and PDF access."""
        logger.info("Testing Bangladesh Supreme Court portal...")

        # Test listing page
        self._init_session(1)
        time.sleep(1)
        judgments = self._fetch_listing_page(1, 0)
        if not judgments:
            logger.error("Failed to parse listing page")
            return False
        logger.info(f"Listing OK: {len(judgments)} judgments on first page")

        # Test PDF download
        time.sleep(2)
        text = self._extract_text_from_pdf(judgments[0]["pdf_url"], 1)
        if not text:
            logger.error("PDF extraction failed")
            return False
        logger.info(f"PDF extraction OK: {len(text)} chars")

        logger.info("All tests passed")
        return True


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = BDSupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        since_str = None
        for i, arg in enumerate(sys.argv):
            if arg == "--since" and i + 1 < len(sys.argv):
                since_str = sys.argv[i + 1]

        if not since_str:
            print("Usage: python bootstrap.py update --since YYYY-MM-DD")
            sys.exit(1)

        since = datetime.strptime(since_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
