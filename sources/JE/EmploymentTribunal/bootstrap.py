#!/usr/bin/env python3
"""
JE/EmploymentTribunal -- Jersey Employment & Discrimination Tribunal

Fetches tribunal decisions from jerseylaw.je by enumerating URLs:
  Pattern: /judgments/tribunal/Pages/[YEAR]TRE[NNN].aspx
  Years: 2005-present, numbers 001-300 (sparse, case registry numbers)
  ~30-60 decisions per year, ~500-1000 total.

Full text extracted from server-rendered SharePoint HTML.

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
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JE.EmploymentTribunal")

BASE_URL = "https://www.jerseylaw.je"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

START_YEAR = 2005
MAX_NUMBER = 300


def clean_html(html: str) -> str:
    """Strip HTML tags and clean entities."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|li|h[1-6]|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&#160;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"&#58;", ":", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"\u200b", "", text)  # zero-width space
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def extract_judgment_content(html: str) -> Optional[str]:
    """Extract the judgment text from the SharePoint page HTML."""
    # Primary: look for <div class="law"> (SharePoint rich text field)
    match = re.search(
        r'<div\s+class="law"[^>]*>(.*?)(?:</div>\s*</div>\s*</div>\s*</div>)',
        html, re.DOTALL
    )
    if match:
        return clean_html(match.group(1))

    # Fallback: ExternalClass div (older SharePoint layout)
    match = re.search(
        r'class="ExternalClass[^"]*"[^>]*>(.*?)(?:</div>\s*</div>\s*</div>\s*</div>)',
        html, re.DOTALL
    )
    if match:
        return clean_html(match.group(1))

    # Last resort: look for the main content area with judgment markers
    match = re.search(
        r'(IN THE JERSEY (?:EMPLOYMENT|DISCRIMINATION).*?)(?:<div[^>]*class="(?:page-actions|footer|ms-webpart))',
        html, re.DOTALL | re.IGNORECASE
    )
    if match:
        return clean_html(match.group(1))

    return None


def extract_metadata(text: str, year: int, num: int) -> dict:
    """Extract case metadata from the judgment text."""
    meta = {}

    # Normalize whitespace for reliable regex matching (dates may span lines)
    flat = re.sub(r"\s+", " ", text)

    # Case reference
    ref_match = re.search(r"Reference:\s*\[(\d{4})\]\s*TRE\s*(\d+)", flat)
    if ref_match:
        meta["case_ref"] = f"[{ref_match.group(1)}] TRE {ref_match.group(2)}"
    else:
        meta["case_ref"] = f"[{year}] TRE {num:03d}"

    # Parties - look for CLAIMANT and RESPONDENT
    claimant_match = re.search(
        r"(?:MATTER OF|IN THE MATTER)\s*[:\s]*(.*?)\s*(?:CLAIMANT|Claimant)",
        flat
    )
    respondent_match = re.search(
        r"(?:AND|and)\s+(.*?)\s*(?:RESPONDENT|Respondent)",
        flat
    )

    claimant = ""
    respondent = ""
    if claimant_match:
        claimant = claimant_match.group(1).strip()
    if respondent_match:
        respondent = respondent_match.group(1).strip()

    if claimant and respondent:
        meta["title"] = f"{claimant} v {respondent}"
    elif claimant:
        meta["title"] = claimant
    else:
        meta["title"] = meta["case_ref"]

    # Decision date - look for "Sent to the parties on" or hearing date
    sent_match = re.search(
        r"Sent to the parties on\s+(\d{1,2}\s+\w+\s+\d{4})", flat, re.IGNORECASE
    )
    if sent_match:
        meta["date"] = parse_date(sent_match.group(1))

    if not meta.get("date"):
        # Try "Decision Date:" or "Date of Decision:"
        date_match = re.search(
            r"(?:Decision Date|Date of Decision)[:\s]*(\d{1,2}\s+\w+\s+\d{4})",
            flat, re.IGNORECASE
        )
        if date_match:
            meta["date"] = parse_date(date_match.group(1))

    if not meta.get("date"):
        # Try "Hearing Date" as fallback for decision date
        hearing_date_match = re.search(
            r"Hearing Date[s]?[:\s]*(\d{1,2}\s+\w+\s+\d{4})", flat, re.IGNORECASE
        )
        if hearing_date_match:
            meta["date"] = parse_date(hearing_date_match.group(1))

    # Hearing date
    hearing_match = re.search(
        r"Hearing Date[s]?[:\s]*([\d\w\s,and]+\d{4})", flat, re.IGNORECASE
    )
    if hearing_match:
        meta["hearing_date"] = hearing_match.group(1).strip()

    # Tribunal members
    before_match = re.search(r"Before:\s*(.*?)(?:Hearing)", flat)
    if before_match:
        meta["tribunal_members"] = before_match.group(1).strip()

    return meta


def parse_date(date_str: str) -> Optional[str]:
    """Parse date string to ISO 8601."""
    date_str = date_str.strip()
    for fmt in ["%d %B %Y", "%d %b %Y", "%d/%m/%Y"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


class EmploymentTribunalScraper(BaseScraper):
    """Scraper for Jersey Employment & Discrimination Tribunal decisions."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _build_url(self, year: int, num: int) -> str:
        """Build the URL for a specific decision."""
        return (
            f"{BASE_URL}/judgments/tribunal/Pages/"
            f"%5B{year}%5DTRE{num:03d}.aspx"
        )

    def _fetch_decision(self, year: int, num: int) -> Optional[dict]:
        """Fetch a single decision page, return raw data or None if not found."""
        url = self._build_url(year, num)
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()

            text = extract_judgment_content(resp.text)
            if not text or len(text) < 100:
                return None

            return {
                "year": year,
                "num": num,
                "url": url,
                "text": text,
                "html": resp.text,
            }
        except requests.RequestException:
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions by enumerating URLs year by year."""
        current_year = datetime.now().year

        for year in range(current_year, START_YEAR - 1, -1):
            logger.info(f"Scanning year {year}...")
            consecutive_misses = 0
            found_in_year = 0

            for num in range(1, MAX_NUMBER + 1):
                time.sleep(0.5)
                result = self._fetch_decision(year, num)

                if result:
                    consecutive_misses = 0
                    found_in_year += 1
                    yield result
                else:
                    consecutive_misses += 1
                    # After scanning past the likely range with many misses, skip ahead
                    if consecutive_misses > 30 and num > 50:
                        break

            logger.info(f"Year {year}: found {found_in_year} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions from recent years only."""
        current_year = datetime.now().year
        for year in range(current_year, current_year - 2, -1):
            logger.info(f"Scanning year {year} for updates...")
            for num in range(1, MAX_NUMBER + 1):
                time.sleep(0.5)
                result = self._fetch_decision(year, num)
                if result:
                    yield result

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        year = raw["year"]
        num = raw["num"]
        meta = extract_metadata(text, year, num)

        return {
            "_id": meta["case_ref"],
            "_source": "JE/EmploymentTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_ref": meta["case_ref"],
            "title": meta.get("title", meta["case_ref"]),
            "text": text,
            "date": meta.get("date"),
            "url": raw["url"],
            "hearing_date": meta.get("hearing_date"),
            "tribunal_members": meta.get("tribunal_members"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to jerseylaw.je."""
        try:
            # Test a known recent decision
            result = self._fetch_decision(2025, 81)
            if result and len(result.get("text", "")) > 100:
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: no content returned")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = EmploymentTribunalScraper()

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
