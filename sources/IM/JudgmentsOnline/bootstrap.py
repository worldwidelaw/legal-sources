#!/usr/bin/env python3
"""
IM/JudgmentsOnline -- Isle of Man Court Judgments

Fetches ~3,400+ judgments from the Isle of Man Courts of Justice (2000 onwards).
Enumerates judgments by document reference number (J1-J3500).
Full text extracted from HTML via document reference search endpoint.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Connectivity test
"""

import sys
import re
import html
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IM.JudgmentsOnline")

BASE_URL = "https://www.judgments.im"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Approximate upper bound of judgment numbers
MAX_JUDGMENT_NUMBER = 3500

NO_RESULTS_MARKER = "Your search returned no results"


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities, preserving paragraph breaks."""
    # Replace block-level tags with newlines
    text = re.sub(r'<(?:br|BR)\s*/?>', '\n', text)
    text = re.sub(r'</(?:p|P|div|DIV|tr|TR|li|LI|h[1-6]|H[1-6])>', '\n', text)
    text = re.sub(r'<(?:p|P|div|DIV|tr|TR|li|LI|h[1-6]|H[1-6])[^>]*>', '\n', text)
    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = html.unescape(text)
    # Fix encoding artifacts: non-breaking spaces showing as Â
    text = text.replace('\u00a0', ' ')
    text = text.replace('\u00c2', '')  # Stray Â from double-encoded UTF-8
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse a date string like '24 May 2006' to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Handle ordinal suffixes: "24th May 2006" -> "24 May 2006"
    date_str = re.sub(r'(\d+)(?:st|nd|rd|th)', r'\1', date_str)
    for fmt in ["%d %B %Y", "%d %b %Y", "%B %d %Y", "%d/%m/%Y", "%Y-%m-%d"]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_fields(page_html: str) -> dict:
    """Extract metadata fields from the judgment detail page HTML."""
    fields = {}
    # Pattern: <th class="elementcontainer">Label</th> ... <div class="contentelementvalue">Value</div>
    pattern = re.compile(
        r'<th\s+class="elementcontainer">(.*?)</th>\s*'
        r'<td\s+class="elementcontainer">\s*'
        r'<div\s+class="contentelementvalue">(.*?)</div>',
        re.DOTALL
    )
    for match in pattern.finditer(page_html):
        label = match.group(1).strip()
        value = match.group(2)
        fields[label] = value
    return fields


class JudgmentsOnlineScraper(BaseScraper):
    """Scraper for Isle of Man Judgments Online."""

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

    def _fetch_judgment(self, j_number: int) -> Optional[dict]:
        """Fetch a single judgment by its J-number."""
        url = (
            f"/content/search.mth?searchtype=2&filename=J{j_number}"
            f"&pagesize=50&searchaction=Search&doctype-1=1&doctype-2=2"
        )
        try:
            resp = self.client.get(url, timeout=30)
            resp.raise_for_status()
            page_html = resp.text

            # Check if no results
            if NO_RESULTS_MARKER in page_html:
                return None

            # Check if we actually got judgment content
            if "elementcontainer" not in page_html:
                return None

            fields = extract_fields(page_html)
            if not fields:
                return None

            # Extract judgment text (the largest field, typically "Judgment")
            judgment_html = fields.get("Judgment", "")
            if not judgment_html:
                logger.warning(f"J{j_number}: No Judgment field found")
                return None

            judgment_text = strip_html(judgment_html)
            if len(judgment_text) < 50:
                logger.warning(f"J{j_number}: Judgment text too short ({len(judgment_text)} chars)")
                return None

            # Extract title from Title field
            title_html = fields.get("Title", "")
            title = strip_html(title_html).strip() if title_html else ""

            # Fall back to parties if no title
            if not title or len(title) < 3:
                p1 = strip_html(fields.get("Party One", "")).strip()
                p2 = strip_html(fields.get("Party Two", "")).strip()
                if p1 and p2:
                    title = f"{p1} v {p2}"
                elif p1:
                    title = p1
                else:
                    title = f"Judgment J{j_number}"

            # Extract version ID for reference
            version_match = re.search(r'contentdocumentversionid=(\d+)', page_html)
            version_id = version_match.group(1) if version_match else None

            return {
                "j_number": j_number,
                "title": title,
                "text": judgment_text,
                "court": strip_html(fields.get("Court", "")).strip(),
                "division": strip_html(fields.get("Division", "")).strip(),
                "division_code": strip_html(fields.get("Case Reference - Division Code", "")).strip(),
                "case_year": strip_html(fields.get("Case Reference - Year", "")).strip(),
                "case_number": strip_html(fields.get("Case Reference - Number", "")).strip(),
                "judgment_date": strip_html(fields.get("Judgment Date", "")).strip(),
                "judge": strip_html(fields.get("The Judgment of", "")).strip(),
                "party_one": strip_html(fields.get("Party One", "")).strip(),
                "party_two": strip_html(fields.get("Party Two", "")).strip(),
                "version_id": version_id,
            }

        except Exception as e:
            logger.warning(f"Failed to fetch J{j_number}: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all judgments by enumerating J1 to J3500."""
        consecutive_misses = 0
        found = 0

        for j_num in range(1, MAX_JUDGMENT_NUMBER + 1):
            time.sleep(1)  # Rate limiting

            if j_num % 100 == 0:
                logger.info(f"Progress: J{j_num}/{MAX_JUDGMENT_NUMBER} (found {found})")

            result = self._fetch_judgment(j_num)
            if result:
                found += 1
                consecutive_misses = 0
                yield result
            else:
                consecutive_misses += 1
                # If we get 200 consecutive misses after finding some, we've likely exhausted
                if consecutive_misses > 200 and found > 100:
                    logger.info(f"Stopping after {consecutive_misses} consecutive misses at J{j_num}")
                    break

        logger.info(f"Enumeration complete: found {found} judgments out of J1-J{j_num}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch recent judgments (start from high numbers)."""
        # Work backwards from MAX to find recent additions
        consecutive_misses = 0
        for j_num in range(MAX_JUDGMENT_NUMBER, 0, -1):
            time.sleep(1)
            result = self._fetch_judgment(j_num)
            if result:
                date_str = result.get("judgment_date", "")
                date = parse_date(date_str)
                if date and date < since.strftime("%Y-%m-%d"):
                    break
                consecutive_misses = 0
                yield result
            else:
                consecutive_misses += 1
                if consecutive_misses > 100:
                    break

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw judgment into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        j_number = raw["j_number"]
        judgment_id = f"J{j_number}"

        # Build case reference
        div_code = raw.get("division_code", "")
        case_year = raw.get("case_year", "")
        case_num = raw.get("case_number", "")
        case_ref = ""
        if div_code and case_year:
            case_ref = f"{div_code} {case_year}/{case_num}" if case_num else f"{div_code} {case_year}"

        date = parse_date(raw.get("judgment_date", ""))

        url = (
            f"{BASE_URL}/content/search.mth?searchtype=2&filename={judgment_id}"
            f"&pagesize=50&searchaction=Search&doctype-1=1&doctype-2=2"
        )

        return {
            "_id": judgment_id,
            "_source": "IM/JudgmentsOnline",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "judgment_id": judgment_id,
            "title": raw.get("title", f"Judgment {judgment_id}"),
            "text": text,
            "date": date,
            "url": url,
            "court": raw.get("court", ""),
            "division": raw.get("division", ""),
            "case_reference": case_ref,
            "judge": raw.get("judge", ""),
            "party_one": raw.get("party_one", ""),
            "party_two": raw.get("party_two", ""),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to judgments.im."""
        try:
            resp = self.client.get(
                "/content/search.mth?searchtype=2&filename=J100"
                "&pagesize=50&searchaction=Search&doctype-1=1&doctype-2=2",
                timeout=15,
            )
            if resp.status_code == 200 and "elementcontainer" in resp.text:
                logger.info("Connection test passed")
                return True
            logger.error(f"Unexpected response: status={resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = JudgmentsOnlineScraper()

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
