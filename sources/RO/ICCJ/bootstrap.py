#!/usr/bin/env python3
"""
RO/ICCJ -- Înalta Curte de Casație și Justiție Data Fetcher

Fetches Romanian supreme court decisions from scj.ro.
Full text is embedded in HTML detail pages, accessed by sequential ID.

Strategy:
  - Bootstrap: Iterate through all decision IDs (1 to ~235,000).
  - Update: Fetch recent IDs beyond last known max.
  - Sample: Fetch 15 recent decisions for validation.

Detail page: https://www.scj.ro/1093/Detalii-jurisprudenta?customQuery[0].Key=id&customQuery[0].Value={id}

Usage:
  python bootstrap.py bootstrap          # Full initial pull (~235K decisions)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (new IDs)
"""

import sys
import json
import logging
import re
import html as html_mod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RO.ICCJ")

BASE_URL = "https://www.scj.ro"
DETAIL_PATH = "/1093/Detalii-jurisprudenta"

# Approximate max ID as of April 2026
APPROX_MAX_ID = 235000


class RoICCJScraper(BaseScraper):
    """
    Scraper for RO/ICCJ -- Romanian High Court of Cassation and Justice.
    Country: RO
    URL: https://www.scj.ro

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "ro,en;q=0.5",
            },
            timeout=30,
        )

    def _fetch_detail(self, decision_id: int) -> Optional[str]:
        """Fetch the HTML detail page for a given decision ID."""
        url = f"{DETAIL_PATH}?customQuery%5B0%5D.Key=id&customQuery%5B0%5D.Value={decision_id}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch ID {decision_id}: {e}")
            return None

    def _parse_detail(self, html_content: str, decision_id: int) -> Optional[dict]:
        """Parse a detail page HTML into a raw record dict."""
        if "jurisprudence_details" not in html_content:
            return None

        # Extract header section
        header_match = re.search(
            r'<div class="header">(.*?)</div>\s*<div',
            html_content,
            re.DOTALL,
        )
        header_text = ""
        section = ""
        decision_number = ""
        decision_date = ""

        if header_match:
            header_html = header_match.group(1)
            header_text = self._strip_html(header_html)

            # Extract court section — line after JUSTIȚIE
            section_match = re.search(
                r"(?:JUSTIŢIE|JUSTIȚIE)\s+(.+?)(?:\s*Decizia|\s*Decizie|\s*Sentinţ|\s*Sentința|\s*Hotărâre|\s*Încheiere|\s*$)",
                header_text,
                re.DOTALL | re.IGNORECASE,
            )
            if section_match:
                section = section_match.group(1).strip().strip("-–—\n").strip()
                # Clean up multiline section names
                section = re.sub(r"\s+", " ", section).strip()

            # Extract decision number
            dec_match = re.search(
                r"(?:Decizia|Decizie|Hotărârea|Sentinţa|Sentința|Încheierea)\s+nr\.\s*(\d+(?:/\d+)?)",
                header_text,
                re.IGNORECASE,
            )
            if dec_match:
                decision_number = dec_match.group(1)

            # Extract date from header
            date_match = re.search(
                r"din\s+(\d{1,2})\s+(ianuarie|februarie|martie|aprilie|mai|iunie|iulie|august|septembrie|octombrie|noiembrie|decembrie)\s+(\d{4})",
                header_text,
                re.IGNORECASE,
            )
            if date_match:
                day, month_name, year = date_match.groups()
                month_map = {
                    "ianuarie": "01", "februarie": "02", "martie": "03",
                    "aprilie": "04", "mai": "05", "iunie": "06",
                    "iulie": "07", "august": "08", "septembrie": "09",
                    "octombrie": "10", "noiembrie": "11", "decembrie": "12",
                }
                month = month_map.get(month_name.lower(), "01")
                decision_date = f"{year}-{month}-{int(day):02d}"

        # Extract content div
        content_start = html_content.find('<div class="content">')
        if content_start < 0:
            return None

        # Find matching closing </div> with depth tracking
        pos = content_start + 21  # len('<div class="content">')
        depth = 0
        content_end = len(html_content)
        while pos < len(html_content):
            next_open = html_content.find("<div", pos)
            next_close = html_content.find("</div>", pos)
            if next_close < 0:
                break
            if next_open >= 0 and next_open < next_close:
                depth += 1
                pos = next_open + 4
            else:
                if depth == 0:
                    content_end = next_close
                    break
                depth -= 1
                pos = next_close + 6

        content_html = html_content[content_start + 21:content_end]
        text = self._strip_html(content_html)

        if not text or len(text) < 50:
            return None

        # Build title
        if decision_number:
            title = f"Decizia nr. {decision_number}"
            if decision_date:
                title += f" din {decision_date}"
        else:
            # Use first ~100 chars of text as title
            title = text[:100].strip()
            if len(text) > 100:
                title = title.rsplit(" ", 1)[0] + "..."

        # Try to extract date from text if not found in header
        if not decision_date:
            date_match = re.search(
                r"(?:Şedinţa|Ședința|şedinţa|ședința)\s+public[ăa]\s+din\s+(?:data\s+de\s+)?(\d{1,2})\s+(ianuarie|februarie|martie|aprilie|mai|iunie|iulie|august|septembrie|octombrie|noiembrie|decembrie)\s+(\d{4})",
                text,
                re.IGNORECASE,
            )
            if date_match:
                day, month_name, year = date_match.groups()
                month_map = {
                    "ianuarie": "01", "februarie": "02", "martie": "03",
                    "aprilie": "04", "mai": "05", "iunie": "06",
                    "iulie": "07", "august": "08", "septembrie": "09",
                    "octombrie": "10", "noiembrie": "11", "decembrie": "12",
                }
                month = month_map.get(month_name.lower(), "01")
                decision_date = f"{year}-{month}-{int(day):02d}"

        return {
            "decision_id": decision_id,
            "title": title,
            "text": text,
            "date": decision_date or None,
            "section": section or None,
            "decision_number": decision_number or None,
            "header": header_text.strip() if header_text else None,
            "url": f"{BASE_URL}{DETAIL_PATH}?customQuery%5B0%5D.Key=id&customQuery%5B0%5D.Value={decision_id}",
        }

    def _strip_html(self, html_str: str) -> str:
        """Strip HTML tags and clean up text."""
        # Remove style and script blocks
        text = re.sub(r"<style[^>]*>.*?</style>", "", html_str, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
        # Replace <br> and </p> with newlines
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</div>", "\n", text, flags=re.IGNORECASE)
        # Remove all remaining tags
        text = re.sub(r"<[^>]+>", " ", text)
        # Decode HTML entities
        text = html_mod.unescape(text)
        # Clean up whitespace
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _find_max_id(self) -> int:
        """Binary search for the current max decision ID."""
        lo, hi = APPROX_MAX_ID - 2000, APPROX_MAX_ID + 5000
        # Verify lo exists
        html = self._fetch_detail(lo)
        if not html or "jurisprudence_details" not in html:
            lo = 1

        # Find upper bound
        html = self._fetch_detail(hi)
        if html and "jurisprudence_details" in html:
            hi += 10000

        while lo < hi - 1:
            mid = (lo + hi) // 2
            html = self._fetch_detail(mid)
            if html and "jurisprudence_details" in html:
                lo = mid
            else:
                hi = mid

        logger.info(f"Current max decision ID: {lo}")
        return lo

    # -- BaseScraper interface -----------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions by iterating through IDs."""
        max_id = self._find_max_id()
        consecutive_misses = 0
        max_misses = 50  # Allow gaps in IDs

        for decision_id in range(1, max_id + 1):
            html = self._fetch_detail(decision_id)
            if not html:
                consecutive_misses += 1
                if consecutive_misses > max_misses:
                    logger.warning(f"Too many consecutive misses at ID {decision_id}, skipping ahead")
                    consecutive_misses = 0
                continue

            record = self._parse_detail(html, decision_id)
            if record:
                consecutive_misses = 0
                yield record
            else:
                consecutive_misses += 1

            if decision_id % 1000 == 0:
                logger.info(f"Progress: ID {decision_id}/{max_id}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch decisions added since the last run by checking new IDs."""
        last_max = self.status.get("last_max_id", APPROX_MAX_ID - 1000)
        current_max = self._find_max_id()

        logger.info(f"Fetching updates: IDs {last_max + 1} to {current_max}")
        consecutive_misses = 0

        for decision_id in range(last_max + 1, current_max + 1):
            html = self._fetch_detail(decision_id)
            if not html:
                consecutive_misses += 1
                if consecutive_misses > 50:
                    break
                continue

            record = self._parse_detail(html, decision_id)
            if record:
                consecutive_misses = 0
                yield record

        self.status["last_max_id"] = current_max

    def normalize(self, raw: dict) -> dict:
        """Transform a raw decision into a standardized record."""
        return {
            "_id": f"RO-ICCJ-{raw['decision_id']}",
            "_source": "RO/ICCJ",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "decision_id": raw["decision_id"],
            "decision_number": raw.get("decision_number"),
            "section": raw.get("section"),
            "header": raw.get("header"),
        }


# -- CLI entry point ----------------------------------------------------------

if __name__ == "__main__":
    scraper = RoICCJScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))

    elif command == "test-api":
        print("Testing scj.ro detail page access...")
        html = scraper._fetch_detail(232100)
        if html and "jurisprudence_details" in html:
            record = scraper._parse_detail(html, 232100)
            if record:
                normalized = scraper.normalize(record)
                print(f"Success! Decision: {normalized['title']}")
                print(f"Text length: {len(normalized['text'])} chars")
                print(f"Date: {normalized['date']}")
                print(f"Section: {normalized['section']}")
                print(f"First 200 chars: {normalized['text'][:200]}")
            else:
                print("ERROR: Could not parse detail page")
                sys.exit(1)
        else:
            print("ERROR: Could not fetch detail page")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
