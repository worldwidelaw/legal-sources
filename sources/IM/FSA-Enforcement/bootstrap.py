#!/usr/bin/env python3
"""
IM/FSA-Enforcement -- Isle of Man FSA Enforcement Actions

Fetches enforcement actions from the Isle of Man Financial Services Authority:
  - Discretionary civil penalties (from HTML table)
  - Prohibited persons (from accordion sections)
  - Disqualified directors (from accordion sections)
  - Full text from linked press releases / public statements

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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IM.FSA-Enforcement")

BASE_URL = "https://www.iomfsa.im"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def strip_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|li|h[1-6]|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_date(date_str: str) -> Optional[str]:
    """Parse various date formats to ISO 8601."""
    date_str = date_str.strip()
    for fmt in [
        "%d %B %Y", "%d %b %Y", "%B %Y", "%d/%m/%Y",
        "%Y-%m-%d", "%d %B, %Y",
    ]:
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    # Try extracting just month/year
    match = re.search(r"(\w+)\s+(\d{4})", date_str)
    if match:
        try:
            return datetime.strptime(f"1 {match.group(1)} {match.group(2)}", "%d %B %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


class FSAEnforcementScraper(BaseScraper):
    """Scraper for Isle of Man FSA enforcement actions."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers=HEADERS,
            max_retries=4,
            backoff_factor=2.0,
            timeout=30,
        )

    def _get_page(self, url: str) -> Optional[str]:
        """Fetch an HTML page (HttpClient handles retries)."""
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

    def _fetch_press_release(self, url: str) -> Optional[dict]:
        """Fetch full text from a press release page."""
        if not url.startswith("http"):
            url = BASE_URL + url
        html = self._get_page(url)
        if not html:
            return None

        # Extract title from <h1>
        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
        title = strip_html(title_match.group(1)) if title_match else ""

        # Extract date from <time>
        date_match = re.search(r"<time[^>]*>(.*?)</time>", html, re.DOTALL)
        date_str = strip_html(date_match.group(1)) if date_match else ""
        # Remove "Published on: " prefix
        date_str = re.sub(r"^Published on:\s*", "", date_str, flags=re.IGNORECASE)

        # Extract content from <div class="rte">
        rte_match = re.search(r'<div\s+class="rte"[^>]*>(.*?)</div>\s*</div>', html, re.DOTALL)
        if not rte_match:
            # Try broader match
            rte_match = re.search(r'<div\s+class="rte">(.*?)(?=<div\s+class="(?:page-actions|related))', html, re.DOTALL)
        if not rte_match:
            # Fallback: extract from page-content
            rte_match = re.search(r'<div\s+class="page-content"[^>]*>(.*?)</div>\s*</section>', html, re.DOTALL)

        text = strip_html(rte_match.group(1)) if rte_match else ""

        return {
            "title": title,
            "date": parse_date(date_str),
            "text": text,
        }

    def _parse_civil_penalties(self, html: str) -> list:
        """Parse the civil penalties table."""
        entries = []
        # Find the table
        table_match = re.search(r"<table[^>]*>(.*?)</table>", html, re.DOTALL)
        if not table_match:
            return entries

        table_html = table_match.group(1)
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", table_html, re.DOTALL)

        for row in rows[1:]:  # Skip header row
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            if len(cells) < 6:
                continue

            name = strip_html(cells[0])
            gross_amount = strip_html(cells[1])
            net_amount = strip_html(cells[2])

            # Extract press release link
            link_match = re.search(r'href="([^"]+)"', cells[3])
            press_release_url = link_match.group(1) if link_match else ""

            date_str = strip_html(cells[4])
            description = strip_html(cells[5])

            legislation = strip_html(cells[6]) if len(cells) > 6 else ""
            regulations = strip_html(cells[7]) if len(cells) > 7 else ""

            entries.append({
                "enforcement_type": "civil_penalty",
                "subject_name": name,
                "penalty_gross": gross_amount,
                "penalty_net": net_amount,
                "press_release_url": press_release_url,
                "date_str": date_str,
                "description": description,
                "applicable_legislation": f"{legislation}; {regulations}".strip("; "),
            })

        return entries

    def _parse_accordion_sections(self, html: str, enforcement_type: str) -> list:
        """Parse accordion sections for prohibited persons or disqualified directors."""
        entries = []
        # Find all accordion items
        sections = re.findall(
            r'<section\s+class="accordion-item"[^>]*>(.*?)</section>',
            html, re.DOTALL
        )

        for section in sections:
            # Extract name from accordion header
            header_match = re.search(r'<(?:h[2-4]|button|span)[^>]*class="[^"]*accordion[^"]*"[^>]*>(.*?)</(?:h[2-4]|button|span)>', section, re.DOTALL)
            if not header_match:
                header_match = re.search(r'<h[2-4][^>]*>(.*?)</h[2-4]>', section, re.DOTALL)
            name = strip_html(header_match.group(1)) if header_match else "Unknown"

            # Extract all content
            content = strip_html(section)

            # Look for press release/public statement links
            link_match = re.search(r'href="(/fsa-news/[^"]+)"', section)
            press_url = link_match.group(1) if link_match else ""

            # Try to extract date
            date_match = re.search(r"Date\s+of\s+(?:prohibition|disqualification)[:\s]*([\d\w\s,]+?)(?:\.|<|$)", section, re.IGNORECASE)
            date_str = strip_html(date_match.group(1)) if date_match else ""

            entries.append({
                "enforcement_type": enforcement_type,
                "subject_name": name,
                "press_release_url": press_url,
                "date_str": date_str,
                "description": content,
            })

        return entries

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all enforcement actions with full text."""
        # 1. Civil penalties and prohibited persons from enforcement-action page
        logger.info("Fetching enforcement actions page...")
        ea_html = self._get_page(f"{BASE_URL}/enforcement/enforcement-action/")
        if ea_html:
            # Civil penalties
            penalties = self._parse_civil_penalties(ea_html)
            logger.info(f"Found {len(penalties)} civil penalties")
            for entry in penalties:
                time.sleep(0.5)
                if entry.get("press_release_url"):
                    pr = self._fetch_press_release(entry["press_release_url"])
                    if pr and pr.get("text"):
                        entry["full_text"] = pr["text"]
                        entry["title"] = pr.get("title", entry.get("subject_name", ""))
                        if pr.get("date"):
                            entry["date_parsed"] = pr["date"]
                yield entry

            # Prohibited persons
            # Split HTML after the table for prohibited persons section
            parts = ea_html.split("Prohibited Person", 1)
            if len(parts) > 1:
                prohibited = self._parse_accordion_sections(parts[1], "prohibited_person")
                logger.info(f"Found {len(prohibited)} prohibited persons")
                for entry in prohibited:
                    time.sleep(0.5)
                    if entry.get("press_release_url"):
                        pr = self._fetch_press_release(entry["press_release_url"])
                        if pr and pr.get("text"):
                            entry["full_text"] = pr["text"]
                            entry["title"] = pr.get("title", entry.get("subject_name", ""))
                            if pr.get("date"):
                                entry["date_parsed"] = pr["date"]
                    yield entry

        # 2. Disqualified directors
        logger.info("Fetching disqualified directors page...")
        dd_html = self._get_page(f"{BASE_URL}/enforcement/disqualified-directors/")
        if dd_html:
            directors = self._parse_accordion_sections(dd_html, "disqualified_director")
            logger.info(f"Found {len(directors)} disqualified directors")
            for entry in directors:
                time.sleep(0.5)
                if entry.get("press_release_url"):
                    pr = self._fetch_press_release(entry["press_release_url"])
                    if pr and pr.get("text"):
                        entry["full_text"] = pr["text"]
                        entry["title"] = pr.get("title", entry.get("subject_name", ""))
                        if pr.get("date"):
                            entry["date_parsed"] = pr["date"]
                yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Re-fetch all (small dataset)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw entry into standard schema."""
        enforcement_type = raw.get("enforcement_type", "unknown")
        subject = raw.get("subject_name", "Unknown")

        # Build doc_id
        slug = re.sub(r"[^a-z0-9]+", "-", subject.lower()).strip("-")[:60]
        doc_id = f"{enforcement_type}/{slug}"

        # Determine title
        title = raw.get("title", "")
        if not title:
            title = f"{enforcement_type.replace('_', ' ').title()}: {subject}"

        # Build full text
        text = raw.get("full_text", "")
        if not text:
            # Use description as fallback
            text = raw.get("description", "")

        if not text or len(text) < 50:
            return None  # Skip entries with no meaningful text

        # Parse date
        date = raw.get("date_parsed")
        if not date and raw.get("date_str"):
            date = parse_date(raw["date_str"])

        # Build URL
        url = raw.get("press_release_url", "")
        if url and not url.startswith("http"):
            url = BASE_URL + url
        if not url:
            url = f"{BASE_URL}/enforcement/enforcement-action/"

        return {
            "_id": doc_id,
            "_source": "IM/FSA-Enforcement",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": doc_id,
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "enforcement_type": enforcement_type,
            "subject_name": subject,
            "penalty_amount": raw.get("penalty_net") or raw.get("penalty_gross"),
            "applicable_legislation": raw.get("applicable_legislation", ""),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to IOMFSA."""
        try:
            resp = self.client.get(
                f"{BASE_URL}/enforcement/enforcement-action/", timeout=15
            )
            if resp.status_code == 200 and ("enforcement" in resp.text.lower() or "penalty" in resp.text.lower()):
                logger.info("Connection test passed")
                return True
            logger.error(f"Unexpected response: status={resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = FSAEnforcementScraper()

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
