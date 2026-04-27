#!/usr/bin/env python3
"""
KY/OfficialGazette -- Cayman Islands Consolidated Legislation

Fetches all current legislation from legislation.gov.ky:
  - Alphabetical listing (A-Z) via POST to /cms/legislation/current/by-title.html
  - Both Principal Acts and Subordinate legislation (regulations)
  - PDF downloads from /cms/images/LEGISLATION/{PRINCIPAL,SUBORDINATE}/
  - Text extraction via common.pdf_extract
  - ~400+ current laws and regulations

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
from common.pdf_extract import extract_pdf_markdown

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KY.OfficialGazette")

BASE_URL = "https://legislation.gov.ky"
BY_TITLE_URL = f"{BASE_URL}/cms/legislation/current/by-title.html"

# Site blocks browser-like User-Agents; use a simple identifier
HEADERS = {
    "Accept": "text/html,application/xhtml+xml",
}

MONTH_MAP = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}

LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def _parse_date(date_str: str) -> Optional[str]:
    """Parse dates like 'Thursday, 01 February 2024' to ISO."""
    if not date_str:
        return None
    m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", date_str)
    if not m:
        return None
    day, month_name, year = m.groups()
    month = MONTH_MAP.get(month_name.lower())
    if not month:
        return None
    return f"{year}-{month}-{int(day):02d}"


def _extract_year_from_title(title: str) -> Optional[str]:
    """Extract year from title like 'Companies Act (2022 Revision)'."""
    m = re.search(r"\((\d{4})\s+(?:Revision|Consolidation)\)", title)
    if m:
        return m.group(1)
    m = re.search(r"Act\s+(\d+)\s+of\s+(\d{4})", title)
    if m:
        return m.group(2)
    m = re.search(r"SL\s+\d+\s+of\s+(\d{4})", title)
    if m:
        return m.group(1)
    return None


class LegislationScraper(BaseScraper):
    """Scraper for Cayman Islands consolidated legislation."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _fetch_letter(self, letter: str) -> list[dict]:
        """Fetch all legislation entries for a given letter."""
        try:
            resp = self.session.post(
                BY_TITLE_URL,
                data={
                    "submit4": letter,
                    "pointintime_post_alpha": datetime.now().strftime("%Y-%m-%d 00:00:00"),
                },
                timeout=60,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch letter {letter}: {e}")
            return []

        html = resp.text
        entries = []
        seen_ids = set()

        # Parse each table row with a PDF link
        for row_match in re.finditer(
            r'<tr\s+class="(?:row\d+|collapse[^"]*)">(.*?)</tr>',
            html,
            re.DOTALL,
        ):
            row = row_match.group(1)

            # Extract item number from data-bs-content
            item_match = re.search(
                r'Item Number:\s*</strong>\s*([^<"]+)',
                row,
            )
            item_number = item_match.group(1).strip() if item_match else None

            # Extract PDF link (current revision, not _g.pdf gazette version)
            pdf_match = re.search(
                r'<a\s+class="npWrap"\s+href="(/cms/images/LEGISLATION/[^"]+\.pdf)"[^>]*>([^<]+)',
                row,
                re.DOTALL,
            )
            if not pdf_match:
                continue

            pdf_path = pdf_match.group(1)
            title = pdf_match.group(2).strip()

            # Skip gazette versions (_g.pdf)
            if pdf_path.endswith("_g.pdf"):
                continue

            # Determine legislation type from path
            if "/PRINCIPAL/" in pdf_path:
                leg_type = "principal"
            elif "/SUBORDINATE/" in pdf_path:
                leg_type = "subordinate"
            elif "/AMENDING/" in pdf_path:
                leg_type = "amending"
            else:
                leg_type = "other"

            # Build a stable ID from item number or path
            if item_number:
                doc_id = item_number
            else:
                # Extract from path: /YYYY/YYYY-NNNN/
                path_match = re.search(r"/(\d{4}-\d{4})/", pdf_path)
                doc_id = path_match.group(1) if path_match else pdf_path.split("/")[-1].replace(".pdf", "")

            # Skip duplicates (same item may appear in parent and child rows)
            stable_key = f"{doc_id}:{pdf_path}"
            if stable_key in seen_ids:
                continue
            seen_ids.add(stable_key)

            # Extract subject/category
            subject_match = re.search(
                r"class='rowtag'>([^<]+)<",
                row,
            )
            subject = subject_match.group(1).strip() if subject_match else None

            # Extract commencement date
            date_match = re.search(
                r'title="Commencement Date"[^>]*data-bs-content="([^"]+)"',
                row,
            )
            date_iso = _parse_date(date_match.group(1)) if date_match else None

            # Fallback: try to extract year from title
            if not date_iso:
                year = _extract_year_from_title(title)
                if year:
                    date_iso = f"{year}-01-01"

            # Build full PDF URL (handle spaces in URL)
            pdf_url = BASE_URL + pdf_path.replace(" ", "%20")

            # Extract version info from title
            version_match = re.search(
                r'\[([^\]]+)\]',
                row,
            )
            # Also check in the broader context after the title link
            if not version_match:
                version_match = re.search(
                    r'Revision/Version number[^>]*>([^<]*(?:\d{4})\s+(?:Revision|Consolidation))',
                    row,
                )

            entries.append({
                "item_number": doc_id,
                "title": title,
                "date": date_iso,
                "pdf_url": pdf_url,
                "url": pdf_url,
                "legislation_type": leg_type,
                "subject": subject,
                "letter": letter,
            })

        logger.info(f"Letter {letter}: found {len(entries)} entries")
        return entries

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation entries with full text."""
        all_entries = []

        for letter in LETTERS:
            entries = self._fetch_letter(letter)
            all_entries.extend(entries)
            time.sleep(10)  # Respect crawl-delay: 10

        logger.info(f"Total legislation entries found: {len(all_entries)}")

        for i, entry in enumerate(all_entries):
            text = extract_pdf_markdown(
                "KY/OfficialGazette",
                entry["item_number"],
                pdf_url=entry["pdf_url"],
                table="legislation",
            )
            if not text:
                logger.debug(f"No text for {entry['item_number']}, skipping")
                continue

            entry["text"] = text
            yield entry

            if (i + 1) % 25 == 0:
                logger.info(f"Processed {i + 1}/{len(all_entries)} entries")

            time.sleep(2)  # Rate limiting for PDF downloads

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recently changed legislation."""
        since_str = since.strftime("%Y-%m-%d")
        all_entries = []

        for letter in LETTERS:
            entries = self._fetch_letter(letter)
            recent = [e for e in entries if e.get("date") and e["date"] >= since_str]
            all_entries.extend(recent)
            time.sleep(10)

        logger.info(f"Found {len(all_entries)} entries since {since_str}")

        for entry in all_entries:
            text = extract_pdf_markdown(
                "KY/OfficialGazette",
                entry["item_number"],
                pdf_url=entry["pdf_url"],
                table="legislation",
                force=True,
            )
            if text:
                entry["text"] = text
                yield entry
            time.sleep(2)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        text = raw.get("text", "")
        if not text or len(text) < 50:
            return None

        return {
            "_id": raw["item_number"],
            "_source": "KY/OfficialGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "item_number": raw["item_number"],
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "legislation_type": raw.get("legislation_type"),
            "subject": raw.get("subject"),
            "language": "eng",
        }

    def test_connection(self) -> bool:
        """Test connectivity to legislation.gov.ky."""
        try:
            resp = self.session.get(BY_TITLE_URL, timeout=30)
            resp.raise_for_status()
            if "legislation" in resp.text.lower():
                logger.info("Connection test passed")
                return True
            logger.error("Connection test: unexpected content")
            return False
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


if __name__ == "__main__":
    scraper = LegislationScraper()

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
