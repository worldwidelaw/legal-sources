#!/usr/bin/env python3
"""
BG/Parliament -- Bulgarian Parliament Stenographic Records

Fetches plenary stenographic records from the Bulgarian Parliament
(Народно събрание) via its official JSON REST API.

Strategy:
  - Iterate through stenographic record IDs (sparse range ~51 to ~11115+)
  - Fetch full text from /api/v1/pl-sten/{id} endpoint
  - Strip HTML tags from Pl_Sten_body field
  - Skip records with empty body (pre-digital historical records)

Data: ~6,000+ records with full text (2001-present)
License: Open access (government parliamentary records)
Rate limit: 1 req/sec (respectful).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BG.Parliament")

API_BASE = "https://www.parliament.bg/api/v1"

# ID range for stenographic records (sparse)
MIN_ID = 51
MAX_ID = 11200  # Upper bound, grows over time


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    # Replace <br /> and <br> with newlines
    text = re.sub(r'<br\s*/?>', '\n', html_text, flags=re.IGNORECASE)
    # Replace paragraph/div tags with double newlines
    text = re.sub(r'</?(?:p|div)[^>]*>', '\n\n', text, flags=re.IGNORECASE)
    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode HTML entities
    text = unescape(text)
    # Normalize whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' +\n', '\n', text)
    return text.strip()


class BGParliamentScraper(BaseScraper):
    """
    Scraper for BG/Parliament -- Bulgarian Parliament.
    Country: BG
    URL: https://www.parliament.bg

    Data types: doctrine (stenographic records)
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
            "Accept": "application/json",
        })

    def _fetch_record(self, record_id: int) -> Optional[dict]:
        """Fetch a single stenographic record by ID."""
        url = f"{API_BASE}/pl-sten/{record_id}"
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code != 200:
                return None
            data = resp.json()
            if not isinstance(data, dict) or "Pl_Sten_id" not in data:
                return None
            return data
        except (requests.RequestException, ValueError):
            return None

    def _has_text(self, record: dict) -> bool:
        """Check if a record has meaningful full text."""
        body = record.get("Pl_Sten_body", "")
        return len(body) > 200  # Skip empty or trivially short records

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all stenographic records with full text."""
        total = 0
        empty = 0
        consecutive_404 = 0

        for rid in range(MIN_ID, MAX_ID + 1):
            time.sleep(1)  # Rate limit

            record = self._fetch_record(rid)
            if record is None:
                consecutive_404 += 1
                if consecutive_404 > 200:
                    logger.info(f"200+ consecutive missing IDs at {rid}, stopping")
                    break
                continue

            consecutive_404 = 0

            if not self._has_text(record):
                empty += 1
                continue

            total += 1
            yield record

            if total % 100 == 0:
                logger.info(f"Progress: {total} records with text (ID {rid})")

        logger.info(f"Scan complete: {total} records with text, {empty} empty")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch records by scanning recent IDs (highest first)."""
        since_str = since.strftime("%Y-%m-%d")
        logger.info(f"Fetching updates since {since_str}")

        # Scan from highest known ID downward
        consecutive_404 = 0
        for rid in range(MAX_ID, MIN_ID - 1, -1):
            time.sleep(1)
            record = self._fetch_record(rid)
            if record is None:
                consecutive_404 += 1
                if consecutive_404 > 50:
                    break
                continue

            consecutive_404 = 0
            date = record.get("Pl_Sten_date", "")
            if date and date < since_str:
                break

            if self._has_text(record):
                yield record

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample records from recent sessions."""
        found = 0
        # Start from recent IDs (more likely to have full text)
        for rid in range(11100, MIN_ID - 1, -1):
            if found >= count:
                break

            time.sleep(1)
            record = self._fetch_record(rid)
            if record is None:
                continue

            if not self._has_text(record):
                continue

            found += 1
            logger.info(
                f"Sample {found}/{count}: ID {record['Pl_Sten_id']} "
                f"date={record.get('Pl_Sten_date', 'N/A')} "
                f"body={len(record.get('Pl_Sten_body', ''))} chars"
            )
            yield record

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw stenographic record to standard schema."""
        record_id = raw["Pl_Sten_id"]
        date = raw.get("Pl_Sten_date", "")
        subject = raw.get("Pl_Sten_sub", "")
        body_html = raw.get("Pl_Sten_body", "")

        # Clean subject line
        subject = re.sub(r'\r\n', ' | ', subject).strip()

        # Strip HTML from body
        text = strip_html(body_html)

        # Build title
        title = subject if subject else f"Пленарно заседание {date}"
        title = title[:500]

        return {
            "_id": f"BG-PARL-{record_id}",
            "_source": "BG/Parliament",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date if date else None,
            "url": f"https://www.parliament.bg/bg/plenaryst/ID/{record_id}",
            "session_id": record_id,
        }

    def test_api(self) -> bool:
        """Test API connectivity."""
        logger.info("Testing Bulgarian Parliament API...")

        # Test a known record
        record = self._fetch_record(11100)
        if record is None:
            logger.error("Failed to fetch record ID 11100")
            return False

        logger.info(f"API OK: ID={record['Pl_Sten_id']}, date={record.get('Pl_Sten_date')}")

        body = record.get("Pl_Sten_body", "")
        if len(body) < 200:
            logger.error(f"Body too short: {len(body)} chars")
            return False

        text = strip_html(body)
        logger.info(f"Text extraction OK: {len(text)} chars")
        logger.info("All tests passed")
        return True


# -- CLI entry point ---------------------------------------------------------

if __name__ == "__main__":
    scraper = BGParliamentScraper()

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
