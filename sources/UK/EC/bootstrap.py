#!/usr/bin/env python3
"""
Legal Data Hunter - UK Electoral Commission (EC) Scraper

Fetches Electoral Commission enforcement decisions (investigations into
political parties, campaigners, etc.) from the Wayback Machine archive.

The live site (electoralcommission.org.uk) is Cloudflare-protected, so we
use the most recent Wayback Machine snapshot of the investigations page.

Coverage: ~193 enforcement decisions across 5 fiscal year tables.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
"""

import re
import sys
import json
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/EC")

# Wayback Machine snapshot of the investigations page
WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
INVESTIGATIONS_URL = "https://www.electoralcommission.org.uk/political-registration-and-regulation/our-enforcement-work/investigations"

# Fiscal year periods for each table (approximate)
TABLE_PERIODS = [
    "April 2024 - Present",
    "Before April 2024 (recent)",
    "April 2023 - March 2024",
    "April 2022 - March 2023",
    "April 2021 - March 2022",
]


class UKECScraper(BaseScraper):
    """
    Scraper for UK Electoral Commission enforcement decisions.

    Strategy:
    - Find the most recent Wayback Machine snapshot of the investigations page
    - Parse the 5 HTML tables containing enforcement decisions
    - Each row contains: Subject, Investigated for, Decision, Summary of reason(s)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="https://web.archive.org",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "text/html",
                "Accept-Encoding": "gzip, deflate",
            },
            timeout=60,
        )

    def _get_latest_snapshot_url(self) -> Optional[str]:
        """Find the most recent Wayback Machine snapshot URL."""
        self.rate_limiter.wait()
        try:
            import requests
            resp = requests.get(
                WAYBACK_CDX_URL,
                params={
                    "url": INVESTIGATIONS_URL,
                    "output": "json",
                    "limit": 1,
                    "sort": "reverse",
                    "fl": "timestamp,statuscode",
                    "filter": "statuscode:200",
                },
                headers={"User-Agent": "LegalDataHunter/1.0"},
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                if len(data) > 1:  # First row is headers
                    timestamp = data[1][0]
                    return f"/web/{timestamp}id_/{INVESTIGATIONS_URL}"
            logger.warning("No Wayback Machine snapshot found")
            return None
        except Exception as e:
            logger.error(f"CDX query failed: {e}")
            return None

    def _fetch_investigations_page(self, snapshot_path: str) -> Optional[str]:
        """Fetch the HTML content of the investigations page from Wayback Machine."""
        self.rate_limiter.wait()
        try:
            import requests
            url = f"https://web.archive.org{snapshot_path}"
            resp = requests.get(
                url,
                headers={
                    "User-Agent": "LegalDataHunter/1.0",
                    "Accept-Encoding": "gzip, deflate",
                },
                timeout=60,
            )
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"Wayback fetch returned {resp.status_code}")
            return None
        except Exception as e:
            logger.error(f"Wayback fetch failed: {e}")
            return None

    def _parse_tables(self, html: str) -> list:
        """Parse all investigation tables from the HTML."""
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")
        records = []

        for table_idx, table in enumerate(tables):
            period = TABLE_PERIODS[table_idx] if table_idx < len(TABLE_PERIODS) else f"Period {table_idx}"
            rows = table.find_all("tr")

            for row in rows[1:]:  # Skip header row
                cells = row.find_all(["td", "th"])
                if len(cells) < 4:
                    continue

                subject = cells[0].get_text(separator=" ").strip()
                investigated_for = cells[1].get_text(separator=" ").strip()
                decision = cells[2].get_text(separator=" ").strip()
                reasoning = cells[3].get_text(separator=" ").strip()

                if not subject:
                    continue

                records.append({
                    "subject": subject,
                    "investigated_for": investigated_for,
                    "decision": decision,
                    "reasoning": reasoning,
                    "period": period,
                    "table_index": table_idx,
                })

        return records

    def _make_id(self, record: dict) -> str:
        """Generate a unique ID from record content."""
        key = f"{record['subject']}|{record['investigated_for'][:100]}"
        return hashlib.sha256(key.encode()).hexdigest()[:16]

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Electoral Commission enforcement decisions."""
        logger.info("Finding latest Wayback Machine snapshot...")
        snapshot_path = self._get_latest_snapshot_url()
        if not snapshot_path:
            logger.error("No snapshot available")
            return

        logger.info(f"Fetching snapshot: {snapshot_path}")
        html = self._fetch_investigations_page(snapshot_path)
        if not html:
            logger.error("Failed to fetch investigations page")
            return

        records = self._parse_tables(html)
        logger.info(f"Parsed {len(records)} enforcement decisions")

        for record in records:
            yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield all records (no date filtering possible for table data)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw table row into standard schema."""
        subject = raw.get("subject", "")
        investigated_for = raw.get("investigated_for", "")
        decision = raw.get("decision", "")
        reasoning = raw.get("reasoning", "")

        # Build full text from all fields
        text_parts = []
        if subject:
            text_parts.append(f"Subject: {subject}")
        if investigated_for:
            text_parts.append(f"Investigated for: {investigated_for}")
        if decision:
            text_parts.append(f"Decision: {decision}")
        if reasoning:
            text_parts.append(f"Summary of reasons: {reasoning}")

        text = "\n\n".join(text_parts)

        if not text or len(text) < 50:
            return None

        doc_id = self._make_id(raw)
        title = f"Investigation: {subject}" if subject else "Electoral Commission Investigation"
        period = raw.get("period", "")

        return {
            "_id": f"uk_ec_{doc_id}",
            "_source": "UK/EC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": None,  # No specific date in table data
            "url": INVESTIGATIONS_URL,
            "subject": subject,
            "investigated_for": investigated_for,
            "decision_text": decision,
            "reasoning": reasoning,
            "period": period,
        }


# ── CLI entry point ──────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKECScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
