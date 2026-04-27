#!/usr/bin/env python3
"""
MT/OAFS -- Malta Office of the Arbiter for Financial Services — Decisions

Fetches Arbiter decisions from https://www.financialarbiter.org.mt/oafs/decisions

Strategy:
  - Scrapes the paginated table view at /oafs/decisions?page=N&view=table
  - Each row contains: case reference, provider, outcome, date, appeal status
  - The case reference links to a PDF with the full decision text
  - PDFs are downloaded and text extracted via common/pdf_extract

Endpoints:
  - Listing: https://www.financialarbiter.org.mt/oafs/decisions?page={N}&view=table

Data:
  - Arbiter decisions on financial consumer complaints (~1000+ decisions)
  - Full text in Maltese or English (PDF)

License: Public regulatory data (Malta)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html
import logging
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import unquote

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MT.OAFS")

BASE_URL = "https://www.financialarbiter.org.mt"
DECISIONS_PATH = "/oafs/decisions"


class OAFSScraper(BaseScraper):
    """
    Scraper for MT/OAFS -- Malta Arbiter for Financial Services Decisions.
    Country: MT
    URL: https://www.financialarbiter.org.mt/oafs/decisions

    Data types: case_law
    Auth: none (public regulatory data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=60,
        )

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse DD/MM/YYYY date to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _extract_case_id(self, ref: str) -> str:
        """Extract a stable ID from the case reference, e.g. ASF-275-2025."""
        ref = ref.strip()
        # Normalize: "ASF 275/2025" -> "ASF-275-2025"
        ref = re.sub(r"[/\s]+", "-", ref)
        return ref

    def _parse_listing_page(self, page: int) -> list:
        """Fetch and parse one page of the decisions table. Returns list of raw dicts."""
        params = {
            "page": str(page),
            "view": "table",
        }
        try:
            self.rate_limiter.wait()
            resp = self.client.get(DECISIONS_PATH, params=params)
            if resp.status_code == 404:
                return []
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch page {page}: {e}")
            return []

        content = resp.text
        # Find table body
        tbody_match = re.search(r"<tbody>(.*?)</tbody>", content, re.DOTALL)
        if not tbody_match:
            logger.warning(f"No table body on page {page}")
            return []

        tbody = tbody_match.group(1)
        rows = re.findall(r"<tr>(.*?)</tr>", tbody, re.DOTALL)
        items = []

        for row in rows:
            cells = re.findall(r"<td>(.*?)</td>", row, re.DOTALL)
            if len(cells) < 4:
                continue

            # Cell 0: case reference with PDF link
            ref_match = re.search(r'href="([^"]+\.pdf)"[^>]*>([^<]+)</a>', cells[0])
            if not ref_match:
                continue

            pdf_url = ref_match.group(1)
            case_ref = html.unescape(ref_match.group(2)).strip()

            # Cell 1: provider
            provider = re.sub(r"<[^>]+>", "", cells[1]).strip()
            provider = html.unescape(provider)

            # Cell 2: outcome
            outcome = re.sub(r"<[^>]+>", "", cells[2]).strip()
            outcome = html.unescape(outcome)

            # Cell 3: date
            date_str = re.sub(r"<[^>]+>", "", cells[3]).strip()

            # Cell 4: appeal status (optional)
            appeal_status = ""
            if len(cells) > 4:
                appeal_status = re.sub(r"<[^>]+>", " ", cells[4]).strip()
                appeal_status = re.sub(r"\s+", " ", html.unescape(appeal_status)).strip()

            items.append({
                "case_reference": case_ref,
                "pdf_url": pdf_url,
                "provider": provider,
                "outcome": outcome,
                "date_str": date_str,
                "appeal_status": appeal_status,
            })

        return items

    def _fetch_pdf_text(self, pdf_url: str, case_ref: str) -> str:
        """Download a PDF and extract text."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(pdf_url, headers={"Accept": "application/pdf"})
            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {pdf_url}")
                return ""
            text = extract_pdf_markdown(
                source=case_ref,
                source_id="MT/OAFS",
                pdf_bytes=resp.content,
                table="case_law",
            )
            return text or ""
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return ""

    def fetch_all(self, sample_mode: bool = False, sample_size: int = 15) -> Generator[Dict[str, Any], None, None]:
        """Yield all decisions from the OAFS website."""
        page = 1
        total = 0
        empty_streak = 0

        while True:
            logger.info(f"Fetching page {page}...")
            items = self._parse_listing_page(page)

            if not items:
                empty_streak += 1
                if empty_streak >= 2:
                    logger.info(f"No more results after page {page}")
                    break
                page += 1
                continue

            empty_streak = 0

            for item in items:
                pdf_url = item["pdf_url"]
                case_ref = item["case_reference"]

                logger.info(f"Processing {case_ref}: downloading PDF...")
                text = self._fetch_pdf_text(pdf_url, case_ref)

                if not text or len(text) < 100:
                    logger.warning(f"Insufficient text for {case_ref}: {len(text)} chars")
                    continue

                yield {
                    "case_reference": case_ref,
                    "provider": item["provider"],
                    "outcome": item["outcome"],
                    "date_str": item["date_str"],
                    "appeal_status": item["appeal_status"],
                    "pdf_url": pdf_url,
                    "text": text,
                }
                total += 1

                if sample_mode and total >= sample_size:
                    logger.info(f"Sample mode: collected {total} records")
                    return

            page += 1

        logger.info(f"Finished: {total} decisions fetched")

    def fetch_updates(self, since: datetime) -> Generator[Dict[str, Any], None, None]:
        """Yield decisions published since the given date."""
        page = 1
        empty_streak = 0

        while True:
            items = self._parse_listing_page(page)
            if not items:
                empty_streak += 1
                if empty_streak >= 2:
                    break
                page += 1
                continue

            empty_streak = 0
            found_old = False

            for item in items:
                date_iso = self._parse_date(item["date_str"])
                if date_iso:
                    item_date = datetime.strptime(date_iso, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if item_date < since:
                        found_old = True
                        continue

                pdf_url = item["pdf_url"]
                case_ref = item["case_reference"]
                text = self._fetch_pdf_text(pdf_url, case_ref)
                if not text or len(text) < 100:
                    continue

                yield {
                    "case_reference": case_ref,
                    "provider": item["provider"],
                    "outcome": item["outcome"],
                    "date_str": item["date_str"],
                    "appeal_status": item["appeal_status"],
                    "pdf_url": pdf_url,
                    "text": text,
                }

            if found_old:
                break
            page += 1

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform raw decision data into standard schema."""
        case_ref = raw.get("case_reference", "")
        if not case_ref:
            return None

        text = raw.get("text", "")
        if not text or len(text) < 100:
            return None

        case_id = self._extract_case_id(case_ref)
        date_iso = self._parse_date(raw.get("date_str", ""))
        provider = raw.get("provider", "")
        outcome = raw.get("outcome", "")

        title = f"{case_ref} — {provider}" if provider else case_ref

        return {
            "_id": case_id,
            "_source": "MT/OAFS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_iso,
            "url": raw.get("pdf_url", ""),
            "case_reference": case_ref,
            "provider": provider,
            "outcome": outcome,
            "appeal_status": raw.get("appeal_status", ""),
            "language": "mt",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing OAFS decisions endpoint...")
        try:
            resp = self.client.get(DECISIONS_PATH, params={"page": "1", "view": "table"})
            print(f"  Status: {resp.status_code}")
            items = self._parse_listing_page(1)
            print(f"  Decisions on page 1: {len(items)}")
            if items:
                first = items[0]
                print(f"  First: {first['case_reference']} — {first['provider']}")
                print(f"  Date: {first['date_str']}, Outcome: {first['outcome']}")
                print(f"  PDF: {first['pdf_url'][:80]}...")
            print("Connection OK")
        except Exception as e:
            print(f"Connection failed: {e}")


def main():
    scraper = OAFSScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test]")
        print("  --sample  Limit to 15 sample records")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=15)
        else:
            stats = scraper.bootstrap()
        print(json.dumps(stats, indent=2, default=str))
    elif command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        print(json.dumps(stats, indent=2, default=str))
    elif command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
