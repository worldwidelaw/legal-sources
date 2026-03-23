#!/usr/bin/env python3
"""
HR/AdminCourt -- Croatian High Administrative Court Data Fetcher

Fetches decisions from the Visoki upravni sud via the unified court
decisions portal odluke.sudovi.hr.

Strategy:
  - Paginate through list pages: /Document/DisplayList?ct=vus&sort=dat&page=N
  - Extract decision GUIDs from each page
  - Fetch individual decision pages for full text + metadata
  - Full text is embedded in HTML (div.decision-text)

Endpoints:
  - List: https://odluke.sudovi.hr/Document/DisplayList?ct=vus&sort=dat&page=N
  - View: https://odluke.sudovi.hr/Document/View?id={GUID}

Data:
  - ~7,615 decisions (second-instance administrative appeals)
  - Language: Croatian (HR)
  - Case numbers: Usž-NNN/YYYY format
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HR.admincourt")

BASE_URL = "https://odluke.sudovi.hr"
LIST_URL = "/Document/DisplayList"
VIEW_URL = "/Document/View"
COURT_CODE = "vus"  # Visoki upravni sud


class CroatianAdminCourtScraper(BaseScraper):
    """
    Scraper for HR/AdminCourt -- Croatian High Administrative Court.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "hr,en",
            },
            timeout=60,
        )

    def _get_total_pages(self) -> int:
        """Get total number of pages from the list."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"{LIST_URL}?ct={COURT_CODE}&sort=dat&page=1")
            resp.raise_for_status()
            content = resp.text

            # Look for total results count (e.g., "7615 rezultata")
            match = re.search(r'([\d.]+)\s+rezultat', content)
            if match:
                total = int(match.group(1).replace(".", ""))
                pages = (total + 9) // 10  # 10 results per page
                logger.info(f"Found {total} total decisions ({pages} pages)")
                return pages

            # Fallback: look for last page number in pagination
            page_matches = re.findall(r'page=(\d+)', content)
            if page_matches:
                return max(int(p) for p in page_matches)

            return 1
        except Exception as e:
            logger.error(f"Failed to get total pages: {e}")
            return 1

    def _scrape_list_page(self, page: int) -> List[str]:
        """Scrape a list page and return decision GUIDs."""
        guids = []
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"{LIST_URL}?ct={COURT_CODE}&sort=dat&page={page}")
            resp.raise_for_status()
            content = resp.text

            # Extract GUIDs from decision links
            for match in re.finditer(
                r'/Document/View\?id=([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})',
                content, re.IGNORECASE
            ):
                guid = match.group(1)
                if guid not in guids:
                    guids.append(guid)

            logger.info(f"Page {page}: found {len(guids)} decisions")
        except Exception as e:
            logger.error(f"Failed to scrape list page {page}: {e}")

        return guids

    def _extract_metadata(self, content: str) -> Dict[str, str]:
        """Extract all data-metadata-type fields from HTML."""
        metadata = {}
        for match in re.finditer(
            r'data-metadata-type="([^"]+)"[^>]*>.*?'
            r'<p\s+class="metadata-content"[^>]*>(.*?)</p>',
            content, re.DOTALL
        ):
            key = match.group(1)
            value = re.sub(r'<[^>]+>', '', match.group(2)).strip()
            value = html.unescape(value)
            if key not in metadata:  # take first occurrence (desktop, not mobile duplicate)
                metadata[key] = value
        return metadata

    def _parse_date(self, date_str: str) -> str:
        """Parse Croatian date format D.M.YYYY. to ISO 8601."""
        date_str = date_str.rstrip('.')
        match = re.match(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_str)
        if match:
            day, month, year = match.group(1), match.group(2), match.group(3)
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        return ""

    def _fetch_decision(self, guid: str) -> Optional[Dict[str, Any]]:
        """Fetch a single decision page and extract text + metadata."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"{VIEW_URL}?id={guid}")
            resp.raise_for_status()
            content = resp.text

            result = {"guid": guid, "url": f"{BASE_URL}{VIEW_URL}?id={guid}"}

            # Extract structured metadata from data-metadata-type attributes
            meta = self._extract_metadata(content)

            if meta.get("decision-number"):
                result["case_number"] = meta["decision-number"]

            if meta.get("decision-date"):
                result["date"] = self._parse_date(meta["decision-date"])

            if meta.get("ecli-number"):
                result["ecli"] = meta["ecli-number"].rstrip('<').strip()

            if meta.get("decision-type"):
                result["decision_type"] = meta["decision-type"]

            if meta.get("court"):
                result["court"] = meta["court"]

            # Extract full text from decision-text div
            text_match = re.search(
                r'<div[^>]*class="[^"]*decision-text[^"]*"[^>]*>(.*?)</div>\s*(?:</div>|<div)',
                content, re.DOTALL
            )
            if not text_match:
                text_match = re.search(
                    r'class="decision-text"[^>]*>(.*?)</div>',
                    content, re.DOTALL
                )

            if text_match:
                text_html = text_match.group(1)
                # Remove style blocks and their content
                text_html = re.sub(r'<style[^>]*>.*?</style>', '', text_html, flags=re.DOTALL)
                text = re.sub(r'<br\s*/?>', '\n', text_html)
                text = re.sub(r'<[^>]+>', ' ', text)
                text = html.unescape(text)
                text = re.sub(r'[ \t]+', ' ', text)
                text = re.sub(r'\n\s*\n', '\n\n', text)
                text = text.strip()

                if len(text) > 100:
                    result["text"] = text

            return result if result.get("text") else None

        except Exception as e:
            logger.warning(f"Failed to fetch decision {guid}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standard schema."""
        case_number = raw.get("case_number", raw.get("guid", "unknown"))
        title = f"Visoki upravni sud - {case_number}"
        if raw.get("decision_type"):
            title = f"{raw['decision_type']} - {case_number}"

        return {
            "_id": f"HR-VUS-{raw['guid']}",
            "_source": "HR/AdminCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "case_number": case_number,
            "ecli": raw.get("ecli", ""),
            "decision_type": raw.get("decision_type", ""),
            "language": "hr",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions."""
        count = 0

        if sample:
            limit = 15
            total_pages = 2  # Just 2 pages for sample
        else:
            limit = 99999
            total_pages = self._get_total_pages()

        logger.info(f"Fetching up to {limit} decisions from {total_pages} pages")

        for page in range(1, total_pages + 1):
            if count >= limit:
                break

            guids = self._scrape_list_page(page)

            for guid in guids:
                if count >= limit:
                    break

                decision = self._fetch_decision(guid)
                if decision:
                    yield decision
                    count += 1
                    logger.info(
                        f"[{count}] {decision.get('case_number', guid)}: "
                        f"{len(decision.get('text', ''))} chars"
                    )

        logger.info(f"=== Total: {count} decisions fetched ===")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch decisions updated since a date (sorted by date desc)."""
        for doc in self.fetch_all():
            if doc.get("date", "") >= since:
                yield doc
            elif doc.get("date"):
                # Since sorted by date desc, we can stop early
                break


def main():
    scraper = CroatianAdminCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
