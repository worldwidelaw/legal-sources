"""
World Wide Law — Scraper Template

Copy this file to sources/{COUNTRY}/{SOURCE}/bootstrap.py
and implement the three abstract methods.

The same file serves as both bootstrap and update script:
  python bootstrap.py bootstrap          # Full initial fetch
  python bootstrap.py bootstrap --sample  # Fetch 10 sample records
  python bootstrap.py update             # Incremental update

All you need to implement:
  1. fetch_all()      - yield all documents from the source
  2. fetch_updates()  - yield documents modified since a given date
  3. normalize()      - transform raw data into standard format
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]  # Go up to legal-data-hunter/
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter")


class SourceScraper(BaseScraper):
    """
    Scraper for: TODO_SOURCE_NAME
    Country: TODO_COUNTRY_CODE
    URL: TODO_SOURCE_URL

    Data types: TODO (legislation / case_law / both)
    Auth: TODO (none / api_key / oauth2)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Set up HTTP client with auth headers from config
        self.client = HttpClient(
            base_url=self.config.get("api", {}).get("base_url", ""),
            headers=self._auth_headers,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from the source.

        TODO: Implement pagination and data fetching.
        Common patterns:

        # For paginated REST API:
        page = 1
        while True:
            resp = self.client.get_json("/api/endpoint", params={"page": page, "per_page": 100})
            items = resp.get("results", [])
            if not items:
                break
            for item in items:
                yield item
            page += 1

        # For XML dump:
        import xml.etree.ElementTree as ET
        resp = self.client.get("https://example.com/dump.xml")
        root = ET.fromstring(resp.text)
        for elem in root.findall(".//article"):
            yield { "id": elem.get("id"), "text": elem.text, ... }

        # For HTML scraping:
        from bs4 import BeautifulSoup
        resp = self.client.get("https://example.com/list")
        soup = BeautifulSoup(resp.text, "html.parser")
        for item in soup.select("div.result"):
            detail_url = item.select_one("a")["href"]
            detail = self.client.get(detail_url)
            yield parse_detail_page(detail.text)
        """
        raise NotImplementedError("TODO: implement fetch_all()")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents modified/created since the given datetime.

        TODO: Implement incremental fetching.
        Common patterns:

        # For APIs with date filters:
        since_str = since.strftime("%Y-%m-%d")
        yield from self._fetch_with_date_filter(since_str)

        # For APIs without date filters (fall back to full fetch + dedup):
        # The base class handles deduplication, so you can just:
        yield from self.fetch_all()
        """
        raise NotImplementedError("TODO: implement fetch_updates()")

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        MUST return a dict with at least these fields:
          _id:          Unique identifier (str)
          _source:      Source identifier, e.g., "FR/legifrance" (str)
          _type:        "legislation" or "case_law" (str)
          _fetched_at:  Current UTC timestamp in ISO 8601 (str)

        Plus all source-specific fields that matter.

        TODO: Map the raw API fields to our standard format.

        Example:
        return {
            "_id": raw["articleId"],
            "_source": "FR/legifrance",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("content", ""),
            "date_publication": raw.get("datePubli"),
            "date_effective": raw.get("dateDebut"),
            "status": raw.get("etat"),
            "code": raw.get("codeName"),
            "url": raw.get("url"),
            # ... include ALL fields from the raw data
            "_raw_fields": {k: v for k, v in raw.items() if k not in mapped_fields},
        }
        """
        raise NotImplementedError("TODO: implement normalize()")


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = SourceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
