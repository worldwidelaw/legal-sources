#!/usr/bin/env python3
"""
DK/NaevnenesHus -- Denmark Centralized Tribunal Portal (Naevnenes Hus)

Fetches decisions from 14 Danish administrative tribunals via their shared
REST API at https://{subdomain}.naevneneshus.dk/api/search.

Each tribunal runs an Angular SPA backed by a .NET Core REST API.
The search endpoint returns full decision text (HTML) in the `body` field.
No authentication required.

Strategy:
  - For each tribunal subdomain, POST /api/search with year-by-year date ranges
    to stay under the 10,000 totalCount cap (two portals exceed 10K total).
  - Use size=1000 per page, paginate with skip.
  - Full text comes in the body field; strip HTML tags.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (current year)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import re
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
logger = logging.getLogger("legal-data-hunter.DK.NaevnenesHus")

SOURCE_ID = "DK/NaevnenesHus"

# All 14 tribunal subdomains, ordered by approximate size (largest first)
TRIBUNALS = [
    ("mfkn", "Miljø- og Fødevareklagenævnet"),
    ("pkn", "Planklagenævnet"),
    ("dkbb", "Disciplinær- og klagenævnet for beskikkede bygningssagkyndige"),
    ("ekn", "Energiklagenævnet"),
    ("byg", "Byggeklageenheden"),
    ("klfu", "Klagenævnet for Udbud"),
    ("apv", "Ankenævnet for Patenter og Varemærker"),
    ("rn", "Revisornævnet"),
    ("ean", "Erhvervsankenævnet"),
    ("fkn", "Forbrugerklagenævnet"),
    ("dnfe", "Disciplinærnævnet for Ejendomsmæglere"),
    ("tvist", "Tvistighedsnævnet"),
    ("tele", "Teleklagenævnet"),
    ("byf", "Byfornyelsesnævnene"),
]

PAGE_SIZE = 1000
MIN_YEAR = 2000
RATE_DELAY = 1.0


class NaevnenesHusScraper(BaseScraper):
    """
    Scraper for DK/NaevnenesHus -- Danish Centralized Tribunal Portal.
    Country: DK
    URL: https://naevneneshus.dk/afgoerelsesportaler/

    Data types: case_law (administrative tribunal decisions)
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all decisions from all tribunals."""
        for subdomain, tribunal_name in TRIBUNALS:
            logger.info(f"=== Fetching tribunal: {tribunal_name} ({subdomain}) ===")
            yield from self._fetch_tribunal(subdomain, tribunal_name)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch decisions from the current year for all tribunals."""
        current_year = datetime.now().year
        for subdomain, tribunal_name in TRIBUNALS:
            logger.info(f"Updating {tribunal_name} ({subdomain}) for {current_year}")
            yield from self._fetch_tribunal_year(subdomain, tribunal_name, current_year)

    def _fetch_tribunal(self, subdomain: str, tribunal_name: str) -> Generator[dict, None, None]:
        """Fetch all decisions for a single tribunal, year by year."""
        current_year = datetime.now().year
        for year in range(current_year, MIN_YEAR - 1, -1):
            count = 0
            for record in self._fetch_tribunal_year(subdomain, tribunal_name, year):
                count += 1
                yield record
            if count > 0:
                logger.info(f"  {subdomain}/{year}: {count} decisions")

    def _fetch_tribunal_year(
        self, subdomain: str, tribunal_name: str, year: int
    ) -> Generator[dict, None, None]:
        """Fetch all decisions for a tribunal in a given year."""
        base_url = f"https://{subdomain}.naevneneshus.dk"
        search_url = f"{base_url}/api/search"

        skip = 0
        total = None
        while True:
            payload = {
                "categories": [],
                "query": "",
                "sort": "Descending",
                "types": ["ruling"],
                "skip": skip,
                "size": PAGE_SIZE,
                "from": f"{year}-01-01",
                "to": f"{year}-12-31",
            }

            time.sleep(RATE_DELAY)
            try:
                resp = self.client.post(search_url, json_data=payload)
            except Exception as e:
                logger.warning(f"Request failed for {subdomain} year {year} skip {skip}: {e}")
                break

            if not resp or resp.status_code != 200:
                status = resp.status_code if resp else "no response"
                logger.warning(f"HTTP {status} for {subdomain} year {year} skip {skip}")
                break

            try:
                data = resp.json()
            except Exception:
                logger.warning(f"Invalid JSON from {subdomain} year {year} skip {skip}")
                break

            publications = data.get("publications", [])
            if total is None:
                total = data.get("totalCount", 0)
                if total > 0:
                    logger.info(f"  {subdomain}/{year}: {total} decisions found")

            if not publications:
                break

            for pub in publications:
                normalized = self.normalize(pub, subdomain, tribunal_name, base_url)
                if normalized and normalized.get("text"):
                    yield normalized

            skip += PAGE_SIZE
            if skip >= (total or 0):
                break

    def normalize(self, raw: dict, subdomain: str = "", tribunal_name: str = "", base_url: str = "") -> Optional[dict]:
        """Transform a raw API publication record into the standard schema."""
        pub_id = raw.get("id", "")
        if not pub_id:
            return None

        body_html = raw.get("body", "") or ""
        text = self._strip_html(body_html).strip()
        if not text:
            return None

        title = raw.get("title", "").strip()
        date_str = raw.get("date") or raw.get("published_date", "")
        if date_str:
            date_str = date_str[:10]  # YYYY-MM-DD

        case_numbers = raw.get("jnr", []) or []
        case_number = "; ".join(case_numbers) if case_numbers else None

        categories = raw.get("categories", []) or []
        authority = raw.get("authority", tribunal_name) or tribunal_name

        url = f"{base_url}/api/publication/{pub_id}" if base_url else None

        return {
            "_id": f"{subdomain}_{pub_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title or f"Decision {pub_id}",
            "text": text,
            "date": date_str or None,
            "url": url,
            "tribunal": authority,
            "tribunal_subdomain": subdomain,
            "case_number": case_number,
            "categories": categories,
            "is_board_ruling": raw.get("is_board_ruling", False),
            "is_brought_to_court": raw.get("is_brought_to_court", False),
        }

    @staticmethod
    def _strip_html(html_text: str) -> str:
        """Remove HTML tags and decode entities."""
        if not html_text:
            return ""
        text = re.sub(r"<br\s*/?>", "\n", html_text)
        text = re.sub(r"</(p|div|li|tr|h[1-6])>", "\n", text)
        text = re.sub(r"<[^>]+>", "", text)
        # Decode common HTML entities
        text = text.replace("&amp;", "&")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&quot;", '"')
        text = text.replace("&#39;", "'")
        text = text.replace("&nbsp;", " ")
        # Remove any remaining XML-like tags (e.g. <w>, <fig> from patent records)
        text = re.sub(r"<[a-zA-Z/][^>]*>", "", text)
        # Collapse whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text.strip()


def main():
    import argparse

    parser = argparse.ArgumentParser(description="DK/NaevnenesHus scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    args = parser.parse_args()

    scraper = NaevnenesHusScraper()

    if args.command == "test-api":
        logger.info("Testing API connectivity...")
        url = "https://fkn.naevneneshus.dk/api/search"
        payload = {"categories": [], "query": "", "sort": "Descending",
                    "types": ["ruling"], "skip": 0, "size": 1}
        resp = scraper.client.post(url, json_data=payload)
        if resp and resp.status_code == 200:
            data = resp.json()
            logger.info(f"API OK — {data.get('totalCount', '?')} total decisions in fkn")
            if data.get("publications"):
                pub = data["publications"][0]
                logger.info(f"  Sample: {pub.get('title', 'N/A')}")
                body = pub.get("body", "")
                logger.info(f"  Body length: {len(body)} chars")
        else:
            logger.error(f"API test failed: {resp.status_code if resp else 'no response'}")
        return

    if args.command == "bootstrap" and args.sample:
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        target = 15  # At least 10, grab 15 for safety
        # Sample from a few different tribunals
        sample_tribunals = [
            ("fkn", "Forbrugerklagenævnet"),
            ("klfu", "Klagenævnet for Udbud"),
            ("ekn", "Energiklagenævnet"),
            ("apv", "Ankenævnet for Patenter og Varemærker"),
            ("tele", "Teleklagenævnet"),
        ]
        for subdomain, tname in sample_tribunals:
            if count >= target:
                break
            base_url = f"https://{subdomain}.naevneneshus.dk"
            search_url = f"{base_url}/api/search"
            payload = {
                "categories": [], "query": "", "sort": "Descending",
                "types": ["ruling"], "skip": 0, "size": 3,
            }
            time.sleep(RATE_DELAY)
            try:
                resp = scraper.client.post(search_url, json_data=payload)
                if resp and resp.status_code == 200:
                    data = resp.json()
                    for pub in data.get("publications", []):
                        if count >= target:
                            break
                        record = scraper.normalize(pub, subdomain, tname, base_url)
                        if record and record.get("text"):
                            fname = f"{count+1:03d}_{subdomain}_{record['_id'][:40]}.json"
                            fname = re.sub(r'[^\w\-.]', '_', fname)
                            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                                json.dump(record, f, indent=2, ensure_ascii=False)
                            count += 1
                            logger.info(f"  Sample {count}: {record['title'][:60]} ({len(record['text'])} chars)")
            except Exception as e:
                logger.warning(f"Sample fetch error for {subdomain}: {e}")

        logger.info(f"Saved {count} sample records to {sample_dir}")
        return

    if args.command == "bootstrap":
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if count % 500 == 0:
                logger.info(f"  Progress: {count} records fetched")
        logger.info(f"Bootstrap complete: {count} total records")

    elif args.command == "update":
        count = 0
        for record in scraper.fetch_updates():
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
