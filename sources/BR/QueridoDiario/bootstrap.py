#!/usr/bin/env python3
"""
BR/QueridoDiario -- Querido Diário (Brazilian Municipal Gazettes) Fetcher

Fetches municipal official gazettes from the Querido Diário REST API.
Full text is available via TXT download URLs for each gazette.

Strategy:
  - Search API for recent gazettes by date range
  - Download full text from txt_url for each gazette
  - Normalize into standard schema

Usage:
  python bootstrap.py bootstrap          # Full initial pull (recent dates)
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BR.QueridoDiario")

API_BASE = "https://api.queridodiario.ok.org.br"
DATA_BASE = "https://data.queridodiario.ok.org.br"


class QueridoDiarioScraper(BaseScraper):
    """Scraper for BR/QueridoDiario -- Querido Diário municipal gazettes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        try:
            from common.http_client import HttpClient
            self.client = HttpClient(timeout=60)
        except ImportError:
            self.client = None

    def _api_get(self, endpoint: str, params: dict = None) -> Optional[dict]:
        """Make API request and return JSON."""
        url = f"{API_BASE}{endpoint}"
        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.get(url, params=params)
                    if resp.status_code == 200:
                        return resp.json(strict=False)
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                else:
                    import urllib.request
                    import urllib.parse
                    if params:
                        qs = urllib.parse.urlencode(params, doseq=True)
                        url = f"{url}?{qs}"
                    req = urllib.request.Request(url)
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        raw = resp.read().decode("utf-8", errors="replace")
                        return json.loads(raw, strict=False)
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _fetch_full_text(self, txt_url: str) -> Optional[str]:
        """Download full text of a gazette from its txt_url."""
        for attempt in range(3):
            try:
                if self.client:
                    resp = self.client.get(txt_url)
                    if resp.status_code == 200:
                        return resp.text
                    logger.warning(f"HTTP {resp.status_code} for txt_url")
                else:
                    import urllib.request
                    req = urllib.request.Request(txt_url)
                    with urllib.request.urlopen(req, timeout=60) as resp:
                        return resp.read().decode("utf-8", errors="replace")
            except Exception as e:
                logger.warning(f"Attempt {attempt+1} txt fetch failed: {e}")
                time.sleep(2 * (attempt + 1))
        return None

    def _gazette_id(self, gazette: dict) -> str:
        """Build unique ID from gazette data."""
        territory = gazette.get("territory_id", "unknown")
        date = gazette.get("date", "unknown")
        # Extract hash from URL
        url = gazette.get("url", "")
        url_hash = url.rsplit("/", 1)[-1].replace(".pdf", "") if "/" in url else ""
        return f"BR-QD-{territory}-{date}-{url_hash[:12]}"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        territory_name = raw.get("territory_name", "")
        state_code = raw.get("state_code", "")
        date = raw.get("date", "")
        edition = raw.get("edition", "")
        text = raw.get("_full_text", "")

        title = f"Diário Oficial de {territory_name}"
        if state_code:
            title += f" ({state_code})"
        if edition:
            title += f" - Edição {edition}"
        if date:
            title += f" - {date}"

        return {
            "_id": self._gazette_id(raw),
            "_source": "BR/QueridoDiario",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw.get("url", ""),
            "txt_url": raw.get("txt_url", ""),
            "territory_id": raw.get("territory_id", ""),
            "territory_name": territory_name,
            "state_code": state_code,
            "edition": edition,
            "is_extra_edition": raw.get("is_extra_edition", False),
        }

    def _fetch_gazettes_page(self, published_since: str, published_until: str,
                              offset: int = 0, size: int = 10) -> Optional[dict]:
        """Fetch a page of gazette results."""
        params = {
            "published_since": published_since,
            "published_until": published_until,
            "size": size,
            "offset": offset,
            "sort_by": "descending_date",
        }
        return self._api_get("/gazettes", params)

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch gazettes by iterating over recent dates."""
        today = datetime.now()
        count = 0
        page_size = 10

        # Iterate in weekly chunks going backwards
        for week_back in range(0, 52):
            end_date = today - timedelta(days=7 * week_back)
            start_date = end_date - timedelta(days=6)

            since = start_date.strftime("%Y-%m-%d")
            until = end_date.strftime("%Y-%m-%d")

            offset = 0
            while True:
                data = self._fetch_gazettes_page(since, until, offset, page_size)
                if not data or not data.get("gazettes"):
                    break

                for gazette in data["gazettes"]:
                    txt_url = gazette.get("txt_url")
                    if not txt_url:
                        continue

                    time.sleep(1)
                    full_text = self._fetch_full_text(txt_url)
                    if full_text and len(full_text) > 100:
                        gazette["_full_text"] = full_text
                        normalized = self.normalize(gazette)
                        count += 1
                        yield normalized

                total = data.get("total_gazettes", 0)
                offset += page_size
                if offset >= total or offset >= 100:
                    break

                time.sleep(0.5)

            time.sleep(0.5)

        logger.info(f"Completed: {count} gazettes")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch gazettes from recent days."""
        today = datetime.now()
        days = 7
        if since:
            try:
                since_date = datetime.strptime(since[:10], "%Y-%m-%d")
                days = min((today - since_date).days + 1, 30)
            except ValueError:
                pass

        since_str = (today - timedelta(days=days)).strftime("%Y-%m-%d")
        until_str = today.strftime("%Y-%m-%d")

        count = 0
        offset = 0
        while True:
            data = self._fetch_gazettes_page(since_str, until_str, offset, 10)
            if not data or not data.get("gazettes"):
                break
            for gazette in data["gazettes"]:
                txt_url = gazette.get("txt_url")
                if not txt_url:
                    continue
                time.sleep(1)
                full_text = self._fetch_full_text(txt_url)
                if full_text and len(full_text) > 100:
                    gazette["_full_text"] = full_text
                    normalized = self.normalize(gazette)
                    count += 1
                    yield normalized
            offset += 10
            if offset >= data.get("total_gazettes", 0) or offset >= 100:
                break
            time.sleep(0.5)

        logger.info(f"Updates: {count} gazettes")

    def test(self) -> bool:
        """Quick connectivity test."""
        today = datetime.now().strftime("%Y-%m-%d")
        week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        data = self._api_get("/gazettes", {
            "published_since": week_ago,
            "published_until": today,
            "size": 1,
            "sort_by": "descending_date",
        })
        if data and data.get("total_gazettes", 0) > 0:
            gazette = data["gazettes"][0]
            logger.info(
                f"Test passed: {data['total_gazettes']} total gazettes. "
                f"Latest: {gazette.get('territory_name')} on {gazette.get('date')}"
            )
            txt_url = gazette.get("txt_url")
            if txt_url:
                text = self._fetch_full_text(txt_url)
                if text:
                    logger.info(f"Full text available: {len(text)} chars")
                    return True
            return True
        logger.error("Test failed: no gazettes returned")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="BR/QueridoDiario data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = QueridoDiarioScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")
if __name__ == "__main__":
    main()
