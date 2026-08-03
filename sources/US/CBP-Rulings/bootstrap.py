#!/usr/bin/env python3
"""
US/CBP-Rulings -- Customs and Border Protection Rulings (CROSS)

Fetches CBP binding rulings, internal advice memoranda, and HQ rulings
via the CROSS JSON API at rulings.cbp.gov.

Strategy:
  1. Search API returns ruling metadata (paginated, up to 100/page)
  2. Individual ruling endpoint returns full text + metadata
  3. Normalize into standard schema

Data: Public domain (US government works). No auth required.
Rate limit: 1 req/sec (respectful crawling).

Usage:
  python bootstrap.py bootstrap            # Full pull (all rulings via search)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample rulings
  python bootstrap.py bootstrap-fast       # Alias for bootstrap
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import time
import re
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
logger = logging.getLogger("legal-data-hunter.US.CBP-Rulings")

BASE_URL = "https://rulings.cbp.gov"
SEARCH_URL = f"{BASE_URL}/api/search"
RULING_URL = f"{BASE_URL}/api/ruling"
STATS_URL = f"{BASE_URL}/api/stat/lastupdate"

# CROSS full-text search requires a non-empty term (an empty term returns 0
# hits, and single common stopwords like "a"/"the" are ignored). No single
# term covers the whole corpus: "merchandise" (~214K) beats "tariff" (~206K)
# but both fall short of the ~221K total. We therefore sweep a union of broad
# terms and dedup by ruling number so coverage approaches 100% of the corpus.
BROAD_SEARCH_TERM = "merchandise"
BROAD_SEARCH_TERMS = [
    "merchandise", "tariff", "classification", "duty", "origin",
    "country", "value", "marking", "protest", "NAFTA", "USMCA", "drawback",
]


def clean_text(raw: str) -> str:
    """Clean ruling text: strip control chars, normalize whitespace."""
    if not raw:
        return ""
    text = raw.replace("\r\r", "\n\n").replace("\r", "\n")
    text = re.sub(r"\x07", " ", text)  # bell chars used as tab in tables
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class CBPRulingsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "application/json",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get_json(self, url: str) -> Optional[dict]:
        time.sleep(self.delay)
        resp = self.http.get(url)
        if resp.headers.get("content-type", "").startswith("text/html"):
            return None
        return resp.json()

    def test_api(self):
        """Test connectivity to CROSS API."""
        logger.info("Testing CROSS API...")
        try:
            stats = self._get_json(STATS_URL)
            if not stats:
                logger.error("API test FAILED: stats endpoint returned HTML")
                return False
            total = stats.get("totalSearchableRulingsCount", 0)
            logger.info(f"  Total searchable rulings: {total}")

            data = self._get_json(f"{SEARCH_URL}?term=tariff&sortBy=date&page=1&pageSize=1")
            if not data or not data.get("rulings"):
                logger.error("API test FAILED: search returned no results")
                return False

            ruling_num = data["rulings"][0]["rulingNumber"]
            detail = self._get_json(f"{RULING_URL}/{ruling_num}")
            if not detail or not detail.get("text"):
                logger.error("API test FAILED: ruling detail has no text")
                return False

            logger.info(f"  Test ruling {ruling_num}: {len(detail['text'])} chars")
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def search_rulings(self, term: str = BROAD_SEARCH_TERM, page: int = 1,
                       page_size: int = 100, sort_by: str = "date",
                       from_date: str = None, to_date: str = None) -> Optional[dict]:
        url = f"{SEARCH_URL}?term={term}&sortBy={sort_by}&page={page}&pageSize={page_size}"
        if from_date:
            url += f"&fromDate={from_date}"
        if to_date:
            url += f"&toDate={to_date}"
        return self._get_json(url)

    def fetch_ruling_detail(self, ruling_number: str) -> Optional[dict]:
        return self._get_json(f"{RULING_URL}/{ruling_number}")

    def normalize(self, detail: dict) -> Optional[dict]:
        ruling_num = detail.get("rulingNumber", "")
        raw_date = detail.get("rulingDate", "")
        date_str = raw_date[:10] if raw_date else ""

        text = clean_text(detail.get("text", ""))
        # Skip rulings without usable full text (framework treats None as a skip).
        if not ruling_num or not text or len(text) < 50:
            return None

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {
            "_id": f"cbp-{ruling_num}",
            "_source": "US/CBP-Rulings",
            "_type": "doctrine",
            "_fetched_at": now,
            "title": detail.get("subject", "") or f"CBP Ruling {ruling_num}",
            "text": text,
            "date": date_str,
            "url": f"https://rulings.cbp.gov/ruling/{ruling_num}",
            "ruling_number": ruling_num,
            "categories": detail.get("categories", ""),
            "collection": detail.get("collection", ""),
            "tariffs": detail.get("tariffs", []),
            "related_rulings": detail.get("relatedRulings", []),
            "is_usmca": detail.get("isUsmca", False),
            "is_nafta": detail.get("isNafta", False),
            "operationally_revoked": detail.get("operationallyRevoked", False),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW ruling-detail dicts (framework normalizes — see issue #1012).

        fetch_all must yield raw API objects; BaseScraper.bootstrap_fast()/update()
        call self.normalize() on each. Previously this yielded already-normalized
        records, so the framework double-normalized them: normalize() read the raw
        key ``rulingNumber`` which the normalized record lacks, producing an empty
        ``_id`` for every record and collapsing them all to one dedup key.

        Coverage: no single search term returns the whole corpus, so we sweep a
        union of broad terms (BROAD_SEARCH_TERMS) and dedup ruling numbers with a
        ``seen`` set — each ruling detail is fetched at most once no matter how
        many term-searches surface it.
        """
        total = 0
        seen = set()
        any_search_ok = False
        for term in BROAD_SEARCH_TERMS:
            page = 1
            term_total_hits = None
            while True:
                data = self.search_rulings(term=term, page=page)
                if not data:
                    logger.warning(f"[{term}] page {page} returned HTML, stopping this term")
                    break
                any_search_ok = True
                rulings = data.get("rulings", [])
                term_total_hits = data.get("totalHits", 0)
                if not rulings:
                    break
                for r in rulings:
                    ruling_num = r.get("rulingNumber", "")
                    if not ruling_num or ruling_num in seen:
                        continue
                    seen.add(ruling_num)
                    detail = self.fetch_ruling_detail(ruling_num)
                    if not detail:
                        logger.warning(f"Skipping {ruling_num}: detail returned HTML")
                        continue
                    yield detail
                    total += 1
                    if total % 100 == 0:
                        logger.info(f"  Progress: {total} unique rulings fetched (on term '{term}')")
                page += 1
            logger.info(f"[{term}] swept {term_total_hits} hits; running unique total {total}")
        if not any_search_ok:
            # Every broad term's search came back as HTML rather than JSON — the
            # CROSS API rejected us wholesale (datacenter-IP block / WAF). Fail
            # loud instead of returning 0 rulings on exit 0, which the fleet
            # would silently accept as an empty corpus and ingest samples only.
            raise RuntimeError(
                "CROSS search API returned no JSON for any broad term "
                "(HTML block page?) — aborting so the run fails loud instead of "
                "reporting a false-empty corpus; needs a non-datacenter vantage"
            )
        logger.info(f"Total unique rulings fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch rulings from a given date onwards."""
        total = 0
        page = 1
        while True:
            data = self.search_rulings(page=page, from_date=since)
            if not data:
                break
            rulings = data.get("rulings", [])
            if not rulings:
                break
            for r in rulings:
                ruling_num = r.get("rulingNumber", "")
                if not ruling_num:
                    continue
                detail = self.fetch_ruling_detail(ruling_num)
                if not detail:
                    continue
                yield detail  # RAW — framework normalizes (see fetch_all note)
                total += 1
            page += 1
        logger.info(f"Updates fetched: {total} rulings since {since}")

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Yield normalized sample records for local CLI validation."""
        logger.info("Fetching sample CBP rulings...")
        data = self.search_rulings(page=1, page_size=40)
        if not data:
            logger.error("Search returned no data")
            return
        rulings = data.get("rulings", [])
        count = 0
        for r in rulings:
            if count >= 15:
                break
            ruling_num = r.get("rulingNumber", "")
            if not ruling_num:
                continue
            detail = self.fetch_ruling_detail(ruling_num)
            if not detail:
                logger.warning(f"Skipping {ruling_num}: no detail")
                continue
            record = self.normalize(detail)
            if not record:
                logger.warning(f"Skipping {ruling_num}: insufficient text")
                continue
            yield record
            count += 1
        logger.info(f"Sample complete: {count} rulings fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/CBP-Rulings bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CBPRulingsScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        if args.sample:
            # Local validation: normalized records written to sample/ only.
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0
            for record in scraper.fetch_sample():
                safe_id = record["_id"].replace("/", "_")
                out_path = sample_dir / f"{safe_id}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                count += 1
                logger.info(f"Saved: {record['_id']} - {record['title'][:60]} ({len(record['text'])} chars)")
            logger.info(f"Sample complete: {count} records saved to {sample_dir}")
        else:
            # Full run: stream to data/records.jsonl via the framework so the
            # ingest pipeline picks them up (previously wrote to sample/ → 0 ingested).
            stats = scraper.bootstrap_fast()
            logger.info(f"Bootstrap complete: {stats}")


if __name__ == "__main__":
    main()
