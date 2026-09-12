#!/usr/bin/env python3
"""
US/TTAB -- US Trademark Trial and Appeal Board Decisions

Fetches TTAB decisions with full text via CourtListener's free search
API and PDF storage.

Strategy:
  1. Search CourtListener API for TTAB decisions (no auth needed)
  2. Download PDFs from CourtListener storage (no auth needed)
  3. Extract full text from PDFs using common pdf_extract
  4. Normalize into standard schema

Data: Public domain (US government works). No auth required.
Rate limit: 1 req / 2 sec.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample decisions
  python bootstrap.py bootstrap-fast       # Full pull, concurrent normalize
  python bootstrap.py update               # Incremental pull
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import logging
import time
from pathlib import Path
from datetime import datetime, date, timezone, timedelta
from typing import Generator, Optional, Union

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TTAB")

SEARCH_URL = "https://www.courtlistener.com/api/rest/v4/search/"
STORAGE_URL = "https://storage.courtlistener.com"

# CourtListener held ~1,245 TTAB opinions as of 2026-08. An empty first page
# means the search endpoint changed or is refusing us, not that the board
# stopped issuing decisions — so it must fail loud rather than exit 0.
KNOWN_CORPUS_FLOOR = 100


class TTABUnavailable(RuntimeError):
    """The CourtListener search endpoint did not return a usable result set."""


def extract_pdf_text(pdf_bytes: bytes, source_id: str = "") -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/TTAB",
        source_id=source_id,
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""


class TTABScraper(BaseScraper):

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
            timeout=120,
        )
        self.delay = 2.0

    def _get_json(self, url: str) -> dict:
        time.sleep(self.delay)
        resp = self.http.get(url)
        if resp.status_code != 200:
            # HttpClient.get() does not raise_for_status, so an error body would
            # otherwise be read as {"detail": ...} with no "results" key and be
            # mistaken for "no more pages".
            raise TTABUnavailable(
                f"CourtListener returned HTTP {resp.status_code} for {url}: "
                f"{resp.text[:300]}"
            )
        return resp.json()

    def _get_bytes(self, url: str) -> bytes:
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.content

    def test_api(self):
        """Test connectivity to CourtListener search API."""
        logger.info("Testing CourtListener search API for TTAB...")
        try:
            data = self._get_json(
                f"{SEARCH_URL}?type=o&court=ttab&order_by=dateFiled+desc"
            )
            count = data.get("count", 0)
            results = data.get("results", [])
            if count > 0 and results:
                case = results[0]
                logger.info(f"  TTAB decisions: {count}")
                logger.info(f"  Latest: {case['caseName']} ({case['dateFiled']})")

                if case.get("opinions") and case["opinions"][0].get("local_path"):
                    pdf_url = f"{STORAGE_URL}/{case['opinions'][0]['local_path']}"
                    pdf_bytes = self._get_bytes(pdf_url)
                    text = extract_pdf_text(pdf_bytes)
                    logger.info(f"  PDF: {len(pdf_bytes)} bytes, text: {len(text)} chars")
                    if len(text) > 100:
                        logger.info("API test PASSED")
                        return True
                    else:
                        logger.error("API test FAILED: text extraction too short")
                        return False
                else:
                    logger.info("API test PASSED (search OK, no PDF to test)")
                    return True
            else:
                logger.error("API test FAILED: no results")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def search_decisions(self, cursor: str = None,
                         filed_after: str = None) -> dict:
        """One page of the TTAB opinion search. `cursor` is a full `next` URL."""
        if cursor:
            return self._get_json(cursor)
        url = f"{SEARCH_URL}?type=o&court=ttab&order_by=dateFiled+desc"
        if filed_after:
            url += f"&filed_after={filed_after}"
        return self._get_json(url)

    def fetch_opinion_text(self, result: dict) -> Optional[str]:
        opinions = result.get("opinions", [])
        if not opinions:
            return None

        source_id = str(result.get("cluster_id", ""))
        for opinion in opinions:
            local_path = opinion.get("local_path")
            if not local_path:
                continue
            pdf_url = f"{STORAGE_URL}/{local_path}"
            try:
                pdf_bytes = self._get_bytes(pdf_url)
                if len(pdf_bytes) < 500:
                    continue
                text = extract_pdf_text(pdf_bytes, source_id)
                if text and len(text) > 50:
                    return text
            except Exception as e:
                logger.warning(f"Failed to download/extract PDF {pdf_url}: {e}")
                continue

        for opinion in opinions:
            snippet = opinion.get("snippet", "")
            if snippet and len(snippet) > 50:
                logger.warning(f"Using snippet fallback for {result.get('caseName')}")
                return snippet

        return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Turn one raw search hit into a record, downloading its opinion PDF.

        Takes the raw hit only — the previous two-argument
        `normalize(result, text)` did not match the BaseScraper contract, so
        `bootstrap()`/`bootstrap_fast()` could not drive this scraper at all
        and every run fell back to writing sample/ by hand.
        """
        text = self.fetch_opinion_text(raw)
        if not text or len(text) <= 50:
            logger.warning(f"Skipping {raw.get('caseName')}: no text")
            return None

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cluster_id = raw.get("cluster_id", 0)

        return {
            "_id": f"ttab-{cluster_id}",
            "_source": "US/TTAB",
            "_type": "case_law",
            "_fetched_at": now,
            "title": raw.get("caseName", "Unknown"),
            "text": text,
            "date": raw.get("dateFiled", ""),
            "url": f"https://www.courtlistener.com{raw.get('absolute_url', '')}",
            "cluster_id": cluster_id,
            "docket_number": raw.get("docketNumber", ""),
            "court": raw.get("court", ""),
            "court_id": raw.get("court_id", ""),
            "status": raw.get("status", ""),
            "judge": raw.get("judge", ""),
            "syllabus": raw.get("syllabus", ""),
            "citation": raw.get("court_citation_string", ""),
        }

    def _iter_search(self, filed_after: str = None) -> Generator[dict, None, None]:
        """Walk every search page, yielding RAW hits for normalize()."""
        total = 0
        pages = 0
        cursor = None
        while True:
            data = self.search_decisions(cursor=cursor, filed_after=filed_after)
            results = data.get("results", [])
            if pages == 0:
                reported = data.get("count", 0)
                logger.info(
                    "TTAB search: %s decisions reported%s",
                    reported,
                    f" since {filed_after}" if filed_after else "",
                )
                if not filed_after and reported < KNOWN_CORPUS_FLOOR:
                    raise TTABUnavailable(
                        f"Full TTAB search reported only {reported} decisions "
                        f"(expected >{KNOWN_CORPUS_FLOOR}) — the search endpoint "
                        "or the ttab court id has changed."
                    )
            pages += 1
            if not results:
                break
            for result in results:
                yield result
                total += 1
                if total % 50 == 0:
                    logger.info(f"  Progress: {total} hits enumerated")
            cursor = data.get("next")
            if not cursor:
                break
        logger.info(f"Enumerated {total} TTAB hits across {pages} page(s)")

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_search()

    def fetch_updates(self, since: Union[str, datetime, date]) -> Generator[dict, None, None]:
        # CourtListener's `filed_after` only accepts YYYY-MM-DD; a bare
        # datetime is rejected with HTTP 400 (#1441).
        since_str = as_date_str(since)
        if not since_str:
            # No usable date — fall back to a recent window rather than
            # silently sending `filed_after=` and re-walking the whole corpus.
            since_str = (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
            logger.warning("Unparseable `since` (%r) — defaulting to %s", since, since_str)
        yield from self._iter_search(filed_after=since_str)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/TTAB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = TTABScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "update":
        stats = scraper.update()
    elif args.command == "bootstrap-fast" and not args.sample:
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)

    fetched = stats.get("records_fetched", 0)
    logger.info(
        "%s complete: %s fetched, %s written, %s errors",
        args.command, fetched,
        stats.get("records_new", 0) + stats.get("records_updated", 0),
        stats.get("errors", 0),
    )
    if args.command != "update" and fetched == 0:
        logger.error("No records fetched — exiting non-zero so the run is not "
                     "recorded as a success with only sample/ ingested.")
        sys.exit(1)


if __name__ == "__main__":
    main()
