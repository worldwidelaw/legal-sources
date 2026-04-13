#!/usr/bin/env python3
"""
US/WI-Courts -- Wisconsin Supreme Court & Court of Appeals Opinions

Fetches Wisconsin appellate opinions with full text via CourtListener's free
search API and PDF storage.

Strategy:
  1. Search CourtListener API for Wisconsin opinions (no auth needed)
  2. Download PDFs from CourtListener storage (no auth needed)
  3. Extract full text from PDFs using common pdf_extract
  4. Normalize into standard schema

Data: Public domain (US government works). No auth required.
Rate limit: 1 req / 2 sec.

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample opinions
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.WI-Courts")

SEARCH_URL = "https://www.courtlistener.com/api/rest/v4/search/"
STORAGE_URL = "https://storage.courtlistener.com"

WI_COURTS = {
    "wis": "Wisconsin Supreme Court",
    "wisctapp": "Wisconsin Court of Appeals",
}


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/WI-Courts",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""


class WICourtsScraper(BaseScraper):

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
        return resp.json()

    def _get_bytes(self, url: str) -> bytes:
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.content

    def test_api(self):
        """Test connectivity to CourtListener search API."""
        logger.info("Testing CourtListener search API for WI courts...")
        try:
            data = self._get_json(
                f"{SEARCH_URL}?type=o&court=wis&order_by=dateFiled+desc&page_size=1"
            )
            count = data.get("count", 0)
            results = data.get("results", [])
            if count > 0 and results:
                case = results[0]
                logger.info(f"  Wisconsin Supreme Court opinions: {count}")
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

    def search_opinions(self, court_ids: str = "wis,wisctapp",
                        page_size: int = 20, cursor: str = None,
                        filed_after: str = None) -> dict:
        url = f"{SEARCH_URL}?type=o&court={court_ids}&order_by=dateFiled+desc&page_size={page_size}"
        if cursor:
            url = cursor
        if filed_after and not cursor:
            url += f"&filed_after={filed_after}"
        return self._get_json(url)

    def fetch_opinion_text(self, result: dict) -> Optional[str]:
        opinions = result.get("opinions", [])
        if not opinions:
            return None

        for opinion in opinions:
            local_path = opinion.get("local_path")
            if not local_path:
                continue
            pdf_url = f"{STORAGE_URL}/{local_path}"
            try:
                pdf_bytes = self._get_bytes(pdf_url)
                if len(pdf_bytes) < 500:
                    continue
                text = extract_pdf_text(pdf_bytes)
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

    def normalize(self, result: dict, text: str) -> dict:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        cluster_id = result.get("cluster_id", 0)

        return {
            "_id": f"wi-{cluster_id}",
            "_source": "US/WI-Courts",
            "_type": "case_law",
            "_fetched_at": now,
            "title": result.get("caseName", "Unknown"),
            "text": text,
            "date": result.get("dateFiled", ""),
            "url": f"https://www.courtlistener.com{result.get('absolute_url', '')}",
            "cluster_id": cluster_id,
            "docket_number": result.get("docketNumber", ""),
            "court": result.get("court", ""),
            "court_id": result.get("court_id", ""),
            "status": result.get("status", ""),
            "judge": result.get("judge", ""),
            "syllabus": result.get("syllabus", ""),
            "citation": result.get("court_citation_string", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        total = 0
        cursor = None
        while True:
            data = self.search_opinions(cursor=cursor)
            results = data.get("results", [])
            if not results:
                break
            for result in results:
                text = self.fetch_opinion_text(result)
                if text and len(text) > 50:
                    yield self.normalize(result, text)
                    total += 1
                    if total % 50 == 0:
                        logger.info(f"  Progress: {total} opinions fetched")
                else:
                    logger.warning(f"Skipping {result.get('caseName')}: no text")
            cursor = data.get("next")
            if not cursor:
                break
        logger.info(f"Total opinions fetched: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        total = 0
        cursor = None
        while True:
            if cursor:
                data = self.search_opinions(cursor=cursor)
            else:
                data = self.search_opinions(filed_after=since)
            results = data.get("results", [])
            if not results:
                break
            for result in results:
                text = self.fetch_opinion_text(result)
                if text and len(text) > 50:
                    yield self.normalize(result, text)
                    total += 1
            cursor = data.get("next")
            if not cursor:
                break
        logger.info(f"Updates fetched: {total} opinions since {since}")

    def fetch_sample(self) -> Generator[dict, None, None]:
        logger.info("Fetching sample Wisconsin court opinions...")
        count = 0
        for court_id, court_name in WI_COURTS.items():
            logger.info(f"  Sampling from {court_name}...")
            data = self.search_opinions(court_ids=court_id, page_size=10)
            results = data.get("results", [])
            for result in results:
                if count >= 15:
                    break
                text = self.fetch_opinion_text(result)
                if text and len(text) > 50:
                    yield self.normalize(result, text)
                    count += 1
                else:
                    logger.warning(f"Skipping {result.get('caseName')}: no text")
        logger.info(f"Sample complete: {count} opinions fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/WI-Courts bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    scraper = WICourtsScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if args.sample:
            gen = scraper.fetch_sample()
        else:
            gen = scraper.fetch_all()

        count = 0
        for record in gen:
            safe_id = record["_id"].replace("/", "_")
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} - {record['title']} ({len(record['text'])} chars)")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
