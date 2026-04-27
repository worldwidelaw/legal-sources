#!/usr/bin/env python3
"""
US/DE-Courts -- Delaware State Court Opinions

Fetches case law via CourtListener's public search API (no auth needed),
then downloads opinion PDFs and extracts full text.

Courts covered:
  - Supreme Court of Delaware (del)
  - Court of Chancery of Delaware (delch)
  - Superior Court of Delaware (delsuperct)
  - Court of Common Pleas of Delaware (delctcompl)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (paginated)
  python bootstrap.py update --since YYYY-MM-DD  # Incremental updates
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests
from html.parser import HTMLParser

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DE-Courts")

SEARCH_URL = "https://www.courtlistener.com/api/rest/v4/search/"
STORAGE_BASE = "https://storage.courtlistener.com/"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

COURTS = "del,delch,delsuperct,delctcompl"

COURT_NAMES = {
    "del": "Supreme Court of Delaware",
    "delch": "Court of Chancery of Delaware",
    "delsuperct": "Superior Court of Delaware",
    "delctcompl": "Court of Common Pleas of Delaware"
}

COURT_ABBRS = {
    "del": "DESC",
    "delch": "DECH",
    "delsuperct": "DESU",
    "delctcompl": "DECP"
}


class _HTMLTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self._pieces = []

    def handle_data(self, data):
        self._pieces.append(data)

    def get_text(self):
        return "".join(self._pieces)


def strip_html(html: str) -> str:
    extractor = _HTMLTextExtractor()
    extractor.feed(html)
    text = extractor.get_text()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class DECourtsScraper(BaseScraper):
    """Scraper for US/DE-Courts via CourtListener API."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def _search_opinions(self, court: str = COURTS, page_size: int = 20,
                         filed_after: str = None, filed_before: str = None,
                         cursor_url: str = None) -> Dict[str, Any]:
        for attempt in range(3):
            try:
                if cursor_url:
                    resp = self.session.get(cursor_url, timeout=60)
                else:
                    params = {
                        "format": "json",
                        "type": "o",
                        "court": court,
                        "page_size": min(page_size, 20),
                        "order_by": "dateFiled desc",
                    }
                    if filed_after:
                        params["filed_after"] = filed_after
                    if filed_before:
                        params["filed_before"] = filed_before
                    resp = self.session.get(SEARCH_URL, params=params, timeout=60)

                if resp.status_code == 429:
                    wait = 2 ** (attempt + 1)
                    logger.warning(f"Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.Timeout:
                if attempt < 2:
                    logger.warning("Timeout, retrying...")
                    time.sleep(2)
                    continue
                raise
            except Exception as e:
                logger.error(f"Search API error: {e}")
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise
        return {"count": 0, "results": []}

    def _download_file(self, url: str) -> Optional[bytes]:
        try:
            resp = self.session.get(url, timeout=60, allow_redirects=True)
            resp.raise_for_status()
            if len(resp.content) > 100:
                return resp.content
            return None
        except Exception as e:
            logger.warning(f"Failed to download {url}: {e}")
            return None

    def _extract_pdf_text(self, pdf_data: bytes) -> str:
        return extract_pdf_markdown(
            source="US/DE-Courts",
            source_id="",
            pdf_bytes=pdf_data,
            table="case_law",
        ) or ""

    def _get_file_url(self, opinion: Dict) -> Optional[str]:
        local_path = opinion.get("local_path")
        if local_path:
            return STORAGE_BASE + local_path
        download_url = opinion.get("download_url")
        if download_url:
            return download_url
        return None

    def _extract_text_from_url(self, url: str) -> str:
        data = self._download_file(url)
        if not data:
            return ""
        if url.endswith(".html") or url.endswith(".htm"):
            return strip_html(data.decode("utf-8", errors="replace"))
        header = data[:100].lower()
        if b"<!doctype html" in header or b"<html" in header:
            return strip_html(data.decode("utf-8", errors="replace"))
        return self._extract_pdf_text(data)

    def _process_search_result(self, result: Dict) -> Optional[Dict[str, Any]]:
        opinions = result.get("opinions", [])
        if not opinions:
            return None
        opinion = opinions[0]
        file_url = self._get_file_url(opinion)
        if not file_url:
            return None
        text = self._extract_text_from_url(file_url)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for {result.get('caseName', 'unknown')}: {len(text)} chars")
            return None
        citations = result.get("citation", [])
        citation_str = citations[0] if citations else ""
        return {
            "cluster_id": result.get("cluster_id"),
            "case_name": result.get("caseName", ""),
            "case_name_full": result.get("caseNameFull", ""),
            "docket_number": result.get("docketNumber", ""),
            "court_id": result.get("court_id", ""),
            "court": result.get("court", ""),
            "date_filed": result.get("dateFiled"),
            "citation": citation_str,
            "status": result.get("status", ""),
            "file_url": file_url,
            "cl_url": f"https://www.courtlistener.com{result.get('absolute_url', '')}",
            "text": text,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        cursor_url = None
        total_fetched = 0
        page = 0
        while True:
            page += 1
            logger.info(f"Fetching search results page {page}...")
            data = self._search_opinions(cursor_url=cursor_url)
            results = data.get("results", [])
            if not results:
                break
            for result in results:
                time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                raw = self._process_search_result(result)
                if raw:
                    total_fetched += 1
                    yield raw
            cursor_url = data.get("next")
            if not cursor_url:
                break
        logger.info(f"Total fetched: {total_fetched}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        if not since:
            from datetime import timedelta
            since = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        cursor_url = None
        page = 0
        while True:
            page += 1
            data = self._search_opinions(filed_after=since, cursor_url=cursor_url)
            results = data.get("results", [])
            if not results:
                break
            for result in results:
                time.sleep(self.config.get("fetch", {}).get("delay", 1.5))
                raw = self._process_search_result(result)
                if raw:
                    yield raw
            cursor_url = data.get("next")
            if not cursor_url:
                break

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        court_id = raw.get("court_id", "")
        cluster_id = raw.get("cluster_id", "")
        court_abbr = COURT_ABBRS.get(court_id, "DECT")
        doc_id = f"US-DE-{court_abbr}-{cluster_id}"
        court_name = COURT_NAMES.get(court_id, raw.get("court", "Delaware Court"))
        return {
            "_id": doc_id,
            "_source": "US/DE-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date_filed"),
            "url": raw.get("cl_url", ""),
            "case_number": raw.get("docket_number", ""),
            "court": court_name,
            "citation": raw.get("citation", ""),
            "status": raw.get("status", ""),
            "jurisdiction": "US-DE",
            "file_url": raw.get("file_url", ""),
        }

    def test_connection(self) -> bool:
        try:
            data = self._search_opinions(page_size=5)
            count = data.get("count", 0)
            results = data.get("results", [])
            logger.info(f"Connection test: {count} total opinions, got {len(results)} results")
            return len(results) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/DE-Courts data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--since", help="ISO date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = DECourtsScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999
        for raw in scraper.fetch_all():
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            text_len = len(record.get("text", ""))
            logger.info(f"[{count + 1}] {record['_id']}: {record['title'][:60]} ({text_len} chars)")
            count += 1
            if count >= target:
                break
        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates(since=args.since):
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
