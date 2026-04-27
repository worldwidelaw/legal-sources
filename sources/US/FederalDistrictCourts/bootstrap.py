#!/usr/bin/env python3
"""
US/FederalDistrictCourts -- Federal District Court Opinions

Fetches case law via CourtListener's public search API (no auth needed),
then downloads opinion PDFs and extracts full text.

Covers all 94 federal judicial districts plus historical/territorial courts
(~462K opinions total).

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
from html.parser import HTMLParser

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.FederalDistrictCourts")

SEARCH_URL = "https://www.courtlistener.com/api/rest/v4/search/"
STORAGE_BASE = "https://storage.courtlistener.com/"
USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# All federal district courts
DISTRICT_COURTS = (
    "akd,ald,almd,alnd,alsd,ard,ared,arwd,azd,cacd,caed,californiad,"
    "canalzoned,cand,casd,cod,ctd,dcd,ded,fld,flmd,flnd,flsd,gad,gamd,"
    "gand,gasd,gud,hid,iad,iand,iasd,idd,ilcd,illinoisd,illinoised,ilnd,"
    "ilsd,indianad,innd,insd,ksd,kyd,kyed,kywd,lad,laed,lamd,lawd,mad,"
    "mdd,med,michd,mied,missd,miwd,mnd,mod,moed,mowd,msnd,mssd,mtd,ncd,"
    "nced,ncmd,ncwd,ndd,ned,nhd,njd,nmd,nmid,nvd,nyd,nyed,nynd,nysd,nywd,"
    "ohiod,ohnd,ohsd,okd,oked,oknd,okwd,ord,orld,paed,pamd,pawd,"
    "pennsylvaniad,prd,rid,scd,sdd,southcarolinaed,southcarolinawd,"
    "superctsdfla,tennessed,texd,tned,tnmd,tnwd,txed,txnd,txsd,txwd,"
    "usdistct,utd,vad,vaed,vawd,vid,vtd,waed,washd,wawd,wied,wisd,wiwd,"
    "wvad,wvnd,wvsd,wyd"
)

# Map court IDs to readable names (major districts)
COURT_NAMES = {
    "dcd": "U.S. District Court for the District of Columbia",
    "nysd": "U.S. District Court for the Southern District of New York",
    "nynd": "U.S. District Court for the Northern District of New York",
    "nyed": "U.S. District Court for the Eastern District of New York",
    "nywd": "U.S. District Court for the Western District of New York",
    "cacd": "U.S. District Court for the Central District of California",
    "cand": "U.S. District Court for the Northern District of California",
    "caed": "U.S. District Court for the Eastern District of California",
    "casd": "U.S. District Court for the Southern District of California",
    "ilnd": "U.S. District Court for the Northern District of Illinois",
    "ilsd": "U.S. District Court for the Southern District of Illinois",
    "txnd": "U.S. District Court for the Northern District of Texas",
    "txsd": "U.S. District Court for the Southern District of Texas",
    "txed": "U.S. District Court for the Eastern District of Texas",
    "txwd": "U.S. District Court for the Western District of Texas",
    "mad": "U.S. District Court for the District of Massachusetts",
    "paed": "U.S. District Court for the Eastern District of Pennsylvania",
    "pamd": "U.S. District Court for the Middle District of Pennsylvania",
    "pawd": "U.S. District Court for the Western District of Pennsylvania",
    "flsd": "U.S. District Court for the Southern District of Florida",
    "flmd": "U.S. District Court for the Middle District of Florida",
    "flnd": "U.S. District Court for the Northern District of Florida",
    "njd": "U.S. District Court for the District of New Jersey",
    "mdd": "U.S. District Court for the District of Maryland",
    "vaed": "U.S. District Court for the Eastern District of Virginia",
    "vawd": "U.S. District Court for the Western District of Virginia",
    "ohnd": "U.S. District Court for the Northern District of Ohio",
    "ohsd": "U.S. District Court for the Southern District of Ohio",
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


class FederalDistrictCourtsScraper(BaseScraper):
    """Scraper for US/FederalDistrictCourts via CourtListener search API (no auth)."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def _search_opinions(self, court: str = DISTRICT_COURTS, page_size: int = 20,
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
            source="US/FederalDistrictCourts",
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
        doc_id = f"US-FED-DIST-{court_id.upper()}-{cluster_id}"
        court_name = COURT_NAMES.get(court_id, raw.get("court", "U.S. District Court"))
        return {
            "_id": doc_id,
            "_source": "US/FederalDistrictCourts",
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
            "jurisdiction": "US",
            "file_url": raw.get("file_url", ""),
        }

    def test_connection(self) -> bool:
        try:
            data = self._search_opinions(page_size=5)
            count = data.get("count", 0)
            results = data.get("results", [])
            logger.info(f"Connection test: {count:,} total opinions, got {len(results)} results")
            if results:
                first = results[0]
                logger.info(f"  First: {first.get('caseName', '')[:60]}, date={first.get('dateFiled')}")
            return count > 0 and len(results) > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/FederalDistrictCourts case law fetcher")
    subparsers = parser.add_subparsers(dest="command")

    boot_parser = subparsers.add_parser("bootstrap", help="Bootstrap data")
    boot_parser.add_argument("--sample", action="store_true", help="Sample mode")
    boot_parser.add_argument("--full", action="store_true", help="Full bootstrap")
    boot_parser.add_argument("--count", type=int, default=15, help="Sample count")

    upd_parser = subparsers.add_parser("update", help="Incremental update")
    upd_parser.add_argument("--since", required=True, help="YYYY-MM-DD")

    subparsers.add_parser("test", help="Test connectivity")

    args = parser.parse_args()

    scraper = FederalDistrictCourtsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)

    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
        elif args.full:
            stats = scraper.bootstrap(sample_mode=False)
        else:
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.count)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")

    elif args.command == "update":
        count = 0
        data_dir = scraper.source_dir / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        with open(data_dir / "updates.jsonl", "w") as f:
            for raw in scraper.fetch_updates(since=args.since):
                record = scraper.normalize(raw)
                f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
                count += 1
        logger.info(f"Fetched {count} updates since {args.since}")

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
