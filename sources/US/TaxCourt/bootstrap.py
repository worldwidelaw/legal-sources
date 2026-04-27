#!/usr/bin/env python3
"""
US/TaxCourt -- United States Tax Court Published Opinions

Fetches T.C. Opinions, Memorandum Opinions, and Summary Opinions from the
DAWSON public API. Full-text extracted from PDFs via common/pdf_extract.

Coverage: May 1, 1986 to present.

API endpoints:
  - Search: public-api-{green|blue}.dawson.ustaxcourt.gov/public-api/opinion-search
  - PDF:    {base}/{docketNumber}/{docketEntryId}/public-document-download-url

Rate limit: 15 requests per 60 seconds.

Usage:
  python bootstrap.py bootstrap          # Full initial pull (1986-present)
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py update             # Fetch recent 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List, Tuple
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TaxCourt")

USER_AGENT = "LegalDataHunter/1.0 (open-data research; https://github.com/worldwidelaw/legal-sources)"

API_HOSTS = [
    "https://public-api-green.dawson.ustaxcourt.gov",
    "https://public-api-blue.dawson.ustaxcourt.gov",
]
OPINION_TYPES = "MOP,SOP,TCOP"
# 15 requests per 60 seconds → ~4s between requests to stay safe
REQUEST_DELAY = 4.5
# Backscrape in 30-day intervals
INTERVAL_DAYS = 30
# Start date for backscraping
EARLIEST_DATE = datetime(1986, 5, 1)


def _api_get(path: str, timeout: int = 30) -> Optional[dict]:
    """Try green then blue DAWSON API host. Returns parsed JSON or None."""
    for host in API_HOSTS:
        url = f"{host}{path}"
        req = Request(url, headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })
        try:
            resp = urlopen(req, timeout=timeout)
            return json.loads(resp.read())
        except (HTTPError, URLError, json.JSONDecodeError) as e:
            logger.debug(f"API call failed ({host}): {e}")
            continue
    return None


def _get_pdf_url(docket_number: str, docket_entry_id: str) -> Optional[str]:
    """Get temporary signed S3 URL for a document PDF."""
    path = f"/public-api/{docket_number}/{docket_entry_id}/public-document-download-url"
    data = _api_get(path)
    if data and "url" in data:
        return data["url"]
    return None


def _download_pdf(url: str, timeout: int = 60) -> Optional[bytes]:
    """Download PDF bytes from a signed S3 URL."""
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        resp = urlopen(req, timeout=timeout)
        data = resp.read()
        if data and b"%PDF" in data[:20]:
            return data
    except (HTTPError, URLError) as e:
        logger.debug(f"PDF download failed: {e}")
    return None


def _search_opinions(start_date: str, end_date: str) -> List[dict]:
    """Search for opinions in a date range. Dates in MM/DD/YYYY format."""
    path = (
        f"/public-api/opinion-search"
        f"?dateRange=customDates"
        f"&startDate={start_date}"
        f"&endDate={end_date}"
        f"&opinionTypes={OPINION_TYPES}"
    )
    data = _api_get(path)
    if data and "results" in data:
        return [r for r in data["results"] if not r.get("isStricken", False)]
    return []


def _date_ranges(start: datetime, end: datetime, interval_days: int = INTERVAL_DAYS) -> List[Tuple[datetime, datetime]]:
    """Generate (start, end) date pairs covering the full range."""
    ranges = []
    current = start
    while current < end:
        range_end = min(current + timedelta(days=interval_days), end)
        ranges.append((current, range_end))
        current = range_end + timedelta(days=1)
    return ranges


def _parse_opinion_ref(title: str) -> Optional[str]:
    """Extract opinion reference like 'T.C. Memo. 2026-33' from documentTitle."""
    import re
    # T.C. Memo. YYYY-NN
    m = re.search(r'T\.C\.\s*Memo\.\s*(\d{4}-\d+)', title)
    if m:
        return f"T.C. Memo. {m.group(1)}"
    # T.C. Summary Opinion YYYY-NN
    m = re.search(r'T\.C\.\s*Summary\s*Opinion\s*(\d{4}-\d+)', title)
    if m:
        return f"T.C. Summary Opinion {m.group(1)}"
    # NNN T.C. No. NN
    m = re.search(r'(\d+)\s*T\.C\.\s*No\.\s*(\d+)', title)
    if m:
        return f"{m.group(1)} T.C. No. {m.group(2)}"
    return None


class USTaxCourtScraper(BaseScraper):
    """
    Scraper for US/TaxCourt.
    Country: US
    URL: https://www.ustaxcourt.gov

    Data types: case_law
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _fetch_opinion(self, result: dict) -> Optional[dict]:
        """Fetch full text for a single opinion search result."""
        docket_number = result["docketNumber"]
        entry_id = result["docketEntryId"]

        time.sleep(REQUEST_DELAY)
        pdf_url = _get_pdf_url(docket_number, entry_id)
        if not pdf_url:
            logger.warning(f"No PDF URL for docket {docket_number}")
            return None

        time.sleep(REQUEST_DELAY)
        pdf_bytes = _download_pdf(pdf_url)
        if not pdf_bytes:
            logger.warning(f"PDF download failed for docket {docket_number}")
            return None

        source_id = f"{docket_number}_{entry_id[:8]}"
        text = extract_pdf_markdown(
            source="US/TaxCourt",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        ) or ""

        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for docket {docket_number}: {len(text)} chars")
            return None

        return {
            "docket_number": docket_number,
            "docket_number_suffix": result.get("docketNumberWithSuffix", docket_number),
            "docket_entry_id": entry_id,
            "case_caption": result.get("caseCaption", ""),
            "document_title": result.get("documentTitle", ""),
            "document_type": result.get("documentType", ""),
            "event_code": result.get("eventCode", ""),
            "filing_date": result.get("filingDate", ""),
            "judge": result.get("judge", ""),
            "num_pages": result.get("numberOfPages"),
            "opinion_ref": _parse_opinion_ref(result.get("documentTitle", "")),
            "text": text,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all Tax Court opinions with full text (1986-present)."""
        now = datetime.now()
        ranges = _date_ranges(EARLIEST_DATE, now)
        # Process newest first
        ranges.reverse()

        for start, end in ranges:
            start_str = start.strftime("%m/%d/%Y")
            end_str = end.strftime("%m/%d/%Y")
            logger.info(f"Searching {start_str} - {end_str}...")

            time.sleep(REQUEST_DELAY)
            results = _search_opinions(start_str, end_str)
            if not results:
                continue

            logger.info(f"  Found {len(results)} opinions")
            for result in results:
                doc = self._fetch_opinion(result)
                if doc:
                    yield doc

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch opinions from the last 90 days."""
        end = datetime.now()
        start = end - timedelta(days=90)
        ranges = _date_ranges(start, end)
        ranges.reverse()

        for s, e in ranges:
            start_str = s.strftime("%m/%d/%Y")
            end_str = e.strftime("%m/%d/%Y")
            logger.info(f"Searching updates {start_str} - {end_str}...")

            time.sleep(REQUEST_DELAY)
            results = _search_opinions(start_str, end_str)
            for result in results:
                doc = self._fetch_opinion(result)
                if doc:
                    yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw opinion into standard schema."""
        # Build title from opinion reference and case caption
        ref = raw.get("opinion_ref") or raw.get("document_type", "Opinion")
        caption = raw.get("case_caption", "")
        title = f"{ref}: {caption}" if caption else ref

        # Parse filing date
        date = None
        filing_date = raw.get("filing_date", "")
        if filing_date:
            try:
                dt = datetime.fromisoformat(filing_date.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                date = None

        docket = raw.get("docket_number", "unknown")
        entry_short = raw.get("docket_entry_id", "")[:8]

        return {
            "_id": f"{docket}_{entry_short}",
            "_source": "US/TaxCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date,
            "url": f"https://www.ustaxcourt.gov/find-a-case?docketNumber={docket}",
            "docket_number": docket,
            "docket_number_suffix": raw.get("docket_number_suffix"),
            "document_type": raw.get("document_type"),
            "event_code": raw.get("event_code"),
            "judge": raw.get("judge"),
            "case_caption": raw.get("case_caption"),
            "opinion_ref": raw.get("opinion_ref"),
            "num_pages": raw.get("num_pages"),
        }


# === CLI entry point ===
if __name__ == "__main__":
    scraper = USTaxCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        # Quick connectivity test
        results = _search_opinions("04/01/2026", "04/21/2026")
        if results:
            print(f"OK: DAWSON API returned {len(results)} opinions")
            print(f"  First: {results[0].get('caseCaption', 'N/A')}")
        else:
            print("FAIL: No results from DAWSON API")
            sys.exit(1)

    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        max_records = 12 if sample else 999999

        if sample:
            logger.info("=== SAMPLE MODE: fetching ~12 recent opinions ===")
            # Search recent 60 days for sample
            end = datetime.now()
            start = end - timedelta(days=60)
            ranges = _date_ranges(start, end)
            ranges.reverse()

            for s, e in ranges:
                start_str = s.strftime("%m/%d/%Y")
                end_str = e.strftime("%m/%d/%Y")
                logger.info(f"Searching {start_str} - {end_str}...")

                time.sleep(REQUEST_DELAY)
                results = _search_opinions(start_str, end_str)
                logger.info(f"  Found {len(results)} results")

                for result in results:
                    doc = scraper._fetch_opinion(result)
                    if doc:
                        record = scraper.normalize(doc)
                        out_file = sample_dir / f"{record['_id']}.json"
                        out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                        count += 1
                        logger.info(f"Saved [{count}]: {record['title'][:80]}")
                        if count >= max_records:
                            break
                if count >= max_records:
                    break

        elif command == "update":
            for raw in scraper.fetch_updates(""):
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:80]}")

        else:
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                out_file = sample_dir / f"{record['_id']}.json"
                out_file.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                count += 1
                logger.info(f"Saved [{count}]: {record['title'][:80]}")

        logger.info(f"Done. Total records: {count}")
        if count == 0:
            logger.error("No records fetched — check connectivity")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
