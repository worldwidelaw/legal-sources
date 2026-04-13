#!/usr/bin/env python3
"""
US/KY-Courts -- Kentucky Supreme Court & Court of Appeals Opinions

Fetches case law from the Kentucky C-Track Public Access system
(appellatepublic.kycourts.net). Uses the official REST API with
header-based pagination and full text retrieval.

Courts covered:
  - Kentucky Supreme Court (~10,800 opinions)
  - Kentucky Court of Appeals (~56,600 opinions)

Strategy:
  1. Search opinions by date range (year-by-year, month-by-month for dense periods)
  2. For each opinion, fetch full text via publicaccessdocuments endpoint
  3. Normalize into standard schema

Data source: https://appellatepublic.kycourts.net/
License: Public Domain (U.S. government works)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all years)
  python bootstrap.py update --since YYYY-MM-DD  # Filter by date
  python bootstrap.py test                  # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.KY-Courts")

API_BASE = "https://appellatepublic.kycourts.net/api/api/v1"
OPINIONS_SEARCH = f"{API_BASE}/opinions/search"
DOCUMENTS_URL = f"{API_BASE}/publicaccessdocuments"
CASES_URL = f"{API_BASE}/cases"

COURTS = [
    "Kentucky Supreme Court",
    "Kentucky Court of Appeals",
]

COURT_ABBR = {
    "Kentucky Supreme Court": "KYSC",
    "Kentucky Court of Appeals": "KYCA",
}

USER_AGENT = "LegalDataHunter/1.0 (legal research; open data collection)"

# First opinion date per Juriscraper
FIRST_OPINION_DATE = "1982-02-18"
PAGE_SIZE = 200  # Max 500, use 200 for safety


class KYCourtsScraper(BaseScraper):
    """
    Scraper for US/KY-Courts — Kentucky Supreme Court & Court of Appeals.
    Uses the C-Track Public Access REST API.
    """

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        })

    def _search_opinions(
        self,
        date_from: str,
        date_to: str,
        court: Optional[str] = None,
        start_index: int = 1,
        max_results: int = PAGE_SIZE,
    ) -> Tuple[List[Dict], int, bool]:
        """
        Search opinions by date range.

        Args:
            date_from: MM/DD/YYYY format
            date_to: MM/DD/YYYY format
            court: Optional court name filter
            start_index: 1-based pagination start
            max_results: Results per page (max 500)

        Returns:
            (items, total_count, more_results)
        """
        params = {
            "queryString": "true",
            "searchFields[0].operation": ">=",
            "searchFields[0].values[0]": date_from,
            "searchFields[0].indexFieldName": "filedDate",
            "searchFields[1].operation": "<=",
            "searchFields[1].values[0]": date_to,
            "searchFields[1].indexFieldName": "filedDate",
        }

        if court:
            params["searchFilters[0].operation"] = "="
            params["searchFilters[0].indexFieldName"] = "caseHeader.court"
            params["searchFilters[0].values[0]"] = court

        headers = {
            "X-CTrack-Paging-StartIndex": str(start_index),
            "X-CTrack-Paging-MaxResults": str(max_results),
        }

        try:
            resp = self.session.get(
                OPINIONS_SEARCH, params=params, headers=headers, timeout=60
            )
            resp.raise_for_status()
            data = resp.json()

            total_count = int(resp.headers.get("X-CTrack-Paging-TotalCount", 0))
            more_results = resp.headers.get("X-CTrack-Paging-MoreResults", "false").lower() == "true"

            return data.get("resultItems", []), total_count, more_results
        except Exception as e:
            logger.error(f"Opinion search failed ({date_from}-{date_to}): {e}")
            return [], 0, False

    def _get_case_details(self, case_id: str) -> Optional[Dict]:
        """Fetch case metadata."""
        try:
            resp = self.session.get(f"{CASES_URL}/{case_id}", timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to fetch case {case_id}: {e}")
            return None

    def _get_document_text(self, docket_entry_id: str) -> str:
        """Fetch full text of a document by docket entry ID."""
        try:
            resp = self.session.get(
                DOCUMENTS_URL,
                params={"filter": f"parentCategory=docketentries,parentID={docket_entry_id}"},
                timeout=60,
            )
            resp.raise_for_status()
            docs = resp.json()

            if not docs or not isinstance(docs, list):
                return ""

            # Combine all documentText parts from the first document
            doc = docs[0]
            text_parts = doc.get("documentText", [])
            if not text_parts:
                return ""

            full_text = "\n".join(text_parts)
            # Clean up the text
            full_text = self._clean_text(full_text)
            return full_text
        except Exception as e:
            logger.warning(f"Failed to fetch document text for {docket_entry_id}: {e}")
            return ""

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text."""
        if not text:
            return ""
        # Remove excessive whitespace but preserve paragraph breaks
        text = re.sub(r"\r\n", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        # Strip HTML tags if any
        text = re.sub(r"<[^>]+>", "", text)
        return text.strip()

    def _generate_date_ranges(
        self, start_date: str, end_date: str
    ) -> List[Tuple[str, str]]:
        """Generate monthly date ranges in MM/DD/YYYY format."""
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        ranges = []

        current = start
        while current < end:
            # End of month
            if current.month == 12:
                month_end = datetime(current.year + 1, 1, 1) - timedelta(days=1)
            else:
                month_end = datetime(current.year, current.month + 1, 1) - timedelta(days=1)

            if month_end > end:
                month_end = end

            from_str = current.strftime("%m/%d/%Y")
            to_str = month_end.strftime("%m/%d/%Y")
            ranges.append((from_str, to_str))

            # Move to next month
            if current.month == 12:
                current = datetime(current.year + 1, 1, 1)
            else:
                current = datetime(current.year, current.month + 1, 1)

        return ranges

    def _fetch_opinions_for_range(
        self, date_from: str, date_to: str, court: Optional[str] = None
    ) -> Generator[Dict[str, Any], None, None]:
        """Fetch all opinions in a date range with pagination."""
        start_index = 1
        while True:
            items, total, more = self._search_opinions(
                date_from, date_to, court=court, start_index=start_index
            )

            if not items:
                break

            for item in items:
                row = item.get("rowMap", {})
                docket_entry_id = row.get("docketEntryID", "")
                case_id = row.get("caseHeader.caseID", "")
                case_number = row.get("caseHeader.caseNumber", "")
                filed_date = row.get("filedDate", "")
                entry_desc = row.get("docketEntryDescription", "")
                entry_type = row.get("docketEntryType", "")
                has_docs = row.get("hasDocuments", False)

                if not docket_entry_id or not has_docs:
                    continue

                yield {
                    "docket_entry_id": docket_entry_id,
                    "case_id": case_id,
                    "case_number": case_number,
                    "filed_date": filed_date,
                    "entry_description": entry_desc,
                    "entry_type": entry_type,
                }

            if not more:
                break

            start_index += len(items)
            time.sleep(0.5)

    def _process_opinion(self, opinion_meta: Dict) -> Optional[Dict[str, Any]]:
        """Process an opinion: fetch case details and full text."""
        docket_entry_id = opinion_meta["docket_entry_id"]
        case_id = opinion_meta["case_id"]

        # Fetch full text
        time.sleep(1.0)
        text = self._get_document_text(docket_entry_id)
        if not text or len(text) < 100:
            logger.warning(
                f"Skipping {opinion_meta['case_number']}: insufficient text ({len(text)} chars)"
            )
            return None

        # Fetch case details
        time.sleep(1.0)
        case_details = self._get_case_details(case_id)

        court_name = ""
        case_type = ""
        case_classification = ""
        short_title = ""

        if case_details:
            court_name = case_details.get("court", "")
            case_type = case_details.get("caseType", "")
            case_classification = case_details.get("caseClassification", "")
            short_title = case_details.get("shortTitle", "")

        # Parse filed date
        filed_date = opinion_meta.get("filed_date", "")
        if filed_date:
            try:
                dt = datetime.strptime(filed_date[:10], "%Y-%m-%d")
                filed_date = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # Clean title
        if short_title:
            short_title = short_title.replace("\r\n", " ").replace("\n", " ").strip()

        return {
            "docket_entry_id": docket_entry_id,
            "case_id": case_id,
            "case_number": opinion_meta["case_number"],
            "court": court_name,
            "title": short_title,
            "date": filed_date,
            "text": text,
            "case_type": case_type,
            "case_classification": case_classification,
            "entry_description": opinion_meta.get("entry_description", ""),
            "entry_type": opinion_meta.get("entry_type", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all Kentucky appellate opinions."""
        today = datetime.now().strftime("%Y-%m-%d")
        date_ranges = self._generate_date_ranges(FIRST_OPINION_DATE, today)

        for date_from, date_to in date_ranges:
            logger.info(f"Fetching opinions for {date_from} - {date_to}...")

            for opinion_meta in self._fetch_opinions_for_range(date_from, date_to):
                raw = self._process_opinion(opinion_meta)
                if raw:
                    yield raw

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch opinions filed after a given date."""
        if not since:
            since = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

        today = datetime.now().strftime("%Y-%m-%d")
        date_ranges = self._generate_date_ranges(since, today)

        for date_from, date_to in date_ranges:
            logger.info(f"Fetching updates for {date_from} - {date_to}...")

            for opinion_meta in self._fetch_opinions_for_range(date_from, date_to):
                raw = self._process_opinion(opinion_meta)
                if raw:
                    yield raw

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw opinion record into the standard schema."""
        court = raw.get("court", "")
        abbr = COURT_ABBR.get(court, "KY")
        case_number = raw.get("case_number", "")
        doc_id = f"US-{abbr}-{case_number}" if case_number else f"US-{abbr}-{raw.get('docket_entry_id', '')[:12]}"

        return {
            "_id": doc_id,
            "_source": "US/KY-Courts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": f"https://appellatepublic.kycourts.net/",
            "case_number": case_number,
            "court": court,
            "case_type": raw.get("case_type", ""),
            "case_classification": raw.get("case_classification", ""),
            "entry_description": raw.get("entry_description", ""),
            "jurisdiction": "US-KY",
        }

    def test_connection(self) -> bool:
        """Test connectivity to the C-Track API."""
        try:
            items, total, _ = self._search_opinions(
                "01/01/2025", "12/31/2025", max_results=1
            )
            logger.info(f"Connection test: {total} opinions found for 2025")
            return total > 0
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/KY-Courts data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample for validation",
    )
    parser.add_argument(
        "--since",
        help="ISO date for incremental updates (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full bootstrap (all years)",
    )
    args = parser.parse_args()

    scraper = KYCourtsScraper()

    if args.command == "test":
        success = scraper.test_connection()
        print(f"Connection test: {'PASSED' if success else 'FAILED'}")
        sys.exit(0 if success else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        count = 0
        target = 15 if args.sample else 999999

        if args.sample:
            # For sample: fetch recent opinions from both courts
            today = datetime.now()
            date_from = (today - timedelta(days=60)).strftime("%m/%d/%Y")
            date_to = today.strftime("%m/%d/%Y")

            seen_courts = set()
            for court in COURTS:
                if count >= target:
                    break

                logger.info(f"Sampling from {court}...")
                for opinion_meta in scraper._fetch_opinions_for_range(
                    date_from, date_to, court=court
                ):
                    if count >= target:
                        break

                    raw = scraper._process_opinion(opinion_meta)
                    if not raw:
                        continue

                    record = scraper.normalize(raw)
                    safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
                    out_path = sample_dir / f"{safe_id}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)

                    text_len = len(record.get("text", ""))
                    logger.info(
                        f"[{count + 1}] {record['_id']}: "
                        f"{record['title'][:60]} ({text_len} chars)"
                    )
                    count += 1
        else:
            gen = scraper.fetch_all()
            for raw in gen:
                record = scraper.normalize(raw)
                safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
                out_path = sample_dir / f"{safe_id}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

                text_len = len(record.get("text", ""))
                logger.info(
                    f"[{count + 1}] {record['_id']}: "
                    f"{record['title'][:60]} ({text_len} chars)"
                )
                count += 1

                if count >= target:
                    break

        print(f"\nBootstrap complete: {count} records saved to {sample_dir}")

    elif args.command == "update":
        count = 0
        gen = scraper.fetch_updates(since=args.since)
        for raw in gen:
            record = scraper.normalize(raw)
            safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
            out_path = sample_dir / f"{safe_id}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1

        print(f"\nUpdate complete: {count} records")


if __name__ == "__main__":
    main()
