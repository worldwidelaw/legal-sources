#!/usr/bin/env python3
"""
GR/Diavgeia -- Greek Government Decisions Data Fetcher

Fetches Greek government decisions from the Diavgeia (Διαύγεια) OpenData API.
Diavgeia is Greece's transparency program requiring all government decisions
to be published online.

Strategy:
  - Uses the Diavgeia OpenData API for decision search and retrieval.
  - API returns JSON metadata including decision subjects, signers, organizations.
  - Full text is in PDF format - extract using pdfplumber.
  - API supports pagination (page/size parameters).

Endpoints:
  - Search: https://diavgeia.gov.gr/luminapi/opendata/search?page={n}&size={n}
  - Decision details: https://diavgeia.gov.gr/luminapi/api/decisions/{ada}
  - Document PDF: https://diavgeia.gov.gr/doc/{ada}
  - Organizations: https://diavgeia.gov.gr/luminapi/opendata/organizations

Data:
  - Government decisions from all public sector organizations
  - 71+ million decisions available
  - Published since 2010 (law 3861/2010)
  - Data types: administrative decisions, contracts, appointments, expenditures

License: CC-BY (Creative Commons Attribution)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.diavgeia")

# Base URL for Diavgeia API
BASE_URL = "https://diavgeia.gov.gr"
API_URL = "https://diavgeia.gov.gr/luminapi"


class DiavgeiaScraper(BaseScraper):
    """
    Scraper for GR/Diavgeia -- Greek Government Decisions.
    Country: GR
    URL: https://diavgeia.gov.gr

    Data types: administrative_decisions
    Auth: none (Open Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _download_and_extract_pdf(self, ada: str) -> str:
        """
        Download PDF and extract text using centralized pdf_extract module.
        """
        url = f"{BASE_URL}/doc/{ada}"
        text = extract_pdf_markdown(
            source="GR/Diavgeia",
            source_id=ada,
            pdf_url=url,
            table="doctrine",
        )
        return text or ""

    def _search_decisions(self, page: int = 0, size: int = 10,
                          from_issue_date: str = None, to_issue_date: str = None) -> Optional[Dict[str, Any]]:
        """
        Search for decisions via the OpenData API.

        Args:
            page: Page number (0-indexed)
            size: Number of results per page (max 500)
            from_issue_date: Start date filter (YYYY-MM-DD). API caps range to 6 months.
            to_issue_date: End date filter (YYYY-MM-DD).

        Returns:
            Dict with decisions list and pagination info
        """
        try:
            self.rate_limiter.wait()
            params = f"page={page}&size={size}"
            if from_issue_date:
                params += f"&from_issue_date={from_issue_date}"
            if to_issue_date:
                params += f"&to_issue_date={to_issue_date}"
            resp = self.client.get(f"/opendata/search?{params}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to search decisions: {e}")
            return None

    def _get_decision(self, ada: str) -> Optional[Dict[str, Any]]:
        """
        Get full decision details by ADA (unique identifier).

        Args:
            ada: The ADA (unique decision identifier)

        Returns:
            Dict with decision details
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/api/decisions/{ada}")
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Failed to get decision {ada}: {e}")
            return None

    def _process_decision(self, decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single decision: fetch full text and normalize.

        Args:
            decision: Raw decision from API

        Returns:
            Processed decision with full text
        """
        ada = decision.get("ada")
        if not ada:
            return None

        # Get full text from PDF
        full_text = self._download_and_extract_pdf(ada)
        if not full_text or len(full_text) < 50:
            logger.warning(f"Insufficient text for {ada}: {len(full_text) if full_text else 0} chars")
            return None

        # Convert timestamps
        issue_date = decision.get("issueDate")
        if issue_date:
            issue_date = datetime.fromtimestamp(issue_date / 1000, tz=timezone.utc).isoformat()

        publish_ts = decision.get("publishTimestamp")
        if publish_ts:
            publish_ts = datetime.fromtimestamp(publish_ts / 1000, tz=timezone.utc).isoformat()

        return {
            "ada": ada,
            "subject": decision.get("subject", ""),
            "full_text": full_text,
            "issue_date": issue_date,
            "publish_timestamp": publish_ts,
            "organization_id": decision.get("organizationId"),
            "decision_type_id": decision.get("decisionTypeId"),
            "thematic_categories": decision.get("thematicCategoryIds", []),
            "extra_fields": decision.get("extraFieldValues", {}),
            "status": decision.get("status"),
            "url": decision.get("url", f"{BASE_URL}/decision/view/{ada}"),
            "document_url": decision.get("documentUrl", f"{BASE_URL}/doc/{ada}"),
        }

    @staticmethod
    def _generate_date_windows(start_year: int = 2010) -> list:
        """Generate 6-month date windows from start_year to present."""
        from datetime import date
        windows = []
        today = date.today()
        year = start_year
        while True:
            for half in [(1, 1, 6, 30), (7, 1, 12, 31)]:
                start = date(year, half[0], half[1])
                end = date(year, half[2], half[3])
                if start > today:
                    return windows
                if end > today:
                    end = today
                windows.append((start.isoformat(), end.isoformat()))
            year += 1
        return windows

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from Diavgeia by iterating 6-month windows.

        The API caps date ranges to 6 months, so we iterate from 2010 to present.
        Note: With 71M+ decisions, this takes a very long time.
        """
        page_size = 500
        windows = self._generate_date_windows(start_year=2010)

        for from_date, to_date in windows:
            logger.info(f"Window {from_date} to {to_date}...")
            page = 0
            while True:
                result = self._search_decisions(
                    page=page, size=page_size,
                    from_issue_date=from_date, to_issue_date=to_date
                )
                if not result or "decisions" not in result:
                    break
                decisions = result["decisions"]
                if not decisions:
                    break
                for decision in decisions:
                    processed = self._process_decision(decision)
                    if processed:
                        yield processed
                page += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given date.

        Uses from_issue_date parameter to only fetch recent decisions.
        """
        page = 0
        page_size = 500
        from_date = since.strftime("%Y-%m-%d")

        while True:
            logger.info(f"Fetching updates page {page} (since {from_date})...")
            result = self._search_decisions(
                page=page, size=page_size,
                from_issue_date=from_date
            )

            if not result or "decisions" not in result:
                break

            decisions = result["decisions"]
            if not decisions:
                break

            for decision in decisions:
                processed = self._process_decision(decision)
                if processed:
                    yield processed

            page += 1

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw API response to standard schema.
        """
        return {
            "_id": raw.get("ada"),
            "_source": "GR/Diavgeia",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("subject", ""),
            "text": raw.get("full_text", ""),
            "date": raw.get("issue_date"),
            "published_date": raw.get("publish_timestamp"),
            "url": raw.get("url"),
            "document_url": raw.get("document_url"),
            "organization_id": raw.get("organization_id"),
            "decision_type": raw.get("decision_type_id"),
            "thematic_categories": raw.get("thematic_categories", []),
            "extra_fields": raw.get("extra_fields", {}),
            "status": raw.get("status"),
        }

    def _fetch_sample(self, sample_size: int = 15) -> list:
        """Fetch sample records from diverse years for validation."""
        samples = []
        # Sample from different year windows for diversity
        sample_windows = [
            ("2012-01-01", "2012-06-30"),
            ("2015-01-01", "2015-06-30"),
            ("2018-01-01", "2018-06-30"),
            ("2021-01-01", "2021-06-30"),
            ("2024-01-01", "2024-06-30"),
        ]
        per_window = max(3, sample_size // len(sample_windows))

        for from_date, to_date in sample_windows:
            if len(samples) >= sample_size:
                break
            logger.info(f"Sampling window {from_date} to {to_date}...")
            result = self._search_decisions(
                page=0, size=per_window * 2,
                from_issue_date=from_date, to_issue_date=to_date
            )
            if not result or "decisions" not in result:
                continue
            for decision in result["decisions"]:
                if len(samples) >= sample_size:
                    break
                processed = self._process_decision(decision)
                if processed:
                    normalized = self.normalize(processed)
                    samples.append(normalized)
                    logger.info(f"Sample {len(samples)}/{sample_size}: {normalized['_id']} ({len(normalized.get('text', ''))} chars) date={normalized.get('date')}")

        return samples


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GR/Diavgeia Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DiavgeiaScraper()

    if args.command == "test":
        print("Testing Diavgeia API connection...")
        result = scraper._search_decisions(page=0, size=1)
        if result and "decisions" in result:
            print(f"SUCCESS: API returned {len(result['decisions'])} decision(s)")
            print(f"Sample ADA: {result['decisions'][0].get('ada')}")
            print(f"Sample subject: {result['decisions'][0].get('subject')[:100]}...")
        else:
            print("FAILED: Could not retrieve decisions")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            samples = scraper._fetch_sample(sample_size=15)

            # Save samples
            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)

            for record in samples:
                filepath = sample_dir / f"{record['_id']}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to {sample_dir}/")

            # Print summary
            if samples:
                text_lengths = [len(s.get("text", "")) for s in samples]
                avg_len = sum(text_lengths) / len(text_lengths)
                print(f"Average text length: {avg_len:.0f} characters")
                print(f"Min text length: {min(text_lengths)} chars")
                print(f"Max text length: {max(text_lengths)} chars")
        else:
            print("Full bootstrap not recommended - 71M+ decisions would take months.")
            print("Use update command for incremental pulls.")

    elif args.command == "update":
        # Default to last 24 hours
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=1)
        print(f"Fetching updates since {since.isoformat()}...")

        count = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
            count += 1

        print(f"\nFetched {count} new decisions")


if __name__ == "__main__":
    main()
