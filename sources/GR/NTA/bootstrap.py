#!/usr/bin/env python3
"""
GR/NTA -- National Transparency Authority (AEAD)

Fetches AEAD decisions from the Diavgeia transparency portal API.

Strategy:
  - Uses the Diavgeia OpenData API filtered for AEAD organization
  - Organization latin name: NATIONAL_TRANSPARENCY_AUTHORITY
  - Full text extracted from PDF documents via pdfplumber
  - API supports pagination with from_issue_date/to_issue_date

Endpoints:
  - Search: https://diavgeia.gov.gr/luminapi/opendata/search?org=NATIONAL_TRANSPARENCY_AUTHORITY
  - Document PDF: https://diavgeia.gov.gr/doc/{ada}

Data:
  - Administrative decisions, audit findings, ethics committee decisions
  - ~2,925 decisions since 2020
  - Published via Diavgeia (law 3861/2010)

License: CC-BY (via Diavgeia)

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
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.NTA")

BASE_URL = "https://diavgeia.gov.gr"
API_URL = "https://diavgeia.gov.gr/luminapi"
ORG_NAME = "NATIONAL_TRANSPARENCY_AUTHORITY"


class NTAScraper(BaseScraper):
    """
    Scraper for GR/NTA -- National Transparency Authority (AEAD) decisions.
    Country: GR
    URL: https://aead.gr (data via Diavgeia)

    Data types: doctrine
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

    def _download_and_extract_pdf(self, ada: str, max_pdf_size_mb: int = 10) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="GR/NTA",
            source_id="",
            pdf_bytes=ada,
            table="doctrine",
        ) or ""

    def _search_decisions(
        self,
        page: int = 0,
        size: int = 50,
        from_date: str = "2020-01-01",
        to_date: str = "2030-12-31",
    ) -> Optional[Dict[str, Any]]:
        """Search for NTA decisions via the OpenData API."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(
                "/opendata/search",
                params={
                    "org": ORG_NAME,
                    "page": page,
                    "size": size,
                    "from_issue_date": from_date,
                    "to_issue_date": to_date,
                }
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.error(f"Failed to search NTA decisions: {e}")
            return None

    def _process_decision(self, decision: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single decision: fetch full text and normalize."""
        ada = decision.get("ada")
        if not ada:
            return None

        full_text = self._download_and_extract_pdf(ada)
        if not full_text or len(full_text) < 50:
            logger.warning(f"Insufficient text for {ada}: {len(full_text) if full_text else 0} chars")
            return None

        issue_date = decision.get("issueDate")
        if issue_date:
            issue_date = datetime.fromtimestamp(issue_date / 1000, tz=timezone.utc).isoformat()

        publish_ts = decision.get("publishTimestamp")
        if publish_ts:
            publish_ts = datetime.fromtimestamp(publish_ts / 1000, tz=timezone.utc).isoformat()

        return {
            "ada": ada,
            "protocol_number": decision.get("protocolNumber", ""),
            "subject": decision.get("subject", ""),
            "full_text": full_text,
            "issue_date": issue_date,
            "publish_timestamp": publish_ts,
            "decision_type_id": decision.get("decisionTypeId"),
            "thematic_categories": decision.get("thematicCategoryIds", []),
            "status": decision.get("status"),
            "url": decision.get("url", f"{BASE_URL}/decision/view/{ada}"),
            "document_url": decision.get("documentUrl", f"{BASE_URL}/doc/{ada}"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all NTA decisions, year by year to handle date window."""
        for year in range(2020, 2027):
            logger.info(f"Fetching NTA decisions for {year}...")
            page = 0
            page_size = 50

            while True:
                logger.info(f"  Year {year}, page {page}...")
                result = self._search_decisions(
                    page=page, size=page_size,
                    from_date=f"{year}-01-01",
                    to_date=f"{year}-12-31",
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
        """Yield documents published since the given date."""
        cutoff_ts = int(since.timestamp() * 1000)
        from_date = since.strftime("%Y-%m-%d")
        page = 0

        while True:
            result = self._search_decisions(page=page, size=50, from_date=from_date)
            if not result or "decisions" not in result:
                break

            decisions = result["decisions"]
            if not decisions:
                break

            for decision in decisions:
                publish_ts = decision.get("publishTimestamp", 0)
                if publish_ts >= cutoff_ts:
                    processed = self._process_decision(decision)
                    if processed:
                        yield processed

            page += 1

    def normalize(self, raw: dict) -> dict:
        """Transform raw API response to standard schema."""
        subject = raw.get("subject", "")
        title = subject or f"NTA Decision {raw.get('ada')}"

        return {
            "_id": raw.get("ada"),
            "_source": "GR/NTA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": raw.get("issue_date"),
            "published_date": raw.get("publish_timestamp"),
            "url": raw.get("url"),
            "document_url": raw.get("document_url"),
            "protocol_number": raw.get("protocol_number"),
            "decision_type": raw.get("decision_type_id"),
            "thematic_categories": raw.get("thematic_categories", []),
            "status": raw.get("status"),
        }

    def _fetch_sample(self, sample_size: int = 12) -> list:
        """Fetch sample records for validation."""
        samples = []

        logger.info("Fetching samples...")
        result = self._search_decisions(page=0, size=20)

        if not result or "decisions" not in result:
            return samples

        for decision in result["decisions"]:
            if len(samples) >= sample_size:
                break

            processed = self._process_decision(decision)
            if processed:
                normalized = self.normalize(processed)
                samples.append(normalized)
                logger.info(
                    f"Sample {len(samples)}/{sample_size}: {normalized['_id']} "
                    f"({len(normalized.get('text', ''))} chars)"
                )

        return samples


def main():
    import argparse

    parser = argparse.ArgumentParser(description="GR/NTA Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    args = parser.parse_args()

    scraper = NTAScraper()

    if args.command == "test":
        print("Testing NTA via Diavgeia API connection...")
        result = scraper._search_decisions(page=0, size=1)
        if result and "decisions" in result:
            total = result.get("info", {}).get("total", "?")
            print(f"SUCCESS: API returned {len(result['decisions'])} decision(s)")
            print(f"Total decisions available: {total}")
            if result["decisions"]:
                d = result["decisions"][0]
                print(f"Sample ADA: {d.get('ada')}")
                print(f"Sample subject: {d.get('subject', '')[:100]}...")
        else:
            print("FAILED: Could not retrieve decisions")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records...")
            if not PDF_SUPPORT:
                print("\nERROR: pdfplumber not installed. Run: pip install pdfplumber")
                sys.exit(1)

            samples = scraper._fetch_sample(sample_size=12)

            sample_dir = scraper.source_dir / "sample"
            sample_dir.mkdir(exist_ok=True)

            for record in samples:
                filepath = sample_dir / f"{record['_id']}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            print(f"\nSaved {len(samples)} sample records to {sample_dir}/")

            if samples:
                text_lengths = [len(s.get("text", "")) for s in samples]
                avg_len = sum(text_lengths) / len(text_lengths)
                print(f"Average text length: {avg_len:.0f} characters")
                print(f"Min text length: {min(text_lengths)} chars")
                print(f"Max text length: {max(text_lengths)} chars")
        else:
            print("Running full bootstrap...")
            if not PDF_SUPPORT:
                print("\nERROR: pdfplumber not installed. Run: pip install pdfplumber")
                sys.exit(1)

            count = 0
            for record in scraper.fetch_all():
                normalized = scraper.normalize(record)
                print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
                count += 1

            print(f"\nFetched {count} NTA decisions")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=7)
        print(f"Fetching updates since {since.isoformat()}...")

        count = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            print(f"  {normalized['_id']}: {len(normalized.get('text', ''))} chars")
            count += 1

        print(f"\nFetched {count} new NTA decisions")


if __name__ == "__main__":
    main()
