#!/usr/bin/env python3
"""
AU/OpenAusLegalCorpus -- State-level Australian Legal Corpus Fetcher

Fetches state-level legislation and case law from the Open Australian
Legal Corpus on HuggingFace (isaacus/open-australian-legal-corpus).

Excludes sources already covered by AU/FedCourt and AU/FederalRegister:
  - federal_court_of_australia -> AU/FedCourt
  - federal_register_of_legislation -> AU/FederalRegister

Includes:
  - high_court_of_australia (~8K decisions)
  - nsw_caselaw (~117K decisions)
  - nsw_legislation
  - qld_legislation
  - wa_legislation
  - sa_legislation
  - tas_legislation

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AU.OpenAusLegalCorpus")

DATASET_ID = "isaacus/open-australian-legal-corpus"

# Sources already covered by other AU scrapers
EXCLUDED_SOURCES = {
    "federal_court_of_australia",       # AU/FedCourt
    "federal_register_of_legislation",  # AU/FederalRegister
}

# Map HuggingFace source names to human-readable court/source names
SOURCE_MAP = {
    "high_court_of_australia": ("High Court of Australia", "case_law"),
    "nsw_caselaw": ("NSW Courts", "case_law"),
    "nsw_legislation": ("NSW Legislation", "legislation"),
    "queensland_legislation": ("QLD Legislation", "legislation"),
    "western_australian_legislation": ("WA Legislation", "legislation"),
    "south_australian_legislation": ("SA Legislation", "legislation"),
    "tasmanian_legislation": ("TAS Legislation", "legislation"),
}

# Map HuggingFace type field to our _type
TYPE_MAP = {
    "decision": "case_law",
    "primary_legislation": "legislation",
    "secondary_legislation": "legislation",
    "bill": "legislation",
}

# Map jurisdiction field to short form
JURISDICTION_MAP = {
    "commonwealth": "AU-CTH",
    "new_south_wales": "AU-NSW",
    "queensland": "AU-QLD",
    "western_australia": "AU-WA",
    "south_australia": "AU-SA",
    "tasmania": "AU-TAS",
    "norfolk_island": "AU-NFK",
}


class OpenAusLegalCorpusScraper(BaseScraper):
    """
    Scraper for AU/OpenAusLegalCorpus -- State + High Court documents.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _parse_date(self, date_str: str) -> Optional[str]:
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str.strip()[:10], "%Y-%m-%d").strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        version_id = raw.get("version_id", "")
        text = raw.get("text", "")
        citation = raw.get("citation", "") or version_id
        date_str = raw.get("date", "")
        url = raw.get("url", "")
        source = raw.get("source", "")
        jurisdiction = raw.get("jurisdiction", "")
        doc_type = raw.get("type", "")

        source_info = SOURCE_MAP.get(source, (source, "legislation"))
        mapped_type = TYPE_MAP.get(doc_type, "legislation")

        return {
            "_id": version_id,
            "_source": "AU/OpenAusLegalCorpus",
            "_type": mapped_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": citation,
            "text": text,
            "date": self._parse_date(date_str),
            "url": url,
            "jurisdiction": JURISDICTION_MAP.get(jurisdiction, jurisdiction),
            "source_name": source_info[0],
            "doc_type": doc_type,
            "version_id": version_id,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        try:
            from datasets import load_dataset
        except ImportError:
            logger.error("HuggingFace datasets library required: pip install datasets")
            return

        logger.info(f"Streaming dataset {DATASET_ID} (excluding {EXCLUDED_SOURCES})")

        ds = load_dataset(DATASET_ID, split="corpus", streaming=True)
        count = 0
        skipped = 0
        total_scanned = 0

        for row in ds:
            total_scanned += 1
            if total_scanned % 25000 == 0:
                logger.info(
                    f"Scanned {total_scanned} records, yielded {count}, "
                    f"skipped {skipped} (excluded sources)"
                )

            source = row.get("source", "")
            if source in EXCLUDED_SOURCES:
                skipped += 1
                continue

            text = row.get("text", "")
            if not text or len(text.strip()) < 50:
                continue

            normalized = self.normalize(row)
            count += 1
            yield normalized

        logger.info(
            f"Completed: {count} documents from {total_scanned} total "
            f"({skipped} excluded)"
        )

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        logger.info("Updates not supported for HuggingFace bulk dataset. Use fetch_all.")
        return
        yield  # Make it a generator

    def test(self) -> bool:
        try:
            from datasets import load_dataset
            ds = load_dataset(DATASET_ID, split="corpus", streaming=True)
            count = 0
            scanned = 0
            for row in ds:
                scanned += 1
                source = row.get("source", "")
                if source not in EXCLUDED_SOURCES:
                    text = row.get("text", "")
                    if text and len(text.strip()) >= 50:
                        count += 1
                        logger.info(
                            f"  [{count}] {row.get('citation', '?')[:80]} "
                            f"(source={source}, {len(text):,} chars)"
                        )
                        if count >= 3:
                            logger.info(f"Test passed: found {count} records in {scanned} scanned")
                            return True
                if scanned > 5000:
                    break
            return count > 0
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="AU/OpenAusLegalCorpus data fetcher")
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

    scraper = OpenAusLegalCorpusScraper()

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
