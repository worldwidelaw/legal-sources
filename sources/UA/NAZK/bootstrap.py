#!/usr/bin/env python3
"""
UA/NAZK -- National Agency on Corruption Prevention (NAZK) Open Data

Fetches open data from Ukraine's data.gov.ua portal published by NAZK:
1. Unified State Register of Corruption Offenders (~50K records)
   - JSON bulk download with offense descriptions and punishment details
2. NAZK Monitoring Conclusions on conflict-of-interest violations (~170 records)
   - CSV with violation descriptions and legal qualifications

Data access:
  - Both datasets are CC BY 4.0 on data.gov.ua
  - Register: JSON array of objects with offenseName, punishment, court info
  - Conclusions: CSV with uid, date, description, legalQualification

Usage:
  python bootstrap.py bootstrap          # Full pull (~50K records)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Same as bootstrap (bulk download)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import csv
import io
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UA.NAZK")

SOURCE_ID = "UA/NAZK"

REGISTER_URL = (
    "https://data.gov.ua/dataset/560e5f5d-0b8b-491e-a0b1-72eb08602edc/"
    "resource/b09c40e0-8cab-468f-ae99-fe7b8eda3463/download/"
    "corruptinfo-nazk-gov-ua-ep-1-0-corrupt-getalldata.json"
)
CONCLUSIONS_URL = (
    "https://data.gov.ua/dataset/b20fbfc3-c0f1-4630-9707-c8dc71c6bd7c/"
    "resource/5b6f4585-b8c8-40b0-a892-018758d03705/download/"
    "conclusions.csv"
)

PORTAL_BASE = "https://corruptinfo.nazk.gov.ua"


class NAZKScraper(BaseScraper):
    """Scraper for NAZK open data from data.gov.ua."""

    def __init__(self):
        source_dir = Path(__file__).resolve().parent
        super().__init__(str(source_dir))
        self.http = HttpClient(
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
            }
        )

    def _fetch_register(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch corruption offenders register from data.gov.ua JSON."""
        logger.info("Downloading corruption offenders register from data.gov.ua...")
        resp = self.http.get(REGISTER_URL, timeout=300)
        resp.raise_for_status()
        records = resp.json()
        logger.info(f"Loaded {len(records)} register records")

        count = 0
        for rec in records:
            if sample and count >= 12:
                break

            offense_name = rec.get("offenseName", "") or ""
            punishment = rec.get("punishment", "") or ""
            text_parts = []
            if offense_name:
                text_parts.append(offense_name.strip())
            if punishment:
                text_parts.append(punishment.strip())
            text = "\n\n".join(text_parts)

            if not text.strip():
                continue

            rec_id = rec.get("id", "")
            sentence_date = rec.get("sentenceDate", "")
            court_name = rec.get("courtName", "")
            court_case = rec.get("courtCaseNumber", "")

            codex_articles = rec.get("codexArticles", []) or []
            articles_str = "; ".join(
                a.get("codexArticleName", "") for a in codex_articles if a.get("codexArticleName")
            )

            person_parts = []
            for field in ["indLastNameOnOffenseMoment", "indFirstNameOnOffenseMoment", "indPatronymicOnOffenseMoment"]:
                val = rec.get(field, "")
                if val:
                    person_parts.append(val)
            person_name = " ".join(person_parts)

            entity_type = ""
            if rec.get("entityType"):
                entity_type = rec["entityType"].get("name", "")

            punishment_type = ""
            if rec.get("punishmentType"):
                punishment_type = rec["punishmentType"].get("name", "")

            title = f"Корупційне правопорушення: {person_name}" if person_name else f"Corruption offense #{rec_id}"

            normalized = {
                "_id": f"nazk-register-{rec_id}",
                "_source": SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": text,
                "date": sentence_date if sentence_date else None,
                "url": f"{PORTAL_BASE}/#record-{rec_id}",
                "offense_type": articles_str,
                "legal_articles": articles_str,
                "court_name": court_name,
                "court_case_number": court_case,
                "person_name": person_name,
                "entity_type": entity_type,
                "punishment_type": punishment_type,
                "dataset": "corruption_offenders_register",
            }
            yield normalized
            count += 1

        logger.info(f"Yielded {count} register records")

    def _fetch_conclusions(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch NAZK monitoring conclusions on conflict-of-interest violations."""
        logger.info("Downloading monitoring conclusions CSV...")
        resp = self.http.get(CONCLUSIONS_URL, timeout=60)
        resp.raise_for_status()

        content = resp.content.decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))

        count = 0
        for row in reader:
            if sample and count >= 3:
                break

            uid = row.get("uid", "").strip()
            date_str = row.get("date", "").strip()
            person_family = row.get("personFamilyName", "").strip()
            person_first = row.get("personName", "").strip()
            person_patronymic = row.get("personAdditionalName", "").strip()
            position = row.get("personPosition", "").strip()
            description = row.get("description", "").strip()
            legal_qual = row.get("legalQualification", "").strip()
            forwarded_to = row.get("forwardedTo", "").strip()

            text_parts = []
            if position:
                text_parts.append(f"Посада: {position}")
            if description:
                text_parts.append(f"\n{description}")
            if legal_qual:
                text_parts.append(f"\nПравова кваліфікація: {legal_qual}")
            if forwarded_to:
                text_parts.append(f"\nСкеровано до: {forwarded_to}")

            text = "\n".join(text_parts)
            if not text.strip():
                continue

            person_name = " ".join(p for p in [person_family, person_first, person_patronymic] if p)
            title = f"Обґрунтований висновок: {person_name}" if person_name else f"Monitoring conclusion #{uid}"

            normalized = {
                "_id": f"nazk-conclusion-{uid}",
                "_source": SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": title,
                "text": text,
                "date": date_str if date_str else None,
                "url": f"https://nazk.gov.ua/uk/vysnovky/",
                "offense_type": legal_qual,
                "legal_articles": legal_qual,
                "court_name": None,
                "person_name": person_name,
                "dataset": "monitoring_conclusions",
            }
            yield normalized
            count += 1

        logger.info(f"Yielded {count} conclusion records")

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all NAZK records."""
        yield from self._fetch_register(sample=False)
        yield from self._fetch_conclusions(sample=False)

    def fetch_sample(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch sample records for testing."""
        yield from self._fetch_register(sample=True)
        yield from self._fetch_conclusions(sample=True)

    def fetch_updates(self, since) -> Generator[Dict[str, Any], None, None]:
        """Bulk download - same as fetch_all (dataset is a single file)."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Records are already normalized during fetch."""
        return raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="UA/NAZK bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NAZKScraper()
    source_dir = Path(__file__).resolve().parent
    sample_dir = source_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "test":
        logger.info("Testing connectivity to data.gov.ua...")
        try:
            resp = scraper.http.get(CONCLUSIONS_URL, timeout=30)
            resp.raise_for_status()
            logger.info(f"Conclusions CSV: {resp.status_code}, {len(resp.content)} bytes")

            resp2 = scraper.http.get(REGISTER_URL, timeout=30, stream=True)
            logger.info(f"Register JSON: {resp2.status_code}")
            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)
        return

    if args.command == "bootstrap":
        if args.sample:
            logger.info("Fetching sample records...")
            records = list(scraper.fetch_sample())
        else:
            logger.info("Fetching all records...")
            records = list(scraper.fetch_all())

        logger.info(f"Total records: {len(records)}")

        # Save samples
        for i, rec in enumerate(records[:15]):
            sample_path = sample_dir / f"sample_{i+1:03d}.json"
            with open(sample_path, "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {sample_path.name}: {rec.get('title', '')[:60]}")

        # Validate
        text_lengths = [len(rec.get("text", "")) for rec in records[:15]]
        non_empty = sum(1 for t in text_lengths if t > 0)
        logger.info(f"Sample validation: {non_empty}/{len(records[:15])} have non-empty text")
        if text_lengths:
            logger.info(f"Text lengths: min={min(text_lengths)}, max={max(text_lengths)}, avg={sum(text_lengths)/len(text_lengths):.0f}")

    elif args.command == "update":
        logger.info("Running update (same as full bootstrap for bulk download source)...")
        records = list(scraper.fetch_all())
        logger.info(f"Total records: {len(records)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
