#!/usr/bin/env python3
"""
UZ/LexUzCaseLaw -- Uzbekistan Criminal Court Decisions via publication.sud.uz

Fetches full text of criminal court decisions from publication.sud.uz REST API.

Strategy:
  - Paginate through /criminal/findAll JSON endpoint
  - For each record, download PDF via /api/file/criminal/{claimId}
  - Extract text from PDF using pypdf
  - Normalize into standard schema

Data: Public (publication.sud.uz, no auth required for criminal cases).
Rate limit: 2 sec between requests.

Coverage: ~441K criminal court decisions (first instance, appeal, cassation, control).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample decisions
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import io
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional


# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UZ.LexUzCaseLaw")

API_BASE = "https://publication.sud.uz"
CRIMINAL_LIST = f"{API_BASE}/criminal/findAll"
CRIMINAL_PDF = f"{API_BASE}/api/file/criminal"
DELAY = 2

INSTANCE_NAMES = {
    1: "First Instance",
    2: "Appeal",
    3: "Cassation",
    4: "Control",
}


class LexUzCaseLawScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
            },
        )

    def _fetch_page(self, page: int, size: int = 20) -> Optional[dict]:
        """Fetch a page of criminal case listings."""
        url = f"{CRIMINAL_LIST}?page={page}&size={size}"
        resp = self.http.get(url, timeout=30)
        if not resp or resp.status_code != 200:
            return None
        outer = resp.json()
        data_str = outer.get("data", "")
        if isinstance(data_str, str):
            return json.loads(data_str)
        return data_str

    def _fetch_pdf_text(self, claim_id: int) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="UZ/LexUzCaseLaw",
            source_id="",
            pdf_url=claim_id,
            table="case_law",
        ) or ""

    def test_api(self):
        """Test connectivity to publication.sud.uz."""
        logger.info("Testing publication.sud.uz connectivity...")
        try:
            data = self._fetch_page(0, size=2)
            if data and data.get("content"):
                total = data.get("totalElements", 0)
                logger.info(f"Connectivity test PASSED — {total} total records")
                return True
            logger.error("Connectivity test FAILED: no content")
            return False
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw case data into standard schema."""
        if not raw or not raw.get("text") or len(raw["text"]) < 50:
            return None

        claim_id = raw.get("claimId", "unknown")
        case_number = raw.get("caseNumber", "unknown")
        court = raw.get("dbName", "")
        instance = raw.get("instance", 0)
        instance_name = INSTANCE_NAMES.get(instance, f"Instance {instance}")

        _id = f"UZ-SUD-{claim_id}"

        articles = raw.get("claimArticles", [])
        articles_str = "; ".join(articles) if articles else ""

        title_parts = [f"Case {case_number}"]
        if court:
            title_parts.append(court)
        if instance_name:
            title_parts.append(instance_name)
        title = " — ".join(title_parts)

        return {
            "_id": _id,
            "_source": "UZ/LexUzCaseLaw",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": raw.get("hearingDate"),
            "url": f"{API_BASE}/criminal/findAll",
            "case_number": case_number,
            "court": court,
            "instance": instance_name,
            "judge": raw.get("judge", ""),
            "articles": articles_str,
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Fetch criminal court decisions with full text from PDFs."""
        sample_limit = 15 if sample else None
        count = 0
        page = 0
        page_size = 20
        max_pages = 5 if sample else 50000
        consecutive_failures = 0

        while page < max_pages:
            time.sleep(DELAY)
            data = self._fetch_page(page, size=page_size)
            if not data or not data.get("content"):
                logger.warning(f"  No data on page {page}")
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    break
                page += 1
                continue

            consecutive_failures = 0
            records = data["content"]
            total = data.get("totalElements", 0)

            if page == 0:
                logger.info(f"Total criminal cases: {total}")

            for rec in records:
                claim_id = rec.get("claimId")
                case_num = rec.get("caseNumber", "?")
                if not claim_id:
                    continue

                time.sleep(DELAY)
                text = self._fetch_pdf_text(claim_id)
                if not text or len(text) < 50:
                    logger.warning(f"  Skipping {case_num} (claim {claim_id}): no text from PDF")
                    continue

                rec["text"] = text
                record = self.normalize(rec)
                if record:
                    count += 1
                    logger.info(f"  [{count}] {case_num} ({rec.get('dbName', '')[:40]}) — {len(text)} chars")
                    yield record

                    if sample_limit and count >= sample_limit:
                        logger.info(f"Sample limit ({sample_limit}) reached")
                        return

            if data.get("last", False):
                break
            page += 1

        logger.info(f"Total decisions fetched: {count}")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch decisions since a date (re-fetches from beginning)."""
        yield from self.fetch_all(sample=False)

    def bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = Path(self.source_dir) / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in self.fetch_all(sample=sample):
            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
            count += 1
            logger.info(f"Saved: {out_file.name}")

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="UZ/LexUzCaseLaw bootstrapper")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 decisions)")
    args = parser.parse_args()

    scraper = LexUzCaseLawScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = scraper.bootstrap(sample=args.sample)
        if count == 0:
            logger.error("No records fetched!")
            sys.exit(1)
        sys.exit(0)


if __name__ == "__main__":
    main()
