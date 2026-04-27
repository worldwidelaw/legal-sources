#!/usr/bin/env python3
"""
INTL/WorldBankDocs — World Bank Legal Agreements

Fetches World Bank legal documents (loan agreements, credit agreements,
grant agreements, etc.) via the World Bank Documents & Reports REST API.

Strategy:
  - Query search.worldbank.org/api/v3/wds for legal document types
  - Download full text directly from txturl (pre-extracted by World Bank)
  - ~48,000 legal agreements

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update --since 2024-01-01
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WorldBankDocs")

API_URL = "https://search.worldbank.org/api/v3/wds"

LEGAL_DOC_TYPES = [
    "Agreement",
    "Loan Agreement",
    "Credit Agreement",
    "Guarantee Agreement",
    "Project Agreement",
    "Financing Agreement",
]

FIELDS = "id,display_title,docdt,url,txturl,pdfurl,docty,count,lang,projectid,repnb,seccl,disclosure_date"

PAGE_SIZE = 200


class WorldBankDocsScraper(BaseScraper):
    """
    Scraper for INTL/WorldBankDocs — World Bank Documents & Reports.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research project)",
            "Accept": "application/json",
        })

    def _fetch_page(self, doctype: str, offset: int = 0, since: Optional[datetime] = None) -> dict:
        """Fetch a single page of results from the API."""
        params = {
            "format": "json",
            "docty_exact": doctype,
            "rows": PAGE_SIZE,
            "os": offset,
            "fl": FIELDS,
            "srt": "docdt",
            "order": "desc",
        }
        if since:
            params["strdate"] = since.strftime("%Y-%m-%d")

        time.sleep(0.5)
        resp = self.session.get(API_URL, params=params, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _download_text(self, txturl: str) -> str:
        """Download full text from the World Bank text URL."""
        try:
            time.sleep(0.5)
            resp = self.session.get(txturl, timeout=60)
            if resp.status_code == 200:
                return resp.text.strip()
            logger.debug(f"Text download failed ({resp.status_code}): {txturl}")
        except requests.exceptions.RequestException as e:
            logger.debug(f"Text download error: {e}")
        return ""

    def _iter_documents(self, since: Optional[datetime] = None, max_per_type: int = 0) -> Generator[dict, None, None]:
        """Iterate over all legal documents across all types."""
        for doctype in LEGAL_DOC_TYPES:
            offset = 0
            type_count = 0
            logger.info(f"Fetching {doctype} documents...")

            while True:
                data = self._fetch_page(doctype, offset, since)
                total = data.get("total", 0)
                if total == 0:
                    break

                if offset == 0:
                    logger.info(f"  {doctype}: {total} total documents")

                docs = data.get("documents", {})
                page_docs = []
                for key, doc in docs.items():
                    if key == "facets" or not isinstance(doc, dict):
                        continue
                    page_docs.append(doc)

                if not page_docs:
                    break

                for doc in page_docs:
                    yield doc
                    type_count += 1
                    if max_per_type > 0 and type_count >= max_per_type:
                        break

                if max_per_type > 0 and type_count >= max_per_type:
                    break

                offset += len(page_docs)
                if offset >= total:
                    break

                if offset % 1000 == 0:
                    logger.info(f"  {doctype}: {offset}/{total}")

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all World Bank legal documents."""
        yield from self._iter_documents()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch documents added since the given date."""
        yield from self._iter_documents(since=since)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw API result into standardized schema."""
        doc_id = raw.get("id", "")
        if not doc_id:
            return None

        title = raw.get("display_title", "")
        if not title:
            return None

        # Download full text
        txturl = raw.get("txturl", "")
        text = ""
        if txturl:
            text = self._download_text(txturl)

        if not text or len(text) < 50:
            logger.debug(f"No/short text for {doc_id}, skipping")
            return None

        # Parse date
        date_str = raw.get("docdt", "")
        date = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        doctype = raw.get("docty", "")
        country = raw.get("count", "")
        project_id = raw.get("projectid", "")
        url = raw.get("url", "")
        if url and url.startswith("http://"):
            url = url.replace("http://", "https://", 1)

        return {
            "_id": f"WB-{doc_id}",
            "_source": "INTL/WorldBankDocs",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url or f"https://documents.worldbank.org/curated/en/{doc_id}",
            "document_type": doctype,
            "country": country,
            "project_id": project_id,
        }

    # ── CLI ────────────────────────────────────────────────────────────

    def run_sample(self, count: int = 15):
        """Fetch sample records for validation."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(parents=True, exist_ok=True)

        records = []
        seen_ids = set()

        # Get a few from each type
        per_type = max(3, count // len(LEGAL_DOC_TYPES) + 1)

        for raw in self._iter_documents(max_per_type=per_type):
            if len(records) >= count:
                break
            doc_id = raw.get("id", "")
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            record = self.normalize(raw)
            if record:
                records.append(record)
                logger.info(f"Sample {len(records)}/{count}: {record['document_type']} - {record['title'][:60]} ({len(record['text'])} chars)")

        # Save samples
        for i, record in enumerate(records):
            path = sample_dir / f"record_{i:04d}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)

        all_path = sample_dir / "all_samples.json"
        with open(all_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)

        return records

    def run_test(self):
        """Quick connectivity test."""
        logger.info("Testing World Bank Documents API...")

        data = self._fetch_page("Agreement", offset=0)
        total = data.get("total", 0)
        logger.info(f"API test: {total} Agreement documents found")

        docs = data.get("documents", {})
        for key, doc in docs.items():
            if key == "facets" or not isinstance(doc, dict):
                continue
            txturl = doc.get("txturl", "")
            if txturl:
                text = self._download_text(txturl)
                logger.info(f"Text test: {len(text)} chars from {doc.get('display_title', '')[:60]}")
                return total > 0 and len(text) > 100
            break

        return total > 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WorldBankDocs data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--count", type=int, default=15, help="Number of sample records")
    parser.add_argument("--since", type=str, help="Fetch updates since date (YYYY-MM-DD)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()
    scraper = WorldBankDocsScraper()

    if args.command == "test":
        success = scraper.run_test()
        return 0 if success else 1

    if args.command == "bootstrap":
        if args.sample:
            records = scraper.run_sample(args.count)
            logger.info(f"\nFetched {len(records)} sample records")

            texts = [r for r in records if len(r.get("text", "")) > 100]
            logger.info(f"Records with substantial text: {len(texts)}/{len(records)}")
            if texts:
                avg_len = sum(len(r["text"]) for r in texts) / len(texts)
                logger.info(f"Average text length: {avg_len:,.0f} chars")

            if len(records) >= 10 and len(texts) >= 10:
                logger.info("VALIDATION PASSED")
                return 0
            else:
                logger.error("VALIDATION FAILED - not enough records with text")
                return 1
        else:
            stats = scraper.bootstrap()
            logger.info(f"Bootstrap complete: {stats}")
            return 0

    if args.command == "update":
        if not args.since:
            logger.error("--since required for update")
            return 1
        since = datetime.fromisoformat(args.since)
        stats = scraper.bootstrap()
        logger.info(f"Update complete: {stats}")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
