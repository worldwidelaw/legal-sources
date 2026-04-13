#!/usr/bin/env python3
"""
INTL/WorldBank-Documents -- World Bank Open Knowledge Repository

Fetches World Bank legal/governance documents with full text from the
WDS Search API v2, filtered by the "Law and Development" topic.

Strategy:
  - GET /api/v2/wds with teratopic_exact=Law and Development
  - Paginate with rows=200 and os= offset
  - For each document, fetch full text from the txturl field
  - ~40,000 documents available, no authentication required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recently updated documents
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WorldBank-Documents")

API_URL = "https://search.worldbank.org/api/v2/wds"
PAGE_SIZE = 200
TOPIC = "Law and Development"

FIELDS = ",".join([
    "id", "display_title", "docdt", "docty", "pdfurl", "txturl",
    "abstracts", "count", "lang", "teratopic", "subtopic",
    "repnb", "repnme", "keywd", "guid",
])


class WorldBankDocumentsScraper(BaseScraper):
    SOURCE_ID = "INTL/WorldBank-Documents"

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; open data)",
            "Accept": "application/json",
        })

    def _search(self, offset: int = 0, rows: int = PAGE_SIZE,
                extra_params: Optional[Dict] = None) -> Dict:
        params = {
            "format": "json",
            "teratopic_exact": TOPIC,
            "fl": FIELDS,
            "rows": rows,
            "os": offset,
            "srt": "docdt",
            "order": "desc",
            "lang_exact": "English",
        }
        if extra_params:
            params.update(extra_params)

        for attempt in range(3):
            try:
                resp = self.session.get(API_URL, params=params, timeout=60)
                resp.raise_for_status()
                return resp.json()
            except (requests.RequestException, ValueError) as e:
                if attempt == 2:
                    raise
                logger.warning("Attempt %d failed: %s", attempt + 1, e)
                time.sleep(2 * (attempt + 1))

    def _fetch_full_text(self, txturl: str) -> str:
        if not txturl:
            return ""
        for attempt in range(3):
            try:
                resp = self.session.get(txturl, timeout=60, allow_redirects=True)
                resp.raise_for_status()
                text = resp.text.strip()
                # Filter out error pages from the text extraction service
                if "The system was not able to generate" in text:
                    return ""
                # Clean up excessive whitespace
                text = re.sub(r"\n{3,}", "\n\n", text)
                return text
            except requests.RequestException as e:
                if attempt == 2:
                    logger.warning("Failed to fetch text from %s: %s", txturl, e)
                    return ""
                time.sleep(2 * (attempt + 1))

    def test_connection(self) -> bool:
        try:
            data = self._search(rows=1)
            total = data.get("total", 0)
            logger.info("Connection OK: %d total Law & Development documents", total)
            return total > 0
        except Exception as e:
            logger.error("Connection failed: %s", e)
            return False

    def fetch_all(self) -> Generator[Dict, None, None]:
        logger.info("Fetching World Bank Law & Development documents...")
        first_page = self._search(offset=0)
        total = first_page.get("total", 0)
        logger.info("Total documents: %d", total)

        offset = 0
        while offset < total:
            if offset == 0:
                data = first_page
            else:
                self.rate_limiter.wait()
                data = self._search(offset=offset)

            docs = data.get("documents", {})
            page_count = 0
            for key, doc in docs.items():
                if key == "facets" or not isinstance(doc, dict):
                    continue
                yield doc
                page_count += 1

            logger.info("Fetched offset %d: %d docs (total %d)", offset, page_count, total)
            if page_count == 0:
                break
            offset += PAGE_SIZE

    def fetch_updates(self, since: datetime) -> Generator[Dict, None, None]:
        since_str = since.strftime("%Y-%m-%d")
        logger.info("Fetching documents updated since %s", since_str)
        offset = 0
        while True:
            self.rate_limiter.wait()
            data = self._search(offset=offset, extra_params={
                "strdate": since_str,
            })
            docs = data.get("documents", {})
            page_count = 0
            for key, doc in docs.items():
                if key == "facets" or not isinstance(doc, dict):
                    continue
                yield doc
                page_count += 1
            if page_count == 0:
                break
            offset += PAGE_SIZE

    def normalize(self, raw: dict) -> dict:
        doc_id = raw.get("id", "")
        title = raw.get("display_title", "").strip()
        # Clean up multi-line whitespace in title
        title = re.sub(r"\s+", " ", title).strip()

        date_str = None
        docdt = raw.get("docdt", "")
        if docdt:
            try:
                dt = datetime.fromisoformat(docdt.replace("Z", "+00:00"))
                date_str = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                pass

        abstracts = raw.get("abstracts", "")
        if isinstance(abstracts, dict):
            abstracts = abstracts.get("cdata!", "")
        if abstracts:
            abstracts = re.sub(r"\s+", " ", abstracts).strip()

        full_text = raw.get("_full_text", "")

        country = raw.get("count", "")
        if isinstance(country, list):
            country = ", ".join(country)

        return {
            "_id": f"WB-{doc_id}",
            "_source": "INTL/WorldBank-Documents",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "abstract": abstracts,
            "date": date_str,
            "url": raw.get("pdfurl", ""),
            "document_type": raw.get("docty", ""),
            "country": country,
            "language": raw.get("lang", ""),
            "topics": raw.get("teratopic", ""),
            "subtopics": raw.get("subtopic", ""),
            "keywords": raw.get("keywd", ""),
            "report_number": raw.get("repnb", ""),
        }

    def run_bootstrap(self, sample: bool = False):
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        label = "SAMPLE" if sample else "FULL"
        logger.info("Running %s bootstrap", label)

        count = 0
        with_text = 0
        for raw in self.fetch_all():
            # Fetch full text from txturl
            txturl = raw.get("txturl", "")
            self.rate_limiter.wait()
            raw["_full_text"] = self._fetch_full_text(txturl)

            normalized = self.normalize(raw)

            fname = re.sub(r'[^\w\-.]', '_', f"{normalized['_id'][:80]}.json")
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            count += 1

            text_len = len(normalized.get("text", ""))
            if text_len > 0:
                with_text += 1
            logger.info("  [%d] %s -> %d chars", count, normalized["title"][:60], text_len)

            if sample and count >= 15:
                break

        logger.info("%s bootstrap complete: %d records saved, %d with full text",
                    label, count, with_text)
        return count


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/WorldBank-Documents Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    args = parser.parse_args()

    scraper = WorldBankDocumentsScraper()

    if args.command == "test":
        ok = scraper.test_connection()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.run_bootstrap(sample=args.sample)
    elif args.command == "update":
        count = 0
        for raw in scraper.fetch_updates(since=datetime.now(timezone.utc)):
            scraper.normalize(raw)
            count += 1
        logger.info("Update complete: %d records", count)


if __name__ == "__main__":
    main()
