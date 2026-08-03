#!/usr/bin/env python3
"""
US/AR-AGOpinions -- Arkansas Attorney General Opinions

Fetches the full text of official legal opinions issued by the Arkansas
Office of the Attorney General. Each opinion answers a legal question
posed by a public official (legislator, agency head, prosecutor, etc.)
and is an authoritative (advisory) interpretation of Arkansas law —
classified as doctrine.

Strategy:
  The official opinions search at arkansasag.gov embeds a React SPA
  (prod.opinions-search.arkansasag.gov) backed by a public Meilisearch
  index. The index `opinions` (primary key OpenFileNo) stores every
  opinion with a `FullText` field — no per-document PDF download or HTML
  scraping is required.

  1. Page through GET /indexes/opinions/documents?limit=N&offset=M using
     the public read-only search key embedded in the SPA bundle.
  2. Each document already carries FullText, OpinionSummary, requestor,
     publication date and a PDF URL.
  3. Normalize into the standard doctrine schema (text = FullText).

Usage:
  python bootstrap.py bootstrap            # Full pull (all opinions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AR-AGOpinions")

# Public Meilisearch backend powering the official opinions search SPA at
# arkansasag.gov (iframe -> prod.opinions-search.arkansasag.gov). The key
# below is the read-only search key shipped in the public SPA bundle.
MEILI_HOST = "https://ms-20efc50b2abc-32991.sfo.meilisearch.io"
MEILI_KEY = "cb51b2e810cd6a29268206daf4f2f0db9fabeefa"
INDEX = "opinions"
PAGE_SIZE = 500
PUBLIC_URL = "https://arkansasag.gov/divisions/opinions-foia/attorney-general-opinions-search/"


def clean_text(text: str) -> str:
    """Normalize whitespace in the opinion body."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_date(raw: str) -> str | None:
    """Convert a Meili '2003-01-10T00:00:00.0000000' value -> YYYY-MM-DD."""
    raw = (raw or "").strip()
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m and m.group(1) != "0001":
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return None


class ARAGOpinionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open-data research project; +https://github.com/worldwidelaw/legal-sources)",
                "Authorization": f"Bearer {MEILI_KEY}",
                "Accept": "application/json",
            },
            timeout=60,
        )
        self.delay = 0.5

    def _get_json(self, url: str, retries: int = 4) -> dict | None:
        """Fetch a JSON URL with rate limiting and retry/backoff."""
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.json()
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _documents_url(self, offset: int, limit: int) -> str:
        return f"{MEILI_HOST}/indexes/{INDEX}/documents?limit={limit}&offset={offset}"

    def test_api(self) -> bool:
        """Test connectivity and document structure of the Meili index."""
        logger.info("Testing Arkansas AG opinions Meilisearch index...")
        try:
            data = self._get_json(self._documents_url(0, 3))
            if not data or not data.get("results"):
                logger.error("  Documents endpoint returned no results")
                return False
            total = data.get("total")
            logger.info(f"  Documents endpoint OK ({total} total opinions)")
            doc = data["results"][0]
            text = clean_text(doc.get("FullText") or "")
            if text and len(text) > 200:
                logger.info(
                    f"  Full text OK ({len(text)} chars, OpenFileNo "
                    f"{doc.get('OpenFileNo')})"
                )
            else:
                logger.error("  FullText field missing or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Page through the Meili documents endpoint, newest first by storage."""
        emitted = 0
        offset = 0
        seen = set()
        while True:
            data = self._get_json(self._documents_url(offset, PAGE_SIZE))
            if not data:
                logger.warning(f"No data at offset {offset}; stopping")
                break
            results = data.get("results") or []
            if not results:
                break
            total = data.get("total")
            logger.info(f"Offset {offset}: {len(results)} docs (total {total})")
            for doc in results:
                ofn = (doc.get("OpenFileNo") or "").strip()
                if not ofn or ofn in seen:
                    continue
                seen.add(ofn)
                text = clean_text(doc.get("FullText") or "")
                if not text or len(text) < 100:
                    # Skip metadata-only rows (e.g. opinions with no usable
                    # full-text layer); we require real body text.
                    continue
                doc["_clean_text"] = text
                yield doc
                emitted += 1
                if sample and emitted >= 12:
                    return
            offset += PAGE_SIZE
            if total is not None and offset >= total:
                break

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw Meili document into the standard doctrine schema."""
        ofn = (raw.get("OpenFileNo") or "").strip()
        requestor = (raw.get("RequestorNameFull") or "").strip()
        summary = (raw.get("OpinionSummary") or "").strip()
        title = f"Arkansas AG Opinion {ofn}"
        if requestor:
            title = f"{title} — request of {requestor}"
        text = raw.get("_clean_text") or clean_text(raw.get("FullText") or "")
        return {
            "_id": f"US/AR-AGOpinions/{ofn}",
            "_source": "US/AR-AGOpinions",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "opinion_number": ofn,
            "title": title,
            "text": text,
            "summary": summary or None,
            "requestor": requestor or None,
            "date": parse_date(raw.get("Date_Published") or ""),
            "url": raw.get("PDFUrl") or PUBLIC_URL,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_docs(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield opinions published on/after `since` (ISO date)."""
        for raw in self.fetch_all():
            d = parse_date(raw.get("Date_Published") or "")
            if not since or (d and d >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/AR-AGOpinions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ARAGOpinionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
