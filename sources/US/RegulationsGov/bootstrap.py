#!/usr/bin/env python3
"""
US/RegulationsGov -- Federal Rulemaking Documents from the Federal Register

Fetches the full text of every federal Rule, Proposed Rule, and Notice published
in the Federal Register via the free Federal Register API v1 (no API key required).

Previously this source listed documents through the Regulations.gov API v4, which
under the shared DEMO_KEY is capped at ~25 requests/hour — so a full run only ever
pulled the first page (25 records, see issue #944). The full text of every
rulemaking document already lives in the Federal Register, whose API is free,
unauthenticated, and covers the complete digital archive back to 1994. Listing and
full-text retrieval are now both done against the Federal Register, removing the
rate-limit bottleneck entirely.

Document counts (Federal Register): ~230K rules, ~120K proposed rules, ~600K+
notices since 1994.

Data access:
  - Federal Register API: /api/v1/documents.json
      Paginated listing filtered by document type and publication-date window.
      No key needed. Deep pagination is capped at 2,000 results per query, so we
      iterate in short date windows that stay well under that cap.
  - Full text: each document exposes raw_text_url (preferred) and body_html_url.

Usage:
  python bootstrap.py bootstrap          # Full pull (newest-first, back to 1994)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Full pull, stream to data/records.jsonl
  python bootstrap.py update --since DATE # Incremental (newest first)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from typing import Generator, Optional, Dict, Any

import requests

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.RegulationsGov")

SOURCE_ID = "US/RegulationsGov"
FR_API = "https://www.federalregister.gov/api/v1"
DELAY = 0.3
PAGE_SIZE = 1000
WINDOW_DAYS = 5          # short enough to stay under the 2,000-result page cap
ARCHIVE_START = date(1994, 1, 1)   # Federal Register digital full-text archive start

# Federal Register document types -> our normalized _type.
# Rules and proposed rules are regulations (legislation); notices and presidential
# documents are official state writing that is not itself binding law (doctrine).
FR_TYPES = {
    "RULE": "legislation",
    "PRORULE": "legislation",
    "NOTICE": "doctrine",
    "PRESDOCU": "doctrine",
}
LIST_FIELDS = [
    "document_number", "title", "type", "publication_date", "html_url",
    "raw_text_url", "body_html_url", "agencies", "abstract",
]


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
        "Accept": "application/json",
    })
    return session


def clean_html(html: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    if not html:
        return ""
    if HAS_BS4:
        soup = BeautifulSoup(html, "html.parser")
        return soup.get_text(separator="\n", strip=True)
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class RegulationsGovScraper:
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        self.session = get_session()

    def _get(self, url: str, params: Optional[Dict] = None, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(4):
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                # 500/502/504 are the same transient gateway failure as the 503
                # already handled here; letting them fall through to the
                # `return None` below silently drops a whole page of documents
                # on one blip (#1453 class).
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = 10 * (attempt + 1)
                    logger.warning("HTTP %d, waiting %ds...", resp.status_code, wait)
                    time.sleep(wait)
                    continue
                if resp.status_code == 404:
                    return None
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return None
            except requests.RequestException as e:
                logger.warning("Request error (attempt %d): %s", attempt + 1, e)
                time.sleep(5)
        return None

    def list_window(self, start: date, end: date, page: int) -> Dict[str, Any]:
        """List documents published within [start, end] (inclusive), one page."""
        params = {
            "per_page": PAGE_SIZE,
            "page": page,
            "order": "newest",
            "conditions[type][]": list(FR_TYPES.keys()),
            "conditions[publication_date][gte]": start.isoformat(),
            "conditions[publication_date][lte]": end.isoformat(),
            "fields[]": LIST_FIELDS,
        }
        resp = self._get(f"{FR_API}/documents.json", params=params)
        if not resp:
            return {"results": [], "total_pages": 0, "count": 0}
        try:
            return resp.json()
        except ValueError:
            return {"results": [], "total_pages": 0, "count": 0}

    def get_full_text(self, doc: Dict[str, Any]) -> Optional[str]:
        """Fetch full text for a document, preferring raw text over HTML body."""
        raw_url = doc.get("raw_text_url")
        if raw_url:
            resp = self._get(raw_url, timeout=120)
            if resp:
                text = clean_html(resp.text)
                if len(text) > 100:
                    return text

        html_url = doc.get("body_html_url")
        if html_url:
            time.sleep(DELAY)
            resp = self._get(html_url, timeout=120)
            if resp:
                text = clean_html(resp.text)
                if len(text) > 100:
                    return text

        return None

    def normalize(self, doc: Dict[str, Any], full_text: str) -> Dict[str, Any]:
        """Normalize a Federal Register document into the standard schema."""
        doc_num = doc.get("document_number", "")
        fr_type = (doc.get("type") or "").strip()
        # Map the human-readable type back to our taxonomy bucket.
        type_code = {
            "Rule": "RULE", "Proposed Rule": "PRORULE",
            "Notice": "NOTICE", "Presidential Document": "PRESDOCU",
        }.get(fr_type, "NOTICE")

        agencies = [
            a.get("name") for a in (doc.get("agencies") or []) if a.get("name")
        ]

        pub_date = doc.get("publication_date") or None

        return {
            "_id": doc_num,
            "_source": SOURCE_ID,
            "_type": FR_TYPES.get(type_code, "doctrine"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": doc.get("title", ""),
            "text": full_text,
            "date": pub_date,
            "url": doc.get("html_url") or f"https://www.federalregister.gov/d/{doc_num}",
            "document_type": fr_type,
            "agencies": agencies or None,
            "abstract": doc.get("abstract") or None,
        }

    def _iter_windows(self, until: date) -> Generator[tuple, None, None]:
        """Yield (start, end) date windows from today back to `until`, newest first."""
        end = datetime.now(timezone.utc).date()
        while end >= until:
            start = end - timedelta(days=WINDOW_DAYS - 1)
            if start < until:
                start = until
            yield start, end
            end = start - timedelta(days=1)

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Yield all documents with full text, newest first, back to 1994."""
        target = 15 if sample else float("inf")
        until = ARCHIVE_START
        yielded = 0

        for start, end in self._iter_windows(until):
            if yielded >= target:
                break
            page = 1
            while yielded < target:
                time.sleep(DELAY)
                result = self.list_window(start, end, page)
                docs = result.get("results", [])
                total_pages = result.get("total_pages", 0) or 0
                if not docs:
                    break

                for doc in docs:
                    if yielded >= target:
                        break
                    doc_num = doc.get("document_number")
                    if not doc_num:
                        continue
                    time.sleep(DELAY)
                    full_text = self.get_full_text(doc)
                    if not full_text or len(full_text) < 100:
                        logger.debug("No full text for %s", doc_num)
                        continue
                    record = self.normalize(doc, full_text)
                    yielded += 1
                    logger.info(
                        "[%d] %s %s: %s (%d chars)",
                        yielded, doc.get("type", ""), doc_num,
                        (record["title"] or "")[:50], len(full_text),
                    )
                    yield record

                if page >= total_pages:
                    break
                page += 1

        logger.info("Total records yielded: %d", yielded)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Yield documents published on or after `since` (YYYY-MM-DD)."""
        try:
            until = datetime.strptime(since, "%Y-%m-%d").date()
        except ValueError:
            until = datetime.now(timezone.utc).date() - timedelta(days=30)

        for start, end in self._iter_windows(until):
            page = 1
            while True:
                time.sleep(DELAY)
                result = self.list_window(start, end, page)
                docs = result.get("results", [])
                total_pages = result.get("total_pages", 0) or 0
                if not docs:
                    break
                for doc in docs:
                    pub = doc.get("publication_date") or ""
                    if pub and pub < since:
                        continue
                    doc_num = doc.get("document_number")
                    if not doc_num:
                        continue
                    time.sleep(DELAY)
                    full_text = self.get_full_text(doc)
                    if not full_text or len(full_text) < 100:
                        continue
                    yield self.normalize(doc, full_text)
                if page >= total_pages:
                    break
                page += 1

    def test(self) -> bool:
        """Quick connectivity test."""
        logger.info("Testing Federal Register API...")
        today = datetime.now(timezone.utc).date()
        result = self.list_window(today - timedelta(days=14), today, 1)
        docs = result.get("results", [])
        if not docs:
            logger.error("No documents returned from Federal Register API")
            return False
        logger.info("API OK: %d documents in window (count=%s)", len(docs), result.get("count"))
        text = self.get_full_text(docs[0])
        if text and len(text) > 100:
            logger.info("Full text OK: %d chars from %s", len(text), docs[0].get("document_number"))
            return True
        logger.error("Failed to get full text")
        return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/RegulationsGov data fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only ~15 sample records")
    parser.add_argument("--since", type=str, default=None,
                        help="ISO date for incremental updates (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSONL file path")
    args = parser.parse_args()

    scraper = RegulationsGovScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    base = Path(__file__).parent
    sample_dir = base / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "update":
        since = args.since or "2024-01-01"
        records = scraper.fetch_updates(since)
        outfile = open(args.output, "w") if args.output else None
    else:
        # bootstrap / bootstrap-fast
        records = scraper.fetch_all(sample=args.sample)
        if args.command == "bootstrap-fast":
            data_dir = base / "data"
            data_dir.mkdir(exist_ok=True)
            outfile = open(args.output or (data_dir / "records.jsonl"), "w", encoding="utf-8")
        else:
            outfile = open(args.output, "w", encoding="utf-8") if args.output else None

    count = 0
    for record in records:
        count += 1
        if args.sample or count <= 15:
            safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])
            with open(sample_dir / f"{safe_id}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)
        if outfile:
            outfile.write(json.dumps(record, ensure_ascii=False) + "\n")
            outfile.flush()

    if outfile:
        outfile.close()

    logger.info("Done. %d records processed.", count)
    if count == 0:
        logger.error("No records fetched — check Federal Register API connectivity")
        sys.exit(1)


if __name__ == "__main__":
    main()
