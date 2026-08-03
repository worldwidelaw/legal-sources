#!/usr/bin/env python3
"""
US/DOI-IBLA-Orders -- U.S. DOI, Interior Board of Land Appeals: Dispositive Orders

Fetches the full text of the *dispositive orders* issued by the Interior Board
of Land Appeals (IBLA) -- the orders (as distinct from the merits decisions in
US/DOI-IBLA) that dispose of a docketed appeal without a full reported opinion:
dismissals for mootness, untimeliness, lack of standing or jurisdiction, orders
granting withdrawal or approving settlement, stay rulings, etc. Each order
resolves a specific docketed appeal (e.g. "IBLA 2021-123"), so the corpus is
`case_law`. IBLA orders are federal-government works in the public domain
(17 U.S.C. § 105).

Access (no JavaScript execution needed, no CAPTCHA, no auth):
  Interior's Office of Hearings and Appeals publishes the orders in a full-text
  ISYS / Perceptive Enterprise Search database reachable over plain HTTP GET at
  https://www.oha.doi.gov:8080/ -- the same portal and identical query flow as
  US/DOI-IBLA, but with IW_DATABASE="IBLA Dispositive Orders":

    1. GET /search/?IW_FIELD_NATURAL_LANGUAGE=<query>&IW_DATABASE=IBLA Dispositive Orders
       -> results page carrying a server-cached query GUID + document count.
    2. GET /isysquery/<GUID>/<start>-<end>/list/
       -> a page of hits; each hit N links its PDF at
          /isysquery/<GUID>/<N>/doc/<name>.pdf where the filename is the appeal
          docket (e.g. 2021-0123.pdf == "IBLA 2021-123").
    3. GET /isysquery/<GUID>/<N>/doc/<name>.pdf -> the born-digital order PDF.

  The GUID persists server-side so no cookie/session handling is needed and
  there is no stable direct PDF path.

Enumeration:
  The corpus is enumerated by running broad natural-language seed queries whose
  union matches essentially every order, paging each query's full hit list, and
  de-duplicating by the PDF filename (a stable unique id per order).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DOI-IBLA-Orders")

BASE_URL = "https://www.oha.doi.gov:8080"
DATABASE = "IBLA Dispositive Orders"

MIN_TEXT_CHARS = 200
PAGE = 50  # hits per /list/ request

BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Broad seed queries whose union covers the corpus; deduped by filename.
SEED_QUERIES = [
    "Bureau of Land Management",   # near-universal: IBLA reviews BLM actions
    "appeal dismissed",
    "motion withdraw",
    "moot untimely",
    "oil and gas lease",
    "mining claim",
    "grazing permit",
    "right of way",
    "stay pending appeal",
    "settlement remand",
]

HIT_RE = re.compile(
    r'/isysquery/([0-9a-fA-F-]{36})/(\d+)/doc/([0-9A-Za-z._-]+\.pdf)', re.I
)
GUID_RE = re.compile(r'/isysquery/([0-9a-fA-F-]{36})/', re.I)
DOCS_RE = re.compile(r'([0-9][0-9,]*)\s*documents', re.I)
HITS_RE = re.compile(r'([0-9][0-9,]*)\s*(?:hits|matches)', re.I)

# Docket parsed from the order body, e.g. "IBLA 2021-123".
DOCKET_RE = re.compile(r'\bIBLA\s+(\d{2,4}-\d+[A-Z0-9-]*)\b')
_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}
# Orders open with the decision date, e.g. "January 8, 2021".
DECIDED_RE = re.compile(
    r'Decided\s+(' + "|".join(_MONTHS) + r')\s+(\d{1,2}),?\s+(\d{4})', re.I
)
BARE_DATE_RE = re.compile(
    r'\b(' + "|".join(_MONTHS) + r')\s+(\d{1,2}),\s+(\d{4})\b', re.I
)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _parse_decided(text: str) -> str | None:
    head = text[:1500]
    m = DECIDED_RE.search(head) or BARE_DATE_RE.search(head)
    if not m:
        return None
    mon = int(_MONTHS[m.group(1).lower()])
    try:
        d, y = int(m.group(2)), int(m.group(3))
    except ValueError:
        return None
    if 1960 <= y <= 2100 and 1 <= mon <= 12 and 1 <= d <= 31:
        return f"{y}-{mon:02d}-{d:02d}"
    return None


class DOIIBLAOrdersScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get_text(self, url: str, retries: int = 3) -> str | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.text:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = (resp.headers.get("Content-Type") or "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF content ({ctype}) for {url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def _run_query(self, query: str) -> tuple[str | None, int]:
        url = (
            f"{BASE_URL}/search/?"
            + urllib.parse.urlencode({
                "IW_FIELD_NATURAL_LANGUAGE": query,
                "IW_DATABASE": DATABASE,
            })
        )
        html = self._get_text(url)
        if not html:
            return None, 0
        gm = GUID_RE.search(html)
        dm = DOCS_RE.search(html)
        hm = HITS_RE.search(html)
        guid = gm.group(1) if gm else None
        if dm:
            total = int(dm.group(1).replace(",", ""))
        elif hm:
            total = int(hm.group(1).replace(",", ""))
        else:
            total = 0
        return guid, total

    def _iter_hits(self, guid: str, total: int) -> Generator[tuple[int, str], None, None]:
        start = 1
        hard_cap = max(total, 0) + PAGE
        while start <= hard_cap:
            end = start + PAGE - 1
            url = f"{BASE_URL}/isysquery/{guid}/{start}-{end}/list/"
            html = self._get_text(url)
            if not html:
                break
            hits = [(int(n), fn) for (_g, n, fn) in HIT_RE.findall(html)]
            seen_ranks = set()
            page_hits = []
            for n, fn in hits:
                if start <= n <= end and n not in seen_ranks:
                    seen_ranks.add(n)
                    page_hits.append((n, fn))
            if not page_hits:
                break
            for n, fn in page_hits:
                yield n, fn
            start += PAGE

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        queries = SEED_QUERIES[:1] if sample else SEED_QUERIES
        total_yielded = 0
        for query in queries:
            guid, total = self._run_query(query)
            if not guid:
                logger.warning(f"No GUID for query {query!r}")
                continue
            logger.info(f"Query {query!r}: {total} documents (guid {guid})")
            for rank, fname in self._iter_hits(guid, total):
                stem = fname.rsplit(".", 1)[0].upper()
                if stem in seen:
                    continue
                seen.add(stem)
                doc_url = f"{BASE_URL}/isysquery/{guid}/{rank}/doc/{fname}"
                yield {
                    "file_id": stem,
                    "doc_url": doc_url,
                    "slug": re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")[:80],
                }
                total_yielded += 1
                if sample and total_yielded >= 30:
                    return
        logger.info(f"Discovered {len(seen)} unique IBLA orders")

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["doc_url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/DOI-IBLA-Orders",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        dm = DOCKET_RE.search(text[:2000])
        doc["docket"] = f"IBLA {dm.group(1)}" if dm else None
        doc["date"] = _parse_decided(text)
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing DOI IBLA Dispositive Orders (ISYS)...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No orders discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ orders (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('docket') or raw.get('file_id')}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        docket = raw.get("docket")
        caption = ""
        for line in (raw.get("text") or "").split("\n"):
            s = line.strip()
            if (s and not s.upper().startswith("UNITED STATES DEPARTMENT")
                    and not s.upper().startswith("INTERIOR BOARD")
                    and not BARE_DATE_RE.match(s)):
                caption = s
                break
        caption = caption[:160]
        label = docket or raw.get("file_id")
        if caption and label:
            title = f"{caption} ({label})"
        elif label:
            title = f"Interior Board of Land Appeals — Order {label}"
        else:
            title = "Interior Board of Land Appeals dispositive order"
        title = title[:300]
        return {
            "_id": f"US/DOI-IBLA-Orders/{raw['slug']}",
            "_source": "US/DOI-IBLA-Orders",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket,
            "file_id": raw.get("file_id"),
            "court": "Interior Board of Land Appeals",
            "document_type": "dispositive order",
            "title": title,
            "text": raw["text"],
            "url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 30:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/DOI-IBLA-Orders bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DOIIBLAOrdersScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
