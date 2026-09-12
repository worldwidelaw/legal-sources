#!/usr/bin/env python3
"""
JP/CourtsGoJp — Japanese Courts Case Law Database

Fetches official Japanese court decisions from courts.go.jp.

Strategy:
  - Enumerate the whole corpus from the search result tables (limit=1000 per
    page, ~50 requests for all ~50,000 cases).  Every field we need (case
    number, case name, judgment date, court, judgment type, PDF URL) is already
    in the result row, so the per-case detail page is never fetched.
  - normalize() downloads the judgment PDF and extracts its text.  Because the
    heavy work lives in normalize(), ``bootstrap_fast`` overlaps the PDF
    downloads across worker threads.
  - Incremental / resumable: every successfully processed case ID is appended to
    ``data/processed_ids.txt`` and the enumeration offset of each court is kept
    in ``data/checkpoint.json``.  A refresh re-walks only the (cheap) search
    pages and downloads PDFs for cases it has never seen, so it finishes in
    minutes instead of re-walking the full corpus (issue #1391).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Concurrent full pull (fleet entry point)
  python bootstrap.py update --since 2024-01-01
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import html as htmlmod
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JP.CourtsGoJp")

BASE_URL = "https://www.courts.go.jp"

# Court type search pages
# courtCaseType: 1=Supreme, 2=High, 3=District/Family/Summary
SEARCH_CONFIGS = [
    {"key": "supreme", "path": "/hanrei/search2/index.html", "court_type": "1", "court_name": "Supreme Court"},
    {"key": "high", "path": "/hanrei/search2/index.html", "court_type": "2", "court_name": "High Court"},
    {"key": "lower", "path": "/hanrei/search4/index.html", "court_type": "3", "court_name": "Lower Courts"},
]

# The search endpoint honours an arbitrary `limit`; 1000 rows/page keeps the
# whole enumeration at ~50 requests.
PAGE_SIZE = 1000

# The date filter is not a real filter (the backend ignores the range) but the
# endpoint only runs the query when both bounds are present.
DATE_FROM = "1947-01-01"
DATE_TO = "2099-12-31"

DATA_DIR = Path(__file__).parent / "data"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"
PROCESSED_IDS_PATH = DATA_DIR / "processed_ids.txt"

JUDGMENT_TYPES = {"判決", "決定", "命令", "審判", "和解", "但書判決"}


def _load_checkpoint() -> dict:
    """Load the enumeration checkpoint (per-court offset + done flag)."""
    try:
        with open(CHECKPOINT_PATH, encoding="utf-8") as fh:
            state = json.load(fh)
        if isinstance(state, dict) and isinstance(state.get("enum"), dict):
            return state
    except (OSError, ValueError):
        pass
    return {"enum": {}}


def _save_checkpoint(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
    tmp.replace(CHECKPOINT_PATH)


def _load_processed_ids() -> set:
    """Case IDs whose PDF has already been fetched and written."""
    try:
        with open(PROCESSED_IDS_PATH, encoding="utf-8") as fh:
            return {line.strip() for line in fh if line.strip()}
    except OSError:
        return set()


def _strip_tags(fragment: str) -> list[str]:
    """Turn an HTML fragment into a list of non-empty text lines."""
    text = re.sub(r"<[^>]+>", "\n", fragment)
    text = htmlmod.unescape(text)
    return [line.strip() for line in text.split("\n") if line.strip()]


class CourtsGoJpScraper(BaseScraper):
    """
    Scraper for JP/CourtsGoJp — Japanese Courts Case Law Database.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "ja,en;q=0.5",
        })
        self._processed = _load_processed_ids()
        self._processed_lock = threading.Lock()
        self._record_processed = True  # disabled in sample mode
        self._reconcile_processed()

    # ── Checkpoint bookkeeping ────────────────────────────────────────

    def _reconcile_processed(self) -> None:
        """
        Drop resume entries whose record never reached storage.

        ``bootstrap_fast`` writes in batches, so a kill can leave up to
        ``batch_size`` cases flagged as processed while their records were still
        in flight.  Re-syncing against the storage index on startup makes those
        cases eligible again instead of silently losing them.
        """
        index = getattr(self.storage, "_index", None)
        if index is None or not self._processed:
            return
        stored = {key.rsplit("/", 1)[-1] for key in index}
        stale = self._processed - stored
        if not stale:
            return
        logger.info(f"Re-queueing {len(stale)} case(s) marked processed but never stored")
        self._processed &= stored
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        with open(PROCESSED_IDS_PATH, "w", encoding="utf-8") as fh:
            for case_id in sorted(self._processed):
                fh.write(case_id + "\n")

    def _mark_processed(self, case_id: str) -> None:
        """Append a case ID to the resume log (thread-safe, crash-safe)."""
        if not self._record_processed:
            return
        with self._processed_lock:
            if case_id in self._processed:
                return
            self._processed.add(case_id)
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            with open(PROCESSED_IDS_PATH, "a", encoding="utf-8") as fh:
                fh.write(case_id + "\n")

    # Japanese era base years
    ERA_BASES = {
        "令和": 2018, "平成": 1988, "昭和": 1925, "大正": 1911, "明治": 1867,
    }

    def _parse_japanese_date(self, date_str: str) -> Optional[str]:
        """Parse Japanese era date like '令和8年2月24日' or Western '2024/1/15'."""
        # Try Japanese era format: 令和8年2月24日
        era_match = re.search(
            r'(令和|平成|昭和|大正|明治)(元|\d{1,2})年(\d{1,2})月(\d{1,2})日', date_str,
        )
        if era_match:
            era, year, month, day = era_match.groups()
            year = 1 if year == "元" else int(year)
            western_year = self.ERA_BASES[era] + year
            return f"{western_year}-{int(month):02d}-{int(day):02d}"
        # Try Western format: 2024年1月15日 or 2024/1/15
        western_match = re.search(r'(\d{4})[年/](\d{1,2})[月/](\d{1,2})', date_str)
        if western_match:
            return f"{western_match.group(1)}-{int(western_match.group(2)):02d}-{int(western_match.group(3)):02d}"
        return None

    def _extract_text_from_pdf(self, pdf_content: bytes) -> str:
        """Extract text from PDF using centralized extractor."""
        return extract_pdf_markdown(
            source="JP/CourtsGoJp",
            source_id="",
            pdf_bytes=pdf_content,
            table="case_law",
        ) or ""

    def _download_pdf(self, pdf_url: str, retries: int = 3) -> Optional[bytes]:
        """Download a case PDF."""
        for attempt in range(retries):
            try:
                time.sleep(0.5)
                resp = self.session.get(pdf_url, timeout=120)
                if resp.status_code == 200 and len(resp.content) > 500:
                    return resp.content
                if resp.status_code == 404:
                    logger.debug(f"No PDF at {pdf_url}")
                    return None
                logger.warning(f"HTTP {resp.status_code} for PDF {pdf_url}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"PDF download attempt {attempt + 1} failed: {e}")
                if attempt < retries - 1:
                    time.sleep(5 * (attempt + 1))
        return None

    # ── Search result parsing ─────────────────────────────────────────

    def _parse_rows(self, html: str, court_name: str) -> list[dict]:
        """
        Parse the search result table into case dicts.

        Each row already carries everything the record needs, so the per-case
        detail page (one extra request + sleep per case) is never fetched.
        """
        table_match = re.search(
            r'<table[^>]*search-result-table.*?</table>', html, re.DOTALL,
        )
        if not table_match:
            return []

        rows = []
        for tr in re.findall(r'<tr\b.*?</tr>', table_match.group(0), re.DOTALL):
            id_match = re.search(r'/(\d+)/(detail\d*)/index\.html', tr)
            pdf_match = re.search(r'href="([^"]*hanrei-pdf-\d+\.pdf)"', tr)
            if not id_match or not pdf_match:
                continue
            case_id, detail_seg = id_match.group(1), id_match.group(2)

            paragraphs = [
                _strip_tags(p) for p in re.findall(r'<p\b.*?</p>', tr, re.DOTALL)
            ]
            paragraphs = [p for p in paragraphs if p]

            case_number = case_name = ""
            if paragraphs:
                case_number = paragraphs[0][0] if paragraphs[0] else ""
                case_name = paragraphs[0][1] if len(paragraphs[0]) > 1 else ""

            date = court = judgment_type = result = ""
            extras: list[str] = []
            if len(paragraphs) > 1:
                lines = list(paragraphs[1])
                if lines:
                    date = self._parse_japanese_date(lines.pop(0)) or ""
                if lines:
                    court = lines.pop(0)
                # A branch office ("沼津支部") follows the court name.
                if lines and lines[0].endswith("支部"):
                    court = f"{court} {lines.pop(0)}"
                for i, line in enumerate(lines):
                    if line in JUDGMENT_TYPES and not judgment_type:
                        judgment_type = line
                        if i + 1 < len(lines):
                            result = lines[i + 1]
                    else:
                        extras.append(line)

            rows.append({
                "case_id": case_id,
                "detail_seg": detail_seg,
                "court_name": court or court_name,
                "collection": court_name,
                "case_number": case_number,
                "case_name": case_name,
                "date": date,
                "judgment_type": judgment_type,
                "result": result,
                "extras": extras,
                "pdf_url": f"{BASE_URL}/assets/hanrei/hanrei-pdf-{case_id}.pdf",
                "url": f"{BASE_URL}/hanrei/{case_id}/{detail_seg}/index.html",
            })
        return rows

    def _search_page(self, court_config: dict, offset: int,
                     limit: int = PAGE_SIZE) -> tuple[list[dict], int]:
        """Fetch one search page; return (rows, total_count)."""
        url = f"{BASE_URL}{court_config['path']}"
        params = {
            "courtCaseType": court_config["court_type"],
            "filter[judgeDateFrom]": DATE_FROM,
            "filter[judgeDateTo]": DATE_TO,
            "offset": str(offset),
            "limit": str(limit),
        }

        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, params=params, timeout=120)
                resp.encoding = "utf-8"
                if resp.status_code != 200:
                    logger.warning(f"Search returned HTTP {resp.status_code}")
                    continue
                html = resp.text
                total = 0
                count_match = re.search(r'(\d[\d,]*)件中', html)
                if count_match:
                    total = int(count_match.group(1).replace(",", ""))
                return self._parse_rows(html, court_config["court_name"]), total
            except requests.exceptions.RequestException as e:
                logger.warning(f"Search attempt {attempt + 1} failed: {e}")
                time.sleep(5 * (attempt + 1))
        return [], 0

    # ── Crawl ─────────────────────────────────────────────────────────

    def _enumerate(self, skip_processed: bool = True) -> Generator[dict, None, None]:
        """
        Walk every court's result set, yielding rows not yet processed.

        The offset of each court is checkpointed so an interrupted run resumes
        where it stopped.  Once every court is done, the next run starts a fresh
        pass from offset 0 (to pick up newly published judgments) — the
        processed-ID log keeps it from re-downloading anything already stored.
        """
        state = _load_checkpoint()
        enum_state = state["enum"]
        if enum_state and all(c.get("done") for c in enum_state.values()):
            logger.info("Previous enumeration finished — starting a fresh pass")
            enum_state = state["enum"] = {}

        for court_config in SEARCH_CONFIGS:
            key = court_config["key"]
            court_state = enum_state.setdefault(key, {"offset": 0, "total": 0, "done": False})
            if court_state.get("done"):
                logger.info(f"{key}: already enumerated in this pass — skipping")
                continue

            offset = int(court_state.get("offset", 0))
            logger.info(f"{court_config['court_name']}: enumerating from offset {offset}")

            while True:
                rows, total = self._search_page(court_config, offset, limit=PAGE_SIZE)
                if total:
                    court_state["total"] = total
                if not rows:
                    court_state["done"] = True
                    _save_checkpoint(state)
                    break

                new = 0
                for row in rows:
                    if skip_processed and row["case_id"] in self._processed:
                        continue
                    new += 1
                    yield row

                offset += PAGE_SIZE
                court_state["offset"] = offset
                if total and offset >= total:
                    court_state["done"] = True
                _save_checkpoint(state)
                logger.info(
                    f"{court_config['court_name']}: offset {offset}/{court_state['total']} "
                    f"({new} new on this page)"
                )
                if court_state.get("done"):
                    break

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all Japanese court cases (metadata only; normalize pulls the PDF)."""
        yield from self._enumerate(skip_processed=True)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Fetch cases published since a given date.

        Results are ordered newest-first, so the walk stops once a full page is
        older than `since`.
        """
        cutoff = since.date().isoformat()
        for court_config in SEARCH_CONFIGS:
            offset = 0
            while True:
                rows, total = self._search_page(court_config, offset, limit=200)
                if not rows:
                    break
                fresh = [r for r in rows if not r["date"] or r["date"] >= cutoff]
                for row in fresh:
                    if row["case_id"] in self._processed:
                        continue
                    yield row
                if len(fresh) < len(rows):
                    break  # reached judgments older than the cutoff
                offset += 200
                if total and offset >= total:
                    break

    def normalize(self, raw: dict) -> Optional[dict]:
        """Download the judgment PDF and build the standardized record."""
        case_id = raw["case_id"]

        pdf_bytes = self._download_pdf(raw["pdf_url"])
        if not pdf_bytes:
            return None

        text = self._extract_text_from_pdf(pdf_bytes)
        if not text or len(text) < 50:
            logger.warning(f"Insufficient text for case {case_id}")
            return None

        title = raw.get("case_name") or raw.get("case_number") or f"Case {case_id}"

        record = {
            "_id": f"JP/CourtsGoJp/{case_id}",
            "_source": "JP/CourtsGoJp",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date") or None,
            "url": raw["url"],
            "case_number": raw.get("case_number", ""),
            "court": raw.get("court_name", ""),
            "judgment_type": raw.get("judgment_type", ""),
            "result": raw.get("result", ""),
            "collection": raw.get("collection", ""),
            "pdf_url": raw["pdf_url"],
        }
        self._mark_processed(case_id)
        return record


# ── CLI entry point ──────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="JP/CourtsGoJp bootstrap")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    boot_parser = subparsers.add_parser("bootstrap", help="Full bootstrap or sample")
    boot_parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    boot_parser.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    boot_parser.add_argument("--full", action="store_true", help="Fetch all records (default)")

    fast_parser = subparsers.add_parser("bootstrap-fast", help="Concurrent full bootstrap")
    fast_parser.add_argument("--full", action="store_true", help="Fetch all records (default)")
    fast_parser.add_argument("--max-workers", type=int, default=None, help="Concurrent download threads")
    fast_parser.add_argument("--batch-size", type=int, default=50, help="Records per batch write")

    update_parser = subparsers.add_parser("update", help="Incremental update")
    update_parser.add_argument("--since", required=True, help="ISO date (e.g. 2024-01-01)")
    update_parser.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    scraper = CourtsGoJpScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        rows, total = scraper._search_page(SEARCH_CONFIGS[0], 0, limit=20)
        logger.info(f"Search page: {len(rows)} rows parsed, {total} total cases")
        if rows:
            logger.info(f"First row: {json.dumps(rows[0], ensure_ascii=False)}")
            record = scraper.normalize(rows[0])
            logger.info(f"PDF text: {len(record['text']) if record else 0} chars")
        logger.info("Connectivity test passed!")

    elif args.command == "bootstrap":
        if args.sample:
            scraper._record_processed = False
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "bootstrap-fast":
        kwargs = {"batch_size": args.batch_size}
        if args.max_workers:
            kwargs["max_workers"] = args.max_workers
        stats = scraper.bootstrap_fast(**kwargs)
        logger.info(f"Bootstrap-fast complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        since = datetime.fromisoformat(args.since).replace(tzinfo=timezone.utc)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
