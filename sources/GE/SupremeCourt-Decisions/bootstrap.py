#!/usr/bin/env python3
"""
GE/SupremeCourt-Decisions -- Georgia Supreme Court Data Fetcher

Fetches case law from Georgia's Supreme Court via AJAX endpoints.

Strategy:
  - Bootstrap: Paginates through all cases from /ka/getCases, then fetches
    full text for each case from /fullcase/{id}/{palata}.
  - Update: Fetches recent pages and stops when reaching old cases.
  - Sample: Fetches cases from different pages and palatas.

API: https://www.supremecourt.ge/ka/getCases (HTML listing)
     https://www.supremecourt.ge/fullcase/{id}/{palata} (full text AJAX)
Website: https://www.supremecourt.ge

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py update               # Incremental update
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from html import unescape

import requests

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GE.supremecourt")

BASE_URL = "https://www.supremecourt.ge"

# Palata IDs: 0=administrative, 1=civil, 2=criminal
PALATAS = {
    0: "ადმინისტრაციული (Administrative)",
    1: "სამოქალაქო (Civil)",
    2: "სისხლის სამართლის (Criminal)",
}

PAGE_SIZE = 30

# supremecourt.ge runs a Laravel `throttle` middleware advertising
# X-RateLimit-Limit: 10 (per minute) with a real Retry-After header. Once the
# bucket is empty the AJAX routes answer 429 ("Too Many Attempts.") and
# /fullcase/ additionally falls back to rendering the full page under a 403.
# Both are throttle signals, not end-of-data (issue #1158).
THROTTLE_STATUSES = (403, 429)
MIN_REQUEST_INTERVAL = 6.5   # ~9.2 req/min, just under the advertised 10/min
MAX_REQUEST_INTERVAL = 60.0
REQUEST_ATTEMPTS = 8
MAX_BACKOFF = 180.0


class ThrottledOut(RuntimeError):
    """Raised when a request could not be completed within REQUEST_ATTEMPTS.

    Deliberately fatal: the previous code returned "" here and the caller read
    that as "this palata has no more pages", turning a rate-limit wall into a
    clean-looking 60-record run (#1158).
    """


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean text."""
    if not html_text:
        return ""
    text = re.sub(r"<style[^>]*>.*?</style>", "", html_text, flags=re.DOTALL)
    text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\xa0", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def parse_case_listing(html: str) -> list:
    """Parse the HTML listing from getCases into case records."""
    cases = []
    # Each case is in a <div class="cases clearfix"> block
    blocks = re.split(r'<div class="cases clearfix">', html)

    for block in blocks[1:]:  # Skip first (before first case)
        case = {}

        # Case number
        m = re.search(r"საქმის ნომერი:</span>\s*(.+?)\s*</div>", block)
        if m:
            case["case_number"] = m.group(1).strip()

        # Date
        m = re.search(r"თარიღი:</span>\s*(\d{4}-\d{2}-\d{2})", block)
        if m:
            case["date"] = m.group(1)

        # Subject
        m = re.search(r"დავის საგანი:</span>\s*(.*?)</span>", block, re.DOTALL)
        if m:
            case["subject"] = strip_html(m.group(1)).strip()

        # Result
        m = re.search(r"შედეგი:</span>\s*(.*?)</div>", block)
        if m:
            case["result"] = strip_html(m.group(1)).strip()

        # Appeal type
        m = re.search(r"საჩივრის სახე:</span>\s*(.*?)</div>", block, re.DOTALL)
        if m:
            case["appeal_type"] = strip_html(m.group(1)).strip()

        # Extract ID and palata from fullcase link
        m = re.search(r'href="/ka/fullcase/(\d+)/(\d+)"', block)
        if m:
            case["id"] = int(m.group(1))
            case["palata"] = int(m.group(2))

        if case.get("id"):
            cases.append(case)

    return cases


class SupremeCourtScraper(BaseScraper):
    """
    Scraper for GE/SupremeCourt-Decisions -- Georgia Supreme Court.
    Country: GE
    URL: https://www.supremecourt.ge

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "X-Requested-With": "XMLHttpRequest",
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })
        # Self-tuning pace: starts at the advertised ceiling and slows on every
        # throttle signal, relaxing again only after sustained success.
        self._interval = MIN_REQUEST_INTERVAL
        self._last_request = 0.0
        self._consecutive_ok = 0
        self._throttle_hits = 0
        self._checkpoint_path = Path(self.source_dir) / "data" / "checkpoint.json"

    # ---------------------------------------------------------------- HTTP --

    def _pace(self):
        """Sleep so consecutive requests stay at least self._interval apart."""
        gap = time.monotonic() - self._last_request
        if gap < self._interval:
            time.sleep(self._interval - gap)
        self._last_request = time.monotonic()

    def _on_throttle(self, retry_after: Optional[float]):
        self._throttle_hits += 1
        self._consecutive_ok = 0
        self._interval = min(self._interval * 1.5, MAX_REQUEST_INTERVAL)
        logger.warning(
            f"Throttled by supremecourt.ge — slowing to {self._interval:.1f}s/request"
            + (f", honoring Retry-After={retry_after}s" if retry_after else "")
        )
        time.sleep(retry_after if retry_after else self._interval)

    def _on_success(self):
        self._consecutive_ok += 1
        if self._consecutive_ok >= 50 and self._interval > MIN_REQUEST_INTERVAL:
            self._interval = max(self._interval / 1.2, MIN_REQUEST_INTERVAL)
            self._consecutive_ok = 0
            logger.info(f"Sustained success — easing to {self._interval:.1f}s/request")

    def _request(self, url: str, params: dict, timeout: int, what: str) -> str:
        """GET with throttle-aware backoff. Raises ThrottledOut on exhaustion.

        Never returns an empty string for a *failed* request — callers rely on
        "empty body" meaning "the server really has nothing here".
        """
        backoff = 5.0
        for attempt in range(1, REQUEST_ATTEMPTS + 1):
            self._pace()
            try:
                resp = self.session.get(url, params=params, timeout=timeout)
            except requests.RequestException as e:
                logger.warning(f"{what}: network error (attempt {attempt}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
                continue

            if resp.status_code == 200:
                self._on_success()
                return resp.text

            if resp.status_code in THROTTLE_STATUSES:
                retry_after = resp.headers.get("Retry-After")
                try:
                    retry_after = float(retry_after) if retry_after else None
                except ValueError:
                    retry_after = None
                logger.warning(
                    f"{what}: HTTP {resp.status_code} throttle (attempt {attempt})"
                )
                self._on_throttle(retry_after)
                continue

            logger.warning(f"{what}: HTTP {resp.status_code} (attempt {attempt})")
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF)

        raise ThrottledOut(
            f"{what}: giving up after {REQUEST_ATTEMPTS} attempts "
            f"({self._throttle_hits} throttle hits so far). Refusing to report "
            f"this as end-of-data — see issue #1158."
        )

    def _get_cases_page(self, palata: int = 1, page: int = 1) -> str:
        """Fetch a page of case listings. Raises ThrottledOut if unreachable."""
        return self._request(
            f"{BASE_URL}/ka/getCases",
            {"palata": palata, "page": page},
            timeout=30,
            what=f"getCases(palata={palata}, page={page})",
        )

    def _get_full_case(self, case_id: int, palata: int) -> Optional[str]:
        """Fetch full text of a case. Returns None only if genuinely empty."""
        try:
            html = self._request(
                f"{BASE_URL}/fullcase/{case_id}/{palata}",
                {"action": "js", "id": case_id, "fulltext": ""},
                timeout=60,
                what=f"fullcase(id={case_id})",
            )
        except ThrottledOut as e:
            # One unreachable document must not abort a multi-day crawl, but it
            # is an error, not an empty decision.
            logger.error(str(e))
            return None
        if len(html) <= 100:
            return None
        return strip_html(html)

    # ---------------------------------------------------------- checkpoint --

    def _load_checkpoint(self) -> dict:
        try:
            with open(self._checkpoint_path, encoding="utf-8") as f:
                cp = json.load(f)
            return {
                "done": set(int(p) for p in cp.get("done", [])),
                "page": {int(k): int(v) for k, v in cp.get("page", {}).items()},
            }
        except (OSError, ValueError, TypeError):
            return {"done": set(), "page": {}}

    def _save_checkpoint(self, cp: dict):
        self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._checkpoint_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {"done": sorted(cp["done"]), "page": {str(k): v for k, v in cp["page"].items()}},
                f,
            )
        tmp.replace(self._checkpoint_path)

    @staticmethod
    def _reported_total(html: str) -> Optional[int]:
        """Read the listing's own 'სულ მოიძებნა N' (found N total) counter."""
        m = re.search(r"სულ მოიძებნა\s*(\d+)", html)
        return int(m.group(1)) if m else None

    def normalize(self, raw: dict) -> dict:
        """Transform raw case record into standard schema."""
        case_id = raw.get("id", 0)
        palata = raw.get("palata", 1)
        case_number = raw.get("case_number", f"Case-{case_id}")
        date = raw.get("date")

        # A case whose /fullcase/ fetch failed used to fall back to `subject`,
        # emitting a one-line stub that still counted as an ingested decision.
        # Skip it instead so the run's error count reflects the real loss.
        text = raw.get("full_text", "")
        if not text or len(text) < 200:
            logger.debug(f"Skipping {case_id}: no usable full text")
            return None

        return {
            "_id": f"GE-SC-{case_id}",
            "_source": "GE/SupremeCourt-Decisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": f"საქმე {case_number}" if case_number else f"Case GE-SC-{case_id}",
            "text": text,
            "date": date,
            "url": f"{BASE_URL}/ka/fullcase/{case_id}/{palata}",
            "case_number": case_number,
            "palata": PALATAS.get(palata, str(palata)),
            "subject": raw.get("subject", ""),
            "result": raw.get("result", ""),
            "appeal_type": raw.get("appeal_type", ""),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Sweep every palata, page by page, with full text for each case.

        Yields raw case dicts (not normalized). BaseScraper.bootstrap()
        calls self.normalize() on each yielded record.

        The corpus is ~88K decisions and supremecourt.ge sustains only ~9
        requests/minute (see MIN_REQUEST_INTERVAL), so one full sweep runs
        well past a single fleet slot. Progress is therefore checkpointed
        per (palata, page): a relaunched slot skips completed palatas with
        **zero** network calls and resumes the in-progress one at its last
        unfinished page, so successive runs advance monotonically instead of
        re-walking the head of the listing (#1158).
        """
        cp = self._load_checkpoint()
        total_fetched = 0
        expected = {}
        swept = {}

        for palata in [0, 1, 2]:
            if palata in cp["done"]:
                logger.info(f"Palata {palata} already complete per checkpoint — skipping")
                continue

            page = cp["page"].get(palata, 1)
            logger.info(f"Fetching palata {palata}: {PALATAS[palata]} (from page {page})")
            seen_here = 0

            while True:
                html = self._get_cases_page(palata=palata, page=page)

                if palata not in expected:
                    reported = self._reported_total(html)
                    if reported:
                        expected[palata] = reported
                        logger.info(
                            f"Palata {palata}: listing reports {reported} decisions "
                            f"(~{-(-reported // PAGE_SIZE)} pages)"
                        )

                cases = parse_case_listing(html)
                if not cases:
                    # A *successful* response with no case blocks is the only
                    # legitimate end-of-listing signal. Throttles and network
                    # failures raise ThrottledOut from _request instead.
                    logger.info(f"Palata {palata}: listing exhausted at page {page}")
                    break

                for case in cases:
                    full_text = self._get_full_case(case["id"], case["palata"])
                    if full_text:
                        case["full_text"] = full_text

                    yield case
                    total_fetched += 1
                    seen_here += 1

                logger.info(
                    f"Palata {palata}, page {page}: {len(cases)} cases "
                    f"(palata {seen_here}, run total {total_fetched})"
                )
                page += 1
                cp["page"][palata] = page
                self._save_checkpoint(cp)

            cp["done"].add(palata)
            cp["page"][palata] = page
            self._save_checkpoint(cp)
            swept[palata] = seen_here

        self._report_shortfall(expected, swept, total_fetched)

    def _report_shortfall(self, expected: dict, swept: dict, total_fetched: int):
        """Compare what we swept against the listing's own totals.

        #1158 went unnoticed for a full cycle because a 60-record run exited 0
        and read as a clean completion. A run that falls materially short of
        the site's advertised count now says so, loudly and in status.yaml.
        """
        logger.info(f"Total fetched this run: {total_fetched} decisions")
        for palata, want in expected.items():
            got = swept.get(palata)
            if got is None:
                continue  # palata aborted mid-way; the raised error is the signal
            if got < want * 0.95:
                logger.error(
                    f"COVERAGE SHORTFALL: palata {palata} swept {got} of {want} "
                    f"decisions the listing reports ({got / want:.1%})"
                )
                self.record_coverage_gap(
                    f"palata-{palata}",
                    "swept fewer decisions than the listing reports",
                    swept=got,
                    reported=want,
                )
            else:
                self.clear_coverage_gap(f"palata-{palata}")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Fetch recently added cases since the last checkpoint.

        Listings are newest-first, so we page through each palata and keep
        yielding until we reach a page whose cases are *entirely* older than
        ``since`` (robust to minor date-ordering jitter). There is no small
        page cap here: the update lane must enumerate the full set of new
        decisions since the checkpoint, however many pages that spans
        (issue #1158 — a 10-page cap was truncating the sweep to a head slice
        while the live corpus had grown ~25K rows beyond the last ingest).
        """
        since_str = str(since)[:10] if since else "2026-01-01"
        # Absolute safety ceiling so a pathological/unsorted listing can't loop
        # forever; ~87K rows / 30 per page ≈ 2,900 pages per palata worst case.
        hard_page_cap = 4000
        for palata in [0, 1, 2]:
            page = 1

            while page <= hard_page_cap:
                html = self._get_cases_page(palata=palata, page=page)

                cases = parse_case_listing(html)
                if not cases:
                    break

                new_on_page = 0
                for case in cases:
                    if case.get("date") and case["date"] < since_str:
                        continue

                    full_text = self._get_full_case(case["id"], case["palata"])
                    if full_text:
                        case["full_text"] = full_text

                    yield case
                    new_on_page += 1

                # Stop only once an entire page is older than the checkpoint —
                # one stray old row no longer aborts the sweep.
                if new_on_page == 0:
                    break
                page += 1

    def test_api(self):
        """Quick API connectivity test."""
        print("Testing GE/SupremeCourt-Decisions API...")

        grand_total = 0
        for palata in [0, 1, 2]:
            try:
                html = self._get_cases_page(palata=palata, page=1)
            except ThrottledOut as e:
                print(f"  Palata {palata}: FAILED to fetch — {e}")
                continue

            total = self._reported_total(html)
            grand_total += total or 0
            cases = parse_case_listing(html)
            print(f"  Palata {palata} ({PALATAS[palata]}): {total or '?'} total, "
                  f"{len(cases)} on page 1")

            if cases:
                case = cases[0]
                print(f"    First: {case.get('case_number')} ({case.get('date')})")
                full_text = self._get_full_case(case["id"], case["palata"])
                if full_text:
                    print(f"    Full text: {len(full_text)} chars")
                    print(f"    Preview: {full_text[:150]}...")
                else:
                    print("    Full text: FAILED")

        cp = self._load_checkpoint()
        print(f"\nCorpus reported by site: {grand_total} decisions")
        print(f"Checkpoint: palatas done={sorted(cp['done'])} pages={cp['page']}")
        print(f"Pace: {self._interval:.1f}s/request, {self._throttle_hits} throttle hits")
        print("API test complete.")


def main():
    scraper = SupremeCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if "--reset-checkpoint" in sys.argv:
        scraper._checkpoint_path.unlink(missing_ok=True)
        logger.info("Checkpoint cleared — next sweep restarts from palata 0, page 1")

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
            count = stats.get("sample_records_saved", 0)
        else:
            stats = scraper.bootstrap()
            count = stats.get("records_new", 0)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2, default=str)}")
        if stats.get("error_message"):
            # Partial progress is fine — the checkpoint means the next slot
            # resumes — but it must not read as a clean full sweep.
            logger.error(f"Sweep ended early: {stats['error_message']}")
        sys.exit(0 if count >= 10 else (1 if not sample else 0))

    elif command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2, default=str)}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
